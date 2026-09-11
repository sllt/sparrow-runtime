#[cfg(feature = "demo-io")]
use std::net::SocketAddr;
#[cfg(feature = "demo-io")]
use std::sync::atomic::AtomicU16;
#[cfg(any(test, feature = "demo-io"))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "demo-io")]
use std::sync::Mutex;
use std::time::Duration;

use sparrow_formats::encode_json_batch_bounded_with_capacity;
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, RestoreClaim, RowBatch, Schema,
};
#[cfg(any(test, feature = "demo-io"))]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(any(test, feature = "demo-io"))]
use tokio::net::TcpListener;
#[cfg(feature = "demo-io")]
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::error::{ConnectorError, Result};
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;
use crate::tls::{http_client, TlsConfig};

const MAX_OUTBOX: usize = 1024;
const MAX_RETRIES: u32 = 8;
const DELIVERY_OVERHEAD: usize = 512;

#[derive(Clone, Debug)]
pub struct HttpSinkConfig {
    pub url: String,
    pub tls: TlsConfig,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
    pub header_secret: Option<String>,
    /// Coalescing target, not a split of an already-formed upstream batch.
    pub batch_rows: usize,
    /// Hard encoded request bound (including brackets). Oversized input fails.
    pub batch_bytes: usize,
    pub linger: Duration,
    /// >1 explicitly permits request completion/delivery reordering.
    pub max_inflight: usize,
}

impl HttpSinkConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::HTTP_SINK
    }

    pub fn demo(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            tls: TlsConfig::disabled(),
            timeout: Duration::from_millis(1500),
            connect_timeout: Duration::from_millis(500),
            max_retries: 2,
            retry_backoff: Duration::from_millis(40),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
            header_secret: None,
            batch_rows: 1,
            batch_bytes: 256 * 1024,
            linger: Duration::ZERO,
            max_inflight: 1,
        }
    }

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        if !(1..=65536).contains(&self.batch_rows)
            || !(2..=1024 * 1024).contains(&self.batch_bytes)
            || self.linger > Duration::from_secs(1)
            || !(1..=8).contains(&self.max_inflight)
            || (self.batch_bytes + 512).saturating_mul(self.max_inflight + 2) > 4 * 1024 * 1024
        {
            return Err(ConnectorError::new(ErrorCode::BoundExceeded,
                "HTTP batch_rows 1..65536, batch_bytes 2..1048576, linger_ms 0..1000, max_inflight 1..8; combined buffers must fit 4MiB"));
        }
        self.tls.validate()?;
        if self.url.starts_with("https://") && !self.tls.enabled {
            // HTTPS always verifies; tls.enabled is a documentation hook.
        }
        if self.tls.enabled && !self.url.starts_with("https://") {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "HTTP sink tls.enabled requires an https:// URL (reqwest rustls verifies certificates)",
            ));
        }
        if self.outbox_capacity == 0 || self.outbox_capacity > MAX_OUTBOX {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "HTTP outbox_capacity {} is outside 1..={MAX_OUTBOX}",
                    self.outbox_capacity
                ),
            ));
        }
        if self.max_retries > MAX_RETRIES {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "HTTP max_retries {} exceeds {MAX_RETRIES}",
                    self.max_retries
                ),
            ));
        }
        policy.check_http_url(&self.url)?;
        if let Some(name) = &self.header_secret {
            if !self.url.starts_with("https://") {
                return Err(ConnectorError::new(
                    ErrorCode::PolicyDenied,
                    "HTTP header_secret requires an https:// URL (refusing plaintext credentials)",
                ));
            }
            let _ = secrets.resolve(name)?;
        }
        Ok(())
    }
}

pub struct HttpSink {
    pub config: HttpSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    client: reqwest::Client,
    auth: Option<String>,
}

struct Receipts {
    batches: usize,
    outbox: Option<Arc<InflightCounter>>,
    diag: Arc<IoDiagnostics>,
}
impl Receipts {
    fn ack(&mut self) {
        if let Some(outbox) = &self.outbox {
            for _ in 0..self.batches {
                outbox.ack();
            }
        }
        self.diag
            .http_acked_batches
            .fetch_add(self.batches as u64, Ordering::Relaxed);
        self.batches = 0;
    }
}
impl Drop for Receipts {
    fn drop(&mut self) {
        if let Some(outbox) = &self.outbox {
            for _ in 0..self.batches {
                outbox.fail();
            }
        }
        self.diag
            .http_dropped
            .fetch_add(self.batches as u64, Ordering::Relaxed);
    }
}

struct Delivery {
    bytes: Vec<u8>,
    lease: MemoryLease,
    schema: Arc<Schema>,
    rows: usize,
    receipts: Receipts,
    first: tokio::time::Instant,
}
enum MergeFailure {
    Separate(Delivery),
    DiscardGroup(Delivery),
}
impl Delivery {
    fn merge(
        &mut self,
        mut next: Self,
        cap: usize,
        target_rows: usize,
    ) -> std::result::Result<(), MergeFailure> {
        let comma = usize::from(self.rows > 0 && next.rows > 0);
        let size = self.bytes.len() + next.bytes.len() - 2 + comma;
        if size > cap
            || self.rows.saturating_add(next.rows) > target_rows
            || self.schema != next.schema
            || !Arc::ptr_eq(self.lease.owner(), next.lease.owner())
        {
            return Err(MergeFailure::Separate(next));
        }
        if size > self.bytes.capacity() {
            let capacity = size.next_power_of_two().min(cap);
            // Credit both source buffers while growing the destination. If
            // headroom is unavailable, flush separately through the existing
            // carry path; neither input has failed and receipts stay separate.
            if self.lease.grow_to(capacity + DELIVERY_OVERHEAD).is_err() {
                return Err(MergeFailure::Separate(next));
            }
            if self
                .bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .is_err()
            {
                return Err(MergeFailure::Separate(next));
            }
            // Reconcile any allocator-provided surplus before using it. A
            // failure abandons this group conservatively, never an unbilled
            // request. Standard Global Vec allocation uses exact capacity.
            if self.bytes.capacity() > capacity
                && self
                    .lease
                    .grow_to(self.bytes.capacity() + DELIVERY_OVERHEAD)
                    .is_err()
            {
                self.receipts
                    .diag
                    .http_budget_drops
                    .fetch_add(self.receipts.batches as u64, Ordering::Relaxed);
                return Err(MergeFailure::DiscardGroup(next));
            }
        }
        self.bytes.pop();
        if comma != 0 {
            self.bytes.push(b',');
        }
        self.bytes
            .extend_from_slice(&next.bytes[1..next.bytes.len() - 1]);
        self.bytes.push(b']');
        self.rows += next.rows;
        self.receipts.batches += next.receipts.batches;
        next.receipts.batches = 0;
        Ok(())
    }
}

struct HttpInflight(Arc<IoDiagnostics>);
impl Drop for HttpInflight {
    fn drop(&mut self) {
        self.0.http_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl HttpSink {
    pub fn bind(
        config: HttpSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        let client = http_client(config.timeout, config.connect_timeout)?;
        let auth = match &config.header_secret {
            Some(n) => Some(secrets.resolve(n)?),
            None => None,
        };
        Ok(Self {
            config,
            diag,
            client,
            auth,
        })
    }

    pub async fn run(
        self,
        mut rx: mpsc::Receiver<RowBatch>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let sink = Arc::new(self);
        let mut tasks = tokio::task::JoinSet::new();
        let mut group: Option<Delivery> = None;
        let mut carry: Option<Delivery> = None;
        let mut closed = false;
        let mut flush = false;
        let mut force_flush = false;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            if outbox.as_ref().is_some_and(|o| o.pending() == 0) {
                force_flush = false;
            }
            if group.is_none() && tasks.len() < sink.config.max_inflight {
                group = carry.take();
            }
            if group.as_ref().is_some_and(|g| {
                flush
                    || force_flush
                    || closed
                    || g.rows >= sink.config.batch_rows
                    || g.bytes.len() >= sink.config.batch_bytes
                    || g.first.elapsed() >= sink.config.linger
            }) && tasks.len() < sink.config.max_inflight
            {
                let mut delivery = group.take().unwrap();
                let worker = sink.clone();
                tasks.spawn(async move {
                    worker.diag.http_inflight.fetch_add(1, Ordering::Relaxed);
                    let _inflight = HttpInflight(worker.diag.clone());
                    if worker
                        .post_body(reqwest::Body::from(std::mem::take(&mut delivery.bytes)))
                        .await
                    {
                        worker.diag.http_posted.fetch_add(1, Ordering::Relaxed);
                        delivery.receipts.ack();
                    }
                });
                flush = false;
                continue;
            }
            if closed && group.is_none() && carry.is_none() && tasks.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = tasks.join_next(), if !tasks.is_empty() => {},
                _ = async { match &outbox { Some(o) => o.flush_requested().await, None => std::future::pending().await } } => { force_flush = true; },
                _ = async { match &group { Some(g) => tokio::time::sleep_until(g.first + sink.config.linger).await, None => std::future::pending().await } }, if group.is_some() && !flush => { flush = true; },
                next = rx.recv(), if !closed && carry.is_none() && !flush && tasks.len() < sink.config.max_inflight => {
                    match next {
                        None => closed = true,
                        Some(batch) => {
                            if let Some(next) = sink.encode_delivery(batch, outbox.clone()) {
                                if let Some(g) = &mut group {
                                    match g.merge(next, sink.config.batch_bytes, sink.config.batch_rows) {
                                        Ok(()) => {},
                                        Err(MergeFailure::Separate(next)) => {
                                            carry = Some(next);
                                            flush = true;
                                        }
                                        Err(MergeFailure::DiscardGroup(next)) => {
                                            drop(group.take());
                                            carry = Some(next);
                                        }
                                    }
                                } else { group = Some(next); }
                            }
                        }
                    }
                }
            }
            // A flush notification with no collector data must not disable recv.
            if group.is_none() && carry.is_none() {
                flush = false;
            }
        }
        // On EOF all accepted groups have completed. On stop, unresolved work
        // fails receipts (including buffered batches), never a false SinkFlushed.
        rx.close();
        drop(group);
        drop(carry);
        tasks.shutdown().await;
        while let Ok(_batch) = rx.try_recv() {
            drop(Receipts {
                batches: 1,
                outbox: outbox.clone(),
                diag: sink.diag.clone(),
            });
        }
    }

    fn encode_delivery(
        &self,
        batch: RowBatch,
        outbox: Option<Arc<InflightCounter>>,
    ) -> Option<Delivery> {
        let first = tokio::time::Instant::now();
        let receipts = Receipts {
            batches: 1,
            outbox,
            diag: self.diag.clone(),
        };
        // Keep the input lease alive and bill each encoded capacity increase
        // before allocation. Small bodies no longer reserve batch_bytes.
        let mut lease = batch
            .lease()
            .owner()
            .acquire(CreditKind::Reservation, DELIVERY_OVERHEAD)
            .map_err(|error| {
                self.diag.http_budget_drops.fetch_add(1, Ordering::Relaxed);
                error
            })
            .ok()?;
        let bytes = encode_json_batch_bounded_with_capacity(
            batch.schema(),
            batch.rows(),
            self.config.batch_bytes,
            |capacity| lease.grow_to(capacity + DELIVERY_OVERHEAD),
        )
        .map_err(|error| {
            if error.code == ErrorCode::ResourceExhausted {
                self.diag.http_budget_drops.fetch_add(1, Ordering::Relaxed);
            } else {
                self.diag.http_encode_errors.fetch_add(1, Ordering::Relaxed);
            }
            error
        })
        .ok()?;
        Some(Delivery {
            bytes,
            lease,
            schema: batch.schema_arc(),
            rows: batch.num_rows(),
            receipts,
            first,
        })
    }

    #[cfg(test)]
    async fn post_bytes(&self, body: &[u8]) -> bool {
        self.post_body(reqwest::Body::from(body.to_vec())).await
    }

    // Body owns the encoded Vec once. Retrying clones its shared byte handle,
    // not the payload; callers construct only replayable in-memory bodies.
    async fn post_body(&self, body: reqwest::Body) -> bool {
        let mut template = self
            .client
            .post(&self.config.url)
            .header("content-type", "application/json")
            .body(body);
        if let Some(token) = &self.auth {
            template = template.header("authorization", format!("Bearer {token}"));
        }
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                self.diag.http_retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            let Some(req) = template.try_clone() else {
                // Invalid request construction (for example a malformed header)
                // fails delivery; it must not panic the sink task.
                self.diag.http_failed.fetch_add(1, Ordering::Relaxed);
                return false;
            };
            match req.send().await {
                Ok(mut resp) if resp.status().is_success() => {
                    // Consume small response bodies so reqwest can reuse HTTP/1.1
                    // connections. A 2xx already accepted the POST: a malformed or
                    // oversized response must not cause a duplicate retry.
                    let mut drained = 0usize;
                    while let Ok(Some(chunk)) = resp.chunk().await {
                        drained = drained.saturating_add(chunk.len());
                        if drained > 64 * 1024 {
                            break;
                        }
                    }
                    return true;
                }
                Ok(resp) if resp.status().is_client_error() => {
                    // Do not retry 4xx (P1-22).
                    self.diag.http_failed.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                Ok(_) | Err(_) => {
                    self.diag.http_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        false
    }
}

/// One complete HTTP request observed by the demo/benchmark receiver.
#[cfg(feature = "demo-io")]
#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub body: Vec<u8>,
    /// Captured when the full request body is received, not when a later poll
    /// inspects the buffer. Same monotonic clock as an in-process benchmark sender.
    pub received_at: std::time::Instant,
}

#[cfg(feature = "demo-io")]
#[derive(Default)]
struct CaptureBuffer {
    requests: Vec<CapturedRequest>,
    bytes: usize,
}

#[cfg(feature = "demo-io")]
/// Tiny HTTP/1.1 capture server for demos and CI. Optional per-request delay
/// demonstrates slow-sink backpressure. Only Content-Length framing is supported.
pub struct HttpCapture {
    pub addr: SocketAddr,
    bodies: Arc<Mutex<CaptureBuffer>>,
    connections: Arc<AtomicU64>,
    overflows: Arc<AtomicU64>,
    delay_ms: Arc<AtomicU64>,
    status: Arc<AtomicU16>,
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "demo-io")]
impl HttpCapture {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("bind HTTP capture: {e}"))
        })?;
        let addr = listener.local_addr().map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("HTTP local_addr: {e}"))
        })?;
        let bodies = Arc::new(Mutex::new(CaptureBuffer::default()));
        let connections = Arc::new(AtomicU64::new(0));
        let overflows = Arc::new(AtomicU64::new(0));
        let delay_ms = Arc::new(AtomicU64::new(0));
        let status = Arc::new(AtomicU16::new(200));
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let bodies_task = Arc::clone(&bodies);
        let delay_task = Arc::clone(&delay_ms);
        let status_task = Arc::clone(&status);
        let connections_task = Arc::clone(&connections);
        let overflows_task = Arc::clone(&overflows);
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = child.cancelled() => break,
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, _)) => {
                                connections_task.fetch_add(1, Ordering::Relaxed);
                                let overflows = Arc::clone(&overflows_task);
                                let bodies = Arc::clone(&bodies_task);
                                let delay = Arc::clone(&delay_task);
                                let status = Arc::clone(&status_task);
                                let child = child.clone();
                                tokio::spawn(async move {
                                    let _ = handle_http(stream, bodies, delay, status, overflows, child).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        Ok(Self {
            addr,
            bodies,
            connections,
            overflows,
            delay_ms,
            status,
            cancel,
            join,
        })
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}/ingest", self.addr.port())
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub fn set_delay_ms(&self, ms: u64) {
        self.delay_ms.store(ms, Ordering::SeqCst);
    }

    /// Capture response status. 4xx is not retried by the HTTP sink (P1-22).
    pub fn set_status(&self, code: u16) {
        self.status.store(code, Ordering::SeqCst);
    }

    pub fn bodies(&self) -> Vec<Vec<u8>> {
        self.bodies
            .lock()
            .expect("http bodies")
            .requests
            .iter()
            .map(|r| r.body.clone())
            .collect()
    }

    pub fn drain_requests(&self) -> Vec<CapturedRequest> {
        let mut buffer = self.bodies.lock().expect("http bodies");
        buffer.bytes = 0;
        std::mem::take(&mut buffer.requests)
    }

    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }
    pub fn overflows(&self) -> u64 {
        self.overflows.load(Ordering::Relaxed)
    }

    pub fn body_strings(&self) -> Vec<String> {
        self.bodies()
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect()
    }

    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

#[cfg(feature = "demo-io")]
async fn handle_http(
    mut stream: TcpStream,
    bodies: Arc<Mutex<CaptureBuffer>>,
    delay_ms: Arc<AtomicU64>,
    status: Arc<AtomicU16>,
    overflows: Arc<AtomicU64>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let header_end = loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            if let Some(pos) = find_header_end(&buf) {
                break pos;
            }
            let n = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                read = stream.read(&mut tmp) => read,
            }
            .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("HTTP read: {e}")))?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.len() > 128 * 1024 {
                return Err(ConnectorError::new(
                    ErrorCode::MaxRecordSize,
                    "HTTP request exceeded 128KiB",
                ));
            }
            if let Some(pos) = find_header_end(&buf) {
                break pos;
            }
        };
        let header = String::from_utf8_lossy(&buf[..header_end]);
        let close = header
            .lines()
            .any(|line| line.eq_ignore_ascii_case("connection: close"));
        let content_len = header
            .lines()
            .find_map(|l| {
                let l = l.to_ascii_lowercase();
                l.strip_prefix("content-length:")
                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0);
        if content_len > 64 * 1024 {
            return Err(ConnectorError::new(
                ErrorCode::MaxRecordSize,
                "HTTP body exceeds 64KiB",
            ));
        }
        let frame_end = header_end + 4 + content_len;
        while buf.len() < frame_end {
            let n = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                read = stream.read(&mut tmp) => read,
            }
            .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("HTTP body: {e}")))?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        let received_at = std::time::Instant::now();
        let body = buf[header_end + 4..frame_end].to_vec();
        buf.drain(..frame_end);
        let delay = delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
            }
        }
        let captured = {
            let mut buffer = bodies.lock().expect("http bodies");
            if buffer.bytes.saturating_add(body.len()) > 32 * 1024 * 1024
                || buffer.requests.len() >= 262_144
            {
                overflows.fetch_add(1, Ordering::Relaxed);
                false
            } else {
                buffer.bytes += body.len();
                buffer.requests.push(CapturedRequest { body, received_at });
                true
            }
        };
        let code = if captured {
            status.load(Ordering::SeqCst)
        } else {
            503
        };
        let connection = if close { "close" } else { "keep-alive" };
        let resp = if code == 204 {
            format!("HTTP/1.1 204 No Content\r\nconnection: {connection}\r\n\r\n").into_bytes()
        } else if code >= 400 {
            format!("HTTP/1.1 {code} ERR\r\ncontent-length: 0\r\nconnection: {connection}\r\n\r\n")
                .into_bytes()
        } else {
            format!("HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: {connection}\r\n\r\nok")
                .into_bytes()
        };
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            write = stream.write_all(&resp) => write,
        }
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("HTTP write: {e}")))?;
        if close {
            return Ok(());
        }
    }
}

#[cfg(feature = "demo-io")]
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::MapSecretResolver;
    #[cfg(feature = "demo-io")]
    use sparrow_model::InflightCounter;
    use sparrow_model::{
        CreditKind, DataType, Field, FieldId, MemoryOwner, ResourceBudget, Row, RowBatchBuilder,
        Scalar, Schema, SchemaId,
    };

    fn number_batch(owner: &Arc<MemoryOwner>, n: i64) -> RowBatch {
        let schema = Arc::new(
            Schema::new(
                1,
                vec![Field::new(FieldId::new(1), "n", DataType::Int64, false)],
            )
            .unwrap(),
        );
        let mut b =
            RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 1, 1024).unwrap();
        b.push(Row {
            values: vec![Scalar::Int64(n)],
        })
        .unwrap();
        b.finish().unwrap()
    }

    fn text_batch(owner: &Arc<MemoryOwner>, text: &str) -> RowBatch {
        let schema = Arc::new(
            Schema::new(
                1,
                vec![Field::new(FieldId::new(1), "text", DataType::Utf8, false)],
            )
            .unwrap(),
        );
        let mut b = RowBatchBuilder::new(
            schema,
            owner.clone(),
            CreditKind::Reservation,
            1,
            owner.budget().reservation_bytes,
        )
        .unwrap();
        b.push(Row {
            values: vec![Scalar::utf8(text)],
        })
        .unwrap();
        b.finish().unwrap()
    }

    fn accounting_sink(diag: Arc<IoDiagnostics>) -> HttpSink {
        let mut cfg = HttpSinkConfig::demo("http://127.0.0.1:12345/ingest");
        cfg.batch_rows = 1024;
        cfg.batch_bytes = 1024 * 1024;
        HttpSink::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", 12345),
            diag,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn incremental_delivery_fits_small_job_and_bills_capacity() {
        let mut budget = ResourceBudget::compact();
        budget.reservation_bytes = 8192;
        let root = MemoryOwner::new(budget);
        let owner = MemoryOwner::child(root.clone(), budget, "small-job");
        let diag = IoDiagnostics::new();
        let sink = accounting_sink(diag.clone());
        let outbox = Arc::new(InflightCounter::new());
        outbox.enqueue();
        let mut delivery = sink
            .encode_delivery(number_batch(&owner, 7), Some(outbox.clone()))
            .unwrap();
        assert_eq!(delivery.bytes, br#"[{"n":7}]"#);
        assert_eq!(
            delivery.lease.bytes(),
            delivery.bytes.capacity() + DELIVERY_OVERHEAD
        );
        assert!(
            delivery.lease.bytes() <= 1024,
            "must not reserve the 1 MiB configured cap"
        );
        assert_eq!(root.usage().reservation_bytes, delivery.lease.bytes());
        let working = owner.acquire(CreditKind::Reservation, 4096).unwrap();
        delivery.receipts.ack();
        drop(delivery);
        drop(working);
        assert_eq!(outbox.acked(), 1);
        assert_eq!(outbox.failed(), 0);
        assert_eq!(diag.snapshot().http_budget_drops, 0);
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(root.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn encoding_growth_rejects_job_and_process_pressure_without_leaks() {
        for parent_pressure in [false, true] {
            let mut budget = ResourceBudget::compact();
            budget.reservation_bytes = 8192;
            let root = MemoryOwner::new(budget);
            let owner = MemoryOwner::child(root.clone(), budget, "encode-job");
            let diag = IoDiagnostics::new();
            let sink = accounting_sink(diag.clone());
            let outbox = Arc::new(InflightCounter::new());
            let batch = text_batch(&owner, &"x".repeat(1000));
            // Allow overhead and the first 256-byte capacity, deny later
            // growth inside serde. A parent-only hold simulates a sibling job.
            let remaining = budget.reservation_bytes
                - owner.usage().reservation_bytes
                - DELIVERY_OVERHEAD
                - 256;
            let held = if parent_pressure { &root } else { &owner }
                .acquire(CreditKind::Reservation, remaining)
                .unwrap();
            outbox.enqueue();
            assert!(sink.encode_delivery(batch, Some(outbox.clone())).is_none());
            assert_eq!(outbox.pending(), 0);
            assert_eq!(outbox.failed(), 1);
            assert_eq!(diag.snapshot().http_budget_drops, 1);
            assert_eq!(diag.snapshot().http_encode_errors, 0);
            assert_eq!(diag.snapshot().http_dropped, 1);
            assert_eq!(root.usage().reservation_bytes, held.bytes());
            drop(held);
            assert_eq!(owner.usage().physical_bytes, 0);
            assert_eq!(root.usage().physical_bytes, 0);
        }
    }

    #[tokio::test]
    async fn merge_growth_is_incremental_and_pressure_keeps_both_receipts() {
        let mut budget = ResourceBudget::compact();
        budget.reservation_bytes = 8192;
        let root = MemoryOwner::new(budget);
        let owner = MemoryOwner::child(root.clone(), budget, "merge-job");
        let diag = IoDiagnostics::new();
        let sink = accounting_sink(diag.clone());
        let outbox = Arc::new(InflightCounter::new());
        let text = "x".repeat(180);
        outbox.enqueue();
        let mut left = sink
            .encode_delivery(text_batch(&owner, &text), Some(outbox.clone()))
            .unwrap();
        outbox.enqueue();
        let right = sink
            .encode_delivery(text_batch(&owner, &text), Some(outbox.clone()))
            .unwrap();
        let before = left.bytes.clone();
        let charged = left.lease.bytes();
        let held = root
            .acquire(
                CreditKind::Reservation,
                budget.reservation_bytes - root.usage().reservation_bytes,
            )
            .unwrap();
        let right = match left.merge(right, sink.config.batch_bytes, sink.config.batch_rows) {
            Err(MergeFailure::Separate(right)) => right,
            _ => panic!("growth must fail before mutating or settling either input"),
        };
        assert_eq!(left.bytes, before);
        assert_eq!(left.lease.bytes(), charged);
        assert_eq!(outbox.pending(), 2);
        assert_eq!(outbox.failed(), 0);
        assert_eq!(diag.snapshot().http_budget_drops, 0);
        drop(held);
        assert!(left
            .merge(right, sink.config.batch_bytes, sink.config.batch_rows)
            .is_ok());
        assert_eq!(
            left.lease.bytes(),
            left.bytes.capacity() + DELIVERY_OVERHEAD
        );
        assert_eq!(root.usage().reservation_bytes, left.lease.bytes());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&left.bytes).unwrap(),
            serde_json::json!([{"text":text}, {"text":text}])
        );
        left.receipts.ack();
        drop(left);
        assert_eq!(outbox.acked(), 2);
        assert_eq!(outbox.pending(), 0);
        assert_eq!(root.usage().physical_bytes, 0);
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn multiple_small_jobs_share_parent_credit_and_reuse_http_connections() {
        let http = HttpCapture::start().await.unwrap();
        http.set_status(204);
        http.set_delay_ms(20);
        let mut budget = ResourceBudget::compact();
        budget.reservation_bytes = 16 * 1024;
        let root = MemoryOwner::new(budget);
        let mut jobs = tokio::task::JoinSet::new();
        let mut owners = Vec::new();
        for job in 0..4 {
            let mut child_budget = budget;
            child_budget.reservation_bytes = 4096;
            let owner = MemoryOwner::child(root.clone(), child_budget, format!("http-job-{job}"));
            owners.push(owner.clone());
            let diag = IoDiagnostics::new();
            let outbox = Arc::new(InflightCounter::new());
            // Default batch_bytes is 256 KiB, much larger than each job quota.
            let sink = HttpSink::bind(
                HttpSinkConfig::demo(http.url()),
                &MapSecretResolver::empty(),
                &TargetPolicy::allow("127.0.0.1", http.port()),
                diag.clone(),
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(2);
            jobs.spawn(async move {
                let produce = async {
                    for n in 0..40 {
                        let batch = number_batch(&owner, job * 40 + n);
                        outbox.enqueue();
                        tx.send(batch).await.unwrap();
                    }
                    drop(tx);
                };
                tokio::join!(
                    produce,
                    sink.run(rx, CancellationToken::new(), Some(outbox.clone()))
                );
                assert_eq!(outbox.acked(), 40);
                assert_eq!(outbox.failed(), 0);
                assert_eq!(outbox.pending(), 0);
                assert_eq!(diag.snapshot().http_budget_drops, 0);
                assert_eq!(diag.snapshot().http_encode_errors, 0);
                assert_eq!(diag.snapshot().http_inflight, 0);
            });
        }
        tokio::time::timeout(Duration::from_secs(8), async {
            while let Some(result) = jobs.join_next().await {
                result.unwrap();
            }
        })
        .await
        .unwrap();
        let mut values: Vec<i64> = http
            .bodies()
            .iter()
            .map(|body| {
                serde_json::from_slice::<serde_json::Value>(body).unwrap()[0]["n"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        values.sort_unstable();
        assert_eq!(values, (0..160).collect::<Vec<_>>());
        assert!(http.connections() <= 4);
        for owner in owners {
            assert_eq!(owner.usage().live_handles, 0);
        }
        assert_eq!(root.usage().physical_bytes, 0);
        assert_eq!(root.usage().live_handles, 0);
        assert!(root.usage().peak_physical_bytes <= budget.reservation_bytes);
        http.stop().await;
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn coalescing_preserves_rows_and_settles_every_input_receipt() {
        for status in [204, 400] {
            let http = HttpCapture::start().await.unwrap();
            http.set_status(status);
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let diag = IoDiagnostics::new();
            let outbox = Arc::new(InflightCounter::new());
            let mut cfg = HttpSinkConfig::demo(http.url());
            cfg.batch_rows = 4;
            cfg.batch_bytes = 1024;
            cfg.linger = Duration::from_secs(1);
            let sink = HttpSink::bind(
                cfg,
                &MapSecretResolver::empty(),
                &TargetPolicy::allow("127.0.0.1", http.port()),
                diag.clone(),
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(8);
            for n in 0..4 {
                outbox.enqueue();
                tx.send(number_batch(&owner, n)).await.unwrap();
            }
            drop(tx);
            tokio::time::timeout(
                Duration::from_secs(2),
                sink.run(rx, CancellationToken::new(), Some(outbox.clone())),
            )
            .await
            .unwrap();
            let bodies = http.bodies();
            assert_eq!(bodies.len(), 1);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bodies[0]).unwrap(),
                serde_json::json!([{"n":0},{"n":1},{"n":2},{"n":3}])
            );
            assert_eq!(outbox.pending(), 0);
            assert_eq!(outbox.acked(), if status == 204 { 4 } else { 0 });
            assert_eq!(outbox.failed(), if status == 400 { 4 } else { 0 });
            assert_eq!(owner.usage().physical_bytes, 0);
            http.stop().await;
        }
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn byte_cap_splits_groups_and_rejects_oversized_single_batch() {
        for cap in [2, 16] {
            let http = HttpCapture::start().await.unwrap();
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let outbox = Arc::new(InflightCounter::new());
            let diag = IoDiagnostics::new();
            let mut cfg = HttpSinkConfig::demo(http.url());
            cfg.batch_rows = 10;
            cfg.batch_bytes = cap;
            cfg.linger = Duration::from_millis(50);
            let sink = HttpSink::bind(
                cfg,
                &MapSecretResolver::empty(),
                &TargetPolicy::allow("127.0.0.1", http.port()),
                diag.clone(),
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(8);
            for n in 0..3 {
                outbox.enqueue();
                tx.send(number_batch(&owner, n)).await.unwrap();
            }
            drop(tx);
            tokio::time::timeout(
                Duration::from_secs(2),
                sink.run(rx, CancellationToken::new(), Some(outbox.clone())),
            )
            .await
            .unwrap();
            assert_eq!(http.bodies().len(), if cap == 2 { 0 } else { 3 });
            assert!(http.bodies().iter().all(|b| b.len() <= cap));
            assert_eq!(outbox.pending(), 0);
            assert_eq!(outbox.failed(), if cap == 2 { 3 } else { 0 });
            assert_eq!(owner.usage().physical_bytes, 0);
            http.stop().await;
        }
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn checkpoint_flush_interrupts_linger_without_waiting_for_eof() {
        let http = HttpCapture::start().await.unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let outbox = Arc::new(InflightCounter::new());
        let diag = IoDiagnostics::new();
        let mut cfg = HttpSinkConfig::demo(http.url());
        cfg.batch_rows = 256;
        cfg.batch_bytes = 1024;
        cfg.linger = Duration::from_secs(1);
        let sink = HttpSink::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", http.port()),
            diag.clone(),
        )
        .unwrap();
        let (tx, rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
        outbox.enqueue();
        tx.send(number_batch(&owner, 7)).await.unwrap();
        // Let the collector receive the batch, but do not rely on a sleep to
        // trigger the flush. A request before collection must be retained too.
        tokio::task::yield_now().await;
        outbox.request_flush();
        tokio::time::timeout(Duration::from_millis(300), async {
            while outbox.pending() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(outbox.acked(), 1);
        cancel.cancel();
        task.await.unwrap();
        drop(tx);
        assert_eq!(owner.usage().physical_bytes, 0);
        http.stop().await;
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn concurrent_requests_are_bounded_and_stop_fails_unresolved_receipts() {
        for stop in [false, true] {
            let http = HttpCapture::start().await.unwrap();
            http.set_delay_ms(200);
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let outbox = Arc::new(InflightCounter::new());
            let diag = IoDiagnostics::new();
            let mut cfg = HttpSinkConfig::demo(http.url());
            cfg.max_inflight = 3;
            cfg.batch_bytes = 1024;
            let sink = HttpSink::bind(
                cfg,
                &MapSecretResolver::empty(),
                &TargetPolicy::allow("127.0.0.1", http.port()),
                diag.clone(),
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(8);
            for n in 0..6 {
                outbox.enqueue();
                tx.send(number_batch(&owner, n)).await.unwrap();
            }
            drop(tx);
            let cancel = CancellationToken::new();
            let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
            tokio::time::timeout(Duration::from_secs(2), async {
                while diag.http_inflight.load(Ordering::Relaxed) != 3 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if stop {
                cancel.cancel();
            }
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap();
            assert!(http.connections() <= 3);
            assert_eq!(diag.http_inflight.load(Ordering::Relaxed), 0);
            assert_eq!(outbox.pending(), 0);
            if !stop {
                assert_eq!(outbox.acked(), 6);
            } else {
                assert!(outbox.failed() > 0);
            }
            assert_eq!(owner.usage().physical_bytes, 0);
            http.stop().await;
        }
    }

    #[tokio::test]
    async fn v01_http_sink_does_not_follow_redirect_to_disallowed() {
        let hits = Arc::new(AtomicU64::new(0));
        let evil = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let evil_port = evil.local_addr().unwrap().port();
        let hits2 = Arc::clone(&hits);
        tokio::spawn(async move {
            if let Ok((mut s, _)) = evil.accept().await {
                hits2.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 64];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        });
        let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front_port = front.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut s, _)) = front.accept().await {
                let mut buf = [0u8; 256];
                let _ = s.read(&mut buf).await;
                let loc = format!("HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{evil_port}/secret\r\nContent-Length: 0\r\n\r\n");
                let _ = s.write_all(loc.as_bytes()).await;
            }
        });
        let url = format!("http://127.0.0.1:{front_port}/ingest");
        let cfg = HttpSinkConfig::demo(&url);
        let policy = TargetPolicy::allow("127.0.0.1", front_port);
        let sink = HttpSink::bind(
            cfg,
            &MapSecretResolver::empty(),
            &policy,
            IoDiagnostics::new(),
        )
        .unwrap();
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(
                FieldId::new(1),
                "device_id",
                DataType::Utf8,
                false,
            )],
        )
        .unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut b = RowBatchBuilder::new(
            std::sync::Arc::new(schema),
            owner,
            CreditKind::Reservation,
            1,
            1024,
        )
        .unwrap();
        b.push(Row {
            values: vec![Scalar::utf8("d")],
        })
        .unwrap();
        let batch = b.finish().unwrap();
        let (tx, rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        tokio::spawn(sink.run(rx, child, None));
        tx.send(batch).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "redirect hop to a disallowed target must not be followed"
        );
        cancel.cancel();
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn benchmark_capture_uses_receive_time_and_reuses_http_connection() {
        let http = HttpCapture::start().await.unwrap();
        let sink = HttpSink::bind(
            HttpSinkConfig::demo(http.url()),
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", http.port()),
            IoDiagnostics::new(),
        )
        .unwrap();
        assert!(sink.post_bytes(br#"[{"seq":1},{"seq":2}]"#).await);
        let received_before = std::time::Instant::now();
        assert!(sink.post_bytes(br#"{"seq":3}"#).await);
        let records = http.drain_requests();
        assert_eq!(records.len(), 2);
        assert!(records[0].received_at <= received_before);
        assert_eq!(
            http.connections(),
            1,
            "sink must consume replies and reuse its connection"
        );
        assert_eq!(http.overflows(), 0);
        assert!(http.body_strings().is_empty());
        http.set_status(204);
        assert!(sink.post_bytes(br#"{"seq":4}"#).await);
        assert!(sink.post_bytes(br#"{"seq":5}"#).await);
        assert_eq!(http.connections(), 1, "204 replies must also reuse TCP");
        assert_eq!(http.drain_requests().len(), 2);
        http.stop().await;
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn retries_replay_identical_owned_body() {
        let http = HttpCapture::start().await.unwrap();
        http.set_status(503);
        let diag = IoDiagnostics::new();
        let sink = HttpSink::bind(
            HttpSinkConfig::demo(http.url()),
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", http.port()),
            Arc::clone(&diag),
        )
        .unwrap();
        let body = br#"[{"seq":1,"text":"a\"b"}]"#.to_vec();
        assert!(!sink.post_body(reqwest::Body::from(body.clone())).await);
        assert_eq!(http.bodies(), vec![body; 3]);
        assert_eq!(diag.http_retries.load(Ordering::Relaxed), 2);
        http.stop().await;
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn accepted_post_with_truncated_response_is_not_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n[]") {
                let mut chunk = [0u8; 1024];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&chunk[..n]);
            }
            // The application accepted the POST, but closes before the body ends.
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nok")
                .await
                .unwrap();
        });
        let diag = IoDiagnostics::new();
        let sink = HttpSink::bind(
            HttpSinkConfig::demo(format!("http://127.0.0.1:{port}/ingest")),
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
            Arc::clone(&diag),
        )
        .unwrap();
        assert!(sink.post_bytes(b"[]").await);
        assert_eq!(diag.http_retries.load(Ordering::Relaxed), 0);
        server.await.unwrap();
    }

    #[cfg(feature = "demo-io")]
    #[tokio::test]
    async fn http_sink_4xx_fails_outbox_not_success_ack() {
        let http = HttpCapture::start().await.unwrap();
        http.set_status(400);
        let policy = TargetPolicy::allow("127.0.0.1", http.port());
        let diag = IoDiagnostics::new();
        let sink = HttpSink::bind(
            HttpSinkConfig::demo(http.url()),
            &MapSecretResolver::empty(),
            &policy,
            Arc::clone(&diag),
        )
        .unwrap();
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(
                FieldId::new(1),
                "device_id",
                DataType::Utf8,
                false,
            )],
        )
        .unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut b = RowBatchBuilder::new(
            std::sync::Arc::new(schema),
            owner,
            CreditKind::Reservation,
            1,
            1024,
        )
        .unwrap();
        b.push(Row {
            values: vec![Scalar::utf8("d")],
        })
        .unwrap();
        let batch = b.finish().unwrap();
        let (tx, rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let outbox = Arc::new(InflightCounter::new());
        outbox.enqueue();
        let outbox_r = Arc::clone(&outbox);
        tokio::spawn(sink.run(rx, child, Some(outbox_r)));
        tx.send(batch).await.unwrap();
        let start = std::time::Instant::now();
        while outbox.pending() > 0 && start.elapsed() < Duration::from_secs(2) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(outbox.pending(), 0);
        assert_eq!(
            outbox.acked(),
            0,
            "4xx must not count as a successful flush ack"
        );
        assert_eq!(outbox.failed(), 1);
        assert!(
            diag.snapshot().http_dropped >= 1 || diag.snapshot().http_failed >= 1,
            "{:?}",
            diag.snapshot()
        );
        cancel.cancel();
        http.stop().await;
    }

    #[test]
    fn n16_http_header_secret_requires_https() {
        let secrets =
            MapSecretResolver::new([("tok".into(), "secret".into())].into_iter().collect());
        let mut http = HttpSinkConfig::demo("http://127.0.0.1:1/ingest");
        http.header_secret = Some("tok".into());
        let err = http
            .validate(&secrets, &TargetPolicy::allow("127.0.0.1", 1))
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::PolicyDenied);
        assert!(err.to_string().contains("https"), "{}", err);

        let mut https = HttpSinkConfig::demo("https://127.0.0.1:443/ingest");
        https.header_secret = Some("tok".into());
        https
            .validate(&secrets, &TargetPolicy::allow("127.0.0.1", 443))
            .expect("https + header_secret must be accepted");
    }
}

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
#[cfg(feature = "demo-io")]
use std::sync::atomic::AtomicU16;
#[cfg(any(test, feature = "demo-io"))]
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
#[cfg(feature = "demo-io")]
use std::sync::Mutex;
use std::time::Duration;

use sparrow_formats::{encode_json_batch, JsonLimits};
use sparrow_model::{ErrorCode, InflightCounter, RestoreClaim, RowBatch};
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
        }
    }

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
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
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                next = rx.recv() => {
                    match next {
                        Some(batch) => {
                            let ok = self.post_batch(&batch).await;
                            if let Some(o) = &outbox {
                                if ok {
                                    o.ack();
                                } else {
                                    // Aligned treats a drop/4xx as a failed flush,
                                    // not a successful SinkFlushed.
                                    o.fail();
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    }

    async fn post_batch(&self, batch: &RowBatch) -> bool {
        let schema = batch.schema();
        let body = match encode_json_batch(schema, batch.rows()) {
            Ok(b)
                if b.len()
                    <= JsonLimits::default()
                        .max_bytes
                        .saturating_mul(batch.num_rows().max(1)) =>
            {
                b
            }
            Ok(_) => {
                self.diag.http_dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            Err(_) => {
                self.diag.http_dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };
        self.diag.http_inflight.fetch_add(1, Ordering::Relaxed);
        let ok = self.post_bytes(&body).await;
        self.diag.http_inflight.fetch_sub(1, Ordering::Relaxed);
        if ok {
            self.diag.http_posted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.diag.http_dropped.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    async fn post_bytes(&self, body: &[u8]) -> bool {
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                self.diag.http_retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            let mut req = self
                .client
                .post(&self.config.url)
                .header("content-type", "application/json")
                .body(body.to_vec());
            if let Some(token) = &self.auth {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => return true,
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

/// Tiny HTTP/1.1 capture server for demos and CI. Optional per-request delay
/// demonstrates slow-sink backpressure.
#[cfg(feature = "demo-io")]
pub struct HttpCapture {
    pub addr: SocketAddr,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
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
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let delay_ms = Arc::new(AtomicU64::new(0));
        let status = Arc::new(AtomicU16::new(200));
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let bodies_task = Arc::clone(&bodies);
        let delay_task = Arc::clone(&delay_ms);
        let status_task = Arc::clone(&status);
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = child.cancelled() => break,
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, _)) => {
                                let bodies = Arc::clone(&bodies_task);
                                let delay = Arc::clone(&delay_task);
                                let status = Arc::clone(&status_task);
                                let child = child.clone();
                                tokio::spawn(async move {
                                    let _ = handle_http(stream, bodies, delay, status, child).await;
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
        self.bodies.lock().expect("http bodies").clone()
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
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    delay_ms: Arc<AtomicU64>,
    status: Arc<AtomicU16>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let header_end = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let n = stream
            .read(&mut tmp)
            .await
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
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_len {
        let n = stream
            .read(&mut tmp)
            .await
            .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("HTTP body: {e}")))?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_len);
    let delay = delay_ms.load(Ordering::SeqCst);
    if delay > 0 {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
        }
    }
    bodies.lock().expect("http bodies").push(body);
    let code = status.load(Ordering::SeqCst);
    let resp = if code >= 400 {
        format!("HTTP/1.1 {code} ERR\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .into_bytes()
    } else {
        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok".to_vec()
    };
    stream
        .write_all(&resp)
        .await
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("HTTP write: {e}")))?;
    Ok(())
}

#[cfg(feature = "demo-io")]
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::MapSecretResolver;
    use sparrow_model::{
        CreditKind, DataType, Field, FieldId, InflightCounter, MemoryOwner, ResourceBudget, Row,
        RowBatchBuilder, Scalar, Schema, SchemaId,
    };

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
        let secrets = MapSecretResolver::new(
            [("tok".into(), "secret".into())].into_iter().collect(),
        );
        let mut http = HttpSinkConfig::demo("http://127.0.0.1:1/ingest");
        http.header_secret = Some("tok".into());
        let err = http
            .validate(&secrets, &TargetPolicy::allow("127.0.0.1", 1))
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::PolicyDenied);
        assert!(
            err.to_string().contains("https"),
            "{}",
            err
        );

        let mut https = HttpSinkConfig::demo("https://127.0.0.1:443/ingest");
        https.header_secret = Some("tok".into());
        https
            .validate(&secrets, &TargetPolicy::allow("127.0.0.1", 443))
            .expect("https + header_secret must be accepted");
    }
}

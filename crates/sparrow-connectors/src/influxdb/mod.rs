//! InfluxDB v2 Sink: rows → line protocol → `POST /api/v2/write`.
//!
//! Live-only (`live_best_effort`, `restart_fresh`, no replay). A batch
//! receipt is acked only when every row was encoded and every request that
//! carried one of its rows got HTTP 204. Anything else fails the receipt:
//!
//! * `400` (malformed line protocol: InfluxDB writes nothing of the request)
//!   and `413` (request too large) — terminal for that request, counted
//!   `rejected_requests` / `rejected_rows`, not retried.
//! * `422` — InfluxDB's *partial write*: points it could accept (for example
//!   no field-type conflict) ARE stored, the rest are dropped. Not retried,
//!   counted `partial_writes`.
//! * `401` / `403` / `404` (bad token, no permission, unknown org/bucket) —
//!   fatal: the Sink stops and fails the job (`fatal`).
//! * `429` / `503` and transport errors — retried up to `max_retries` with a
//!   jittered exponential backoff capped at `retry_max`; a delta-seconds
//!   `Retry-After` replaces the backoff (capped at `retry_max`, counted
//!   `retry_after_capped`). Without a time column InfluxDB stamps points with
//!   its receive time, so a resend could duplicate points: then only
//!   connection-establishment errors (request never sent) are retried.
//! * other statuses — terminal for the request.
//!
//! Requests are sent one at a time (InfluxDB recommends serial writes for
//! ordered points). Each request is built in a buffer charged to the job's
//! reservation before every capacity growth and bounded by `batch_bytes`;
//! the charge lives until the request, including retries, is finished.
//! Optional gzip charges the compressor scratch and the output bound
//! before compressing.
//!
//! Stop: the first cancellation starts one `flush_timeout` deadline that
//! covers the request in flight (and its retries/backoff), already queued
//! batches and the final buffer. Rows not acknowledged by then are counted
//! `discarded_on_close`; an aborted request may still have been applied by
//! the server.

pub mod line;

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{DeliveryGuard, EncodedGuard, HealthState, Latency};
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, RestoreClaim, Result,
    RowBatch, Schema, SparrowError,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;
use crate::tls::{https_client, MAX_CA_PEM_BYTES};
pub use line::{CompiledMapping, InfluxMapping, LineError, Measurement, Precision};

const MAX_OUTBOX: usize = 4096;
/// Response bytes read (and discarded) per request.
const RESPONSE_LIMIT: usize = 64 * 1024;
/// Per-request bookkeeping beyond the body (URL, headers, response).
const REQUEST_OVERHEAD: usize = 8 * 1024 + RESPONSE_LIMIT;
/// Heap scratch of one fast-level deflate compressor in miniz_oxide 0.9.1
/// (`CompressorOxide`: dictionary 33 KiB, hash chains 128 KiB, LZ code
/// buffer 64 KiB, output buffer 83 KiB, Huffman tables ~5 KiB), rounded up.
pub const GZIP_SCRATCH: usize = 384 * 1024;
const GZIP_HEADER: [u8; 10] = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
const ACK_SLOT: usize = std::mem::size_of::<Arc<BatchAck>>();
// The Arc pointer alone does not account for the retained receipt object.
const ACK_STATE: usize = std::mem::size_of::<BatchAck>() + 2 * std::mem::size_of::<usize>();
const MAX_TOKEN_BYTES: usize = 4096;

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

/// Upper bound of the gzip member for `n` input bytes (deflate stored
/// blocks add 5 bytes per 64 KiB; rounded up generously) plus header and
/// trailer. `None` on overflow.
pub fn gzip_bound(n: usize) -> Option<usize> {
    n.checked_add(n / 1024)?.checked_add(1024 + 18)
}

#[derive(Clone, Debug)]
pub struct InfluxDbSinkConfig {
    /// `https://host[:port][/prefix]`; the Sink appends `/api/v2/write`.
    pub url: String,
    pub org: String,
    pub bucket: String,
    /// Secret reference resolving to the API token.
    pub token_secret: String,
    /// PEM bundle replacing the built-in roots (verification stays on).
    pub ca_pem: Option<String>,
    pub mapping: InfluxMapping,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub flush_interval: Duration,
    pub gzip: bool,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub flush_timeout: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
}

impl InfluxDbSinkConfig {
    pub fn new(
        url: impl Into<String>,
        org: impl Into<String>,
        bucket: impl Into<String>,
        token_secret: impl Into<String>,
        mapping: InfluxMapping,
    ) -> Self {
        Self {
            url: url.into(),
            org: org.into(),
            bucket: bucket.into(),
            token_secret: token_secret.into(),
            ca_pem: None,
            mapping,
            batch_rows: 5000,
            batch_bytes: 256 * 1024,
            flush_interval: Duration::from_secs(1),
            gzip: false,
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            max_retries: 5,
            retry_initial: Duration::from_millis(200),
            retry_max: Duration::from_secs(30),
            flush_timeout: Duration::from_secs(5),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::INFLUXDB_SINK
    }

    /// `<url>/api/v2/write?org=..&bucket=..&precision=..`.
    pub fn write_url(&self) -> Result<url::Url> {
        let mut url = url::Url::parse(&self.url).map_err(|_| {
            err(
                ErrorCode::InvalidArgument,
                "InfluxDB url is not a valid URL",
            )
        })?;
        if url.scheme() != "https" {
            return Err(err(
                ErrorCode::PolicyDenied,
                "InfluxDB url must be https:// (the API token is a credential)",
            ));
        }
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "InfluxDB url needs a host and no userinfo, query or fragment",
            ));
        }
        let path = format!("{}/api/v2/write", url.path().trim_end_matches('/'));
        url.set_path(&path);
        url.query_pairs_mut()
            .append_pair("org", &self.org)
            .append_pair("bucket", &self.bucket)
            .append_pair("precision", self.mapping.precision.as_str());
        if url.as_str().len() > 4096 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "InfluxDB write URL exceeds 4096 bytes",
            ));
        }
        Ok(url)
    }

    /// Largest reservation one request may hold: body, ack slots, gzip
    /// output + scratch and the request/response overhead. `None` on
    /// overflow.
    pub fn peak_bytes(&self) -> Option<usize> {
        let mut peak = self
            .batch_bytes
            // At most one distinct receipt per successful row, plus the
            // current batch while flushing a previous full request.
            .checked_add(self.batch_rows.checked_mul(ACK_SLOT + ACK_STATE)?)?
            .checked_add(ACK_STATE)?
            .checked_add(REQUEST_OVERHEAD)?;
        if self.gzip {
            peak = peak
                .checked_add(gzip_bound(self.batch_bytes)?)?
                .checked_add(GZIP_SCRATCH)?;
        }
        Some(peak)
    }

    /// The Sink may use at most half of the job reservation, leaving the
    /// rest to the operators that produce its input.
    pub fn check_reservation_budget(&self, reservation_bytes: usize) -> Result<()> {
        match self.peak_bytes() {
            Some(peak) if peak <= reservation_bytes / 2 => Ok(()),
            peak => Err(err(
                ErrorCode::BoundExceeded,
                format!(
                    "InfluxDB sink needs up to {} bytes per request (batch_bytes, batch_rows ack slots{}, overhead) but may use half of the {reservation_bytes}-byte job reservation; lower batch_bytes/batch_rows{}",
                    peak.map_or_else(|| "usize::MAX+".to_string(), |p| p.to_string()),
                    if self.gzip { ", gzip output and scratch" } else { "" },
                    if self.gzip { " or disable gzip" } else { "" },
                ),
            )),
        }
    }

    /// Target policy for the write URL and the token secret (never
    /// returned). Returns the `Authorization` header value.
    pub fn validate_target(
        &self,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
    ) -> Result<reqwest::header::HeaderValue> {
        let write_url = self.write_url()?;
        policy
            .check_http_url(write_url.as_str())
            .map_err(SparrowError::from)?;
        let token = secrets
            .resolve(&self.token_secret)
            .map_err(SparrowError::from)?;
        if token.is_empty() {
            return Err(err(
                ErrorCode::SecretMissing,
                "InfluxDB token secret is empty",
            ));
        }
        if token.len() > MAX_TOKEN_BYTES {
            return Err(err(
                ErrorCode::BoundExceeded,
                "InfluxDB token exceeds 4096 bytes",
            ));
        }
        let mut authorization = reqwest::header::HeaderValue::from_str(&format!("Token {token}"))
            .map_err(|_| {
            err(
                ErrorCode::InvalidArgument,
                "InfluxDB token contains characters not allowed in an HTTP header",
            )
        })?;
        authorization.set_sensitive(true);
        Ok(authorization)
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore).map_err(SparrowError::from)?;
        self.write_url()?;
        self.mapping.check()?;
        for (what, v) in [("org", &self.org), ("bucket", &self.bucket)] {
            if v.is_empty() || v.len() > 256 || v.chars().any(char::is_control) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("InfluxDB {what} must be 1..=256 bytes without control characters"),
                ));
            }
        }
        if self.token_secret.is_empty() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "InfluxDB token_secret is required",
            ));
        }
        if let Some(pem) = &self.ca_pem {
            if pem.len() > MAX_CA_PEM_BYTES {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    "InfluxDB ca_pem exceeds 65536 bytes",
                ));
            }
            if !reqwest::Certificate::from_pem_bundle(pem.as_bytes()).is_ok_and(|c| !c.is_empty()) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "InfluxDB ca_pem is not a PEM certificate bundle",
                ));
            }
        }
        let ms = |d: Duration| d.as_millis();
        if !(1..=100_000).contains(&self.batch_rows)
            || !(1024..=4 * 1024 * 1024).contains(&self.batch_bytes)
            || !(10..=60_000).contains(&ms(self.flush_interval))
            || !(100..=300_000).contains(&ms(self.timeout))
            || !(100..=60_000).contains(&ms(self.connect_timeout))
            || self.max_retries > 20
            || !(10..=60_000).contains(&ms(self.retry_initial))
            || !(100..=300_000).contains(&ms(self.retry_max))
            || self.retry_max < self.retry_initial
            || !(10..=60_000).contains(&ms(self.flush_timeout))
            || !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "InfluxDB sink bounds: batch_rows 1..=100000, batch_bytes 1024..=4194304, flush_interval_ms 10..=60000, timeout_ms 100..=300000, connect_timeout_ms 100..=60000, max_retries 0..=20, retry_initial_ms 10..=60000, retry_max_ms 100..=300000 (>= retry_initial_ms), flush_timeout_ms 10..=60000, outbox_capacity 1..=4096",
            ));
        }
        Ok(())
    }
}

/// One upstream batch. Acked when its last holder drops, only if every row
/// was encoded (`encoded`) and no request carrying its rows failed.
struct BatchAck {
    outbox: Option<Arc<InflightCounter>>,
    guard: Option<DeliveryGuard>,
    encoded: AtomicBool,
    failed: AtomicBool,
    _lease: MemoryLease,
}

impl Drop for BatchAck {
    fn drop(&mut self) {
        let ok = self.encoded.load(Ordering::Relaxed) && !self.failed.load(Ordering::Relaxed);
        if ok {
            if let Some(guard) = &mut self.guard {
                guard.complete();
            }
        }
        if let Some(outbox) = self.outbox.take() {
            if ok {
                outbox.ack();
            } else {
                outbox.fail();
            }
        }
    }
}

/// One write request under construction or in flight.
struct Request {
    body: Vec<u8>,
    lease: MemoryLease,
    credit: EncodedGuard,
    rows: usize,
    acks: Vec<Arc<BatchAck>>,
    first: Instant,
    ok: bool,
}

impl Request {
    fn charge(&self) -> usize {
        self.body
            .capacity()
            .saturating_add(self.acks.capacity().saturating_mul(ACK_SLOT))
            .saturating_add(REQUEST_OVERHEAD)
    }

    /// Hold `ack` for this request's rows; ack slots are charged before
    /// they are allocated.
    fn hold(
        &mut self,
        ack: &Arc<BatchAck>,
        max_slots: usize,
    ) -> std::result::Result<(), LineError> {
        if self.acks.last().is_some_and(|a| Arc::ptr_eq(a, ack)) {
            return Ok(());
        }
        if self.acks.len() == self.acks.capacity() {
            if self.acks.len() >= max_slots {
                return Err(LineError::Budget);
            }
            let slots = self.acks.capacity().saturating_mul(2).max(4).min(max_slots);
            let need = self
                .body
                .capacity()
                .saturating_add(slots.saturating_mul(ACK_SLOT))
                .saturating_add(REQUEST_OVERHEAD);
            self.lease.grow_to(need).map_err(|_| LineError::Budget)?;
            self.credit.resize(self.lease.bytes());
            self.acks
                .try_reserve_exact(slots - self.acks.len())
                .map_err(|_| LineError::Budget)?;
            if self.acks.capacity() > slots {
                return Err(LineError::Budget);
            }
        }
        self.acks.push(ack.clone());
        Ok(())
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        if !self.ok {
            for ack in &self.acks {
                ack.failed.store(true, Ordering::Relaxed);
            }
        }
    }
}

/// Outcome of one request.
#[derive(Debug, PartialEq, Eq)]
enum Sent {
    Ok,
    Failed,
    /// Bad credentials / permissions / unknown bucket: stop the Sink.
    Fatal,
    /// The stop deadline passed first.
    Deadline,
}

pub struct InfluxDbSink {
    pub config: InfluxDbSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    client: reqwest::Client,
    write_url: url::Url,
    authorization: reqwest::header::HeaderValue,
    owner: Arc<MemoryOwner>,
    jitter: RandomState,
    jitter_seq: AtomicU64,
}

impl std::fmt::Debug for InfluxDbSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No URL (may carry org/bucket) or token.
        f.debug_struct("InfluxDbSink").finish_non_exhaustive()
    }
}

/// Exponential backoff for retry `attempt` (1-based): `initial * 2^(attempt-1)`
/// capped at `max`, then jittered uniformly into `[d/2, d]` by `random`.
pub fn backoff(initial: Duration, max: Duration, attempt: u32, random: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(31);
    let nominal = initial.saturating_mul(1u32 << shift).min(max);
    let nanos = u64::try_from(nominal.as_nanos()).unwrap_or(u64::MAX);
    let half = nanos / 2;
    let span = nanos - half;
    Duration::from_nanos(half + if span == 0 { 0 } else { random % (span + 1) })
}

/// Delta-seconds `Retry-After`; HTTP-dates are not honoured (→ `None`).
pub fn retry_after(value: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    let text = value?.to_str().ok()?.trim();
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Saturate absurd values; the caller caps at retry_max anyway.
    Some(Duration::from_secs(text.parse::<u64>().unwrap_or(u64::MAX)))
}

impl InfluxDbSink {
    pub fn bind(
        config: InfluxDbSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let write_url = config.write_url()?;
        let authorization = config.validate_target(secrets, policy)?;
        let client = https_client(
            config.timeout,
            config.connect_timeout,
            config.ca_pem.as_deref(),
        )
        .map_err(SparrowError::from)?;
        Ok(Self {
            config,
            diag,
            client,
            write_url,
            authorization,
            owner,
            jitter: RandomState::new(),
            jitter_seq: AtomicU64::new(0),
        })
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx = rx.into();
        let _lifecycle = self.diag.observation.lifecycle(false);
        self.diag.observation.health(
            false,
            HealthState::Ready,
            "influxdb_ready_not_connected",
            None,
        );
        let mut state = State {
            pending: None,
            compiled: None,
            deadline: None,
            fatal: false,
        };
        loop {
            if state.fatal {
                break;
            }
            let flush_at = state
                .pending
                .as_ref()
                .map(|p| p.first + self.config.flush_interval);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    // A request in flight may already have started the
                    // deadline; never extend it.
                    state.deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
                    break;
                }
                _ = async { match flush_at { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                    self.flush(&mut state, &cancel).await;
                }
                _ = async { match &outbox { Some(o) => o.flush_requested().await, None => std::future::pending().await } } => {
                    // A barrier/EOF hint: send what is queued now without
                    // waiting for the interval.
                    for _ in 0..self.config.outbox_capacity {
                        let Ok(batch) = rx.try_recv() else { break };
                        self.write_batch(batch, &mut state, &cancel, outbox.as_ref()).await;
                        if state.fatal { break; }
                    }
                    self.flush(&mut state, &cancel).await;
                }
                batch = rx.recv() => match batch {
                    None => break,
                    Some(batch) => self.write_batch(batch, &mut state, &cancel, outbox.as_ref()).await,
                },
            }
        }
        rx.close();
        if !state.fatal {
            // EOF (no deadline unless a stop arrives) or stop (shared deadline).
            loop {
                if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
                let Ok(batch) = rx.try_recv() else { break };
                self.write_batch(batch, &mut state, &cancel, outbox.as_ref())
                    .await;
            }
            if !state.fatal {
                self.flush(&mut state, &cancel).await;
            }
        }
        if let Some(request) = state.pending.take() {
            self.diag
                .influxdb_sink_discarded_on_close
                .fetch_add(request.rows as u64, Ordering::Relaxed);
        }
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .influxdb_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = &outbox {
                outbox.fail();
            }
        }
        if !state.fatal {
            self.diag.observation.health(
                false,
                HealthState::Stopped,
                "influxdb_sink_stopped",
                None,
            );
        }
    }

    fn new_request(&self) -> Option<Request> {
        let lease = self
            .owner
            .acquire(CreditKind::Reservation, REQUEST_OVERHEAD)
            .ok()?;
        let credit = self.diag.observation.encoded_credit(lease.bytes());
        Some(Request {
            body: Vec::new(),
            lease,
            credit,
            rows: 0,
            acks: Vec::new(),
            first: Instant::now(),
            ok: false,
        })
    }

    async fn write_batch(
        &self,
        batch: RowBatch,
        state: &mut State,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) {
        let guard = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        self.observe_stop(state, cancel);
        if state.fatal || state.deadline.is_some_and(|at| Instant::now() >= at) {
            self.diag
                .influxdb_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
            return;
        }
        let Ok(lease) = self.owner.acquire(CreditKind::Reservation, ACK_STATE) else {
            self.diag
                .influxdb_sink_dropped_budget
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
            return;
        };
        let ack = Arc::new(BatchAck {
            outbox: outbox.cloned(),
            guard: Some(guard),
            encoded: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            _lease: lease,
        });
        if batch.output_sequence().is_some() {
            // Reliable output identity cannot be honoured (live-only).
            self.diag
                .influxdb_sink_dropped_bad
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            self.fatal(
                state,
                cancel,
                "influxdb_reliable_output_rejected",
                ErrorCode::UnsupportedRestore,
            );
            return;
        }
        let schema = batch.schema_arc();
        if state.compiled.as_ref().is_none_or(|(s, _)| **s != *schema) {
            match self.config.mapping.compile(&schema) {
                Ok(compiled) => state.compiled = Some((schema.clone(), compiled)),
                Err(e) => {
                    self.fatal(state, cancel, "influxdb_schema_mismatch", e.code);
                    return;
                }
            }
        }
        let limit = self.config.batch_bytes;
        for (i, row) in batch.rows().iter().enumerate() {
            if i % 16 == 15 {
                tokio::task::yield_now().await;
            }
            self.observe_stop(state, cancel);
            if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                self.diag
                    .influxdb_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                ack.failed.store(true, Ordering::Relaxed);
                return;
            }
            let mut retried = false;
            loop {
                if state.pending.is_none() {
                    state.pending = self.new_request();
                }
                let Some(request) = state.pending.as_mut() else {
                    // No credit for even an empty request.
                    self.diag
                        .influxdb_sink_dropped_budget
                        .fetch_add(1, Ordering::Relaxed);
                    ack.failed.store(true, Ordering::Relaxed);
                    break;
                };
                let compiled = &state.compiled.as_ref().expect("compiled above").1;
                let started = std::time::Instant::now();
                let mark = request.body.len();
                let outcome = {
                    let Request {
                        body,
                        lease,
                        credit,
                        acks,
                        ..
                    } = request;
                    let acks_bytes = acks.capacity().saturating_mul(ACK_SLOT);
                    compiled.encode(
                        row,
                        &mut line::BoundedOut {
                            bytes: body,
                            limit,
                            admit: |capacity: usize| {
                                let ok = lease
                                    .grow_to(
                                        capacity
                                            .saturating_add(acks_bytes)
                                            .saturating_add(REQUEST_OVERHEAD),
                                    )
                                    .is_ok();
                                credit.resize(lease.bytes());
                                ok
                            },
                        },
                    )
                }
                .and_then(|()| request.hold(&ack, self.config.batch_rows));
                self.diag
                    .observation
                    .record(Latency::Encode, started.elapsed());
                match outcome {
                    Ok(()) => {
                        request.rows += 1;
                        if request.rows >= self.config.batch_rows {
                            self.flush(state, cancel).await;
                        }
                        break;
                    }
                    Err(LineError::Bad(_)) => {
                        request.body.truncate(mark);
                        self.diag
                            .influxdb_sink_dropped_bad
                            .fetch_add(1, Ordering::Relaxed);
                        ack.failed.store(true, Ordering::Relaxed);
                        break;
                    }
                    Err(e @ (LineError::Oversize | LineError::Budget)) => {
                        request.body.truncate(mark);
                        if request.rows > 0 && !retried {
                            // Send what we have (freeing its charge) and try
                            // this row again in a fresh request.
                            retried = true;
                            self.flush(state, cancel).await;
                            if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                                self.diag
                                    .influxdb_sink_discarded_on_close
                                    .fetch_add(1, Ordering::Relaxed);
                                ack.failed.store(true, Ordering::Relaxed);
                                break;
                            }
                            continue;
                        }
                        let counter = if e == LineError::Oversize {
                            &self.diag.influxdb_sink_dropped_oversize
                        } else {
                            &self.diag.influxdb_sink_dropped_budget
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                        ack.failed.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
        ack.encoded.store(true, Ordering::Relaxed);
    }

    /// Send the pending request, if it holds rows.
    async fn flush(&self, state: &mut State, cancel: &CancellationToken) {
        let Some(mut request) = state.pending.take() else {
            return;
        };
        if request.rows == 0 {
            // Holds acks only of rows that all failed to encode.
            return;
        }
        let rows = request.rows as u64;
        let outcome = self.send(&mut request, &mut state.deadline, cancel).await;
        match outcome {
            Sent::Ok => {
                request.ok = true;
                self.diag
                    .influxdb_sink_rows_written
                    .fetch_add(rows, Ordering::Relaxed);
            }
            Sent::Failed => {}
            Sent::Deadline => {
                self.diag
                    .influxdb_sink_discarded_on_close
                    .fetch_add(rows, Ordering::Relaxed);
            }
            Sent::Fatal => {
                self.diag
                    .influxdb_sink_fatal
                    .fetch_add(1, Ordering::Relaxed);
                state.fatal = true;
                cancel.cancel();
            }
        }
    }

    fn observe_stop(&self, state: &mut State, cancel: &CancellationToken) {
        if cancel.is_cancelled() {
            state
                .deadline
                .get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
        }
    }

    fn fatal(
        &self,
        state: &mut State,
        cancel: &CancellationToken,
        reason: &'static str,
        code: ErrorCode,
    ) {
        self.diag
            .influxdb_sink_fatal
            .fetch_add(1, Ordering::Relaxed);
        self.diag
            .observation
            .health(false, HealthState::Failed, reason, Some(code));
        state.fatal = true;
        cancel.cancel();
    }

    /// Gzip `request.body` in place when configured. Falls back to the
    /// plain body (counted) if the scratch cannot be charged.
    fn compress(&self, request: &mut Request) -> bool {
        if !self.config.gzip {
            return false;
        }
        let Some(bound) = gzip_bound(request.body.len()) else {
            return false;
        };
        let need = request
            .charge()
            .saturating_add(bound)
            .saturating_add(GZIP_SCRATCH);
        if request.lease.grow_to(need).is_err() {
            self.diag
                .influxdb_sink_gzip_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        request.credit.resize(request.lease.bytes());
        let mut out = Vec::new();
        if out.try_reserve_exact(bound).is_err() {
            self.diag
                .influxdb_sink_gzip_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        out.extend_from_slice(&GZIP_HEADER);
        let mut deflate = flate2::Compress::new(flate2::Compression::fast(), false);
        // compress_vec writes only into spare capacity; it never grows `out`.
        let done = matches!(
            deflate.compress_vec(&request.body, &mut out, flate2::FlushCompress::Finish),
            Ok(flate2::Status::StreamEnd)
        ) && deflate.total_in() == request.body.len() as u64
            && out.capacity() - out.len() >= 8;
        drop(deflate);
        if !done {
            self.diag
                .influxdb_sink_gzip_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let mut crc = flate2::Crc::new();
        crc.update(&request.body);
        out.extend_from_slice(&crc.sum().to_le_bytes());
        out.extend_from_slice(&(request.body.len() as u32).to_le_bytes());
        request.body = out;
        let _ = request.lease.shrink_to(request.charge());
        request.credit.resize(request.lease.bytes());
        true
    }

    fn random(&self) -> u64 {
        self.jitter
            .hash_one(self.jitter_seq.fetch_add(1, Ordering::Relaxed))
    }

    async fn send(
        &self,
        request: &mut Request,
        deadline: &mut Option<Instant>,
        cancel: &CancellationToken,
    ) -> Sent {
        if cancel.is_cancelled() {
            deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
        }
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Sent::Deadline;
        }
        let gzip = self.compress(request);
        let rows = request.rows as u64;
        let bytes = request.body.len() as u64;
        // The body is moved into a shared handle once; retries clone the
        // handle. The lease (in `request`) outlives every attempt.
        let mut template = self
            .client
            .post(self.write_url.clone())
            .header(reqwest::header::AUTHORIZATION, self.authorization.clone())
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(reqwest::header::ACCEPT, "application/json")
            .body(std::mem::take(&mut request.body));
        if gzip {
            template = template.header(reqwest::header::CONTENT_ENCODING, "gzip");
        }
        let idempotent = self.config.mapping.time_column.is_some();
        let mut attempt = 0u32;
        loop {
            let Some(req) = template.try_clone() else {
                return Sent::Failed;
            };
            let mut observed = self.diag.observation.request();
            let started = std::time::Instant::now();
            let Some(response) = self.bounded(req.send(), deadline, cancel).await else {
                return Sent::Deadline;
            };
            self.diag
                .observation
                .record(Latency::HttpHeaders, started.elapsed());
            let wait = match response {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let retry_after =
                        retry_after(response.headers().get(reqwest::header::RETRY_AFTER));
                    // Drain a bounded response so the connection can be reused.
                    let mut response = response;
                    let mut read = 0usize;
                    let drained = self
                        .bounded(
                            async {
                                while let Ok(Some(chunk)) = response.chunk().await {
                                    read = read.saturating_add(chunk.len());
                                    if read > RESPONSE_LIMIT {
                                        break;
                                    }
                                }
                            },
                            deadline,
                            cancel,
                        )
                        .await;
                    observed.finish();
                    match status {
                        200..=299 => {
                            self.diag
                                .influxdb_sink_requests_ok
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag
                                .influxdb_sink_bytes_sent
                                .fetch_add(bytes, Ordering::Relaxed);
                            self.diag.observation.progress(false, request.rows);
                            self.diag.observation.health(
                                false,
                                HealthState::Ready,
                                "influxdb_write_ok",
                                None,
                            );
                            // A 2xx is final even when the stop deadline cut
                            // the (empty) response body short.
                            let _ = drained;
                            return Sent::Ok;
                        }
                        401 | 403 | 404 => {
                            let reason = match status {
                                401 => "influxdb_unauthorized",
                                403 => "influxdb_forbidden",
                                _ => "influxdb_org_or_bucket_not_found",
                            };
                            self.diag.observation.health(
                                false,
                                HealthState::Failed,
                                reason,
                                Some(ErrorCode::PolicyDenied),
                            );
                            return Sent::Fatal;
                        }
                        422 => {
                            self.diag
                                .influxdb_sink_partial_writes
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag.observation.health(
                                false,
                                HealthState::Failed,
                                "influxdb_partial_write",
                                Some(ErrorCode::InvalidArgument),
                            );
                            return Sent::Failed;
                        }
                        429 | 503 if !idempotent => {
                            self.diag
                                .influxdb_sink_rejected_requests
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag
                                .influxdb_sink_rejected_rows
                                .fetch_add(rows, Ordering::Relaxed);
                            self.diag.observation.health(
                                false,
                                HealthState::Failed,
                                "influxdb_response_not_retried_without_time",
                                Some(ErrorCode::Internal),
                            );
                            return Sent::Failed;
                        }
                        429 | 503 => match retry_after {
                            Some(after) => {
                                self.diag
                                    .influxdb_sink_retry_after_waits
                                    .fetch_add(1, Ordering::Relaxed);
                                if after > self.config.retry_max {
                                    self.diag
                                        .influxdb_sink_retry_after_capped
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                Some(after.min(self.config.retry_max))
                            }
                            None => None,
                        },
                        _ => {
                            self.diag
                                .influxdb_sink_rejected_requests
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag
                                .influxdb_sink_rejected_rows
                                .fetch_add(rows, Ordering::Relaxed);
                            let reason = match status {
                                400 => "influxdb_bad_request",
                                413 => "influxdb_request_too_large",
                                500..=599 => "influxdb_server_error",
                                _ => "influxdb_unexpected_status",
                            };
                            self.diag.observation.health(
                                false,
                                HealthState::Failed,
                                reason,
                                Some(ErrorCode::InvalidArgument),
                            );
                            return Sent::Failed;
                        }
                    }
                }
                Err(e) => {
                    observed.finish();
                    // Without a time column a resend after the request may
                    // have reached the server could duplicate points.
                    if !(idempotent || e.is_connect()) {
                        self.diag.observation.health(
                            false,
                            HealthState::Failed,
                            "influxdb_transport_error_not_retried",
                            Some(ErrorCode::Internal),
                        );
                        return Sent::Failed;
                    }
                    None
                }
            };
            attempt += 1;
            if attempt > self.config.max_retries {
                self.diag.observation.health(
                    false,
                    HealthState::Failed,
                    "influxdb_retries_exhausted",
                    Some(ErrorCode::Internal),
                );
                return Sent::Failed;
            }
            self.diag
                .influxdb_sink_retries
                .fetch_add(1, Ordering::Relaxed);
            self.diag.observation.health(
                false,
                HealthState::Reconnecting,
                "influxdb_retry_backoff",
                None,
            );
            let delay = wait.unwrap_or_else(|| {
                backoff(
                    self.config.retry_initial,
                    self.config.retry_max,
                    attempt,
                    self.random(),
                )
            });
            if self
                .bounded(tokio::time::sleep(delay), deadline, cancel)
                .await
                .is_none()
            {
                return Sent::Deadline;
            }
        }
    }

    /// Run `fut`; the first cancellation starts the shared stop deadline,
    /// after which `fut` is abandoned at the deadline (`None`).
    async fn bounded<F: std::future::Future>(
        &self,
        fut: F,
        deadline: &mut Option<Instant>,
        cancel: &CancellationToken,
    ) -> Option<F::Output> {
        tokio::pin!(fut);
        loop {
            if cancel.is_cancelled() {
                deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
            }
            match *deadline {
                Some(at) => {
                    if Instant::now() >= at {
                        return None;
                    }
                    return tokio::time::timeout_at(at, fut.as_mut()).await.ok();
                }
                None => tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        *deadline = Some(Instant::now() + self.config.flush_timeout);
                    }
                    out = fut.as_mut() => return Some(out),
                },
            }
        }
    }
}

struct State {
    pending: Option<Request>,
    compiled: Option<(Arc<Schema>, CompiledMapping)>,
    deadline: Option<Instant>,
    fatal: bool,
}

#[cfg(test)]
mod tests;

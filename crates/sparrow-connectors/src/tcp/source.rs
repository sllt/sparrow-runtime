//! TCP client Source: live, at-most-once, no ack, no replay.
//!
//! - `lines`: one record per `\n`-terminated line (`\r\n` accepted, blank
//!   lines skipped). JSON objects, or CSV records where each connection is
//!   one CSV document: with `header: true` the first line after connecting
//!   is the header. `multiline` CSV is refused (a record is one line).
//! - `length_prefixed`: one record per frame (JSON object, or CSV header +
//!   record like an MQTT message).
//! - Limits are checked by the framer before decoding: oversize records are
//!   counted `dropped_oversize` and skipped (`resync`) or close the
//!   connection (`disconnect`). A record cut off by EOF is counted
//!   `dropped_partial`, never decoded.
//! - Rows enter the byte-accounted Kernel ingress one at a time; while the
//!   inbox is full the Source stops reading (TCP backpressure on the peer).
//!   No bytes within `idle_timeout` while waiting to read closes the
//!   connection; time blocked on our own ingress does not count.
//! - Reconnect uses capped, jittered backoff; `reconnect_attempts`
//!   consecutive failures fail the Source retryably.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{CsvMapping, JsonLimits, PayloadFormat};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryOwner, QueuedRow, RestoreClaim, Result, Row, Schema,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::client::{error, BoundTcp, TcpClientConfig};
use super::framing::{Frame, OversizePolicy, TcpFraming};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::net::{reconnect_delay, NetStream};
use crate::TargetPolicy;

pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
const MAX_INBOX: usize = 4096;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct TcpSourceConfig {
    pub client: TcpClientConfig,
    pub oversize: OversizePolicy,
    /// No bytes received for this long while waiting to read = dead peer.
    pub idle_timeout: Duration,
    pub inbox_capacity: usize,
    /// Decoded-row Queue credit for the inbox.
    pub inbox_bytes: usize,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    pub fail_on_decode: bool,
    pub restore: RestoreClaim,
    pub payload_format: PayloadFormat,
}

impl TcpSourceConfig {
    pub fn new(host: impl Into<String>, port: u16, schema: Schema) -> Self {
        Self {
            client: TcpClientConfig::new(host, port),
            oversize: OversizePolicy::Resync,
            idle_timeout: Duration::from_secs(60),
            inbox_capacity: 16,
            inbox_bytes: DEFAULT_INBOX_BYTES,
            schema,
            json_limits: JsonLimits::default(),
            fail_on_decode: false,
            restore: RestoreClaim::None,
            payload_format: PayloadFormat::Json,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::TCP_SOURCE
    }

    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        if let Some(csv) = self.payload_format.as_csv() {
            if self.client.framing == TcpFraming::Lines && csv.options().multiline {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "TCP lines framing takes one CSV record per line; csv.multiline is refused (use length_prefixed)",
                ));
            }
            csv.check_schema(&self.schema)?;
        }
        if !(1..=MAX_INBOX).contains(&self.inbox_capacity)
            || self.json_limits.max_bytes > 64 * 1024
            || !(100..=86_400_000).contains(&self.idle_timeout.as_millis())
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP source bounds: inbox_capacity 1..=4096, idle_timeout_ms 100..=86400000; records remain <=64KiB",
            ));
        }
        self.check_inbox_budget(sparrow_model::ResourceBudget::compact().queue_bytes)?;
        self.check_reservation_budget(sparrow_model::ResourceBudget::compact().reservation_bytes)?;
        self.client.validate(policy)
    }

    /// Inbox payload credit plus channel metadata within the job Queue
    /// budget and the static 4 MiB ceiling.
    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        if self.inbox_bytes == 0
            || self
                .inbox_bytes
                .saturating_add(QueuedRow::channel_budget(self.inbox_capacity))
                > queue_budget.min(4 * 1024 * 1024)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
            ));
        }
        Ok(())
    }

    pub fn reservation(&self) -> usize {
        self.client.connection_reservation()
    }

    /// One connector may use at most half the job reservation budget.
    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP connection buffers exceed half the job reservation budget",
            ));
        }
        Ok(())
    }
}

pub struct TcpSource {
    pub config: TcpSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    client: BoundTcp,
}

impl std::fmt::Debug for TcpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSource")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

struct Ingress<'a> {
    tx: &'a ObservedSender<QueuedRow>,
    owner: &'a Arc<MemoryOwner>,
    queue: &'a Arc<MemoryOwner>,
    max_row_bytes: usize,
}

enum SessionEnd {
    Stop,
    Lost(&'static str),
}

/// Outcome of one record.
enum Ingested {
    Continue,
    Stop,
    /// The connection's CSV header is unusable: reconnect for a new one.
    BadHeader,
}

impl TcpSource {
    pub fn bind(
        config: TcpSourceConfig,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(policy)?;
        let client = config.client.bind(policy)?;
        Ok(Self {
            config,
            diag,
            client,
        })
    }

    /// Read until cancelled. Requires the byte-accounted ingress.
    pub async fn run_budgeted(
        self,
        tx: impl Into<ObservedSender<QueuedRow>>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        let tx = tx.into();
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(error(
                ErrorCode::InvalidArgument,
                "TCP inbox_capacity does not match channel",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes)?;
        let _connection = owner.acquire(CreditKind::Reservation, self.config.reservation())?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "tcp-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let _lifecycle = self.diag.observation.lifecycle(true);
        let mut connected_before = false;
        loop {
            let Some(stream) = self.connect(&cancel, connected_before).await? else {
                return Ok(());
            };
            if connected_before {
                self.diag
                    .tcp_source_reconnects
                    .fetch_add(1, Ordering::Relaxed);
            }
            connected_before = true;
            match self.session(stream, &ingress, &cancel).await? {
                SessionEnd::Stop => {
                    self.diag
                        .observation
                        .health(true, HealthState::Stopped, "tcp_stopped", None);
                    return Ok(());
                }
                SessionEnd::Lost(reason) => {
                    self.diag
                        .tcp_source_disconnects
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag
                        .observation
                        .health(true, HealthState::Reconnecting, reason, None);
                }
            }
        }
    }

    /// Connect with capped jittered backoff. `Ok(None)` = cancelled.
    async fn connect(
        &self,
        cancel: &CancellationToken,
        reconnecting: bool,
    ) -> Result<Option<NetStream>> {
        let mut attempt = 0usize;
        loop {
            if reconnecting || attempt > 0 {
                // Back off before every reconnect, including the first one
                // after a lost session (a flapping peer is rate limited).
                let delay = reconnect_delay(attempt + 1, self.client.config.reconnect_max);
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(None),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            self.diag.observation.health(
                true,
                if reconnecting || attempt > 0 {
                    HealthState::Reconnecting
                } else {
                    HealthState::Connecting
                },
                "tcp_connecting",
                None,
            );
            let attempted = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(None),
                c = self.client.connect() => c,
            };
            match attempted {
                Ok(stream) => {
                    self.diag
                        .tcp_source_connects
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag
                        .observation
                        .health(true, HealthState::Ready, "tcp_connected", None);
                    return Ok(Some(stream));
                }
                Err(failure) => {
                    self.diag
                        .tcp_source_connect_failures
                        .fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    if attempt >= self.client.config.reconnect_attempts {
                        self.diag.observation.health(
                            true,
                            HealthState::Failed,
                            "tcp_connect_exhausted",
                            Some(ErrorCode::JobFailed),
                        );
                        return Err(error(
                            ErrorCode::JobFailed,
                            format!(
                                "{} ({attempt} consecutive attempts); the supervisor restart policy applies",
                                failure.reason("TCP")
                            ),
                        )
                        .retryable(true));
                    }
                }
            }
        }
    }

    async fn session(
        &self,
        mut stream: NetStream,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
    ) -> Result<SessionEnd> {
        enum Event {
            Stop,
            Idle,
            Read(std::io::Result<usize>),
        }
        let mut reader = self.client.reader(self.config.oversize);
        // Lines + CSV: each connection is one document.
        let mut mapping: Option<CsvMapping> = match self.config.payload_format.as_csv() {
            Some(csv) if self.client.config.framing == TcpFraming::Lines && !csv.header() => {
                Some(csv.positional_mapping(&self.config.schema)?)
            }
            _ => None,
        };
        let mut last_read = Instant::now();
        loop {
            let mut drained = false;
            while let Some(frame) = reader.next_frame() {
                drained = true;
                let range = match frame {
                    Frame::Oversize => {
                        self.diag
                            .tcp_source_dropped_oversize
                            .fetch_add(1, Ordering::Relaxed);
                        if self.config.oversize == OversizePolicy::Disconnect {
                            return Ok(SessionEnd::Lost("tcp_frame_too_big"));
                        }
                        continue;
                    }
                    Frame::Record(range) => range,
                };
                self.diag
                    .tcp_source_received
                    .fetch_add(1, Ordering::Relaxed);
                let received_at = std::time::Instant::now();
                match self
                    .ingest(
                        reader.bytes(range),
                        &mut mapping,
                        ingress,
                        cancel,
                        received_at,
                    )
                    .await?
                {
                    Ingested::Continue => {}
                    Ingested::Stop => {
                        let _ = tokio::time::timeout(CLOSE_TIMEOUT, stream.shutdown()).await;
                        return Ok(SessionEnd::Stop);
                    }
                    Ingested::BadHeader => return Ok(SessionEnd::Lost("tcp_csv_header_invalid")),
                }
            }
            // Time spent blocked on our own bounded ingress is not idleness.
            if drained {
                last_read = Instant::now();
            }
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => Event::Stop,
                _ = tokio::time::sleep_until(last_read + self.config.idle_timeout) => Event::Idle,
                n = reader.fill(&mut stream) => Event::Read(n),
            };
            match event {
                Event::Stop => {
                    let _ = tokio::time::timeout(CLOSE_TIMEOUT, stream.shutdown()).await;
                    return Ok(SessionEnd::Stop);
                }
                Event::Idle => {
                    self.diag
                        .tcp_source_idle_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(SessionEnd::Lost("tcp_idle_timeout"));
                }
                Event::Read(Ok(0)) => {
                    if reader.has_partial() {
                        self.diag
                            .tcp_source_dropped_partial
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(SessionEnd::Lost("tcp_peer_closed"));
                }
                Event::Read(Ok(n)) => {
                    self.diag
                        .tcp_source_bytes_read
                        .fetch_add(n as u64, Ordering::Relaxed);
                    last_read = Instant::now();
                }
                Event::Read(Err(_)) => {
                    if reader.has_partial() {
                        self.diag
                            .tcp_source_dropped_partial
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(SessionEnd::Lost("tcp_read_failed"));
                }
            }
        }
    }

    async fn ingest(
        &self,
        record: &[u8],
        mapping: &mut Option<CsvMapping>,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        received_at: std::time::Instant,
    ) -> Result<Ingested> {
        if record.len() > self.config.json_limits.max_bytes {
            self.diag
                .tcp_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Ingested::Continue);
        }
        let started = std::time::Instant::now();
        let lines_csv = match self.config.payload_format.as_csv() {
            Some(csv) if self.client.config.framing == TcpFraming::Lines => Some(csv),
            _ => None,
        };
        let decoded = match (lines_csv, mapping.as_ref()) {
            (Some(csv), None) => {
                // First line of the connection: the CSV header.
                match csv.header_mapping(&self.config.schema, record) {
                    Ok(m) => {
                        *mapping = Some(m);
                        return Ok(Ingested::Continue);
                    }
                    Err(e) => {
                        self.bad(&e)?;
                        return Ok(Ingested::BadHeader);
                    }
                }
            }
            (Some(csv), Some(m)) => csv.decode_record(&self.config.schema, m, record, None),
            (None, _) => self.config.payload_format.decode_row(
                &self.config.schema,
                record,
                &self.config.json_limits,
                None,
            ),
        };
        self.diag
            .observation
            .record(Latency::Decode, started.elapsed());
        let row = match decoded {
            Ok(row) => row,
            Err(e) => {
                self.bad(&e)?;
                return Ok(Ingested::Continue);
            }
        };
        self.diag.observation.progress(true, 1);
        Ok(
            if self
                .admit(row, ingress, cancel, OriginSpan::at(received_at))
                .await?
            {
                Ingested::Continue
            } else {
                Ingested::Stop
            },
        )
    }

    /// Count a decode failure; `fail_on_decode` turns it into a job error.
    fn bad(&self, e: &sparrow_model::SparrowError) -> Result<()> {
        self.diag.csv_decode_error(&self.config.payload_format, e);
        self.diag
            .tcp_source_dropped_bad
            .fetch_add(1, Ordering::Relaxed);
        self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
        if self.config.fail_on_decode {
            self.diag.observation.health(
                true,
                HealthState::Failed,
                "tcp_decode_failed",
                Some(e.code),
            );
            return Err(error(e.code, "TCP record decode failed (fail_on_decode)"));
        }
        Ok(())
    }

    /// Cancel-aware wait for a channel slot and Queue credit, holding one
    /// Reservation-charged working row (same contract as NATS / WebSocket).
    async fn admit(
        &self,
        row: Row,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        origin: OriginSpan,
    ) -> Result<bool> {
        let _admission = self.diag.observation.timer(Latency::SourceAdmission);
        let bytes = QueuedRow::accounted_bytes(&row);
        if bytes
            > ingress
                .queue
                .budget()
                .queue_bytes
                .min(ingress.max_row_bytes)
        {
            self.diag
                .tcp_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        let Ok(_working) = ingress.owner.acquire(CreditKind::Reservation, bytes) else {
            self.diag
                .tcp_source_dropped_budget
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        };
        let mut row = Some(row);
        let mut observed_wait: Option<sparrow_io::observed::Wait<'_>> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let budget_full = match ingress.tx.try_reserve() {
                Ok(permit) => match QueuedRow::try_new(
                    row.take().expect("pending row"),
                    ingress.queue,
                    &self.diag.tcp_source_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        self.diag.tcp_source_rows.fetch_add(1, Ordering::Relaxed);
                        return Ok(true);
                    }
                    Err((r, _)) => {
                        row = Some(r);
                        true
                    }
                },
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => false,
            };
            if observed_wait.is_none() {
                observed_wait = Some(ingress.tx.observe_wait());
                self.diag
                    .tcp_source_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(false),
                _ = async {
                    if budget_full {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        let _ = ingress.tx.reserve().await;
                    }
                } => {}
            }
        }
    }
}

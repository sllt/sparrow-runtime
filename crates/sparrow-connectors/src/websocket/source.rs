//! WebSocket client Source: live, at-most-once, no ack, no replay.
//!
//! - Each text message is one record (JSON object or CSV record with its
//!   header) or, with `framing = ndjson`, newline-delimited JSON objects.
//!   Binary messages are dropped and counted unless `binary_frames = decode`
//!   (JSON only; CSV is text and refuses binary decoding).
//! - The protocol layer rejects a frame above `max_message_bytes` from its
//!   header, before reserving its payload, and a fragmented message once its
//!   running size would pass the bound; the connection is then closed
//!   (counted `dropped_oversize`) and reconnected. A record above the format's
//!   decode limit (64 KiB JSON / `max_record_bytes` CSV) is dropped by length
//!   and counted the same way, without decoding.
//! - Each record's format-specific decode working set is charged to the job
//!   reservation before decoding and held until the row is admitted;
//!   without credit the record is counted `dropped_budget` and not parsed.
//! - Rows enter the byte-accounted Kernel ingress one at a time; while the
//!   bounded inbox is full the Source stops reading the socket (TCP
//!   backpressure on the server). Time blocked on our own ingress does not
//!   count as connection idleness.
//! - A Ping is sent every `ping_interval`; no frame within `idle_timeout`
//!   while waiting to read closes the connection. Reconnect uses capped,
//!   jittered backoff; `reconnect_attempts` consecutive failures fail the
//!   Source retryably and the supervisor restart policy applies.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use sparrow_formats::{JsonLimits, PayloadFormat};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryOwner, QueuedRow, RestoreClaim, Result, Row, Schema,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_util::sync::CancellationToken;

use super::client::{error, reconnect_delay, BoundClient, WebSocketClientConfig, WsStream};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::{SecretResolver, TargetPolicy};

pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
const MAX_INBOX: usize = 4096;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// How one WebSocket message maps to records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WebSocketFraming {
    /// One record per message (JSON object, or CSV header + record).
    #[default]
    Message,
    /// Newline-delimited JSON objects; blank lines skipped (JSON only).
    Ndjson,
}

/// What a Source does with binary messages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BinaryFrames {
    /// Count (`dropped_binary`) and skip.
    #[default]
    Drop,
    /// Decode like a text message (JSON only; strict UTF-8 JSON parsing).
    Decode,
}

#[derive(Clone, Debug)]
pub struct WebSocketSourceConfig {
    pub client: WebSocketClientConfig,
    pub framing: WebSocketFraming,
    pub binary_frames: BinaryFrames,
    pub inbox_capacity: usize,
    /// Decoded-row Queue credit for the inbox.
    pub inbox_bytes: usize,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    pub fail_on_decode: bool,
    pub restore: RestoreClaim,
    pub payload_format: PayloadFormat,
}

impl WebSocketSourceConfig {
    pub fn new(url: impl Into<String>, schema: Schema) -> Self {
        Self {
            client: WebSocketClientConfig::new(url),
            framing: WebSocketFraming::Message,
            binary_frames: BinaryFrames::Drop,
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
        ConnectorCapabilities::WEBSOCKET_SOURCE
    }

    /// Static validation; credentials are resolved by `bind`.
    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        if let Some(csv) = self.payload_format.as_csv() {
            if self.framing != WebSocketFraming::Message || self.binary_frames != BinaryFrames::Drop
            {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "WebSocket CSV takes one record per text message: framing=ndjson and binary_frames=decode are JSON-only",
                ));
            }
            csv.check_schema(&self.schema)?;
        }
        if !(1..=MAX_INBOX).contains(&self.inbox_capacity) || self.json_limits.max_bytes > 64 * 1024
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket inbox_capacity must be 1..=4096; records remain <=64KiB",
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
                "WebSocket inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
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
                "WebSocket connection buffers (2 x max_message_bytes + 128 KiB) exceed half the job reservation budget; lower max_message_bytes",
            ));
        }
        Ok(())
    }
}

pub struct WebSocketSource {
    pub config: WebSocketSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    client: BoundClient,
}

impl std::fmt::Debug for WebSocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSource")
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

/// How a connected session ended.
enum SessionEnd {
    /// Cancelled or the inbox closed: stop.
    Stop,
    /// Connection lost (closed, protocol error, oversize, idle).
    Lost(&'static str),
}

impl WebSocketSource {
    pub fn bind(
        config: WebSocketSourceConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(policy)?;
        let client = config.client.bind(secrets, policy)?;
        Ok(Self {
            config,
            diag,
            client,
        })
    }

    /// Read until cancelled. Requires the byte-accounted ingress
    /// (`JobRequest::with_budgeted_live_io` / graph `budgeted`).
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
                "WebSocket inbox_capacity does not match channel",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes)?;
        let _connection = owner.acquire(CreditKind::Reservation, self.config.reservation())?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "websocket-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let _lifecycle = self.diag.observation.lifecycle(true);
        let mut connected_before = false;
        loop {
            let Some(ws) = self.connect(&cancel, connected_before).await? else {
                return Ok(());
            };
            if connected_before {
                self.diag
                    .websocket_source_reconnects
                    .fetch_add(1, Ordering::Relaxed);
            }
            connected_before = true;
            match self.session(ws, &ingress, &cancel).await? {
                SessionEnd::Stop => {
                    self.diag.observation.health(
                        true,
                        HealthState::Stopped,
                        "websocket_stopped",
                        None,
                    );
                    return Ok(());
                }
                SessionEnd::Lost(reason) => {
                    self.diag
                        .websocket_source_disconnects
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
    ) -> Result<Option<WsStream>> {
        let mut attempt = 0usize;
        loop {
            if reconnecting || attempt > 0 {
                // Back off before every reconnect, including the first one
                // after a lost session (a flapping server is rate limited).
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
                "websocket_connecting",
                None,
            );
            let attempted = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(None),
                c = self.client.connect() => c,
            };
            match attempted {
                Ok(ws) => {
                    self.diag
                        .websocket_source_connects
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.health(
                        true,
                        HealthState::Ready,
                        "websocket_connected",
                        None,
                    );
                    return Ok(Some(ws));
                }
                Err(failure) => {
                    self.diag
                        .websocket_source_connect_failures
                        .fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    if attempt >= self.client.config.reconnect_attempts {
                        self.diag.observation.health(
                            true,
                            HealthState::Failed,
                            "websocket_connect_exhausted",
                            Some(ErrorCode::JobFailed),
                        );
                        return Err(error(
                            ErrorCode::JobFailed,
                            format!(
                                "{} ({attempt} consecutive attempts); the supervisor restart policy applies",
                                failure.reason("WebSocket")
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
        mut ws: WsStream,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
    ) -> Result<SessionEnd> {
        enum Event {
            Stop,
            Idle,
            Ping,
            Frame(Option<std::result::Result<Message, tungstenite::Error>>),
        }
        let cfg = &self.client.config;
        let mut ping =
            tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_frame = Instant::now();
        loop {
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => Event::Stop,
                _ = tokio::time::sleep_until(last_frame + cfg.idle_timeout) => Event::Idle,
                _ = ping.tick() => Event::Ping,
                frame = ws.next() => Event::Frame(frame),
            };
            let message = match event {
                Event::Stop => {
                    let _ = tokio::time::timeout(CLOSE_TIMEOUT, ws.close(None)).await;
                    return Ok(SessionEnd::Stop);
                }
                Event::Idle => {
                    self.diag
                        .websocket_source_heartbeat_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(SessionEnd::Lost("websocket_heartbeat_timeout"));
                }
                Event::Ping => {
                    match tokio::time::timeout(
                        cfg.connect_timeout,
                        ws.send(Message::Ping(Default::default())),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            self.diag
                                .websocket_source_pings_sent
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        _ => return Ok(SessionEnd::Lost("websocket_ping_failed")),
                    }
                }
                Event::Frame(None) => return Ok(SessionEnd::Lost("websocket_closed")),
                Event::Frame(Some(Err(tungstenite::Error::Capacity(_)))) => {
                    // Rejected from the frame header / running message size,
                    // before the payload was buffered or decoded.
                    self.diag
                        .websocket_source_dropped_oversize
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(SessionEnd::Lost("websocket_message_too_big"));
                }
                Event::Frame(Some(Err(_))) => {
                    return Ok(SessionEnd::Lost("websocket_protocol_error"))
                }
                Event::Frame(Some(Ok(message))) => message,
            };
            let received_at = std::time::Instant::now();
            let admitted = match message {
                Message::Text(text) => {
                    self.diag
                        .websocket_source_received
                        .fetch_add(1, Ordering::Relaxed);
                    self.ingest(text.as_bytes(), ingress, cancel, received_at)
                        .await?
                }
                Message::Binary(bytes) => {
                    self.diag
                        .websocket_source_received
                        .fetch_add(1, Ordering::Relaxed);
                    if self.config.binary_frames == BinaryFrames::Drop {
                        self.diag
                            .websocket_source_dropped_binary
                            .fetch_add(1, Ordering::Relaxed);
                        true
                    } else {
                        self.ingest(&bytes, ingress, cancel, received_at).await?
                    }
                }
                Message::Close(_) => return Ok(SessionEnd::Lost("websocket_server_closed")),
                // Pings are answered by the protocol layer; Pongs only prove
                // liveness.
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => true,
            };
            if !admitted {
                let _ = tokio::time::timeout(CLOSE_TIMEOUT, ws.close(None)).await;
                return Ok(SessionEnd::Stop);
            }
            // Time spent blocked on our own bounded ingress is not idleness.
            last_frame = Instant::now();
        }
    }

    /// Decode one message into rows and admit them. `Ok(false)` = stop.
    async fn ingest(
        &self,
        payload: &[u8],
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        received_at: std::time::Instant,
    ) -> Result<bool> {
        match self.config.framing {
            WebSocketFraming::Message => {
                self.ingest_record(payload, ingress, cancel, received_at)
                    .await
            }
            WebSocketFraming::Ndjson => {
                for line in payload.split(|&b| b == b'\n') {
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    // JSON whitespace only (RFC 8259: space, tab, LF, CR).
                    if line
                        .iter()
                        .all(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
                    {
                        continue;
                    }
                    if !self
                        .ingest_record(line, ingress, cancel, received_at)
                        .await?
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }

    async fn ingest_record(
        &self,
        record: &[u8],
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        received_at: std::time::Instant,
    ) -> Result<bool> {
        let format = &self.config.payload_format;
        if record.len() > format.max_message_bytes(&self.config.json_limits) {
            self.diag
                .websocket_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        // Format-specific parser + Row working set, charged before decoding
        // and held until the row is admitted (or dropped).
        let estimate = format.decode_scratch(&self.config.schema, record.len());
        let Ok(_scratch) = ingress.owner.acquire(CreditKind::Reservation, estimate) else {
            self.diag
                .websocket_source_dropped_budget
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        };
        let started = std::time::Instant::now();
        let decoded = self.config.payload_format.decode_row(
            &self.config.schema,
            record,
            &self.config.json_limits,
            None,
        );
        self.diag
            .observation
            .record(Latency::Decode, started.elapsed());
        let row = match decoded {
            Ok(row) => row,
            Err(e) => {
                self.diag.csv_decode_error(&self.config.payload_format, &e);
                self.diag
                    .websocket_source_dropped_bad
                    .fetch_add(1, Ordering::Relaxed);
                self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                if self.config.fail_on_decode {
                    self.diag.observation.health(
                        true,
                        HealthState::Failed,
                        "websocket_decode_failed",
                        Some(e.code),
                    );
                    return Err(error(
                        e.code,
                        "WebSocket record decode failed (fail_on_decode)",
                    ));
                }
                return Ok(true);
            }
        };
        self.diag.observation.progress(true, 1);
        self.admit(row, ingress, cancel, OriginSpan::at(received_at))
            .await
    }

    /// Cancel-aware wait for a channel slot and Queue credit, holding one
    /// Reservation-charged working row (same contract as NATS / MQTT).
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
                .websocket_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        let Ok(_working) = ingress.owner.acquire(CreditKind::Reservation, bytes) else {
            self.diag
                .websocket_source_dropped_budget
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
                    &self.diag.websocket_source_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        self.diag
                            .websocket_source_rows
                            .fetch_add(1, Ordering::Relaxed);
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
                    .websocket_source_backpressure_waits
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

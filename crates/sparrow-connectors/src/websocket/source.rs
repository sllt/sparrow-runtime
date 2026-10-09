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
//! - A separate wire actor continues reading and answering heartbeats while
//!   the decode/admission pump waits on its inbox. Complete data messages
//!   enter a reservation-charged bounded prefetch queue; a full queue drops
//!   the newest message and counts `dropped_overflow` (not rows).
//! - A Ping is sent every `ping_interval`; no frame within `idle_timeout`
//!   while waiting to read closes the connection. Reconnect uses capped,
//!   jittered backoff; `reconnect_attempts` consecutive failures fail the
//!   Source retryably and the supervisor restart policy applies.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use sparrow_formats::{JsonLimits, PayloadFormat};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, QueuedRow, RestoreClaim, Result, Row, Schema,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_util::sync::CancellationToken;

use super::client::{
    error, reconnect_delay, send_duplex, BoundClient, DuplexSent, Incoming, WebSocketClientConfig,
    WsStream,
};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::{SecretResolver, TargetPolicy};

pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
pub const DEFAULT_PREFETCH_CAPACITY: usize = 4;
const MAX_PREFETCH_CAPACITY: usize = 64;
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
    /// Complete messages waiting for decode. Full = drop newest, never stop
    /// servicing the WebSocket control frames. Explicit capacities are not clamped.
    pub prefetch_capacity: usize,
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
            prefetch_capacity: DEFAULT_PREFETCH_CAPACITY,
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
        if !(1..=MAX_INBOX).contains(&self.inbox_capacity)
            || !(1..=MAX_PREFETCH_CAPACITY).contains(&self.prefetch_capacity)
            || self.json_limits.max_bytes > 64 * 1024
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket inbox_capacity must be 1..=4096, prefetch_capacity 1..=64; JSON records remain <=64KiB",
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
        Self::reservation_for(&self.client, self.prefetch_capacity)
    }

    /// Queue capacity plus one pump-held message. A permit is acquired
    /// before copying the SDK payload to an exact-size owned buffer, so no
    /// extra pending copy exists when the prefetch queue is full.
    pub fn reservation_for(client: &WebSocketClientConfig, prefetch_capacity: usize) -> usize {
        client.connection_reservation().saturating_add(
            prefetch_capacity
                .saturating_add(1)
                .saturating_mul(client.max_message_bytes.saturating_add(256))
                .saturating_add(8192),
        )
    }

    /// One connector may use at most half the job reservation budget.
    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket connection/prefetch buffers exceed half the job reservation budget; lower max_message_bytes or prefetch_capacity",
            ));
        }
        Ok(())
    }
}

pub struct WebSocketSource {
    pub config: WebSocketSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    client: BoundClient,
    failed: Mutex<bool>,
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

/// A payload cannot outlive its reservation, even when the caller drops the
/// receiving Wire while the transport actor is still closing its socket.
pub(super) struct WireMessage {
    pub(super) payload: Box<[u8]>,
    pub(super) received_at: std::time::Instant,
    _reservation: Arc<MemoryLease>,
}

pub(super) struct Wire {
    pub(super) messages: mpsc::Receiver<WireMessage>,
    task: tokio::task::JoinHandle<Result<()>>,
    stop: CancellationToken,
    _reservation: Arc<MemoryLease>,
    source: Arc<WebSocketSource>,
}

impl Wire {
    pub(super) fn start(
        source: Arc<WebSocketSource>,
        owner: Arc<MemoryOwner>,
        stop: CancellationToken,
    ) -> Result<Self> {
        Self::start_actor(
            source,
            owner,
            stop,
            |source, tx, stop, reservation| async move {
                source.wire_loop(&tx, &stop, &reservation).await
            },
        )
    }

    fn start_actor<A, F>(
        source: Arc<WebSocketSource>,
        owner: Arc<MemoryOwner>,
        stop: CancellationToken,
        actor: A,
    ) -> Result<Self>
    where
        A: FnOnce(
                Arc<WebSocketSource>,
                mpsc::Sender<WireMessage>,
                CancellationToken,
                Arc<MemoryLease>,
            ) -> F
            + Send
            + 'static,
        F: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let reservation =
            Arc::new(owner.acquire(CreditKind::Reservation, source.config.reservation())?);
        let (tx, messages) = mpsc::channel(source.config.prefetch_capacity);
        let actor_stop = stop.clone();
        let actor_reservation = reservation.clone();
        let actor_source = source.clone();
        let task = tokio::spawn(async move {
            // Also interrupt a pump waiting on ingress if the actor panics.
            let _cancel_on_exit = actor_stop.clone().drop_guard();
            let _lifecycle = actor_source.diag.observation.lifecycle(true);
            actor(actor_source, tx, actor_stop, actor_reservation).await
        });
        Ok(Self {
            messages,
            task,
            stop,
            _reservation: reservation,
            source,
        })
    }

    pub(super) async fn close(mut self) -> Result<()> {
        self.stop.cancel();
        self.messages.close();
        (&mut self.task).await.map_err(|_| {
            self.source
                .fail_once("websocket_wire_panicked", ErrorCode::JobFailed);
            error(ErrorCode::JobFailed, "WebSocket wire task panicked")
        })?
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.stop.cancel();
    }
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
            failed: Mutex::new(false),
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
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "websocket-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let source = Arc::new(self);
        let child = cancel.child_token();
        let _cancel_on_drop = child.clone().drop_guard();
        let mut wire = match Wire::start(source.clone(), owner.clone(), child.clone()) {
            Ok(wire) => wire,
            Err(e) => {
                source.fail_once("websocket_budget_failed", e.code);
                return Err(e);
            }
        };
        let result = loop {
            let message = tokio::select! {
                biased;
                _ = child.cancelled() => break Ok(()),
                message = wire.messages.recv() => message,
            };
            let Some(message) = message else { break Ok(()) };
            match source
                .ingest(&message.payload, &ingress, &child, message.received_at)
                .await
            {
                Ok(true) => {}
                Ok(false) => break Ok(()),
                Err(e) => break Err(e),
            }
        };
        let result = result.and(wire.close().await);
        if let Err(e) = &result {
            source.fail_once("websocket_source_failed", e.code);
        } else {
            source.health(HealthState::Stopped, "websocket_stopped");
        }
        result
    }

    async fn wire_loop(
        &self,
        messages: &mpsc::Sender<WireMessage>,
        cancel: &CancellationToken,
        reservation: &Arc<MemoryLease>,
    ) -> Result<()> {
        let mut connected_before = false;
        loop {
            let Some(ws) = self.connect(cancel, connected_before).await? else {
                return Ok(());
            };
            if connected_before {
                self.diag
                    .websocket_source_reconnects
                    .fetch_add(1, Ordering::Relaxed);
            }
            connected_before = true;
            match self.session(ws, messages, reservation, cancel).await {
                SessionEnd::Stop => {
                    return Ok(());
                }
                SessionEnd::Lost(reason) => {
                    self.diag
                        .websocket_source_disconnects
                        .fetch_add(1, Ordering::Relaxed);
                    self.health(HealthState::Reconnecting, reason);
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
            self.health(
                if reconnecting || attempt > 0 {
                    HealthState::Reconnecting
                } else {
                    HealthState::Connecting
                },
                "websocket_connecting",
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
                    self.health(HealthState::Ready, "websocket_connected");
                    return Ok(Some(ws));
                }
                Err(failure) => {
                    self.diag
                        .websocket_source_connect_failures
                        .fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    if attempt >= self.client.config.reconnect_attempts {
                        self.fail_once("websocket_connect_exhausted", ErrorCode::JobFailed);
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
        ws: WsStream,
        messages: &mpsc::Sender<WireMessage>,
        reservation: &Arc<MemoryLease>,
        cancel: &CancellationToken,
    ) -> SessionEnd {
        enum Event {
            Stop,
            Idle,
            Ping,
            Frame(Incoming),
        }
        let (mut writer, mut reader) = ws.split();
        let cfg = &self.client.config;
        let mut ping =
            tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_frame = Instant::now();
        let mut rounds = 0usize;
        loop {
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => Event::Stop,
                _ = messages.closed() => Event::Stop,
                _ = tokio::time::sleep_until(last_frame + cfg.idle_timeout) => Event::Idle,
                _ = ping.tick() => Event::Ping,
                frame = reader.next() => Event::Frame(frame),
            };
            match event {
                Event::Stop => {
                    let _ = tokio::time::timeout(CLOSE_TIMEOUT, writer.send(Message::Close(None)))
                        .await;
                    return SessionEnd::Stop;
                }
                Event::Idle => {
                    self.diag
                        .websocket_source_heartbeat_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    return SessionEnd::Lost("websocket_heartbeat_timeout");
                }
                Event::Ping => {
                    match send_duplex(
                        &mut writer,
                        &mut reader,
                        Message::Ping(Default::default()),
                        cancel,
                        None,
                        cfg.connect_timeout,
                        cfg.idle_timeout,
                        &mut last_frame,
                        self.config.prefetch_capacity.min(16),
                        |frame| self.forward(frame, messages, reservation),
                    )
                    .await
                    {
                        DuplexSent::Ok => {
                            self.diag
                                .websocket_source_pings_sent
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        DuplexSent::Cancelled => return SessionEnd::Stop,
                        DuplexSent::Idle => {
                            self.diag
                                .websocket_source_heartbeat_timeouts
                                .fetch_add(1, Ordering::Relaxed);
                            return SessionEnd::Lost("websocket_heartbeat_timeout");
                        }
                        _ => return SessionEnd::Lost("websocket_ping_failed"),
                    }
                }
                Event::Frame(frame) => {
                    if !self.forward(frame, messages, reservation) {
                        return SessionEnd::Lost("websocket_connection_lost");
                    }
                    last_frame = Instant::now();
                }
            };
            rounds += 1;
            if rounds % self.config.prefetch_capacity.min(16) == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    fn forward(
        &self,
        frame: Incoming,
        messages: &mpsc::Sender<WireMessage>,
        reservation: &Arc<MemoryLease>,
    ) -> bool {
        let payload = match frame {
            Some(Ok(Message::Text(text))) => {
                self.diag
                    .websocket_source_received
                    .fetch_add(1, Ordering::Relaxed);
                text.into()
            }
            Some(Ok(Message::Binary(bytes))) => {
                self.diag
                    .websocket_source_received
                    .fetch_add(1, Ordering::Relaxed);
                if self.config.binary_frames == BinaryFrames::Drop {
                    self.diag
                        .websocket_source_dropped_binary
                        .fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                bytes
            }
            Some(Ok(Message::Close(_))) | None => return false,
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => return true,
            Some(Err(tungstenite::Error::Capacity(_))) => {
                self.diag
                    .websocket_source_dropped_oversize
                    .fetch_add(1, Ordering::Relaxed);
                return false;
            }
            Some(Err(_)) => return false,
        };
        let received_at = std::time::Instant::now();
        match messages.try_reserve() {
            Ok(permit) => {
                // SDK Bytes may retain an entire backing frame/read slab.
                // Copy only after obtaining a slot, under its prepaid credit.
                permit.send(WireMessage {
                    payload: payload.as_ref().to_vec().into_boxed_slice(),
                    received_at,
                    _reservation: reservation.clone(),
                });
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.diag
                    .websocket_source_dropped_overflow
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
        true
    }

    fn fail_once(&self, reason: &'static str, code: ErrorCode) {
        let mut failed = self.failed.lock().unwrap_or_else(|e| e.into_inner());
        if !*failed {
            *failed = true;
            self.diag
                .observation
                .health(true, HealthState::Failed, reason, Some(code));
        }
    }

    fn health(&self, state: HealthState, reason: &'static str) {
        let failed = self.failed.lock().unwrap_or_else(|e| e.into_inner());
        if !*failed {
            self.diag.observation.health(true, state, reason, None);
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
                    self.fail_once("websocket_decode_failed", e.code);
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

#[cfg(test)]
mod contract_tests {
    use super::super::tests::probe_stream;
    use super::*;
    use crate::MapSecretResolver;
    use sparrow_model::{DataType, Field, FieldId, ResourceBudget, SchemaId};

    #[tokio::test(start_paused = true)]
    async fn websocket_wire_close_awaiter_abort_keeps_actor_lifecycle_and_credit() {
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "v", DataType::Int64, false)],
        )
        .unwrap();
        let source = Arc::new(
            WebSocketSource::bind(
                WebSocketSourceConfig::new("ws://127.0.0.1:9000/", schema),
                &MapSecretResolver::new(Default::default()),
                &TargetPolicy::allow("127.0.0.1", 9000),
                IoDiagnostics::new(),
            )
            .unwrap(),
        );
        let owner = MemoryOwner::new(ResourceBudget::compact());
        source.diag.observation.initialize(&owner).unwrap();
        let (ws, probe) = probe_stream(10, 0, true).await;
        let wire = Wire::start_actor(
            source.clone(),
            owner.clone(),
            CancellationToken::new(),
            move |source, tx, stop, lease| async move {
                source.health(HealthState::Ready, "websocket_connected");
                source.session(ws, &tx, &lease, &stop).await;
                Ok(())
            },
        )
        .unwrap();
        let task = tokio::spawn(wire.close());
        for _ in 0..16 {
            if probe.write_attempts.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            probe.write_attempts.load(Ordering::Relaxed) > 0,
            "Close must reach the gated writer"
        );
        assert_eq!(
            source.diag.observation.endpoints().unwrap().0.state,
            HealthState::Ready
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            source.diag.observation.endpoints().unwrap().0.state,
            HealthState::Ready
        );
        assert!(owner.usage().reservation_bytes > 0);
        tokio::time::advance(CLOSE_TIMEOUT).await;
        for _ in 0..16 {
            if owner.usage().reservation_bytes == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(owner.usage().reservation_bytes, 0);
        assert_eq!(
            source.diag.observation.endpoints().unwrap().0.state,
            HealthState::Stopped
        );
        drop(source);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

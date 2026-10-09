//! WebSocket client Sink: one message per row (JSON object, or CSV header +
//! record), live, at-most-once, no application receipt.
//!
//! Two cooperating loops share one bounded send queue (`queue_capacity`
//! messages of at most `max_message_bytes`, charged to the job reservation):
//!
//! - the pump encodes outbox batches into the queue. When the queue is full
//!   (slow server, or reconnecting) `overflow = block` waits, propagating
//!   backpressure into the pipeline outbox (`backpressure_waits`), and
//!   `overflow = drop_newest` drops the row (`dropped_overflow`). An outbox
//!   batch is acknowledged once every row was queued;
//! - the writer owns the connection: it sends queued messages, Pings every
//!   `ping_interval`, treats `idle_timeout` without any received frame as a
//!   dead connection, and a send that does not complete within
//!   `send_timeout` as a stalled one (`send_timeouts`). Either way the
//!   in-flight message is lost (at-most-once), the connection is closed and
//!   re-established with capped jittered backoff; queued messages wait for
//!   the new connection. `reconnect_attempts` consecutive failures fail the
//!   job closed (`websocket_sink_fatal`).
//!
//! On stop the pump queues batches already in the outbox and the writer
//! sends the queue, both within `flush_timeout`, then sends a Close frame.
//! Whatever is left is counted `discarded_on_close`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use sparrow_formats::PayloadFormat;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, RestoreClaim, Result,
    RowBatch,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_util::sync::CancellationToken;

use super::client::{error, reconnect_delay, BoundClient, WebSocketClientConfig, WsStream};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::{SecretResolver, TargetPolicy};

const MAX_OUTBOX: usize = 4096;
const MAX_QUEUE: usize = 1024;

/// Frame type of every message the Sink sends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SinkFrame {
    #[default]
    Text,
    /// JSON only: CSV is text.
    Binary,
}

/// What the pump does when the send queue is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Overflow {
    /// Wait (pipeline backpressure).
    #[default]
    Block,
    /// Drop the row and count `dropped_overflow`.
    DropNewest,
}

#[derive(Clone, Debug)]
pub struct WebSocketSinkConfig {
    pub client: WebSocketClientConfig,
    pub frame: SinkFrame,
    pub queue_capacity: usize,
    pub overflow: Overflow,
    pub send_timeout: Duration,
    /// Shutdown budget: queue the outbox, send the queue, close.
    pub flush_timeout: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
    pub payload_format: PayloadFormat,
}

impl WebSocketSinkConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: WebSocketClientConfig::new(url),
            frame: SinkFrame::Text,
            queue_capacity: 16,
            overflow: Overflow::Block,
            send_timeout: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(2),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
            payload_format: PayloadFormat::Json,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::WEBSOCKET_SINK
    }

    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        if self.payload_format.as_csv().is_some() && self.frame == SinkFrame::Binary {
            return Err(error(
                ErrorCode::InvalidArgument,
                "WebSocket CSV is sent as text frames; frame=binary is JSON-only",
            ));
        }
        let window = Duration::from_millis(10)..=Duration::from_secs(60);
        if !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
            || !(1..=MAX_QUEUE).contains(&self.queue_capacity)
            || !window.contains(&self.send_timeout)
            || !window.contains(&self.flush_timeout)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket sink bounds: outbox_capacity 1..=4096, queue_capacity 1..=1024, send/flush timeouts 10..=60000 ms",
            ));
        }
        self.check_reservation_budget(sparrow_model::ResourceBudget::compact().reservation_bytes)?;
        self.client.validate(policy)
    }

    /// Connection buffers plus a full send queue of maximal messages.
    pub fn reservation(&self) -> usize {
        self.client.connection_reservation().saturating_add(
            self.queue_capacity
                .saturating_mul(self.client.max_message_bytes),
        )
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket sink buffers (queue_capacity x max_message_bytes + connection) exceed half the job reservation budget; lower queue_capacity or max_message_bytes",
            ));
        }
        Ok(())
    }
}

pub struct WebSocketSink {
    pub config: WebSocketSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    client: BoundClient,
    _lease: MemoryLease,
}

impl std::fmt::Debug for WebSocketSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSink")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

/// Outbox receipt: ack only if every row of the batch was queued.
struct BatchReceipt(Option<Arc<InflightCounter>>);
impl BatchReceipt {
    fn ack(&mut self) {
        if let Some(outbox) = self.0.take() {
            outbox.ack();
        }
    }
}
impl Drop for BatchReceipt {
    fn drop(&mut self) {
        if let Some(outbox) = self.0.take() {
            outbox.fail();
        }
    }
}

enum Queued {
    Yes,
    Dropped,
    /// The writer is gone (fatal) or the flush deadline passed.
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sent {
    Ok,
    Failed,
    TimedOut,
}

enum SessionEnd {
    /// Queue closed and drained (or flush deadline reached): closed politely.
    Finished,
    Lost(&'static str),
}

impl WebSocketSink {
    pub fn bind(
        config: WebSocketSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(policy)?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let client = config.client.bind(secrets, policy)?;
        let lease = owner.acquire(CreditKind::Reservation, config.reservation())?;
        Ok(Self {
            config,
            diag,
            client,
            _lease: lease,
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
        let (queue_tx, queue_rx) = mpsc::channel(self.config.queue_capacity);
        tokio::join!(
            self.pump(&mut rx, queue_tx, &cancel, outbox.as_ref()),
            self.writer(queue_rx, &cancel),
        );
        self.diag
            .observation
            .health(false, HealthState::Stopped, "websocket_sink_stopped", None);
    }

    async fn pump(
        &self,
        rx: &mut ObservedReceiver<RowBatch>,
        queue: mpsc::Sender<Message>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) {
        loop {
            let batch = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                b = rx.recv() => b,
            };
            let Some(batch) = batch else {
                return; // input ended: the writer drains the queue and closes
            };
            if !self.queue_batch(&batch, &queue, cancel, outbox, None).await {
                break;
            }
        }
        // Stop: queue what the outbox already holds within flush_timeout.
        let deadline = Instant::now() + self.config.flush_timeout;
        rx.close();
        while Instant::now() < deadline && !queue.is_closed() {
            let Ok(batch) = rx.try_recv() else { break };
            if !self
                .queue_batch(&batch, &queue, cancel, outbox, Some(deadline))
                .await
            {
                break;
            }
        }
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .websocket_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
        }
    }

    /// Encode and queue every row. Returns false once the queue is closed.
    async fn queue_batch(
        &self,
        batch: &RowBatch,
        queue: &mpsc::Sender<Message>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
        deadline: Option<Instant>,
    ) -> bool {
        let mut receipt = BatchReceipt(outbox.cloned());
        let mut delivered = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        let schema = batch.schema();
        let mut all = true;
        for (i, row) in batch.rows().iter().enumerate() {
            let started = std::time::Instant::now();
            let encoded = self.config.payload_format.encode_row(schema, row);
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            let message = match encoded {
                Ok(body) if body.len() > self.config.client.max_message_bytes => {
                    all = false;
                    self.diag
                        .websocket_sink_dropped_oversize
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(body) => match self.config.frame {
                    SinkFrame::Binary => Message::Binary(body.into()),
                    SinkFrame::Text => match String::from_utf8(body) {
                        Ok(text) => Message::Text(text.into()),
                        Err(_) => {
                            all = false;
                            self.diag
                                .websocket_sink_dropped_bad
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    },
                },
                Err(_) => {
                    all = false;
                    self.diag.csv_encode_error(&self.config.payload_format);
                    self.diag
                        .websocket_sink_dropped_bad
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            match self.enqueue(message, queue, cancel, deadline).await {
                Queued::Yes => {}
                Queued::Dropped => all = false,
                Queued::Closed => {
                    let rest = (batch.num_rows() - i) as u64;
                    self.diag
                        .websocket_sink_discarded_on_close
                        .fetch_add(rest, Ordering::Relaxed);
                    return false;
                }
            }
        }
        if all {
            delivered.complete();
            receipt.ack();
        }
        true
    }

    async fn enqueue(
        &self,
        message: Message,
        queue: &mpsc::Sender<Message>,
        cancel: &CancellationToken,
        mut deadline: Option<Instant>,
    ) -> Queued {
        let message = match queue.try_send(message) {
            Ok(()) => {
                self.diag
                    .websocket_sink_queue_items
                    .fetch_add(1, Ordering::Relaxed);
                return Queued::Yes;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Queued::Closed,
            Err(mpsc::error::TrySendError::Full(message)) => message,
        };
        if self.config.overflow == Overflow::DropNewest && deadline.is_none() {
            self.diag
                .websocket_sink_dropped_overflow
                .fetch_add(1, Ordering::Relaxed);
            return Queued::Dropped;
        }
        self.diag
            .websocket_sink_backpressure_waits
            .fetch_add(1, Ordering::Relaxed);
        loop {
            let at = deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            tokio::select! {
                biased;
                permit = queue.reserve() => {
                    let Ok(permit) = permit else { return Queued::Closed };
                    permit.send(message);
                    self.diag
                        .websocket_sink_queue_items
                        .fetch_add(1, Ordering::Relaxed);
                    return Queued::Yes;
                }
                _ = cancel.cancelled(), if deadline.is_none() => {
                    // Stop while blocked: keep waiting, but only until the
                    // flush deadline.
                    deadline = Some(Instant::now() + self.config.flush_timeout);
                }
                _ = tokio::time::sleep_until(at) => return Queued::Closed,
            }
        }
    }

    async fn writer(&self, mut queue: mpsc::Receiver<Message>, cancel: &CancellationToken) {
        let mut connected_before = false;
        let mut attempt = 0usize;
        loop {
            // Stopped while disconnected (nothing can be flushed), or the
            // input ended and everything was sent.
            if cancel.is_cancelled() || (queue.is_closed() && queue.is_empty()) {
                break;
            }
            if connected_before || attempt > 0 {
                let delay = reconnect_delay(attempt + 1, self.client.config.reconnect_max);
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            self.diag.observation.health(
                false,
                if connected_before || attempt > 0 {
                    HealthState::Reconnecting
                } else {
                    HealthState::Connecting
                },
                "websocket_sink_connecting",
                None,
            );
            let attempted = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                c = self.client.connect() => c,
            };
            let ws = match attempted {
                Ok(ws) => ws,
                Err(_) => {
                    self.diag
                        .websocket_sink_connect_failures
                        .fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    if attempt >= self.client.config.reconnect_attempts {
                        self.fatal(cancel);
                        break;
                    }
                    continue;
                }
            };
            attempt = 0;
            self.diag
                .websocket_sink_connects
                .fetch_add(1, Ordering::Relaxed);
            if connected_before {
                self.diag
                    .websocket_sink_reconnects
                    .fetch_add(1, Ordering::Relaxed);
            }
            connected_before = true;
            self.diag.observation.health(
                false,
                HealthState::Ready,
                "websocket_sink_connected",
                None,
            );
            match self.session(ws, &mut queue, cancel).await {
                SessionEnd::Finished => break,
                SessionEnd::Lost(reason) => {
                    self.diag
                        .websocket_sink_disconnects
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag
                        .observation
                        .health(false, HealthState::Reconnecting, reason, None);
                }
            }
        }
        queue.close();
        while queue.try_recv().is_ok() {
            self.diag
                .websocket_sink_queue_items
                .fetch_sub(1, Ordering::Relaxed);
            self.diag
                .websocket_sink_discarded_on_close
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn session(
        &self,
        mut ws: WsStream,
        queue: &mut mpsc::Receiver<Message>,
        cancel: &CancellationToken,
    ) -> SessionEnd {
        enum Event {
            Stop,
            Deadline,
            Idle,
            Ping,
            Frame(Option<std::result::Result<Message, tungstenite::Error>>),
            Send(Option<Message>),
        }
        let cfg = &self.client.config;
        let mut ping =
            tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_frame = Instant::now();
        // Set on stop: keep sending the queue until it closes or this passes.
        let mut flush_deadline: Option<Instant> = None;
        loop {
            let deadline =
                flush_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled(), if flush_deadline.is_none() => Event::Stop,
                _ = tokio::time::sleep_until(deadline), if flush_deadline.is_some() => Event::Deadline,
                _ = tokio::time::sleep_until(last_frame + cfg.idle_timeout) => Event::Idle,
                _ = ping.tick() => Event::Ping,
                frame = ws.next() => Event::Frame(frame),
                item = queue.recv() => Event::Send(item),
            };
            match event {
                Event::Stop => {
                    flush_deadline = Some(Instant::now() + self.config.flush_timeout);
                }
                Event::Deadline => {
                    self.close(&mut ws, Duration::from_millis(10)).await;
                    return SessionEnd::Finished;
                }
                Event::Idle => {
                    self.diag
                        .websocket_sink_heartbeat_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    return SessionEnd::Lost("websocket_sink_heartbeat_timeout");
                }
                Event::Ping => {
                    let sent = self
                        .send_bounded(
                            &mut ws,
                            Message::Ping(Default::default()),
                            cancel,
                            &mut flush_deadline,
                        )
                        .await;
                    if sent != Sent::Ok {
                        return SessionEnd::Lost("websocket_sink_ping_failed");
                    }
                    self.diag
                        .websocket_sink_pings_sent
                        .fetch_add(1, Ordering::Relaxed);
                }
                Event::Frame(Some(Ok(message))) => {
                    last_frame = Instant::now();
                    match message {
                        Message::Text(_) | Message::Binary(_) => {
                            self.diag
                                .websocket_sink_ignored_frames
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Message::Close(_) => {
                            return SessionEnd::Lost("websocket_sink_server_closed")
                        }
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                    }
                }
                Event::Frame(_) => return SessionEnd::Lost("websocket_sink_connection_lost"),
                Event::Send(None) => {
                    // Pump finished and the queue is drained: close politely.
                    let budget = flush_deadline
                        .map(|at| at.saturating_duration_since(Instant::now()))
                        .unwrap_or(self.config.flush_timeout)
                        .max(Duration::from_millis(10));
                    self.close(&mut ws, budget).await;
                    return SessionEnd::Finished;
                }
                Event::Send(Some(message)) => {
                    self.diag
                        .websocket_sink_queue_items
                        .fetch_sub(1, Ordering::Relaxed);
                    match self
                        .send_bounded(&mut ws, message, cancel, &mut flush_deadline)
                        .await
                    {
                        Sent::Ok => {
                            self.diag
                                .websocket_sink_sent
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag.observation.progress(false, 1);
                        }
                        Sent::Failed => {
                            self.diag
                                .websocket_sink_send_failed
                                .fetch_add(1, Ordering::Relaxed);
                            return SessionEnd::Lost("websocket_sink_send_failed");
                        }
                        Sent::TimedOut => {
                            // A partially written frame leaves the stream
                            // unusable: drop the connection (and the message).
                            self.diag
                                .websocket_sink_send_timeouts
                                .fetch_add(1, Ordering::Relaxed);
                            if flush_deadline.is_some() {
                                self.diag
                                    .websocket_sink_discarded_on_close
                                    .fetch_add(1, Ordering::Relaxed);
                                return SessionEnd::Finished;
                            }
                            return SessionEnd::Lost("websocket_sink_send_timeout");
                        }
                    }
                }
            }
        }
    }

    /// Send within `send_timeout`, cut short by the flush deadline once the
    /// job stops (a stop observed while blocked starts that deadline).
    async fn send_bounded(
        &self,
        ws: &mut WsStream,
        message: Message,
        cancel: &CancellationToken,
        flush_deadline: &mut Option<Instant>,
    ) -> Sent {
        let send_deadline = Instant::now() + self.config.send_timeout;
        let send = ws.send(message);
        tokio::pin!(send);
        loop {
            let at = flush_deadline.map_or(send_deadline, |f| f.min(send_deadline));
            tokio::select! {
                biased;
                r = &mut send => return if r.is_ok() { Sent::Ok } else { Sent::Failed },
                _ = cancel.cancelled(), if flush_deadline.is_none() => {
                    *flush_deadline = Some(Instant::now() + self.config.flush_timeout);
                }
                _ = tokio::time::sleep_until(at) => return Sent::TimedOut,
            }
        }
    }

    async fn close(&self, ws: &mut WsStream, budget: Duration) {
        match tokio::time::timeout(budget, ws.close(None)).await {
            Ok(Ok(())) => {
                self.diag
                    .websocket_sink_closes
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.diag
                    .websocket_sink_close_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn fatal(&self, cancel: &CancellationToken) {
        self.diag
            .websocket_sink_fatal
            .fetch_add(1, Ordering::Relaxed);
        self.diag.observation.health(
            false,
            HealthState::Failed,
            "websocket_sink_connect_exhausted",
            Some(ErrorCode::JobFailed),
        );
        cancel.cancel();
    }
}

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
//! sends the queue and a Close frame within the same `flush_timeout`.
//! With no time left (or a partial frame), drop the socket instead.
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
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::client::{
    error, reconnect_delay, send_duplex, BoundClient, DuplexSent, FlushBudget, Incoming,
    WebSocketClientConfig, WsStream, WsWrite,
};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::scratch::{encode_row_charged, EncodeRejected};
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
        if self.payload_format.as_protobuf().is_some() && self.frame != SinkFrame::Binary {
            return Err(error(
                ErrorCode::InvalidArgument,
                "WebSocket protobuf is sent as binary frames; set frame=binary",
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

    /// Connection read side, the message being sent plus its formatted frame
    /// in the write buffer (tungstenite copies the payload into
    /// `out_buffer`), and a full send queue of maximal messages. A row
    /// encoded but not yet queued is charged separately by its encode lease.
    pub fn reservation(&self) -> usize {
        let max = self.client.max_message_bytes;
        self.client
            .connection_reservation()
            .saturating_add(max.saturating_mul(2))
            .saturating_add(self.queue_capacity.saturating_mul(max))
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket sink buffers ((queue_capacity + 4) x max_message_bytes + 128 KiB) exceed half the job reservation budget; lower queue_capacity or max_message_bytes",
            ));
        }
        Ok(())
    }
}

pub struct WebSocketSink {
    pub config: WebSocketSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    client: BoundClient,
    owner: Arc<MemoryOwner>,
    _lease: Arc<MemoryLease>,
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
enum SessionEnd {
    /// Queue closed and drained (or flush deadline reached): closed politely.
    Finished,
    Lost(&'static str),
}

struct QueuedMessage {
    message: Message,
    // Credit follows the frame, not just the actor handle. The encoding
    // lease hands off to this prepaid slab only after successful admission.
    _reservation: Arc<MemoryLease>,
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
        let lease = Arc::new(owner.acquire(CreditKind::Reservation, config.reservation())?);
        Ok(Self {
            config,
            diag,
            client,
            owner,
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
        let flush = FlushBudget::new(self.config.flush_timeout);
        tokio::join!(
            self.pump(&mut rx, queue_tx, &cancel, outbox.as_ref(), &flush),
            self.writer(queue_rx, &cancel, &flush),
        );
        if self.diag.websocket_sink_fatal.load(Ordering::Relaxed) == 0 {
            self.diag.observation.health(
                false,
                HealthState::Stopped,
                "websocket_sink_stopped",
                None,
            );
        }
    }

    async fn pump(
        &self,
        rx: &mut ObservedReceiver<RowBatch>,
        queue: mpsc::Sender<QueuedMessage>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
        flush: &FlushBudget,
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
            if !self
                .queue_batch(&batch, &queue, cancel, outbox, flush)
                .await
            {
                break;
            }
        }
        // Stop: queue what the outbox already holds within flush_timeout.
        flush.deadline(cancel);
        rx.close();
        while !flush.expired(cancel) && !queue.is_closed() {
            let Ok(batch) = rx.try_recv() else { break };
            if !self
                .queue_batch(&batch, &queue, cancel, outbox, flush)
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
        queue: &mpsc::Sender<QueuedMessage>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
        flush: &FlushBudget,
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
            if flush.expired(cancel) || queue.is_closed() {
                self.diag
                    .websocket_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                return false;
            }
            let started = std::time::Instant::now();
            // Scratch and every output growth are charged first; output is
            // capped at max_message_bytes. The lease covers the bytes until
            // they sit in the (pre-charged) send queue.
            let encoded = encode_row_charged(
                &self.owner,
                &self.config.payload_format,
                schema,
                row,
                self.config.client.max_message_bytes,
            );
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            let (message, _encode_lease) = match encoded {
                Err(EncodeRejected::Oversize) => {
                    all = false;
                    self.diag
                        .websocket_sink_dropped_oversize
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(EncodeRejected::Budget) => {
                    all = false;
                    self.diag
                        .websocket_sink_dropped_budget
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok((body, lease)) => match self.config.frame {
                    SinkFrame::Binary => (Message::Binary(body.into()), lease),
                    SinkFrame::Text => match String::from_utf8(body) {
                        Ok(text) => (Message::Text(text.into()), lease),
                        Err(_) => {
                            all = false;
                            self.diag
                                .websocket_sink_dropped_bad
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    },
                },
                Err(EncodeRejected::Bad) => {
                    all = false;
                    self.diag.format_encode_error(&self.config.payload_format);
                    self.diag
                        .websocket_sink_dropped_bad
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            match self.enqueue(message, queue, cancel, flush).await {
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
            if i % 16 == 15 {
                tokio::task::yield_now().await;
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
        queue: &mpsc::Sender<QueuedMessage>,
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) -> Queued {
        if flush.expired(cancel) {
            return Queued::Closed;
        }
        let message = QueuedMessage {
            message,
            _reservation: self._lease.clone(),
        };
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
        if self.config.overflow == Overflow::DropNewest && flush.deadline(cancel).is_none() {
            self.diag
                .websocket_sink_dropped_overflow
                .fetch_add(1, Ordering::Relaxed);
            return Queued::Dropped;
        }
        self.diag
            .websocket_sink_backpressure_waits
            .fetch_add(1, Ordering::Relaxed);
        loop {
            let deadline = flush.deadline(cancel);
            if deadline.is_some_and(|at| Instant::now() >= at) {
                return Queued::Closed;
            }
            let at = deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            tokio::select! {
                biased;
                _ = cancel.cancelled(), if deadline.is_none() => { flush.deadline(cancel); }
                _ = tokio::time::sleep_until(at), if deadline.is_some() => return Queued::Closed,
                permit = queue.reserve() => {
                    let Ok(permit) = permit else { return Queued::Closed };
                    permit.send(message);
                    self.diag
                        .websocket_sink_queue_items
                        .fetch_add(1, Ordering::Relaxed);
                    return Queued::Yes;
                }
            }
        }
    }

    async fn writer(
        &self,
        mut queue: mpsc::Receiver<QueuedMessage>,
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) {
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
            match self.session(ws, &mut queue, cancel, flush).await {
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
        ws: WsStream,
        queue: &mut mpsc::Receiver<QueuedMessage>,
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) -> SessionEnd {
        enum Event {
            Stop,
            Deadline,
            Idle,
            Ping,
            Frame(Incoming),
            Send(Option<QueuedMessage>),
        }
        let (mut writer, mut reader) = ws.split();
        let cfg = &self.client.config;
        let mut ping =
            tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_frame = Instant::now();
        // Alternate the two ready work branches, without compromising the
        // priority of cancellation/deadlines. Neither reverse data traffic
        // nor a permanently full send queue may starve the other direction.
        let mut prefer_send = false;
        let mut rounds = 0usize;
        loop {
            let flush_deadline = flush.deadline(cancel);
            let deadline =
                flush_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            if flush_deadline.is_some_and(|at| Instant::now() >= at) {
                return SessionEnd::Finished;
            }
            let event = if prefer_send {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled(), if flush_deadline.is_none() => Event::Stop,
                    _ = tokio::time::sleep_until(deadline), if flush_deadline.is_some() => Event::Deadline,
                    _ = tokio::time::sleep_until(last_frame + cfg.idle_timeout) => Event::Idle,
                    _ = ping.tick() => Event::Ping,
                    item = queue.recv() => Event::Send(item),
                    frame = reader.next() => Event::Frame(frame),
                }
            } else {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled(), if flush_deadline.is_none() => Event::Stop,
                    _ = tokio::time::sleep_until(deadline), if flush_deadline.is_some() => Event::Deadline,
                    _ = tokio::time::sleep_until(last_frame + cfg.idle_timeout) => Event::Idle,
                    _ = ping.tick() => Event::Ping,
                    frame = reader.next() => Event::Frame(frame),
                    item = queue.recv() => Event::Send(item),
                }
            };
            match event {
                Event::Stop => {
                    flush.deadline(cancel);
                }
                Event::Deadline => {
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
                            &mut writer,
                            &mut reader,
                            Message::Ping(Default::default()),
                            cancel,
                            flush,
                            &mut last_frame,
                        )
                        .await;
                    if sent == DuplexSent::Idle {
                        self.diag
                            .websocket_sink_heartbeat_timeouts
                            .fetch_add(1, Ordering::Relaxed);
                        return SessionEnd::Lost("websocket_sink_heartbeat_timeout");
                    }
                    if sent != DuplexSent::Ok {
                        if flush.expired(cancel) {
                            return SessionEnd::Finished;
                        }
                        return SessionEnd::Lost("websocket_sink_ping_failed");
                    }
                    self.diag
                        .websocket_sink_pings_sent
                        .fetch_add(1, Ordering::Relaxed);
                }
                Event::Frame(frame) => {
                    prefer_send = true;
                    last_frame = Instant::now();
                    if !self.observe_frame(frame) {
                        return SessionEnd::Lost("websocket_sink_connection_lost");
                    }
                }
                Event::Send(None) => {
                    // Pump finished and the queue is drained: close politely.
                    let budget = flush_deadline
                        .map(|at| at.saturating_duration_since(Instant::now()))
                        .unwrap_or(self.config.flush_timeout);
                    self.close(&mut writer, budget).await;
                    return SessionEnd::Finished;
                }
                Event::Send(Some(queued)) => {
                    prefer_send = false;
                    self.diag
                        .websocket_sink_queue_items
                        .fetch_sub(1, Ordering::Relaxed);
                    match self
                        .send_bounded(
                            &mut writer,
                            &mut reader,
                            queued.message,
                            cancel,
                            flush,
                            &mut last_frame,
                        )
                        .await
                    {
                        DuplexSent::Ok => {
                            self.diag
                                .websocket_sink_sent
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag.observation.progress(false, 1);
                        }
                        DuplexSent::Failed | DuplexSent::ReadClosed | DuplexSent::Cancelled => {
                            self.diag
                                .websocket_sink_send_failed
                                .fetch_add(1, Ordering::Relaxed);
                            if flush.expired(cancel) {
                                return SessionEnd::Finished;
                            }
                            return SessionEnd::Lost("websocket_sink_send_failed");
                        }
                        DuplexSent::Idle => {
                            self.diag
                                .websocket_sink_heartbeat_timeouts
                                .fetch_add(1, Ordering::Relaxed);
                            self.diag
                                .websocket_sink_send_failed
                                .fetch_add(1, Ordering::Relaxed);
                            return SessionEnd::Lost("websocket_sink_heartbeat_timeout");
                        }
                        DuplexSent::TimedOut => {
                            // A partially written frame leaves the stream
                            // unusable: drop the connection (and the message).
                            self.diag
                                .websocket_sink_send_timeouts
                                .fetch_add(1, Ordering::Relaxed);
                            if flush.deadline(cancel).is_some() {
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
            rounds += 1;
            if rounds % 16 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    fn observe_frame(&self, frame: Incoming) -> bool {
        match frame {
            Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                self.diag
                    .websocket_sink_ignored_frames
                    .fetch_add(1, Ordering::Relaxed);
                true
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => true,
            _ => false,
        }
    }

    /// Send within `send_timeout`, cut short by the flush deadline once the
    /// job stops (a stop observed while blocked starts that deadline).
    async fn send_bounded(
        &self,
        writer: &mut WsWrite,
        reader: &mut super::client::WsRead,
        message: Message,
        cancel: &CancellationToken,
        flush: &FlushBudget,
        last_frame: &mut Instant,
    ) -> DuplexSent {
        send_duplex(
            writer,
            reader,
            message,
            cancel,
            Some(flush),
            self.config.send_timeout,
            self.client.config.idle_timeout,
            last_frame,
            16,
            |frame| self.observe_frame(frame),
        )
        .await
    }

    async fn close(&self, writer: &mut WsWrite, budget: Duration) {
        if budget.is_zero() {
            self.diag
                .websocket_sink_close_failed
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        match tokio::time::timeout(budget, writer.send(Message::Close(None))).await {
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

#[cfg(test)]
mod contract_tests {
    use super::super::tests::probe_stream;
    use super::*;
    use crate::MapSecretResolver;
    use sparrow_model::ResourceBudget;

    fn sink() -> (WebSocketSink, Arc<MemoryOwner>) {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let sink = WebSocketSink::bind(
            WebSocketSinkConfig::new("ws://127.0.0.1:9000/"),
            &MapSecretResolver::new(Default::default()),
            &TargetPolicy::allow("127.0.0.1", 9000),
            owner.clone(),
            IoDiagnostics::new(),
        )
        .unwrap();
        (sink, owner)
    }

    #[tokio::test]
    async fn websocket_sink_ready_reverse_frames_do_not_starve_send_queue_or_controls() {
        for opcode in [1, 9] {
            let (sink, owner) = sink();
            let (ws, probe) = probe_stream(opcode, 1000, false).await;
            let (tx, mut rx) = mpsc::channel(8);
            let cancel = CancellationToken::new();
            let flush = FlushBudget::new(Duration::from_millis(100));
            for _ in 0..8 {
                assert!(matches!(
                    sink.enqueue(Message::text("payload"), &tx, &cancel, &flush)
                        .await,
                    Queued::Yes
                ));
            }
            drop(tx);
            assert_eq!(
                sink.session(ws, &mut rx, &cancel, &flush).await,
                SessionEnd::Finished
            );
            assert_eq!(sink.diag.snapshot().websocket_sink_sent, 8);
            let reads = probe.reads.load(Ordering::Relaxed);
            assert!(
                (8..=10).contains(&reads),
                "both directions progress fairly: reads={reads}"
            );
            drop((sink, rx));
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_sink_expired_flush_refuses_even_an_immediately_ready_slot() {
        let (sink, _) = sink();
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let flush = FlushBudget::new(Duration::from_millis(100));
        cancel.cancel();
        let first = flush.deadline(&cancel).unwrap();
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(flush.deadline(&cancel), Some(first));
        assert!(matches!(
            sink.enqueue(Message::text("late"), &tx, &cancel, &flush)
                .await,
            Queued::Closed
        ));
        assert!(rx.try_recv().is_err());
        assert_eq!(sink.diag.snapshot().websocket_sink_queue_items, 0);
    }

    #[tokio::test]
    async fn websocket_sink_frame_keeps_prepaid_credit_after_sink_drop() {
        let (sink, owner) = sink();
        let expected = sink.config.reservation();
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let flush = FlushBudget::new(Duration::from_millis(100));
        assert!(matches!(
            sink.enqueue(Message::text("retained"), &tx, &cancel, &flush)
                .await,
            Queued::Yes
        ));
        let frame = rx.recv().await.unwrap();
        drop((sink, tx, rx));
        assert_eq!(owner.usage().reservation_bytes, expected);
        assert!(frame.message.is_text());
        drop(frame);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn websocket_sink_dynamic_bytes_escape_fallback_is_charged_and_bounded() {
        use sparrow_model::{
            DataType, DynamicValue, Field, FieldId, Row, RowBatchBuilder, Scalar, Schema, SchemaId,
        };
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut config = WebSocketSinkConfig::new("ws://127.0.0.1:9000/");
        config.client.max_message_bytes = 1024;
        let sink = WebSocketSink::bind(
            config,
            &MapSecretResolver::new(Default::default()),
            &TargetPolicy::allow("127.0.0.1", 9000),
            owner.clone(),
            IoDiagnostics::new(),
        )
        .unwrap();
        let bound = owner.usage().reservation_bytes;
        let rows_owner = MemoryOwner::new(ResourceBudget::compact());
        let schema = Arc::new(
            Schema::new(
                SchemaId::new(1),
                vec![Field::new(
                    FieldId::new(1),
                    "nested",
                    DataType::Dynamic,
                    false,
                )],
            )
            .unwrap(),
        );
        let mut builder =
            RowBatchBuilder::new(schema, rows_owner, CreditKind::Reservation, 1, 64 * 1024)
                .unwrap();
        builder
            .push(Row {
                values: vec![Scalar::Dynamic(DynamicValue::Array(
                    vec![
                        DynamicValue::utf8("\0".repeat(2000)),
                        DynamicValue::Bytes(vec![0u8; 2000].into()),
                    ]
                    .into(),
                ))],
            })
            .unwrap();
        let batch = builder.finish().unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let flush = FlushBudget::new(Duration::from_millis(100));
        let outbox = Arc::new(InflightCounter::new());
        let hog = owner
            .acquire(
                CreditKind::Reservation,
                owner.budget().reservation_bytes - bound,
            )
            .unwrap();
        outbox.enqueue();
        assert!(
            sink.queue_batch(&batch, &tx, &cancel, Some(&outbox), &flush)
                .await
        );
        assert_eq!(sink.diag.snapshot().websocket_sink_dropped_budget, 1);
        assert_eq!(sink.diag.snapshot().websocket_sink_dropped_bad, 0);
        assert!(rx.try_recv().is_err());
        drop(hog);
        outbox.enqueue();
        assert!(
            sink.queue_batch(&batch, &tx, &cancel, Some(&outbox), &flush)
                .await
        );
        assert_eq!(sink.diag.snapshot().websocket_sink_dropped_oversize, 1);
        assert_eq!(outbox.failed(), 2, "neither rejected row was acknowledged");
        assert!(rx.try_recv().is_err());
        assert_eq!(
            owner.usage().reservation_bytes,
            bound,
            "fallback scratch/output credit returned"
        );
        drop((sink, tx, rx, batch));
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

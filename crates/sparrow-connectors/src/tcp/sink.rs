//! TCP client Sink: one framed record per row, live, at-most-once, no
//! application receipt.
//!
//! The Sink appends the framing itself: `lines` writes the encoded row plus
//! `\n` (a row whose encoding contains a newline is dropped as bad);
//! `length_prefixed` writes a big-endian length then the payload. With CSV
//! over `lines` each connection is one CSV document: the header line (when
//! `header: true`) is written before the first record of every connection.
//!
//! A pump encodes outbox batches into a bounded queue (`queue_capacity`
//! frames, charged to the job reservation): `overflow = block` propagates
//! backpressure into the outbox, `drop_newest` drops the row
//! (`dropped_overflow`). A batch is acknowledged once every row was queued.
//! The writer owns the connection; one frame write (including partial
//! writes) longer than `send_timeout` counts `send_timeouts` and drops the
//! connection, since a torn frame leaves the stream unusable. Bytes sent by
//! the peer are read and discarded (`ignored_bytes`); EOF is a lost
//! connection. Reconnect is capped and jittered; `reconnect_attempts`
//! consecutive failures fail the job closed (`tcp_sink_fatal`).
//!
//! On stop the pump queues batches already in the outbox and the writer
//! sends the queue within `flush_timeout`, then shuts the write side down.
//! Whatever is left is counted `discarded_on_close`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sparrow_formats::PayloadFormat;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, RestoreClaim, Result,
    RowBatch, Schema,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::client::{error, BoundTcp, TcpClientConfig};
use super::framing::{PrefixWidth, TcpFraming};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::net::{reconnect_delay, FlushBudget, NetStream};
use crate::scratch::{encode_csv_record_charged, encode_row_charged, EncodeRejected};
use crate::TargetPolicy;

const MAX_OUTBOX: usize = 4096;
const MAX_QUEUE: usize = 1024;
const DISCARD_BUFFER: usize = 4 * 1024;

/// What the pump does when the send queue is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TcpOverflow {
    /// Wait (pipeline backpressure).
    #[default]
    Block,
    /// Drop the row and count `dropped_overflow`.
    DropNewest,
}

#[cfg(test)]
#[path = "sink_tests.rs"]
mod tests;

#[derive(Clone, Debug)]
pub struct TcpSinkConfig {
    pub client: TcpClientConfig,
    pub queue_capacity: usize,
    pub overflow: TcpOverflow,
    /// Bound on writing one frame (partial writes included).
    pub send_timeout: Duration,
    /// Shutdown budget: queue the outbox, send the queue, shut down.
    pub flush_timeout: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
    pub payload_format: PayloadFormat,
}

impl TcpSinkConfig {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            client: TcpClientConfig::new(host, port),
            queue_capacity: 16,
            overflow: TcpOverflow::Block,
            send_timeout: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(2),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
            payload_format: PayloadFormat::Json,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::TCP_SINK
    }

    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        let window = Duration::from_millis(10)..=Duration::from_secs(60);
        if !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
            || !(1..=MAX_QUEUE).contains(&self.queue_capacity)
            || !window.contains(&self.send_timeout)
            || !window.contains(&self.flush_timeout)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP sink bounds: outbox_capacity 1..=4096, queue_capacity 1..=1024, send/flush timeouts 10..=60000 ms",
            ));
        }
        self.check_reservation_budget(sparrow_model::ResourceBudget::compact().reservation_bytes)?;
        self.client.validate(policy)
    }

    /// Connection buffers, a full send queue of maximal frames, the frame
    /// the writer is sending, and (CSV over lines) the cached header line.
    /// The frame being encoded is charged separately while it is built.
    pub fn reservation(&self) -> usize {
        let frame = self.client.frame_bytes();
        let header = if self.per_connection_header() {
            frame
        } else {
            0
        };
        self.client
            .connection_reservation()
            .saturating_add(self.queue_capacity.saturating_add(1).saturating_mul(frame))
            .saturating_add(header)
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP sink buffers (queue_capacity x max_frame_bytes + connection) exceed half the job reservation budget; lower queue_capacity or max_frame_bytes",
            ));
        }
        Ok(())
    }

    /// CSV over lines writes a header per connection.
    pub(crate) fn per_connection_header(&self) -> bool {
        self.client.framing == TcpFraming::Lines
            && self.payload_format.as_csv().is_some_and(|c| c.header())
    }
}

pub struct TcpSink {
    pub config: TcpSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    client: BoundTcp,
    /// Job ledger: per-row encode scratch and the frame being built.
    owner: Arc<MemoryOwner>,
    /// CSV header line, set by the pump from the first batch schema (at most
    /// `max_frame_bytes`, charged in the static reservation).
    header: OnceLock<Vec<u8>>,
    _lease: Arc<MemoryLease>,
}

/// Queue/in-flight bytes retain their prepaid credit, even if the owning
/// task is cancelled or the Sink is dropped first.
struct QueuedFrame {
    bytes: Vec<u8>,
    _reservation: Arc<MemoryLease>,
}

impl std::fmt::Debug for TcpSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSink")
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

/// Why a row was not framed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rejected {
    Bad,
    Oversize,
    /// The job reservation could not cover the encode working set.
    Budget,
}

impl From<EncodeRejected> for Rejected {
    fn from(e: EncodeRejected) -> Self {
        match e {
            EncodeRejected::Bad => Self::Bad,
            EncodeRejected::Oversize => Self::Oversize,
            EncodeRejected::Budget => Self::Budget,
        }
    }
}

/// Grow `lease` by `extra` bytes before allocating them.
fn charge_more(lease: &mut MemoryLease, extra: usize) -> std::result::Result<(), Rejected> {
    lease
        .grow_to(lease.bytes().saturating_add(extra))
        .map_err(|_| Rejected::Budget)
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
    Finished,
    Lost(&'static str),
}

impl TcpSink {
    pub fn bind(
        config: TcpSinkConfig,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(policy)?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let client = config.client.bind(policy)?;
        let lease = owner.acquire(CreditKind::Reservation, config.reservation())?;
        Ok(Self {
            config,
            diag,
            client,
            owner,
            header: OnceLock::new(),
            _lease: Arc::new(lease),
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
        let flush = FlushBudget::new(self.config.flush_timeout);
        let (queue_tx, queue_rx) = mpsc::channel(self.config.queue_capacity);
        tokio::join!(
            self.pump(&mut rx, queue_tx, &cancel, outbox.as_ref(), &flush),
            self.writer(queue_rx, &cancel, &flush),
        );
        if self.diag.tcp_sink_fatal.load(Ordering::Relaxed) == 0 {
            self.diag
                .observation
                .health(false, HealthState::Stopped, "tcp_sink_stopped", None);
        }
    }

    async fn pump(
        &self,
        rx: &mut ObservedReceiver<RowBatch>,
        queue: mpsc::Sender<QueuedFrame>,
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
                .tcp_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
        }
    }

    /// Encode one row into a complete wire frame. The encoder scratch and
    /// every output growth are charged before allocating, the encoded output
    /// is capped at `max_frame_bytes` (plus a line terminator), and the
    /// returned lease covers the frame until it sits in the pre-charged
    /// send queue.
    fn frame(
        &self,
        schema: &Schema,
        row: &sparrow_model::Row,
    ) -> std::result::Result<(Vec<u8>, MemoryLease), Rejected> {
        let cfg = &self.client.config;
        let limit = cfg.frame_limit();
        let format = &self.config.payload_format;
        let encoded = match (format.as_csv(), cfg.framing) {
            // The record terminator (`\n` or `\r\n`) is stripped below.
            (Some(csv), TcpFraming::Lines) => {
                encode_csv_record_charged(&self.owner, csv, schema, row, limit.saturating_add(2))
            }
            _ => encode_row_charged(&self.owner, format, schema, row, limit),
        };
        let (encoded, mut lease) = encoded.map_err(|e| {
            if e == EncodeRejected::Bad {
                self.diag.csv_encode_error(format);
            }
            Rejected::from(e)
        })?;
        match cfg.framing {
            TcpFraming::Lines => {
                let mut line = encoded;
                if line.last() == Some(&b'\n') {
                    line.pop();
                }
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.len() > limit {
                    return Err(Rejected::Oversize);
                }
                if line.contains(&b'\n') || line.last() == Some(&b'\r') {
                    // A newline inside the record (multiline CSV field)
                    // would split it on the wire.
                    return Err(Rejected::Bad);
                }
                if line.len() == line.capacity() {
                    // Charge the reallocation (old + new buffer) first.
                    charge_more(&mut lease, line.len().saturating_add(1))?;
                    line.try_reserve_exact(1).map_err(|_| Rejected::Budget)?;
                }
                line.push(b'\n');
                Ok((line, lease))
            }
            TcpFraming::LengthPrefixed => {
                if encoded.len() > limit {
                    return Err(Rejected::Oversize);
                }
                let total = cfg.prefix_width.bytes().saturating_add(encoded.len());
                charge_more(&mut lease, total)?;
                let mut frame = Vec::new();
                frame
                    .try_reserve_exact(total)
                    .map_err(|_| Rejected::Budget)?;
                // `limit` never exceeds what the prefix can express
                // (validate refuses it), so these conversions are exact.
                match cfg.prefix_width {
                    PrefixWidth::U16 => {
                        let n = u16::try_from(encoded.len()).map_err(|_| Rejected::Oversize)?;
                        frame.extend(n.to_be_bytes())
                    }
                    PrefixWidth::U32 => {
                        let n = u32::try_from(encoded.len()).map_err(|_| Rejected::Oversize)?;
                        frame.extend(n.to_be_bytes())
                    }
                }
                frame.extend_from_slice(&encoded);
                Ok((frame, lease))
            }
        }
    }

    /// Encode and queue every row. Returns false once the queue is closed.
    async fn queue_batch(
        &self,
        batch: &RowBatch,
        queue: &mpsc::Sender<QueuedFrame>,
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
        if let Err(kind) = self.ensure_header(schema) {
            // Without its header a CSV document is unusable: the batch is
            // not sent (and not acknowledged).
            let rows = batch.num_rows() as u64;
            match kind {
                Rejected::Oversize => &self.diag.tcp_sink_dropped_oversize,
                Rejected::Bad => &self.diag.tcp_sink_dropped_bad,
                Rejected::Budget => &self.diag.tcp_sink_dropped_budget,
            }
            .fetch_add(rows, Ordering::Relaxed);
            return true;
        }
        let mut all = true;
        for (i, row) in batch.rows().iter().enumerate() {
            if i % 16 == 15 {
                tokio::task::yield_now().await;
            }
            if flush.expired(cancel) || queue.is_closed() {
                self.diag
                    .tcp_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                return false;
            }
            let started = std::time::Instant::now();
            let framed = self.frame(schema, row);
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            // The lease covers the frame until it is in the queue (whose
            // slots are part of the static reservation).
            let (frame, _frame_lease) = match framed {
                Ok(framed) => framed,
                Err(kind) => {
                    all = false;
                    match kind {
                        Rejected::Oversize => &self.diag.tcp_sink_dropped_oversize,
                        Rejected::Bad => &self.diag.tcp_sink_dropped_bad,
                        Rejected::Budget => &self.diag.tcp_sink_dropped_budget,
                    }
                    .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            match self.enqueue(frame, queue, cancel, flush).await {
                Queued::Yes => {}
                Queued::Dropped => all = false,
                Queued::Closed => {
                    let rest = (batch.num_rows() - i) as u64;
                    self.diag
                        .tcp_sink_discarded_on_close
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

    /// CSV over lines: build the per-connection header line once. Its length
    /// is computed without allocating and must fit `max_frame_bytes`, which
    /// is the header slot charged in [`TcpSinkConfig::reservation`].
    fn ensure_header(&self, schema: &Schema) -> std::result::Result<(), Rejected> {
        if !self.config.per_connection_header() || self.header.get().is_some() {
            return Ok(());
        }
        let Some(csv) = self.config.payload_format.as_csv() else {
            return Ok(());
        };
        csv.check_schema(schema).map_err(|_| Rejected::Bad)?;
        if schema.fields.iter().any(|f| f.name.contains(['\n', '\r'])) {
            return Err(Rejected::Bad);
        }
        let len = csv.header_len(schema).map_err(|_| Rejected::Bad)?;
        // header_len includes the one LF terminator; frame_limit does not.
        if len.saturating_sub(1) > self.client.config.frame_limit() {
            return Err(Rejected::Oversize);
        }
        // Bound capacity, not only length: unbounded Vec geometric growth
        // can exceed the statically reserved header slot.
        let header = csv
            .encode_header_bounded(schema, len, |_| Ok(()))
            .map_err(|e| {
                if e.code == ErrorCode::ResourceExhausted {
                    Rejected::Budget
                } else {
                    Rejected::Bad
                }
            })?;
        let _ = self.header.set(header);
        Ok(())
    }

    async fn enqueue(
        &self,
        frame: Vec<u8>,
        queue: &mpsc::Sender<QueuedFrame>,
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) -> Queued {
        if flush.expired(cancel) {
            return Queued::Closed;
        }
        let frame = QueuedFrame {
            bytes: frame,
            _reservation: self._lease.clone(),
        };
        let frame = match queue.try_send(frame) {
            Ok(()) => {
                self.diag
                    .tcp_sink_queue_items
                    .fetch_add(1, Ordering::Relaxed);
                return Queued::Yes;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Queued::Closed,
            Err(mpsc::error::TrySendError::Full(frame)) => frame,
        };
        if self.config.overflow == TcpOverflow::DropNewest && flush.deadline(cancel).is_none() {
            self.diag
                .tcp_sink_dropped_overflow
                .fetch_add(1, Ordering::Relaxed);
            return Queued::Dropped;
        }
        self.diag
            .tcp_sink_backpressure_waits
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
                    permit.send(frame);
                    self.diag.tcp_sink_queue_items.fetch_add(1, Ordering::Relaxed);
                    return Queued::Yes;
                }
            }
        }
    }

    async fn writer(
        &self,
        mut queue: mpsc::Receiver<QueuedFrame>,
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) {
        let mut connected_before = false;
        let mut attempt = 0usize;
        loop {
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
                "tcp_sink_connecting",
                None,
            );
            let attempted = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                c = self.client.connect() => c,
            };
            let stream = match attempted {
                Ok(stream) => stream,
                Err(_) => {
                    self.diag
                        .tcp_sink_connect_failures
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
            self.diag.tcp_sink_connects.fetch_add(1, Ordering::Relaxed);
            if connected_before {
                self.diag
                    .tcp_sink_reconnects
                    .fetch_add(1, Ordering::Relaxed);
            }
            connected_before = true;
            self.diag
                .observation
                .health(false, HealthState::Ready, "tcp_sink_connected", None);
            match self.session(stream, &mut queue, cancel, flush).await {
                SessionEnd::Finished => break,
                SessionEnd::Lost(reason) => {
                    self.diag
                        .tcp_sink_disconnects
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
                .tcp_sink_queue_items
                .fetch_sub(1, Ordering::Relaxed);
            self.diag
                .tcp_sink_discarded_on_close
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn session(
        &self,
        stream: NetStream,
        queue: &mut mpsc::Receiver<QueuedFrame>,
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) -> SessionEnd {
        enum Event {
            Stop,
            Deadline,
            Read(std::io::Result<usize>),
            Send(Option<QueuedFrame>),
        }
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut discard = [0u8; DISCARD_BUFFER];
        let mut header_pending = self.config.per_connection_header();
        let mut prefer_send = false;
        let mut turns = 0usize;
        loop {
            let flush_deadline = flush.deadline(cancel);
            if flush_deadline.is_some_and(|at| Instant::now() >= at) {
                return SessionEnd::Finished;
            }
            let deadline =
                flush_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            // A peer whose read side is always ready must not starve queued
            // output. Alternate priorities, rather than depending on chance.
            let event = if prefer_send {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled(), if flush_deadline.is_none() => Event::Stop,
                    _ = tokio::time::sleep_until(deadline), if flush_deadline.is_some() => Event::Deadline,
                    item = queue.recv() => Event::Send(item),
                    n = reader.read(&mut discard) => Event::Read(n),
                }
            } else {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled(), if flush_deadline.is_none() => Event::Stop,
                    _ = tokio::time::sleep_until(deadline), if flush_deadline.is_some() => Event::Deadline,
                    n = reader.read(&mut discard) => Event::Read(n),
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
                Event::Read(Ok(0)) => return SessionEnd::Lost("tcp_sink_peer_closed"),
                Event::Read(Ok(n)) => {
                    prefer_send = true;
                    self.diag
                        .tcp_sink_ignored_bytes
                        .fetch_add(n as u64, Ordering::Relaxed);
                }
                Event::Read(Err(_)) => return SessionEnd::Lost("tcp_sink_connection_lost"),
                Event::Send(None) => {
                    // Pump finished and the queue is drained: shut down.
                    let budget = flush_deadline
                        .map(|at| at.saturating_duration_since(Instant::now()))
                        .unwrap_or(self.config.flush_timeout);
                    self.close(&mut writer, budget).await;
                    return SessionEnd::Finished;
                }
                Event::Send(Some(frame)) => {
                    prefer_send = false;
                    self.diag
                        .tcp_sink_queue_items
                        .fetch_sub(1, Ordering::Relaxed);
                    // CSV over lines: the connection's header goes out with
                    // its first record, in the same bounded send (no copy).
                    let head = self
                        .header
                        .get()
                        .filter(|_| header_pending)
                        .map(Vec::as_slice);
                    header_pending = false;
                    match self
                        .send_bounded(&mut writer, &mut reader, head, &frame.bytes, cancel, flush)
                        .await
                    {
                        Sent::Ok => {
                            let written = frame
                                .bytes
                                .len()
                                .saturating_add(head.map_or(0, <[u8]>::len));
                            self.diag.tcp_sink_sent.fetch_add(1, Ordering::Relaxed);
                            self.diag
                                .tcp_sink_bytes_written
                                .fetch_add(written as u64, Ordering::Relaxed);
                            self.diag.observation.progress(false, 1);
                        }
                        Sent::Failed => {
                            self.diag
                                .tcp_sink_send_failed
                                .fetch_add(1, Ordering::Relaxed);
                            return SessionEnd::Lost("tcp_sink_send_failed");
                        }
                        Sent::TimedOut => {
                            // A torn frame leaves the stream unusable: drop
                            // the connection (and the frame).
                            self.diag
                                .tcp_sink_send_timeouts
                                .fetch_add(1, Ordering::Relaxed);
                            if flush.deadline(cancel).is_some() {
                                self.diag
                                    .tcp_sink_discarded_on_close
                                    .fetch_add(1, Ordering::Relaxed);
                                return SessionEnd::Finished;
                            }
                            return SessionEnd::Lost("tcp_sink_send_timeout");
                        }
                    }
                }
            }
            turns += 1;
            if turns % 16 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Write one frame (after `head`, the connection's CSV header) within
    /// `send_timeout`, cut short by the flush deadline once the job stops.
    async fn send_bounded(
        &self,
        writer: &mut WriteHalf<NetStream>,
        reader: &mut ReadHalf<NetStream>,
        head: Option<&[u8]>,
        frame: &[u8],
        cancel: &CancellationToken,
        flush: &FlushBudget,
    ) -> Sent {
        let send_deadline = Instant::now() + self.config.send_timeout;
        let send = async {
            if let Some(head) = head {
                writer.write_all(head).await?;
            }
            writer.write_all(frame).await?;
            writer.flush().await
        };
        tokio::pin!(send);
        let mut discard = [0u8; DISCARD_BUFFER];
        let mut reads = 0usize;
        loop {
            let flush_deadline = flush.deadline(cancel);
            let at = flush_deadline.map_or(send_deadline, |f| f.min(send_deadline));
            if Instant::now() >= at {
                return Sent::TimedOut;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled(), if flush_deadline.is_none() => {
                    flush.deadline(cancel);
                }
                _ = tokio::time::sleep_until(at) => return Sent::TimedOut,
                r = &mut send => return if r.is_ok() { Sent::Ok } else { Sent::Failed },
                n = reader.read(&mut discard) => {
                    match n {
                        Ok(0) | Err(_) => return Sent::Failed,
                        Ok(n) => { self.diag.tcp_sink_ignored_bytes.fetch_add(n as u64, Ordering::Relaxed); }
                    }
                    reads += 1;
                    if reads % 16 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }

    async fn close(&self, stream: &mut WriteHalf<NetStream>, budget: Duration) {
        match tokio::time::timeout(budget, stream.shutdown()).await {
            Ok(Ok(())) => {
                self.diag.tcp_sink_closes.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.diag
                    .tcp_sink_close_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn fatal(&self, cancel: &CancellationToken) {
        self.diag.tcp_sink_fatal.fetch_add(1, Ordering::Relaxed);
        self.diag.observation.health(
            false,
            HealthState::Failed,
            "tcp_sink_connect_exhausted",
            Some(ErrorCode::JobFailed),
        );
        cancel.cancel();
    }
}

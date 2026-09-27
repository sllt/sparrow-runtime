//! Bounded mailbox: item cap + byte cap. Senders select on cancel so a
//! full queue cannot deadlock a stop.

use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use crate::mailbox_observe::{MailboxObserver, QueueDepth, QueuedStamp, SendObservation};
use sparrow_model::{ErrorCode, Result, RowBatch, SparrowError};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
pub struct MailboxConfig {
    pub max_items: usize,
    pub max_bytes: usize,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            max_items: 8,
            max_bytes: 256 * 1024,
        }
    }
}

impl MailboxConfig {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_items == 0
            || self.max_bytes == 0
            || self.max_items > Semaphore::MAX_PERMITS
            || self.max_bytes > Semaphore::MAX_PERMITS
            || self.max_bytes > u32::MAX as usize
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "mailbox bounds must be non-zero and fit semaphore/u32 limits",
            ));
        }
        Ok(())
    }
}

/// Watermark / idle punctuation travelling with the dataflow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamControl {
    /// Live-only FIFO round. No durable identity or wall-clock interpretation.
    LiveFeedStart { micros: i64, epoch: u64 },
    /// Probe start time, -1 for data-only, -2 for unavailable. The receiving
    /// silence stage checks freshness AFTER all preceding ingress/mailboxes.
    LiveFeedEnd { probe_started: i64 },
    /// Durable source-ordered logical time; only admitted by paused-time plans.
    ProcessingTime { micros: i64 },
    /// End of a source-observed decision, after its optional input. -1 means
    /// no qualifying observation; otherwise this is the validated coverage
    /// start in paused logical microseconds. Only the silence profile admits it.
    FeedObservation { coverage_since: i64 },
    /// Full per-edge progress in a durable graph decision (watermark -1 means
    /// uninitialized; flags bit0=idle, bit1=permanent source EOF).
    GraphProgress { watermark_micros: i64, flags: u8 },
    /// End of one globally persisted graph decision, before its barrier.
    GraphRoundEnd { sequence: u64 },
    /// Permanent data completion. Channels closing without this mark in a
    /// graph are failures/cancellation, never authorization for final time.
    EndOfInput,
    Watermark {
        input: u16,
        wm_micros: i64,
    },
    Idle {
        input: u16,
    },
    Active {
        input: u16,
    },
    /// Aligned checkpoint barrier (single-job). Not exactly-once.
    CheckpointBarrier {
        checkpoint_id: u64,
    },
}

pub struct Envelope {
    pub batch: Option<RowBatch>,
    pub control: Option<StreamControl>,
    bytes: usize,
    permits: Arc<Semaphore>,
    observer: Option<Arc<MailboxObserver>>,
    depth: QueueDepth,
    held: bool,
}

impl Drop for Envelope {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            let mut state = observer.lock();
            if self.held {
                if state.snapshot.consumer_held.remove(self.depth) {
                    state.snapshot.accounting_errors_total =
                        state.snapshot.accounting_errors_total.saturating_add(1);
                }
            }
            let invalid = state.snapshot.credits_reserved_bytes < self.bytes;
            debug_assert!(!invalid, "mailbox observation credit underflow");
            state.snapshot.accounting_errors_total = state
                .snapshot
                .accounting_errors_total
                .saturating_add(u64::from(invalid));
            state.snapshot.credits_reserved_bytes = state
                .snapshot
                .credits_reserved_bytes
                .saturating_sub(self.bytes);
        }
        self.permits.add_permits(self.bytes.max(1));
    }
}

impl Envelope {
    pub fn take(&mut self) -> (Option<RowBatch>, Option<StreamControl>) {
        (self.batch.take(), self.control.take())
    }
}

#[derive(Clone)]
pub struct MailboxTx {
    tx: mpsc::Sender<Envelope>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
    cancel: CancellationToken,
    observer: Option<Arc<MailboxObserver>>,
}

pub struct MailboxRx {
    rx: mpsc::Receiver<Envelope>,
    cancel: CancellationToken,
    bytes: Arc<Semaphore>,
    observer: Option<Arc<MailboxObserver>>,
}

pub fn channel(cfg: MailboxConfig, cancel: CancellationToken) -> Result<(MailboxTx, MailboxRx)> {
    channel_inner(cfg, cancel, None)
}

/// Opt-in for standalone embedders; Kernel uses this accounting on every edge.
pub fn observed_channel(
    cfg: MailboxConfig,
    cancel: CancellationToken,
    owner: &Arc<sparrow_model::MemoryOwner>,
) -> Result<(MailboxTx, MailboxRx, Arc<MailboxObserver>)> {
    let observer = MailboxObserver::new(cfg, owner)?;
    let (tx, rx) = channel_observed(cfg, cancel, observer.clone())?;
    Ok((tx, rx, observer))
}

pub(crate) fn channel_observed(
    cfg: MailboxConfig,
    cancel: CancellationToken,
    observer: Arc<MailboxObserver>,
) -> Result<(MailboxTx, MailboxRx)> {
    channel_inner(cfg, cancel, Some(observer))
}

fn channel_inner(
    cfg: MailboxConfig,
    cancel: CancellationToken,
    observer: Option<Arc<MailboxObserver>>,
) -> Result<(MailboxTx, MailboxRx)> {
    cfg.validate()?;
    let (tx, rx) = mpsc::channel(cfg.max_items);
    let bytes = Arc::new(Semaphore::new(cfg.max_bytes));
    Ok((
        MailboxTx {
            tx,
            bytes: bytes.clone(),
            max_bytes: cfg.max_bytes,
            cancel: cancel.clone(),
            observer: observer.clone(),
        },
        MailboxRx {
            rx,
            cancel,
            bytes,
            observer,
        },
    ))
}

impl MailboxTx {
    pub async fn send(&self, batch: RowBatch) -> Result<bool> {
        self.send_inner(Some(batch), None, true).await
    }

    /// Nonblocking best-effort DATA admission. Control always uses send_control.
    pub async fn try_send(&self, batch: RowBatch) -> Result<bool> {
        self.send_inner(Some(batch), None, false).await
    }

    pub async fn send_control(&self, control: StreamControl) -> Result<bool> {
        self.send_inner(None, Some(control), true).await
    }

    pub(crate) async fn try_send_control(&self, control: StreamControl) -> Result<bool> {
        self.send_inner(None, Some(control), false).await
    }
    pub(crate) fn cancel_path(&self) -> bool {
        let active=!self.cancel.is_cancelled();self.cancel.cancel();active
    }

    async fn send_inner(
        &self,
        batch: Option<RowBatch>,
        control: Option<StreamControl>,
        wait: bool,
    ) -> Result<bool> {
        let mut observation = SendObservation::new(self.observer.clone());
        let n = batch
            .as_ref()
            .map(|b| b.tracked_bytes().max(1))
            .unwrap_or(1);
        if n > self.max_bytes {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("batch {n}B exceeds mailbox max_bytes {}", self.max_bytes),
            ));
        }
        if self.cancel.is_cancelled() {
            return Ok(false);
        }
        let permit = match self.bytes.try_acquire_many(n as u32) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::Closed) => return Err(closed()),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                if !wait { return Ok(false); }
                observation.blocked();
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => return Ok(false),
                    permit = self.bytes.acquire_many(n as u32) => permit.map_err(|_| closed())?,
                }
            }
        };
        if let Some(observer) = &self.observer {
            observer.lock().snapshot.credits_reserved_bytes += n;
        }
        permit.forget();
        let depth = QueueDepth {
            items: 1,
            data_batches: usize::from(batch.is_some()),
            rows: batch.as_ref().map(|b| b.num_rows()).unwrap_or(0),
            controls: usize::from(control.is_some()),
            accounted_bytes: n,
        };
        let env = Envelope {
            batch,
            control,
            bytes: n,
            permits: Arc::clone(&self.bytes),
            observer: self.observer.clone(),
            depth,
            held: false,
        };
        let slot = match self.tx.try_reserve() {
            Ok(slot) => slot,
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(closed()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !wait { return Ok(false); }
                observation.blocked();
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => return Ok(false),
                    slot = self.tx.reserve() => slot.map_err(|_| closed())?,
                }
            }
        };
        if self.cancel.is_cancelled() {
            return Ok(false);
        }
        if let Some(observer) = &self.observer {
            let mut state = observer.lock();
            // Receiver close shares this lock. An already-reserved Tokio slot
            // must not publish/drop its envelope reentrantly under this lock.
            if !state.snapshot.receiver_open {
                return Err(closed());
            }
            let now = Instant::now();
            state.stamps.push_back(QueuedStamp {
                at: now,
                data: depth.data_batches != 0,
            });
            debug_assert!(state.stamps.len() <= self.max_items());
            state.snapshot.queued.add(depth);
            state.snapshot.peak_queued_items = state
                .snapshot
                .peak_queued_items
                .max(state.snapshot.queued.items);
            state.snapshot.peak_queued_bytes = state
                .snapshot
                .peak_queued_bytes
                .max(state.snapshot.queued.accounted_bytes);
            state.snapshot.enqueued_items_total =
                state.snapshot.enqueued_items_total.saturating_add(1);
            observation.finish(&mut state, now, true);
            // No await or telemetry allocation in this per-edge critical section
            // (Tokio may still allocate its own bounded channel blocks).
            slot.send(env);
        } else {
            slot.send(env);
        }
        Ok(true)
    }

    fn max_items(&self) -> usize {
        self.tx.max_capacity()
    }
}

impl MailboxRx {
    pub(crate) fn barrier_blocked(&self,blocked:bool) {
        if let Some(observer)=&self.observer{if let Some(progress)=&mut observer.lock().snapshot.input_progress{progress.barrier_blocked=blocked;}}
    }
    /// Poll one physical input without consuming other inputs or spawning a
    /// relay. UnionAll rotates the first polled edge after each envelope.
    pub(crate) fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<Envelope>> {
        let mut state = self.observer.as_ref().map(|observer| observer.lock());
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(mut env)) => {
                if let Some(state) = &mut state {
                    if let Some(progress)=&mut state.snapshot.input_progress{progress.received(&env);}
                    let stamp = state.stamps.pop_front().expect("queued observation");
                    state.snapshot.residence.record(stamp.at.elapsed());
                    if state.snapshot.queued.remove(env.depth) {
                        state.snapshot.accounting_errors_total = state.snapshot.accounting_errors_total.saturating_add(1);
                    }
                    state.snapshot.consumer_held.add(env.depth);
                    state.snapshot.received_items_total = state.snapshot.received_items_total.saturating_add(1);
                    env.held = true;
                }
                Poll::Ready(Some(env))
            }
            other => other,
        }
    }
    pub async fn recv(&mut self) -> Result<Option<Envelope>> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Ok(None),
            msg = std::future::poll_fn(|cx| {
                // Serialize the channel pop with metric removal. Merely calling
                // recv().await then adjusting counters permits a concurrent send
                // to overrun the bounded timestamp ring or reorder its entries.
                let mut state = self.observer.as_ref().map(|observer| observer.lock());
                let result = self.rx.poll_recv(cx);
                match result {
                    Poll::Ready(Some(mut env)) => {
                        if let Some(state) = &mut state {
                            if let Some(progress)=&mut state.snapshot.input_progress{progress.received(&env);}
                            let stamp=state.stamps.pop_front().expect("queued observation");
                            state.snapshot.residence.record(stamp.at.elapsed());
                            if state.snapshot.queued.remove(env.depth) {
                                state.snapshot.accounting_errors_total = state.snapshot.accounting_errors_total.saturating_add(1);
                            }
                            state.snapshot.consumer_held.add(env.depth);
                            state.snapshot.received_items_total = state.snapshot.received_items_total.saturating_add(1);
                            env.held = true;
                        }
                        Poll::Ready(Some(env))
                    }
                    other => other,
                }
            }) => Ok(msg),
        }
    }
}

impl Drop for MailboxRx {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            let mut state = observer.lock();
            state.snapshot.receiver_open = false;
            if let Some(progress)=&mut state.snapshot.input_progress{progress.barrier_blocked=false;}
            state.snapshot.discarded_on_close_total = state
                .snapshot
                .discarded_on_close_total
                .saturating_add(state.snapshot.queued.items as u64);
            state.snapshot.queued = QueueDepth::default();
            state.stamps.clear();
        }
        // Also wake senders blocked on byte credit, even if a consumer still
        // owns the last envelope. Dropping only the mpsc receiver is insufficient.
        self.bytes.close();
        self.rx.close();
        // Field destruction drains queued envelopes after the observer lock has
        // been released. Their credits are returned exactly once by Envelope.
    }
}

fn closed() -> SparrowError {
    SparrowError::new(ErrorCode::Cancelled, "mailbox closed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{
        CreditKind, DataType, Field, MemoryOwner, ResourceBudget, Row, RowBatchBuilder, Scalar,
        Schema,
    };
    use std::time::Duration;

    fn owner() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget::compact())
    }

    fn batch(owner: &Arc<MemoryOwner>, values: &[i64]) -> RowBatch {
        let schema =
            Arc::new(Schema::new(1, vec![Field::new(1, "v", DataType::Int64, false)]).unwrap());
        let mut b =
            RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 16, 4096).unwrap();
        for value in values {
            b.push(Row {
                values: vec![Scalar::Int64(*value)],
            })
            .unwrap();
        }
        b.finish().unwrap()
    }

    async fn waiting(observer: &MailboxObserver) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while observer.snapshot().waiting_senders == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sender must block");
    }

    #[tokio::test]
    async fn obs_queue_control_held_and_payload_ownership_are_distinct() {
        let owner = owner();
        let (tx, mut rx, observer) = observed_channel(
            MailboxConfig {
                max_items: 4,
                max_bytes: 1024,
            },
            CancellationToken::new(),
            &owner,
        )
        .unwrap();
        let b = batch(&owner, &[1, 2]);
        let bytes = b.tracked_bytes();
        tx.send(b).await.unwrap();
        tx.send_control(StreamControl::CheckpointBarrier { checkpoint_id: 7 })
            .await
            .unwrap();
        let s = observer.snapshot();
        assert_eq!(
            s.queued,
            QueueDepth {
                items: 2,
                data_batches: 1,
                rows: 2,
                controls: 1,
                accounted_bytes: bytes + 1
            }
        );
        assert_eq!(s.credits_reserved_bytes, bytes + 1);
        assert_eq!(s.blocked_sends_total, 0);
        assert!(s.oldest_queued_data_age_us.is_some());
        let mut env = rx.recv().await.unwrap().unwrap();
        let s = observer.snapshot();
        assert_eq!(s.queued.items, 1);
        assert_eq!(s.queued.controls, 1);
        assert_eq!(s.consumer_held.rows, 2);
        assert!(s.oldest_queued_data_age_us.is_none());
        assert!(s.oldest_queued_age_us.is_some());
        let (payload, _) = env.take();
        drop(env);
        assert_eq!(observer.snapshot().consumer_held.items, 0);
        assert_eq!(
            owner.usage().reservation_bytes,
            bytes,
            "payload still owns its physical credit"
        );
        assert_eq!(observer.snapshot().credits_reserved_bytes, 1);
        drop(rx.recv().await.unwrap());
        let s = observer.snapshot();
        assert_eq!(s.credits_reserved_bytes, 0);
        assert_eq!(s.received_items_total, 2);
        assert!(s.oldest_queued_age_us.is_none());
        drop(payload);
        drop(rx);
        drop(tx);
        assert_eq!(
            owner.usage().queue_bytes,
            s.metadata_bytes,
            "observer retains metadata, not payloads"
        );
        drop(observer);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[cfg(not(debug_assertions))]
    #[tokio::test]
    async fn r9_release_accounting_error_is_visible_without_credit_leak() {
        let owner = owner();
        let (tx, mut rx, observer) =
            observed_channel(MailboxConfig::default(), CancellationToken::new(), &owner).unwrap();
        tx.send_control(StreamControl::Idle { input: 0 })
            .await
            .unwrap();
        let env = rx.recv().await.unwrap().unwrap();
        {
            let mut state = observer.lock();
            state.snapshot.consumer_held = QueueDepth::default();
            state.snapshot.credits_reserved_bytes = 0;
        }
        drop(env);
        let snapshot = observer.snapshot();
        assert_eq!(snapshot.accounting_errors_total, 2);
        assert_eq!(snapshot.consumer_held.items, 0);
        assert_eq!(snapshot.credits_reserved_bytes, 0);
        assert_eq!(
            tx.bytes.available_permits(),
            MailboxConfig::default().max_bytes
        );
        drop(tx);
        drop(rx);
        drop(observer);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn obs_slot_wait_and_future_abort_do_not_claim_an_enqueue() {
        let owner = owner();
        let (tx, rx, observer) = observed_channel(
            MailboxConfig {
                max_items: 1,
                max_bytes: 1024,
            },
            CancellationToken::new(),
            &owner,
        )
        .unwrap();
        let b = batch(&owner, &[1]);
        let bytes = b.tracked_bytes();
        tx.send(b).await.unwrap();
        let next = batch(&owner, &[2]);
        let tx2 = tx.clone();
        let pending = tokio::spawn(async move { tx2.send(next).await });
        waiting(&observer).await;
        let s = observer.snapshot();
        assert_eq!(s.queued.items, 1);
        assert_eq!(s.peak_queued_items, 1);
        assert_eq!(s.enqueued_items_total, 1);
        assert_eq!(
            s.credits_reserved_bytes,
            bytes * 2,
            "pending sender already holds byte credit"
        );
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        let s = observer.snapshot();
        assert_eq!(s.aborted_sends_total, 1);
        assert_eq!(s.blocked_sends_total, 1);
        assert_eq!(s.completed_waits_total, 1);
        assert_eq!(s.waiting_senders, 0);
        assert_eq!(s.credits_reserved_bytes, bytes);
        drop(rx);
        let s = observer.snapshot();
        assert!(!s.receiver_open);
        assert_eq!(s.discarded_on_close_total, 1);
        assert_eq!(s.credits_reserved_bytes, 0);
        assert_eq!(s.queued.items, 0);
        drop(tx);
        drop(observer);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn obs_receiver_close_wakes_byte_waiter_while_consumer_holds_credit() {
        let owner = owner();
        let b = batch(&owner, &[1]);
        let bytes = b.tracked_bytes();
        let (tx, mut rx, observer) = observed_channel(
            MailboxConfig {
                max_items: 2,
                max_bytes: bytes,
            },
            CancellationToken::new(),
            &owner,
        )
        .unwrap();
        tx.send(b).await.unwrap();
        let held = rx.recv().await.unwrap().unwrap();
        let next = batch(&owner, &[2]);
        let tx2 = tx.clone();
        let pending = tokio::spawn(async move { tx2.send(next).await });
        waiting(&observer).await;
        assert_eq!(observer.snapshot().queued.items, 0);
        drop(rx);
        let err = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Cancelled);
        let s = observer.snapshot();
        assert_eq!(s.consumer_held.items, 1);
        assert_eq!(s.credits_reserved_bytes, bytes);
        assert_eq!(s.waiting_senders, 0);
        assert_eq!(s.aborted_sends_total, 1);
        drop(held);
        assert_eq!(observer.snapshot().credits_reserved_bytes, 0);
        drop(tx);
        drop(observer);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn obs_cancel_returns_pending_credit_and_discards_controls_on_close() {
        for byte_limited in [false, true] {
            let owner = owner();
            let cancel = CancellationToken::new();
            let (tx, rx, observer) = observed_channel(
                MailboxConfig {
                    max_items: 1,
                    max_bytes: if byte_limited { 1 } else { 16 },
                },
                cancel.clone(),
                &owner,
            )
            .unwrap();
            tx.send_control(StreamControl::Idle { input: 0 })
                .await
                .unwrap();
            let tx2 = tx.clone();
            let pending =
                tokio::spawn(
                    async move { tx2.send_control(StreamControl::Active { input: 0 }).await },
                );
            waiting(&observer).await;
            cancel.cancel();
            assert!(!tokio::time::timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .unwrap());
            assert_eq!(observer.snapshot().enqueued_items_total, 1);
            assert_eq!(observer.snapshot().credits_reserved_bytes, 1);
            assert_eq!(observer.snapshot().completed_waits_total, 1);
            drop(rx);
            assert_eq!(observer.snapshot().discarded_on_close_total, 1);
            assert_eq!(observer.snapshot().queued.controls, 0);
            assert_eq!(observer.snapshot().credits_reserved_bytes, 0);
            drop(tx);
            drop(observer);
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn obs_multi_sender_fifo_ring_is_bounded_and_drains_exactly() {
        let owner = owner();
        let cfg = MailboxConfig {
            max_items: 4,
            max_bytes: 128,
        };
        let (tx, mut rx, observer) =
            observed_channel(cfg, CancellationToken::new(), &owner).unwrap();
        let capacity = observer.lock().stamps.capacity();
        let mut sends = Vec::new();
        for producer in 0..4 {
            let tx = tx.clone();
            let owner = owner.clone();
            sends.push(tokio::spawn(async move {
                for n in 0..250 {
                    if n % 7 == 0 {
                        tx.send_control(StreamControl::Idle { input: producer })
                            .await
                            .unwrap();
                    } else {
                        tx.send(batch(&owner, &[i64::from(producer) * 1000 + n]))
                            .await
                            .unwrap();
                    }
                }
            }));
        }
        drop(tx);
        let mut received = 0;
        let mut last = [-1i64; 4];
        while let Some(env) = rx.recv().await.unwrap() {
            received += 1;
            if let Some(b) = &env.batch {
                let Scalar::Int64(value) = b.rows()[0].values[0] else {
                    unreachable!()
                };
                let producer = (value / 1000) as usize;
                assert!(value > last[producer]);
                last[producer] = value;
            }
            let s = observer.snapshot();
            assert!(s.queued.items <= cfg.max_items);
            assert!(s.credits_reserved_bytes <= cfg.max_bytes);
            assert_eq!(
                s.enqueued_items_total,
                s.received_items_total + s.queued.items as u64
            );
        }
        for send in sends {
            send.await.unwrap();
        }
        assert_eq!(received, 1000);
        let s = observer.snapshot();
        assert_eq!(s.enqueued_items_total, 1000);
        assert_eq!(s.received_items_total, 1000);
        assert_eq!(s.aborted_sends_total, 0);
        assert_eq!(
            s.queued.items + s.consumer_held.items + s.waiting_senders,
            0
        );
        assert_eq!(s.credits_reserved_bytes, 0);
        assert!(s.peak_queued_items <= cfg.max_items);
        assert_eq!(observer.lock().stamps.capacity(), capacity);
        drop(rx);
        drop(observer);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn obs_shared_backing_is_counted_per_edge_not_as_two_allocations() {
        let owner = owner();
        let cfg = MailboxConfig {
            max_items: 2,
            max_bytes: 1024,
        };
        let (tx1, rx1, a) = observed_channel(cfg, CancellationToken::new(), &owner).unwrap();
        let (tx2, rx2, b) = observed_channel(cfg, CancellationToken::new(), &owner).unwrap();
        let batch = batch(&owner, &[1]);
        let bytes = batch.tracked_bytes();
        tx1.send(batch.share()).await.unwrap();
        tx2.send(batch).await.unwrap();
        assert_eq!(
            a.snapshot().queued.accounted_bytes + b.snapshot().queued.accounted_bytes,
            2 * bytes
        );
        assert_eq!(owner.usage().reservation_bytes, bytes);
        drop(rx1);
        assert_eq!(b.snapshot().queued.items, 1);
        drop(rx2);
        assert_eq!(owner.usage().reservation_bytes, 0);
        drop((tx1, tx2, a, b));
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn obs_metadata_admission_and_config_bounds_fail_before_allocation() {
        let cfg = MailboxConfig {
            max_items: 8,
            max_bytes: 1024,
        };
        let need = MailboxObserver::metadata_bytes(cfg).unwrap();
        let owner = MemoryOwner::new(ResourceBudget {
            queue_bytes: need - 1,
            ..ResourceBudget::compact()
        });
        assert!(observed_channel(cfg, CancellationToken::new(), &owner).is_err());
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(owner.usage().peak_physical_bytes, 0);
        for cfg in [
            MailboxConfig {
                max_items: 0,
                max_bytes: 1,
            },
            MailboxConfig {
                max_items: usize::MAX,
                max_bytes: 1,
            },
            MailboxConfig {
                max_items: 1,
                max_bytes: usize::MAX,
            },
        ] {
            assert!(channel(cfg, CancellationToken::new()).is_err());
        }
    }
}

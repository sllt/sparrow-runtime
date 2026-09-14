//! Per-mailbox accounting, not a global per-event registry. Queue metadata is
//! preallocated and billed before channel startup. Durations use process-local
//! monotonic time, never event time or a timestamp persisted in a checkpoint.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result, SparrowError};

use crate::mailbox::MailboxConfig;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueDepth {
    pub items: usize,
    pub data_batches: usize,
    pub rows: usize,
    pub controls: usize,
    /// Logical credit bytes: RowBatch::tracked_bytes().max(1), or 1/control.
    /// Shared backing is charged on each edge; this is NOT physical RAM/RSS.
    pub accounted_bytes: usize,
}

impl QueueDepth {
    pub(crate) fn add(&mut self, item: Self) {
        self.items += item.items;
        self.data_batches += item.data_batches;
        self.rows += item.rows;
        self.controls += item.controls;
        self.accounted_bytes += item.accounted_bytes;
    }

    /// Return an invariant violation without wrapping release-build gauges.
    pub(crate) fn remove(&mut self, item: Self) -> bool {
        let invalid = self.items < item.items
            || self.data_batches < item.data_batches
            || self.rows < item.rows
            || self.controls < item.controls
            || self.accounted_bytes < item.accounted_bytes;
        debug_assert!(!invalid, "mailbox observation depth underflow");
        self.items = self.items.saturating_sub(item.items);
        self.data_batches = self.data_batches.saturating_sub(item.data_batches);
        self.rows = self.rows.saturating_sub(item.rows);
        self.controls = self.controls.saturating_sub(item.controls);
        self.accounted_bytes = self.accounted_bytes.saturating_sub(item.accounted_bytes);
        invalid
    }
}

#[derive(Clone, Debug)]
pub struct MailboxSnapshot {
    /// Nonzero means the observation is invalid, not a trustworthy zero gauge.
    pub accounting_errors_total: u64,
    pub residence: sparrow_model::observation::HistogramSnapshot,
    pub max_items: usize,
    pub max_bytes: usize,
    pub metadata_bytes: usize,
    pub receiver_open: bool,
    pub queued: QueueDepth,
    /// Received envelopes still held by consumers. `take()` may move payload
    /// elsewhere; this is not a gauge of all operator state or processing work.
    pub consumer_held: QueueDepth,
    /// Includes pending senders, queued and held envelopes; NOT queue depth.
    pub credits_reserved_bytes: usize,
    pub peak_queued_items: usize,
    pub peak_queued_bytes: usize,
    pub oldest_queued_age_us: Option<u64>,
    pub oldest_queued_data_age_us: Option<u64>,
    pub send_attempts_total: u64,
    pub enqueued_items_total: u64,
    pub received_items_total: u64,
    pub discarded_on_close_total: u64,
    pub aborted_sends_total: u64,
    /// One count per send that actually encounters insufficient byte/item
    /// capacity, not per poll. Cancelled/closed sends also complete their wait.
    pub blocked_sends_total: u64,
    pub waiting_senders: usize,
    pub completed_waits_total: u64,
    pub completed_wait_us_total: u64,
    pub max_completed_wait_us: u64,
}

pub(crate) struct QueuedStamp {
    pub at: Instant,
    pub data: bool,
}

pub(crate) struct QueueState {
    pub snapshot: MailboxSnapshot,
    pub stamps: VecDeque<QueuedStamp>,
}

/// Read-only observer handle. Keeping this handle after a Job ends retains only
/// its bounded metadata credit, not data batches or a global history registry.
pub struct MailboxObserver {
    state: Mutex<QueueState>,
    _lease: MemoryLease,
}

impl MailboxObserver {
    pub fn metadata_bytes(cfg: MailboxConfig) -> Result<usize> {
        cfg.validate()?;
        // Include channel Envelope storage, not only the timestamp ring. The
        // fixed slack conservatively covers channel blocks and Kernel's held /
        // pending envelopes; allocator-exact RSS is not promised. Standalone
        // callers still own/budget any additional producer task/future storage.
        let channel = cfg
            .max_items
            .checked_add(64)
            .and_then(|n| n.checked_mul(std::mem::size_of::<crate::mailbox::Envelope>() + 64));
        cfg.max_items
            .checked_mul(std::mem::size_of::<QueuedStamp>())
            .and_then(|n| n.checked_add(channel?))
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>() + 512))
            .ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "mailbox observation metadata overflow",
                )
            })
    }

    pub(crate) fn new(cfg: MailboxConfig, owner: &Arc<MemoryOwner>) -> Result<Arc<Self>> {
        let bytes = Self::metadata_bytes(cfg)?;
        let lease = owner.acquire(CreditKind::Queue, bytes)?;
        Self::with_lease(cfg, lease)
    }

    pub(crate) fn with_lease(cfg: MailboxConfig, lease: MemoryLease) -> Result<Arc<Self>> {
        let bytes = Self::metadata_bytes(cfg)?;
        // The constant allowance also covers the Arc and per-job edge/view
        // bookkeeping. No strings/device labels or per-send allocations here.
        let stamps = VecDeque::with_capacity(cfg.max_items);
        Ok(Arc::new(Self {
            state: Mutex::new(QueueState {
                snapshot: MailboxSnapshot {
                    accounting_errors_total: 0,
                    residence: sparrow_model::observation::HistogramSnapshot::default(),
                    max_items: cfg.max_items,
                    max_bytes: cfg.max_bytes,
                    metadata_bytes: bytes,
                    receiver_open: true,
                    queued: QueueDepth::default(),
                    consumer_held: QueueDepth::default(),
                    credits_reserved_bytes: 0,
                    peak_queued_items: 0,
                    peak_queued_bytes: 0,
                    oldest_queued_age_us: None,
                    oldest_queued_data_age_us: None,
                    send_attempts_total: 0,
                    enqueued_items_total: 0,
                    received_items_total: 0,
                    discarded_on_close_total: 0,
                    aborted_sends_total: 0,
                    blocked_sends_total: 0,
                    waiting_senders: 0,
                    completed_waits_total: 0,
                    completed_wait_us_total: 0,
                    max_completed_wait_us: 0,
                },
                stamps,
            }),
            _lease: lease,
        }))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub fn snapshot(&self) -> MailboxSnapshot {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&self, now: Instant) -> MailboxSnapshot {
        let state = self.lock();
        let mut s = state.snapshot.clone();
        s.oldest_queued_age_us = state.stamps.front().map(|stamp| elapsed_us(stamp.at, now));
        s.oldest_queued_data_age_us = state
            .stamps
            .iter()
            .find(|stamp| stamp.data)
            .map(|stamp| elapsed_us(stamp.at, now));
        s
    }
}

#[derive(Clone, Debug)]
pub struct EdgeMailboxSnapshot {
    pub edge_index: usize,
    pub from_stage: usize,
    pub to_stage: usize,
    pub from_kind: &'static str,
    pub to_kind: &'static str,
    pub queue: MailboxSnapshot,
}

#[derive(Clone, Debug)]
pub struct JobMailboxSnapshot {
    pub pipeline_id: u64,
    pub plan_revision: u64,
    /// Kernel execution ID, NOT the catalog's lifecycle-transition counter.
    pub runtime_attempt_id: u64,
    pub initialized: bool,
    pub edges: Vec<EdgeMailboxSnapshot>,
}

struct EdgeObserver {
    from_kind: &'static str,
    to_kind: &'static str,
    observer: Arc<MailboxObserver>,
}

pub struct JobMailboxObserver {
    pipeline_id: u64,
    plan_revision: u64,
    runtime_attempt_id: u64,
    initialized: std::sync::atomic::AtomicBool,
    edges: Vec<EdgeObserver>,
}

impl JobMailboxObserver {
    pub(crate) fn metadata_bytes(cfg: MailboxConfig, edges: usize) -> Result<usize> {
        MailboxObserver::metadata_bytes(cfg)?
            .checked_mul(edges)
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::BoundExceeded, "job mailbox metadata overflow")
            })
    }

    pub(crate) fn new(
        plan: &sparrow_plan::PhysicalPlan,
        attempt: u64,
        cfg: MailboxConfig,
        owner: &Arc<MemoryOwner>,
    ) -> Result<Arc<Self>> {
        let bytes = Self::metadata_bytes(cfg, plan.mailbox_count())?;
        // Reserve the complete metadata pool before Vec/Arc/ring allocation.
        // Each edge shares its owner; keeping a snapshot HANDLE pins this bounded
        // pool, never a batch. Snapshot VALUES contain no owner references.
        let lease = owner.acquire(CreditKind::Queue, bytes)?;
        let mut edges = Vec::with_capacity(plan.mailbox_count());
        for pair in plan.stages.windows(2) {
            edges.push(EdgeObserver {
                from_kind: stage_kind(&pair[0]),
                to_kind: stage_kind(&pair[1]),
                observer: MailboxObserver::with_lease(cfg, lease.share())?,
            });
        }
        Ok(Arc::new(Self {
            pipeline_id: plan.pipeline.raw(),
            plan_revision: plan.revision.raw(),
            runtime_attempt_id: attempt,
            initialized: std::sync::atomic::AtomicBool::new(false),
            edges,
        }))
    }

    pub(crate) fn edge(&self, index: usize) -> Arc<MailboxObserver> {
        self.edges[index].observer.clone()
    }
    pub(crate) fn initialized(&self) {
        self.initialized
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn snapshot(&self) -> JobMailboxSnapshot {
        let now = Instant::now();
        JobMailboxSnapshot {
            pipeline_id: self.pipeline_id,
            plan_revision: self.plan_revision,
            runtime_attempt_id: self.runtime_attempt_id,
            initialized: self.initialized.load(std::sync::atomic::Ordering::Acquire),
            edges: self
                .edges
                .iter()
                .enumerate()
                .map(|(i, edge)| EdgeMailboxSnapshot {
                    edge_index: i,
                    from_stage: i,
                    to_stage: i + 1,
                    from_kind: edge.from_kind,
                    to_kind: edge.to_kind,
                    queue: edge.observer.snapshot_at(now),
                })
                .collect(),
        }
    }
}

fn stage_kind(stage: &sparrow_plan::PhysicalStage) -> &'static str {
    use sparrow_plan::PhysicalStage;
    match stage {
        PhysicalStage::MemorySource { .. } => "source",
        PhysicalStage::Transform { .. } => "transform",
        PhysicalStage::CaptureSink { .. } => "sink",
        PhysicalStage::WindowAgg { .. } => "window",
        PhysicalStage::Deduplicate { .. } => "deduplicate",
        PhysicalStage::Lookup { .. } => "lookup",
    }
}

pub(crate) fn elapsed_us(start: Instant, now: Instant) -> u64 {
    now.saturating_duration_since(start)
        .as_micros()
        .min(u64::MAX as u128) as u64
}

pub(crate) struct SendObservation {
    observer: Option<Arc<MailboxObserver>>,
    blocked_at: Option<Instant>,
    finished: bool,
}

impl SendObservation {
    pub fn new(observer: Option<Arc<MailboxObserver>>) -> Self {
        if let Some(observer) = &observer {
            let mut state = observer.lock();
            state.snapshot.send_attempts_total =
                state.snapshot.send_attempts_total.saturating_add(1);
        }
        Self {
            observer,
            blocked_at: None,
            finished: false,
        }
    }

    pub fn blocked(&mut self) {
        if self.blocked_at.is_some() {
            return;
        }
        if let Some(observer) = &self.observer {
            let mut state = observer.lock();
            state.snapshot.blocked_sends_total =
                state.snapshot.blocked_sends_total.saturating_add(1);
            state.snapshot.waiting_senders += 1;
            self.blocked_at = Some(Instant::now());
        }
    }

    pub fn finish(&mut self, state: &mut QueueState, now: Instant, sent: bool) {
        if let Some(start) = self.blocked_at.take() {
            let duration = elapsed_us(start, now);
            state.snapshot.waiting_senders -= 1;
            state.snapshot.completed_waits_total =
                state.snapshot.completed_waits_total.saturating_add(1);
            state.snapshot.completed_wait_us_total = state
                .snapshot
                .completed_wait_us_total
                .saturating_add(duration);
            state.snapshot.max_completed_wait_us =
                state.snapshot.max_completed_wait_us.max(duration);
        }
        if !sent {
            state.snapshot.aborted_sends_total =
                state.snapshot.aborted_sends_total.saturating_add(1);
        }
        self.finished = true;
    }
}

impl Drop for SendObservation {
    fn drop(&mut self) {
        if !self.finished {
            if let Some(observer) = self.observer.clone() {
                self.finish(&mut observer.lock(), Instant::now(), false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "mailbox observation depth underflow")
    )]
    fn r9_depth_underflow_never_wraps_release_gauges() {
        let mut depth = QueueDepth::default();
        assert!(depth.remove(QueueDepth {
            items: 1,
            rows: 2,
            accounted_bytes: 8,
            ..QueueDepth::default()
        }));
        assert_eq!(depth, QueueDepth::default());
    }

    #[test]
    fn obs_age_is_monotonic_queue_residence_not_event_time() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let o = MailboxObserver::new(MailboxConfig::default(), &owner).unwrap();
        let start = Instant::now();
        {
            let mut state = o.lock();
            state.stamps.push_back(QueuedStamp {
                at: start,
                data: false,
            });
            state.stamps.push_back(QueuedStamp {
                at: start + std::time::Duration::from_micros(10),
                data: true,
            });
        }
        let s = o.snapshot_at(start + std::time::Duration::from_micros(25));
        assert_eq!(s.oldest_queued_age_us, Some(25));
        assert_eq!(s.oldest_queued_data_age_us, Some(15));
        o.lock().stamps.clear();
        let s = o.snapshot_at(start + std::time::Duration::from_secs(1));
        assert_eq!(s.oldest_queued_age_us, None);
        assert_eq!(s.oldest_queued_data_age_us, None);
        drop(o);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

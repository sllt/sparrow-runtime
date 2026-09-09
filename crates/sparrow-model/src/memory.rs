//! Tracked allocation owner and [`MemoryLease`].
//!
//! Physical bytes are counted once per allocation. [`MemoryLease::share`]
//! adds a handle without a new alloc. [`MemoryLease::detach`] copies and
//! creates a new physical allocation (typically on the retention ledger).
//! Drop / [`MemoryLease::release`] returns credits when the last handle dies.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{ErrorCode, Result, SparrowError};
use crate::resource::{CreditKind, CreditUsage, ResourceBudget};

#[derive(Debug)]
struct AllocInner {
    id: u64,
    bytes: usize,
    kind: CreditKind,
    handles: AtomicUsize,
}

/// Process-local owner of tracked allocations for one job attempt.
#[derive(Debug)]
pub struct MemoryOwner {
    budget: ResourceBudget,
    next_id: AtomicU64,
    reservation: AtomicUsize,
    retention: AtomicUsize,
    queue: AtomicUsize,
    physical: AtomicUsize,
    peak_physical: AtomicUsize,
    live_handles: AtomicUsize,
    /// Expansion watermark observed by builders (not silent growth).
    peak_builder_bytes: AtomicUsize,
    /// Serializes credit checks so two acquires cannot oversubscribe.
    lock: Mutex<()>,
}

impl MemoryOwner {
    pub fn new(budget: ResourceBudget) -> Arc<Self> {
        Arc::new(Self {
            budget,
            next_id: AtomicU64::new(1),
            reservation: AtomicUsize::new(0),
            retention: AtomicUsize::new(0),
            queue: AtomicUsize::new(0),
            physical: AtomicUsize::new(0),
            peak_physical: AtomicUsize::new(0),
            live_handles: AtomicUsize::new(0),
            peak_builder_bytes: AtomicUsize::new(0),
            lock: Mutex::new(()),
        })
    }

    pub fn budget(&self) -> ResourceBudget {
        self.budget
    }

    pub fn usage(&self) -> CreditUsage {
        CreditUsage {
            reservation_bytes: self.reservation.load(Ordering::SeqCst),
            retention_bytes: self.retention.load(Ordering::SeqCst),
            queue_bytes: self.queue.load(Ordering::SeqCst),
            physical_bytes: self.physical.load(Ordering::SeqCst),
            peak_physical_bytes: self.peak_physical.load(Ordering::SeqCst),
            live_handles: self.live_handles.load(Ordering::SeqCst),
        }
    }

    pub fn peak_builder_bytes(&self) -> usize {
        self.peak_builder_bytes.load(Ordering::SeqCst)
    }

    pub(crate) fn note_builder_peak(&self, bytes: usize) {
        self.peak_builder_bytes.fetch_max(bytes, Ordering::SeqCst);
    }

    fn ledger(&self, kind: CreditKind) -> &AtomicUsize {
        match kind {
            CreditKind::Reservation => &self.reservation,
            CreditKind::Retention => &self.retention,
            CreditKind::Queue => &self.queue,
        }
    }

    /// Acquire a new physical allocation and draw `kind` credits.
    pub fn acquire(self: &Arc<Self>, kind: CreditKind, bytes: usize) -> Result<MemoryLease> {
        if bytes == 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "cannot acquire a zero-byte lease",
            ));
        }
        let _gate = self.lock.lock().expect("memory owner lock");
        let used = self.ledger(kind).load(Ordering::SeqCst);
        let cap = self.budget.cap(kind);
        if used.saturating_add(bytes) > cap {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "{kind} credits exhausted: used={used} request={bytes} cap={cap}",
                    kind = kind.as_str()
                ),
            )
            .retryable(true)
            .context("credit", kind.as_str())
            .context("used", used.to_string())
            .context("request", bytes.to_string())
            .context("cap", cap.to_string()));
        }
        self.ledger(kind).fetch_add(bytes, Ordering::SeqCst);
        let physical = self.physical.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.peak_physical.fetch_max(physical, Ordering::SeqCst);
        self.live_handles.fetch_add(1, Ordering::SeqCst);
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        Ok(MemoryLease {
            owner: Arc::clone(self),
            alloc: Arc::new(AllocInner {
                id,
                bytes,
                kind,
                handles: AtomicUsize::new(1),
            }),
        })
    }
}

/// Handle to a tracked allocation. Clone is deliberately not implemented —
/// use [`share`](Self::share) so the intent is explicit.
#[derive(Debug)]
pub struct MemoryLease {
    owner: Arc<MemoryOwner>,
    alloc: Arc<AllocInner>,
}

impl MemoryLease {
    pub fn alloc_id(&self) -> u64 {
        self.alloc.id
    }

    pub fn bytes(&self) -> usize {
        self.alloc.bytes
    }

    pub fn kind(&self) -> CreditKind {
        self.alloc.kind
    }

    pub fn refcount(&self) -> usize {
        self.alloc.handles.load(Ordering::SeqCst)
    }

    pub fn owner(&self) -> &Arc<MemoryOwner> {
        &self.owner
    }

    /// Additional handle on the *same* physical allocation.
    pub fn share(&self) -> MemoryLease {
        self.alloc.handles.fetch_add(1, Ordering::SeqCst);
        self.owner.live_handles.fetch_add(1, Ordering::SeqCst);
        MemoryLease {
            owner: Arc::clone(&self.owner),
            alloc: Arc::clone(&self.alloc),
        }
    }

    /// Copy: new physical allocation, typically for long-lived state.
    pub fn detach(&self, kind: CreditKind) -> Result<MemoryLease> {
        self.owner.acquire(kind, self.alloc.bytes)
    }

    /// Explicit release. Equivalent to drop.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        self.owner.live_handles.fetch_sub(1, Ordering::SeqCst);
        let previous = self.alloc.handles.fetch_sub(1, Ordering::SeqCst);
        if previous == 1 {
            let bytes = self.alloc.bytes;
            self.owner
                .ledger(self.alloc.kind)
                .fetch_sub(bytes, Ordering::SeqCst);
            self.owner.physical.fetch_sub(bytes, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget {
            reservation_bytes: 1024,
            retention_bytes: 1024,
            queue_bytes: 1024,
            max_rows: 16,
            work_units: 100,
        })
    }

    #[test]
    fn lease_returns_on_drop() {
        let pool = owner();
        {
            let lease = pool.acquire(CreditKind::Reservation, 128).unwrap();
            assert_eq!(pool.usage().reservation_bytes, 128);
            assert_eq!(pool.usage().physical_bytes, 128);
            assert_eq!(pool.usage().live_handles, 1);
            lease.release();
        }
        let usage = pool.usage();
        assert_eq!(usage.reservation_bytes, 0);
        assert_eq!(usage.physical_bytes, 0);
        assert_eq!(usage.live_handles, 0);
        assert_eq!(usage.peak_physical_bytes, 128);
    }

    #[test]
    fn share_counts_once_physically() {
        let pool = owner();
        let a = pool.acquire(CreditKind::Queue, 200).unwrap();
        let b = a.share();
        let c = b.share();
        assert_eq!(a.alloc_id(), b.alloc_id());
        assert_eq!(a.alloc_id(), c.alloc_id());
        assert_eq!(a.refcount(), 3);
        let usage = pool.usage();
        assert_eq!(usage.physical_bytes, 200);
        assert_eq!(usage.queue_bytes, 200);
        assert_eq!(usage.live_handles, 3);
        drop(a);
        drop(b);
        assert_eq!(pool.usage().physical_bytes, 200);
        drop(c);
        assert_eq!(pool.usage().physical_bytes, 0);
        assert_eq!(pool.usage().queue_bytes, 0);
    }

    #[test]
    fn detach_creates_new_physical_alloc() {
        let pool = owner();
        let live = pool.acquire(CreditKind::Reservation, 64).unwrap();
        let state = live.detach(CreditKind::Retention).unwrap();
        assert_ne!(live.alloc_id(), state.alloc_id());
        let usage = pool.usage();
        assert_eq!(usage.physical_bytes, 128);
        assert_eq!(usage.reservation_bytes, 64);
        assert_eq!(usage.retention_bytes, 64);
        drop(live);
        assert_eq!(pool.usage().physical_bytes, 64);
        assert_eq!(pool.usage().reservation_bytes, 0);
        drop(state);
        assert_eq!(pool.usage().physical_bytes, 0);
    }

    #[test]
    fn acquire_is_bounded() {
        let pool = owner();
        let _held = pool.acquire(CreditKind::Reservation, 1000).unwrap();
        let err = pool.acquire(CreditKind::Reservation, 25).unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        assert!(err.retryable);
    }
}

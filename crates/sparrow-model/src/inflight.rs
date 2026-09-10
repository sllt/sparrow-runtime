//! Process-visible in-flight counter for sink flush alignment.
//!
//! Enqueue **before** a batch is handed to a sink. `ack` after a successful
//! transport write. `fail` after a drop / 4xx / retries exhausted. A checkpoint
//! barrier waits until `pending() == 0`, then refuses commit if any `fail`
//! landed since the last barrier.

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared sent/acked/failed counter. Not a channel-empty check.
#[derive(Debug, Default)]
pub struct InflightCounter {
    sent: AtomicU64,
    acked: AtomicU64,
    failed: AtomicU64,
}

impl InflightCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a batch has been handed to the sink path.
    pub fn enqueue(&self) {
        self.sent.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that the sink successfully flushed this batch.
    pub fn ack(&self) {
        self.acked.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that the sink dropped this batch (4xx, encode, retries exhausted).
    /// Resolves in-flight so the barrier can observe the failure instead of hanging.
    pub fn fail(&self) {
        self.failed.fetch_add(1, Ordering::SeqCst);
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    pub fn acked(&self) -> u64 {
        self.acked.load(Ordering::SeqCst)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::SeqCst)
    }

    /// Unresolved batches. Barrier commit is dishonest while this is > 0.
    pub fn pending(&self) -> u64 {
        self.sent()
            .saturating_sub(self.acked().saturating_add(self.failed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_tracks_unacked() {
        let c = InflightCounter::new();
        c.enqueue();
        c.enqueue();
        assert_eq!(c.pending(), 2);
        c.ack();
        assert_eq!(c.pending(), 1);
        c.ack();
        assert_eq!(c.pending(), 0);
    }

    #[test]
    fn fail_resolves_pending_but_is_not_success() {
        let c = InflightCounter::new();
        c.enqueue();
        c.fail();
        assert_eq!(c.pending(), 0);
        assert_eq!(c.failed(), 1);
        assert_eq!(c.acked(), 0);
    }
}

//! Per-poll work quantum. Compact / Performance only change the cap.
//!
//! `WorkBudget` is **not** a lifetime job event cap. Long-running live jobs
//! call [`WorkBudget::begin_quantum`] at the start of each poll / batch so
//! cumulative events cannot kill the job. A separate optional lifetime cap
//! exists only for finite preview / test runs.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ErrorCode, Result, SparrowError};

/// Per-poll kernel-step quantum. Exhaustion of the current quantum is an
/// error for that poll, not a silent wrap. Lifetime exhaustion is opt-in.
#[derive(Debug)]
pub struct WorkBudget {
    remaining: AtomicU64,
    quantum: u64,
    lifetime_left: AtomicU64,
}

impl WorkBudget {
    /// Create a live budget. `quantum` is replenished on each [`Self::begin_quantum`].
    /// Lifetime is unlimited (`u64::MAX`).
    pub fn new(quantum: u64) -> Self {
        Self {
            remaining: AtomicU64::new(quantum),
            quantum,
            lifetime_left: AtomicU64::new(u64::MAX),
        }
    }

    /// Preview-only total cap. Exhausting it fails the job (finite demos).
    pub fn with_lifetime_cap(self, cap: u64) -> Self {
        self.lifetime_left.store(cap, Ordering::SeqCst);
        self
    }

    pub fn cap(&self) -> u64 {
        self.quantum
    }

    pub fn remaining(&self) -> u64 {
        self.remaining.load(Ordering::SeqCst)
    }

    /// True when `consume(units)` would fail the current quantum.
    /// Stages should yield, then [`Self::begin_quantum`], then consume.
    pub fn would_exhaust(&self, units: u64) -> bool {
        units > 0 && self.remaining() < units
    }

    /// Replenish the per-poll quantum. Live stages must call this each batch.
    pub fn begin_quantum(&self) {
        self.remaining.store(self.quantum, Ordering::SeqCst);
    }

    pub fn consume(&self, units: u64) -> Result<()> {
        if units == 0 {
            return Ok(());
        }
        loop {
            let life = self.lifetime_left.load(Ordering::SeqCst);
            if life != u64::MAX && life < units {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "preview work lifetime exhausted: left={life} request={units}"
                    ),
                )
                .retryable(false)
                .context("work_lifetime", life.to_string()));
            }
            let left = self.remaining.load(Ordering::SeqCst);
            if left < units {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "work quantum exhausted: left={left} request={units} quantum={}",
                        self.quantum
                    ),
                )
                .retryable(true)
                .context("work_left", left.to_string())
                .context("work_quantum", self.quantum.to_string()));
            }
            if self
                .remaining
                .compare_exchange(left, left - units, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                if life != u64::MAX {
                    self.lifetime_left.fetch_sub(units, Ordering::SeqCst);
                }
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_overspend() {
        let b = WorkBudget::new(3);
        b.consume(2).unwrap();
        assert_eq!(b.remaining(), 1);
        assert_eq!(b.consume(2).unwrap_err().code, ErrorCode::ResourceExhausted);
    }

    #[test]
    fn r02_quantum_replenish_does_not_kill_after_old_cap() {
        let b = WorkBudget::new(3);
        b.consume(3).unwrap();
        assert_eq!(
            b.consume(1).unwrap_err().code,
            ErrorCode::ResourceExhausted
        );
        b.begin_quantum();
        b.consume(3).unwrap();
        b.begin_quantum();
        b.consume(3).unwrap();
        assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn r02_preview_lifetime_still_caps_finite_runs() {
        let b = WorkBudget::new(10).with_lifetime_cap(4);
        b.consume(3).unwrap();
        b.begin_quantum();
        assert_eq!(
            b.consume(2).unwrap_err().code,
            ErrorCode::ResourceExhausted
        );
    }
}

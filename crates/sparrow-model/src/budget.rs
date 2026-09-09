//! Consumable work budget. Compact / Performance only change the cap.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ErrorCode, Result, SparrowError};

/// Per-job kernel-step budget. Exhaustion is an error, not silent wrap.
#[derive(Debug)]
pub struct WorkBudget {
    remaining: AtomicU64,
    cap: u64,
}

impl WorkBudget {
    pub fn new(cap: u64) -> Self {
        Self {
            remaining: AtomicU64::new(cap),
            cap,
        }
    }

    pub fn cap(&self) -> u64 {
        self.cap
    }

    pub fn remaining(&self) -> u64 {
        self.remaining.load(Ordering::SeqCst)
    }

    pub fn consume(&self, units: u64) -> Result<()> {
        if units == 0 {
            return Ok(());
        }
        loop {
            let left = self.remaining.load(Ordering::SeqCst);
            if left < units {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    format!("work budget exhausted: left={left} request={units} cap={}", self.cap),
                )
                .retryable(true)
                .context("work_left", left.to_string())
                .context("work_cap", self.cap.to_string()));
            }
            if self
                .remaining
                .compare_exchange(left, left - units, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
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
}

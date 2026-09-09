//! Process-time clock. Tests inject [`SharedVirtualClock`]; production uses
//! [`WallClock`]. The trait is `Send + Sync` so kernel tasks can share it.
//!
//! Processing-time windows read this clock. They are **not** event-time and
//! are not crash-identical after restart (`recovery=none`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Microseconds since Unix epoch (or a virtual origin).
pub trait InstantSource: Send + Sync {
    fn now_micros(&self) -> i64;
}

/// Host wall clock. Used by live jobs.
#[derive(Clone, Copy, Debug, Default)]
pub struct WallClock;

impl InstantSource for WallClock {
    fn now_micros(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }
}

/// Explicitly advanced clock. Kernel timers observe the same instant.
#[derive(Clone, Debug)]
pub struct SharedVirtualClock {
    now: Arc<AtomicI64>,
}

impl SharedVirtualClock {
    pub fn new(start_micros: i64) -> Self {
        Self {
            now: Arc::new(AtomicI64::new(start_micros)),
        }
    }

    pub fn set(&self, now: i64) {
        self.now.store(now, Ordering::SeqCst);
    }

    pub fn advance_micros(&self, delta: i64) {
        self.now.fetch_add(delta, Ordering::SeqCst);
    }
}

impl InstantSource for SharedVirtualClock {
    fn now_micros(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}

impl Default for SharedVirtualClock {
    fn default() -> Self {
        Self::new(1_700_000_000_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_clock_is_deterministic() {
        let clock = SharedVirtualClock::new(100);
        assert_eq!(clock.now_micros(), 100);
        clock.advance_micros(50);
        let alias = clock.clone();
        assert_eq!(alias.now_micros(), 150);
    }
}

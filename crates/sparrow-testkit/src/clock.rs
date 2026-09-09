//! Virtual clock hooks. Tests must not depend on wall time.

use std::cell::Cell;
use std::rc::Rc;

/// Microseconds since Unix epoch.
pub trait Clock {
    fn now_micros(&self) -> i64;
}

/// Monotonic, explicitly advanced clock for deterministic tests.
#[derive(Clone, Debug)]
pub struct VirtualClock {
    inner: Rc<Cell<i64>>,
}

impl VirtualClock {
    pub fn new(start_micros: i64) -> Self {
        Self {
            inner: Rc::new(Cell::new(start_micros)),
        }
    }

    pub fn advance_micros(&self, delta: i64) {
        self.inner.set(self.inner.get().saturating_add(delta));
    }

    pub fn set(&self, now: i64) {
        self.inner.set(now);
    }
}

impl Clock for VirtualClock {
    fn now_micros(&self) -> i64 {
        self.inner.get()
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new(1_700_000_000_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_clock_is_deterministic() {
        let clock = VirtualClock::new(100);
        assert_eq!(clock.now_micros(), 100);
        clock.advance_micros(50);
        let alias = clock.clone();
        assert_eq!(alias.now_micros(), 150);
    }
}

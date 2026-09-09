//! Kernel clock: wall time or a shared virtual clock with wakeup.

use std::sync::Arc;
use std::time::Duration;

use sparrow_model::{InstantSource, SharedVirtualClock, WallClock};
use tokio::sync::Notify;

/// Clock injected into a job. Virtual clocks wake timer waits on `advance`.
#[derive(Clone)]
pub struct RuntimeClock {
    inner: ClockInner,
}

#[derive(Clone)]
enum ClockInner {
    Wall,
    Virtual {
        clock: SharedVirtualClock,
        wake: Arc<Notify>,
    },
}

impl RuntimeClock {
    pub fn wall() -> Self {
        Self {
            inner: ClockInner::Wall,
        }
    }

    pub fn virtual_clock(clock: SharedVirtualClock) -> Self {
        Self {
            inner: ClockInner::Virtual {
                clock,
                wake: Arc::new(Notify::new()),
            },
        }
    }

    pub fn is_virtual(&self) -> bool {
        matches!(self.inner, ClockInner::Virtual { .. })
    }

    pub fn now_micros(&self) -> i64 {
        match &self.inner {
            ClockInner::Wall => WallClock.now_micros(),
            ClockInner::Virtual { clock, .. } => clock.now_micros(),
        }
    }

    pub fn set_virtual(&self, now: i64) {
        if let ClockInner::Virtual { clock, wake } = &self.inner {
            clock.set(now);
            wake.notify_waiters();
        }
    }

    pub fn advance_virtual(&self, delta: i64) {
        if let ClockInner::Virtual { clock, wake } = &self.inner {
            clock.advance_micros(delta);
            wake.notify_waiters();
        }
    }

    pub fn notify(&self) {
        if let ClockInner::Virtual { wake, .. } = &self.inner {
            wake.notify_waiters();
        }
    }

    /// Wait until `deadline` (inclusive) or forever if `None`.
    pub async fn sleep_until(&self, deadline: Option<i64>) {
        match &self.inner {
            ClockInner::Wall => match deadline {
                Some(d) => {
                    let now = WallClock.now_micros();
                    if d > now {
                        tokio::time::sleep(Duration::from_micros((d - now) as u64)).await;
                    }
                }
                None => std::future::pending::<()>().await,
            },
            ClockInner::Virtual { clock, wake } => loop {
                if let Some(d) = deadline {
                    if clock.now_micros() >= d {
                        return;
                    }
                }
                wake.notified().await;
                if deadline.is_none() {
                    return;
                }
            },
        }
    }
}

impl InstantSource for RuntimeClock {
    fn now_micros(&self) -> i64 {
        RuntimeClock::now_micros(self)
    }
}

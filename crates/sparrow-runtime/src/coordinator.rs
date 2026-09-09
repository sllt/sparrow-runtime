//! Checkpoint coordinator: timeout / abort / stop arbitration.
//!
//! A checkpoint must not remain stuck in `Checkpointing`. Stop and abort
//! win over an in-flight barrier. Timeouts leave the store uncommitted.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sparrow_model::{ErrorCode, Result, SparrowError};

/// Coordinator phase. `Checkpointing` is transient and always resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointPhase {
    Idle,
    Checkpointing,
    Committed,
    Aborted,
    TimedOut,
    Stopped,
}

/// Single-job aligned checkpoint coordinator (not distributed).
pub struct CheckpointCoordinator {
    phase: CheckpointPhase,
    timeout: Duration,
    started: Option<Instant>,
    abort: AtomicBool,
    stop: AtomicBool,
}

impl CheckpointCoordinator {
    pub fn new(timeout: Duration) -> Self {
        Self {
            phase: CheckpointPhase::Idle,
            timeout,
            started: None,
            abort: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        }
    }

    pub fn with_default_timeout() -> Self {
        Self::new(Duration::from_secs(5))
    }

    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    pub fn request_abort(&self) {
        self.abort.store(true, Ordering::SeqCst);
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.abort.store(true, Ordering::SeqCst);
    }

    pub fn is_stop_requested(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Enter Checkpointing. Fails if already checkpointing, stopped, or aborted.
    pub fn begin(&mut self) -> Result<()> {
        self.arbitrate()?;
        if self.phase == CheckpointPhase::Checkpointing {
            return Err(self.fail_stuck("already Checkpointing; refusing a second barrier"));
        }
        self.phase = CheckpointPhase::Checkpointing;
        self.started = Some(Instant::now());
        Ok(())
    }

    /// Leave Checkpointing after a successful commit.
    pub fn complete(&mut self) -> Result<CheckpointPhase> {
        self.arbitrate()?;
        if self.phase != CheckpointPhase::Checkpointing {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                format!("complete() in phase {:?}", self.phase),
            ));
        }
        self.phase = CheckpointPhase::Committed;
        self.started = None;
        Ok(self.phase)
    }

    /// Leave Checkpointing without committing (caller already failed or aborted).
    pub fn abort_now(&mut self, reason: &str) -> CheckpointPhase {
        let _ = reason;
        if self.stop.load(Ordering::SeqCst) {
            self.phase = CheckpointPhase::Stopped;
        } else {
            self.phase = CheckpointPhase::Aborted;
        }
        self.started = None;
        self.phase
    }

    /// Poll timeout / stop / abort. Must be called during a barrier.
    pub fn arbitrate(&mut self) -> Result<()> {
        if self.stop.load(Ordering::SeqCst) {
            if self.phase == CheckpointPhase::Checkpointing {
                self.phase = CheckpointPhase::Stopped;
                self.started = None;
            }
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "checkpoint aborted: stop requested (no stuck Checkpointing)",
            )
            .context("phase", format!("{:?}", self.phase)));
        }
        if self.abort.load(Ordering::SeqCst) && self.phase == CheckpointPhase::Checkpointing {
            self.phase = CheckpointPhase::Aborted;
            self.started = None;
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "checkpoint aborted by coordinator",
            ));
        }
        if self.phase == CheckpointPhase::Checkpointing {
            if let Some(t0) = self.started {
                if t0.elapsed() > self.timeout {
                    self.phase = CheckpointPhase::TimedOut;
                    self.started = None;
                    return Err(SparrowError::new(
                        ErrorCode::Cancelled,
                        format!(
                            "checkpoint timed out after {:?}; leaving Checkpointing",
                            self.timeout
                        ),
                    )
                    .context("phase", "TimedOut"));
                }
            }
        }
        Ok(())
    }

    fn fail_stuck(&mut self, msg: &str) -> SparrowError {
        self.phase = CheckpointPhase::Aborted;
        self.started = None;
        SparrowError::new(ErrorCode::Internal, msg.to_string()).context("phase", "Aborted")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_wins_over_checkpointing() {
        let mut c = CheckpointCoordinator::new(Duration::from_secs(5));
        c.begin().unwrap();
        assert_eq!(c.phase(), CheckpointPhase::Checkpointing);
        c.request_stop();
        let err = c.arbitrate().unwrap_err();
        assert_eq!(err.code, ErrorCode::Cancelled);
        assert_eq!(c.phase(), CheckpointPhase::Stopped);
    }

    #[test]
    fn timeout_leaves_checkpointing() {
        let mut c = CheckpointCoordinator::new(Duration::from_millis(1));
        c.begin().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let err = c.arbitrate().unwrap_err();
        assert_eq!(err.code, ErrorCode::Cancelled);
        assert_eq!(c.phase(), CheckpointPhase::TimedOut);
    }

    #[test]
    fn complete_returns_to_committed() {
        let mut c = CheckpointCoordinator::with_default_timeout();
        c.begin().unwrap();
        c.complete().unwrap();
        assert_eq!(c.phase(), CheckpointPhase::Committed);
    }
}

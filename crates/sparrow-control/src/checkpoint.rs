//! Per-attempt checkpoint policy/observation. One request owns admission until
//! the source actor really finishes, even if an HTTP waiter times out.
use serde::{Deserialize, Serialize};
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckpointSpec {
    /// None means manual checkpoints only. No implicit automatic replay.
    pub interval_ms: Option<u64>,
    pub timeout_ms: u64,
    pub retain_generations: usize,
    pub max_store_bytes: u64,
    /// Explicitly resume the latest committed point on a start/restart.
    /// Only a genuinely empty store may start fresh under this policy.
    pub resume_latest: bool,
}
impl Default for CheckpointSpec {
    fn default() -> Self {
        Self {
            interval_ms: None,
            timeout_ms: 5000,
            retain_generations: 3,
            max_store_bytes: sparrow_runtime::checkpoint::MAX_STORE_BYTES,
            resume_latest: false,
        }
    }
}
impl CheckpointSpec {
    pub fn validate(&self) -> Result<()> {
        if self
            .interval_ms
            .is_some_and(|ms| !(100..=86_400_000).contains(&ms))
            || !(100..=120_000).contains(&self.timeout_ms)
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint interval must be 100..=86400000 ms; timeout 100..=120000 ms",
            ));
        }
        self.retention().validate()
    }
    pub fn retention(&self) -> sparrow_runtime::checkpoint::CheckpointRetention {
        sparrow_runtime::checkpoint::CheckpointRetention {
            generations: self.retain_generations,
            max_bytes: self.max_store_bytes,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CheckpointStatus {
    pub active: bool,
    pub started: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub busy: u64,
    pub timed_out_waiters: u64,
    pub last_trigger: Option<&'static str>,
    pub phase: &'static str,
    pub last_error: Option<ErrorCode>,
    pub last_success_id: Option<u64>,
    pub restored_from: Option<u64>,
    pub state_generation: Option<[u8;16]>,
    pub downstream_semantics_changed: bool,
    pub reliable_source: Option<ReliableSourceStatus>,
    pub last_success_at: Option<tokio::time::Instant>,
    pub storage: Option<sparrow_runtime::checkpoint::CheckpointInventory>,
}
#[derive(Clone, Debug)]
pub struct ReliableSourceStatus {
    pub restored_cut:u64,
    pub published_cut:u64,
    pub committed_cut:u64,
    pub pending_messages:usize,
    pub pending_bytes:usize,
    pub max_pending_messages:usize,
    pub max_pending_bytes:usize,
    pub sampled_at:tokio::time::Instant,
    pub redeliveries:u64,
    pub pull_requests:u64,
    pub ack_retries:u64,
    pub retention_available:usize,
}
pub struct CheckpointControl {
    pub policy: CheckpointSpec,
    pub attempt: u64,
    state: std::sync::Mutex<CheckpointStatus>,
    gate: Arc<Semaphore>,
}
impl CheckpointControl {
    pub fn observe_reliable_source(&self,status:ReliableSourceStatus) {
        self.state.lock().unwrap_or_else(|e|e.into_inner()).reliable_source=Some(status);
    }
    pub fn new(
        policy: CheckpointSpec,
        attempt: u64,
        storage: sparrow_runtime::checkpoint::CheckpointInventory,
    ) -> Arc<Self> {
        Arc::new(Self {
            policy,
            attempt,
            gate: Arc::new(Semaphore::new(1)),
            state: std::sync::Mutex::new(CheckpointStatus {
                phase: "idle",
                storage: Some(storage),
                ..CheckpointStatus::default()
            }),
        })
    }
    pub fn snapshot(&self) -> CheckpointStatus {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    pub fn restored_from(&self, id: Option<u64>) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.restored_from = id;
        // A parsed CURRENT pointer alone is not a verified success. Restore
        // calls this only after snapshot compatibility and source seek succeed.
        s.last_success_id = id;
    }
    pub fn state_generation(&self,generation:[u8;16]) {
        self.state.lock().unwrap_or_else(|e|e.into_inner()).state_generation=Some(generation);
    }
    pub fn downstream_changed(&self, changed: bool) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).downstream_semantics_changed = changed;
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.policy.timeout_ms)
    }
    pub fn begin(self: &Arc<Self>, trigger: &'static str) -> Result<CheckpointAdmission> {
        self.try_begin(trigger)?.ok_or_else(|| SparrowError::new(
            ErrorCode::ResourceExhausted, "checkpoint already queued or active").retryable(true))
    }
    /// Contention is an expected scheduling outcome, never a source failure.
    pub fn try_begin(self: &Arc<Self>, trigger: &'static str) -> Result<Option<CheckpointAdmission>> {
        let permit = match self.gate.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            s.busy = s.busy.saturating_add(1);
                return Ok(None);
            }
            Err(_) => return Err(SparrowError::new(ErrorCode::Cancelled, "checkpoint admission closed")),
        };
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.active = true;
        s.started = s.started.saturating_add(1);
        s.phase = "queued";
        s.last_trigger = Some(trigger);
        Ok(Some(CheckpointAdmission {
            control: self.clone(),
            _permit: permit,
            finished: false,
            deadline: tokio::time::Instant::now() + self.timeout(),
        }))
    }
    pub fn waiter_timeout(&self) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.timed_out_waiters = s.timed_out_waiters.saturating_add(1);
    }
}
pub struct CheckpointAdmission {
    control: Arc<CheckpointControl>,
    _permit: OwnedSemaphorePermit,
    finished: bool,
    pub deadline: tokio::time::Instant,
}
impl CheckpointAdmission {
    pub fn phase(&self, phase: &'static str) {
        self.control
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .phase = phase;
    }
    pub fn finish(
        &mut self,
        outcome: &Result<u64>,
        storage: Option<sparrow_runtime::checkpoint::CheckpointInventory>,
    ) {
        let mut s = self.control.state.lock().unwrap_or_else(|e| e.into_inner());
        s.active = false;
        s.phase = "idle";
        match outcome {
            Ok(id) => {
                s.succeeded = s.succeeded.saturating_add(1);
                s.last_success_id = Some(*id);
                s.last_success_at = Some(tokio::time::Instant::now());
                s.last_error = None;
            }
            Err(e) => {
                s.failed = s.failed.saturating_add(1);
                s.last_error = Some(e.code);
            }
        }
        if let Some(storage) = storage {
            s.storage = Some(storage);
        }
        self.finished = true;
    }
}
impl Drop for CheckpointAdmission {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(
                &Err(SparrowError::new(
                    ErrorCode::Cancelled,
                    "checkpoint request cancelled",
                )),
                None,
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    #[test]
    fn k2_checkpoint_source_full_contention_is_not_a_fatal_error() {
        let control=control();
        std::thread::scope(|scope| {
            let barrier=Arc::new(std::sync::Barrier::new(3));
            for trigger in ["manual","source_full"] {
                let control=control.clone();let barrier=barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let admission=control.try_begin(trigger).unwrap();
                    barrier.wait();
                    drop(admission);
                });
            }
            barrier.wait();barrier.wait();
        });
        assert_eq!(control.snapshot().started,1);
        assert_eq!(control.snapshot().busy,1);
        assert!(control.try_begin("periodic").unwrap().is_some());
    }
    pub(crate) fn control() -> Arc<CheckpointControl> {
        CheckpointControl::new(
            CheckpointSpec {
                interval_ms: Some(100),
                timeout_ms: 100,
                ..CheckpointSpec::default()
            },
            1,
            sparrow_runtime::checkpoint::CheckpointInventory {
                current: None,
                pinned: None,
                current_error: None,
                generations: vec![],
                bytes: 0,
                maintenance_error: None,
                state_generation_marker: None,
                marker_error: None,
            },
        )
    }
    #[test]
    fn production_checkpoint_policy_is_opt_in_and_bounded() {
        let policy = CheckpointSpec::default();
        assert!(policy.interval_ms.is_none());
        assert!(!policy.resume_latest);
        policy.validate().unwrap();
        for ms in [0, 99, 86_400_001] {
            let mut p = policy.clone();
            p.interval_ms = Some(ms);
            assert!(p.validate().is_err());
        }
        let mut p = policy;
        p.retain_generations = 0;
        assert!(p.validate().is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn production_checkpoint_wait_timeout_never_releases_active_actor_admission() {
        let c = control();
        let mut request = c.begin("manual").unwrap();
        tokio::time::advance(Duration::from_millis(101)).await;
        c.waiter_timeout();
        assert!(c.begin("periodic").is_err());
        assert!(c.snapshot().active);
        request.phase("committing");
        request.finish(&Ok(7), None);
        drop(request);
        let s = c.snapshot();
        assert_eq!(
            (
                s.started,
                s.succeeded,
                s.failed,
                s.busy,
                s.timed_out_waiters
            ),
            (1, 1, 0, 1, 1)
        );
        tokio::time::advance(Duration::from_millis(20)).await;
        assert_eq!(
            s.last_success_at.unwrap().elapsed(),
            Duration::from_millis(20)
        );
        drop(c.begin("periodic").unwrap());
        let s = c.snapshot();
        assert_eq!((s.started, s.succeeded, s.failed), (2, 1, 1));
        assert!(!s.active);
    }
}

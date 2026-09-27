//! Cold, charged storage keeps new business state out of legacy stage futures.
use crate::{
    alarm_iot::AlarmIot,
    iot::{IotFreeze, IotStats},
    resample_iot::{ResampleIot, ResampleStats},
    silence_iot::SilenceIot,
    timed_iot::TimedIot,
};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, RowBatch, Schema,
    SparrowError,
};
use sparrow_plan::IotSpec;
use std::sync::Arc;

enum State {
    Basic(TimedIot),
    Alarm(AlarmIot),
    /// Boxed so the new machine does not enlarge the legacy enum layout.
    Silence(Box<SilenceIot>),
    Resample(Box<ResampleIot>),
}
pub(crate) struct TimedState {
    // The allocation drops before its accounting credit.
    state: Box<State>,
    _allocation: MemoryLease,
}
macro_rules! read {
    ($this:expr, $method:ident $(, $arg:expr)*) => {
        match $this.state.as_ref() {
            State::Basic(s) => s.$method($($arg),*),
            State::Alarm(s) => s.$method($($arg),*),
            State::Silence(s) => s.$method($($arg),*),
            State::Resample(s) => s.$method($($arg),*),
        }
    };
}
macro_rules! write {
    ($this:expr, $method:ident $(, $arg:expr)*) => {
        match $this.state.as_mut() {
            State::Basic(s) => s.$method($($arg),*),
            State::Alarm(s) => s.$method($($arg),*),
            State::Silence(s) => s.$method($($arg),*),
            State::Resample(s) => s.$method($($arg),*),
        }
    };
}
impl TimedState {
    #[cold]
    pub fn new(
        operator: OperatorId,
        spec: &IotSpec,
        input: Arc<Schema>,
        owner: Arc<MemoryOwner>,
        keys: Vec<usize>,
        fields: Vec<usize>,
    ) -> Result<Self> {
        let allocation =
            owner.acquire(CreditKind::Reservation, std::mem::size_of::<State>() + 64)?;
        let state = if spec.is_resample() {
            State::Resample(Box::new(ResampleIot::new(
                operator, spec, input, owner, keys, fields,
            )?))
        } else if spec.is_silence() {
            State::Silence(Box::new(SilenceIot::new(
                operator, spec, input, owner, keys, fields,
            )?))
        } else if spec.is_alarm() {
            State::Alarm(AlarmIot::new(operator, spec, input, owner, keys, fields)?)
        } else {
            State::Basic(TimedIot::new(operator, spec, input, owner, keys, fields)?)
        };
        Ok(Self {
            state: Box::new(state),
            _allocation: allocation,
        })
    }
    pub fn bind_generation(&mut self, generation: [u8; 16]) -> Result<()> {
        match self.state.as_mut() {
            State::Basic(_) => Ok(()),
            State::Alarm(s) => s.bind_generation(generation),
            State::Silence(s) => s.bind_generation(generation),
            State::Resample(s) => s.bind_generation(generation),
        }
    }
    /// This round's verified source coverage. Only silence decisions use it.
    pub fn observe_feed(&mut self, coverage_since: Option<i64>) -> Result<()> {
        match self.state.as_mut() {
            State::Basic(_) | State::Alarm(_) | State::Resample(_) => Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "feed coverage requires the silence state machine",
            )),
            State::Silence(s) => s.observe_feed(coverage_since),
        }
    }
    pub fn set_time(&mut self, now: i64) -> Result<()> {
        write!(self, set_time, now)
    }
    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Option<RowBatch>> {
        write!(self, on_batch, batch, now)
    }
    pub fn take_due(&mut self, now: i64) -> Result<Option<RowBatch>> {
        write!(self, take_due, now)
    }
    pub fn validate_cut(&mut self, now: i64) -> Result<()> {
        write!(self, validate_cut, now)
    }
    pub fn cleanup(&mut self) {
        write!(self, cleanup)
    }
    pub fn key_count(&self) -> usize {
        read!(self, key_count)
    }
    pub fn max_keys(&self) -> usize {
        read!(self, max_keys)
    }
    pub fn stats(&self) -> IotStats {
        read!(self, stats)
    }
    pub fn resample_stats(&self) -> ResampleStats {
        match self.state.as_ref() {
            State::Resample(s) => s.resample_stats(),
            _ => ResampleStats::default(),
        }
    }
    pub fn report_resample_metrics(&mut self, metrics: &crate::metrics::RuntimeMetrics) {
        if let State::Resample(s) = self.state.as_mut() {
            s.report_metrics(metrics);
        }
    }
    pub fn pending_timers(&self) -> usize {
        read!(self, pending_timers)
    }
    pub fn next_deadline(&self) -> Option<i64> {
        read!(self, next_deadline)
    }
    pub fn retention_bytes(&self) -> usize {
        read!(self, retention_bytes)
    }
    pub fn estimated_freeze_bytes(&self) -> usize {
        read!(self, estimated_freeze_bytes)
    }
    #[cfg(test)]
    pub fn freeze(&self) -> Result<IotFreeze> {
        read!(self, freeze)
    }
    pub fn encode(&self, out: &mut Vec<u8>, max: usize) -> Result<()> {
        read!(self, encode, out, max)
    }
    pub fn restore(&mut self, freeze: &IotFreeze) -> Result<()> {
        write!(self, restore, freeze)
    }
}

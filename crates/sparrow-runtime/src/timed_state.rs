//! Cold, charged storage keeps new business state out of legacy stage futures.
use crate::{
    alarm_iot::AlarmIot,
    iot::{IotFreeze, IotStats},
    timed_iot::TimedIot,
};
use sparrow_model::{CreditKind, MemoryLease, MemoryOwner, OperatorId, Result, RowBatch, Schema};
use sparrow_plan::IotSpec;
use std::sync::Arc;

enum State {
    Basic(TimedIot),
    Alarm(AlarmIot),
}
pub(crate) struct TimedState {
    // The allocation drops before its accounting credit.
    state: Box<State>,
    _allocation: MemoryLease,
}
macro_rules! read {
    ($this:expr, $method:ident $(, $arg:expr)*) => {
        match $this.state.as_ref() { State::Basic(s) => s.$method($($arg),*), State::Alarm(s) => s.$method($($arg),*) }
    };
}
macro_rules! write {
    ($this:expr, $method:ident $(, $arg:expr)*) => {
        match $this.state.as_mut() { State::Basic(s) => s.$method($($arg),*), State::Alarm(s) => s.$method($($arg),*) }
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
        let state = if spec.is_alarm() {
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

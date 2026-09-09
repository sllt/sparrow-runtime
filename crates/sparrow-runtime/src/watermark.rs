//! Per-input watermark generation and propagation (V0.3).
//!
//! Combined watermark = min of **active + initialized** inputs.
//! * Active + uninitialized → effective is None (do not emit).
//! * Idle inputs are excluded.
//! * All-idle → no progress (effective None); last value is not advanced.
//! * Per-input and effective watermarks never go backward.

use std::collections::BTreeMap;

use sparrow_model::{
    ErrorCode, EventTimeBinding, InputActivity, InputId, Result, SparrowError, Watermark,
    DEFAULT_MAX_WATERMARK_INPUTS,
};

#[derive(Clone, Debug)]
struct InputState {
    activity: InputActivity,
    wm: Option<i64>,
    max_event_time: Option<i64>,
}

impl InputState {
    fn new() -> Self {
        Self {
            activity: InputActivity::Active,
            wm: None,
            max_event_time: None,
        }
    }
}

/// Bounded per-input watermark hub.
#[derive(Clone, Debug)]
pub struct WatermarkHub {
    inputs: BTreeMap<InputId, InputState>,
    last_effective: Option<i64>,
    max_inputs: u16,
    binding: Option<EventTimeBinding>,
}

impl WatermarkHub {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_WATERMARK_INPUTS)
    }

    pub fn with_capacity(max_inputs: u16) -> Self {
        Self {
            inputs: BTreeMap::new(),
            last_effective: None,
            max_inputs: max_inputs.max(1),
            binding: None,
        }
    }

    pub fn with_binding(mut self, binding: EventTimeBinding) -> Result<Self> {
        binding.validate()?;
        self.binding = Some(binding);
        Ok(self)
    }

    pub fn register(&mut self, id: InputId) -> Result<()> {
        if self.inputs.contains_key(&id) {
            return Ok(());
        }
        if self.inputs.len() as u16 >= self.max_inputs {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "watermark inputs exceed max {} (got {})",
                    self.max_inputs,
                    self.inputs.len() + 1
                ),
            ));
        }
        self.inputs.insert(id, InputState::new());
        Ok(())
    }

    pub fn ensure(&mut self, id: InputId) -> Result<()> {
        self.register(id)
    }

    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    pub fn activity(&self, id: InputId) -> Option<InputActivity> {
        self.inputs.get(&id).map(|s| s.activity)
    }

    pub fn input_wm(&self, id: InputId) -> Option<i64> {
        self.inputs.get(&id).and_then(|s| s.wm)
    }

    pub fn last_effective(&self) -> Option<i64> {
        self.last_effective
    }

    pub fn all_idle(&self) -> bool {
        !self.inputs.is_empty()
            && self
                .inputs
                .values()
                .all(|s| s.activity == InputActivity::Idle)
    }

    pub fn any_active_uninitialized(&self) -> bool {
        self.inputs
            .values()
            .any(|s| s.activity == InputActivity::Active && s.wm.is_none())
    }

    /// Effective watermark for progress. None if blocked or all-idle.
    pub fn effective(&self) -> Option<i64> {
        if self.inputs.is_empty() {
            return None;
        }
        if self.all_idle() {
            return None;
        }
        if self.any_active_uninitialized() {
            return None;
        }
        let mut min_wm: Option<i64> = None;
        for s in self.inputs.values() {
            if s.activity != InputActivity::Active {
                continue;
            }
            let wm = s.wm?;
            min_wm = Some(min_wm.map(|m| m.min(wm)).unwrap_or(wm));
        }
        min_wm
    }

    /// Monotonic progress watermark. None means "do not advance".
    pub fn progress(&self) -> Option<i64> {
        let Some(eff) = self.effective() else {
            return None;
        };
        match self.last_effective {
            Some(prev) if eff < prev => Some(prev),
            _ => Some(eff),
        }
    }

    pub fn mark_idle(&mut self, id: InputId) -> Result<Option<i64>> {
        self.ensure(id)?;
        if let Some(s) = self.inputs.get_mut(&id) {
            s.activity = InputActivity::Idle;
        }
        self.commit_progress()
    }

    pub fn mark_active(&mut self, id: InputId) -> Result<Option<i64>> {
        self.ensure(id)?;
        if let Some(s) = self.inputs.get_mut(&id) {
            s.activity = InputActivity::Active;
        }
        self.commit_progress()
    }

    /// Observe an event timestamp. Generates `wm = max_et - out_of_orderness`.
    /// Rejects future timestamps when the binding sets a skew.
    pub fn observe_event(
        &mut self,
        id: InputId,
        event_time: i64,
        now_micros: i64,
    ) -> Result<Option<i64>> {
        if event_time < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("event_time {event_time} must be >= 0"),
            ));
        }
        if let Some(bind) = &self.binding {
            if let Some(skew) = bind.max_future_skew_micros {
                if event_time > now_micros.saturating_add(skew) {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!(
                            "future timestamp {event_time} exceeds now {now_micros} + skew {skew}"
                        ),
                    )
                    .context("event_time", event_time.to_string())
                    .context("now", now_micros.to_string()));
                }
            }
            let ooo = bind.out_of_orderness_micros;
            self.ensure(id)?;
            let proposed = {
                let s = self.inputs.get_mut(&id).expect("registered");
                s.activity = InputActivity::Active;
                let max_et = s.max_event_time.map(|m| m.max(event_time)).unwrap_or(event_time);
                s.max_event_time = Some(max_et);
                max_et.saturating_sub(ooo).max(0)
            };
            return self.set_watermark(id, proposed);
        }
        self.ensure(id)?;
        if let Some(s) = self.inputs.get_mut(&id) {
            s.activity = InputActivity::Active;
            let max_et = s
                .max_event_time
                .map(|m| m.max(event_time))
                .unwrap_or(event_time);
            s.max_event_time = Some(max_et);
        }
        self.set_watermark(id, event_time)
    }

    /// Explicit punctuation. Never moves a per-input watermark backward.
    pub fn set_watermark(&mut self, id: InputId, wm: i64) -> Result<Option<i64>> {
        if wm < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "watermark must be >= 0",
            ));
        }
        self.ensure(id)?;
        if let Some(s) = self.inputs.get_mut(&id) {
            match s.wm {
                Some(prev) if wm < prev => {
                    // monotonic: ignore backward punctuation
                }
                _ => s.wm = Some(wm),
            }
        }
        self.commit_progress()
    }

    fn commit_progress(&mut self) -> Result<Option<i64>> {
        let Some(next) = self.progress() else {
            return Ok(None);
        };
        match self.last_effective {
            Some(prev) if next < prev => {
                return Err(SparrowError::new(
                    ErrorCode::Internal,
                    format!("watermark went backward: {prev} -> {next}"),
                ));
            }
            Some(prev) if next == prev => Ok(Some(prev)),
            _ => {
                self.last_effective = Some(next);
                Ok(Some(next))
            }
        }
    }

    pub fn snapshot(&self) -> Watermark {
        Watermark {
            input: InputId(0),
            timestamp_micros: self.last_effective.unwrap_or(0),
        }
    }

    /// Restore the last committed effective watermark (experimental).
    pub fn restore_effective(&mut self, last: Option<i64>) {
        self.last_effective = last;
    }
}

impl Default for WatermarkHub {
    fn default() -> Self {
        Self::new()
    }
}

/// Output watermark with holdback L: emit finals at `wm_out`, then advance.
#[derive(Clone, Debug)]
pub struct OutputHoldback {
    pub lateness_micros: i64,
    wm_in: Option<i64>,
    wm_out: Option<i64>,
}

impl OutputHoldback {
    pub fn new(lateness_micros: i64) -> Result<Self> {
        if lateness_micros < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "holdback L must be >= 0",
            ));
        }
        Ok(Self {
            lateness_micros,
            wm_in: None,
            wm_out: None,
        })
    }

    pub fn wm_in(&self) -> Option<i64> {
        self.wm_in
    }

    pub fn wm_out(&self) -> Option<i64> {
        self.wm_out
    }

    /// Apply a new input watermark. Returns the new `wm_out` once finals
    /// for windows with `end <= wm_out` should be emitted. Caller emits
    /// **then** the returned value is the advanced output watermark.
    pub fn on_wm_in(&mut self, wm_in: i64) -> Result<Option<i64>> {
        if let Some(prev) = self.wm_in {
            if wm_in < prev {
                return Ok(self.wm_out);
            }
        }
        self.wm_in = Some(wm_in);
        let proposed = sparrow_model::holdback_wm_out(wm_in, self.lateness_micros)?;
        if let Some(out) = self.wm_out {
            if proposed < out {
                return Ok(Some(out));
            }
        }
        Ok(Some(proposed))
    }

    /// Advance output watermark **after** finals have been emitted.
    pub fn advance_out(&mut self, wm_out: i64) -> Result<()> {
        if let Some(inp) = self.wm_in {
            let cap = sparrow_model::holdback_wm_out(inp, self.lateness_micros)?;
            if wm_out > cap {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("wm_out {wm_out} > wm_in {inp} - L {} (cap {cap})", self.lateness_micros),
                ));
            }
        }
        if let Some(prev) = self.wm_out {
            if wm_out < prev {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "output watermark must not go backward",
                ));
            }
        }
        self.wm_out = Some(wm_out);
        Ok(())
    }

    /// Restore holdback from a committed snapshot (experimental).
    pub fn restore(&mut self, wm_in: Option<i64>, wm_out: Option<i64>) {
        self.wm_in = wm_in;
        self.wm_out = wm_out;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uninitialized_active_blocks() {
        let mut h = WatermarkHub::new();
        h.register(InputId(0)).unwrap();
        h.register(InputId(1)).unwrap();
        h.set_watermark(InputId(0), 10).unwrap();
        assert!(h.effective().is_none(), "input 1 active uninitialized");
    }

    #[test]
    fn idle_excludes_from_min() {
        let mut h = WatermarkHub::new();
        h.register(InputId(0)).unwrap();
        h.register(InputId(1)).unwrap();
        h.set_watermark(InputId(0), 20).unwrap();
        h.mark_idle(InputId(1)).unwrap();
        assert_eq!(h.effective(), Some(20));
    }

    #[test]
    fn all_idle_does_not_advance() {
        let mut h = WatermarkHub::new();
        h.register(InputId(0)).unwrap();
        h.register(InputId(1)).unwrap();
        h.set_watermark(InputId(0), 5).unwrap();
        h.mark_idle(InputId(0)).unwrap();
        h.mark_idle(InputId(1)).unwrap();
        assert!(h.all_idle());
        assert!(h.effective().is_none());
        assert!(h.progress().is_none());
    }

    #[test]
    fn watermark_monotonic() {
        let mut h = WatermarkHub::new();
        h.set_watermark(InputId(0), 10).unwrap();
        h.set_watermark(InputId(0), 7).unwrap();
        assert_eq!(h.input_wm(InputId(0)), Some(10));
        assert_eq!(h.effective(), Some(10));
    }

    #[test]
    fn future_timestamp_rejected() {
        let bind = EventTimeBinding {
            field: "ts".into(),
            out_of_orderness_micros: 0,
            max_future_skew_micros: Some(1_000_000),
        };
        let mut h = WatermarkHub::new().with_binding(bind).unwrap();
        let err = h
            .observe_event(InputId(0), 10_000_000, 0)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("future timestamp"));
    }

    #[test]
    fn holdback_formula() {
        let mut hb = OutputHoldback::new(3_000_000).unwrap();
        let out = hb.on_wm_in(13_000_000).unwrap();
        assert_eq!(out, Some(10_000_000));
        hb.advance_out(10_000_000).unwrap();
        assert_eq!(hb.wm_out(), Some(10_000_000));
        assert!(hb.advance_out(13_000_000).is_err());
    }
}

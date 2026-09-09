//! Event-time watermarks (V0.3).
//!
//! A watermark is a per-input declaration: no later event with
//! `event_time < wm` is expected from that input. Combined watermark
//! across inputs is the **min of active, initialized** inputs.
//!
//! Hard rules:
//! * watermarks never go backward (per-input and effective)
//! * an **active** input with no watermark blocks the effective watermark
//! * **idle** inputs are excluded from the min
//! * **all-idle** does not advance time (effective is unset for progress)
//! * output holdback: `wm_out <= wm_in - L` (see window lateness)

use crate::error::{ErrorCode, Result, SparrowError};

/// Stable input identity for watermark tracking (source edge or named ingress).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InputId(pub u16);

impl InputId {
    pub const fn new(id: u16) -> Self {
        Self(id)
    }

    pub const fn raw(self) -> u16 {
        self.0
    }
}

impl std::fmt::Display for InputId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "input-{}", self.0)
    }
}

/// Whether an input currently participates in watermark min.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputActivity {
    Active,
    Idle,
}

impl InputActivity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
        }
    }
}

/// Punctuated watermark from one input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Watermark {
    pub input: InputId,
    pub timestamp_micros: i64,
}

impl Watermark {
    pub fn new(input: InputId, timestamp_micros: i64) -> Result<Self> {
        if timestamp_micros < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "watermark timestamp_micros must be >= 0",
            ));
        }
        Ok(Self {
            input,
            timestamp_micros,
        })
    }
}

/// How a stream binds event time and generates watermarks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventTimeBinding {
    pub field: String,
    /// Source-side out-of-orderness subtracted from max observed event time.
    /// Holdback `L` for finals lives on the window (`lateness_micros`), not here.
    pub out_of_orderness_micros: i64,
    /// If set, `event_time > now + skew` is `invalid_argument` (future timestamp).
    pub max_future_skew_micros: Option<i64>,
}

impl EventTimeBinding {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            out_of_orderness_micros: 0,
            max_future_skew_micros: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.field.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "event-time binding requires a non-empty field name",
            ));
        }
        if self.out_of_orderness_micros < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "out_of_orderness_micros must be >= 0",
            ));
        }
        if let Some(skew) = self.max_future_skew_micros {
            if skew < 0 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "max_future_skew_micros must be >= 0 when set",
                ));
            }
        }
        Ok(())
    }
}

/// Planner cap: hopping `ceil(size/slide)` must be <= this (fail closed).
pub const DEFAULT_MAX_HOP_OVERLAP: u32 = 8;

/// Max distinct watermark inputs per operator.
pub const DEFAULT_MAX_WATERMARK_INPUTS: u16 = 16;

/// Output watermark holdback: `wm_out <= wm_in - L`.
pub fn holdback_wm_out(wm_in: i64, lateness_micros: i64) -> Result<i64> {
    if lateness_micros < 0 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "lateness / holdback L must be >= 0",
        ));
    }
    Ok(wm_in.saturating_sub(lateness_micros).max(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holdback_never_exceeds_input() {
        assert_eq!(holdback_wm_out(13_000_000, 3_000_000).unwrap(), 10_000_000);
        assert_eq!(holdback_wm_out(3, 10).unwrap(), 0);
    }
}

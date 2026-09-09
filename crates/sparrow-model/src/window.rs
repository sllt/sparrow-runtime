//! V0.2 window and aggregate vocabulary. Event-time / hop / session are
//! declared so binders can reject them with a stable code.

use crate::error::{ErrorCode, Result, SparrowError};

/// Time domain for window assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeDomain {
    /// Operator-local processing clock ([`crate::clock::InstantSource`]).
    ProcessingTime,
    /// Event-time (watermark). **Not available in V0.2.**
    EventTime,
}

impl TimeDomain {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessingTime => "processing_time",
            Self::EventTime => "event_time",
        }
    }

    pub fn require_processing_time(self) -> Result<()> {
        match self {
            Self::ProcessingTime => Ok(()),
            Self::EventTime => Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "event-time windows and watermarks are V0.3, not V0.2",
            )),
        }
    }
}

/// Window assigner supported in V0.2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind {
    /// Fixed-size, non-overlapping processing-time windows.
    TumblingProcessingTime { size_micros: i64 },
    /// Emit after `size` records per key (no timer).
    Count { size: u64 },
}

impl WindowKind {
    pub fn tumbling_pt(size_micros: i64) -> Result<Self> {
        if size_micros <= 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "tumbling window size_micros must be > 0",
            ));
        }
        Ok(Self::TumblingProcessingTime { size_micros })
    }

    pub fn count(size: u64) -> Result<Self> {
        if size == 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "count window size must be > 0",
            ));
        }
        Ok(Self::Count { size })
    }

    pub fn uses_processing_time_timer(self) -> bool {
        matches!(self, Self::TumblingProcessingTime { .. })
    }

    /// Assign a tumbling window `[start, end)` from processing time.
    pub fn assign_tumble(now_micros: i64, size_micros: i64) -> Result<(i64, i64)> {
        if size_micros <= 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "window size must be > 0",
            ));
        }
        let start = now_micros.div_euclid(size_micros).saturating_mul(size_micros);
        let end = start.saturating_add(size_micros);
        Ok((start, end))
    }
}

/// Incremental aggregate function. Accumulators store O(1) per key, never raw rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFn {
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Ok(Self::Count),
            "sum" => Ok(Self::Sum),
            "avg" | "average" | "mean" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            other => Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("aggregate '{other}' is not part of V0.2"),
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

/// Integer overflow policy for incremental SUM / COUNT (documented in V0.2 report).
///
/// * `Int64` / `UInt64` use **checked** add. Overflow fails the job with
///   [`ErrorCode::IntegerOverflow`] — never wrap, never saturate, never NULL.
/// * `Float64` uses IEEE addition (Inf/NaN may appear; not treated as overflow).
/// * NULL inputs are skipped for SUM/AVG/MIN/MAX and for `COUNT(col)`.
///   `COUNT(*)` counts every row including all-NULL rows.
pub const INTEGER_OVERFLOW_POLICY: &str = "checked integer overflow: SUM/COUNT on integers fail the job (integer_overflow); never wrap or silent-drop";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tumble_assign_is_aligned() {
        let (s, e) = WindowKind::assign_tumble(1_250_000, 1_000_000).unwrap();
        assert_eq!((s, e), (1_000_000, 2_000_000));
    }

    #[test]
    fn event_time_is_rejected() {
        assert_eq!(
            TimeDomain::EventTime.require_processing_time().unwrap_err().code,
            ErrorCode::FeatureUnavailable
        );
    }
}

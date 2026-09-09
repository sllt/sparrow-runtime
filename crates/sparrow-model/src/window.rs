//! V0.3 window and aggregate vocabulary.
//!
//! Processing-time / count windows stay arrival-order. Event-time tumble and
//! hop require an explicit [`TimeDomain::EventTime`] assigner plus holdback.
//! Count / PT windows must **not** silently impersonate event-time.

use crate::error::{ErrorCode, Result, SparrowError};
use crate::watermark::DEFAULT_MAX_HOP_OVERLAP;

/// Time domain for window assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeDomain {
    /// Operator-local processing clock ([`crate::clock::InstantSource`]).
    ProcessingTime,
    /// Event-time (watermark + holdback).
    EventTime,
}

impl TimeDomain {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessingTime => "processing_time",
            Self::EventTime => "event_time",
        }
    }
}

/// Window assigner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind {
    /// Fixed-size, non-overlapping processing-time windows.
    TumblingProcessingTime { size_micros: i64 },
    /// Emit after `size` records per key (arrival-order; no event-time).
    Count { size: u64 },
    /// Fixed-size, non-overlapping event-time windows.
    TumblingEventTime { size_micros: i64 },
    /// Sliding event-time windows. Overlap `ceil(size/slide)` is planner-capped.
    HoppingEventTime {
        size_micros: i64,
        slide_micros: i64,
    },
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

    pub fn tumbling_et(size_micros: i64) -> Result<Self> {
        if size_micros <= 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "event-time tumbling size_micros must be > 0",
            ));
        }
        Ok(Self::TumblingEventTime { size_micros })
    }

    pub fn hopping_et(size_micros: i64, slide_micros: i64) -> Result<Self> {
        if size_micros <= 0 || slide_micros <= 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "hopping size_micros and slide_micros must be > 0",
            ));
        }
        if slide_micros > size_micros {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "hopping slide_micros must be <= size_micros (no gaps)",
            ));
        }
        Ok(Self::HoppingEventTime {
            size_micros,
            slide_micros,
        })
    }

    pub fn time_domain(self) -> TimeDomain {
        match self {
            Self::TumblingProcessingTime { .. } | Self::Count { .. } => TimeDomain::ProcessingTime,
            Self::TumblingEventTime { .. } | Self::HoppingEventTime { .. } => TimeDomain::EventTime,
        }
    }

    pub fn uses_processing_time_timer(self) -> bool {
        matches!(self, Self::TumblingProcessingTime { .. })
    }

    pub fn uses_event_time(self) -> bool {
        matches!(
            self,
            Self::TumblingEventTime { .. } | Self::HoppingEventTime { .. }
        )
    }

    /// Assign a tumbling window `[start, end)` from a timestamp.
    pub fn assign_tumble(now_micros: i64, size_micros: i64) -> Result<(i64, i64)> {
        if size_micros <= 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "window size must be > 0",
            ));
        }
        let start = now_micros
            .div_euclid(size_micros)
            .saturating_mul(size_micros);
        let end = start.saturating_add(size_micros);
        Ok((start, end))
    }

    /// Concurrent hopping windows that cover `ts` (planner already capped overlap).
    pub fn assign_hop(
        ts: i64,
        size_micros: i64,
        slide_micros: i64,
        max_overlap: u32,
    ) -> Result<Vec<(i64, i64)>> {
        let overlap = hop_overlap(size_micros, slide_micros)?;
        if overlap > max_overlap {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "hopping overlap {overlap} exceeds planner max_overlap {max_overlap} (size={size_micros} slide={slide_micros})"
                ),
            ));
        }
        let last = ts.div_euclid(slide_micros).saturating_mul(slide_micros);
        let mut out = Vec::new();
        let mut start = last;
        while start > ts.saturating_sub(size_micros) {
            if ts >= start && ts < start.saturating_add(size_micros) {
                out.push((start, start.saturating_add(size_micros)));
            }
            let next = start.saturating_sub(slide_micros);
            if next == start {
                break;
            }
            start = next;
            if out.len() as u32 > max_overlap {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "hopping assignment exceeded overlap cap",
                ));
            }
        }
        if ts >= last && ts < last.saturating_add(size_micros) && !out.iter().any(|(s, _)| *s == last)
        {
            out.insert(0, (last, last.saturating_add(size_micros)));
        }
        Ok(out)
    }
}

/// `ceil(size / slide)` concurrent windows per event.
pub fn hop_overlap(size_micros: i64, slide_micros: i64) -> Result<u32> {
    if size_micros <= 0 || slide_micros <= 0 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "hop overlap requires size > 0 and slide > 0",
        ));
    }
    let q = size_micros.div_euclid(slide_micros);
    let rem = size_micros.rem_euclid(slide_micros);
    let n = if rem == 0 { q } else { q + 1 };
    if n <= 0 || n > u32::MAX as i64 {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "hop overlap is not representable",
        ));
    }
    Ok(n as u32)
}

pub fn check_hop_overlap_bound(
    size_micros: i64,
    slide_micros: i64,
    max_overlap: u32,
) -> Result<u32> {
    let n = hop_overlap(size_micros, slide_micros)?;
    let cap = if max_overlap == 0 {
        DEFAULT_MAX_HOP_OVERLAP
    } else {
        max_overlap
    };
    if n > cap {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!(
                "planner rejected hopping window: overlap {n} = ceil({size_micros}/{slide_micros}) > max_overlap {cap}"
            ),
        ));
    }
    Ok(n)
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
                format!("aggregate '{other}' is not part of V0.3"),
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
    fn hop_overlap_is_ceil_div() {
        assert_eq!(hop_overlap(10, 5).unwrap(), 2);
        assert_eq!(hop_overlap(10, 3).unwrap(), 4);
        assert!(check_hop_overlap_bound(90, 10, 8).is_err());
        assert_eq!(check_hop_overlap_bound(10, 5, 8).unwrap(), 2);
    }

    #[test]
    fn hop_assign_covers_event() {
        let wins = WindowKind::assign_hop(8_000_000, 10_000_000, 5_000_000, 8).unwrap();
        assert!(wins.contains(&(5_000_000, 15_000_000)));
        assert!(wins.contains(&(0, 10_000_000)));
    }
}

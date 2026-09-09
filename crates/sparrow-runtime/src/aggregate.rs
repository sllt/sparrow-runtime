//! Incremental COUNT / SUM / AVG / MIN / MAX. Accumulators are O(1) per key.
//!
//! Overflow policy ([`sparrow_model::INTEGER_OVERFLOW_POLICY`]):
//! integer SUM uses checked add and fails the job; never wraps.

use sparrow_model::{
    AggFn, DataType, ErrorCode, Result, Scalar, SparrowError, INTEGER_OVERFLOW_POLICY,
};

/// One incremental accumulator. Never stores input rows.
#[derive(Clone, Debug, PartialEq)]
pub enum Accumulator {
    Count {
        /// COUNT(*) including NULL rows.
        rows: u64,
        /// COUNT(col) excluding NULL.
        non_null: u64,
        star: bool,
    },
    SumI64 {
        sum: i64,
        n: u64,
    },
    SumU64 {
        sum: u64,
        n: u64,
    },
    SumF64 {
        sum: f64,
        n: u64,
    },
    Avg {
        sum: f64,
        n: u64,
        /// When all inputs were integers, still emit Float64 (SQL AVG).
        saw_float: bool,
    },
    Min {
        v: Option<Scalar>,
    },
    Max {
        v: Option<Scalar>,
    },
}

impl Accumulator {
    pub fn new(func: AggFn, ty: DataType, count_star: bool) -> Result<Self> {
        Ok(match func {
            AggFn::Count => Self::Count {
                rows: 0,
                non_null: 0,
                star: count_star,
            },
            AggFn::Sum => match ty {
                DataType::Int64 | DataType::Null => Self::SumI64 { sum: 0, n: 0 },
                DataType::UInt64 => Self::SumU64 { sum: 0, n: 0 },
                DataType::Float64 => Self::SumF64 { sum: 0.0, n: 0 },
                other => {
                    return Err(SparrowError::new(
                        ErrorCode::TypeMismatch,
                        format!("SUM does not accept {other}"),
                    ))
                }
            },
            AggFn::Avg => Self::Avg {
                sum: 0.0,
                n: 0,
                saw_float: matches!(ty, DataType::Float64),
            },
            AggFn::Min => Self::Min { v: None },
            AggFn::Max => Self::Max { v: None },
        })
    }

    pub fn tracked_bytes(&self) -> usize {
        48
    }

    pub fn update(&mut self, value: &Scalar) -> Result<()> {
        match self {
            Self::Count {
                rows,
                non_null,
                star,
            } => {
                *rows = rows.checked_add(1).ok_or_else(overflow)?;
                if *star || !value.is_null() {
                    if !value.is_null() {
                        *non_null = non_null.checked_add(1).ok_or_else(overflow)?;
                    } else if *star {
                        // COUNT(*) already counted in rows.
                    }
                }
                let _ = star;
                Ok(())
            }
            Self::SumI64 { sum, n } => {
                if value.is_null() {
                    return Ok(());
                }
                let v = match value {
                    Scalar::Int64(v) => *v,
                    Scalar::UInt64(v) if *v <= i64::MAX as u64 => *v as i64,
                    Scalar::Bool(b) => {
                        if *b {
                            1
                        } else {
                            0
                        }
                    }
                    other => {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            format!("SUM(int64) got {}", other.data_type()),
                        ))
                    }
                };
                *sum = sum.checked_add(v).ok_or_else(overflow)?;
                *n = n.checked_add(1).ok_or_else(overflow)?;
                Ok(())
            }
            Self::SumU64 { sum, n } => {
                if value.is_null() {
                    return Ok(());
                }
                let v = match value {
                    Scalar::UInt64(v) => *v,
                    Scalar::Int64(v) if *v >= 0 => *v as u64,
                    other => {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            format!("SUM(uint64) got {}", other.data_type()),
                        ))
                    }
                };
                *sum = sum.checked_add(v).ok_or_else(overflow)?;
                *n = n.checked_add(1).ok_or_else(overflow)?;
                Ok(())
            }
            Self::SumF64 { sum, n } => {
                if value.is_null() {
                    return Ok(());
                }
                let v = value.as_f64().ok_or_else(|| {
                    SparrowError::new(ErrorCode::TypeMismatch, "SUM(float64) expects numeric")
                })?;
                *sum += v;
                *n += 1;
                Ok(())
            }
            Self::Avg { sum, n, saw_float } => {
                if value.is_null() {
                    return Ok(());
                }
                if matches!(value, Scalar::Float64(_)) {
                    *saw_float = true;
                }
                let v = value.as_f64().ok_or_else(|| {
                    SparrowError::new(ErrorCode::TypeMismatch, "AVG expects numeric")
                })?;
                *sum += v;
                *n = n.checked_add(1).ok_or_else(overflow)?;
                Ok(())
            }
            Self::Min { v } => {
                if value.is_null() {
                    return Ok(());
                }
                match v {
                    None => *v = Some(value.detach_copy()),
                    Some(cur) => {
                        if cmp_ord(value, cur)? == std::cmp::Ordering::Less {
                            *v = Some(value.detach_copy());
                        }
                    }
                }
                Ok(())
            }
            Self::Max { v } => {
                if value.is_null() {
                    return Ok(());
                }
                match v {
                    None => *v = Some(value.detach_copy()),
                    Some(cur) => {
                        if cmp_ord(value, cur)? == std::cmp::Ordering::Greater {
                            *v = Some(value.detach_copy());
                        }
                    }
                }
                Ok(())
            }
        }
    }

    pub fn finish(&self) -> Scalar {
        match self {
            Self::Count { rows, non_null, star } => {
                Scalar::Int64(if *star { *rows as i64 } else { *non_null as i64 })
            }
            Self::SumI64 { sum, n } => {
                if *n == 0 {
                    Scalar::Null
                } else {
                    Scalar::Int64(*sum)
                }
            }
            Self::SumU64 { sum, n } => {
                if *n == 0 {
                    Scalar::Null
                } else {
                    Scalar::UInt64(*sum)
                }
            }
            Self::SumF64 { sum, n } => {
                if *n == 0 {
                    Scalar::Null
                } else {
                    Scalar::Float64(*sum)
                }
            }
            Self::Avg { sum, n, .. } => {
                if *n == 0 {
                    Scalar::Null
                } else {
                    Scalar::Float64(*sum / *n as f64)
                }
            }
            Self::Min { v } | Self::Max { v } => v.clone().unwrap_or(Scalar::Null),
        }
    }
}

fn overflow() -> SparrowError {
    SparrowError::new(
        ErrorCode::IntegerOverflow,
        format!("integer aggregate overflow ({INTEGER_OVERFLOW_POLICY})"),
    )
}

fn cmp_ord(a: &Scalar, b: &Scalar) -> Result<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return Ok(x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal));
    }
    match (a, b) {
        (Scalar::Utf8(x), Scalar::Utf8(y)) => Ok(x.as_ref().cmp(y.as_ref())),
        (Scalar::Bytes(x), Scalar::Bytes(y)) => Ok(x.as_ref().cmp(y.as_ref())),
        (Scalar::Bool(x), Scalar::Bool(y)) => Ok(x.cmp(y)),
        _ => Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            format!("MIN/MAX cannot compare {} and {}", a.data_type(), b.data_type()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_and_overflow_semantics() {
        let mut sum = Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap();
        sum.update(&Scalar::Int64(1)).unwrap();
        sum.update(&Scalar::Null).unwrap();
        sum.update(&Scalar::Int64(2)).unwrap();
        assert_eq!(sum.finish(), Scalar::Int64(3));

        let mut empty = Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap();
        empty.update(&Scalar::Null).unwrap();
        assert_eq!(empty.finish(), Scalar::Null);

        let mut avg = Accumulator::new(AggFn::Avg, DataType::Float64, false).unwrap();
        avg.update(&Scalar::Float64(10.0)).unwrap();
        avg.update(&Scalar::Null).unwrap();
        avg.update(&Scalar::Float64(20.0)).unwrap();
        assert_eq!(avg.finish(), Scalar::Float64(15.0));

        let mut c_star = Accumulator::new(AggFn::Count, DataType::Int64, true).unwrap();
        c_star.update(&Scalar::Null).unwrap();
        c_star.update(&Scalar::Int64(1)).unwrap();
        assert_eq!(c_star.finish(), Scalar::Int64(2));

        let mut c_col = Accumulator::new(AggFn::Count, DataType::Int64, false).unwrap();
        c_col.update(&Scalar::Null).unwrap();
        c_col.update(&Scalar::Int64(1)).unwrap();
        assert_eq!(c_col.finish(), Scalar::Int64(1));

        let mut ov = Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap();
        ov.update(&Scalar::Int64(i64::MAX)).unwrap();
        assert_eq!(
            ov.update(&Scalar::Int64(1)).unwrap_err().code,
            ErrorCode::IntegerOverflow
        );
    }
}

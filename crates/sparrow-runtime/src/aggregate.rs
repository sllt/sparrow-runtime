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
        const BASE: usize = 48;
        match self {
            Self::Min { v } | Self::Max { v } => {
                BASE + v.as_ref().map(Scalar::tracked_bytes).unwrap_or(0)
            }
            Self::Count { .. } | Self::SumI64 { .. } | Self::SumU64 { .. } | Self::SumF64 { .. }
            | Self::Avg { .. } => BASE,
        }
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

    pub fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Count {
                rows,
                non_null,
                star,
            } => {
                out.push(1);
                out.extend_from_slice(&rows.to_le_bytes());
                out.extend_from_slice(&non_null.to_le_bytes());
                out.push(u8::from(*star));
            }
            Self::SumI64 { sum, n } => {
                out.push(2);
                out.extend_from_slice(&sum.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            Self::SumU64 { sum, n } => {
                out.push(3);
                out.extend_from_slice(&sum.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            Self::SumF64 { sum, n } => {
                out.push(4);
                out.extend_from_slice(&sum.to_bits().to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            Self::Avg { sum, n, saw_float } => {
                out.push(5);
                out.extend_from_slice(&sum.to_bits().to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
                out.push(u8::from(*saw_float));
            }
            Self::Min { v } => {
                out.push(6);
                encode_opt_scalar(v, out)?;
            }
            Self::Max { v } => {
                out.push(7);
                encode_opt_scalar(v, out)?;
            }
        }
        Ok(())
    }

    pub fn decode(src: &mut &[u8]) -> Result<Self> {
        if src.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "truncated accumulator",
            ));
        }
        let tag = src[0];
        *src = &src[1..];
        match tag {
            1 => {
                let rows = take_u64(src)?;
                let non_null = take_u64(src)?;
                let star = take_u8(src)? != 0;
                Ok(Self::Count {
                    rows,
                    non_null,
                    star,
                })
            }
            2 => Ok(Self::SumI64 {
                sum: take_i64(src)?,
                n: take_u64(src)?,
            }),
            3 => Ok(Self::SumU64 {
                sum: take_u64(src)?,
                n: take_u64(src)?,
            }),
            4 => Ok(Self::SumF64 {
                sum: f64::from_bits(take_u64(src)?),
                n: take_u64(src)?,
            }),
            5 => Ok(Self::Avg {
                sum: f64::from_bits(take_u64(src)?),
                n: take_u64(src)?,
                saw_float: take_u8(src)? != 0,
            }),
            6 => Ok(Self::Min {
                v: decode_opt_scalar(src)?,
            }),
            7 => Ok(Self::Max {
                v: decode_opt_scalar(src)?,
            }),
            other => Err(SparrowError::new(
                ErrorCode::CodecViolation,
                format!("unknown accumulator tag {other}"),
            )),
        }
    }
}

fn take_u8(src: &mut &[u8]) -> Result<u8> {
    if src.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated accumulator byte",
        ));
    }
    let v = src[0];
    *src = &src[1..];
    Ok(v)
}

fn take_bytes<'a>(src: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if src.len() < n {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated accumulator integer",
        ));
    }
    let (head, rest) = src.split_at(n);
    *src = rest;
    Ok(head)
}

fn take_u64(src: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take_bytes(src, 8)?.try_into().unwrap()))
}

fn take_i64(src: &mut &[u8]) -> Result<i64> {
    Ok(i64::from_le_bytes(take_bytes(src, 8)?.try_into().unwrap()))
}

fn encode_opt_scalar(v: &Option<Scalar>, out: &mut Vec<u8>) -> Result<()> {
    match v {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            s.encode_value(out)?;
        }
    }
    Ok(())
}

fn decode_opt_scalar(src: &mut &[u8]) -> Result<Option<Scalar>> {
    match take_u8(src)? {
        0 => Ok(None),
        1 => Ok(Some(Scalar::decode_value(src)?)),
        _ => Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "invalid optional scalar tag",
        )),
    }
}

fn overflow() -> SparrowError {
    SparrowError::new(
        ErrorCode::IntegerOverflow,
        format!("integer aggregate overflow ({INTEGER_OVERFLOW_POLICY})"),
    )
}

fn cmp_ord(a: &Scalar, b: &Scalar) -> Result<std::cmp::Ordering> {
    match (a, b) {
        (Scalar::Int64(x), Scalar::Int64(y)) => Ok(x.cmp(y)),
        (Scalar::UInt64(x), Scalar::UInt64(y)) => Ok(x.cmp(y)),
        (Scalar::Int64(x), Scalar::UInt64(y)) if *x >= 0 => Ok((*x as u64).cmp(y)),
        (Scalar::UInt64(x), Scalar::Int64(y)) if *y >= 0 => Ok(x.cmp(&(*y as u64))),
        (Scalar::TimestampMicrosUTC(x), Scalar::TimestampMicrosUTC(y)) => Ok(x.cmp(y)),
        (Scalar::TimestampMicrosUTC(x), Scalar::Int64(y)) => Ok(x.cmp(y)),
        (Scalar::Int64(x), Scalar::TimestampMicrosUTC(y)) => Ok(x.cmp(y)),
        (Scalar::Float64(x), Scalar::Float64(y)) => x.partial_cmp(y).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "MIN/MAX refuses unordered NaN (no implicit Equal)",
            )
        }),
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

    #[test]
    fn r05_min_max_tracked_bytes_include_payload() {
        let mut mn = Accumulator::new(AggFn::Min, DataType::Utf8, false).unwrap();
        assert_eq!(mn.tracked_bytes(), 48);
        mn.update(&Scalar::utf8("xxxxxxxxxxxxxxxxxxxxxxxx")).unwrap();
        assert!(
            mn.tracked_bytes() > 48,
            "MIN must bill the stored utf8 payload, got {}",
            mn.tracked_bytes()
        );
        let mut mx = Accumulator::new(AggFn::Max, DataType::Utf8, false).unwrap();
        mx.update(&Scalar::utf8("yyyyyyyyyyyyyyyyyyyyyyyy")).unwrap();
        assert!(mx.tracked_bytes() > 48);
    }

    #[test]
    fn r05_small_retention_rejects_minmax_growth() {
        use crate::state::{MemoryState, StateKey};
        use sparrow_model::{MemoryOwner, OperatorId, ResourceBudget, StateSlotId};
        let owner = MemoryOwner::new(ResourceBudget {
            retention_bytes: 200,
            ..ResourceBudget::compact()
        });
        let mut st = MemoryState::<Accumulator>::new(
            owner,
            OperatorId::new(1),
            StateSlotId::new(1),
            16,
        )
        .unwrap();
        let mut acc = Accumulator::new(AggFn::Min, DataType::Utf8, false).unwrap();
        acc.update(&Scalar::utf8(&"z".repeat(400))).unwrap();
        let err = st
            .put(
                StateKey::new(
                    OperatorId::new(1),
                    StateSlotId::new(1),
                    vec![Scalar::utf8("k")],
                ),
                acc,
                400,
            )
            .unwrap_err();
        assert!(
            err.code == ErrorCode::BoundExceeded || err.code == ErrorCode::ResourceExhausted,
            "{err}"
        );
    }
}

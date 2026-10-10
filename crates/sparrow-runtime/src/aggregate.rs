//! Incremental COUNT / SUM / AVG / MIN / MAX. Accumulators are O(1) per key.
//!
//! Overflow policy ([`sparrow_model::INTEGER_OVERFLOW_POLICY`]):
//! integer SUM uses checked add and fails the job; never wraps.

use sparrow_model::{
    AggFn, DataType, ErrorCode, Result, Scalar, SparrowError, INTEGER_OVERFLOW_POLICY,
};

/// First/last skip SQL NULL; moments use sequential Welford f64 arithmetic.
/// Arrival order is defined by the enclosing window, not wall-clock guesses.
#[derive(Clone, Debug, PartialEq)]
pub enum ExtendedAccumulator {
    Value {
        first: bool,
        value: Option<Scalar>,
    },
    Moment {
        sample: bool,
        sqrt: bool,
        n: u64,
        mean: f64,
        m2: f64,
    },
}
impl ExtendedAccumulator {
    fn update(&mut self, input: &Scalar) -> Result<()> {
        if input.is_null() {
            return Ok(());
        }
        match self {
            Self::Value { first, value } => {
                if !*first || value.is_none() {
                    *value = Some(input.detach_copy());
                }
            }
            Self::Moment { n, mean, m2, .. } => {
                let x = match input {
                    Scalar::Int64(v) => *v as f64,
                    Scalar::UInt64(v) => *v as f64,
                    Scalar::Float64(v) if v.is_finite() => *v,
                    _ => {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            "variance/stddev require finite numeric values",
                        ))
                    }
                };
                let count = n.checked_add(1).ok_or_else(overflow)?;
                let delta = x - *mean;
                let next_mean = *mean + delta / count as f64;
                let next_m2 = *m2 + delta * (x - next_mean);
                if !next_mean.is_finite() || !next_m2.is_finite() {
                    return Err(overflow());
                }
                *n = count;
                *mean = next_mean;
                *m2 = next_m2;
            }
        }
        Ok(())
    }
    fn finish(&self) -> Scalar {
        match self {
            Self::Value { value, .. } => value.clone().unwrap_or(Scalar::Null),
            Self::Moment {
                sample,
                sqrt,
                n,
                m2,
                ..
            } => {
                if *n <= u64::from(*sample) {
                    return Scalar::Null;
                }
                let variance = (*m2 / (*n - u64::from(*sample)) as f64).max(0.0);
                Scalar::Float64(if *sqrt { variance.sqrt() } else { variance })
            }
        }
    }
}

/// Accumulator wire grammar selected by the participant codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AccumulatorCodec {
    /// Participant codec 1: tags 1..=7 only.
    Window,
    /// Participant codec 3: tags 1..=9.
    WindowExt,
}

pub(crate) const ACC_BASE_BYTES: usize = 48;
pub(crate) const EXT_ACC_BASE_BYTES: usize = 128;
const MOMENT_ENCODED_BYTES: usize = 1 + 1 + 8 + 8 + 8;

pub(crate) fn codec1_extended() -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        "window state codec 1 cannot carry FIRST/LAST/VAR/STDDEV accumulator tags 8/9; codec 3 (v29/v30) is required",
    )
    .context("checkpoint_guard", "extended_codec_mismatch")
}

fn ext_invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

/// Resident bytes of a scalar materialized from one validated value encoding.
/// Mirrors `Scalar::resident_bytes` for the non-Dynamic value codec.
pub(crate) fn encoded_scalar_resident(encoded: &[u8]) -> usize {
    let heap = match encoded.first() {
        Some(5 | 6) => encoded.len().saturating_sub(5).saturating_add(48),
        _ => 0,
    };
    std::mem::size_of::<Scalar>().saturating_add(heap)
}

/// Strict scalar value validation for tag 8: a non-NULL, non-Dynamic value
/// whose Bool byte is canonical. Returns the encoded length.
fn check_value_scalar(src: &[u8]) -> Result<usize> {
    match src.first() {
        Some(1) => {
            if !matches!(src.get(1), Some(0 | 1)) {
                return Err(ext_invalid("FIRST/LAST Bool value is not canonical"));
            }
        }
        Some(2..=7) => {}
        _ => return Err(ext_invalid("FIRST/LAST value must be a non-NULL scalar tag 1..=7")),
    }
    let mut cursor = src;
    Scalar::skip_encoded_value(&mut cursor)?;
    Ok(src.len() - cursor.len())
}

impl ExtendedAccumulator {
    fn check_moment(n: u64, mean: f64, m2: f64) -> Result<()> {
        let valid = if n == 0 {
            mean.to_bits() == 0 && m2.to_bits() == 0
        } else {
            mean.is_finite() && m2.is_finite() && m2 >= 0.0 && (n != 1 || m2.to_bits() == 0)
        };
        if !valid {
            return Err(ext_invalid("invalid VAR/STDDEV moment state"));
        }
        Ok(())
    }

    fn encoded_len(&self) -> Result<usize> {
        Ok(match self {
            Self::Value { value, .. } => {
                3 + match value {
                    None => 0,
                    Some(v) => {
                        if v.is_null() || matches!(v, Scalar::Dynamic(_)) {
                            return Err(SparrowError::new(
                                ErrorCode::UnsupportedRestore,
                                "FIRST/LAST state value has no durable scalar encoding",
                            ));
                        }
                        v.encoded_value_len()?
                    }
                }
            }
            Self::Moment { .. } => MOMENT_ENCODED_BYTES,
        })
    }

    /// Tag 8: `[8][mode 0=FIRST,1=LAST][has 0|1][scalar tag 1..=7]`.
    /// Tag 9: `[9][sample | sqrt<<1][n u64][mean f64 bits][m2 f64 bits]`.
    /// The encoder applies the decoder's validator; invalid state never lands.
    fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Value { first, value } => {
                let start = out.len();
                out.push(8);
                out.push(u8::from(!*first));
                match value {
                    None => out.push(0),
                    Some(v) => {
                        if v.is_null() || matches!(v, Scalar::Dynamic(_)) {
                            out.truncate(start);
                            return Err(SparrowError::new(
                                ErrorCode::UnsupportedRestore,
                                "FIRST/LAST state value has no durable scalar encoding",
                            ));
                        }
                        if let Scalar::Utf8(_) | Scalar::Bytes(_) = v {
                            if v.encoded_value_len()? - 5 > u32::MAX as usize {
                                out.truncate(start);
                                return Err(SparrowError::new(
                                    ErrorCode::BoundExceeded,
                                    "FIRST/LAST value exceeds the scalar codec length",
                                ));
                            }
                        }
                        out.push(1);
                        if let Err(error) = v.encode_value(out) {
                            out.truncate(start);
                            return Err(error);
                        }
                        if let Err(error) = check_value_scalar(&out[start + 3..]) {
                            out.truncate(start);
                            return Err(error);
                        }
                    }
                }
            }
            Self::Moment { sample, sqrt, n, mean, m2 } => {
                Self::check_moment(*n, *mean, *m2)?;
                out.push(9);
                out.push(u8::from(*sample) | (u8::from(*sqrt) << 1));
                out.extend_from_slice(&n.to_le_bytes());
                out.extend_from_slice(&mean.to_bits().to_le_bytes());
                out.extend_from_slice(&m2.to_bits().to_le_bytes());
            }
        }
        Ok(())
    }

    /// Validates tags 8/9 and returns the materialized `tracked_bytes`.
    fn skip_encoded(src: &mut &[u8]) -> Result<usize> {
        let truncated = || ext_invalid("truncated extended accumulator");
        match src.first() {
            Some(8) => {
                if src.len() < 3 {
                    return Err(truncated());
                }
                if src[1] > 1 {
                    return Err(ext_invalid("invalid FIRST/LAST mode"));
                }
                match src[2] {
                    0 => {
                        *src = &src[3..];
                        Ok(EXT_ACC_BASE_BYTES)
                    }
                    1 => {
                        let len = check_value_scalar(&src[3..])?;
                        let resident = encoded_scalar_resident(&src[3..3 + len]);
                        *src = &src[3 + len..];
                        Ok(EXT_ACC_BASE_BYTES.saturating_add(resident))
                    }
                    _ => Err(ext_invalid("invalid FIRST/LAST presence flag")),
                }
            }
            Some(9) => {
                if src.len() < MOMENT_ENCODED_BYTES {
                    return Err(truncated());
                }
                if src[1] > 3 {
                    return Err(ext_invalid("invalid VAR/STDDEV mode"));
                }
                let n = u64::from_le_bytes(src[2..10].try_into().unwrap());
                let mean = f64::from_bits(u64::from_le_bytes(src[10..18].try_into().unwrap()));
                let m2 = f64::from_bits(u64::from_le_bytes(src[18..26].try_into().unwrap()));
                Self::check_moment(n, mean, m2)?;
                *src = &src[MOMENT_ENCODED_BYTES..];
                Ok(EXT_ACC_BASE_BYTES)
            }
            _ => Err(ext_invalid("unknown extended accumulator tag")),
        }
    }

    fn decode(src: &mut &[u8]) -> Result<Self> {
        let mut cursor = *src;
        Self::skip_encoded(&mut cursor)?;
        let tag = src[0];
        let decoded = if tag == 8 {
            let first = src[1] == 0;
            let value = if src[2] == 1 {
                let mut value_src = &src[3..];
                Some(Scalar::decode_value(&mut value_src)?)
            } else {
                None
            };
            Self::Value { first, value }
        } else {
            Self::Moment {
                sample: src[1] & 1 != 0,
                sqrt: src[1] & 2 != 0,
                n: u64::from_le_bytes(src[2..10].try_into().unwrap()),
                mean: f64::from_bits(u64::from_le_bytes(src[10..18].try_into().unwrap())),
                m2: f64::from_bits(u64::from_le_bytes(src[18..26].try_into().unwrap())),
            }
        };
        *src = cursor;
        Ok(decoded)
    }

    /// Participant-level checks after codec validation. Mismatch is an
    /// incompatible restore, never corruption fallback.
    pub(crate) fn check_restore(&self, func: AggFn, input: &DataType, count: Option<u64>) -> Result<()> {
        let mismatch = |message: &str| {
            Err(SparrowError::new(ErrorCode::UnsupportedRestore, message.to_string())
                .context("checkpoint_guard", "extended_state_mismatch"))
        };
        match self {
            Self::Value { first, value } => {
                if !matches!(func, AggFn::First | AggFn::Last) || *first != (func == AggFn::First) {
                    return mismatch("restored FIRST/LAST mode differs from the live aggregate");
                }
                if let Some(v) = value {
                    if v.is_null() || !v.matches_type(input) {
                        return mismatch("restored FIRST/LAST value type differs from the live input type");
                    }
                }
                if value.is_some() && count == Some(0) {
                    return mismatch("restored FIRST/LAST value exceeds the window row count");
                }
            }
            Self::Moment { sample, sqrt, n, .. } => {
                let expected = match func {
                    AggFn::VarPop => (false, false),
                    AggFn::VarSamp => (true, false),
                    AggFn::StddevPop => (false, true),
                    AggFn::StddevSamp => (true, true),
                    _ => return mismatch("restored moment state has no matching live aggregate"),
                };
                if (*sample, *sqrt) != expected {
                    return mismatch("restored VAR/STDDEV mode differs from the live aggregate");
                }
                if count.is_some_and(|rows| *n > rows) {
                    return mismatch("restored moment count exceeds the window row count");
                }
            }
        }
        Ok(())
    }
}

/// One incremental accumulator. Never stores input rows.
#[derive(Clone, Debug, PartialEq)]
pub enum Accumulator {
    /// Cold boxed family: keep the legacy accumulator inline layout unchanged.
    Extended(Box<ExtendedAccumulator>),
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
        if matches!(
            func,
            AggFn::VarPop | AggFn::VarSamp | AggFn::StddevPop | AggFn::StddevSamp
        ) && !matches!(
            ty,
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Null
        ) {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "variance/stddev require numeric inputs",
            ));
        }
        Ok(match func {
            AggFn::First | AggFn::Last => Self::Extended(Box::new(ExtendedAccumulator::Value {
                first: func == AggFn::First,
                value: None,
            })),
            AggFn::VarPop | AggFn::VarSamp | AggFn::StddevPop | AggFn::StddevSamp => {
                Self::Extended(Box::new(ExtendedAccumulator::Moment {
                    sample: matches!(func, AggFn::VarSamp | AggFn::StddevSamp),
                    sqrt: matches!(func, AggFn::StddevPop | AggFn::StddevSamp),
                    n: 0,
                    mean: 0.0,
                    m2: 0.0,
                }))
            }
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
        const BASE: usize = ACC_BASE_BYTES;
        match self {
            Self::Extended(extra) => {
                EXT_ACC_BASE_BYTES + match extra.as_ref() {
                    ExtendedAccumulator::Value { value, .. } => {
                        value.as_ref().map_or(0, Scalar::resident_bytes)
                    }
                    _ => 0,
                }
            }
            Self::Min { v } | Self::Max { v } => {
                BASE + v.as_ref().map(Scalar::resident_bytes).unwrap_or(0)
            }
            Self::Count { .. }
            | Self::SumI64 { .. }
            | Self::SumU64 { .. }
            | Self::SumF64 { .. }
            | Self::Avg { .. } => BASE,
        }
    }

    pub fn update(&mut self, value: &Scalar) -> Result<()> {
        match self {
            Self::Extended(extra) => extra.update(value),
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
                if value.is_null() || value.is_nan() {
                    return Ok(());
                }
                match v {
                    None => *v = Some(value.detach_copy()),
                    Some(cur) if cur.is_nan() => *v = Some(value.detach_copy()),
                    Some(cur) => {
                        if cmp_ord(value, cur)? == Some(std::cmp::Ordering::Less) {
                            *v = Some(value.detach_copy());
                        }
                    }
                }
                Ok(())
            }
            Self::Max { v } => {
                if value.is_null() || value.is_nan() {
                    return Ok(());
                }
                match v {
                    None => *v = Some(value.detach_copy()),
                    Some(cur) if cur.is_nan() => *v = Some(value.detach_copy()),
                    Some(cur) => {
                        if cmp_ord(value, cur)? == Some(std::cmp::Ordering::Greater) {
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
            Self::Extended(extra) => extra.finish(),
            Self::Count {
                rows,
                non_null,
                star,
            } => Scalar::Int64(if *star {
                *rows as i64
            } else {
                *non_null as i64
            }),
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

    /// Codec 1 (WindowFreeze) encoding. Tags 8/9 are never written here.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        self.encode_codec(out, AccumulatorCodec::Window)
    }

    pub(crate) fn encode_codec(&self, out: &mut Vec<u8>, codec: AccumulatorCodec) -> Result<()> {
        match self {
            Self::Extended(extra) => {
                if codec != AccumulatorCodec::WindowExt {
                    return Err(codec1_extended());
                }
                extra.encode(out)?;
            }
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

    /// Exact codec 1 wire length.
    pub fn encoded_len(&self) -> Result<usize> {
        self.encoded_len_codec(AccumulatorCodec::Window)
    }

    pub(crate) fn encoded_len_codec(&self, codec: AccumulatorCodec) -> Result<usize> {
        Ok(match self {
            Self::Extended(extra) => {
                if codec != AccumulatorCodec::WindowExt {
                    return Err(codec1_extended());
                }
                extra.encoded_len()?
            }
            Self::Count { .. } | Self::Avg { .. } => 18,
            Self::SumI64 { .. } | Self::SumU64 { .. } | Self::SumF64 { .. } => 17,
            Self::Min { v } | Self::Max { v } => {
                2 + v
                    .as_ref()
                    .map(Scalar::encoded_value_len)
                    .transpose()?
                    .unwrap_or(0)
            }
        })
    }

    /// Validate and advance one accumulator without materializing it. Returns
    /// the exact [`Self::tracked_bytes`] the materialized value would have, so
    /// restore credit can be reserved before any state is built.
    pub(crate) fn skip_encoded_codec(src: &mut &[u8], codec: AccumulatorCodec) -> Result<usize> {
        let invalid = || {
            SparrowError::new(
                ErrorCode::CodecViolation,
                "invalid or truncated accumulator encoding",
            )
        };
        let Some((&tag, tail)) = src.split_first() else {
            return Err(invalid());
        };
        if matches!(tag, 8 | 9) {
            if codec != AccumulatorCodec::WindowExt {
                return Err(codec1_extended());
            }
            return ExtendedAccumulator::skip_encoded(src);
        }
        *src = tail;
        let size = match tag {
            1 | 5 => 17,
            2 | 3 | 4 => 16,
            6 | 7 => {
                let Some((&option, tail)) = src.split_first() else {
                    return Err(invalid());
                };
                *src = tail;
                return match option {
                    0 => Ok(ACC_BASE_BYTES),
                    1 => {
                        let before = *src;
                        Scalar::skip_encoded_value(src)?;
                        Ok(ACC_BASE_BYTES
                            .saturating_add(encoded_scalar_resident(&before[..before.len() - src.len()])))
                    }
                    _ => Err(invalid()),
                };
            }
            _ => return Err(invalid()),
        };
        if src.len() < size {
            return Err(invalid());
        }
        *src = &src[size..];
        Ok(ACC_BASE_BYTES)
    }

    /// Codec 1 decode. Tags 8/9 are rejected.
    pub fn decode(src: &mut &[u8]) -> Result<Self> {
        Self::decode_codec(src, AccumulatorCodec::Window)
    }

    pub(crate) fn decode_codec(src: &mut &[u8], codec: AccumulatorCodec) -> Result<Self> {
        if src.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "truncated accumulator",
            ));
        }
        let tag = src[0];
        if matches!(tag, 8 | 9) {
            if codec != AccumulatorCodec::WindowExt {
                return Err(codec1_extended());
            }
            return Ok(Self::Extended(Box::new(ExtendedAccumulator::decode(src)?)));
        }
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

fn cmp_ord(a: &Scalar, b: &Scalar) -> Result<Option<std::cmp::Ordering>> {
    match (a, b) {
        (Scalar::Int64(x), Scalar::Int64(y)) => Ok(Some(x.cmp(y))),
        (Scalar::UInt64(x), Scalar::UInt64(y)) => Ok(Some(x.cmp(y))),
        (Scalar::Int64(x), Scalar::UInt64(y)) if *x >= 0 => Ok(Some((*x as u64).cmp(y))),
        (Scalar::UInt64(x), Scalar::Int64(y)) if *y >= 0 => Ok(Some(x.cmp(&(*y as u64)))),
        (Scalar::TimestampMicrosUTC(x), Scalar::TimestampMicrosUTC(y)) => Ok(Some(x.cmp(y))),
        (Scalar::TimestampMicrosUTC(x), Scalar::Int64(y)) => Ok(Some(x.cmp(y))),
        (Scalar::Int64(x), Scalar::TimestampMicrosUTC(y)) => Ok(Some(x.cmp(y))),
        (Scalar::Float64(x), Scalar::Float64(y)) => Ok(x.partial_cmp(y)),
        (Scalar::Utf8(x), Scalar::Utf8(y)) => Ok(Some(x.as_ref().cmp(y.as_ref()))),
        (Scalar::Bytes(x), Scalar::Bytes(y)) => Ok(Some(x.as_ref().cmp(y.as_ref()))),
        (Scalar::Bool(x), Scalar::Bool(y)) => Ok(Some(x.cmp(y))),
        _ => Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            format!(
                "MIN/MAX cannot compare {} and {}",
                a.data_type(),
                b.data_type()
            ),
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

        let mut mn = Accumulator::new(AggFn::Min, DataType::Float64, false).unwrap();
        mn.update(&Scalar::Float64(f64::NAN)).unwrap();
        mn.update(&Scalar::Float64(10.0)).unwrap();
        mn.update(&Scalar::Float64(f64::NAN)).unwrap();
        mn.update(&Scalar::Float64(3.0)).unwrap();
        assert_eq!(mn.finish(), Scalar::Float64(3.0));
        let mut mx = Accumulator::new(AggFn::Max, DataType::Float64, false).unwrap();
        mx.update(&Scalar::Float64(f64::NAN)).unwrap();
        mx.update(&Scalar::Float64(10.0)).unwrap();
        mx.update(&Scalar::Float64(30.0)).unwrap();
        assert_eq!(mx.finish(), Scalar::Float64(30.0));
    }

    #[test]
    fn r05_min_max_tracked_bytes_include_payload() {
        let mut mn = Accumulator::new(AggFn::Min, DataType::Utf8, false).unwrap();
        assert_eq!(mn.tracked_bytes(), 48);
        mn.update(&Scalar::utf8("xxxxxxxxxxxxxxxxxxxxxxxx"))
            .unwrap();
        assert!(
            mn.tracked_bytes() > 48,
            "MIN must bill the stored utf8 payload, got {}",
            mn.tracked_bytes()
        );
        let mut mx = Accumulator::new(AggFn::Max, DataType::Utf8, false).unwrap();
        mx.update(&Scalar::utf8("yyyyyyyyyyyyyyyyyyyyyyyy"))
            .unwrap();
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
        let mut st =
            MemoryState::<Accumulator>::new(owner, OperatorId::new(1), StateSlotId::new(1), 16)
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

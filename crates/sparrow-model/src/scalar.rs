//! Provisional scalar values for the RowBatch layout.
//!
//! Nested Array/Struct/Map values are *not* first-class here; they may appear
//! only inside [`DynamicValue`]. Layout is provisional until the G1a decision
//! (see `docs/adr/003-layout-decision.md`).

use std::sync::Arc;

use crate::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(Arc<str>),
    Bytes(Arc<[u8]>),
    TimestampMicrosUTC(i64),
    Dynamic(DynamicValue),
}

impl Scalar {
    /// Event-time micros from Int64 or TimestampMicrosUTC. Other types are None.
    pub fn as_event_time_micros(&self) -> Option<i64> {
        match self {
            Self::Int64(v) => Some(*v),
            Self::TimestampMicrosUTC(v) => Some(*v),
            Self::UInt64(v) if *v <= i64::MAX as u64 => Some(*v as i64),
            _ => None,
        }
    }

    pub fn utf8(s: impl AsRef<str>) -> Self {
        Self::Utf8(Arc::from(s.as_ref()))
    }

    pub fn bytes(b: impl AsRef<[u8]>) -> Self {
        Self::Bytes(Arc::from(b.as_ref()))
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool(_) => DataType::Bool,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Bytes(_) => DataType::Bytes,
            Self::TimestampMicrosUTC(_) => DataType::TimestampMicrosUTC,
            Self::Dynamic(_) => DataType::Dynamic,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null) || matches!(self, Self::Dynamic(DynamicValue::Null))
    }

    /// Estimated tracked size, including string/bytes payload.
    pub fn tracked_bytes(&self) -> usize {
        const TAG: usize = 16;
        match self {
            Self::Null | Self::Bool(_) | Self::Int64(_) | Self::UInt64(_) | Self::Float64(_)
            | Self::TimestampMicrosUTC(_) => TAG,
            Self::Utf8(s) => TAG + s.len(),
            Self::Bytes(b) => TAG + b.len(),
            Self::Dynamic(d) => TAG + d.tracked_bytes(),
        }
    }

    /// Deep-copy variable-length payloads so detached state does not alias
    /// the live batch allocation.
    pub fn detach_copy(&self) -> Self {
        match self {
            Self::Utf8(s) => Self::Utf8(Arc::<str>::from(s.as_ref().to_owned())),
            Self::Bytes(b) => Self::Bytes(Arc::<[u8]>::from(b.to_vec())),
            Self::Dynamic(d) => Self::Dynamic(d.detach_copy()),
            other => other.clone(),
        }
    }

    /// Stable key encoding for keyed state (NaN-safe via to_bits).
    pub fn encode_key(&self, out: &mut Vec<u8>) {
        match self {
            Self::Null => out.push(0),
            Self::Bool(v) => {
                out.push(1);
                out.push(u8::from(*v));
            }
            Self::Int64(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Self::UInt64(v) => {
                out.push(3);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Self::Float64(v) => {
                out.push(4);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            Self::Utf8(s) => {
                out.push(5);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Self::Bytes(b) => {
                out.push(6);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
            Self::TimestampMicrosUTC(v) => {
                out.push(7);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Self::Dynamic(_) => {
                out.push(8);
                out.extend_from_slice(&(self.tracked_bytes() as u64).to_le_bytes());
            }
        }
    }

    /// Reversible value encoding used by aligned checkpoint snapshots.
    /// Dynamic values are rejected (not a V1 state key type).
    pub fn encode_value(&self, out: &mut Vec<u8>) -> crate::error::Result<()> {
        use crate::error::{ErrorCode, SparrowError};
        if matches!(self, Self::Dynamic(_)) {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "Dynamic scalars cannot be snapshotted in V1 aligned checkpoint",
            ));
        }
        self.encode_key(out);
        Ok(())
    }

    pub fn decode_value(src: &mut &[u8]) -> crate::error::Result<Self> {
        use crate::error::{ErrorCode, SparrowError};
        let take = |src: &mut &[u8], n: usize| -> crate::error::Result<Vec<u8>> {
            if src.len() < n {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "truncated scalar in checkpoint snapshot",
                ));
            }
            let (head, rest) = src.split_at(n);
            *src = rest;
            Ok(head.to_vec())
        };
        if src.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "empty scalar in checkpoint snapshot",
            ));
        }
        let tag = src[0];
        *src = &src[1..];
        match tag {
            0 => Ok(Self::Null),
            1 => {
                let b = take(src, 1)?;
                Ok(Self::Bool(b[0] != 0))
            }
            2 => {
                let b = take(src, 8)?;
                Ok(Self::Int64(i64::from_le_bytes(b.try_into().unwrap())))
            }
            3 => {
                let b = take(src, 8)?;
                Ok(Self::UInt64(u64::from_le_bytes(b.try_into().unwrap())))
            }
            4 => {
                let b = take(src, 8)?;
                Ok(Self::Float64(f64::from_bits(u64::from_le_bytes(
                    b.try_into().unwrap(),
                ))))
            }
            5 => {
                let n = u32::from_le_bytes(take(src, 4)?.try_into().unwrap()) as usize;
                let bytes = take(src, n)?;
                let s = std::str::from_utf8(&bytes).map_err(|_| {
                    SparrowError::new(ErrorCode::CodecViolation, "utf8 scalar not valid UTF-8")
                })?;
                Ok(Self::utf8(s))
            }
            6 => {
                let n = u32::from_le_bytes(take(src, 4)?.try_into().unwrap()) as usize;
                let bytes = take(src, n)?;
                Ok(Self::bytes(bytes))
            }
            7 => {
                let b = take(src, 8)?;
                Ok(Self::TimestampMicrosUTC(i64::from_le_bytes(
                    b.try_into().unwrap(),
                )))
            }
            8 => Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "Dynamic scalars cannot be restored from V0.4 experimental checkpoint",
            )),
            other => Err(SparrowError::new(
                ErrorCode::CodecViolation,
                format!("unknown scalar tag {other}"),
            )),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int64(v) => Some(*v as f64),
            Self::UInt64(v) => Some(*v as f64),
            Self::Float64(v) => Some(*v),
            Self::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DynamicValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(Arc<str>),
    Bytes(Arc<[u8]>),
    Array(Arc<[DynamicValue]>),
    Object(Arc<[(Arc<str>, DynamicValue)]>),
}

impl DynamicValue {
    pub fn utf8(s: impl AsRef<str>) -> Self {
        Self::Utf8(Arc::from(s.as_ref()))
    }

    pub fn object(fields: Vec<(impl AsRef<str>, DynamicValue)>) -> Self {
        let pairs: Vec<(Arc<str>, DynamicValue)> = fields
            .into_iter()
            .map(|(k, v)| (Arc::from(k.as_ref()), v))
            .collect();
        Self::Object(Arc::from(pairs))
    }

    pub fn get(&self, key: &str) -> Option<&DynamicValue> {
        match self {
            Self::Object(pairs) => pairs.iter().find(|(k, _)| k.as_ref() == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn tracked_bytes(&self) -> usize {
        match self {
            Self::Null | Self::Bool(_) | Self::Int64(_) | Self::UInt64(_) | Self::Float64(_) => 8,
            Self::Utf8(s) => 8 + s.len(),
            Self::Bytes(b) => 8 + b.len(),
            Self::Array(items) => 8 + items.iter().map(Self::tracked_bytes).sum::<usize>(),
            Self::Object(pairs) => {
                8 + pairs
                    .iter()
                    .map(|(k, v)| k.len() + v.tracked_bytes())
                    .sum::<usize>()
            }
        }
    }

    pub fn detach_copy(&self) -> Self {
        match self {
            Self::Utf8(s) => Self::Utf8(Arc::<str>::from(s.as_ref().to_owned())),
            Self::Bytes(b) => Self::Bytes(Arc::<[u8]>::from(b.to_vec())),
            Self::Array(items) => {
                let copy: Vec<DynamicValue> = items.iter().map(Self::detach_copy).collect();
                Self::Array(Arc::from(copy))
            }
            Self::Object(pairs) => {
                let copy: Vec<(Arc<str>, DynamicValue)> = pairs
                    .iter()
                    .map(|(k, v)| (Arc::<str>::from(k.as_ref().to_owned()), v.detach_copy()))
                    .collect();
                Self::Object(Arc::from(copy))
            }
            other => other.clone(),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int64(v) => Some(*v as f64),
            Self::UInt64(v) => Some(*v as f64),
            Self::Float64(v) => Some(*v),
            Self::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_extract_and_detach() {
        let v = DynamicValue::object(vec![("temp", DynamicValue::Float64(21.5))]);
        assert_eq!(v.get("temp"), Some(&DynamicValue::Float64(21.5)));
        let detached = v.detach_copy();
        assert_eq!(detached, v);
    }
}

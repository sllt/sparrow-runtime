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

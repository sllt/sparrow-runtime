//! Provisional scalar values for the RowBatch layout.
//!
//! Nested Array/Struct/Map values are *not* first-class here; they may appear
//! only inside [`DynamicValue`]. Layout is provisional until the G1a decision
//! (see `docs/adr/003-layout-decision.md`).

use std::sync::Arc;

use crate::error::Result;
use crate::memory::MemoryOwner;
use crate::resource::CreditKind;
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
    pub(crate) fn resident_heap_bytes(&self) -> usize {
        match self {
            Self::Utf8(v) => v.len().saturating_add(48),
            Self::Bytes(v) => v.len().saturating_add(48),
            Self::Dynamic(v) => v.resident_heap_bytes(),
            _ => 0,
        }
    }
    /// Event-time micros from Int64 or TimestampMicrosUTC. Other types are None.
    pub fn as_event_time_micros(&self) -> Option<i64> {
        match self {
            Self::Int64(v) => Some(*v),
            Self::TimestampMicrosUTC(v) => Some(*v),
            Self::UInt64(v) if *v <= i64::MAX as u64 => Some(*v as i64),
            _ => None,
        }
    }

    /// Untracked UTF-8 constructor. The payload is **not** charged to a
    /// [`MemoryOwner`] until a builder `push`es the row.
    ///
    /// Connector / runtime ingress should prefer [`Self::utf8_tracked`].
    /// A full arena rewrite is out of scope (A2 leftover).
    pub fn utf8(s: impl AsRef<str>) -> Self {
        Self::Utf8(Arc::from(s.as_ref()))
    }

    /// Untracked bytes constructor. Prefer [`Self::bytes_tracked`] on
    /// connector / runtime ingress.
    pub fn bytes(b: impl AsRef<[u8]>) -> Self {
        Self::Bytes(Arc::from(b.as_ref()))
    }

    /// Allocate UTF-8 after checking reservation headroom on `owner`.
    /// Does not hold a lease (no arena); builder `push` still charges the row.
    pub fn utf8_tracked(owner: &MemoryOwner, s: impl AsRef<str>) -> Result<Self> {
        let s = s.as_ref();
        owner.ensure_headroom(CreditKind::Reservation, 16usize.saturating_add(s.len()))?;
        Ok(Self::utf8(s))
    }

    /// Allocate bytes after checking reservation headroom on `owner`.
    pub fn bytes_tracked(owner: &MemoryOwner, b: impl AsRef<[u8]>) -> Result<Self> {
        let b = b.as_ref();
        owner.ensure_headroom(CreditKind::Reservation, 16usize.saturating_add(b.len()))?;
        Ok(Self::bytes(b))
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

    pub fn matches_type(&self, ty: &DataType) -> bool {
        if matches!(ty, DataType::Dynamic) || self.data_type() == *ty { return true; }
        match self {
            Self::Dynamic(value) if ty.is_nested() => value.matches_type(ty, 0),
            _ => false,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null) || matches!(self, Self::Dynamic(DynamicValue::Null))
    }

    /// IEEE NaN (typed Float64 or Dynamic float). Comparisons treat this as
    /// unordered; MIN/MAX skip it like NULL.
    pub fn is_nan(&self) -> bool {
        match self {
            Self::Float64(v) => v.is_nan(),
            Self::Dynamic(DynamicValue::Float64(v)) => v.is_nan(),
            _ => false,
        }
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

    /// Structural key encoding. Signed zero and NaN representations are canonical.
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
                let bits = if *v == 0.0 { 0 } else if v.is_nan() { f64::NAN.to_bits() } else { v.to_bits() };
                out.extend_from_slice(&bits.to_le_bytes());
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
            Self::Dynamic(value) => {
                out.push(8);
                value.encode_key(out);
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
        // Value codecs preserve exact floating bits; only state-key equality
        // canonicalizes them. Existing SPV1 value bytes remain compatible.
        if let Self::Float64(value) = self {
            out.push(4);
            out.extend_from_slice(&value.to_bits().to_le_bytes());
            return Ok(());
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
    fn resident_heap_bytes(&self) -> usize {
        match self {
            Self::Utf8(v) => v.len().saturating_add(48),
            Self::Bytes(v) => v.len().saturating_add(48),
            Self::Array(v) => v.iter().fold(48usize.saturating_add(v.len().saturating_mul(std::mem::size_of::<Self>())),
                |n, x| n.saturating_add(x.resident_heap_bytes())),
            Self::Object(v) => v.iter().fold(48usize.saturating_add(v.len().saturating_mul(std::mem::size_of::<(Arc<str>, Self)>())),
                |n, (k, x)| n.saturating_add(k.len()).saturating_add(48).saturating_add(x.resident_heap_bytes())),
            _ => 0,
        }
    }
    fn encode_key(&self, out: &mut Vec<u8>) {
        match self {
            Self::Null => Scalar::Null.encode_key(out),
            Self::Bool(v) => Scalar::Bool(*v).encode_key(out),
            Self::Int64(v) => Scalar::Int64(*v).encode_key(out),
            Self::UInt64(v) => Scalar::UInt64(*v).encode_key(out),
            Self::Float64(v) => Scalar::Float64(*v).encode_key(out),
            Self::Utf8(v) => Scalar::Utf8(v.clone()).encode_key(out),
            Self::Bytes(v) => Scalar::Bytes(v.clone()).encode_key(out),
            Self::Array(values) => {
                out.push(9);
                out.extend_from_slice(&(values.len() as u64).to_le_bytes());
                for value in values.iter() { value.encode_key(out); }
            }
            Self::Object(values) => {
                out.push(10);
                out.extend_from_slice(&(values.len() as u64).to_le_bytes());
                let mut sorted: Vec<_> = values.iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                for (name, value) in sorted {
                    Scalar::Utf8(name.clone()).encode_key(out);
                    value.encode_key(out);
                }
            }
        }
    }

    fn matches_type(&self, ty: &DataType, depth: usize) -> bool {
        if depth > 64 { return false; }
        if matches!(self, Self::Null) || matches!(ty, DataType::Dynamic) { return true; }
        match (self, ty) {
            (Self::Bool(_), DataType::Bool) | (Self::Int64(_), DataType::Int64 | DataType::TimestampMicrosUTC)
            | (Self::UInt64(_), DataType::UInt64) | (Self::Float64(_), DataType::Float64)
            | (Self::Utf8(_), DataType::Utf8) | (Self::Bytes(_), DataType::Bytes) => true,
            (Self::Array(values), DataType::Array(inner)) => values.iter().all(|v| v.matches_type(inner, depth + 1)),
            (Self::Object(values), DataType::Struct(fields)) => {
                Self::unique_object_keys(values)
                && values.len() == fields.len() && fields.iter().all(|f| {
                    values.iter().find(|(k, _)| k.as_ref() == f.name).map_or(false, |(_, v)|
                        (f.nullable || !matches!(v, Self::Null)) && v.matches_type(&f.data_type, depth + 1))
                })
            }
            (Self::Object(values), DataType::Map { key, value }) => Self::unique_object_keys(values) && values.iter().all(|(k,v)| {
                let valid_key = match key.as_ref() {
                    DataType::Utf8 | DataType::Dynamic => true,
                    DataType::Int64 => k.parse::<i64>().is_ok(),
                    DataType::UInt64 => k.parse::<u64>().is_ok(),
                    _ => false,
                };
                valid_key && v.matches_type(value, depth + 1)
            }),
            _ => false,
        }
    }

    // Object is public: non-JSON callers may bypass try_object. Validate
    // uniqueness, not insertion order, at the typed ingress boundary.
    fn unique_object_keys(values: &[(Arc<str>, DynamicValue)]) -> bool {
        let mut seen = std::collections::HashSet::with_capacity(values.len());
        values.iter().all(|(key, _)| seen.insert(key.as_ref()))
    }

    pub fn utf8(s: impl AsRef<str>) -> Self {
        Self::Utf8(Arc::from(s.as_ref()))
    }

    pub fn object(fields: Vec<(impl AsRef<str>, DynamicValue)>) -> Self {
        match Self::try_object(fields) {
            Ok(v) => v,
            Err((_, last_wins)) => last_wins,
        }
    }

    /// Fail closed on duplicate keys (P2-40). `Err` carries the last-wins object
    /// for callers that must still materialize a value.
    pub fn try_object(
        fields: Vec<(impl AsRef<str>, DynamicValue)>,
    ) -> std::result::Result<Self, (crate::error::SparrowError, Self)> {
        let mut pairs: Vec<(Arc<str>, DynamicValue)> = Vec::with_capacity(fields.len());
        let mut dup = false;
        for (k, v) in fields {
            let key = Arc::<str>::from(k.as_ref());
            if let Some(pos) = pairs.iter().position(|(ek, _)| ek.as_ref() == key.as_ref()) {
                pairs[pos].1 = v;
                dup = true;
            } else {
                pairs.push((key, v));
            }
        }
        let obj = Self::Object(Arc::from(pairs));
        if dup {
            Err((
                crate::error::SparrowError::new(
                    crate::error::ErrorCode::InvalidArgument,
                    "dynamic object has duplicate keys",
                ),
                obj,
            ))
        } else {
            Ok(obj)
        }
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
    fn r3_state_keys_encode_content_and_canonical_float_equality() {
        let encode = |value: Scalar| { let mut bytes = Vec::new(); value.encode_key(&mut bytes); bytes };
        let a = Scalar::Dynamic(DynamicValue::object(vec![("aa", DynamicValue::utf8("bb"))]));
        let b = Scalar::Dynamic(DynamicValue::object(vec![("cc", DynamicValue::utf8("dd"))]));
        assert_eq!(a.tracked_bytes(), b.tracked_bytes());
        assert_ne!(encode(a), encode(b));
        assert_eq!(encode(Scalar::Float64(-0.0)), encode(Scalar::Float64(0.0)));
        assert_eq!(encode(Scalar::Float64(f64::NAN)), encode(Scalar::Float64(f64::from_bits(0x7ff8000000000001))));
        let mut value = Vec::new();
        Scalar::Float64(-0.0).encode_value(&mut value).unwrap();
        let decoded = Scalar::decode_value(&mut value.as_slice()).unwrap();
        let Scalar::Float64(decoded) = decoded else { panic!("float expected") };
        assert_eq!(decoded.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn dynamic_extract_and_detach() {
        let v = DynamicValue::object(vec![("temp", DynamicValue::Float64(21.5))]);
        assert_eq!(v.get("temp"), Some(&DynamicValue::Float64(21.5)));
        let detached = v.detach_copy();
        assert_eq!(detached, v);
    }

    #[test]
    fn a2_utf8_tracked_refuses_without_reservation_headroom() {
        let owner = crate::memory::MemoryOwner::new(crate::resource::ResourceBudget {
            reservation_bytes: 32,
            retention_bytes: 32,
            queue_bytes: 32,
            max_rows: 4,
            work_units: 10,
            max_state_keys: 4,
            max_timers: 4,
        });
        let err = Scalar::utf8_tracked(&owner, "this string is far too long for 32B")
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
        let ok = Scalar::utf8_tracked(&owner, "ok").unwrap();
        assert_eq!(ok, Scalar::utf8("ok"));
        assert_eq!(owner.usage().reservation_bytes, 0);
    }

    #[test]
    fn p2_40_duplicate_dynamic_keys_fail_closed() {
        let err = DynamicValue::try_object(vec![
            ("k", DynamicValue::Int64(1)),
            ("k", DynamicValue::Int64(2)),
        ]);
        assert!(err.is_err(), "duplicate keys must not succeed");
        let (e, last) = err.unwrap_err();
        assert_eq!(e.code, crate::error::ErrorCode::InvalidArgument);
        assert_eq!(last.get("k"), Some(&DynamicValue::Int64(2)));
    }
}

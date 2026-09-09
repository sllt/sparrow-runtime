//! JSON object ↔ [`Row`] with size/depth caps and schema checks.

use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{DataType, DynamicValue, Row, Scalar, Schema, SourceFrame};

/// What to do with a record that fails size, depth, or schema checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BadRecordPolicy {
    /// Skip the record and emit a diagnostic. Default for live_best_effort.
    Drop,
    /// Fail the job. Use only when the operator must not continue.
    FailJob,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JsonLimits {
    pub max_bytes: usize,
    pub max_depth: usize,
}

impl Default for JsonLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024,
            max_depth: 8,
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonCodec {
    pub schema: Schema,
    pub limits: JsonLimits,
    pub policy: BadRecordPolicy,
}

impl JsonCodec {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            limits: JsonLimits::default(),
            policy: BadRecordPolicy::Drop,
        }
    }

    pub fn decode_frame(&self, frame: &SourceFrame) -> Result<Option<Row>> {
        match decode_json_row(&self.schema, &frame.payload, &self.limits) {
            Ok(row) => Ok(Some(row)),
            Err(err) => match self.policy {
                BadRecordPolicy::Drop => Ok(None),
                BadRecordPolicy::FailJob => Err(err),
            },
        }
    }

    pub fn encode_row(&self, row: &Row) -> Result<Vec<u8>> {
        encode_json_row(&self.schema, row)
    }
}

pub fn decode_json_row(schema: &Schema, bytes: &[u8], limits: &JsonLimits) -> Result<Row> {
    if bytes.len() > limits.max_bytes {
        return Err(SparrowError::new(
            ErrorCode::MaxRecordSize,
            format!("JSON record {}B exceeds max_bytes {}", bytes.len(), limits.max_bytes),
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
        SparrowError::new(ErrorCode::CodecViolation, format!("invalid JSON: {e}"))
    })?;
    let depth = json_depth(&value);
    if depth > limits.max_depth {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!("JSON depth {depth} exceeds max_depth {}", limits.max_depth),
        ));
    }
    let obj = value.as_object().ok_or_else(|| {
        SparrowError::new(ErrorCode::CodecViolation, "JSON root must be an object")
    })?;
    let mut values = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        match obj.get(&field.name) {
            None if field.nullable => values.push(Scalar::Null),
            None => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("missing required field '{}'", field.name),
                ));
            }
            Some(v) => values.push(json_to_scalar(v, &field.data_type, field.nullable)?),
        }
    }
    Ok(Row { values })
}

pub fn encode_json_row(schema: &Schema, row: &Row) -> Result<Vec<u8>> {
    if row.values.len() != schema.fields.len() {
        return Err(SparrowError::new(
            ErrorCode::InvalidSchema,
            "row/schema arity mismatch",
        ));
    }
    let mut map = serde_json::Map::new();
    for (field, value) in schema.fields.iter().zip(row.values.iter()) {
        map.insert(field.name.clone(), scalar_to_json(value));
    }
    serde_json::to_vec(&serde_json::Value::Object(map)).map_err(|e| {
        SparrowError::new(ErrorCode::CodecViolation, format!("JSON encode: {e}"))
    })
}

pub fn json_depth(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(items) => {
            1 + items.iter().map(json_depth).max().unwrap_or(0)
        }
        serde_json::Value::Object(map) => {
            1 + map.values().map(json_depth).max().unwrap_or(0)
        }
        _ => 1,
    }
}

fn json_to_scalar(value: &serde_json::Value, ty: &DataType, nullable: bool) -> Result<Scalar> {
    if value.is_null() {
        if nullable {
            return Ok(Scalar::Null);
        }
        return Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "null in non-nullable field",
        ));
    }
    match ty {
        DataType::Bool => value
            .as_bool()
            .map(Scalar::Bool)
            .ok_or_else(|| type_err("bool", value)),
        DataType::Int64 => int64(value).map(Scalar::Int64),
        DataType::UInt64 => uint64(value).map(Scalar::UInt64),
        DataType::Float64 => value
            .as_f64()
            .map(Scalar::Float64)
            .ok_or_else(|| type_err("float64", value)),
        DataType::Utf8 => value
            .as_str()
            .map(Scalar::utf8)
            .ok_or_else(|| type_err("utf8", value)),
        DataType::Bytes => value
            .as_str()
            .map(|s| Scalar::bytes(s.as_bytes()))
            .ok_or_else(|| type_err("bytes", value)),
        DataType::TimestampMicrosUTC => int64(value).map(Scalar::TimestampMicrosUTC),
        DataType::Dynamic | DataType::Null => Ok(Scalar::Dynamic(json_to_dynamic(value))),
        DataType::Array(_) | DataType::Struct(_) | DataType::Map { .. } => {
            Ok(Scalar::Dynamic(json_to_dynamic(value)))
        }
    }
}

fn int64(value: &serde_json::Value) -> Result<i64> {
    if let Some(v) = value.as_i64() {
        return Ok(v);
    }
    if let Some(v) = value.as_u64() {
        if v <= i64::MAX as u64 {
            return Ok(v as i64);
        }
    }
    if let Some(v) = value.as_f64() {
        return Ok(v as i64);
    }
    Err(type_err("int64", value))
}

fn uint64(value: &serde_json::Value) -> Result<u64> {
    if let Some(v) = value.as_u64() {
        return Ok(v);
    }
    if let Some(v) = value.as_i64() {
        if v >= 0 {
            return Ok(v as u64);
        }
    }
    Err(type_err("uint64", value))
}

fn json_to_dynamic(value: &serde_json::Value) -> DynamicValue {
    match value {
        serde_json::Value::Null => DynamicValue::Null,
        serde_json::Value::Bool(v) => DynamicValue::Bool(*v),
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                DynamicValue::Int64(v)
            } else if let Some(v) = n.as_u64() {
                DynamicValue::UInt64(v)
            } else {
                DynamicValue::Float64(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => DynamicValue::utf8(s),
        serde_json::Value::Array(items) => {
            let vals: Vec<DynamicValue> = items.iter().map(json_to_dynamic).collect();
            DynamicValue::Array(vals.into())
        }
        serde_json::Value::Object(map) => {
            let pairs: Vec<(String, DynamicValue)> = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_dynamic(v)))
                .collect();
            DynamicValue::object(pairs)
        }
    }
}

fn scalar_to_json(value: &Scalar) -> serde_json::Value {
    match value {
        Scalar::Null => serde_json::Value::Null,
        Scalar::Bool(v) => serde_json::Value::Bool(*v),
        Scalar::Int64(v) => serde_json::json!(v),
        Scalar::UInt64(v) => serde_json::json!(v),
        Scalar::Float64(v) => serde_json::json!(v),
        Scalar::Utf8(v) => serde_json::Value::String(v.to_string()),
        Scalar::Bytes(v) => serde_json::Value::String(String::from_utf8_lossy(v).into_owned()),
        Scalar::TimestampMicrosUTC(v) => serde_json::json!(v),
        Scalar::Dynamic(d) => dynamic_to_json(d),
    }
}

fn dynamic_to_json(value: &DynamicValue) -> serde_json::Value {
    match value {
        DynamicValue::Null => serde_json::Value::Null,
        DynamicValue::Bool(v) => serde_json::Value::Bool(*v),
        DynamicValue::Int64(v) => serde_json::json!(v),
        DynamicValue::UInt64(v) => serde_json::json!(v),
        DynamicValue::Float64(v) => serde_json::json!(v),
        DynamicValue::Utf8(v) => serde_json::Value::String(v.to_string()),
        DynamicValue::Bytes(v) => serde_json::Value::String(String::from_utf8_lossy(v).into_owned()),
        DynamicValue::Array(items) => {
            serde_json::Value::Array(items.iter().map(dynamic_to_json).collect())
        }
        DynamicValue::Object(pairs) => {
            let mut map = serde_json::Map::new();
            for (k, v) in pairs.iter() {
                map.insert(k.to_string(), dynamic_to_json(v));
            }
            serde_json::Value::Object(map)
        }
    }
}

fn type_err(expected: &str, value: &serde_json::Value) -> SparrowError {
    SparrowError::new(
        ErrorCode::TypeMismatch,
        format!("expected {expected}, got {value}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{Field, FieldId, SchemaId};

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
                Field::new(FieldId::new(3), "payload", DataType::Dynamic, true),
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trip_and_limits() {
        let s = schema();
        let bytes = br#"{"device_id":"edge-a","temperature":26.2,"payload":{"temp":26.2}}"#;
        let row = decode_json_row(&s, bytes, &JsonLimits::default()).unwrap();
        assert_eq!(row.values[0], Scalar::utf8("edge-a"));
        let out = encode_json_row(&s, &row).unwrap();
        assert!(String::from_utf8_lossy(&out).contains("edge-a"));

        let deep = r#"{"device_id":"x","payload":{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":1}}}}}}}}}"#;
        let err = decode_json_row(
            &s,
            deep.as_bytes(),
            &JsonLimits {
                max_bytes: 1024,
                max_depth: 4,
            },
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);

        let big = vec![b'x'; 32];
        let err = decode_json_row(
            &s,
            &big,
            &JsonLimits {
                max_bytes: 8,
                max_depth: 4,
            },
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::MaxRecordSize);
    }

    #[test]
    fn drop_policy_swallows_bad_record() {
        let codec = JsonCodec {
            schema: schema(),
            limits: JsonLimits::default(),
            policy: BadRecordPolicy::Drop,
        };
        let frame = SourceFrame::new(b"not-json".to_vec(), 0);
        assert!(codec.decode_frame(&frame).unwrap().is_none());

        let fail = JsonCodec {
            schema: schema(),
            limits: JsonLimits::default(),
            policy: BadRecordPolicy::FailJob,
        };
        assert_eq!(
            fail.decode_frame(&frame).unwrap_err().code,
            ErrorCode::CodecViolation
        );
    }

    #[test]
    fn missing_required_field_is_invalid_schema() {
        let err = decode_json_row(
            &schema(),
            br#"{"temperature":1.0}"#,
            &JsonLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidSchema);
    }
}

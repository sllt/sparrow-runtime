//! JSON object ↔ [`Row`] with size/depth caps and schema checks.

use serde::de::DeserializeSeed;
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
    let hint = json_byte_depth(bytes);
    if hint > limits.max_depth {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!("JSON depth {hint} exceeds max_depth {}", limits.max_depth),
        ));
    }
    let value = parse_strict_json(bytes, limits.max_depth)?;
    let JsonVal::Object(fields) = value else {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "JSON root must be an object",
        ));
    };
    let mut values = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        match fields.iter().find(|(k, _)| k == &field.name) {
            None if field.nullable => values.push(Scalar::Null),
            None => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("missing required field '{}'", field.name),
                ));
            }
            Some((_, v)) => values.push(json_val_to_scalar(v, &field.data_type, field.nullable)?),
        }
    }
    Ok(Row { values })
}

/// Encode a batch as a JSON array of objects (HTTP sink batch POST, P1-22).
///
/// Each row is encoded once. The array is assembled from those object
/// bytes — no bytes→Value→bytes round-trip per row (N13).
pub fn encode_json_batch(schema: &Schema, rows: &[Row]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'[');
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend(encode_json_row(schema, row)?);
    }
    out.push(b']');
    Ok(out)
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

fn json_val_to_scalar(value: &JsonVal, ty: &DataType, nullable: bool) -> Result<Scalar> {
    if matches!(value, JsonVal::Null) {
        if nullable {
            return Ok(Scalar::Null);
        }
        return Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "null in non-nullable field",
        ));
    }
    match ty {
        DataType::Bool => match value {
            JsonVal::Bool(v) => Ok(Scalar::Bool(*v)),
            other => Err(type_err("bool", other)),
        },
        DataType::Int64 => int64(value).map(Scalar::Int64),
        DataType::UInt64 => uint64(value).map(Scalar::UInt64),
        DataType::Float64 => match value {
            JsonVal::F64(v) => Ok(Scalar::Float64(*v)),
            JsonVal::I64(v) => Ok(Scalar::Float64(*v as f64)),
            JsonVal::U64(v) => Ok(Scalar::Float64(*v as f64)),
            other => Err(type_err("float64", other)),
        },
        DataType::Utf8 => match value {
            JsonVal::Utf8(s) => Ok(Scalar::utf8(s)),
            other => Err(type_err("utf8", other)),
        },
        DataType::Bytes => match value {
            JsonVal::Utf8(s) => {
                let raw = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .map_err(|_| type_err("bytes(base64)", value))?;
                Ok(Scalar::bytes(raw))
            }
            other => Err(type_err("bytes(base64)", other)),
        },
        DataType::TimestampMicrosUTC => int64(value).map(Scalar::TimestampMicrosUTC),
        DataType::Dynamic | DataType::Null => Ok(Scalar::Dynamic(json_val_to_dynamic(value)?)),
        DataType::Array(inner) => {
            let JsonVal::Array(items) = value else {
                return Err(type_err("array", value));
            };
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(scalar_to_dynamic(json_val_to_scalar(item, inner, true)?));
            }
            Ok(Scalar::Dynamic(DynamicValue::Array(out.into())))
        }
        DataType::Struct(fields) => {
            let JsonVal::Object(pairs) = value else {
                return Err(type_err("struct", value));
            };
            for (k, _) in pairs {
                if !fields.iter().any(|f| f.name == *k) {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidSchema,
                        format!("struct has unknown field '{k}'"),
                    ));
                }
            }
            let mut out = Vec::with_capacity(fields.len());
            for f in fields {
                match pairs.iter().find(|(k, _)| k == &f.name) {
                    None if f.nullable => out.push((f.name.clone(), DynamicValue::Null)),
                    None => {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidSchema,
                            format!("missing required struct field '{}'", f.name),
                        ));
                    }
                    Some((_, v)) => {
                        let s = json_val_to_scalar(v, &f.data_type, f.nullable)?;
                        out.push((f.name.clone(), scalar_to_dynamic(s)));
                    }
                }
            }
            DynamicValue::try_object(out)
                .map(Scalar::Dynamic)
                .map_err(|(e, _)| e)
        }
        DataType::Map { key, value: val_ty } => {
            let JsonVal::Object(pairs) = value else {
                return Err(type_err("map", value));
            };
            let mut out = Vec::with_capacity(pairs.len());
            for (k, v) in pairs {
                validate_map_key(k, key)?;
                let s = json_val_to_scalar(v, val_ty, true)?;
                out.push((k.clone(), scalar_to_dynamic(s)));
            }
            DynamicValue::try_object(out)
                .map(Scalar::Dynamic)
                .map_err(|(e, _)| e)
        }
    }
}

fn validate_map_key(key: &str, ty: &DataType) -> Result<()> {
    match ty {
        DataType::Utf8 => Ok(()),
        DataType::Int64 => {
            key.parse::<i64>().map(|_| ()).map_err(|_| {
                SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "map key is not int64",
                )
            })
        }
        DataType::UInt64 => {
            key.parse::<u64>().map(|_| ()).map_err(|_| {
                SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "map key is not uint64",
                )
            })
        }
        other => Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            format!("map key type {other} is not supported for JSON objects"),
        )),
    }
}

fn int64(value: &JsonVal) -> Result<i64> {
    match value {
        JsonVal::I64(v) => Ok(*v),
        JsonVal::U64(v) if *v <= i64::MAX as u64 => Ok(*v as i64),
        JsonVal::F64(_) => Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "JSON number is not an integer; refusing silent truncate into int64",
        )),
        other => Err(type_err("int64", other)),
    }
}

fn uint64(value: &JsonVal) -> Result<u64> {
    match value {
        JsonVal::U64(v) => Ok(*v),
        JsonVal::I64(v) if *v >= 0 => Ok(*v as u64),
        JsonVal::F64(_) => Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "JSON number is not an integer; refusing silent truncate into uint64",
        )),
        other => Err(type_err("uint64", other)),
    }
}

fn json_val_to_dynamic(value: &JsonVal) -> Result<DynamicValue> {
    Ok(match value {
        JsonVal::Null => DynamicValue::Null,
        JsonVal::Bool(v) => DynamicValue::Bool(*v),
        JsonVal::I64(v) => DynamicValue::Int64(*v),
        JsonVal::U64(v) => DynamicValue::UInt64(*v),
        JsonVal::F64(v) => DynamicValue::Float64(*v),
        JsonVal::Utf8(s) => DynamicValue::utf8(s),
        JsonVal::Array(items) => {
            let vals: Result<Vec<DynamicValue>> = items.iter().map(json_val_to_dynamic).collect();
            DynamicValue::Array(vals?.into())
        }
        JsonVal::Object(pairs) => {
            let mut out = Vec::with_capacity(pairs.len());
            for (k, v) in pairs {
                out.push((k.clone(), json_val_to_dynamic(v)?));
            }
            DynamicValue::try_object(out).map_err(|(e, _)| e)?
        }
    })
}

fn scalar_to_dynamic(value: Scalar) -> DynamicValue {
    match value {
        Scalar::Null => DynamicValue::Null,
        Scalar::Bool(v) => DynamicValue::Bool(v),
        Scalar::Int64(v) => DynamicValue::Int64(v),
        Scalar::UInt64(v) => DynamicValue::UInt64(v),
        Scalar::Float64(v) => DynamicValue::Float64(v),
        Scalar::Utf8(s) => DynamicValue::Utf8(s),
        Scalar::Bytes(b) => DynamicValue::Bytes(b),
        Scalar::TimestampMicrosUTC(v) => DynamicValue::Int64(v),
        Scalar::Dynamic(d) => d,
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
        Scalar::Bytes(v) => serde_json::Value::String(
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v.as_ref()),
        ),
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
        DynamicValue::Bytes(v) => serde_json::Value::String(
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v.as_ref()),
        ),
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

fn type_err(expected: &str, value: &JsonVal) -> SparrowError {
    SparrowError::new(
        ErrorCode::TypeMismatch,
        format!("expected {expected}, got {}", value.kind()),
    )
}

/// Nesting depth from bytes, ignoring braces/brackets inside strings.
/// Used to refuse oversized trees *before* serde allocates them.
pub fn json_byte_depth(bytes: &[u8]) -> usize {
    let mut depth = 0usize;
    let mut max = 0usize;
    let mut in_string = false;
    let mut escape = false;
    for &b in bytes {
        if in_string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                max = max.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

#[derive(Clone, Debug, PartialEq)]
enum JsonVal {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Utf8(String),
    Array(Vec<JsonVal>),
    Object(Vec<(String, JsonVal)>),
}

impl JsonVal {
    fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::I64(_) | Self::U64(_) | Self::F64(_) => "number",
            Self::Utf8(_) => "string",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
        }
    }
}

struct JsonSeed {
    depth: usize,
    max_depth: usize,
}

impl<'de> serde::de::DeserializeSeed<'de> for JsonSeed {
    type Value = JsonVal;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> std::result::Result<JsonVal, D::Error> {
        deserializer.deserialize_any(JsonVisitor {
            depth: self.depth,
            max_depth: self.max_depth,
        })
    }
}

struct JsonVisitor {
    depth: usize,
    max_depth: usize,
}

impl JsonVisitor {
    fn child(&self) -> Option<JsonSeed> {
        let next = self.depth.saturating_add(1);
        if next > self.max_depth {
            return None;
        }
        Some(JsonSeed {
            depth: next,
            max_depth: self.max_depth,
        })
    }

    fn depth_err<E: serde::de::Error>(&self) -> E {
        let next = self.depth.saturating_add(1);
        E::custom(format!(
            "JSON depth {next} exceeds max_depth {}",
            self.max_depth
        ))
    }
}

impl<'de> serde::de::Visitor<'de> for JsonVisitor {
    type Value = JsonVal;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::Bool(v))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::I64(v))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::U64(v))
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::F64(v))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::Utf8(v.to_string()))
    }

    fn visit_string<E: serde::de::Error>(self, v: String) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::Utf8(v))
    }

    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::Null)
    }

    fn visit_none<E: serde::de::Error>(self) -> std::result::Result<JsonVal, E> {
        Ok(JsonVal::Null)
    }

    fn visit_some<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<JsonVal, D::Error> {
        JsonSeed {
            depth: self.depth,
            max_depth: self.max_depth,
        }
        .deserialize(deserializer)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<JsonVal, A::Error> {
        let seed = match self.child() {
            Some(s) => s,
            None => return Err(self.depth_err()),
        };
        let mut items = Vec::new();
        while let Some(v) = seq.next_element_seed(JsonSeed {
            depth: seed.depth,
            max_depth: seed.max_depth,
        })? {
            items.push(v);
        }
        Ok(JsonVal::Array(items))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<JsonVal, A::Error> {
        let seed = match self.child() {
            Some(s) => s,
            None => return Err(self.depth_err()),
        };
        let mut pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            if pairs.iter().any(|(k, _)| k == &key) {
                return Err(serde::de::Error::custom("JSON object has duplicate keys"));
            }
            let val = map.next_value_seed(JsonSeed {
                depth: seed.depth,
                max_depth: seed.max_depth,
            })?;
            pairs.push((key, val));
        }
        Ok(JsonVal::Object(pairs))
    }
}

fn parse_strict_json(bytes: &[u8], max_depth: usize) -> Result<JsonVal> {
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let value = serde::de::DeserializeSeed::deserialize(
        JsonSeed {
            depth: 0,
            max_depth,
        },
        &mut de,
    )
    .map_err(|e| map_json_de_error(e, max_depth))?;
    de.end()
        .map_err(|e| SparrowError::new(ErrorCode::CodecViolation, format!("invalid JSON: {e}")))?;
    Ok(value)
}

fn map_json_de_error(err: serde_json::Error, max_depth: usize) -> SparrowError {
    let msg = err.to_string();
    if msg.contains("duplicate keys") {
        SparrowError::new(ErrorCode::InvalidArgument, "JSON object has duplicate keys")
    } else if msg.contains("exceeds max_depth") {
        SparrowError::new(
            ErrorCode::BoundExceeded,
            format!("JSON depth exceeds max_depth {max_depth}"),
        )
    } else {
        SparrowError::new(ErrorCode::CodecViolation, format!("invalid JSON: {err}"))
    }
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

    #[test]
    fn p0_8_json_float_to_int_errors() {
        let s = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "v", DataType::Int64, false)],
        )
        .unwrap();
        let err = decode_json_row(&s, br#"{"v":1.5}"#, &JsonLimits::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        assert!(err.message.contains("integer"), "{}", err.message);

        let bytes = Schema::new(
            SchemaId::new(2),
            vec![Field::new(FieldId::new(1), "b", DataType::Bytes, false)],
        )
        .unwrap();
        let row = decode_json_row(&bytes, br#"{"b":"YWI="}"#, &JsonLimits::default()).unwrap();
        assert_eq!(row.values[0], Scalar::bytes(b"ab".to_vec()));
        let enc = encode_json_row(&bytes, &row).unwrap();
        assert!(String::from_utf8_lossy(&enc).contains("YWI="));
    }

    #[test]
    fn n13_encode_json_batch_is_array_without_reparse() {
        let s = schema();
        let row = decode_json_row(
            &s,
            br#"{"device_id":"edge-a","temperature":26.2,"payload":{"temp":26.2}}"#,
            &JsonLimits::default(),
        )
        .unwrap();
        let one = encode_json_row(&s, &row).unwrap();
        let batch = encode_json_batch(&s, &[row.clone(), row]).unwrap();
        assert_eq!(batch.first().copied(), Some(b'['));
        assert_eq!(batch.last().copied(), Some(b']'));
        let expected: Vec<u8> = {
            let mut v = Vec::new();
            v.push(b'[');
            v.extend_from_slice(&one);
            v.push(b',');
            v.extend_from_slice(&one);
            v.push(b']');
            v
        };
        assert_eq!(
            batch, expected,
            "batch must concatenate row objects; no Value round-trip"
        );
        let parsed: serde_json::Value = serde_json::from_slice(&batch).unwrap();
        assert_eq!(parsed.as_array().map(|a| a.len()), Some(2));
        assert_eq!(parsed[0]["device_id"], "edge-a");
    }

    #[test]
    fn p0_8_array_struct_map_validate_against_schema() {
        let arr = Schema::new(
            SchemaId::new(1),
            vec![Field::new(
                FieldId::new(1),
                "xs",
                DataType::Array(Box::new(DataType::Int64)),
                false,
            )],
        )
        .unwrap();
        let row = decode_json_row(&arr, br#"{"xs":[1,2,3]}"#, &JsonLimits::default()).unwrap();
        match &row.values[0] {
            Scalar::Dynamic(DynamicValue::Array(items)) => assert_eq!(items.len(), 3),
            other => panic!("expected validated array, got {other:?}"),
        }
        let err = decode_json_row(&arr, br#"{"xs":[1,"nope"]}"#, &JsonLimits::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        assert!(!err.message.contains("nope"), "payload must not leak: {}", err.message);

        let st = Schema::new(
            SchemaId::new(2),
            vec![Field::new(
                FieldId::new(1),
                "pt",
                DataType::Struct(vec![
                    Field::new(FieldId::new(1), "x", DataType::Int64, false),
                    Field::new(FieldId::new(2), "y", DataType::Int64, true),
                ]),
                false,
            )],
        )
        .unwrap();
        let ok = decode_json_row(&st, br#"{"pt":{"x":1}}"#, &JsonLimits::default()).unwrap();
        assert!(matches!(ok.values[0], Scalar::Dynamic(DynamicValue::Object(_))));
        let err = decode_json_row(&st, br#"{"pt":{"x":1,"z":9}}"#, &JsonLimits::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidSchema);
        let err = decode_json_row(&st, br#"{"pt":[1,2]}"#, &JsonLimits::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);

        let map = Schema::new(
            SchemaId::new(3),
            vec![Field::new(
                FieldId::new(1),
                "m",
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Int64),
                },
                false,
            )],
        )
        .unwrap();
        let row = decode_json_row(&map, br#"{"m":{"a":1,"b":2}}"#, &JsonLimits::default()).unwrap();
        assert!(matches!(row.values[0], Scalar::Dynamic(DynamicValue::Object(_))));
        let err = decode_json_row(&map, br#"{"m":{"a":"x"}}"#, &JsonLimits::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        assert!(!err.message.contains("\"x\""), "{}", err.message);
    }

    #[test]
    fn p2_40_json_object_duplicate_keys_fail_closed() {
        let s = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "payload", DataType::Dynamic, false)],
        )
        .unwrap();
        let err = decode_json_row(
            &s,
            br#"{"payload":{"k":1,"k":2}}"#,
            &JsonLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("duplicate"), "{}", err.message);

        let err = decode_json_row(&s, br#"{"payload":1,"payload":2}"#, &JsonLimits::default())
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn p3_53_type_err_does_not_include_raw_payload() {
        let s = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "v", DataType::Int64, false)],
        )
        .unwrap();
        let secret = br#"{"v":"this-must-not-land-in-last_error"}"#;
        let err = decode_json_row(&s, secret, &JsonLimits::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        assert!(
            !err.message.contains("this-must-not-land-in-last_error"),
            "{}",
            err.message
        );
        assert!(err.message.contains("int64"), "{}", err.message);
        assert!(err.message.contains("string"), "{}", err.message);
    }

    #[test]
    fn p0_8_depth_checked_from_bytes_before_parse() {
        let s = schema();
        let deep = r#"{"device_id":"x","payload":{"a":{"b":{"c":{"d":1}}}}}"#;
        assert!(json_byte_depth(deep.as_bytes()) > 4);
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
    }
}

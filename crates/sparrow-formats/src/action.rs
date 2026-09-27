//! Versioned, bounded output shaping. This is a serializer, not an evaluator:
//! only explicit field references and literal JSON, no scripts or secret access.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sparrow_model::{DynamicValue, ErrorCode, Result, Row, Scalar, Schema, SparrowError};
use std::{collections::BTreeMap, io::Write};

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message)
}
fn bound(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::BoundExceeded, message)
}
fn one() -> u32 {
    1
}

/// Text templates are arrays of literal strings or {"$field":"column"}.
/// NULL and structured field values fail instead of becoming a destination.
pub type TextTemplate = Vec<Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionSpec {
    #[serde(default = "one")]
    pub version: u32,
    /// JSON tree; {"$field":"name"} substitutes a typed value and
    /// {"$literal":value} escapes an otherwise special object.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_body"
    )]
    pub body: Option<Value>,
    /// HTTP only: one JSON value per request instead of a JSON array.
    #[serde(default)]
    pub single: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<TextTemplate>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, TextTemplate>,
}
impl Default for ActionSpec {
    fn default() -> Self {
        Self {
            version: 1,
            body: None,
            single: false,
            topic: None,
            query: BTreeMap::new(),
        }
    }
}
fn present_body<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

fn field<'a>(value: &'a Value, schema: &Schema) -> Result<Option<usize>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if let Some(name) = object.get("$field") {
        if object.len() != 1 {
            return Err(invalid("$field object must have exactly one member"));
        }
        let name = name
            .as_str()
            .ok_or_else(|| invalid("$field must be a field name"))?;
        return schema
            .index_of_name(name)
            .map(Some)
            .ok_or_else(|| invalid("action references an unknown output field"));
    }
    Ok(None)
}

impl ActionSpec {
    pub fn validate(&self, schema: &Schema) -> Result<()> {
        if self.version != 1 || schema.fields.len() > 128 || self.query.len() > 16 {
            return Err(bound(
                "action v1: at most 128 input fields and 16 query parameters",
            ));
        }
        let mut nodes = 0;
        if let Some(body) = &self.body {
            validate_node(body, schema, 0, &mut nodes, false)?;
        }
        if let Some(topic) = &self.topic {
            validate_text(topic, schema)?;
            if topic
                .iter()
                .filter_map(Value::as_str)
                .any(|s| s.contains(['+', '#', '\0']))
            {
                return Err(invalid("topic literals must not contain wildcard or NUL"));
            }
        }
        for (key, text) in &self.query {
            if key.is_empty() || key.len() > 64 || key.bytes().any(|b| b.is_ascii_control()) {
                return Err(invalid("invalid action query parameter name"));
            }
            validate_text(text, schema)?;
        }
        Ok(())
    }
    pub fn per_row_http(&self) -> bool {
        self.single || !self.query.is_empty()
    }
    /// The caller retains input credit and admits every output capacity growth.
    pub fn encode(
        &self,
        schema: &Schema,
        rows: &[Row],
        array: bool,
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        self.validate(schema)?;
        if !array && rows.len() != 1 {
            return Err(invalid("single action payload requires exactly one row"));
        }
        let mut writer = Buffer::new(limit, admit);
        let result = (|| {
            if array {
                writer.push(b"[")?;
            }
            for (i, row) in rows.iter().enumerate() {
                if row.values.len() != schema.fields.len() {
                    return Err(invalid("action row/schema mismatch"));
                }
                if i > 0 {
                    writer.push(b",")?;
                }
                let mut visited = 0;
                if let Some(body) = &self.body {
                    render_node(&mut writer, body, schema, row, 0, &mut visited)?;
                } else {
                    writer.push(b"{")?;
                    for (i, (column, value)) in schema.fields.iter().zip(&row.values).enumerate() {
                        if i > 0 {
                            writer.push(b",")?;
                        }
                        writer.value(&column.name)?;
                        writer.push(b":")?;
                        scalar(&mut writer, value, 0, &mut visited)?;
                    }
                    writer.push(b"}")?;
                }
            }
            if array {
                writer.push(b"]")?;
            }
            Ok(())
        })();
        writer.finish(result)
    }
    pub fn text(&self, text: &[Value], schema: &Schema, row: &Row, limit: usize) -> Result<String> {
        validate_text(text, schema)?;
        if row.values.len() != schema.fields.len() {
            return Err(invalid("action row/schema mismatch"));
        }
        let mut out = String::new();
        for part in text {
            let value =
                if let Some(text) = part.as_str() {
                    text.to_owned()
                } else {
                    match &row.values[field(part, schema)?.expect("validated text field")] {
                        Scalar::Utf8(text) => {
                            if text.len() > limit {
                                return Err(bound("action text exceeds byte limit"));
                            }
                            text.to_string()
                        }
                        Scalar::Bool(v) => v.to_string(),
                        Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => v.to_string(),
                        Scalar::UInt64(v) => v.to_string(),
                        Scalar::Float64(v) if v.is_finite() => v.to_string(),
                        _ => return Err(invalid(
                            "action destination fields must be non-null finite scalar text/numbers",
                        )),
                    }
                };
            if out.len().saturating_add(value.len()) > limit {
                return Err(bound("action text exceeds byte limit"));
            }
            out.push_str(&value);
        }
        Ok(out)
    }
}
fn validate_text(text: &[Value], schema: &Schema) -> Result<()> {
    if text.is_empty() || text.len() > 32 {
        return Err(bound("text template requires 1..32 parts"));
    }
    let mut literal_bytes = 0usize;
    for part in text {
        if let Some(s) = part.as_str() {
            literal_bytes = literal_bytes.saturating_add(s.len());
        } else if let Some(index) = field(part, schema)? {
            if !matches!(
                schema.fields[index].data_type,
                sparrow_model::DataType::Utf8
                    | sparrow_model::DataType::Bool
                    | sparrow_model::DataType::Int64
                    | sparrow_model::DataType::UInt64
                    | sparrow_model::DataType::Float64
                    | sparrow_model::DataType::TimestampMicrosUTC
            ) {
                return Err(invalid(
                    "structured destination field requires an explicit conversion",
                ));
            }
        } else {
            return Err(invalid("text parts must be strings or $field references"));
        }
    }
    if literal_bytes > 1024 {
        return Err(bound("text template literals exceed 1024 bytes"));
    }
    Ok(())
}
fn validate_node(
    value: &Value,
    schema: &Schema,
    depth: usize,
    nodes: &mut usize,
    literal: bool,
) -> Result<()> {
    *nodes += 1;
    if depth > 8 || *nodes > 256 {
        return Err(bound("action template depth/node bound exceeded"));
    }
    if !literal && field(value, schema)?.is_some() {
        return Ok(());
    }
    match value {
        Value::Object(map) => {
            if !literal && map.contains_key("$literal") {
                if map.len() != 1 {
                    return Err(invalid("$literal object must have exactly one member"));
                }
                return validate_node(&map["$literal"], schema, depth + 1, nodes, true);
            }
            for (key, value) in map {
                if key.len() > 256 {
                    return Err(bound("action template key exceeds 256 bytes"));
                }
                validate_node(value, schema, depth + 1, nodes, literal)?;
            }
        }
        Value::Array(items) => {
            for value in items {
                validate_node(value, schema, depth + 1, nodes, literal)?;
            }
        }
        Value::String(s) if s.len() > 65536 => return Err(bound("action literal exceeds 64KiB")),
        _ => {}
    }
    Ok(())
}

/// Bounded allocation callback shared by new action/File encoders. Existing
/// JSON/HTTP encoding stays untouched when the optional action is absent.
pub struct Buffer<F> {
    bytes: Vec<u8>,
    limit: usize,
    admit: F,
    error: Option<SparrowError>,
}
impl<F: FnMut(usize) -> Result<()>> Buffer<F> {
    pub fn new(limit: usize, admit: F) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            admit,
            error: None,
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| bound("output byte limit exceeded"))?;
        if next > self.bytes.capacity() {
            let cap = next
                .checked_next_power_of_two()
                .unwrap_or(self.limit)
                .max(256)
                .min(self.limit);
            (self.admit)(cap)?;
            self.bytes
                .try_reserve_exact(cap - self.bytes.len())
                .map_err(|_| {
                    SparrowError::new(ErrorCode::ResourceExhausted, "output allocation failed")
                })?;
            if self.bytes.capacity() > cap {
                return Err(bound("unexpected output capacity"));
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    pub fn value(&mut self, value: &(impl Serialize + ?Sized)) -> Result<()> {
        serde_json::to_writer(self, value).map_err(|_| invalid("JSON output encoding failed"))
    }
    pub fn finish(self, result: Result<()>) -> Result<Vec<u8>> {
        if let Some(e) = self.error {
            return Err(e);
        }
        result?;
        Ok(self.bytes)
    }
}
impl<F: FnMut(usize) -> Result<()>> Write for Buffer<F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(e) = self.push(bytes) {
            self.error = Some(e);
            return Err(std::io::Error::other("bounded writer"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn visit(depth: usize, nodes: &mut usize) -> Result<()> {
    *nodes += 1;
    if depth > 16 || *nodes > 16384 {
        return Err(bound("action output traversal bound exceeded"));
    }
    Ok(())
}
fn render_node<F: FnMut(usize) -> Result<()>>(
    w: &mut Buffer<F>,
    value: &Value,
    schema: &Schema,
    row: &Row,
    depth: usize,
    nodes: &mut usize,
) -> Result<()> {
    visit(depth, nodes)?;
    if let Some(index) = field(value, schema)? {
        return scalar(w, &row.values[index], depth + 1, nodes);
    }
    match value {
        Value::Object(map) if map.contains_key("$literal") => w.value(&map["$literal"]),
        Value::Object(map) => {
            w.push(b"{")?;
            for (i, (key, value)) in map.iter().enumerate() {
                if i > 0 {
                    w.push(b",")?;
                }
                w.value(key)?;
                w.push(b":")?;
                render_node(w, value, schema, row, depth + 1, nodes)?;
            }
            w.push(b"}")
        }
        Value::Array(items) => {
            w.push(b"[")?;
            for (i, value) in items.iter().enumerate() {
                if i > 0 {
                    w.push(b",")?;
                }
                render_node(w, value, schema, row, depth + 1, nodes)?;
            }
            w.push(b"]")
        }
        _ => w.value(value),
    }
}
fn bytes<F: FnMut(usize) -> Result<()>>(w: &mut Buffer<F>, bytes: &[u8]) -> Result<()> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    w.push(b"\"")?;
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        w.push(&[
            TABLE[(a >> 2) as usize],
            TABLE[(((a & 3) << 4) | (b >> 4)) as usize],
            if chunk.len() > 1 {
                TABLE[(((b & 15) << 2) | (c >> 6)) as usize]
            } else {
                b'='
            },
            if chunk.len() > 2 {
                TABLE[(c & 63) as usize]
            } else {
                b'='
            },
        ])?;
    }
    w.push(b"\"")
}
fn scalar<F: FnMut(usize) -> Result<()>>(
    w: &mut Buffer<F>,
    value: &Scalar,
    depth: usize,
    nodes: &mut usize,
) -> Result<()> {
    visit(depth, nodes)?;
    match value {
        Scalar::Null => w.push(b"null"),
        Scalar::Bool(v) => w.value(v),
        Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => w.value(v),
        Scalar::UInt64(v) => w.value(v),
        Scalar::Float64(v) if v.is_finite() => w.value(v),
        Scalar::Float64(_) => Err(invalid("non-finite action output")),
        Scalar::Utf8(v) => w.value(v.as_ref()),
        Scalar::Bytes(v) => bytes(w, v.as_ref()),
        Scalar::Dynamic(v) => dynamic(w, v, depth + 1, nodes),
    }
}
pub fn encode_scalar(value: &Scalar, limit: usize) -> Result<Vec<u8>> {
    let mut writer = Buffer::new(limit, |_| Ok(()));
    let mut nodes = 0;
    let result = scalar(&mut writer, value, 0, &mut nodes);
    writer.finish(result)
}
fn dynamic<F: FnMut(usize) -> Result<()>>(
    w: &mut Buffer<F>,
    value: &DynamicValue,
    depth: usize,
    nodes: &mut usize,
) -> Result<()> {
    visit(depth, nodes)?;
    match value {
        DynamicValue::Null => w.push(b"null"),
        DynamicValue::Bool(v) => w.value(v),
        DynamicValue::Int64(v) => w.value(v),
        DynamicValue::UInt64(v) => w.value(v),
        DynamicValue::Float64(v) if v.is_finite() => w.value(v),
        DynamicValue::Float64(_) => Err(invalid("non-finite action output")),
        DynamicValue::Utf8(v) => w.value(v.as_ref()),
        DynamicValue::Bytes(v) => bytes(w, v.as_ref()),
        DynamicValue::Array(items) => {
            w.push(b"[")?;
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    w.push(b",")?;
                }
                dynamic(w, v, depth + 1, nodes)?;
            }
            w.push(b"]")
        }
        DynamicValue::Object(items) => {
            w.push(b"{")?;
            for (i, (k, v)) in items.iter().enumerate() {
                if i > 0 {
                    w.push(b",")?;
                }
                w.value(k.as_ref())?;
                w.push(b":")?;
                dynamic(w, v, depth + 1, nodes)?;
            }
            w.push(b"}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sparrow_model::{DataType, Field, FieldId, SchemaId};
    fn input() -> (Schema, Row) {
        (
            Schema::new(
                SchemaId::new(1),
                vec![
                    Field::new(FieldId::new(1), "name", DataType::Utf8, true),
                    Field::new(FieldId::new(2), "n", DataType::UInt64, false),
                ],
            )
            .unwrap(),
            Row {
                values: vec![Scalar::utf8("测\"\n"), Scalar::UInt64(u64::MAX)],
            },
        )
    }
    #[test]
    fn actions_typed_mapping_literal_escape_and_explicit_null() {
        let (schema, row) = input();
        let action:ActionSpec=serde_json::from_value(json!({"body":{"label":{"$field":"name"},"value":{"$field":"n"},"literal":{"$literal":{"$field":"missing"}}}})).unwrap();
        let encoded = action
            .encode(
                &schema,
                std::slice::from_ref(&row),
                false,
                65536,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            json!({"label":"测\"\n","value":u64::MAX,"literal":{"$field":"missing"}})
        );
        let null: ActionSpec = serde_json::from_value(json!({"body":null})).unwrap();
        assert_eq!(null.body, Some(Value::Null));
        assert_eq!(
            null.encode(&schema, std::slice::from_ref(&row), false, 8, |_| Ok(()))
                .unwrap(),
            b"null"
        );
        assert!(ActionSpec::default()
            .encode(&schema, std::slice::from_ref(&row), false, 8, |_| Ok(()))
            .is_err());
        assert_eq!(
            serde_json::from_value::<ActionSpec>(serde_json::to_value(null).unwrap())
                .unwrap()
                .body,
            Some(Value::Null)
        );
    }
    #[test]
    fn actions_bounds_precredit_errors_and_no_lossy_numbers() {
        let (schema, row) = input();
        let action = ActionSpec::default();
        let error = action
            .encode(&schema, std::slice::from_ref(&row), false, 1024, |_| {
                Err(SparrowError::new(ErrorCode::ResourceExhausted, "quota"))
            })
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::ResourceExhausted);
        assert!(encode_scalar(&Scalar::Float64(f64::NAN), 1024).is_err());
        assert_eq!(
            encode_scalar(&Scalar::Bytes(vec![0, 255, 128].into()), 1024).unwrap(),
            b"\"AP+A\""
        );
        for body in [
            json!({"$field":"unknown"}),
            json!({"$field":"name","extra":1}),
            json!({"$literal":0,"extra":1}),
        ] {
            assert!(ActionSpec {
                body: Some(body),
                ..Default::default()
            }
            .validate(&schema)
            .is_err());
        }
        let mut nested = json!(0);
        for _ in 0..10 {
            nested = json!([nested]);
        }
        assert!(ActionSpec {
            body: Some(nested),
            ..Default::default()
        }
        .validate(&schema)
        .is_err());
    }
    #[test]
    fn actions_text_destinations_reject_null_and_bound_literals() {
        let (schema, mut row) = input();
        let action = ActionSpec::default();
        let text = vec![json!("device/"), json!({"$field":"name"})];
        assert_eq!(
            action.text(&text, &schema, &row, 1024).unwrap(),
            "device/测\"\n"
        );
        row.values[0] = Scalar::Null;
        assert!(action.text(&text, &schema, &row, 1024).is_err());
        assert!(action
            .text(&[json!("x".repeat(1025))], &schema, &row, 2048)
            .is_err());
        assert!(ActionSpec {
            topic: Some(vec![json!("bad/+")]),
            ..Default::default()
        }
        .validate(&schema)
        .is_err());
        assert!(serde_json::from_value::<ActionSpec>(json!({"script":"alert(1)"})).is_err());
    }
}

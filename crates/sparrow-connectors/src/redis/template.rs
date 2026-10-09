//! `{column}` templates for Redis keys, channels and hash fields, and the
//! text form of scalar values.
//!
//! `{{` and `}}` are literal braces. Text form: utf8/bytes as-is, integers
//! and `timestamp` (microseconds since the epoch) in decimal, `bool` as
//! `true`/`false`, finite `float64` in Rust's shortest round-trip form.
//! NULL, NaN/inf and other types cannot be rendered.
//!
//! An *injective* template (used for Lookup keys) also guarantees that two
//! different key tuples never render to the same Redis key: two placeholders
//! must be separated by literal text, and a value may not contain the first
//! byte of the literal that follows its placeholder.

use std::io::Write as _;

use sparrow_model::{DataType, ErrorCode, Result, Row, Scalar, Schema, SparrowError};

pub const MAX_TEMPLATE_BYTES: usize = 1024;
/// Longest rendered key / channel / hash field.
pub const MAX_RENDERED_BYTES: usize = 4096;
const MAX_PLACEHOLDERS: usize = 16;

fn err(message: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message.into())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Lit(String),
    Col(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Template {
    parts: Vec<Part>,
}

impl Template {
    pub fn parse(text: &str) -> Result<Self> {
        if text.is_empty() || text.len() > MAX_TEMPLATE_BYTES || text.chars().any(char::is_control)
        {
            return Err(err(
                "Redis template must be 1..=1024 bytes without control characters",
            ));
        }
        let mut parts = Vec::new();
        let mut lit = String::new();
        let mut chars = text.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '{' if chars.peek() == Some(&'{') => {
                    chars.next();
                    lit.push('{');
                }
                '}' if chars.peek() == Some(&'}') => {
                    chars.next();
                    lit.push('}');
                }
                '{' => {
                    let mut name = String::new();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some('{') | None => {
                                return Err(err(format!(
                                "Redis template `{text}` has an unterminated or nested placeholder"
                            )))
                            }
                            Some(c) => name.push(c),
                        }
                    }
                    if name.is_empty() || name.len() > 128 {
                        return Err(err(format!(
                            "Redis template `{text}` has an empty or over-long placeholder"
                        )));
                    }
                    if !lit.is_empty() {
                        parts.push(Part::Lit(std::mem::take(&mut lit)));
                    }
                    if matches!(parts.last(), Some(Part::Col(_))) {
                        return Err(err(format!(
                            "Redis template `{text}`: adjacent placeholders need literal text between them"
                        )));
                    }
                    parts.push(Part::Col(name));
                }
                '}' => {
                    return Err(err(format!(
                        "Redis template `{text}` has an unmatched `}}` (write `}}}}`)"
                    )))
                }
                c => lit.push(c),
            }
        }
        if !lit.is_empty() {
            parts.push(Part::Lit(lit));
        }
        if parts.iter().filter(|p| matches!(p, Part::Col(_))).count() > MAX_PLACEHOLDERS {
            return Err(err("Redis template allows at most 16 placeholders"));
        }
        Ok(Self { parts })
    }

    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.parts.iter().filter_map(|p| match p {
            Part::Col(c) => Some(c.as_str()),
            Part::Lit(_) => None,
        })
    }

    /// Resolve placeholders against `schema`; each must be a non-float
    /// text-renderable column.
    pub fn compile(&self, schema: &Schema, injective: bool) -> Result<Compiled> {
        let mut parts = Vec::with_capacity(self.parts.len());
        for (i, part) in self.parts.iter().enumerate() {
            parts.push(match part {
                Part::Lit(l) => CPart::Lit(l.as_bytes().into()),
                Part::Col(name) => {
                    let index = schema.index_of_name(name).ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::InvalidSchema,
                            format!("Redis template column `{name}` is not in the sink input"),
                        )
                    })?;
                    if !text_type(&schema.fields[index].data_type, false) {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            format!(
                                "Redis template column `{name}` must be utf8, bytes, int64, uint64, bool or timestamp"
                            ),
                        ));
                    }
                    let stop = match self.parts.get(i + 1) {
                        Some(Part::Lit(l)) if injective => l.as_bytes().first().copied(),
                        _ => None,
                    };
                    CPart::Col { index, stop }
                }
            });
        }
        Ok(Compiled { parts })
    }
}

#[derive(Clone, Debug)]
enum CPart {
    Lit(Box<[u8]>),
    Col { index: usize, stop: Option<u8> },
}

#[derive(Clone, Debug)]
pub struct Compiled {
    parts: Vec<CPart>,
}

impl Compiled {
    /// Rendered length, or why this row cannot be rendered.
    pub fn len(&self, row: &Row) -> std::result::Result<usize, &'static str> {
        let mut n = 0usize;
        let mut buf = [0u8; 32];
        for part in &self.parts {
            n = n.saturating_add(match part {
                CPart::Lit(l) => l.len(),
                CPart::Col { index, stop } => {
                    let text =
                        scalar_text(row.values.get(*index).unwrap_or(&Scalar::Null), &mut buf)?;
                    if let Some(stop) = stop {
                        if text.contains(stop) {
                            return Err("key value contains the separator that follows it");
                        }
                    }
                    text.len()
                }
            });
        }
        if n > MAX_RENDERED_BYTES {
            return Err("rendered key/channel/field exceeds 4096 bytes");
        }
        Ok(n)
    }

    /// Append the rendering (call after `len` succeeded for this row; the
    /// caller has reserved that many bytes).
    pub fn write(&self, row: &Row, out: &mut Vec<u8>) {
        let mut buf = [0u8; 32];
        for part in &self.parts {
            match part {
                CPart::Lit(l) => out.extend_from_slice(l),
                CPart::Col { index, .. } => {
                    if let Ok(text) = scalar_text(&row.values[*index], &mut buf) {
                        out.extend_from_slice(text);
                    }
                }
            }
        }
    }
}

/// Types with a text form (`float64` only where `allow_float`).
pub fn text_type(data_type: &DataType, allow_float: bool) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::Bytes
        | DataType::Int64
        | DataType::UInt64
        | DataType::Bool
        | DataType::TimestampMicrosUTC => true,
        DataType::Float64 => allow_float,
        _ => false,
    }
}

/// Text form of one value; numbers are formatted into `buf` (no heap).
pub fn scalar_text<'a>(
    value: &'a Scalar,
    buf: &'a mut [u8; 32],
) -> std::result::Result<&'a [u8], &'static str> {
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    match value {
        Scalar::Null => return Err("NULL value"),
        Scalar::Utf8(s) => return Ok(s.as_bytes()),
        Scalar::Bytes(b) => return Ok(b),
        Scalar::Bool(true) => return Ok(b"true"),
        Scalar::Bool(false) => return Ok(b"false"),
        Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => write!(cursor, "{v}"),
        Scalar::UInt64(v) => write!(cursor, "{v}"),
        Scalar::Float64(v) if v.is_finite() => write!(cursor, "{v:?}"),
        Scalar::Float64(_) => return Err("non-finite float"),
        Scalar::Dynamic(_) => return Err("unsupported value type"),
    }
    .map_err(|_| "number does not fit its buffer")?;
    let n = cursor.position() as usize;
    Ok(&buf[..n])
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{Field, FieldId, SchemaId};

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "site", DataType::Utf8, true),
                Field::new(FieldId::new(2), "n", DataType::Int64, false),
                Field::new(FieldId::new(3), "f", DataType::Float64, false),
                Field::new(FieldId::new(4), "u", DataType::UInt64, false),
                Field::new(FieldId::new(5), "ok", DataType::Bool, false),
                Field::new(FieldId::new(6), "ts", DataType::TimestampMicrosUTC, false),
                Field::new(FieldId::new(7), "raw", DataType::Bytes, false),
            ],
        )
        .unwrap()
    }

    fn row(site: Scalar) -> Row {
        Row {
            values: vec![
                site,
                Scalar::Int64(-42),
                Scalar::Float64(0.1),
                Scalar::UInt64(u64::MAX),
                Scalar::Bool(true),
                Scalar::TimestampMicrosUTC(1_700_000_000_000_000),
                Scalar::Bytes(b"\x00\xff".to_vec().into()),
            ],
        }
    }

    fn render(t: &str, row: &Row, injective: bool) -> std::result::Result<Vec<u8>, &'static str> {
        let c = Template::parse(t)
            .unwrap()
            .compile(&schema(), injective)
            .unwrap();
        let n = c.len(row)?;
        let mut out = Vec::with_capacity(n);
        c.write(row, &mut out);
        assert_eq!(out.len(), n);
        Ok(out)
    }

    #[test]
    fn renders_every_text_type_and_escapes_braces() {
        let r = row(Scalar::utf8("a:b"));
        assert_eq!(
            render("{{x}}:{site}/{n}/{u}/{ok}/{ts}/{raw}", &r, false).unwrap(),
            b"{x}:a:b/-42/18446744073709551615/true/1700000000000000/\x00\xff"
        );
        assert_eq!(render("plain", &r, false).unwrap(), b"plain");
        let mut buf = [0u8; 32];
        assert_eq!(
            scalar_text(&Scalar::Float64(0.1), &mut buf).unwrap(),
            b"0.1"
        );
        let mut buf = [0u8; 32];
        assert_eq!(
            scalar_text(&Scalar::Float64(-2.2250738585072014e-308), &mut buf).unwrap(),
            b"-2.2250738585072014e-308"
        );
        let mut buf = [0u8; 32];
        assert_eq!(
            scalar_text(&Scalar::Int64(i64::MIN), &mut buf).unwrap(),
            b"-9223372036854775808"
        );
        for bad in [
            Scalar::Float64(f64::NAN),
            Scalar::Float64(f64::INFINITY),
            Scalar::Null,
        ] {
            let mut buf = [0u8; 32];
            assert!(scalar_text(&bad, &mut buf).is_err());
        }
    }

    #[test]
    fn rows_that_cannot_render_are_refused() {
        assert_eq!(
            render("k:{site}", &row(Scalar::Null), false),
            Err("NULL value")
        );
        let long = row(Scalar::utf8("x".repeat(MAX_RENDERED_BYTES)));
        assert_eq!(
            render("k:{site}", &long, false),
            Err("rendered key/channel/field exceeds 4096 bytes")
        );
        assert!(render("{site}", &long, false).is_ok(), "exactly 4096 bytes");
        // Injective: the value may not contain the next literal's first byte.
        assert!(render("{site}:{n}", &row(Scalar::utf8("a:b")), false).is_ok());
        assert_eq!(
            render("{site}:{n}", &row(Scalar::utf8("a:b")), true),
            Err("key value contains the separator that follows it")
        );
        assert!(render("{site}:{n}", &row(Scalar::utf8("a-b")), true).is_ok());
        // The last placeholder has no follower and may hold anything.
        assert!(render("k:{site}", &row(Scalar::utf8("a:b")), true).is_ok());
    }

    #[test]
    fn templates_are_parsed_and_compiled_strictly() {
        for bad in [
            "",
            "{",
            "}",
            "a{b",
            "a}b",
            "{}",
            "{a{b}}",
            "{site}{n}",
            "k\n",
        ] {
            assert!(Template::parse(bad).is_err(), "{bad:?}");
        }
        assert!(Template::parse(&"k".repeat(1025)).is_err());
        assert!(Template::parse(&format!("{{{}}}", "c".repeat(129))).is_err());
        let many = (0..17)
            .map(|i| format!("{{c{i}}}"))
            .collect::<Vec<_>>()
            .join(":");
        assert!(Template::parse(&many).is_err());
        let t = Template::parse("a:{site}:{n}").unwrap();
        assert_eq!(t.columns().collect::<Vec<_>>(), vec!["site", "n"]);
        let s = schema();
        assert_eq!(
            Template::parse("{nope}")
                .unwrap()
                .compile(&s, false)
                .unwrap_err()
                .code,
            ErrorCode::InvalidSchema
        );
        assert_eq!(
            Template::parse("{f}")
                .unwrap()
                .compile(&s, false)
                .unwrap_err()
                .code,
            ErrorCode::TypeMismatch,
            "float keys refused"
        );
    }
}

//! Additive pure functions. Existing CAST, ASCII case mapping, eager argument
//! evaluation, and function identity bytes are deliberately unchanged.
use crate::{check_call_arity, dynamic_from_scalar};
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use sparrow_model::{DataType, DynamicValue, ErrorCode, Result, Scalar, SparrowError};

pub const MAX_BYTES: usize = 65536;
pub fn contains(name: &str) -> bool {
    matches!(
        name,
        "concat"
            | "substring"
            | "replace"
            | "contains"
            | "starts_with"
            | "ends_with"
            | "trim"
            | "round"
            | "floor"
            | "ceil"
            | "to_int64"
            | "to_float64"
            | "to_string"
            | "json_get"
            | "json_object"
            | "json_stringify"
            | "parse_timestamp"
            | "format_timestamp"
    )
}
fn invalid(msg: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, msg)
}
fn types(msg: &str) -> SparrowError {
    SparrowError::new(ErrorCode::TypeMismatch, msg)
}
fn bound() -> SparrowError {
    SparrowError::new(
        ErrorCode::BoundExceeded,
        "builtin value/output exceeds 64KiB",
    )
}
fn text(value: &Scalar) -> Result<&str> {
    if let Scalar::Utf8(s) = value {
        Ok(s)
    } else {
        Err(types("expected Utf8"))
    }
}
fn integer(value: &Scalar) -> Result<i64> {
    if let Scalar::Int64(n) = value {
        Ok(*n)
    } else {
        Err(types("expected Int64 index/count"))
    }
}
fn text_type(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Null)
}
fn numeric(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Null
    )
}

pub fn signature(name: &str, args: &[DataType]) -> Result<DataType> {
    check_call_arity(name, args.len())?;
    let (valid, result) = match name {
        "concat" => (args.iter().all(text_type), DataType::Utf8),
        "substring" => (
            text_type(&args[0])
                && args[1..]
                    .iter()
                    .all(|t| matches!(t, DataType::Int64 | DataType::Null)),
            DataType::Utf8,
        ),
        "replace" => (args.iter().all(text_type), DataType::Utf8),
        "contains" | "starts_with" | "ends_with" => (args.iter().all(text_type), DataType::Bool),
        "trim" => (text_type(&args[0]), DataType::Utf8),
        "round" | "floor" | "ceil" => (
            numeric(&args[0]),
            if args[0] == DataType::Null {
                DataType::Float64
            } else {
                args[0].clone()
            },
        ),
        "to_int64" | "to_float64" => (
            numeric(&args[0])
                || matches!(
                    args[0],
                    DataType::Utf8 | DataType::Dynamic | DataType::TimestampMicrosUTC
                ),
            if name == "to_int64" {
                DataType::Int64
            } else {
                DataType::Float64
            },
        ),
        "to_string" => (
            numeric(&args[0])
                || matches!(
                    args[0],
                    DataType::Utf8
                        | DataType::Bool
                        | DataType::TimestampMicrosUTC
                        | DataType::Dynamic
                ),
            DataType::Utf8,
        ),
        "json_get" => (args.iter().all(text_type), DataType::Dynamic),
        "json_object" => (
            args.len() % 2 == 0 && args.iter().step_by(2).all(text_type),
            DataType::Dynamic,
        ),
        "json_stringify" => (true, DataType::Utf8),
        "parse_timestamp" => (text_type(&args[0]), DataType::TimestampMicrosUTC),
        "format_timestamp" => (
            matches!(
                args[0],
                DataType::Int64 | DataType::TimestampMicrosUTC | DataType::Null
            ),
            DataType::Utf8,
        ),
        _ => return Err(invalid("not an additive builtin")),
    };
    if !valid {
        return Err(types("builtin argument types or key/value arity mismatch"));
    }
    Ok(result)
}

fn bounded_string(s: String) -> Result<Scalar> {
    if s.len() > MAX_BYTES {
        return Err(bound());
    }
    Ok(Scalar::utf8(s))
}
fn unwrap_scalar(value: &Scalar) -> Scalar {
    match value {
        Scalar::Dynamic(DynamicValue::Null) => Scalar::Null,
        Scalar::Dynamic(DynamicValue::Bool(v)) => Scalar::Bool(*v),
        Scalar::Dynamic(DynamicValue::Int64(v)) => Scalar::Int64(*v),
        Scalar::Dynamic(DynamicValue::UInt64(v)) => Scalar::UInt64(*v),
        Scalar::Dynamic(DynamicValue::Float64(v)) => Scalar::Float64(*v),
        Scalar::Dynamic(DynamicValue::Utf8(v)) => Scalar::Utf8(v.clone()),
        other => other.clone(),
    }
}
pub fn eval(name: &str, args: Vec<Scalar>) -> Result<Scalar> {
    check_call_arity(name, args.len())?;
    let mut arg_types: [DataType; 16] = std::array::from_fn(|_| DataType::Null);
    for (index, arg) in args.iter().enumerate() {
        arg_types[index] = arg.data_type();
        let bytes = match arg {
            Scalar::Utf8(s) => s.len(),
            Scalar::Bytes(s) => s.len(),
            Scalar::Dynamic(_) => arg.resident_bytes(),
            _ => 0,
        };
        if bytes > MAX_BYTES {
            return Err(bound());
        }
    }
    signature(name, &arg_types[..args.len()])?;
    if name == "json_stringify" {
        return bounded_string(
            String::from_utf8(sparrow_formats::action::encode_scalar(&args[0], MAX_BYTES)?)
                .map_err(|_| invalid("JSON is not UTF8"))?,
        );
    }
    if name == "json_object" {
        if args.iter().step_by(2).any(Scalar::is_null) {
            return Ok(Scalar::Null);
        }
        let mut bytes = 0usize;
        let mut pairs = Vec::with_capacity(args.len() / 2);
        for pair in args.chunks_exact(2) {
            let key = text(&pair[0])?;
            bytes = bytes.saturating_add(key.len() + pair[1].resident_bytes() + 128);
            if bytes > MAX_BYTES {
                return Err(bound());
            }
            pairs.push((key.to_owned(), dynamic_from_scalar(&pair[1])));
        }
        return DynamicValue::try_object(pairs)
            .map(Scalar::Dynamic)
            .map_err(|(e, _)| e);
    }
    if args.iter().any(Scalar::is_null) {
        return Ok(Scalar::Null);
    }
    match name {
        "concat" => {
            let size = args.iter().try_fold(0usize, |n, a| {
                n.checked_add(text(a)?.len())
                    .filter(|n| *n <= MAX_BYTES)
                    .ok_or_else(bound)
            })?;
            let mut out = String::with_capacity(size);
            for arg in &args {
                out.push_str(text(arg)?);
            }
            bounded_string(out)
        }
        "substring" => {
            let s = text(&args[0])?;
            let start = integer(&args[1])?;
            let len = integer(&args[2])?;
            if start < 1 || len < 0 {
                return Err(invalid(
                    "substring uses 1-based positive start and nonnegative Unicode scalar count",
                ));
            }
            let mut chars = s.char_indices();
            let begin = chars
                .nth((start - 1).min(MAX_BYTES as i64) as usize)
                .map_or(s.len(), |(i, _)| i);
            let end = s[begin..]
                .char_indices()
                .nth(len.min(MAX_BYTES as i64) as usize)
                .map_or(s.len(), |(i, _)| begin + i);
            bounded_string(s[begin..end].to_owned())
        }
        "replace" => {
            let s = text(&args[0])?;
            let from = text(&args[1])?;
            let to = text(&args[2])?;
            if from.is_empty() {
                return Err(invalid("replace search string must not be empty"));
            }
            let count = s.matches(from).count();
            let size = s
                .len()
                .saturating_sub(count * from.len())
                .checked_add(count.checked_mul(to.len()).ok_or_else(bound)?)
                .filter(|n| *n <= MAX_BYTES)
                .ok_or_else(bound)?;
            let mut out = String::with_capacity(size);
            let mut pos = 0;
            for (index, _) in s.match_indices(from) {
                out.push_str(&s[pos..index]);
                out.push_str(to);
                pos = index + from.len();
            }
            out.push_str(&s[pos..]);
            bounded_string(out)
        }
        "contains" | "starts_with" | "ends_with" => {
            let s = text(&args[0])?;
            let needle = text(&args[1])?;
            Ok(Scalar::Bool(match name {
                "contains" => s.contains(needle),
                "starts_with" => s.starts_with(needle),
                _ => s.ends_with(needle),
            }))
        }
        "trim" => bounded_string(text(&args[0])?.trim().to_owned()),
        "round" | "floor" | "ceil" => match &args[0] {
            Scalar::Float64(v) if v.is_finite() => Ok(Scalar::Float64(match name {
                "round" => v.round(),
                "floor" => v.floor(),
                _ => v.ceil(),
            })),
            Scalar::Float64(_) => Err(invalid("round/floor/ceil require finite input")),
            other => Ok(other.clone()),
        },
        "to_int64" => {
            let value = unwrap_scalar(&args[0]);
            match value {
                Scalar::Null => Ok(Scalar::Null),
                Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => Ok(Scalar::Int64(v)),
                Scalar::UInt64(v) => i64::try_from(v)
                    .map(Scalar::Int64)
                    .map_err(|_| invalid("to_int64 overflow")),
                Scalar::Float64(v)
                    if v.is_finite()
                        && v >= -9223372036854775808.0
                        && v < 9223372036854775808.0 =>
                {
                    Ok(Scalar::Int64(v.trunc() as i64))
                }
                Scalar::Utf8(v) => v
                    .parse::<i64>()
                    .map(Scalar::Int64)
                    .map_err(|_| invalid("invalid to_int64 text")),
                _ => Err(types(
                    "to_int64 expects in-range numeric/text scalar; no saturation",
                )),
            }
        }
        "to_float64" => {
            let value = unwrap_scalar(&args[0]);
            let n = match value {
                Scalar::Null => return Ok(Scalar::Null),
                Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => v as f64,
                Scalar::UInt64(v) => v as f64,
                Scalar::Float64(v) => v,
                Scalar::Utf8(v) => v
                    .parse::<f64>()
                    .map_err(|_| invalid("invalid to_float64 text"))?,
                _ => return Err(types("to_float64 expects numeric/text scalar")),
            };
            if !n.is_finite() {
                return Err(invalid("to_float64 requires finite output"));
            }
            Ok(Scalar::Float64(n))
        }
        "to_string" => match unwrap_scalar(&args[0]) {
            Scalar::Null => Ok(Scalar::Null),
            Scalar::Utf8(v) => Ok(Scalar::Utf8(v)),
            Scalar::Bool(v) => bounded_string(v.to_string()),
            Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => bounded_string(v.to_string()),
            Scalar::UInt64(v) => bounded_string(v.to_string()),
            Scalar::Float64(v) if v.is_finite() => bounded_string(v.to_string()),
            _ => Err(types(
                "to_string expects finite primitive; use json_stringify for structured values",
            )),
        },
        "json_get" => json_get(text(&args[0])?, text(&args[1])?),
        "parse_timestamp" => {
            let s = text(&args[0])?;
            if s.len() > 64 || s.ends_with("-00:00") {
                return Err(invalid(
                    "timestamp requires known offset, bounded RFC3339 text",
                ));
            }
            if let Some((_, fraction)) = s.split_once('.') {
                if fraction
                    .bytes()
                    .take_while(u8::is_ascii_digit)
                    .skip(6)
                    .any(|b| b != b'0')
                {
                    return Err(invalid("timestamp precision exceeds microseconds"));
                }
            }
            let parsed = DateTime::parse_from_rfc3339(s)
                .map_err(|_| invalid("invalid RFC3339 timestamp"))?
                .with_timezone(&Utc);
            if !(1..=9999).contains(&parsed.year())
                || parsed.timestamp_subsec_nanos() >= 1_000_000_000
            {
                return Err(invalid("timestamp outside years 1..9999 or leap second"));
            }
            Ok(Scalar::TimestampMicrosUTC(parsed.timestamp_micros()))
        }
        "format_timestamp" => {
            let n = match args[0] {
                Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => v,
                _ => return Err(types("format_timestamp expects timestamp/int64 micros")),
            };
            let date = DateTime::<Utc>::from_timestamp_micros(n)
                .filter(|d| (1..=9999).contains(&d.year()))
                .ok_or_else(|| invalid("timestamp outside years 1..9999"))?;
            bounded_string(date.to_rfc3339_opts(SecondsFormat::Micros, true))
        }
        _ => Err(invalid("unknown builtin")),
    }
}
fn json_get(s: &str, pointer: &str) -> Result<Scalar> {
    if pointer.len() > 1024 || (!pointer.is_empty() && !pointer.starts_with('/')) {
        return Err(invalid("json_get requires a <=1024 byte JSON pointer"));
    }
    let mut keys = Vec::new();
    if !pointer.is_empty() {
        for token in pointer[1..].split('/') {
            let mut key = String::new();
            let mut chars = token.chars();
            while let Some(c) = chars.next() {
                if c == '~' {
                    key.push(match chars.next() {
                        Some('0') => '~',
                        Some('1') => '/',
                        _ => return Err(invalid("invalid JSON pointer escape")),
                    });
                } else {
                    key.push(c);
                }
            }
            keys.push(key);
        }
    }
    let root = sparrow_formats::decode_dynamic_json(
        s.as_bytes(),
        &sparrow_formats::JsonLimits {
            max_bytes: MAX_BYTES,
            max_depth: 8,
        },
    )?;
    let mut value = &root;
    for key in keys {
        value = match value {
            DynamicValue::Object(_) => match value.get(&key) {
                Some(v) => v,
                None => return Ok(Scalar::Null),
            },
            DynamicValue::Array(items) => {
                if key.is_empty()
                    || !key.bytes().all(|b| b.is_ascii_digit())
                    || (key.len() > 1 && key.starts_with('0'))
                {
                    return Err(invalid("invalid JSON pointer array index"));
                }
                match key.parse::<usize>().ok().and_then(|i| items.get(i)) {
                    Some(v) => v,
                    None => return Ok(Scalar::Null),
                }
            }
            _ => return Ok(Scalar::Null),
        };
    }
    if matches!(value, DynamicValue::Null) {
        return Ok(Scalar::Null);
    }
    let result = Scalar::Dynamic(value.clone());
    if result.resident_bytes() > MAX_BYTES {
        return Err(bound());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn s(value: &str) -> Scalar {
        Scalar::utf8(value)
    }
    fn call(name: &str, args: Vec<Scalar>) -> Scalar {
        eval(name, args).unwrap()
    }
    #[test]
    fn actions_strings_unicode_literal_and_null() {
        assert_eq!(call("concat", vec![s("测"), s("🙂")]), s("测🙂"));
        assert_eq!(
            call(
                "substring",
                vec![s("测🙂ab"), Scalar::Int64(2), Scalar::Int64(2)]
            ),
            s("🙂a")
        );
        assert_eq!(
            call(
                "substring",
                vec![s("测🙂"), Scalar::Int64(i64::MAX), Scalar::Int64(9)]
            ),
            s("")
        );
        assert_eq!(
            call(
                "substring",
                vec![s("abc"), Scalar::Int64(1), Scalar::Int64(0)]
            ),
            s("")
        );
        assert_eq!(call("trim", vec![s("\u{2003} 好 \n")]), s("好"));
        assert_eq!(
            call("replace", vec![s("a.b.a"), s("."), s("🙂")]),
            s("a🙂b🙂a")
        );
        for (name, needle, expected) in [
            ("contains", ".", true),
            ("contains", ".*", false),
            ("starts_with", "a", true),
            ("ends_with", "b", true),
        ] {
            assert_eq!(
                call(name, vec![s("a.b"), s(needle)]),
                Scalar::Bool(expected)
            );
        }
        assert_eq!(call("concat", vec![s("a"), Scalar::Null]), Scalar::Null);
        assert!(eval("replace", vec![s("a"), s(""), s("x")]).is_err());
        assert!(eval(
            "substring",
            vec![s("a"), Scalar::Int64(0), Scalar::Int64(1)]
        )
        .is_err());
    }
    #[test]
    fn actions_numeric_strict_conversions_and_nonfinite() {
        assert_eq!(
            call("round", vec![Scalar::Float64(-1.5)]),
            Scalar::Float64(-2.0)
        );
        assert_eq!(
            call("floor", vec![Scalar::Float64(-1.5)]),
            Scalar::Float64(-2.0)
        );
        assert_eq!(
            call("ceil", vec![Scalar::Float64(-1.5)]),
            Scalar::Float64(-1.0)
        );
        assert_eq!(
            call("round", vec![Scalar::UInt64(u64::MAX)]),
            Scalar::UInt64(u64::MAX)
        );
        assert_eq!(
            call("to_int64", vec![Scalar::Float64(-1.9)]),
            Scalar::Int64(-1)
        );
        assert_eq!(
            call("to_int64", vec![s("-9223372036854775808")]),
            Scalar::Int64(i64::MIN)
        );
        for value in [
            s("9223372036854775808"),
            s(" 1"),
            Scalar::UInt64(u64::MAX),
            Scalar::Float64(9223372036854775808.0),
            Scalar::Float64(f64::NAN),
        ] {
            assert!(eval("to_int64", vec![value]).is_err());
        }
        for name in [
            "round",
            "floor",
            "ceil",
            "to_float64",
            "to_string",
            "json_stringify",
        ] {
            assert!(
                eval(name, vec![Scalar::Float64(f64::INFINITY)]).is_err(),
                "{name}"
            );
        }
        assert_eq!(call("to_float64", vec![s("1.25")]), Scalar::Float64(1.25));
        assert_eq!(
            call("to_string", vec![Scalar::UInt64(u64::MAX)]),
            s("18446744073709551615")
        );
        assert_eq!(
            call("to_int64", vec![Scalar::Dynamic(DynamicValue::Null)]),
            Scalar::Null
        );
    }
    #[test]
    fn actions_json_pointer_duplicate_null_and_unsigned() {
        let raw = s(r#"{"a/b":{"~key":[18446744073709551615,null]},"missing":0}"#);
        assert_eq!(
            call("json_get", vec![raw.clone(), s("/a~1b/~0key/0")]),
            Scalar::Dynamic(DynamicValue::UInt64(u64::MAX))
        );
        assert_eq!(
            call("json_get", vec![raw.clone(), s("/a~1b/~0key/1")]),
            Scalar::Null
        );
        assert_eq!(
            call("json_get", vec![raw.clone(), s("/absent")]),
            Scalar::Null
        );
        for pointer in ["/absent/~2", "bad", "/a~1b/~0key/01"] {
            assert!(eval("json_get", vec![raw.clone(), s(pointer)]).is_err());
        }
        assert!(eval("json_get", vec![s(r#"{"a":1,"a":2}"#), s("")]).is_err());
        assert!(eval("json_object", vec![s("a"), s("x"), s("a"), s("y")]).is_err());
        assert!(eval("json_object", vec![s("a"), s("x"), s("b")]).is_err());
        let object = call(
            "json_object",
            vec![s("n"), Scalar::Null, s("u"), Scalar::UInt64(u64::MAX)],
        );
        assert_eq!(
            call("json_stringify", vec![object]),
            s(r#"{"n":null,"u":18446744073709551615}"#)
        );
        assert_eq!(call("json_stringify", vec![Scalar::Null]), s("null"));
        assert_eq!(
            call("json_object", vec![Scalar::Null, s("v")]),
            Scalar::Null
        );
    }
    #[test]
    fn actions_timestamp_precision_offset_and_epoch() {
        assert_eq!(
            call("parse_timestamp", vec![s("1970-01-01T08:00:00+08:00")]),
            Scalar::TimestampMicrosUTC(0)
        );
        assert_eq!(
            call("parse_timestamp", vec![s("1969-12-31T23:59:59.999999Z")]),
            Scalar::TimestampMicrosUTC(-1)
        );
        assert_eq!(
            call("format_timestamp", vec![Scalar::Int64(-1)]),
            s("1969-12-31T23:59:59.999999Z")
        );
        assert_eq!(
            call("parse_timestamp", vec![s("1970-01-01T00:00:00.123456000Z")]),
            Scalar::TimestampMicrosUTC(123456)
        );
        for bad in [
            "2026-01-01",
            "2016-12-31T23:59:60Z",
            "1970-01-01T00:00:00-00:00",
            "1970-01-01T00:00:00.0000001Z",
            "0000-01-01T00:00:00Z",
            "2026-02-30T00:00:00Z",
        ] {
            assert!(eval("parse_timestamp", vec![s(bad)]).is_err(), "{bad}");
        }
        assert!(eval("format_timestamp", vec![Scalar::Int64(i64::MAX)]).is_err());
    }
    #[test]
    fn actions_output_limits_and_allocation_bounds() {
        let large = "x".repeat(MAX_BYTES);
        assert_eq!(call("concat", vec![s(&large), s("")]), s(&large));
        assert!(eval("concat", vec![s(&large), s("a")]).is_err());
        assert!(eval("replace", vec![s(&large), s("x"), s("xx")]).is_err());
        assert!(eval("json_stringify", vec![s(&large)]).is_err());
        for (name, args) in [
            ("concat", vec![s("ab"), s("cd")]),
            ("replace", vec![s("aaa"), s("a"), s(&"b".repeat(10000))]),
            ("to_string", vec![Scalar::Float64(f64::from_bits(1))]),
            ("json_stringify", vec![s("\0\n\"🙂")]),
        ] {
            let output = call(name, args.clone());
            let expr = crate::BoundExpr::Call {
                name: name.into(),
                args: args.into_iter().map(crate::BoundExpr::Literal).collect(),
            };
            let bound = crate::allocation::AllocationBound::for_expr(&expr).estimate(&[]);
            assert!(
                bound.value >= output.resident_bytes(),
                "{name}: {} < {}",
                bound.value,
                output.resident_bytes()
            );
            assert!(bound.allocated >= output.resident_bytes());
        }
    }
    #[test]
    fn actions_bind_type_and_nullable_contract() {
        use sparrow_model::{Field, FieldId, Schema, SchemaId};
        let schema = Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "value", DataType::Utf8, true),
                Field::new(FieldId::new(2), "d", DataType::Dynamic, false),
            ],
        )
        .unwrap();
        let expression = crate::Expr::Call {
            name: "CONCAT".into(),
            args: vec![
                crate::Expr::Literal(s("a")),
                crate::Expr::Column {
                    name: "value".into(),
                },
            ],
        };
        assert!(crate::infer_nullable(&expression, &schema).unwrap());
        assert_eq!(
            crate::infer_type(&expression, &schema).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            crate::eval(&expression, &schema, &[Scalar::Null, Scalar::Null]).unwrap(),
            Scalar::Null
        );
        let wrong = crate::Expr::Call {
            name: "concat".into(),
            args: vec![
                crate::Expr::Literal(s("a")),
                crate::Expr::Literal(Scalar::Int64(1)),
            ],
        };
        assert!(crate::bind(&wrong, &schema).is_err());
        let dynamic = crate::Expr::Call {
            name: "to_int64".into(),
            args: vec![crate::Expr::Column { name: "d".into() }],
        };
        assert!(crate::infer_nullable(&dynamic, &schema).unwrap());
    }
}

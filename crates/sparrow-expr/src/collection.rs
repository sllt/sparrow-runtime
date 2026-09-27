//! Versioned, bounded pure collection/encoding functions. No I/O or scripts.
use base64::Engine;
use sparrow_model::{DataType, DynamicValue as D, ErrorCode, Result, Scalar, SparrowError};
use std::sync::Arc;
const MAX: usize = 65536;
const ITEMS: usize = 1024;

pub(crate) fn contains(name: &str) -> bool {
    matches!(
        name,
        "array_length"
            | "array_get"
            | "array_contains"
            | "array_append"
            | "array_slice"
            | "array_concat"
            | "array_join"
            | "object_keys"
            | "object_values"
            | "object_get"
            | "object_has_key"
            | "object_remove"
            | "object_set"
            | "split"
            | "base64_encode"
            | "base64_decode"
            | "hex_encode"
            | "hex_decode"
            | "sha256"
    )
}
fn error(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message)
}
fn bound() -> SparrowError {
    SparrowError::new(
        ErrorCode::BoundExceeded,
        "collection function exceeds 1024 items or 64KiB",
    )
}
fn array_type(t: &DataType) -> bool {
    matches!(t, DataType::Array(_) | DataType::Dynamic | DataType::Null)
}
fn object_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Map { .. } | DataType::Struct(_) | DataType::Dynamic | DataType::Null
    )
}
fn text_type(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Null)
}
fn integer_type(t: &DataType) -> bool {
    matches!(t, DataType::Int64 | DataType::Null)
}
pub(crate) fn signature(name: &str, t: &[DataType]) -> Result<DataType> {
    crate::check_call_arity(name, t.len())?;
    let (valid, out) = match name {
        "array_length" => (array_type(&t[0]), DataType::Int64),
        "array_get" => (array_type(&t[0]) && integer_type(&t[1]), DataType::Dynamic),
        "array_contains" => (array_type(&t[0]), DataType::Bool),
        "array_append" => (array_type(&t[0]), DataType::Dynamic),
        "array_slice" => (
            array_type(&t[0]) && integer_type(&t[1]) && integer_type(&t[2]),
            DataType::Dynamic,
        ),
        "array_concat" => (array_type(&t[0]) && array_type(&t[1]), DataType::Dynamic),
        "array_join" => (array_type(&t[0]) && text_type(&t[1]), DataType::Utf8),
        "object_keys" | "object_values" => (object_type(&t[0]), DataType::Dynamic),
        "object_get" | "object_remove" | "object_set" => {
            (object_type(&t[0]) && text_type(&t[1]), DataType::Dynamic)
        }
        "object_has_key" => (object_type(&t[0]) && text_type(&t[1]), DataType::Bool),
        "split" => (text_type(&t[0]) && text_type(&t[1]), DataType::Dynamic),
        "base64_decode" | "hex_decode" => (text_type(&t[0]), DataType::Bytes),
        "base64_encode" | "hex_encode" | "sha256" => (
            matches!(t[0], DataType::Utf8 | DataType::Bytes | DataType::Null),
            DataType::Utf8,
        ),
        _ => return Err(error("unknown collection function")),
    };
    if !valid {
        return Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "collection function argument types do not match",
        ));
    }
    Ok(out)
}
fn array(v: &Scalar) -> Result<&[D]> {
    match v {
        Scalar::Dynamic(D::Array(v)) if v.len() <= ITEMS => Ok(v),
        Scalar::Dynamic(D::Array(_)) => Err(bound()),
        _ => Err(error("expected array")),
    }
}
fn object(v: &Scalar) -> Result<&[(Arc<str>, D)]> {
    match v {
        Scalar::Dynamic(D::Object(v)) if v.len() <= ITEMS => Ok(v),
        Scalar::Dynamic(D::Object(_)) => Err(bound()),
        _ => Err(error("expected object")),
    }
}
fn text(v: &Scalar) -> Result<&str> {
    if let Scalar::Utf8(v) = v {
        Ok(v)
    } else {
        Err(error("expected UTF8"))
    }
}
fn bytes(v: &Scalar) -> Result<&[u8]> {
    match v {
        Scalar::Utf8(v) => Ok(v.as_bytes()),
        Scalar::Bytes(v) => Ok(v),
        _ => Err(error("expected UTF8 or bytes")),
    }
}
fn index(v: &Scalar) -> Result<usize> {
    match v {
        Scalar::Int64(n) if *n >= 0 => usize::try_from(*n).map_err(|_| bound()),
        _ => Err(error("index/count must be a nonnegative Int64")),
    }
}
fn value(v: Option<&D>) -> Scalar {
    match v {
        None | Some(D::Null) => Scalar::Null,
        Some(v) => Scalar::Dynamic(v.clone()),
    }
}
fn collection(v: D) -> Result<Scalar> {
    let v = Scalar::Dynamic(v);
    if v.resident_bytes() > MAX {
        Err(bound())
    } else {
        Ok(v)
    }
}
fn make_array(items: Vec<D>) -> Result<Scalar> {
    if items.len() > ITEMS {
        return Err(bound());
    }
    collection(D::Array(items.into()))
}
fn bounded_text(s: String) -> Result<Scalar> {
    if s.len() > MAX {
        Err(bound())
    } else {
        Ok(Scalar::utf8(s))
    }
}
fn hex(data: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(data.len() * 2);
    for v in data {
        s.push(DIGITS[(v >> 4) as usize] as char);
        s.push(DIGITS[(v & 15) as usize] as char);
    }
    s
}
pub(crate) fn eval(name: &str, args: Vec<Scalar>) -> Result<Scalar> {
    crate::check_call_arity(name, args.len())?;
    let mut types: [DataType; 3] = std::array::from_fn(|_| DataType::Null);
    for (i, arg) in args.iter().enumerate() {
        types[i] = arg.data_type();
        let size = match arg {
            Scalar::Utf8(v) => v.len(),
            Scalar::Bytes(v) => v.len(),
            _ => arg.resident_bytes(),
        };
        if size > MAX {
            return Err(bound());
        }
    }
    signature(name, &types[..args.len()])?;
    // SQL NULL differs from an explicit array/object null element.
    let null = |v: &Scalar| matches!(v, Scalar::Null | Scalar::Dynamic(D::Null));
    let null_prefix = match name {
        "array_append" => 1,
        "object_set" => 2,
        _ => args.len(),
    };
    if args[..null_prefix].iter().any(null) {
        return Ok(Scalar::Null);
    }
    match name {
        "array_length" => Ok(Scalar::Int64(array(&args[0])?.len() as i64)),
        "array_get" => Ok(value(array(&args[0])?.get(index(&args[1])?))),
        "array_contains" => Ok(Scalar::Bool(
            array(&args[0])?.contains(&crate::dynamic_from_scalar(&args[1])),
        )),
        "array_append" => {
            let old = array(&args[0])?;
            if old.len() == ITEMS {
                return Err(bound());
            }
            let mut items = old.to_vec();
            items.push(crate::dynamic_from_scalar(&args[1]));
            make_array(items)
        }
        "array_slice" => {
            let a = array(&args[0])?;
            let start = index(&args[1])?.min(a.len());
            let end = start.saturating_add(index(&args[2])?).min(a.len());
            make_array(a[start..end].to_vec())
        }
        "array_concat" => {
            let a = array(&args[0])?;
            let b = array(&args[1])?;
            if a.len() + b.len() > ITEMS {
                return Err(bound());
            }
            make_array(a.iter().chain(b).cloned().collect())
        }
        "array_join" => {
            let a = array(&args[0])?;
            let sep = text(&args[1])?;
            let mut s = String::new();
            for (i, v) in a.iter().enumerate() {
                let v = match v {
                    D::Null => return Ok(Scalar::Null),
                    D::Utf8(v) => v,
                    _ => return Err(error("array_join needs UTF8 elements")),
                };
                let extra = v.len().saturating_add(if i > 0 { sep.len() } else { 0 });
                if s.len().saturating_add(extra) > MAX {
                    return Err(bound());
                }
                if i > 0 {
                    s.push_str(sep);
                }
                s.push_str(v);
            }
            bounded_text(s)
        }
        "object_keys" => make_array(
            object(&args[0])?
                .iter()
                .map(|(k, _)| D::Utf8(k.clone()))
                .collect(),
        ),
        "object_values" => make_array(object(&args[0])?.iter().map(|(_, v)| v.clone()).collect()),
        "object_get" => {
            let key = text(&args[1])?;
            Ok(value(
                object(&args[0])?
                    .iter()
                    .find(|(k, _)| k.as_ref() == key)
                    .map(|(_, v)| v),
            ))
        }
        "object_has_key" => {
            let k = text(&args[1])?;
            Ok(Scalar::Bool(
                object(&args[0])?.iter().any(|(key, _)| key.as_ref() == k),
            ))
        }
        "object_remove" | "object_set" => {
            let key = text(&args[1])?;
            let mut pairs = object(&args[0])?.to_vec();
            if let Some(i) = pairs.iter().position(|(k, _)| k.as_ref() == key) {
                if name == "object_remove" {
                    pairs.remove(i);
                } else {
                    pairs[i].1 = crate::dynamic_from_scalar(&args[2]);
                }
            } else if name == "object_set" {
                if pairs.len() == ITEMS {
                    return Err(bound());
                }
                pairs.push((Arc::from(key), crate::dynamic_from_scalar(&args[2])));
            }
            collection(D::Object(pairs.into()))
        }
        "split" => {
            let sep = text(&args[1])?;
            if sep.is_empty() {
                return Err(error("split separator must not be empty"));
            }
            let mut parts = Vec::new();
            let mut resident = 128usize;
            for s in text(&args[0])?.split(sep) {
                resident = resident.saturating_add(s.len() + 96);
                if parts.len() >= ITEMS || resident > MAX {
                    return Err(bound());
                }
                parts.push(D::utf8(s));
            }
            make_array(parts)
        }
        "base64_encode" => {
            let data = bytes(&args[0])?;
            if data.len().div_ceil(3) * 4 > MAX {
                return Err(bound());
            }
            bounded_text(base64::engine::general_purpose::STANDARD.encode(data))
        }
        "base64_decode" => base64::engine::general_purpose::STANDARD
            .decode(text(&args[0])?)
            .map(Scalar::bytes)
            .map_err(|_| error("invalid canonical padded base64")),
        "hex_encode" => {
            let data = bytes(&args[0])?;
            if data.len() > MAX / 2 {
                return Err(bound());
            }
            bounded_text(hex(data))
        }
        "hex_decode" => {
            let s = text(&args[0])?.as_bytes();
            if s.len() % 2 != 0 {
                return Err(error("hex length must be even"));
            }
            let nibble = |b: u8| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                b'A'..=b'F' => Ok(b - b'A' + 10),
                _ => Err(error("invalid ASCII hex")),
            };
            let data = s
                .chunks_exact(2)
                .map(|p| Ok(nibble(p[0])? * 16 + nibble(p[1])?))
                .collect::<Result<Vec<u8>>>()?;
            Ok(Scalar::bytes(data))
        }
        "sha256" => bounded_text(hex(ring::digest::digest(
            &ring::digest::SHA256,
            bytes(&args[0])?,
        )
        .as_ref())),
        _ => Err(error("unknown collection function")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn analysis_collection_remaining_goldens_and_fail_closed() {
        let array = Scalar::Dynamic(D::Array(vec![D::Int64(1), D::Int64(2), D::Null].into()));
        let expected = |items: Vec<D>| Scalar::Dynamic(D::Array(items.into()));
        assert_eq!(
            eval(
                "array_slice",
                vec![array.clone(), Scalar::Int64(1), Scalar::Int64(1)]
            )
            .unwrap(),
            expected(vec![D::Int64(2)])
        );
        assert_eq!(
            eval(
                "array_slice",
                vec![array.clone(), Scalar::Int64(9), Scalar::Int64(i64::MAX)]
            )
            .unwrap(),
            expected(vec![])
        );
        assert_eq!(
            eval("array_contains", vec![array.clone(), Scalar::Int64(2)]).unwrap(),
            Scalar::Bool(true)
        );
        assert_eq!(
            eval("array_contains", vec![array.clone(), Scalar::UInt64(2)]).unwrap(),
            Scalar::Bool(false)
        );
        assert_eq!(
            eval("array_append", vec![array.clone(), Scalar::Null]).unwrap(),
            expected(vec![D::Int64(1), D::Int64(2), D::Null, D::Null])
        );
        assert_eq!(
            eval(
                "array_concat",
                vec![array.clone(), expected(vec![D::utf8("x")])]
            )
            .unwrap(),
            expected(vec![D::Int64(1), D::Int64(2), D::Null, D::utf8("x")])
        );
        let object = Scalar::Dynamic(D::object(vec![("b", D::Int64(1)), ("a", D::Int64(2))]));
        assert_eq!(
            eval("object_values", vec![object.clone()]).unwrap(),
            expected(vec![D::Int64(1), D::Int64(2)])
        );
        assert_eq!(
            eval("object_remove", vec![object.clone(), Scalar::utf8("b")]).unwrap(),
            Scalar::Dynamic(D::object(vec![("a", D::Int64(2))]))
        );
        assert_eq!(
            eval("object_get", vec![object, Scalar::utf8("missing")]).unwrap(),
            Scalar::Null
        );
        assert_eq!(
            eval("sha256", vec![Scalar::utf8("abc")]).unwrap(),
            Scalar::utf8("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert!(eval("array_get", vec![array.clone(), Scalar::Int64(-1)]).is_err());
        assert!(eval("array_concat", vec![expected(vec![D::Null; ITEMS]), array]).is_err());
        assert!(eval("split", vec![Scalar::utf8("a"), Scalar::utf8("")]).is_err());
        assert!(eval("hex_decode", vec![Scalar::utf8("a")]).is_err());
        assert!(eval("hex_decode", vec![Scalar::utf8("gg")]).is_err());
        assert!(eval(
            "object_get",
            vec![Scalar::Dynamic(D::Array(vec![].into())), Scalar::utf8("a")]
        )
        .is_err());
        assert!(eval("sha256", vec![Scalar::utf8("a".repeat(MAX + 1))]).is_err());
        for d in crate::semantics::FUNCTIONS
            .iter()
            .filter(|d| contains(d.name))
        {
            assert!(
                signature(d.name, &[]).is_err(),
                "{} accepts missing arguments",
                d.name
            );
        }
    }
    #[test]
    fn analysis_collection_null_order_roundtrips_and_bounds() {
        let split = eval("split", vec![Scalar::utf8("甲,,🙂"), Scalar::utf8(",")]).unwrap();
        assert_eq!(
            eval("array_length", vec![split.clone()]).unwrap(),
            Scalar::Int64(3)
        );
        assert_eq!(
            eval("array_get", vec![split.clone(), Scalar::Int64(1)]).unwrap(),
            Scalar::Dynamic(D::utf8(""))
        );
        assert_eq!(
            eval("array_get", vec![split.clone(), Scalar::Int64(9)]).unwrap(),
            Scalar::Null
        );
        assert_eq!(
            eval("array_join", vec![split, Scalar::utf8("/")]).unwrap(),
            Scalar::utf8("甲//🙂")
        );
        for (enc, dec) in [
            ("base64_encode", "base64_decode"),
            ("hex_encode", "hex_decode"),
        ] {
            let raw = Scalar::bytes([0, 255, 1, 128]);
            let coded = eval(enc, vec![raw.clone()]).unwrap();
            assert_eq!(eval(dec, vec![coded]).unwrap(), raw);
        }
        assert_eq!(
            eval("sha256", vec![Scalar::utf8("abc")]).unwrap(),
            Scalar::utf8("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert!(eval("base64_decode", vec![Scalar::utf8("YQ")]).is_err());
        assert!(eval("hex_decode", vec![Scalar::utf8("😃")]).is_err());
        assert!(eval("split", vec![Scalar::utf8("a"), Scalar::utf8("")]).is_err());
        assert!(eval(
            "split",
            vec![Scalar::utf8(",".repeat(1024)), Scalar::utf8(",")]
        )
        .is_err());
        let object = Scalar::Dynamic(D::object(vec![("b", D::Int64(1)), ("a", D::Null)]));
        assert_eq!(
            eval("object_get", vec![object.clone(), Scalar::utf8("a")]).unwrap(),
            Scalar::Null
        );
        assert_eq!(
            eval("object_has_key", vec![object.clone(), Scalar::utf8("a")]).unwrap(),
            Scalar::Bool(true)
        );
        let changed = eval(
            "object_set",
            vec![object.clone(), Scalar::utf8("b"), Scalar::Null],
        )
        .unwrap();
        assert_eq!(
            eval("object_keys", vec![changed]).unwrap(),
            Scalar::Dynamic(D::Array(vec![D::utf8("b"), D::utf8("a")].into()))
        );
        assert_eq!(
            eval(
                "object_remove",
                vec![object.clone(), Scalar::utf8("missing")]
            )
            .unwrap(),
            object
        );
    }
}

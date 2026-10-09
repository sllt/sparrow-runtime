//! Explicit PostgreSQL ⇄ Sparrow type mapping (binary wire format).
//!
//! | PostgreSQL                        | Sparrow field type                  |
//! |-----------------------------------|-------------------------------------|
//! | int2, int4, int8                  | Int64 (writes are range-checked)    |
//! | float4, float8                    | Float64 (finite only)               |
//! | numeric                           | Utf8 (exact decimal text) or Float64 (nearest, lossy) |
//! | text, varchar, bpchar, name       | Utf8                                |
//! | bool                              | Bool                                |
//! | timestamp, timestamptz            | TimestampMicrosUTC (timestamp read/written as UTC; ±infinity refused) |
//! | json, jsonb                       | Utf8 (JSON text)                    |
//! | bytea                             | Bytes                               |
//!
//! Any other server type (including domains, enums, arrays, uuid, date,
//! time, interval, money) is refused when the statement is prepared, never
//! guessed at run time.

use sparrow_model::{DataType, Scalar};
use tokio_postgres::types::{FromSql, Type};

/// Microseconds between 1970-01-01 and the PostgreSQL epoch 2000-01-01.
pub const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PgKind {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Varchar,
    Bpchar,
    Name,
    Bool,
    Timestamp,
    Timestamptz,
    Json,
    Jsonb,
    Bytea,
}

impl PgKind {
    pub fn from_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            21 => Self::Int2,
            23 => Self::Int4,
            20 => Self::Int8,
            700 => Self::Float4,
            701 => Self::Float8,
            1700 => Self::Numeric,
            25 => Self::Text,
            1043 => Self::Varchar,
            1042 => Self::Bpchar,
            19 => Self::Name,
            16 => Self::Bool,
            1114 => Self::Timestamp,
            1184 => Self::Timestamptz,
            114 => Self::Json,
            3802 => Self::Jsonb,
            17 => Self::Bytea,
            _ => return None,
        })
    }

    pub fn from_type(ty: &Type) -> Option<Self> {
        Self::from_oid(ty.oid())
    }

    pub fn oid(self) -> u32 {
        match self {
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Int8 => 20,
            Self::Float4 => 700,
            Self::Float8 => 701,
            Self::Numeric => 1700,
            Self::Text => 25,
            Self::Varchar => 1043,
            Self::Bpchar => 1042,
            Self::Name => 19,
            Self::Bool => 16,
            Self::Timestamp => 1114,
            Self::Timestamptz => 1184,
            Self::Json => 114,
            Self::Jsonb => 3802,
            Self::Bytea => 17,
        }
    }

    /// Schema-qualified type name (safe against `search_path`).
    pub fn sql_name(self) -> &'static str {
        match self {
            Self::Int2 => "pg_catalog.int2",
            Self::Int4 => "pg_catalog.int4",
            Self::Int8 => "pg_catalog.int8",
            Self::Float4 => "pg_catalog.float4",
            Self::Float8 => "pg_catalog.float8",
            Self::Numeric => "pg_catalog.numeric",
            Self::Text => "pg_catalog.text",
            Self::Varchar => "pg_catalog.varchar",
            Self::Bpchar => "pg_catalog.bpchar",
            Self::Name => "pg_catalog.name",
            Self::Bool => "pg_catalog.bool",
            Self::Timestamp => "pg_catalog.timestamp",
            Self::Timestamptz => "pg_catalog.timestamptz",
            Self::Json => "pg_catalog.json",
            Self::Jsonb => "pg_catalog.jsonb",
            Self::Bytea => "pg_catalog.bytea",
        }
    }

    /// Element type a parameter array of this kind is sent as. `numeric`
    /// travels as text (exact) and is cast server-side.
    pub fn wire_element(self) -> PgKind {
        match self {
            Self::Numeric => Self::Text,
            other => other,
        }
    }

    /// `$n::<elem>[]` plus, for numeric, `::numeric[]`.
    pub fn array_param(self, n: usize) -> String {
        match self {
            Self::Numeric => format!("${n}::pg_catalog.text[]::pg_catalog.numeric[]"),
            other => format!("${n}::{}[]", other.sql_name()),
        }
    }

    pub fn is_text(self) -> bool {
        matches!(self, Self::Text | Self::Varchar | Self::Bpchar | Self::Name)
    }

    /// Fixed binary width, `None` for variable-length kinds.
    pub fn fixed_width(self) -> Option<usize> {
        match self {
            Self::Int2 => Some(2),
            Self::Int4 | Self::Float4 => Some(4),
            Self::Int8 | Self::Float8 | Self::Timestamp | Self::Timestamptz => Some(8),
            Self::Bool => Some(1),
            _ => None,
        }
    }

    /// Whether a Sparrow field of `data_type` reads from / writes to this
    /// kind.
    pub fn maps_to(self, data_type: &DataType) -> bool {
        match self {
            Self::Int2 | Self::Int4 | Self::Int8 => *data_type == DataType::Int64,
            Self::Float4 | Self::Float8 => *data_type == DataType::Float64,
            Self::Numeric => matches!(data_type, DataType::Utf8 | DataType::Float64),
            Self::Text | Self::Varchar | Self::Bpchar | Self::Name | Self::Json | Self::Jsonb => {
                *data_type == DataType::Utf8
            }
            Self::Bool => *data_type == DataType::Bool,
            Self::Timestamp | Self::Timestamptz => *data_type == DataType::TimestampMicrosUTC,
            Self::Bytea => *data_type == DataType::Bytes,
        }
    }

    /// Upper bound of the binary wire size of a column value, as a SQL
    /// expression over `col` (already quoted/qualified). Used server-side to
    /// flag oversize rows before they are sent.
    pub fn size_sql(self, col: &str) -> String {
        match self.fixed_width() {
            Some(w) => w.to_string(),
            None => match self {
                Self::Bytea => format!("COALESCE(pg_catalog.octet_length({col}), 0)"),
                // Binary numeric is at most 8 bytes of header plus 2 bytes per
                // 4 digits, which never exceeds the text form by more than 16.
                Self::Numeric => {
                    format!("COALESCE(pg_catalog.octet_length({col}::pg_catalog.text), 0) + 16")
                }
                Self::Jsonb => {
                    format!("COALESCE(pg_catalog.octet_length({col}::pg_catalog.text), 0) + 1")
                }
                _ => format!("COALESCE(pg_catalog.octet_length({col}::pg_catalog.text), 0)"),
            },
        }
    }
}

/// A column value exactly as received (`None` = SQL NULL).
pub struct Raw<'a>(pub Option<&'a [u8]>);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(Some(raw)))
    }
    fn from_sql_null(
        _: &Type,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(None))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}

fn be<const N: usize>(raw: &[u8]) -> Result<[u8; N], &'static str> {
    raw.try_into().map_err(|_| "postgres_value_length_mismatch")
}

/// Decode one binary value of `kind` into a Sparrow value of `data_type`.
/// `max_text` bounds text produced from numeric values (checked before
/// allocating). Refuses NaN/±Infinity floats and numerics, ±infinity
/// timestamps, invalid UTF-8 and malformed binary values.
pub fn decode(
    kind: PgKind,
    data_type: &DataType,
    raw: &[u8],
    max_text: usize,
) -> Result<Scalar, &'static str> {
    if !kind.maps_to(data_type) {
        return Err("postgres_type_mismatch");
    }
    Ok(match kind {
        PgKind::Int2 => Scalar::Int64(i16::from_be_bytes(be(raw)?).into()),
        PgKind::Int4 => Scalar::Int64(i32::from_be_bytes(be(raw)?).into()),
        PgKind::Int8 => Scalar::Int64(i64::from_be_bytes(be(raw)?)),
        PgKind::Float4 => finite(f32::from_be_bytes(be(raw)?).into())?,
        PgKind::Float8 => finite(f64::from_be_bytes(be(raw)?))?,
        PgKind::Numeric => {
            let text = numeric_to_text(raw, max_text)?;
            if *data_type == DataType::Float64 {
                finite(
                    text.parse::<f64>()
                        .map_err(|_| "postgres_numeric_invalid")?,
                )?
            } else {
                Scalar::Utf8(text.into())
            }
        }
        PgKind::Text | PgKind::Varchar | PgKind::Bpchar | PgKind::Name | PgKind::Json => {
            Scalar::Utf8(utf8(raw)?.into())
        }
        PgKind::Jsonb => match raw.split_first() {
            Some((1, rest)) => Scalar::Utf8(utf8(rest)?.into()),
            _ => return Err("postgres_jsonb_version_unsupported"),
        },
        PgKind::Bool => match raw {
            [0] => Scalar::Bool(false),
            [1] => Scalar::Bool(true),
            _ => return Err("postgres_bool_invalid"),
        },
        PgKind::Timestamp | PgKind::Timestamptz => {
            Scalar::TimestampMicrosUTC(timestamp_from_pg(i64::from_be_bytes(be(raw)?))?)
        }
        PgKind::Bytea => Scalar::Bytes(raw.into()),
    })
}

fn finite(v: f64) -> Result<Scalar, &'static str> {
    if v.is_finite() {
        Ok(Scalar::Float64(v))
    } else {
        Err("postgres_float_not_finite")
    }
}

fn utf8(raw: &[u8]) -> Result<&str, &'static str> {
    std::str::from_utf8(raw).map_err(|_| "postgres_text_invalid_utf8")
}

/// PostgreSQL timestamp (µs since 2000-01-01) → µs since 1970-01-01.
pub fn timestamp_from_pg(v: i64) -> Result<i64, &'static str> {
    if v == i64::MAX || v == i64::MIN {
        return Err("postgres_timestamp_infinite");
    }
    v.checked_add(PG_EPOCH_OFFSET_MICROS)
        .ok_or("postgres_timestamp_out_of_range")
}

/// µs since 1970-01-01 → PostgreSQL timestamp. Refuses values that would
/// collide with the ±infinity sentinels or overflow.
pub fn timestamp_to_pg(v: i64) -> Result<i64, &'static str> {
    match v.checked_sub(PG_EPOCH_OFFSET_MICROS) {
        Some(pg) if pg != i64::MAX && pg != i64::MIN => Ok(pg),
        _ => Err("postgres_timestamp_out_of_range"),
    }
}

/// Binary `numeric` → its canonical decimal text (same digits as the
/// server's output function). The output length is computed and checked
/// against `max_text` before anything is allocated.
pub fn numeric_to_text(raw: &[u8], max_text: usize) -> Result<String, &'static str> {
    if raw.len() < 8 {
        return Err("postgres_numeric_invalid");
    }
    let ndigits = i16::from_be_bytes([raw[0], raw[1]]);
    let weight = i16::from_be_bytes([raw[2], raw[3]]);
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = u16::from_be_bytes([raw[6], raw[7]]);
    let negative = match sign {
        0x0000 => false,
        0x4000 => true,
        // NaN, +Infinity, -Infinity have no Sparrow value.
        _ => return Err("postgres_numeric_not_finite"),
    };
    let Ok(ndigits) = usize::try_from(ndigits) else {
        return Err("postgres_numeric_invalid");
    };
    if dscale > 0x3FFF || raw.len() != 8 + 2 * ndigits {
        return Err("postgres_numeric_invalid");
    }
    let digit = |i: i64| -> u16 {
        if i < 0 || i as usize >= ndigits {
            0
        } else {
            let at = 8 + 2 * i as usize;
            u16::from_be_bytes([raw[at], raw[at + 1]])
        }
    };
    for i in 0..ndigits as i64 {
        if digit(i) > 9999 {
            return Err("postgres_numeric_invalid");
        }
    }
    let weight = i64::from(weight);
    let dscale = usize::from(dscale);
    // Integer part: groups 0..=weight; first group without leading zeros.
    let int_groups = if weight >= 0 { weight as usize + 1 } else { 0 };
    let len = usize::from(negative)
        .saturating_add(int_groups.saturating_mul(4).max(1))
        .saturating_add(if dscale > 0 { dscale + 1 } else { 0 });
    if len > max_text {
        return Err("postgres_numeric_exceeds_limit");
    }
    let mut out = String::with_capacity(len);
    if negative {
        out.push('-');
    }
    if weight < 0 {
        out.push('0');
    } else {
        for g in 0..=weight {
            let d = digit(g);
            if g == 0 {
                out.push_str(&d.to_string());
            } else {
                out.push_str(&format!("{d:04}"));
            }
        }
    }
    if dscale > 0 {
        out.push('.');
        let mut written = 0;
        let mut g = weight + 1;
        while written < dscale {
            let group = format!("{:04}", digit(g));
            for c in group.chars() {
                if written == dscale {
                    break;
                }
                out.push(c);
                written += 1;
            }
            g += 1;
        }
    }
    Ok(out)
}

/// `true` if `s` is a finite decimal literal the server's numeric input
/// accepts: `[+-]digits[.digits][e[+-]digits]` (at least one digit, no
/// spaces, no NaN/Infinity).
pub fn is_decimal_literal(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if matches!(b.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - start;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let f = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        digits += i - f;
    }
    if digits == 0 {
        return false;
    }
    if i < b.len() && matches!(b[i], b'e' | b'E') {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let e = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == e || i - e > 6 {
            return false;
        }
    }
    i == b.len()
}

/// Binary length of `value` as an element of a `kind` parameter array, or
/// why it cannot be written. NULL is `Ok(0)` (sent as a NULL element).
/// Nothing is allocated.
pub fn encoded_len(kind: PgKind, value: &Scalar) -> Result<usize, &'static str> {
    let text_ok = |s: &str| -> Result<usize, &'static str> {
        if s.as_bytes().contains(&0) {
            Err("postgres_text_contains_nul")
        } else {
            Ok(s.len())
        }
    };
    match (kind, value) {
        (_, Scalar::Null) => Ok(0),
        (PgKind::Int2, Scalar::Int64(v)) => i16::try_from(*v)
            .map(|_| 2)
            .map_err(|_| "postgres_int2_out_of_range"),
        (PgKind::Int4, Scalar::Int64(v)) => i32::try_from(*v)
            .map(|_| 4)
            .map_err(|_| "postgres_int4_out_of_range"),
        (PgKind::Int8, Scalar::Int64(_)) => Ok(8),
        (PgKind::Float4, Scalar::Float64(v)) => {
            if v.is_finite() && (*v as f32).is_finite() {
                Ok(4)
            } else {
                Err("postgres_float4_out_of_range")
            }
        }
        (PgKind::Float8, Scalar::Float64(v)) => {
            if v.is_finite() {
                Ok(8)
            } else {
                Err("postgres_float_not_finite")
            }
        }
        (PgKind::Numeric, Scalar::Utf8(s)) => {
            if is_decimal_literal(s) {
                Ok(s.len())
            } else {
                Err("postgres_numeric_literal_invalid")
            }
        }
        (PgKind::Numeric, Scalar::Float64(v)) => {
            if v.is_finite() {
                Ok(format!("{v}").len())
            } else {
                Err("postgres_float_not_finite")
            }
        }
        (k, Scalar::Utf8(s)) if k.is_text() => text_ok(s),
        (PgKind::Json, Scalar::Utf8(s)) => {
            text_ok(s)?;
            valid_json(s, false)?;
            Ok(s.len())
        }
        (PgKind::Jsonb, Scalar::Utf8(s)) => {
            text_ok(s)?;
            valid_json(s, true)?;
            Ok(s.len() + 1)
        }
        (PgKind::Bool, Scalar::Bool(_)) => Ok(1),
        (PgKind::Timestamp | PgKind::Timestamptz, Scalar::TimestampMicrosUTC(v)) => {
            timestamp_to_pg(*v).map(|_| 8)
        }
        (PgKind::Bytea, Scalar::Bytes(b)) => Ok(b.len()),
        _ => Err("postgres_type_mismatch"),
    }
}

fn valid_json(s: &str, jsonb: bool) -> Result<(), &'static str> {
    // Skipped without building a value (and without recursion).
    serde_json::from_str::<serde::de::IgnoredAny>(s).map_err(|_| "postgres_json_invalid")?;
    if jsonb && s.contains("\\u0000") {
        return Err("postgres_jsonb_nul_escape");
    }
    Ok(())
}

/// Append the binary element (`int32 length` + bytes, `-1` for NULL) of a
/// `kind` column value (numeric as its text). `value` must have passed
/// [`encoded_len`] for `kind`.
pub fn write_element(kind: PgKind, value: &Scalar, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    match (kind, value) {
        (_, Scalar::Null) => {
            out[start..start + 4].copy_from_slice(&(-1i32).to_be_bytes());
            return;
        }
        (PgKind::Int2, Scalar::Int64(v)) => out.extend_from_slice(&(*v as i16).to_be_bytes()),
        (PgKind::Int4, Scalar::Int64(v)) => out.extend_from_slice(&(*v as i32).to_be_bytes()),
        (PgKind::Int8, Scalar::Int64(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (PgKind::Float4, Scalar::Float64(v)) => out.extend_from_slice(&(*v as f32).to_be_bytes()),
        (PgKind::Float8, Scalar::Float64(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (PgKind::Numeric, Scalar::Float64(v)) => out.extend_from_slice(format!("{v}").as_bytes()),
        (PgKind::Jsonb, Scalar::Utf8(s)) => {
            out.push(1);
            out.extend_from_slice(s.as_bytes());
        }
        (_, Scalar::Utf8(s)) => out.extend_from_slice(s.as_bytes()),
        (_, Scalar::Bool(b)) => out.push(u8::from(*b)),
        (_, Scalar::TimestampMicrosUTC(v)) => out.extend_from_slice(
            &timestamp_to_pg(*v)
                .expect("checked by encoded_len")
                .to_be_bytes(),
        ),
        (_, Scalar::Bytes(b)) => out.extend_from_slice(b),
        _ => unreachable!("checked by encoded_len"),
    }
    let len = (out.len() - start - 4) as i32;
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
}

/// Bytes of a one-dimensional binary array header.
pub const ARRAY_HEADER: usize = 20;

/// One-dimensional parameter array built element by element. The element
/// bytes live in `data`; the header is written when the array is sent.
#[derive(Debug)]
pub struct ArrayParam {
    pub kind: PgKind,
    pub data: Vec<u8>,
    pub len: usize,
    pub has_null: bool,
}

impl ArrayParam {
    pub fn new(kind: PgKind) -> Self {
        Self {
            kind,
            data: Vec::new(),
            len: 0,
            has_null: false,
        }
    }

    pub fn push(&mut self, value: &Scalar) {
        if value.is_null() {
            self.has_null = true;
        }
        write_element(self.kind, value, &mut self.data);
        self.len += 1;
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.len = 0;
        self.has_null = false;
    }

    pub fn wire_len(&self) -> usize {
        ARRAY_HEADER + self.data.len()
    }
}

impl tokio_postgres::types::ToSql for ArrayParam {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        let len = i32::try_from(self.len).map_err(|_| "postgres array too long")?;
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&i32::from(self.has_null).to_be_bytes());
        out.extend_from_slice(&self.kind.wire_element().oid().to_be_bytes());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&self.data);
        Ok(tokio_postgres::types::IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

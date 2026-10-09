//! InfluxDB line protocol encoder (one point per row) and the column
//! mapping it is compiled from.
//!
//! Escaping follows the InfluxDB v2 line protocol reference: measurement
//! names escape `,` and space; tag keys, tag values and field keys escape
//! `,`, `=` and space; string field values escape `"` and `\`. Values the
//! v2 parser cannot round-trip are refused instead of being rewritten:
//! `\n` / `\r` anywhere (the v2 parser drops `\r` from string values), a
//! trailing `\` on a name or tag value (it would escape the delimiter that
//! follows), a `\` directly before `,`, `=` or space in a measurement or
//! field key (InfluxDB 2.9.1 rejects such field keys with 400 and silently
//! drops such points, answering 204), a measurement starting with `#` (a
//! comment line), names starting with `_` (reserved namespace) and the
//! reserved key `time`. Tag keys and values round-trip those sequences.
//! Empty tag values and null tags are omitted (InfluxDB has no empty tag);
//! null fields are omitted; a point needs at least one field. Floats must be
//! finite. String field values are limited to 64 KiB.

use sparrow_model::{DataType, ErrorCode, Row, Scalar, Schema, SparrowError};

/// Largest string field value InfluxDB documents (64 KiB).
pub const MAX_STRING_FIELD_BYTES: usize = 64 * 1024;
/// Timestamps InfluxDB accepts, in nanoseconds.
const MIN_TIME_NS: i128 = -9_223_372_036_854_775_806;
const MAX_TIME_NS: i128 = 9_223_372_036_854_775_806;

/// Timestamp precision of the `precision` query parameter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Precision {
    Ns,
    #[default]
    Us,
    Ms,
    S,
}

impl Precision {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "ns" => Self::Ns,
            "us" => Self::Us,
            "ms" => Self::Ms,
            "s" => Self::S,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ns => "ns",
            Self::Us => "us",
            Self::Ms => "ms",
            Self::S => "s",
        }
    }

    /// Nanoseconds per unit.
    fn ns_per_unit(self) -> i128 {
        match self {
            Self::Ns => 1,
            Self::Us => 1_000,
            Self::Ms => 1_000_000,
            Self::S => 1_000_000_000,
        }
    }

    /// Convert microseconds since the epoch to this precision (floor for
    /// `ms` / `s`). `None` when the point would fall outside the range
    /// InfluxDB accepts.
    pub fn from_micros(self, micros: i64) -> Option<i64> {
        let micros = i128::from(micros);
        let value = match self {
            Self::Ns => micros * 1_000,
            Self::Us => micros,
            Self::Ms => micros.div_euclid(1_000),
            Self::S => micros.div_euclid(1_000_000),
        };
        let ns = value * self.ns_per_unit();
        if !(MIN_TIME_NS..=MAX_TIME_NS).contains(&ns) {
            return None;
        }
        i64::try_from(value).ok()
    }
}

/// Where the measurement name comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Measurement {
    Fixed(String),
    /// A `utf8` column; each row's value is checked like a fixed name.
    Column(String),
}

/// Column mapping of the InfluxDB Sink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InfluxMapping {
    pub measurement: Measurement,
    /// `utf8` columns written as tags.
    pub tags: Vec<String>,
    /// Field columns; `None` = every column that is not the measurement
    /// column, a tag or the time column.
    pub fields: Option<Vec<String>>,
    /// `timestamp` column. Without it InfluxDB assigns its receive time.
    pub time_column: Option<String>,
    pub precision: Precision,
}

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

/// `\` directly followed by `,`, `=` or space.
fn backslash_before_special(s: &str) -> bool {
    s.as_bytes()
        .windows(2)
        .any(|w| w[0] == b'\\' && matches!(w[1], b',' | b'=' | b' '))
}

/// Static name rules shared by the measurement, tag keys and field keys.
fn check_name(what: &str, name: &str) -> Result<(), SparrowError> {
    let strict = what != "tag key";
    let problem = if name.is_empty() {
        Some("is empty")
    } else if name.len() > 256 {
        Some("is longer than 256 bytes")
    } else if name.starts_with('_') {
        Some("starts with `_` (reserved by InfluxDB)")
    } else if name.contains(['\n', '\r']) {
        Some("contains a line break")
    } else if name.ends_with('\\') {
        Some("ends with `\\` (it would escape the following delimiter)")
    } else if strict && backslash_before_special(name) {
        Some("has `\\` before `,`, `=` or space (InfluxDB cannot store it)")
    } else {
        None
    };
    match problem {
        Some(p) => Err(err(
            ErrorCode::InvalidArgument,
            format!("InfluxDB {what} `{}` {p}", name.escape_debug()),
        )),
        None => Ok(()),
    }
}

impl InfluxMapping {
    /// Conservative transient + retained mapping storage: escaped key
    /// buffers, tag sorting/indices, validation sets and Vec growth. Charge
    /// before compile, retain until this compiled mapping is dropped.
    pub fn workspace_bytes(&self, schema: &Schema) -> usize {
        schema.fields.iter().fold(8 * 1024usize, |bytes, field| {
            bytes
                .saturating_add(field.name.len().saturating_mul(8))
                .saturating_add(256)
        })
    }

    /// Schema-independent checks (spec / validate time).
    pub fn check(&self) -> Result<(), SparrowError> {
        if let Measurement::Fixed(name) = &self.measurement {
            check_name("measurement", name)?;
            if name.starts_with('#') {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "InfluxDB measurement must not start with `#` (a comment line)",
                ));
            }
        }
        let fields = self.fields.as_deref().unwrap_or_default();
        if self.tags.len() > 64 || fields.len() > 256 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "InfluxDB mapping: at most 64 tags and 256 fields",
            ));
        }
        if self.fields.as_ref().is_some_and(Vec::is_empty) {
            return Err(err(
                ErrorCode::InvalidArgument,
                "InfluxDB fields must name at least one column when given",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (what, name) in self
            .tags
            .iter()
            .map(|t| ("tag key", t))
            .chain(fields.iter().map(|f| ("field key", f)))
        {
            check_name(what, name)?;
            if name == "time" {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("InfluxDB {what} `time` is reserved"),
                ));
            }
            if !seen.insert(name.as_str()) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("InfluxDB column `{name}` is mapped more than once"),
                ));
            }
        }
        for column in [self.measurement_column(), self.time_column.as_deref()]
            .into_iter()
            .flatten()
        {
            if seen.contains(column) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("InfluxDB column `{column}` cannot also be a tag or field"),
                ));
            }
        }
        if self.measurement_column().is_some()
            && self.measurement_column() == self.time_column.as_deref()
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "InfluxDB measurement column and time column must differ",
            ));
        }
        Ok(())
    }

    fn measurement_column(&self) -> Option<&str> {
        match &self.measurement {
            Measurement::Column(c) => Some(c),
            Measurement::Fixed(_) => None,
        }
    }

    /// Bind the mapping to a schema; refuses missing columns and types
    /// line protocol cannot carry.
    pub fn compile(&self, schema: &Schema) -> Result<CompiledMapping, SparrowError> {
        self.check()?;
        let column = |name: &str, what: &str| {
            schema.index_of_name(name).ok_or_else(|| {
                err(
                    ErrorCode::InvalidSchema,
                    format!("InfluxDB {what} column `{name}` is not in the sink schema"),
                )
            })
        };
        let utf8 = |idx: usize, what: &str| {
            let f = &schema.fields[idx];
            if f.data_type == DataType::Utf8 {
                Ok(idx)
            } else {
                Err(err(
                    ErrorCode::InvalidSchema,
                    format!(
                        "InfluxDB {what} column `{}` must be utf8, got {:?}",
                        f.name, f.data_type
                    ),
                ))
            }
        };
        let measurement = match &self.measurement {
            Measurement::Fixed(name) => {
                let mut escaped = Vec::new();
                escape(&mut escaped, name, b", ");
                CompiledMeasurement::Fixed(escaped)
            }
            Measurement::Column(c) => {
                CompiledMeasurement::Column(utf8(column(c, "measurement")?, "measurement")?)
            }
        };
        let time = match &self.time_column {
            None => None,
            Some(c) => {
                let idx = column(c, "time")?;
                if schema.fields[idx].data_type != DataType::TimestampMicrosUTC {
                    return Err(err(
                        ErrorCode::InvalidSchema,
                        format!("InfluxDB time column `{c}` must be a timestamp"),
                    ));
                }
                Some(idx)
            }
        };
        let mut tags = Vec::with_capacity(self.tags.len());
        for t in &self.tags {
            let idx = utf8(column(t, "tag")?, "tag")?;
            let mut key = Vec::new();
            escape(&mut key, t, b",= ");
            tags.push((t.as_bytes().to_vec(), key, idx));
        }
        // InfluxDB recommends tags sorted by key (byte order).
        tags.sort_by(|a, b| a.0.cmp(&b.0));
        let skip: Vec<usize> = tags
            .iter()
            .map(|t| t.2)
            .chain(time)
            .chain(match measurement {
                CompiledMeasurement::Column(i) => Some(i),
                CompiledMeasurement::Fixed(_) => None,
            })
            .collect();
        let names: Vec<&str> = match &self.fields {
            Some(f) => f.iter().map(String::as_str).collect(),
            None => schema
                .fields
                .iter()
                .enumerate()
                .filter(|(i, _)| !skip.contains(i))
                .map(|(_, f)| f.name.as_str())
                .collect(),
        };
        if names.len() > 256 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "InfluxDB mapping: at most 256 fields (including implicit fields)",
            ));
        }
        if names.is_empty() {
            return Err(err(
                ErrorCode::InvalidSchema,
                "InfluxDB points need at least one field column",
            ));
        }
        let mut fields = Vec::with_capacity(names.len());
        for name in names {
            if self.fields.is_none() {
                // Implicit fields follow the same name rules.
                check_name("field key", name)?;
                if name == "time" {
                    return Err(err(
                        ErrorCode::InvalidArgument,
                        "InfluxDB field key `time` is reserved; list fields explicitly",
                    ));
                }
            }
            let idx = column(name, "field")?;
            let kind = match schema.fields[idx].data_type {
                DataType::Bool => FieldKind::Bool,
                DataType::Int64 => FieldKind::Int,
                DataType::UInt64 => FieldKind::UInt,
                DataType::Float64 => FieldKind::Float,
                DataType::Utf8 => FieldKind::Str,
                ref other => {
                    return Err(err(
                        ErrorCode::InvalidSchema,
                        format!(
                            "InfluxDB field column `{name}` has type {other:?}; fields must be bool, int64, uint64, float64 or utf8"
                        ),
                    ))
                }
            };
            let mut key = Vec::new();
            escape(&mut key, name, b",= ");
            fields.push((key, idx, kind));
        }
        Ok(CompiledMapping {
            measurement,
            tags: tags.into_iter().map(|(_, k, i)| (k, i)).collect(),
            fields,
            time,
            precision: self.precision,
            columns: schema.fields.len(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldKind {
    Bool,
    Int,
    UInt,
    Float,
    Str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CompiledMeasurement {
    Fixed(Vec<u8>),
    Column(usize),
}

/// A mapping bound to one schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledMapping {
    measurement: CompiledMeasurement,
    tags: Vec<(Vec<u8>, usize)>,
    fields: Vec<(Vec<u8>, usize, FieldKind)>,
    time: Option<usize>,
    precision: Precision,
    columns: usize,
}

/// Why a row produced no line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineError {
    /// The row cannot be written as a point (reason for diagnostics).
    Bad(&'static str),
    /// The line would exceed the output limit.
    Oversize,
    /// The job reservation could not cover the output growth.
    Budget,
}

/// Output under a hard byte limit; every capacity growth is admitted
/// (charged) before it is allocated.
pub struct BoundedOut<'a, F> {
    pub bytes: &'a mut Vec<u8>,
    pub limit: usize,
    pub admit: F,
}

impl<F: FnMut(usize) -> bool> BoundedOut<'_, F> {
    pub fn put(&mut self, data: &[u8]) -> Result<(), LineError> {
        let next = self
            .bytes
            .len()
            .checked_add(data.len())
            .filter(|n| *n <= self.limit)
            .ok_or(LineError::Oversize)?;
        if next > self.bytes.capacity() {
            let capacity = next
                .checked_next_power_of_two()
                .unwrap_or(self.limit)
                .max(4096)
                .min(self.limit);
            if !(self.admit)(capacity) {
                return Err(LineError::Budget);
            }
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| LineError::Budget)?;
            if self.bytes.capacity() > capacity {
                return Err(LineError::Budget);
            }
        }
        self.bytes.extend_from_slice(data);
        Ok(())
    }
}

/// Append `s` with every byte in `specials` backslash-escaped.
fn escape(out: &mut Vec<u8>, s: &str, specials: &[u8]) {
    for &b in s.as_bytes() {
        if specials.contains(&b) {
            out.push(b'\\');
        }
        out.push(b);
    }
}

fn put_escaped<F: FnMut(usize) -> bool>(
    out: &mut BoundedOut<'_, F>,
    s: &str,
    specials: &[u8],
) -> Result<(), LineError> {
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if specials.contains(b) {
            out.put(&bytes[start..i])?;
            out.put(&[b'\\', *b])?;
            start = i + 1;
        }
    }
    out.put(&bytes[start..])
}

/// Format into a stack buffer (no heap allocation).
fn put_fmt<F: FnMut(usize) -> bool>(
    out: &mut BoundedOut<'_, F>,
    args: std::fmt::Arguments<'_>,
) -> Result<(), LineError> {
    use std::io::Write;
    let mut buf = [0u8; 64];
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    cursor
        .write_fmt(args)
        .map_err(|_| LineError::Bad("number_format"))?;
    let n = cursor.position() as usize;
    out.put(&buf[..n])
}

/// Runtime checks for a measurement or tag value taken from a row.
fn check_dynamic(s: &str) -> Result<(), LineError> {
    if s.contains(['\n', '\r']) {
        return Err(LineError::Bad("line_break"));
    }
    if s.ends_with('\\') {
        return Err(LineError::Bad("trailing_backslash"));
    }
    Ok(())
}

impl CompiledMapping {
    /// Append one `\n`-terminated point for `row`. On error the output may
    /// hold a partial line; the caller truncates it.
    pub fn encode<F: FnMut(usize) -> bool>(
        &self,
        row: &Row,
        out: &mut BoundedOut<'_, F>,
    ) -> Result<(), LineError> {
        if row.values.len() != self.columns {
            return Err(LineError::Bad("row_width"));
        }
        // Everything that can refuse the row is checked before writing.
        let timestamp = match self.time {
            None => None,
            Some(idx) => match &row.values[idx] {
                Scalar::TimestampMicrosUTC(us) => Some(
                    self.precision
                        .from_micros(*us)
                        .ok_or(LineError::Bad("time_out_of_range"))?,
                ),
                Scalar::Null => return Err(LineError::Bad("null_time")),
                _ => return Err(LineError::Bad("time_type")),
            },
        };
        match &self.measurement {
            CompiledMeasurement::Fixed(m) => out.put(m)?,
            CompiledMeasurement::Column(idx) => match &row.values[*idx] {
                Scalar::Utf8(m) => {
                    if m.is_empty()
                        || m.len() > 256
                        || m.starts_with('#')
                        || m.starts_with('_')
                        || backslash_before_special(m)
                    {
                        return Err(LineError::Bad("measurement_name"));
                    }
                    check_dynamic(m)?;
                    put_escaped(out, m, b", ")?;
                }
                Scalar::Null => return Err(LineError::Bad("null_measurement")),
                _ => return Err(LineError::Bad("measurement_type")),
            },
        }
        for (key, idx) in &self.tags {
            match &row.values[*idx] {
                Scalar::Null => {}
                Scalar::Utf8(v) if v.is_empty() => {}
                Scalar::Utf8(v) => {
                    check_dynamic(v)?;
                    out.put(b",")?;
                    out.put(key)?;
                    out.put(b"=")?;
                    put_escaped(out, v, b",= ")?;
                }
                _ => return Err(LineError::Bad("tag_type")),
            }
        }
        let mut separator: &[u8] = b" ";
        for (key, idx, kind) in &self.fields {
            let value = &row.values[*idx];
            if matches!(value, Scalar::Null) {
                continue;
            }
            // Validate before writing the key.
            match (kind, value) {
                (FieldKind::Float, Scalar::Float64(f)) if !f.is_finite() => {
                    return Err(LineError::Bad("non_finite_float"))
                }
                (FieldKind::Str, Scalar::Utf8(s)) => {
                    if s.len() > MAX_STRING_FIELD_BYTES {
                        return Err(LineError::Bad("string_field_too_long"));
                    }
                    if s.contains(['\n', '\r']) {
                        return Err(LineError::Bad("line_break"));
                    }
                }
                (FieldKind::Bool, Scalar::Bool(_))
                | (FieldKind::Int, Scalar::Int64(_))
                | (FieldKind::UInt, Scalar::UInt64(_))
                | (FieldKind::Float, Scalar::Float64(_)) => {}
                _ => return Err(LineError::Bad("field_type")),
            }
            out.put(separator)?;
            separator = b",";
            out.put(key)?;
            out.put(b"=")?;
            match value {
                Scalar::Bool(true) => out.put(b"true")?,
                Scalar::Bool(false) => out.put(b"false")?,
                Scalar::Int64(v) => put_fmt(out, format_args!("{v}i"))?,
                Scalar::UInt64(v) => put_fmt(out, format_args!("{v}u"))?,
                // Debug prints the shortest round-trip form, using an
                // exponent for very large/small magnitudes (accepted by
                // InfluxDB) instead of hundreds of digits.
                Scalar::Float64(v) => put_fmt(out, format_args!("{v:?}"))?,
                Scalar::Utf8(s) => {
                    out.put(b"\"")?;
                    put_escaped(out, s, b"\"\\")?;
                    out.put(b"\"")?;
                }
                _ => unreachable!("checked above"),
            }
        }
        if separator == b" " {
            return Err(LineError::Bad("no_fields"));
        }
        if let Some(ts) = timestamp {
            put_fmt(out, format_args!(" {ts}"))?;
        }
        out.put(b"\n")
    }
}

#[cfg(test)]
#[path = "line_tests.rs"]
mod tests;

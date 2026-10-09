//! Strict, bounded CSV <-> [`Row`] codec (RFC 4180 dialect).
//!
//! Field tokenizing and unescaping use the `csv` crate. That reader is lenient
//! by design: it accepts an unterminated quote, or a quote inside an unquoted
//! field, without reporting an error. A small structural pre-scan therefore
//! rejects those records, and it also records which fields were quoted. That
//! matters for NULL: an *unquoted* field equal to `null_value` is NULL, while a
//! quoted field never is (so `""` is an empty string when `null_value` is
//! empty).
//!
//! Records end at `\n` (an optional `\r` before it is dropped). A quoted field
//! may contain a line break only with `multiline: true`; the framer then tracks
//! quote parity across the break. Blank lines are skipped. A UTF-8 byte-order
//! mark is accepted only at the start of a document.

use serde::{Deserialize, Serialize};
use sparrow_model::{
    DataType, ErrorCode, Field, MemoryOwner, Result, Row, Scalar, Schema, SparrowError,
};

use crate::json::{decode_json_value, scalar_json_text, JsonLimits};

/// Upper bound for one record, matching the JSON record cap of every source.
pub const MAX_CSV_RECORD_BYTES: usize = 64 * 1024;
/// Upper bound for the number of fields in one record or header.
pub const MAX_CSV_FIELDS: usize = 1024;
const DEFAULT_MAX_FIELDS: usize = 256;
const MAX_NULL_VALUE: usize = 64;
const MAX_COLUMN_NAME: usize = 256;
const BOM: &[u8] = b"\xEF\xBB\xBF";

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

fn malformed(message: impl Into<String>) -> SparrowError {
    err(ErrorCode::CodecViolation, message)
}

/// Header/columns versus schema: what to do when a schema field has no column.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingColumns {
    /// Refuse the header (the source fails / the response is rejected).
    #[default]
    Error,
    /// Nullable fields without a column decode as NULL; required ones still fail.
    Null,
}

/// Header/columns versus schema: what to do with a column the schema lacks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtraColumns {
    /// Skip the column's values.
    #[default]
    Ignore,
    /// Refuse the header.
    Error,
}

fn comma() -> String {
    ",".into()
}
fn double_quote() -> String {
    "\"".into()
}
fn yes() -> bool {
    true
}
fn is_false(value: &bool) -> bool {
    !*value
}
fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// User-facing CSV options (`source.csv` / `sink.csv`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvOptions {
    /// One ASCII byte: tab or punctuation other than the quote character.
    #[serde(default = "comma")]
    pub delimiter: String,
    /// One ASCII punctuation byte, `"` by default. Doubled inside a quoted field.
    #[serde(default = "double_quote")]
    pub quote: String,
    /// Decode: the first record names the columns. Encode: write that record
    /// first (once per message, request body or file segment).
    #[serde(default = "yes")]
    pub header: bool,
    /// Decode without a header: column names in record order. Absent means the
    /// schema field order (positional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Unquoted text that means NULL. Empty by default; a quoted field is
    /// never NULL.
    #[serde(default)]
    pub null_value: String,
    /// Decode: strip spaces/tabs around *unquoted* fields.
    #[serde(default, skip_serializing_if = "is_false")]
    pub trim: bool,
    /// Decode: allow line breaks inside quoted fields.
    #[serde(default, skip_serializing_if = "is_false")]
    pub multiline: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub missing_columns: MissingColumns,
    #[serde(default, skip_serializing_if = "is_default")]
    pub extra_columns: ExtraColumns,
    /// Decode: bytes per record, 1..=65536 (default 65536).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_record_bytes: Option<usize>,
    /// Decode: fields per record or header, 1..=1024 (default 256).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_fields: Option<usize>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: comma(),
            quote: double_quote(),
            header: true,
            columns: None,
            null_value: String::new(),
            trim: false,
            multiline: false,
            missing_columns: MissingColumns::Error,
            extra_columns: ExtraColumns::Ignore,
            max_record_bytes: None,
            max_fields: None,
        }
    }
}

/// Which side of a connector the options configure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CsvRole {
    Decode,
    Encode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CsvLimits {
    pub max_record_bytes: usize,
    pub max_fields: usize,
    /// Depth bound for JSON text carried in Dynamic/nested columns.
    pub max_json_depth: usize,
}

impl Default for CsvLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: MAX_CSV_RECORD_BYTES,
            max_fields: DEFAULT_MAX_FIELDS,
            max_json_depth: JsonLimits::default().max_depth,
        }
    }
}

fn single_byte(name: &str, value: &str) -> Result<u8> {
    match value.as_bytes() {
        [b] if *b == b'\t' || b.is_ascii_punctuation() => Ok(*b),
        _ => Err(err(
            ErrorCode::InvalidArgument,
            format!("csv.{name} must be one ASCII punctuation character or tab, got {value:?}"),
        )),
    }
}

impl CsvOptions {
    /// Validate for one role and compile. Decode-only options on a sink are
    /// refused rather than ignored.
    pub fn compile(&self, role: CsvRole) -> Result<CsvFormat> {
        let delimiter = single_byte("delimiter", &self.delimiter)?;
        let quote = single_byte("quote", &self.quote)?;
        if quote == b'\t' || quote == delimiter {
            return Err(err(
                ErrorCode::InvalidArgument,
                "csv.quote must differ from the delimiter and must not be a tab",
            ));
        }
        if self.null_value.len() > MAX_NULL_VALUE
            || self
                .null_value
                .bytes()
                .any(|b| b == delimiter || b == quote || b.is_ascii_control() || b == b' ')
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "csv.null_value must be <=64 bytes without the delimiter, quote, spaces or control characters",
            ));
        }
        if role == CsvRole::Encode {
            let decode_only = [
                ("columns", self.columns.is_some()),
                ("trim", self.trim),
                ("multiline", self.multiline),
                (
                    "missing_columns",
                    self.missing_columns != MissingColumns::default(),
                ),
                (
                    "extra_columns",
                    self.extra_columns != ExtraColumns::default(),
                ),
                ("max_record_bytes", self.max_record_bytes.is_some()),
                ("max_fields", self.max_fields.is_some()),
            ];
            if let Some((name, _)) = decode_only.iter().find(|(_, set)| *set) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("csv.{name} is a decode option and is not accepted on a sink"),
                ));
            }
        }
        if self.header && self.columns.is_some() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "csv.columns names a headerless layout; it cannot be combined with header=true",
            ));
        }
        let mut limits = CsvLimits::default();
        if let Some(bytes) = self.max_record_bytes {
            if !(1..=MAX_CSV_RECORD_BYTES).contains(&bytes) {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    format!("csv.max_record_bytes must be 1..={MAX_CSV_RECORD_BYTES}"),
                ));
            }
            limits.max_record_bytes = bytes;
        }
        if let Some(fields) = self.max_fields {
            if !(1..=MAX_CSV_FIELDS).contains(&fields) {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    format!("csv.max_fields must be 1..={MAX_CSV_FIELDS}"),
                ));
            }
            limits.max_fields = fields;
        }
        if let Some(columns) = &self.columns {
            if columns.is_empty() || columns.len() > limits.max_fields {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    "csv.columns must name 1..=max_fields columns",
                ));
            }
            check_names(columns.iter().map(String::as_str))?;
        }
        Ok(CsvFormat {
            options: self.clone(),
            delimiter,
            quote,
            limits,
        })
    }
}

fn check_names<'a>(names: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for name in names {
        if name.is_empty() || name.len() > MAX_COLUMN_NAME {
            return Err(err(
                ErrorCode::InvalidSchema,
                format!("CSV column name must be 1..={MAX_COLUMN_NAME} bytes"),
            ));
        }
        if !seen.insert(name) {
            return Err(err(
                ErrorCode::InvalidSchema,
                format!("duplicate CSV column '{name}'"),
            ));
        }
    }
    Ok(())
}

/// Validated CSV dialect, limits and column policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvFormat {
    options: CsvOptions,
    delimiter: u8,
    quote: u8,
    limits: CsvLimits,
}

/// Schema field -> column index for one header (or headerless layout).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvMapping {
    width: usize,
    slots: Vec<Option<usize>>,
}

impl CsvMapping {
    /// Number of fields every record must have.
    pub fn width(&self) -> usize {
        self.width
    }
}

/// Classification used by connectors for the separate CSV counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CsvFault {
    /// Record, field count or nested value over a limit.
    Oversize,
    /// Structure: quoting, field count versus header, line breaks, encoding.
    Malformed,
    /// A field that does not parse as its schema type (or NULL where required).
    Type,
    /// Header/columns do not fit the schema.
    Header,
}

impl CsvFault {
    pub fn of(error: &SparrowError) -> Self {
        match error.code {
            ErrorCode::MaxRecordSize | ErrorCode::BoundExceeded => Self::Oversize,
            ErrorCode::CodecViolation => Self::Malformed,
            ErrorCode::InvalidSchema => Self::Header,
            _ => Self::Type,
        }
    }
}

/// Splits a buffer into records, tracking quote parity across line breaks
/// when `multiline` is on. Usable incrementally (File source) and on whole
/// documents (HTTP Poll, messages).
#[derive(Clone, Debug)]
pub struct CsvFramer {
    quote: u8,
    multiline: bool,
    in_quote: bool,
}

impl CsvFramer {
    /// Index of the `\n` that ends the current record within `chunk`,
    /// continuing the quote state carried from earlier chunks.
    pub fn find_terminator(&mut self, chunk: &[u8]) -> Option<usize> {
        if !self.multiline {
            return chunk.iter().position(|&b| b == b'\n');
        }
        for (i, &b) in chunk.iter().enumerate() {
            if b == self.quote {
                self.in_quote = !self.in_quote;
            } else if b == b'\n' && !self.in_quote {
                return Some(i);
            }
        }
        None
    }

    /// Forget the quote state (new record boundary, seek, or resync).
    pub fn reset(&mut self) {
        self.in_quote = false;
    }
}

/// Strip one LF or CRLF terminator. A bare CR is data (and the strict
/// scanner rejects it outside quotes), not another record terminator.
pub fn strip_terminator(mut record: &[u8]) -> &[u8] {
    if let [rest @ .., b'\n'] = record {
        record = rest;
        if let [rest @ .., b'\r'] = record {
            record = rest;
        }
    }
    record
}

/// `trim: true` strips spaces and tabs only (the characters the encoder
/// quotes at a field edge); other ASCII whitespace is data.
fn trim_blanks(mut text: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = text {
        text = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = text {
        text = rest;
    }
    text
}

fn strip_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(BOM).unwrap_or(bytes)
}

/// Iterator over the non-blank records of an in-memory document.
struct Records<'a> {
    bytes: &'a [u8],
    pos: usize,
    framer: CsvFramer,
}

impl<'a> Records<'a> {
    fn next_record(&mut self) -> Option<&'a [u8]> {
        while self.pos < self.bytes.len() {
            let start = self.pos;
            self.framer.reset();
            let end = self
                .framer
                .find_terminator(&self.bytes[start..])
                .map_or(self.bytes.len(), |i| start + i + 1);
            self.pos = end;
            let record = strip_terminator(&self.bytes[start..end]);
            if !record.is_empty() {
                return Some(record);
            }
        }
        None
    }
}

/// Lazily decoded document: optional header followed by records.
pub struct CsvDocument<'a> {
    format: &'a CsvFormat,
    schema: &'a Schema,
    mapping: CsvMapping,
    records: Records<'a>,
}

impl<'a> CsvDocument<'a> {
    /// Next data record. Errors are per record; the document continues.
    pub fn next_row(&mut self, owner: Option<&MemoryOwner>) -> Option<Result<Row>> {
        let record = self.records.next_record()?;
        Some(self.decode(record, owner))
    }

    /// Next non-blank data record as raw bytes (terminator stripped), so a
    /// caller can reject it by length and charge decode scratch first.
    pub fn next_record(&mut self) -> Option<&'a [u8]> {
        self.records.next_record()
    }

    /// Decode one record returned by [`Self::next_record`].
    pub fn decode(&self, record: &[u8], owner: Option<&MemoryOwner>) -> Result<Row> {
        self.format
            .decode_record(self.schema, &self.mapping, record, owner)
    }
}

impl CsvFormat {
    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    pub fn limits(&self) -> CsvLimits {
        self.limits
    }

    pub fn header(&self) -> bool {
        self.options.header
    }

    pub fn framer(&self) -> CsvFramer {
        CsvFramer {
            quote: self.quote,
            multiline: self.options.multiline,
            in_quote: false,
        }
    }

    /// Schema-dependent checks shared by both roles. A single nullable column
    /// with an empty `null_value` would turn NULL into a blank line, which a
    /// reader skips; that layout needs a visible `null_value`.
    pub fn check_schema(&self, schema: &Schema) -> Result<()> {
        if schema.fields.is_empty() || schema.fields.len() > MAX_CSV_FIELDS {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!("CSV needs 1..={MAX_CSV_FIELDS} columns"),
            ));
        }
        if schema.fields.len() == 1
            && schema.fields[0].nullable
            && self.options.null_value.is_empty()
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "a single nullable CSV column needs a non-empty csv.null_value (a NULL row would be a blank line)",
            ));
        }
        if self.options.header {
            // The header we emit must obey the same name contract as the
            // decoder's header_mapping, including the 256-byte limit.
            check_names(schema.fields.iter().map(|field| field.name.as_str()))?;
        } else {
            self.positional_mapping(schema)?;
        }
        Ok(())
    }

    fn mapping_from_names(&self, schema: &Schema, names: &[String]) -> Result<CsvMapping> {
        if names.len() > self.limits.max_fields {
            return Err(err(
                ErrorCode::InvalidSchema,
                format!(
                    "CSV header has {} columns, max_fields is {}",
                    names.len(),
                    self.limits.max_fields
                ),
            ));
        }
        check_names(names.iter().map(String::as_str))?;
        let mut slots = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            match names.iter().position(|n| *n == field.name) {
                Some(index) => slots.push(Some(index)),
                None if field.nullable && self.options.missing_columns == MissingColumns::Null => {
                    slots.push(None)
                }
                None => {
                    return Err(err(
                        ErrorCode::InvalidSchema,
                        format!("CSV header has no column for field '{}'", field.name),
                    ));
                }
            }
        }
        if self.options.extra_columns == ExtraColumns::Error {
            if let Some(extra) = names.iter().find(|n| schema.index_of_name(n).is_none()) {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    format!("CSV column '{extra}' is not in the schema (extra_columns=error)"),
                ));
            }
        }
        Ok(CsvMapping {
            width: names.len(),
            slots,
        })
    }

    /// Headerless layout: `columns`, or the schema field order.
    pub fn positional_mapping(&self, schema: &Schema) -> Result<CsvMapping> {
        match &self.options.columns {
            Some(columns) => self.mapping_from_names(schema, columns),
            None => {
                let names: Vec<String> = schema.fields.iter().map(|f| f.name.clone()).collect();
                self.mapping_from_names(schema, &names)
            }
        }
    }

    /// Mapping from one header record (terminator optional, BOM allowed).
    pub fn header_mapping(&self, schema: &Schema, record: &[u8]) -> Result<CsvMapping> {
        let record = strip_terminator(strip_bom(record));
        if record.len() > self.limits.max_record_bytes {
            return Err(err(
                ErrorCode::InvalidSchema,
                format!(
                    "CSV header exceeds max_record_bytes {}",
                    self.limits.max_record_bytes
                ),
            ));
        }
        let fields = self.split(record).map_err(|e| {
            err(
                ErrorCode::InvalidSchema,
                format!("CSV header: {}", e.message),
            )
        })?;
        let mut names = Vec::with_capacity(fields.len());
        for (raw, quoted) in &fields {
            let raw = if self.options.trim && !quoted {
                trim_blanks(raw)
            } else {
                raw
            };
            let name = std::str::from_utf8(raw)
                .map_err(|_| err(ErrorCode::InvalidSchema, "CSV header is not UTF-8"))?;
            names.push(name.to_owned());
        }
        self.mapping_from_names(schema, &names)
    }

    /// Mapping for a document: parses its header when `header` is set.
    fn document_mapping<'a>(
        &self,
        schema: &Schema,
        records: &mut Records<'a>,
    ) -> Result<CsvMapping> {
        if self.options.header {
            let header = records.next_record().ok_or_else(|| {
                err(
                    ErrorCode::InvalidSchema,
                    "CSV document has no header record",
                )
            })?;
            self.header_mapping(schema, header)
        } else {
            self.positional_mapping(schema)
        }
    }

    /// Whole document (HTTP Poll body): header errors fail here, record
    /// errors are reported per record by [`CsvDocument::next_row`]. A body
    /// with no records at all (empty or only blank lines) is an empty
    /// document even when `header` is set, like an empty NDJSON body.
    pub fn document<'a>(&'a self, schema: &'a Schema, bytes: &'a [u8]) -> Result<CsvDocument<'a>> {
        let bytes = strip_bom(bytes);
        let mut records = Records {
            bytes,
            pos: 0,
            framer: self.framer(),
        };
        let mapping = if bytes.iter().all(|b| matches!(b, b'\r' | b'\n')) {
            records.pos = bytes.len();
            CsvMapping {
                width: 0,
                slots: Vec::new(),
            }
        } else {
            self.document_mapping(schema, &mut records)?
        };
        Ok(CsvDocument {
            format: self,
            schema,
            mapping,
            records,
        })
    }

    /// One message (MQTT/NATS/JetStream/HTTP push): an optional header record
    /// followed by exactly one data record.
    pub fn decode_message(
        &self,
        schema: &Schema,
        bytes: &[u8],
        owner: Option<&MemoryOwner>,
    ) -> Result<Row> {
        let limit = self.limits.max_record_bytes.saturating_mul(2);
        if bytes.len() > limit {
            return Err(err(
                ErrorCode::MaxRecordSize,
                format!("CSV message {}B exceeds {limit}B", bytes.len()),
            ));
        }
        let mut records = Records {
            bytes: strip_bom(bytes),
            pos: 0,
            framer: self.framer(),
        };
        let mapping = self.document_mapping(schema, &mut records)?;
        let record = records
            .next_record()
            .ok_or_else(|| malformed("CSV message carries no data record"))?;
        if records.next_record().is_some() {
            return Err(malformed("CSV message must carry exactly one data record"));
        }
        self.decode_record(schema, &mapping, record, owner)
    }

    /// Decode one data record (terminator optional).
    pub fn decode_record(
        &self,
        schema: &Schema,
        mapping: &CsvMapping,
        record: &[u8],
        owner: Option<&MemoryOwner>,
    ) -> Result<Row> {
        let record = strip_terminator(record);
        if record.len() > self.limits.max_record_bytes {
            return Err(err(
                ErrorCode::MaxRecordSize,
                format!(
                    "CSV record {}B exceeds max_record_bytes {}",
                    record.len(),
                    self.limits.max_record_bytes
                ),
            ));
        }
        if record.starts_with(BOM) {
            return Err(malformed("byte-order mark inside a CSV document"));
        }
        let fields = self.split(record)?;
        if fields.len() != mapping.width {
            return Err(malformed(format!(
                "CSV record has {} fields, expected {}",
                fields.len(),
                mapping.width
            )));
        }
        let mut values = Vec::with_capacity(schema.fields.len());
        for (field, slot) in schema.fields.iter().zip(&mapping.slots) {
            values.push(match slot {
                None => Scalar::Null,
                Some(index) => {
                    let (raw, quoted) = &fields[*index];
                    self.decode_field(field, raw, *quoted, owner)?
                }
            });
        }
        Ok(Row { values })
    }

    /// Structural pre-scan, then unescape with the `csv` crate. Returns each
    /// field with whether it was quoted.
    fn split<'r>(&self, record: &'r [u8]) -> Result<Vec<(std::borrow::Cow<'r, [u8]>, bool)>> {
        let quoted = self.scan(record)?;
        if quoted.iter().all(|q| !q) {
            // Nothing to unescape: borrow the raw slices.
            return Ok(record
                .split(|&b| b == self.delimiter)
                .map(|f| (std::borrow::Cow::Borrowed(f), false))
                .collect());
        }
        let mut reader = ::csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .double_quote(true)
            .escape(None)
            .comment(None)
            .trim(::csv::Trim::None)
            .buffer_capacity(record.len().clamp(64, 8 * 1024))
            .from_reader(record);
        let mut parsed = ::csv::ByteRecord::new();
        match reader.read_byte_record(&mut parsed) {
            Ok(true) => {}
            Ok(false) => return Err(malformed("empty CSV record")),
            Err(e) => return Err(malformed(format!("CSV record: {e}"))),
        }
        if parsed.len() != quoted.len() {
            return Err(malformed("CSV record structure is ambiguous"));
        }
        Ok(parsed
            .iter()
            .zip(quoted)
            .map(|(f, q)| (std::borrow::Cow::Owned(f.to_vec()), q))
            .collect())
    }

    /// RFC 4180 structure check. The quote may only open a field and, once
    /// closed, must be followed by the delimiter or the end of the record.
    fn scan(&self, record: &[u8]) -> Result<Vec<bool>> {
        #[derive(PartialEq)]
        enum State {
            Start,
            Unquoted,
            Quoted,
            AfterQuote,
        }
        let mut state = State::Start;
        let mut fields = Vec::new();
        let push = |fields: &mut Vec<bool>, quoted: bool| -> Result<()> {
            fields.push(quoted);
            if fields.len() > self.limits.max_fields {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    format!("CSV record exceeds max_fields {}", self.limits.max_fields),
                ));
            }
            Ok(())
        };
        let mut quoted = false;
        for &b in record {
            match state {
                State::Start | State::Unquoted if b == self.delimiter => {
                    push(&mut fields, quoted)?;
                    quoted = false;
                    state = State::Start;
                }
                State::Start if b == self.quote => {
                    quoted = true;
                    state = State::Quoted;
                }
                State::Start | State::Unquoted | State::AfterQuote if b == b'\r' || b == b'\n' => {
                    return Err(malformed("line break outside a quoted field"));
                }
                State::Start | State::Unquoted if b == self.quote => {
                    return Err(malformed("quote inside an unquoted CSV field"));
                }
                State::Start | State::Unquoted => state = State::Unquoted,
                State::Quoted if b == self.quote => state = State::AfterQuote,
                State::Quoted if (b == b'\n' || b == b'\r') && !self.options.multiline => {
                    return Err(malformed(
                        "quoted CSV field contains a line break (set csv.multiline=true to allow)",
                    ));
                }
                State::Quoted => {}
                State::AfterQuote if b == self.quote => state = State::Quoted,
                State::AfterQuote if b == self.delimiter => {
                    push(&mut fields, quoted)?;
                    quoted = false;
                    state = State::Start;
                }
                State::AfterQuote => {
                    return Err(malformed("unexpected character after a closing quote"));
                }
            }
        }
        if state == State::Quoted {
            return Err(malformed("unterminated quoted CSV field"));
        }
        push(&mut fields, quoted)?;
        Ok(fields)
    }

    fn decode_field(
        &self,
        field: &Field,
        raw: &[u8],
        quoted: bool,
        owner: Option<&MemoryOwner>,
    ) -> Result<Scalar> {
        let text = if self.options.trim && !quoted {
            trim_blanks(raw)
        } else {
            raw
        };
        let type_err = |what: &str| {
            err(
                ErrorCode::TypeMismatch,
                format!("CSV column '{}': {what}", field.name),
            )
        };
        if !quoted && text == self.options.null_value.as_bytes() {
            return if field.nullable {
                Ok(Scalar::Null)
            } else {
                Err(type_err("NULL in a non-nullable column"))
            };
        }
        let utf8 = || std::str::from_utf8(text).map_err(|_| type_err("not valid UTF-8"));
        match &field.data_type {
            DataType::Bool => {
                if text.eq_ignore_ascii_case(b"true") {
                    Ok(Scalar::Bool(true))
                } else if text.eq_ignore_ascii_case(b"false") {
                    Ok(Scalar::Bool(false))
                } else {
                    Err(type_err("expected true or false"))
                }
            }
            DataType::Int64 => utf8()?
                .parse::<i64>()
                .map(Scalar::Int64)
                .map_err(|_| type_err("expected int64")),
            DataType::TimestampMicrosUTC => utf8()?
                .parse::<i64>()
                .map(Scalar::TimestampMicrosUTC)
                .map_err(|_| type_err("expected int64 microseconds")),
            DataType::UInt64 => utf8()?
                .parse::<u64>()
                .map(Scalar::UInt64)
                .map_err(|_| type_err("expected uint64")),
            DataType::Float64 => match utf8()?.parse::<f64>() {
                Ok(v) if v.is_finite() => Ok(Scalar::Float64(v)),
                _ => Err(type_err("expected a finite float64")),
            },
            DataType::Utf8 => {
                let s = utf8()?;
                match owner {
                    Some(o) => Scalar::utf8_tracked(o, s),
                    None => Ok(Scalar::utf8(s)),
                }
            }
            DataType::Bytes => {
                let bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, text)
                        .map_err(|_| type_err("expected base64"))?;
                match owner {
                    Some(o) => Scalar::bytes_tracked(o, bytes),
                    None => Ok(Scalar::bytes(bytes)),
                }
            }
            ty => {
                let limits = JsonLimits {
                    max_bytes: self.limits.max_record_bytes,
                    max_depth: self.limits.max_json_depth,
                };
                decode_json_value(text, ty, field.nullable, &limits, owner).map_err(|e| {
                    match e.code {
                        ErrorCode::MaxRecordSize | ErrorCode::BoundExceeded => e,
                        _ => type_err(&format!("expected JSON text ({})", e.message)),
                    }
                })
            }
        }
    }

    fn needs_quote(&self, text: &[u8]) -> bool {
        text.is_empty()
            || text == self.options.null_value.as_bytes()
            || text.starts_with(BOM)
            || matches!(text.first(), Some(b' ' | b'\t'))
            || matches!(text.last(), Some(b' ' | b'\t'))
            || text
                .iter()
                .any(|&b| b == self.delimiter || b == self.quote || b == b'\r' || b == b'\n')
    }

    fn put_text(&self, out: &mut impl Out, text: &[u8]) -> Result<()> {
        if !self.needs_quote(text) {
            return out.put(text);
        }
        out.put(&[self.quote])?;
        for (i, part) in text.split(|&b| b == self.quote).enumerate() {
            if i > 0 {
                out.put(&[self.quote, self.quote])?;
            }
            out.put(part)?;
        }
        out.put(&[self.quote])
    }

    fn put_value(&self, out: &mut impl Out, value: &Scalar) -> Result<()> {
        match value {
            Scalar::Null => out.put(self.options.null_value.as_bytes()),
            Scalar::Bool(v) => self.put_text(out, if *v { b"true" } else { b"false" }),
            Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => {
                self.put_text(out, v.to_string().as_bytes())
            }
            Scalar::UInt64(v) => self.put_text(out, v.to_string().as_bytes()),
            // Non-finite floats have no CSV/JSON number form: NULL, as in JSON.
            Scalar::Float64(v) if !v.is_finite() => out.put(self.options.null_value.as_bytes()),
            Scalar::Float64(v) => self.put_text(out, v.to_string().as_bytes()),
            Scalar::Utf8(v) => self.put_text(out, v.as_bytes()),
            Scalar::Bytes(v) => self.put_text(
                out,
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v.as_ref())
                    .as_bytes(),
            ),
            Scalar::Dynamic(_) => self.put_text(out, scalar_json_text(value)?.as_bytes()),
        }
    }

    fn put_header(&self, schema: &Schema, out: &mut impl Out) -> Result<()> {
        for (i, field) in schema.fields.iter().enumerate() {
            if i > 0 {
                out.put(&[self.delimiter])?;
            }
            self.put_text(out, field.name.as_bytes())?;
        }
        out.put(b"\n")
    }

    fn put_record(&self, schema: &Schema, row: &Row, out: &mut impl Out) -> Result<()> {
        if row.values.len() != schema.fields.len() {
            return Err(err(ErrorCode::InvalidSchema, "row/schema arity mismatch"));
        }
        for (i, value) in row.values.iter().enumerate() {
            if i > 0 {
                out.put(&[self.delimiter])?;
            }
            self.put_value(out, value)?;
        }
        out.put(b"\n")
    }

    /// The header line (`\n`-terminated), regardless of the `header` option.
    pub fn encode_header(&self, schema: &Schema) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.put_header(schema, &mut out)?;
        Ok(out)
    }

    /// Byte length of [`Self::encode_header`], computed without allocating.
    pub fn header_len(&self, schema: &Schema) -> Result<usize> {
        let mut out = Count(0);
        self.put_header(schema, &mut out)?;
        Ok(out.0)
    }

    /// One record line (`\n`-terminated), never a header.
    pub fn encode_record(&self, schema: &Schema, row: &Row) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.put_record(schema, row, &mut out)?;
        Ok(out)
    }

    /// One message: the header line when `header` is set, then the record.
    pub fn encode_message(&self, schema: &Schema, row: &Row) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        if self.options.header {
            self.put_header(schema, &mut out)?;
        }
        self.put_record(schema, row, &mut out)?;
        Ok(out)
    }

    /// One record line under a hard byte bound (File Sink rows), never a
    /// header. `admit` bills each capacity growth before allocation.
    pub fn encode_record_bounded(
        &self,
        schema: &Schema,
        row: &Row,
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        let mut out = Bounded {
            bytes: Vec::new(),
            limit,
            admit,
        };
        self.put_record(schema, row, &mut out)?;
        Ok(out.bytes)
    }

    /// A document (HTTP request body): header (when set) then every row.
    /// `admit` bills each capacity growth before allocation, like the JSON
    /// bounded encoder; `limit` is a hard byte bound.
    pub fn encode_rows_bounded_with_capacity(
        &self,
        schema: &Schema,
        rows: &[Row],
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        let mut out = Bounded {
            bytes: Vec::new(),
            limit,
            admit,
        };
        if self.options.header {
            self.put_header(schema, &mut out)?;
        }
        for row in rows {
            self.put_record(schema, row, &mut out)?;
        }
        Ok(out.bytes)
    }

    /// [`Self::encode_message`] under a hard byte bound: `admit` bills each
    /// capacity growth before allocation.
    pub fn encode_message_bounded_with_capacity(
        &self,
        schema: &Schema,
        row: &Row,
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        self.encode_rows_bounded_with_capacity(schema, std::slice::from_ref(row), limit, admit)
    }

    /// Canonical bytes of every option that changes which rows a document
    /// decodes to (dialect, header/columns, NULL text, trim, multiline,
    /// column policies and effective limits). Durable sources bind this into
    /// their checkpoint identity so a restore under other options is refused.
    pub fn identity_bytes(&self) -> Vec<u8> {
        fn put(out: &mut Vec<u8>, bytes: &[u8]) {
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        let o = &self.options;
        let mut out = b"sparrow-csv-v1".to_vec();
        out.extend_from_slice(&[
            self.delimiter,
            self.quote,
            u8::from(o.header),
            u8::from(o.trim),
            u8::from(o.multiline),
            o.missing_columns as u8,
            o.extra_columns as u8,
        ]);
        put(&mut out, o.null_value.as_bytes());
        match &o.columns {
            None => out.push(0),
            Some(columns) => {
                out.push(1);
                out.extend_from_slice(&(columns.len() as u64).to_le_bytes());
                for column in columns {
                    put(&mut out, column.as_bytes());
                }
            }
        }
        out.extend_from_slice(&(self.limits.max_record_bytes as u64).to_le_bytes());
        out.extend_from_slice(&(self.limits.max_fields as u64).to_le_bytes());
        out.extend_from_slice(&(self.limits.max_json_depth as u64).to_le_bytes());
        out
    }

    /// Conservative decode working set for one CSV message or record of
    /// `len` wire bytes, to be charged before decoding. Callers reject
    /// `len` over the record limit first, without allocating.
    ///
    /// Per wire byte: the unescaped field copies, the `csv` crate record
    /// buffer, header names and Utf8/Bytes values are each at most `len`
    /// (4×). A Dynamic or nested column parses its cell as JSON, so such a
    /// schema uses the JSON factor (64×). Per field: the structural scan,
    /// the split vector and the record bounds (doubled for `Vec` growth),
    /// bounded by `max_fields`. Per schema field: the mapping, the Row and
    /// the positional names. Constants: the reader buffer (≤8 KiB) and slack.
    pub fn decode_scratch(&self, schema: &Schema, len: usize) -> usize {
        let per_byte = if schema.fields.iter().any(|f| !is_flat(&f.data_type)) {
            64
        } else {
            4
        };
        let fields = len
            .saturating_add(1)
            .min(self.limits.max_fields.saturating_add(1))
            .max(schema.fields.len());
        let per_field = std::mem::size_of::<(std::borrow::Cow<'static, [u8]>, bool)>()
            + std::mem::size_of::<bool>()
            + 2 * std::mem::size_of::<usize>();
        let names = schema.fields.iter().fold(0usize, |n, f| {
            n.saturating_add(f.name.len())
                .saturating_add(std::mem::size_of::<String>())
        });
        let per_schema_field =
            2 * std::mem::size_of::<Scalar>() + std::mem::size_of::<Option<usize>>();
        len.saturating_mul(per_byte)
            .saturating_add(fields.saturating_mul(per_field).saturating_mul(2))
            .saturating_add(schema.fields.len().saturating_mul(per_schema_field))
            .saturating_add(names)
            .saturating_add(8 * 1024 + 4096)
    }

    /// Conservative encoder scratch outside the bounded output buffer: number
    /// formatting, base64 text, and the JSON tree of Dynamic cells.
    pub fn encode_scratch(&self, row: &Row) -> usize {
        let factor = if row.values.iter().any(|v| matches!(v, Scalar::Dynamic(_))) {
            8
        } else {
            2
        };
        row.resident_bytes()
            .saturating_mul(factor)
            .saturating_add(row.values.len().saturating_mul(64))
            .saturating_add(4096)
    }
}

/// Column types decoded straight from the cell text (no JSON parse).
fn is_flat(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Bool
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::TimestampMicrosUTC
            | DataType::Utf8
            | DataType::Bytes
    )
}

trait Out {
    fn put(&mut self, bytes: &[u8]) -> Result<()>;
}

impl Out for Vec<u8> {
    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        self.extend_from_slice(bytes);
        Ok(())
    }
}

/// Length-only sink (no allocation).
struct Count(usize);

impl Out for Count {
    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(())
    }
}

struct Bounded<F> {
    bytes: Vec<u8>,
    limit: usize,
    admit: F,
}

impl<F: FnMut(usize) -> Result<()>> Out for Bounded<F> {
    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| err(ErrorCode::BoundExceeded, "CSV byte limit exceeded"))?;
        if next > self.bytes.capacity() {
            let capacity = next
                .checked_next_power_of_two()
                .unwrap_or(self.limit)
                .max(256)
                .min(self.limit);
            (self.admit)(capacity)?;
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|e| {
                    err(
                        ErrorCode::ResourceExhausted,
                        format!("CSV buffer allocation: {e}"),
                    )
                })?;
            if self.bytes.capacity() > capacity {
                return Err(err(
                    ErrorCode::ResourceExhausted,
                    "CSV allocation exceeded admitted capacity",
                ));
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

/// Wire format of a message/record payload for connectors with a format
/// choice. JSON stays the default everywhere.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PayloadFormat {
    #[default]
    Json,
    Csv(std::sync::Arc<CsvFormat>),
    Protobuf(std::sync::Arc<crate::protobuf::ProtobufFormat>),
}

impl PayloadFormat {
    pub fn csv(format: CsvFormat) -> Self {
        Self::Csv(std::sync::Arc::new(format))
    }

    pub fn protobuf(format: crate::protobuf::ProtobufFormat) -> Self {
        Self::Protobuf(std::sync::Arc::new(format))
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv(_) => "csv",
            Self::Protobuf(_) => "protobuf",
        }
    }

    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }

    pub fn as_csv(&self) -> Option<&CsvFormat> {
        match self {
            Self::Csv(format) => Some(format),
            _ => None,
        }
    }

    pub fn as_protobuf(&self) -> Option<&crate::protobuf::ProtobufFormat> {
        match self {
            Self::Protobuf(format) => Some(format),
            _ => None,
        }
    }

    /// Check `schema` against the format before a job starts (CSV: column
    /// layout and NULL spelling; protobuf: the field-path mapping and type
    /// matrix; JSON maps by name at decode time).
    pub fn check_schema(&self, schema: &Schema) -> Result<()> {
        match self {
            Self::Json => Ok(()),
            Self::Csv(format) => format.check_schema(schema),
            Self::Protobuf(format) => format.check_schema(schema),
        }
    }

    /// HTTP `content-type` for an encoded body. A protobuf body is a
    /// length-delimited stream of messages (see `docs/FORMATS.md`).
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Csv(_) => "text/csv; charset=utf-8",
            Self::Protobuf(_) => "application/x-protobuf",
        }
    }

    /// One message payload -> one row.
    pub fn decode_row(
        &self,
        schema: &Schema,
        bytes: &[u8],
        json: &JsonLimits,
        owner: Option<&MemoryOwner>,
    ) -> Result<Row> {
        match self {
            Self::Json => crate::json::decode_json_row_on(schema, bytes, json, owner),
            Self::Csv(format) => format.decode_message(schema, bytes, owner),
            Self::Protobuf(format) => format.decode_message(schema, bytes, owner),
        }
    }

    /// One row -> one message payload (CSV: header line first when set).
    pub fn encode_row(&self, schema: &Schema, row: &Row) -> Result<Vec<u8>> {
        match self {
            Self::Json => crate::json::encode_json_row(schema, row),
            Self::Csv(format) => format.encode_message(schema, row),
            Self::Protobuf(format) => format.encode_message(schema, row),
        }
    }

    /// [`Self::encode_row`] under a hard byte bound. `admit` bills each output
    /// capacity growth before allocation (JSON: the one-row array envelope is
    /// removed in place, so `limit` bounds the object itself).
    pub fn encode_row_bounded_with_capacity(
        &self,
        schema: &Schema,
        row: &Row,
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        match self {
            Self::Json => {
                let mut body = crate::json::encode_json_batch_bounded_with_capacity(
                    schema,
                    std::slice::from_ref(row),
                    limit.saturating_add(2),
                    admit,
                )?;
                body.remove(0);
                body.pop();
                Ok(body)
            }
            Self::Csv(format) => {
                format.encode_message_bounded_with_capacity(schema, row, limit, admit)
            }
            Self::Protobuf(format) => {
                format.encode_message_bounded_with_capacity(schema, row, limit, admit)
            }
        }
    }

    /// Format identity for durable checkpoints: `None` for JSON (so existing
    /// JSON checkpoints keep their identity), the canonical CSV options or
    /// protobuf descriptor + message + mapping + policy otherwise.
    pub fn identity_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::Json => None,
            Self::Csv(format) => Some(format.identity_bytes()),
            Self::Protobuf(format) => Some(format.identity_bytes()),
        }
    }

    /// Largest message payload this format may decode under `json`: the
    /// connector rejects anything longer by length alone, before charging
    /// scratch or allocating. CSV also honours its own record limit (a
    /// message may carry a header record plus one data record).
    pub fn max_message_bytes(&self, json: &JsonLimits) -> usize {
        match self {
            Self::Json => json.max_bytes,
            Self::Csv(format) => json
                .max_bytes
                .min(format.limits.max_record_bytes.saturating_mul(2)),
            Self::Protobuf(format) => json.max_bytes.min(format.limits().max_message_bytes),
        }
    }

    /// Decode working set to charge before decoding one `len`-byte message.
    /// JSON keeps the parse-tree estimate of the NATS/HTTP Poll sources;
    /// CSV uses [`CsvFormat::decode_scratch`].
    pub fn decode_scratch(&self, schema: &Schema, len: usize) -> usize {
        match self {
            Self::Json => len
                .saturating_mul(64)
                .saturating_add(
                    schema
                        .fields
                        .len()
                        .saturating_mul(std::mem::size_of::<Scalar>())
                        .saturating_mul(2),
                )
                .saturating_add(4096),
            Self::Csv(format) => format.decode_scratch(schema, len),
            Self::Protobuf(format) => format.decode_scratch(schema, len),
        }
    }

    /// Encoder scratch to charge before encoding `row`, in addition to the
    /// output buffer billed through `admit`.
    pub fn encode_scratch(&self, schema: &Schema, row: &Row) -> usize {
        match self {
            Self::Json => row
                .resident_bytes()
                .saturating_mul(8)
                .saturating_add(
                    schema
                        .fields
                        .iter()
                        .fold(0usize, |n, f| n.saturating_add(f.name.capacity()))
                        .saturating_mul(4),
                )
                .saturating_add(8192),
            Self::Csv(format) => format.encode_scratch(row),
            Self::Protobuf(format) => format.encode_scratch(row),
        }
    }

    /// Rows -> one document body (HTTP Sink): a JSON array, CSV records
    /// (header first when set) or a length-delimited protobuf stream.
    pub fn encode_document_bounded_with_capacity(
        &self,
        schema: &Schema,
        rows: &[Row],
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        match self {
            Self::Json => {
                crate::json::encode_json_batch_bounded_with_capacity(schema, rows, limit, admit)
            }
            Self::Csv(format) => format.encode_rows_bounded_with_capacity(schema, rows, limit, admit),
            Self::Protobuf(format) => {
                format.encode_rows_bounded_with_capacity(schema, rows, limit, admit)
            }
        }
    }
}

#[cfg(test)]
#[path = "csv_tests.rs"]
mod tests;

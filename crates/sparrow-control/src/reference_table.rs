//! Immutable, bounded reference-table catalog records.
//!
//! The runtime already has an in-memory [`ReferenceTable`] primitive.  This
//! module deliberately only defines the control-plane wire/storage contract:
//! a finite schema, an ordered key, and rows made of deterministic scalar JSON
//! values.  Revisions are assigned and persisted by [`crate::store::Store`].

use std::collections::HashSet;
use std::io::{self, Write};

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sparrow_model::{DataType, ErrorCode, Result, Row, Scalar, Schema, SchemaId, SparrowError};
use sparrow_plan::catalog::schema_from_fields;
use sparrow_plan::graph::FieldSpec;

/// Hard per-revision payload bound.  The HTTP layer has the same body bound;
/// keeping it here also protects embedding callers of `Store`.
pub const MAX_REFERENCE_TABLE_BYTES: usize = 64 * 1024;
/// Hard row bound for one immutable revision.
pub const MAX_REFERENCE_TABLE_ROWS: usize = 1024;
/// Hard number of retained revisions for one table name.
pub const MAX_REFERENCE_TABLE_VERSIONS: usize = 128;
/// Bound the number of names as well as rows/bytes.  Empty tables therefore
/// cannot be used to consume unbounded catalog metadata.
pub const MAX_REFERENCE_TABLE_NAMES: usize = 1024;
/// Bound the number of historical dependency rows returned by one preview.
/// The underlying pin set is never truncated for GC decisions.
pub const MAX_REFERENCE_TABLE_PREVIEW_PINS: usize = 256;
/// Hard bytes bound for all revisions of one table name.
pub const MAX_REFERENCE_TABLE_BYTES_PER_NAME: u64 = 8 * 1024 * 1024;
/// Hard bytes bound for the complete control-plane reference-table catalog.
pub const MAX_REFERENCE_TABLE_CATALOG_BYTES: u64 = 32 * 1024 * 1024;
/// Conservative SQLite/index metadata charge used by the catalog byte caps.
pub const REFERENCE_TABLE_METADATA_BYTES: u64 = 256;
const MAX_REFERENCE_TABLE_FIELDS: usize = 256;
const MAX_REFERENCE_TABLE_KEYS: usize = 64;

/// A finite immutable table revision.  `rows` is ordered for digest
/// reproducibility, while lookup semantics are keyed by `keys`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceTableSpec {
    pub fields: Vec<FieldSpec>,
    pub keys: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// Stored/returned table revision.  `sha256` covers the table name, revision,
/// schema, key order and row contents; it is not a mutable "latest" alias.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceTableRow {
    pub name: String,
    pub revision: u64,
    pub sha256: String,
    pub table: ReferenceTableSpec,
    pub payload_bytes: u64,
    pub row_count: u64,
    pub created_at_ms: i64,
}

/// Metadata-only listing shape.  A list endpoint must not materialize every
/// row of every table just to show available revisions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceTableMetadata {
    pub name: String,
    pub revision: u64,
    pub sha256: String,
    pub payload_bytes: u64,
    pub row_count: u64,
    pub created_at_ms: i64,
}

impl From<&ReferenceTableRow> for ReferenceTableMetadata {
    fn from(row: &ReferenceTableRow) -> Self {
        Self {
            name: row.name.clone(),
            revision: row.revision,
            sha256: row.sha256.clone(),
            payload_bytes: row.payload_bytes,
            row_count: row.row_count,
            created_at_ms: row.created_at_ms,
        }
    }
}

impl ReferenceTableSpec {
    /// Parse and validate one stored/API table payload under the same bound
    /// used by publication.  Storage callers must not deserialize unbounded
    /// JSON before this check.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_REFERENCE_TABLE_BYTES {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                format!(
                    "reference table payload {}B exceeds {MAX_REFERENCE_TABLE_BYTES}B",
                    bytes.len()
                ),
            ));
        }
        let spec: Self = serde_json::from_slice(bytes).map_err(|error| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("reference table JSON: {error}"),
            )
        })?;
        spec.validate()?;
        let encoded = serde_json::to_vec(&spec).map_err(|error| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("encode reference table: {error}"),
            )
        })?;
        if encoded != bytes {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "stored reference table is not canonical JSON",
            ));
        }
        Ok(spec)
    }

    /// Build the model schema used by the planner/runtime.  The name only
    /// supplies a deterministic SchemaId; it is not part of field semantics.
    pub fn schema(&self, name: &str) -> Result<Schema> {
        schema_from_fields(&self.fields, SchemaId::new(schema_id(name)))
    }

    /// Decode rows into detached model scalars after applying the exact same
    /// validation used for publication.  Runtime snapshot assembly can then
    /// pass these rows to `ReferenceTable::snapshot` without accepting a
    /// malformed row through an indexing panic.
    pub fn rows(&self) -> Result<Vec<Row>> {
        self.validate()?;
        let schema = self.schema("reference_table")?;
        self.rows
            .iter()
            .map(|row| {
                let values = row
                    .iter()
                    .zip(&schema.fields)
                    .map(|(value, field)| json_to_scalar(value, &field.data_type))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Row { values })
            })
            .collect()
    }

    /// Validate schema, scalar rows, non-null key columns and key uniqueness.
    /// This is intentionally stricter than `Schema::new`: table keys must be
    /// stable lookup scalars and cannot rely on a nullable/float encoding.
    pub fn validate(&self) -> Result<()> {
        // Prove the compact JSON representation is within the publication
        // budget before constructing any payload Vec.  This is deliberately
        // the first check: rows() and the digest path both go through the
        // same bounded serializer, while encoded_bytes allocates only after
        // this proof succeeds.
        bounded_json_len(self)?;
        if self.fields.is_empty() || self.fields.len() > MAX_REFERENCE_TABLE_FIELDS {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table fields must be 1..={MAX_REFERENCE_TABLE_FIELDS}"
                ),
            ));
        }
        if self.keys.is_empty() || self.keys.len() > MAX_REFERENCE_TABLE_KEYS {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("reference table keys must be 1..={MAX_REFERENCE_TABLE_KEYS}"),
            ));
        }
        if self.rows.len() > MAX_REFERENCE_TABLE_ROWS {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table exceeds max_rows {MAX_REFERENCE_TABLE_ROWS} (got {})",
                    self.rows.len()
                ),
            ));
        }

        let schema = self.schema("reference_table")?;
        let mut key_indexes = Vec::with_capacity(self.keys.len());
        let mut key_names = HashSet::with_capacity(self.keys.len());
        for key in &self.keys {
            if !key_names.insert(key.as_str()) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("duplicate reference table key '{key}'"),
                ));
            }
            let index = schema.index_of_name(key).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("reference table key '{key}' is not a field"),
                )
            })?;
            let field = &schema.fields[index];
            if field.nullable {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("reference table key '{key}' must be non-nullable"),
                ));
            }
            if !is_supported_key_type(&field.data_type) {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    format!(
                        "reference table key '{key}' type {} is not a deterministic scalar key",
                        field.data_type
                    ),
                ));
            }
            key_indexes.push(index);
        }

        let mut seen = HashSet::<Vec<u8>>::with_capacity(self.rows.len());
        for (row_index, row) in self.rows.iter().enumerate() {
            if row.len() != schema.fields.len() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!(
                        "reference table row {row_index} has {} values, schema has {} fields",
                        row.len(),
                        schema.fields.len()
                    ),
                ));
            }
            for (column, (value, field)) in row.iter().zip(&schema.fields).enumerate() {
                validate_scalar(value, &field.data_type, field.nullable).map_err(|error| {
                    error
                        .context("row", row_index.to_string())
                        .context("column", column.to_string())
                })?;
            }
            let mut encoded_key = Vec::new();
            for &index in &key_indexes {
                // The key types are restricted above, so JSON's scalar form is
                // unambiguous after the per-field schema check.  The model
                // encoder also canonicalizes signed zero if this contract is
                // extended to numeric key types later.
                let scalar = json_to_scalar(&row[index], &schema.fields[index].data_type)?;
                scalar.encode_key(&mut encoded_key);
                encoded_key.push(0xff);
            }
            if !seen.insert(encoded_key) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("duplicate reference table key in row {row_index}"),
                ));
            }
        }
        Ok(())
    }

    /// Canonical, compact JSON bytes for storage-size enforcement.  Array
    /// order is semantic; `serde_json::Map` is deterministically ordered in
    /// this build and nested/object values are not admitted by `validate`.
    pub fn encoded_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|error| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("encode reference table: {error}"),
            )
        })?;
        if bytes.len() > MAX_REFERENCE_TABLE_BYTES {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                format!(
                    "reference table payload {}B exceeds {MAX_REFERENCE_TABLE_BYTES}B",
                    bytes.len()
                ),
            ));
        }
        Ok(bytes)
    }
}

/// Hash the full immutable identity, not merely row bytes.  The explicit
/// envelope prevents a table name/revision from being detached from its
/// payload when a pipeline stores its binding.
pub fn reference_table_sha256(
    name: &str,
    revision: u64,
    table: &ReferenceTableSpec,
) -> Result<String> {
    #[derive(Serialize)]
    struct Canonical<'a> {
        name: &'a str,
        revision: u64,
        fields: &'a [FieldSpec],
        keys: &'a [String],
        rows: &'a [Vec<Value>],
    }
    // validate() performs the bounded, non-allocating size proof.  The
    // envelope below is then the only allocation needed for this digest.
    table.validate()?;
    let canonical = Canonical {
        name,
        revision,
        fields: &table.fields,
        keys: &table.keys,
        rows: &table.rows,
    };
    let bytes = serde_json::to_vec(&canonical).map_err(|error| {
        SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("encode reference table digest: {error}"),
        )
    })?;
    let digest = digest(&SHA256, &bytes);
    Ok(digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn schema_id(name: &str) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for byte in name.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash.max(1)
}

fn is_supported_key_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Bool
            | DataType::Int64
            | DataType::UInt64
            | DataType::Utf8
            | DataType::TimestampMicrosUTC
    )
}

fn validate_scalar(value: &Value, ty: &DataType, nullable: bool) -> Result<()> {
    if value.is_null() {
        if nullable {
            return Ok(());
        }
        return Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "non-nullable reference table field contains null",
        ));
    }
    let valid = match ty {
        DataType::Bool => value.is_boolean(),
        DataType::Int64 | DataType::TimestampMicrosUTC => value
            .as_i64()
            .is_some(),
        DataType::UInt64 => value.as_u64().is_some(),
        DataType::Float64 => value.as_f64().is_some_and(|value| value.is_finite()),
        DataType::Utf8 => value.is_string(),
        // Bytes need an explicit base64/hex wire contract.  Dynamic and
        // nested values are intentionally deferred to a later table profile.
        DataType::Bytes | DataType::Dynamic | DataType::Array(_) | DataType::Struct(_) | DataType::Map { .. } | DataType::Null => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            format!("reference table value does not match {ty}"),
        ))
    }
}

fn json_to_scalar(value: &Value, ty: &DataType) -> Result<Scalar> {
    if value.is_null() {
        return Ok(Scalar::Null);
    }
    match ty {
        DataType::Bool => value.as_bool().map(Scalar::Bool),
        DataType::Int64 => value.as_i64().map(Scalar::Int64),
        DataType::UInt64 => value.as_u64().map(Scalar::UInt64),
        DataType::Float64 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Scalar::Float64),
        DataType::Utf8 => value.as_str().map(|value| Scalar::utf8(value)),
        DataType::TimestampMicrosUTC => value.as_i64().map(Scalar::TimestampMicrosUTC),
        _ => None,
    }
    .ok_or_else(|| {
        SparrowError::new(
            ErrorCode::TypeMismatch,
            format!("reference table value does not match {ty}"),
        )
    })
}

/// Count serialized bytes without retaining the payload.  serde_json writes
/// incrementally; returning an I/O error at the first over-budget chunk keeps
/// an attacker-controlled table from forcing a large temporary allocation.
fn bounded_json_len<T: Serialize>(value: &T) -> Result<usize> {
    let mut writer = BoundedJsonWriter::new(MAX_REFERENCE_TABLE_BYTES);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.len),
        Err(_error) if writer.exceeded => Err(SparrowError::new(
            ErrorCode::MaxRecordSize,
            format!(
                "reference table payload exceeds {MAX_REFERENCE_TABLE_BYTES}B"
            ),
        )),
        Err(error) => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("encode reference table: {error}"),
        )),
    }
}

struct BoundedJsonWriter {
    len: usize,
    limit: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            len: 0,
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.len);
        if bytes.len() > remaining {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "reference table JSON exceeds bounded writer",
            ));
        }
        self.len += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

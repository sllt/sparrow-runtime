//! Static and versioned [`ReferenceTable`] enrichment.
//!
//! V0.2: a finite snapshot frozen at job submit (running Job keeps the Arc).
//! V0.3: [`VersionedReferenceTable`] — as-of-event-time lookup; new versions
//! can be published with `valid_from` without replacing the job handle.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryLease, MemoryOwner, Result, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId, SparrowError,
};
use sparrow_plan::LookupSpec;

use crate::window::{finish_rows_metered, resolve_keys};

const REFERENCE_TABLE_HASH_NODE_BYTES: usize = 64;
const REFERENCE_TABLE_METADATA_BYTES: usize = 256;

fn invalid_table(message: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidSchema, message)
}

fn unsupported_key(message: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::FeatureUnavailable, message)
}

fn deterministic_key_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Bool
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Bytes
            | DataType::TimestampMicrosUTC
    )
}

fn deterministic_key_scalar(value: &Scalar) -> bool {
    match value {
        Scalar::Bool(_)
        | Scalar::Int64(_)
        | Scalar::UInt64(_)
        | Scalar::Utf8(_)
        | Scalar::Bytes(_)
        | Scalar::TimestampMicrosUTC(_) => true,
        Scalar::Float64(value) => value.is_finite(),
        Scalar::Null | Scalar::Dynamic(_) => false,
    }
}

fn validate_row(schema: &Schema, row: &Row, row_number: usize) -> Result<()> {
    if row.values.len() != schema.fields.len() {
        return Err(invalid_table(format!(
            "reference row {row_number} has {} values, schema has {} fields",
            row.values.len(),
            schema.fields.len()
        )));
    }
    for (value, field) in row.values.iter().zip(&schema.fields) {
        if value.is_null() {
            if !field.nullable {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!(
                        "reference field '{}' is non-nullable but row {row_number} has Null",
                        field.name
                    ),
                ));
            }
        } else if !value.matches_type(&field.data_type) {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!(
                    "reference field '{}' expected {}, got {} in row {row_number}",
                    field.name,
                    field.data_type,
                    value.data_type()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_key_schema(schema: &Schema, key_fields: &[String], key_idx: &[usize]) -> Result<()> {
    if key_fields.is_empty() || key_idx.len() != key_fields.len() {
        return Err(invalid_table(
            "reference table requires at least one key field",
        ));
    }
    let mut seen = HashSet::with_capacity(key_idx.len());
    for (name, &index) in key_fields.iter().zip(key_idx) {
        if !seen.insert(index) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("reference table has duplicate key field '{name}'"),
            ));
        }
        let field = schema
            .fields
            .get(index)
            .ok_or_else(|| invalid_table("reference table key index is outside schema"))?;
        if field.nullable {
            return Err(invalid_table(format!(
                "reference table key field '{}' must be non-nullable",
                field.name
            )));
        }
        if !deterministic_key_type(&field.data_type) {
            return Err(unsupported_key(format!(
                "reference table key field '{}' has unsupported type {}",
                field.name, field.data_type
            )));
        }
    }
    Ok(())
}

fn validate_key_values(
    schema: &Schema,
    key_idx: &[usize],
    row: &Row,
    row_number: usize,
    allow_null: bool,
) -> Result<()> {
    for &index in key_idx {
        let field = &schema.fields[index];
        let value = &row.values[index];
        if value.is_null() {
            if allow_null {
                // A nullable stream key is a normal lookup miss. The table
                // contract still rejects nullable/null keys at snapshot time.
                continue;
            }
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!(
                    "reference key field '{}' is null in row {row_number}",
                    field.name
                ),
            ));
        }
        if !deterministic_key_scalar(value) {
            return Err(unsupported_key(format!(
                "reference key field '{}' is not a deterministic scalar",
                field.name
            )));
        }
        if !value.matches_type(&field.data_type) {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!(
                    "reference key field '{}' expected {}, got {} in row {row_number}",
                    field.name,
                    field.data_type,
                    value.data_type()
                ),
            ));
        }
    }
    Ok(())
}

fn encode_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u64).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn encode_data_type(out: &mut Vec<u8>, ty: &DataType) {
    match ty {
        DataType::Null => out.push(0),
        DataType::Bool => out.push(1),
        DataType::Int64 => out.push(2),
        DataType::UInt64 => out.push(3),
        DataType::Float64 => out.push(4),
        DataType::Utf8 => out.push(5),
        DataType::Bytes => out.push(6),
        DataType::TimestampMicrosUTC => out.push(7),
        DataType::Dynamic => out.push(8),
        DataType::Array(inner) => {
            out.push(9);
            encode_data_type(out, inner);
        }
        DataType::Struct(fields) => {
            out.push(10);
            out.extend_from_slice(&(fields.len() as u64).to_le_bytes());
            for field in fields {
                out.extend_from_slice(&field.id.raw().to_le_bytes());
                encode_string(out, &field.name);
                encode_data_type(out, &field.data_type);
                out.push(u8::from(field.nullable));
            }
        }
        DataType::Map { key, value } => {
            out.push(11);
            encode_data_type(out, key);
            encode_data_type(out, value);
        }
    }
}

fn encode_schema(out: &mut Vec<u8>, schema: &Schema) {
    out.extend_from_slice(&schema.id.raw().to_le_bytes());
    out.extend_from_slice(&(schema.fields.len() as u64).to_le_bytes());
    for field in &schema.fields {
        out.extend_from_slice(&field.id.raw().to_le_bytes());
        encode_string(out, &field.name);
        encode_data_type(out, &field.data_type);
        out.push(u8::from(field.nullable));
    }
}

fn scalar_key_len(value: &Scalar) -> usize {
    match value {
        Scalar::Null => 1,
        Scalar::Bool(_) => 2,
        Scalar::Int64(_)
        | Scalar::UInt64(_)
        | Scalar::Float64(_)
        | Scalar::TimestampMicrosUTC(_) => 1 + 8,
        Scalar::Utf8(value) => 1usize.saturating_add(4).saturating_add(value.len()),
        Scalar::Bytes(value) => 1usize.saturating_add(4).saturating_add(value.len()),
        // Dynamic keys are rejected before this estimator. Keep a conservative
        // fallback so a malformed caller cannot make the estimate wrap.
        Scalar::Dynamic(value) => Scalar::Dynamic(value.clone())
            .resident_bytes()
            .saturating_mul(2)
            .saturating_add(64),
    }
}

fn data_type_resident_bytes(ty: &DataType) -> usize {
    std::mem::size_of::<DataType>().saturating_add(match ty {
        DataType::Array(inner) => data_type_resident_bytes(inner),
        DataType::Struct(fields) => fields
            .capacity()
            .saturating_mul(std::mem::size_of::<Field>())
            .saturating_add(fields.iter().map(field_resident_bytes).sum::<usize>()),
        DataType::Map { key, value } => {
            data_type_resident_bytes(key).saturating_add(data_type_resident_bytes(value))
        }
        _ => 0,
    })
}

fn field_resident_bytes(field: &Field) -> usize {
    std::mem::size_of::<Field>()
        .saturating_add(field.name.capacity())
        .saturating_add(data_type_resident_bytes(&field.data_type))
}

fn schema_resident_bytes(schema: &Schema) -> usize {
    std::mem::size_of::<Schema>()
        .saturating_add(
            schema
                .fields
                .capacity()
                .saturating_mul(std::mem::size_of::<Field>()),
        )
        .saturating_add(
            schema
                .fields
                .iter()
                .map(field_resident_bytes)
                .sum::<usize>(),
        )
}

fn snapshot_resident_bytes(
    name: &String,
    schema: &Schema,
    key_fields: &Vec<String>,
    rows: &Vec<Row>,
    key_idx: &[usize],
) -> usize {
    let metadata = REFERENCE_TABLE_METADATA_BYTES
        .saturating_add(std::mem::size_of::<ReferenceTable>())
        .saturating_add(name.capacity())
        .saturating_add(
            key_fields
                .capacity()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(key_fields.iter().map(String::capacity).sum::<usize>())
        .saturating_add(schema_resident_bytes(schema))
        // The source Vec and its rows coexist with the detached index while
        // construction is in progress.
        .saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<Row>()));
    let mut total = metadata;
    let mut checksum_scratch = REFERENCE_TABLE_METADATA_BYTES
        .saturating_add(name.capacity())
        .saturating_add(schema_resident_bytes(schema))
        .saturating_add(
            key_fields
                .iter()
                .map(|field| 8usize.saturating_add(field.capacity()))
                .sum::<usize>(),
        );
    for row in rows {
        let row_bytes = row.resident_bytes();
        let key_bytes = key_idx.iter().fold(0usize, |n, &index| {
            n.saturating_add(row.values[index].resident_bytes())
                .saturating_add(scalar_key_len(&row.values[index]))
                .saturating_add(1)
        });
        let encoded_index_key = key_idx.iter().fold(0usize, |n, &index| {
            n.saturating_add(scalar_key_len(&row.values[index]))
                .saturating_add(1)
        });
        checksum_scratch = checksum_scratch.saturating_add(
            8usize.saturating_add(
                row.values
                    .iter()
                    .map(|value| scalar_key_len(value).saturating_add(1))
                    .sum::<usize>(),
            ),
        );
        // `table_crc` appends a length-prefixed encoded index key and holds a
        // sorted reference Vec for the whole table. Include both and leave
        // room for Vec growth while the CRC buffer expands.
        checksum_scratch = checksum_scratch
            .saturating_add(encoded_index_key)
            .saturating_add(4)
            .saturating_add(std::mem::size_of::<&Vec<u8>>());
        // Count both the source row and its detached copy, plus the temporary
        // detached key and encoded key retained until insertion completes.
        total = total
            .saturating_add(row_bytes.saturating_mul(2))
            .saturating_add(key_bytes);
    }
    total
        .saturating_add(rows.len().saturating_mul(
            std::mem::size_of::<(Vec<u8>, Row)>().saturating_add(REFERENCE_TABLE_HASH_NODE_BYTES),
        ))
        .saturating_add(checksum_scratch.saturating_mul(2))
}

/// Finite, detached snapshot. Not an async lookup.
///
/// `snapshot` keeps the historical bounded constructor for embedded callers:
/// its caller owns the external lifetime/budget. Job-attached tables should
/// use [`Self::snapshot_owned`], which holds a retention lease for the full
/// table lifetime.
#[derive(Debug)]
pub struct ReferenceTable {
    pub name: String,
    pub version: u64,
    pub schema: Schema,
    pub key_fields: Vec<String>,
    index: HashMap<Vec<u8>, Row>,
    /// CRC-32 of the frozen snapshot, including the complete schema. Fail
    /// closed on mismatch (P2-36).
    checksum: u32,
    /// Present only for the job-owned constructor. The historical embedding
    /// constructor intentionally leaves this absent and documents that the
    /// caller owns its external budget/lifetime.
    owned_lease: Option<MemoryLease>,
    /// SHA-256 of the control-plane canonical JSON envelope. Control has
    /// already verified this digest; runtime's typed CRC is a separate
    /// integrity contract and must not be compared as the same hash.
    /// Historical constructors leave this absent and cannot be used as a
    /// verified aligned reference-table dependency.
    canonical_sha256: Option<[u8; 32]>,
    /// Cached after construction so batch processing never scans the complete
    /// table to calculate a scratch bound.
    max_row_resident: usize,
}

impl ReferenceTable {
    /// Historical bounded embedding constructor. The caller must externally
    /// own the table's memory/lifetime; no job credit is acquired here.
    pub fn snapshot(
        name: impl Into<String>,
        version: u64,
        schema: Schema,
        key_fields: Vec<String>,
        rows: Vec<Row>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Arc<Self>> {
        Self::snapshot_inner(
            name, version, schema, key_fields, rows, max_rows, max_bytes, None, None,
        )
    }

    /// Build a finite table under the supplied job owner. A retention lease is
    /// acquired before any detached key/row copy and retained until the table
    /// is dropped. The estimate deliberately includes temporary key copies,
    /// encoded index keys, hash nodes, vectors and schema metadata.
    pub fn snapshot_owned(
        name: impl Into<String>,
        version: u64,
        schema: Schema,
        key_fields: Vec<String>,
        rows: Vec<Row>,
        max_rows: usize,
        max_bytes: usize,
        owner: &Arc<MemoryOwner>,
    ) -> Result<Arc<Self>> {
        Self::snapshot_inner(
            name,
            version,
            schema,
            key_fields,
            rows,
            max_rows,
            max_bytes,
            None,
            Some(owner),
        )
    }

    /// Build a job-owned snapshot from a control-plane verified canonical
    /// table digest. The digest is supplied, not recomputed: control hashes
    /// canonical JSON while this crate's CRC protects the detached typed
    /// representation. The caller must obtain it from an exact-revision
    /// `Store::reference_bindings` read.
    pub fn snapshot_owned_verified(
        name: impl Into<String>,
        version: u64,
        schema: Schema,
        key_fields: Vec<String>,
        rows: Vec<Row>,
        max_rows: usize,
        max_bytes: usize,
        canonical_sha256: [u8; 32],
        owner: &Arc<MemoryOwner>,
    ) -> Result<Arc<Self>> {
        if canonical_sha256 == [0; 32] {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "reference table canonical SHA-256 is empty",
            ));
        }
        Self::snapshot_inner(
            name,
            version,
            schema,
            key_fields,
            rows,
            max_rows,
            max_bytes,
            Some(canonical_sha256),
            Some(owner),
        )
    }

    fn snapshot_inner(
        name: impl Into<String>,
        version: u64,
        schema: Schema,
        key_fields: Vec<String>,
        rows: Vec<Row>,
        max_rows: usize,
        max_bytes: usize,
        canonical_sha256: Option<[u8; 32]>,
        owner: Option<&Arc<MemoryOwner>>,
    ) -> Result<Arc<Self>> {
        if rows.len() > max_rows {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table exceeds max_rows {max_rows} (got {})",
                    rows.len()
                ),
            ));
        }
        schema.validate()?;
        let key_idx = resolve_keys(&schema, &key_fields)?;
        validate_key_schema(&schema, &key_fields, &key_idx)?;
        for (row_number, row) in rows.iter().enumerate() {
            validate_row(&schema, row, row_number)?;
            validate_key_values(&schema, &key_idx, row, row_number, false)?;
        }
        let name = name.into();
        let estimated = snapshot_resident_bytes(&name, &schema, &key_fields, &rows, &key_idx);
        if estimated > max_bytes {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table exceeds max_bytes {max_bytes} (estimated resident bytes {estimated})"
                ),
            ));
        }
        let owned_lease = owner
            .map(|owner| owner.acquire(CreditKind::Retention, estimated.max(1)))
            .transpose()?;
        let mut index = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: Vec<Scalar> = key_idx
                .iter()
                .map(|&i| row.values[i].detach_copy())
                .collect();
            let detached = Row {
                values: row.values.iter().map(Scalar::detach_copy).collect(),
            };
            let encoded = encode_scalars(&key);
            if index.contains_key(&encoded) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "reference table contains duplicate key",
                ));
            }
            index.insert(encoded, detached);
        }
        let max_row_resident = index.values().map(Row::resident_bytes).max().unwrap_or(0);
        let checksum = table_crc(&name, version, &schema, &key_fields, &index);
        Ok(Arc::new(Self {
            name,
            version,
            schema,
            key_fields,
            index,
            checksum,
            owned_lease,
            canonical_sha256,
            max_row_resident,
        }))
    }

    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// B2 aligned recovery requires the retention lease to belong to the
    /// admitting Job owner. The historical unowned constructor deliberately
    /// returns false here so it remains usable for restart-fresh embedding but
    /// cannot bypass the aligned Job quota.
    pub fn is_owned_by(&self, owner: &Arc<MemoryOwner>) -> bool {
        self.owned_lease
            .as_ref()
            .is_some_and(|lease| Arc::ptr_eq(lease.owner(), owner))
    }

    /// Return the dependency identity only for snapshots created by
    /// `snapshot_owned_verified`. Calling `verify` first prevents a corrupted
    /// table from becoming valid merely by attaching an old digest.
    pub fn verified_dependency(&self) -> Result<sparrow_plan::ReferenceTableDependency> {
        self.verify()?;
        self.schema.validate()?;
        let key_idx = resolve_keys(&self.schema, &self.key_fields)?;
        validate_key_schema(&self.schema, &self.key_fields, &key_idx)?;
        for (row_number, row) in self.index.values().enumerate() {
            validate_row(&self.schema, row, row_number)?;
            validate_key_values(&self.schema, &key_idx, row, row_number, false)?;
        }
        let canonical_sha256 = self.canonical_sha256.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "reference table was not created from a verified canonical digest",
            )
        })?;
        if canonical_sha256 == [0; 32] || self.name.is_empty() || self.version == 0 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "reference table has an invalid verified dependency identity",
            ));
        }
        Ok(sparrow_plan::ReferenceTableDependency {
            name: self.name.clone(),
            revision: self.version,
            canonical_sha256,
            runtime_crc32: self.checksum,
        })
    }

    /// Recompute CRC-32 from live rows and compare to the stored value.
    pub fn verify(&self) -> Result<()> {
        let got = table_crc(
            &self.name,
            self.version,
            &self.schema,
            &self.key_fields,
            &self.index,
        );
        if got != self.checksum {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                format!(
                    "reference table '{}' crc32 mismatch (stored {:#010x} computed {:#010x})",
                    self.name, self.checksum, got
                ),
            ));
        }
        Ok(())
    }

    /// Test helper: keep live rows, replace the stored checksum.
    #[cfg(test)]
    pub fn with_stored_checksum(&self, checksum: u32) -> Self {
        let mut t = self.test_clone();
        t.checksum = checksum;
        t
    }

    /// Test helper: flip the first cell of one row, keep the stored checksum.
    #[cfg(test)]
    pub fn with_corrupted_first_row(&self) -> Self {
        let mut t = self.test_clone();
        if let Some(row) = t.index.values_mut().next() {
            if let Some(first) = row.values.first_mut() {
                *first = match first {
                    Scalar::Int64(v) => Scalar::Int64(v.wrapping_add(1)),
                    Scalar::UInt64(v) => Scalar::UInt64(v.wrapping_add(1)),
                    Scalar::Float64(v) => Scalar::Float64(*v + 1.0),
                    Scalar::Utf8(_) => Scalar::utf8("__crc_corrupt__"),
                    _ => Scalar::utf8("__crc_corrupt__"),
                };
            }
        }
        t
    }

    #[cfg(test)]
    fn test_clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            version: self.version,
            schema: self.schema.clone(),
            key_fields: self.key_fields.clone(),
            index: self
                .index
                .iter()
                .map(|(key, row)| (key.clone(), row.detach_copy()))
                .collect(),
            checksum: self.checksum,
            owned_lease: self.owned_lease.as_ref().map(MemoryLease::share),
            canonical_sha256: self.canonical_sha256,
            max_row_resident: self.max_row_resident,
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn get(&self, key: &[Scalar]) -> Option<&Row> {
        if key.len() != self.key_fields.len() {
            return None;
        }
        self.index.get(&encode_scalars(key))
    }
}

fn table_crc(
    name: &str,
    version: u64,
    schema: &Schema,
    key_fields: &[String],
    index: &HashMap<Vec<u8>, Row>,
) -> u32 {
    let mut buf = Vec::new();
    let nb = name.as_bytes();
    buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
    buf.extend_from_slice(nb);
    buf.extend_from_slice(&version.to_le_bytes());
    encode_schema(&mut buf, schema);
    buf.extend_from_slice(&(key_fields.len() as u32).to_le_bytes());
    for k in key_fields {
        let b = k.as_bytes();
        buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
        buf.extend_from_slice(b);
    }
    let mut keys: Vec<&Vec<u8>> = index.keys().collect();
    keys.sort();
    for k in keys {
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        if let Some(row) = index.get(k) {
            for s in &row.values {
                s.encode_key(&mut buf);
                buf.push(0xff);
            }
        }
    }
    crate::checkpoint::crc32(&buf)
}

fn encode_scalars(key: &[Scalar]) -> Vec<u8> {
    let mut out = Vec::new();
    for s in key {
        s.encode_key(&mut out);
        out.push(0xff);
    }
    out
}

/// One temporal version of a reference table: valid on `[valid_from, valid_to)`.
#[derive(Clone, Debug)]
pub struct TableVersion {
    pub version: u64,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
    pub table: Arc<ReferenceTable>,
}

/// Versioned table handle. Publishing a new version is visible to a running job.
#[derive(Clone, Debug)]
pub struct VersionedReferenceTable {
    name: String,
    max_versions: usize,
    inner: Arc<RwLock<Vec<TableVersion>>>,
}

impl VersionedReferenceTable {
    pub fn new(name: impl Into<String>, max_versions: usize) -> Result<Self> {
        if max_versions == 0 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "versioned table max_versions must be > 0",
            ));
        }
        Ok(Self {
            name: name.into(),
            max_versions,
            inner: Arc::new(RwLock::new(Vec::new())),
        })
    }

    pub fn from_static(table: Arc<ReferenceTable>, max_versions: usize) -> Result<Self> {
        let v = Self::new(table.name.clone(), max_versions)?;
        v.publish(0, i64::MIN / 4, table)?;
        Ok(v)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version_count(&self) -> usize {
        self.inner.read().expect("versioned table").len()
    }

    /// Publish a new slice. Previous open version is closed at `valid_from`.
    pub fn publish(&self, version: u64, valid_from: i64, table: Arc<ReferenceTable>) -> Result<()> {
        if table.name != self.name {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "versioned table '{}' != snapshot '{}'",
                    self.name, table.name
                ),
            ));
        }
        table.verify()?;
        let mut g = self.inner.write().expect("versioned table");
        if let Some(last) = g.last() {
            if version <= last.version {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "versioned table version must strictly increase",
                ));
            }
            if valid_from <= last.valid_from {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "versioned table valid_from must strictly increase",
                ));
            }
            if table.schema != last.table.schema || table.key_fields != last.table.key_fields {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    "versioned table revisions must keep the same schema and key fields",
                ));
            }
        }
        if g.len() >= self.max_versions {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "versioned table '{}' exceeds max_versions {}",
                    self.name, self.max_versions
                ),
            ));
        }
        if let Some(last) = g.last_mut() {
            if last.valid_to.is_none() {
                last.valid_to = Some(valid_from);
            }
        }
        g.push(TableVersion {
            version,
            valid_from,
            valid_to: None,
            table,
        });
        Ok(())
    }

    pub fn lookup_as_of(&self, key: &[Scalar], as_of: i64) -> Option<Row> {
        self.table_as_of(as_of).and_then(|t| t.get(key).cloned())
    }

    pub fn table_as_of(&self, as_of: i64) -> Option<Arc<ReferenceTable>> {
        let g = self.inner.read().expect("versioned table");
        g.iter()
            .rev()
            .find(|v| as_of >= v.valid_from && v.valid_to.map(|to| as_of < to).unwrap_or(true))
            .map(|v| Arc::clone(&v.table))
    }

    /// Caller must supply a safe watermark for every reader. Do not evict
    /// history automatically at publish: late events may still require it.
    pub fn gc_before(&self, safe_watermark: i64) -> usize {
        let mut g = self.inner.write().expect("versioned table");
        let before = g.len();
        g.retain(|v| v.valid_to.map_or(true, |end| end > safe_watermark));
        before - g.len()
    }

    pub fn latest(&self) -> Option<Arc<ReferenceTable>> {
        self.inner
            .read()
            .expect("versioned table")
            .last()
            .map(|v| Arc::clone(&v.table))
    }
}

enum LookupSource {
    Static(Arc<ReferenceTable>),
    Versioned(VersionedReferenceTable),
}

pub struct LookupOperator {
    /// Kept so OperatorId / table identity stay reconstructible for future recovery.
    #[allow(dead_code)]
    spec: LookupSpec,
    source: LookupSource,
    stream_idx: Vec<usize>,
    keep_idx: Vec<usize>,
    as_of_idx: Option<usize>,
    input: Schema,
    output: Schema,
    owner: Arc<MemoryOwner>,
}

impl LookupOperator {
    pub fn new(
        spec: LookupSpec,
        table: Arc<ReferenceTable>,
        input: Schema,
        owner: Arc<MemoryOwner>,
    ) -> Result<Self> {
        if spec.temporal {
            let ver = VersionedReferenceTable::from_static(table, 16)?;
            return Self::new_versioned(spec, ver, input, owner);
        }
        Self::from_source(spec, LookupSource::Static(table), input, owner)
    }

    pub fn new_versioned(
        spec: LookupSpec,
        table: VersionedReferenceTable,
        input: Schema,
        owner: Arc<MemoryOwner>,
    ) -> Result<Self> {
        Self::from_source(spec, LookupSource::Versioned(table), input, owner)
    }

    fn from_source(
        spec: LookupSpec,
        source: LookupSource,
        input: Schema,
        owner: Arc<MemoryOwner>,
    ) -> Result<Self> {
        spec.validate()?;
        input.validate()?;
        let (name, schema, key_fields) = match &source {
            LookupSource::Static(t) => (t.name.clone(), t.schema.clone(), t.key_fields.clone()),
            LookupSource::Versioned(t) => {
                let latest = t.latest().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("versioned table '{}' has no versions", t.name()),
                    )
                })?;
                (
                    t.name().to_string(),
                    latest.schema.clone(),
                    latest.key_fields.clone(),
                )
            }
        };
        if spec.table != name {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("lookup table '{}' != snapshot '{}'", spec.table, name),
            ));
        }
        match &source {
            LookupSource::Static(t) => t.verify()?,
            LookupSource::Versioned(t) => {
                let versions = t.inner.read().expect("versioned table");
                for version in versions.iter() {
                    version.table.verify()?;
                }
            }
        }
        let stream_idx = resolve_keys(&input, &spec.stream_keys)?;
        if spec.table_keys != key_fields {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "lookup table_keys must match the snapshot key_fields in order",
            ));
        }
        let table_idx = resolve_keys(&schema, &spec.table_keys)?;
        validate_key_schema(&schema, &spec.table_keys, &table_idx)?;
        for (&stream, &table) in stream_idx.iter().zip(&table_idx) {
            let stream_type = &input.fields[stream].data_type;
            let table_type = &schema.fields[table].data_type;
            if stream_type != table_type {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!(
                        "lookup key type mismatch: stream '{}' is {}, table '{}' is {}",
                        input.fields[stream].name,
                        stream_type,
                        schema.fields[table].name,
                        table_type
                    ),
                ));
            }
        }
        let keep = if spec.keep.is_empty() {
            schema
                .fields
                .iter()
                .filter(|f| !key_fields.iter().any(|k| k == &f.name))
                .map(|f| f.name.clone())
                .collect()
        } else {
            spec.keep.clone()
        };
        let keep_idx = resolve_keys(&schema, &keep)?;
        let as_of_idx = if spec.temporal {
            let field = spec.as_of_field.as_deref().unwrap_or("");
            let index = input.index_of_name(field).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown as-of event-time field '{field}'"),
                )
            })?;
            if !matches!(
                &input.fields[index].data_type,
                DataType::Int64 | DataType::UInt64 | DataType::TimestampMicrosUTC
            ) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!("versioned lookup as-of field '{field}' must be event-time"),
                ));
            }
            Some(index)
        } else {
            None
        };
        let output = lookup_output_schema(&input, &schema, &keep)?;
        Ok(Self {
            spec,
            source,
            stream_idx,
            keep_idx,
            as_of_idx,
            input,
            output,
            owner,
        })
    }

    pub fn table_version(&self) -> u64 {
        match &self.source {
            LookupSource::Static(t) => t.version,
            LookupSource::Versioned(t) => t.latest().map(|x| x.version).unwrap_or(0),
        }
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }

    fn validate_input_batch(&self, batch: &RowBatch) -> Result<()> {
        if batch.schema() != &self.input {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "lookup input batch schema does not match the bound input schema",
            ));
        }
        if !Arc::ptr_eq(batch.lease().owner(), &self.owner) {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "lookup input batch belongs to a different memory owner",
            ));
        }
        for (row_number, row) in batch.rows().iter().enumerate() {
            validate_row(&self.input, row, row_number)?;
            validate_key_values(&self.input, &self.stream_idx, row, row_number, true)?;
            if let Some(index) = self.as_of_idx {
                if row.values[index].as_event_time_micros().is_none() {
                    return Err(SparrowError::new(
                        ErrorCode::TypeMismatch,
                        format!(
                            "versioned lookup as-of field '{}' must be event-time",
                            self.input.fields[index].name
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn select_table(&self, row: &Row) -> Result<Option<Arc<ReferenceTable>>> {
        match &self.source {
            LookupSource::Static(table) => Ok(Some(Arc::clone(table))),
            LookupSource::Versioned(table) => {
                let as_of = if let Some(index) = self.as_of_idx {
                    row.values[index].as_event_time_micros().ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::TypeMismatch,
                            "versioned lookup as-of field must be event-time",
                        )
                    })?
                } else {
                    i64::MAX
                };
                Ok(table.table_as_of(as_of))
            }
        }
    }

    fn select_tables(&self, batch: &RowBatch) -> Result<Vec<Option<Arc<ReferenceTable>>>> {
        batch
            .rows()
            .iter()
            .map(|row| self.select_table(row))
            .collect()
    }

    fn output_scratch_bytes(
        &self,
        batch: &RowBatch,
        selected: &Vec<Option<Arc<ReferenceTable>>>,
    ) -> usize {
        let selections = selected
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<Arc<ReferenceTable>>>());
        let rows = batch
            .rows()
            .iter()
            .zip(selected)
            .fold(64usize, |total, (row, table)| {
                let key_bytes = self.stream_idx.iter().fold(0usize, |n, &index| {
                    n.saturating_add(row.values[index].resident_bytes())
                        .saturating_add(scalar_key_len(&row.values[index]))
                        .saturating_add(1)
                });
                let table_row = table.as_ref().map_or(0, |table| table.max_row_resident);
                total
                    .saturating_add(row.resident_bytes())
                    .saturating_add(key_bytes)
                    .saturating_add(table_row)
                    .saturating_add(
                        row.values
                            .len()
                            .saturating_add(self.keep_idx.len())
                            .saturating_mul(std::mem::size_of::<Scalar>()),
                    )
                    .saturating_add(64)
            });
        selections.saturating_add(rows)
    }

    fn output_row(&self, row: &Row, table: Option<&ReferenceTable>) -> Result<Row> {
        let key: Vec<Scalar> = self
            .stream_idx
            .iter()
            .map(|&i| row.values[i].detach_copy())
            .collect();
        let mut values = Vec::with_capacity(row.values.len() + self.keep_idx.len());
        values.extend(row.values.iter().map(Scalar::detach_copy));
        match table.and_then(|table| table.get(&key)) {
            Some(hit) => {
                for &i in &self.keep_idx {
                    let value = hit.values.get(i).ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::InvalidSchema,
                            "reference table row does not match its schema width",
                        )
                    })?;
                    values.push(value.detach_copy());
                }
            }
            None => {
                // Preserve the existing miss contract: one NULL per kept
                // table field, with no change to output row cardinality.
                values.extend(std::iter::repeat(Scalar::Null).take(self.keep_idx.len()));
            }
        }
        Ok(Row { values })
    }

    /// Validate and enrich a batch directly into an owner-metered output
    /// builder. The scratch lease is acquired before any detached output copy;
    /// it is released only after the builder owns the finished rows.
    pub fn on_batch_into(&self, batch: &RowBatch) -> Result<Option<RowBatch>> {
        self.validate_input_batch(batch)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        // Resolve temporal revisions once and keep the selected Arcs alive
        // through both sizing and copy. A concurrent publish therefore cannot
        // change the payload after the scratch reservation was admitted.
        let _selection_lease = self.owner.acquire(
            CreditKind::Reservation,
            batch.num_rows().saturating_mul(std::mem::size_of::<Option<Arc<ReferenceTable>>>())
                .saturating_add(64),
        )?;
        let selected = self.select_tables(batch)?;
        let scratch = self.owner.acquire(
            CreditKind::Reservation,
            self.output_scratch_bytes(batch, &selected).max(1),
        )?;
        let result = (|| {
            let mut builder = RowBatchBuilder::new(
                Arc::new(self.output.clone()),
                Arc::clone(&self.owner),
                CreditKind::Reservation,
                batch.num_rows(),
                self.owner.budget().reservation_bytes,
            )?;
            for (row, table) in batch.rows().iter().zip(&selected) {
                let output = self.output_row(row, table.as_deref())?;
                let minimum = output.resident_bytes().saturating_add(64);
                builder.push_accounted(output, minimum)?;
            }
            Ok(Some(builder.finish()?))
        })();
        drop(scratch);
        result
    }

    /// Compatibility API for embedded callers. Kernel execution uses
    /// [`Self::on_batch_into`] so output copies are metered before publication.
    pub fn on_batch(&self, batch: &RowBatch) -> Result<Vec<Row>> {
        self.validate_input_batch(batch)?;
        let selected = self.select_tables(batch)?;
        batch
            .rows()
            .iter()
            .zip(&selected)
            .map(|(row, table)| self.output_row(row, table.as_deref()))
            .collect()
    }

    pub fn build_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows_metered(&self.output, rows, &self.owner)
    }
}

pub fn lookup_output_schema(stream: &Schema, table: &Schema, keep: &[String]) -> Result<Schema> {
    let mut fields = stream.fields.clone();
    let mut id = fields.iter().map(|f| f.id.raw()).max().unwrap_or(0);
    for name in keep {
        let f = table.field_by_name(name).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("lookup keep column '{name}' missing on table"),
            )
        })?;
        if fields.iter().any(|existing| existing.name == f.name) {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                format!(
                    "lookup output column '{}' duplicates an existing field",
                    f.name
                ),
            ));
        }
        id += 1;
        fields.push(Field::new(
            FieldId::new(id),
            f.name.clone(),
            f.data_type.clone(),
            true,
        ));
    }
    Schema::new(SchemaId::new(stream.id.raw().saturating_add(70)), fields)
}

/// Helper used by demos to build a one-column-key table.
///
/// The supplied owner accounts for the temporary input batch only. The
/// returned table intentionally uses the historical unowned `snapshot`
/// constructor; callers that need job-lifetime accounting should call
/// `ReferenceTable::snapshot_owned` directly.
pub fn table_from_pairs(
    name: &str,
    version: u64,
    key_name: &str,
    value_name: &str,
    value_ty: DataType,
    pairs: Vec<(Scalar, Scalar)>,
    owner: &Arc<MemoryOwner>,
) -> Result<Arc<ReferenceTable>> {
    let schema = Schema::new(
        SchemaId::new(90),
        vec![
            Field::new(FieldId::new(1), key_name, DataType::Utf8, false),
            Field::new(FieldId::new(2), value_name, value_ty, true),
        ],
    )?;
    let mut rows = Vec::new();
    let mut b = RowBatchBuilder::new(
        Arc::new(schema.clone()),
        Arc::clone(owner),
        CreditKind::Reservation,
        pairs.len().max(1),
        owner.budget().reservation_bytes.min(64 * 1024).max(64),
    )?;
    for (k, v) in pairs {
        b.push(Row { values: vec![k, v] })?;
    }
    let batch = b.finish()?;
    rows.extend(batch.rows().iter().map(|r| Row {
        values: r.values.iter().map(Scalar::detach_copy).collect(),
    }));
    ReferenceTable::snapshot(
        name,
        version,
        schema,
        vec![key_name.into()],
        rows,
        1024,
        1024 * 1024,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::ResourceBudget;

    fn owner() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget::compact())
    }

    fn sites(owner: &Arc<MemoryOwner>) -> Arc<ReferenceTable> {
        table_from_pairs(
            "sites",
            1,
            "device_id",
            "site",
            DataType::Utf8,
            vec![(Scalar::utf8("a"), Scalar::utf8("west"))],
            owner,
        )
        .unwrap()
    }

    #[test]
    fn r3_failed_publish_preserves_previous_interval_and_gc_is_explicit() {
        let owner = owner();
        let versioned = VersionedReferenceTable::new("sites", 2).unwrap();
        let table = sites(&owner);
        versioned.publish(1, 0, table.clone()).unwrap();
        versioned.publish(2, 10, table.clone()).unwrap();
        assert!(versioned.publish(3, 20, table.clone()).is_err());
        assert!(versioned.lookup_as_of(&[Scalar::utf8("a")], 100).is_some());
        assert_eq!(versioned.gc_before(10), 1);
        versioned.publish(3, 20, table).unwrap();
        assert_eq!(versioned.version_count(), 2);
    }

    fn stream_schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "temp", DataType::Float64, true),
            ],
        )
        .unwrap()
    }

    fn spec() -> LookupSpec {
        LookupSpec::static_table(
            "sites",
            vec!["device_id".into()],
            vec!["device_id".into()],
            vec!["site".into()],
        )
    }

    #[test]
    fn p2_36_snapshot_checksum_verifies() {
        let t = sites(&owner());
        t.verify().unwrap();
        assert_ne!(t.checksum(), 0);
        let again = sites(&owner());
        assert_eq!(
            t.checksum(),
            again.checksum(),
            "same snapshot bytes must be stable"
        );
    }

    #[test]
    fn p2_36_wrong_stored_checksum_fails_closed() {
        let t = sites(&owner());
        let bad = t.with_stored_checksum(t.checksum().wrapping_add(1));
        let err = bad.verify().unwrap_err();
        assert_eq!(err.code, ErrorCode::CodecViolation);
        assert!(err.to_string().contains("crc32"));
    }

    #[test]
    fn p2_36_corrupted_row_fails_closed() {
        let t = sites(&owner());
        let bad = t.with_corrupted_first_row();
        let err = bad.verify().unwrap_err();
        assert_eq!(err.code, ErrorCode::CodecViolation);
        let owner = owner();
        let err = match LookupOperator::new(spec(), Arc::new(bad), stream_schema(), owner) {
            Err(e) => e,
            Ok(_) => panic!("corrupt table must fail lookup bind"),
        };
        assert_eq!(err.code, ErrorCode::CodecViolation);
    }

    #[test]
    fn p2_36_publish_rejects_corrupt_table() {
        let owner = owner();
        let good = sites(&owner);
        let ver = VersionedReferenceTable::from_static(Arc::clone(&good), 8).unwrap();
        let bad = Arc::new(good.with_stored_checksum(1));
        let err = ver.publish(2, 10, bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::CodecViolation);
    }
}

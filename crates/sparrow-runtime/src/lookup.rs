//! Static and versioned [`ReferenceTable`] enrichment.
//!
//! V0.2: a finite snapshot frozen at job submit (running Job keeps the Arc).
//! V0.3: [`VersionedReferenceTable`] — as-of-event-time lookup; new versions
//! can be published with `valid_from` without replacing the job handle.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, Result, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SchemaId, SparrowError,
};
use sparrow_plan::LookupSpec;

use crate::window::{finish_rows, resolve_keys};

/// Finite, detached snapshot. Not an async lookup.
#[derive(Clone, Debug)]
pub struct ReferenceTable {
    pub name: String,
    pub version: u64,
    pub schema: Schema,
    pub key_fields: Vec<String>,
    index: HashMap<Vec<u8>, Row>,
    /// CRC-32 of the frozen snapshot (name, version, keys, rows). Fail closed
    /// on mismatch (P2-36).
    checksum: u32,
}

impl ReferenceTable {
    pub fn snapshot(
        name: impl Into<String>,
        version: u64,
        schema: Schema,
        key_fields: Vec<String>,
        rows: Vec<Row>,
        max_rows: usize,
        max_bytes: usize,
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
        let key_idx = resolve_keys(&schema, &key_fields)?;
        let mut index = HashMap::new();
        let mut bytes = 0usize;
        for row in rows {
            let key: Vec<Scalar> = key_idx
                .iter()
                .map(|&i| row.values[i].detach_copy())
                .collect();
            let detached = Row {
                values: row.values.iter().map(Scalar::detach_copy).collect(),
            };
            bytes = bytes.saturating_add(detached.tracked_bytes());
            if bytes > max_bytes {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    format!("reference table exceeds max_bytes {max_bytes}"),
                ));
            }
            index.insert(encode_scalars(&key), detached);
        }
        let name = name.into();
        let checksum = table_crc(&name, version, &key_fields, &index);
        Ok(Arc::new(Self {
            name,
            version,
            schema,
            key_fields,
            index,
            checksum,
        }))
    }

    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// Recompute CRC-32 from live rows and compare to the stored value.
    pub fn verify(&self) -> Result<()> {
        let got = table_crc(&self.name, self.version, &self.key_fields, &self.index);
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
    pub fn with_stored_checksum(&self, checksum: u32) -> Self {
        let mut t = self.clone();
        t.checksum = checksum;
        t
    }

    /// Test helper: flip the first cell of one row, keep the stored checksum.
    pub fn with_corrupted_first_row(&self) -> Self {
        let mut t = self.clone();
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

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn get(&self, key: &[Scalar]) -> Option<&Row> {
        self.index.get(&encode_scalars(key))
    }
}

fn table_crc(
    name: &str,
    version: u64,
    key_fields: &[String],
    index: &HashMap<Vec<u8>, Row>,
) -> u32 {
    let mut buf = Vec::new();
    let nb = name.as_bytes();
    buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
    buf.extend_from_slice(nb);
    buf.extend_from_slice(&version.to_le_bytes());
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
                format!("versioned table '{}' != snapshot '{}'", self.name, table.name),
            ));
        }
        table.verify()?;
        let mut g = self.inner.write().expect("versioned table");
        if let Some(last) = g.last() {
            if valid_from < last.valid_from {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "versioned table valid_from must not go backward",
                ));
            }
        }
        if let Some(last) = g.last_mut() {
            if last.valid_to.is_none() {
                last.valid_to = Some(valid_from);
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
        g.push(TableVersion {
            version,
            valid_from,
            valid_to: None,
            table,
        });
        Ok(())
    }

    pub fn lookup_as_of(&self, key: &[Scalar], as_of: i64) -> Option<Row> {
        let g = self.inner.read().expect("versioned table");
        g.iter()
            .rev()
            .find(|v| {
                as_of >= v.valid_from && v.valid_to.map(|to| as_of < to).unwrap_or(true)
            })
            .and_then(|v| v.table.get(key).cloned())
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
    #[allow(dead_code)]
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
        let (name, schema, key_fields) = match &source {
            LookupSource::Static(t) => (t.name.clone(), t.schema.clone(), t.key_fields.clone()),
            LookupSource::Versioned(t) => {
                let latest = t.latest().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("versioned table '{}' has no versions", t.name()),
                    )
                })?;
                (t.name().to_string(), latest.schema.clone(), latest.key_fields.clone())
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
                if let Some(latest) = t.latest() {
                    latest.verify()?;
                }
            }
        }
        let stream_idx = resolve_keys(&input, &spec.stream_keys)?;
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
            Some(input.index_of_name(field).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown as-of event-time field '{field}'"),
                )
            })?)
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
            LookupSource::Versioned(t) => t
                .latest()
                .map(|x| x.version)
                .unwrap_or(0),
        }
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }

    pub fn on_batch(&self, batch: &RowBatch) -> Result<Vec<Row>> {
        let mut out = Vec::with_capacity(batch.num_rows());
        for row in batch.rows() {
            let key: Vec<Scalar> = self
                .stream_idx
                .iter()
                .map(|&i| row.values[i].detach_copy())
                .collect();
            let hit = match &self.source {
                LookupSource::Static(t) => t.get(&key).cloned(),
                LookupSource::Versioned(t) => {
                    let as_of = if let Some(i) = self.as_of_idx {
                        row.values
                            .get(i)
                            .and_then(Scalar::as_event_time_micros)
                            .ok_or_else(|| {
                                SparrowError::new(
                                    ErrorCode::TypeMismatch,
                                    "versioned lookup as-of field must be event-time",
                                )
                            })?
                    } else {
                        i64::MAX
                    };
                    t.lookup_as_of(&key, as_of)
                }
            };
            let mut values: Vec<Scalar> = row.values.iter().map(Scalar::detach_copy).collect();
            match hit {
                Some(hit) => {
                    for &i in &self.keep_idx {
                        values.push(hit.values[i].detach_copy());
                    }
                }
                None => {
                    for _ in &self.keep_idx {
                        values.push(Scalar::Null);
                    }
                }
            }
            out.push(Row { values });
        }
        Ok(out)
    }

    pub fn build_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows(&self.output, rows, &self.owner)
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
        b.push(Row {
            values: vec![k, v],
        })?;
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

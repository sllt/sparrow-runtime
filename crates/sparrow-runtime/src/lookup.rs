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
        Ok(Arc::new(Self {
            name: name.into(),
            version,
            schema,
            key_fields,
            index,
        }))
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn get(&self, key: &[Scalar]) -> Option<&Row> {
        self.index.get(&encode_scalars(key))
    }
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

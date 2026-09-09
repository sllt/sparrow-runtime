//! Static [`ReferenceTable`] enrichment. Snapshot is frozen at job submit.
//! A new Job sees a new table; a running Job keeps the old Arc.

use std::collections::HashMap;
use std::sync::Arc;

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

pub struct LookupOperator {
    spec: LookupSpec,
    table: Arc<ReferenceTable>,
    stream_idx: Vec<usize>,
    keep_idx: Vec<usize>,
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
        spec.validate()?;
        if spec.table != table.name {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("lookup table '{}' != snapshot '{}'", spec.table, table.name),
            ));
        }
        let stream_idx = resolve_keys(&input, &spec.stream_keys)?;
        let keep = if spec.keep.is_empty() {
            table
                .schema
                .fields
                .iter()
                .filter(|f| !table.key_fields.iter().any(|k| k == &f.name))
                .map(|f| f.name.clone())
                .collect()
        } else {
            spec.keep.clone()
        };
        let keep_idx = resolve_keys(&table.schema, &keep)?;
        let output = lookup_output_schema(&input, &table.schema, &keep)?;
        Ok(Self {
            spec,
            table,
            stream_idx,
            keep_idx,
            input,
            output,
            owner,
        })
    }

    pub fn table_version(&self) -> u64 {
        self.table.version
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
            let mut values: Vec<Scalar> = row.values.iter().map(Scalar::detach_copy).collect();
            match self.table.get(&key) {
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

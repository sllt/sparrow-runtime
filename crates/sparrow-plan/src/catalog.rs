//! In-memory table catalog used by Graph and SQL binders.

use std::collections::HashMap;

use crate::expr_spec::parse_type;
use crate::graph::CatalogTableSpec;
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{Field, FieldId, Schema, SchemaId};

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, Schema>,
    next_schema: u32,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, schema: Schema) {
        self.next_schema = self.next_schema.max(schema.id.raw() + 1);
        self.tables.insert(name.into(), schema);
    }

    pub fn get(&self, name: &str) -> Result<&Schema> {
        self.tables.get(name).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, format!("unknown table '{name}'"))
        })
    }

    pub fn extend_from_spec(&mut self, specs: &[CatalogTableSpec]) -> Result<()> {
        for spec in specs {
            let schema = schema_from_fields(&spec.fields, SchemaId::new(self.next_schema))?;
            self.next_schema += 1;
            self.insert(&spec.name, schema);
        }
        Ok(())
    }
}

pub fn schema_from_fields(
    fields: &[crate::graph::FieldSpec],
    id: SchemaId,
) -> Result<Schema> {
    let mapped: Result<Vec<Field>> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Ok(Field::new(
                FieldId::new((i + 1) as u16),
                f.name.clone(),
                parse_type(&f.data_type)?,
                f.nullable,
            ))
        })
        .collect();
    Schema::new(id, mapped?)
}

pub fn project_schema(id: SchemaId, names: &[(String, sparrow_model::DataType, bool)]) -> Result<Schema> {
    let fields = names
        .iter()
        .enumerate()
        .map(|(i, (name, ty, null))| Field::new(FieldId::new((i + 1) as u16), name.clone(), ty.clone(), *null))
        .collect();
    Schema::new(id, fields)
}

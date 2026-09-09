//! V0.2 stateful operator specs shared by Graph and SQL binders.

use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, ErrorCode, Field, FieldId, Result, Schema, SchemaId, SparrowError, WindowKind,
};

#[derive(Clone, Debug, PartialEq)]
pub struct AggCall {
    pub func: AggFn,
    pub input: Option<Expr>,
    pub alias: String,
    pub count_star: bool,
}

impl AggCall {
    pub fn new(func: AggFn, input: Option<Expr>, alias: impl Into<String>) -> Self {
        let count_star = func == AggFn::Count && input.is_none();
        Self {
            func,
            input,
            alias: alias.into(),
            count_star,
        }
    }

    pub fn count_star(alias: impl Into<String>) -> Self {
        Self {
            func: AggFn::Count,
            input: None,
            alias: alias.into(),
            count_star: true,
        }
    }

    pub fn input_type(&self, schema: &Schema) -> Result<DataType> {
        if self.count_star || self.input.is_none() {
            return Ok(DataType::Int64);
        }
        sparrow_expr::infer_type(self.input.as_ref().unwrap(), schema)
    }

    pub fn result_type(&self, schema: &Schema) -> Result<DataType> {
        agg_result_type(self, schema)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub keys: Vec<String>,
    pub aggs: Vec<AggCall>,
}

impl WindowSpec {
    pub fn validate(&self) -> Result<()> {
        if self.aggs.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "window aggregate requires at least one aggregate",
            ));
        }
        match self.kind {
            WindowKind::TumblingProcessingTime { size_micros } if size_micros <= 0 => {
                Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "tumbling size_micros must be > 0",
                ))
            }
            WindowKind::Count { size } if size == 0 => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "count window size must be > 0",
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DedupSpec {
    pub keys: Vec<String>,
    pub ttl_micros: i64,
    pub max_keys: usize,
}

impl DedupSpec {
    pub fn validate(&self) -> Result<()> {
        if self.keys.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "deduplicate requires at least one key",
            ));
        }
        if self.ttl_micros <= 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "unbounded forever-dedup is rejected: ttl_micros must be > 0",
            )
            .context("hint", "set an explicit TTL scope"));
        }
        if self.max_keys == 0 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "unbounded forever-dedup is rejected: max_keys must be > 0",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LookupSpec {
    pub table: String,
    pub stream_keys: Vec<String>,
    pub table_keys: Vec<String>,
    pub keep: Vec<String>,
}

impl LookupSpec {
    pub fn validate(&self) -> Result<()> {
        if self.table.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "lookup requires a reference table name",
            ));
        }
        if self.stream_keys.is_empty()
            || self.stream_keys.len() != self.table_keys.len()
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "lookup ON keys must be non-empty and aligned",
            ));
        }
        Ok(())
    }
}

pub fn window_output_schema(input: &Schema, spec: &WindowSpec) -> Result<Schema> {
    let mut fields = Vec::new();
    let mut id = 1u16;
    for k in &spec.keys {
        let f = input.field_by_name(k).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, format!("unknown key '{k}'"))
        })?;
        fields.push(Field::new(
            FieldId::new(id),
            f.name.clone(),
            f.data_type.clone(),
            f.nullable,
        ));
        id += 1;
    }
    fields.push(Field::new(
        FieldId::new(id),
        "window_start",
        DataType::Int64,
        false,
    ));
    id += 1;
    fields.push(Field::new(
        FieldId::new(id),
        "window_end",
        DataType::Int64,
        false,
    ));
    id += 1;
    for agg in &spec.aggs {
        let ty = agg_result_type(agg, input)?;
        fields.push(Field::new(FieldId::new(id), agg.alias.clone(), ty, true));
        id += 1;
    }
    Schema::new(SchemaId::new(input.id.raw().saturating_add(50)), fields)
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

pub fn agg_result_type(agg: &AggCall, schema: &Schema) -> Result<DataType> {
    Ok(match agg.func {
        AggFn::Count => DataType::Int64,
        AggFn::Avg => DataType::Float64,
        AggFn::Sum | AggFn::Min | AggFn::Max => {
            if agg.count_star || agg.input.is_none() {
                DataType::Int64
            } else {
                sparrow_expr::infer_type(agg.input.as_ref().unwrap(), schema)?
            }
        }
    })
}

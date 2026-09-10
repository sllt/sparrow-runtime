//! V0.2 stateful operator specs shared by Graph and SQL binders.

use sparrow_expr::Expr;
use sparrow_model::{
    check_hop_overlap_bound, AggFn, DataType, ErrorCode, EventTimeBinding, Field, FieldId, Result,
    Schema, SchemaId, SparrowError, WindowKind, DEFAULT_MAX_HOP_OVERLAP,
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
    /// Required for event-time assigners. Forbidden on PT/count (no silent impersonation).
    pub event_time_field: Option<String>,
    /// Output holdback L: `wm_out <= wm_in - L`. Event-time only.
    pub lateness_micros: i64,
    /// Planner cap for hopping overlap (`ceil(size/slide)`).
    pub max_overlap: u32,
    /// Event-time future skew D. `None` means the ET default
    /// ([`sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS`]).
    pub max_future_skew_micros: Option<i64>,
}

impl WindowSpec {
    pub fn new(kind: WindowKind, keys: Vec<String>, aggs: Vec<AggCall>) -> Self {
        Self {
            kind,
            keys,
            aggs,
            event_time_field: None,
            lateness_micros: 0,
            max_overlap: DEFAULT_MAX_HOP_OVERLAP,
            max_future_skew_micros: None,
        }
    }

    pub fn event_time(mut self, field: impl Into<String>, lateness_micros: i64) -> Self {
        self.event_time_field = Some(field.into());
        self.lateness_micros = lateness_micros;
        self
    }

    pub fn binding(&self) -> Option<EventTimeBinding> {
        self.event_time_field.as_ref().map(|f| EventTimeBinding {
            field: f.clone(),
            out_of_orderness_micros: 0,
            max_future_skew_micros: Some(
                self.max_future_skew_micros
                    .unwrap_or(sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS),
            ),
        })
    }

    pub fn with_max_future_skew(mut self, micros: i64) -> Self {
        self.max_future_skew_micros = Some(micros);
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.aggs.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "window aggregate requires at least one aggregate",
            ));
        }
        if self.lateness_micros < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "lateness_micros / holdback L must be >= 0",
            ));
        }
        match self.kind {
            WindowKind::TumblingProcessingTime { size_micros } if size_micros <= 0 => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "tumbling size_micros must be > 0",
                ));
            }
            WindowKind::Count { size } if size == 0 => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "count window size must be > 0",
                ));
            }
            WindowKind::TumblingEventTime { size_micros } if size_micros <= 0 => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "event-time tumbling size_micros must be > 0",
                ));
            }
            WindowKind::HoppingEventTime {
                size_micros,
                slide_micros,
            } => {
                check_hop_overlap_bound(size_micros, slide_micros, self.max_overlap)?;
            }
            _ => {}
        }
        // Hard rule: arrival-order / count windows must not impersonate event-time.
        if !self.kind.uses_event_time() {
            if self.event_time_field.is_some() || self.lateness_micros > 0 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "count / processing-time windows must not impersonate event-time: omit event_time_field and lateness, or reassign to TUMBLE/HOP event-time with holdback",
                ));
            }
        } else if self
            .event_time_field
            .as_deref()
            .map(str::is_empty)
            .unwrap_or(true)
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "event-time window requires event_time_field (stream event-time binding)",
            ));
        }
        Ok(())
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
    /// V0.3 versioned / as-of-event-time lookup. V0.2 static freeze is `false`.
    pub temporal: bool,
    pub as_of_field: Option<String>,
}

impl LookupSpec {
    pub fn static_table(
        table: impl Into<String>,
        stream_keys: Vec<String>,
        table_keys: Vec<String>,
        keep: Vec<String>,
    ) -> Self {
        Self {
            table: table.into(),
            stream_keys,
            table_keys,
            keep,
            temporal: false,
            as_of_field: None,
        }
    }

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
        if self.temporal
            && self
                .as_of_field
                .as_deref()
                .map(str::is_empty)
                .unwrap_or(true)
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "versioned lookup requires as_of_field (FOR SYSTEM_TIME AS OF event-time)",
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
    // Tumble: event-time or processing-time micros. Count: in-window
    // arrival ordinals [0, count) — not timestamps (P3-47).
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
        AggFn::Sum => {
            if agg.count_star || agg.input.is_none() {
                DataType::Int64
            } else {
                let ty = sparrow_expr::infer_type(agg.input.as_ref().unwrap(), schema)?;
                match ty {
                    DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Null => ty,
                    other => {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            format!("SUM does not accept {other}"),
                        ))
                    }
                }
            }
        }
        AggFn::Min | AggFn::Max => {
            if agg.count_star || agg.input.is_none() {
                DataType::Int64
            } else {
                sparrow_expr::infer_type(agg.input.as_ref().unwrap(), schema)?
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_expr::Expr;
    use sparrow_model::FieldId;

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "flag", DataType::Bool, false),
                Field::new(FieldId::new(2), "name", DataType::Utf8, false),
                Field::new(FieldId::new(3), "v", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    #[test]
    fn p3_45_sum_rejects_bool_and_utf8() {
        let s = schema();
        let err = agg_result_type(
            &AggCall::new(
                AggFn::Sum,
                Some(Expr::Column {
                    name: "flag".into(),
                }),
                "s",
            ),
            &s,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        let err = agg_result_type(
            &AggCall::new(
                AggFn::Sum,
                Some(Expr::Column {
                    name: "name".into(),
                }),
                "s",
            ),
            &s,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        assert_eq!(
            agg_result_type(
                &AggCall::new(
                    AggFn::Sum,
                    Some(Expr::Column { name: "v".into() }),
                    "s",
                ),
                &s,
            )
            .unwrap(),
            DataType::Int64
        );
    }
}

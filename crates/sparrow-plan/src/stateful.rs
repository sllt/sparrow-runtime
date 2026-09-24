//! V0.2 stateful operator specs shared by Graph and SQL binders.

use serde::{Deserialize, Serialize};
use sparrow_expr::Expr;
use sparrow_model::{
    check_hop_overlap_bound, AggFn, DataType, ErrorCode, EventTimeBinding, Field, FieldId, Result,
    Schema, SchemaId, SparrowError, WindowKind, DEFAULT_MAX_HOP_OVERLAP,
};

/// How an IoT state operator handles a row whose configured value fields are
/// missing, NULL, or otherwise not a usable sensor value.
///
/// This is deliberately part of the plan contract instead of being a runtime
/// fallback.  `Error` fails the attempt; `Ignore` leaves the per-key state
/// unchanged and accounts the skipped row in the runtime diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidValuePolicy {
    Error,
    Ignore,
}

/// Deadband comparison mode.  Relative values are ratios (`0.05` is 5%).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadbandMode {
    Absolute,
    Relative,
}

/// Which previous value is used as the deadband reference point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadbandBaseline {
    LastInput,
    LastOutput,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadbandSpec {
    pub mode: DeadbandMode,
    pub baseline: DeadbandBaseline,
    pub threshold: f64,
}

/// Direction of a Schmitt trigger. Equality at either threshold transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HysteresisDirection {
    High,
    Low,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HysteresisSpec {
    pub direction: HysteresisDirection,
    pub enter: f64,
    pub exit: f64,
}

/// Source-ordered processing time. Host downtime never advances this clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingTimePolicy { Paused }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IotTimingSpec {
    /// Lifecycle events are never suppressed. Cooldown applies only to the
    /// notification flag; at most one activation notification waits per key.
    Alarm {
        activate_micros: i64,
        resolve_micros: i64,
        cooldown_micros: i64,
        notification_max_age_micros: i64,
        clock: ProcessingTimePolicy,
    },
    /// Missing samples keep the last valid condition; false cancels it.
    HoldFor { duration_micros: i64, clock: ProcessingTimePolicy },
    /// A forced max-wait emission ends the current burst, as does quiet expiry.
    Debounce {
        quiet_micros: i64,
        max_wait_micros: i64,
        leading: bool,
        trailing: bool,
        reset_on_repeat: bool,
        clock: ProcessingTimePolicy,
    },
}
impl IotTimingSpec {
    pub fn kind_name(&self) -> &'static str {
        match self { Self::HoldFor { .. } => "hold_for", Self::Debounce { .. } => "debounce", Self::Alarm { .. } => "alarm" }
    }
    pub fn state_kind(&self) -> u8 {
        match self { Self::HoldFor { .. } => 7, Self::Debounce { .. } => 8, Self::Alarm { .. } => 11 }
    }
    pub fn validate(&self) -> Result<()> {
        let valid = match self {
            Self::Alarm { activate_micros, resolve_micros, cooldown_micros, notification_max_age_micros, .. } =>
                *activate_micros >= 0 && *resolve_micros >= 0 && *cooldown_micros >= 0 && *notification_max_age_micros > 0,
            Self::HoldFor { duration_micros, .. } => *duration_micros > 0,
            Self::Debounce { quiet_micros, max_wait_micros, leading, trailing, .. } =>
                *quiet_micros > 0 && *max_wait_micros >= *quiet_micros && (*leading || *trailing),
        };
        if !valid { return Err(SparrowError::new(ErrorCode::InvalidArgument,
            "timed IoT requires positive durations; debounce max_wait >= quiet and leading or trailing")); }
        Ok(())
    }
}

impl HysteresisSpec {
    pub fn validate(&self) -> Result<()> {
        let separated = match self.direction {
            HysteresisDirection::High => self.enter > self.exit,
            HysteresisDirection::Low => self.enter < self.exit,
        };
        if !self.enter.is_finite() || !self.exit.is_finite() || !separated {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,
                "hysteresis requires finite separated thresholds: high enter > exit, low enter < exit"));
        }
        Ok(())
    }
}

impl DeadbandSpec {
    pub fn validate(&self) -> Result<()> {
        if !self.threshold.is_finite() || self.threshold < 0.0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "deadband threshold must be finite and >= 0",
            ));
        }
        Ok(())
    }
}

/// Shared state contract for the first IoT value operators.
///
/// The operator keeps detached comparison values (or a hysteresis latch) per key. `ttl_micros == 0` means
/// no time-based eviction; it does not remove the mandatory `max_keys` and
/// memory bounds.  The plan binder validates field types against the input
/// schema via [`Self::validate`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IotSpec {
    pub keys: Vec<String>,
    pub fields: Vec<String>,
    pub emit_first: bool,
    pub ttl_micros: i64,
    pub max_keys: usize,
    pub invalid: InvalidValuePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadband: Option<DeadbandSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteresis: Option<HysteresisSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<IotTimingSpec>,
}

impl IotSpec {
    pub fn is_alarm(&self) -> bool { matches!(self.timing, Some(IotTimingSpec::Alarm { .. })) }

    /// Alarm has an independently typed event schema. Legacy IoT stages keep
    /// their original row schema; callers must not silently append fields.
    pub fn output_schema(&self, input: &Schema) -> Result<Schema> {
        if !self.is_alarm() { return Ok(input.clone()); }
        let extra = [
            ("sparrow_alarm_event", DataType::Utf8),
            ("sparrow_alarm_phase", DataType::Utf8),
            ("sparrow_alarm_generation", DataType::Utf8),
            ("sparrow_alarm_operator", DataType::UInt64),
            ("sparrow_alarm_episode", DataType::UInt64),
            ("sparrow_alarm_time", DataType::Int64),
            ("sparrow_alarm_notify", DataType::Bool),
        ];
        let mut id = input.fields.iter().map(|f| f.id.raw()).max().unwrap_or(0);
        let mut fields = input.fields.clone();
        for (name, ty) in extra {
            if input.field_by_name(name).is_some() {
                return Err(SparrowError::new(ErrorCode::InvalidSchema, "alarm output field collides with input"));
            }
            id = id.checked_add(1).ok_or_else(|| SparrowError::new(ErrorCode::InvalidSchema, "alarm field ID overflow"))?;
            fields.push(Field::new(FieldId::new(id), name, ty, false));
        }
        let schema_id = input.id.raw().checked_add(90)
            .ok_or_else(|| SparrowError::new(ErrorCode::InvalidSchema, "alarm schema ID overflow"))?;
        Schema::new(SchemaId::new(schema_id), fields)
    }

    fn validate_params(&self) -> Result<()> {
        validate_names(&self.keys, "keys", 16)?;
        validate_names(&self.fields, "fields", 16)?;
        if self.ttl_micros < 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "IoT ttl_micros must be >= 0 (zero disables TTL)",
            ));
        }
        if self.max_keys == 0 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT max_keys must be > 0",
            ));
        }
        if let Some(timing) = &self.timing {
            timing.validate()?;
            if self.ttl_micros != 0 || self.emit_first || self.deadband.is_some() || self.hysteresis.is_some() {
                return Err(SparrowError::new(ErrorCode::InvalidArgument,
                    "timed IoT requires ttl_micros=0 and emit_first=false; leading is explicit; other IoT modes cannot mix"));
            }
        }
        if let Some(deadband) = &self.deadband {
            deadband.validate()?;
        }
        if let Some(hysteresis) = &self.hysteresis {
            hysteresis.validate()?;
            if self.deadband.is_some() || self.ttl_micros != 0 {
                return Err(SparrowError::new(ErrorCode::InvalidArgument,
                    "hysteresis cannot mix deadband or silently expire its state; ttl_micros must be 0"));
            }
        }
        Ok(())
    }

    /// Validate this specification against a concrete input schema.  Keeping
    /// the schema in the public validation entry point prevents an embedded
    /// caller from constructing a physically typed but semantically invalid
    /// IoT stage.
    pub fn validate(&self, input: &Schema) -> Result<()> {
        self.validate_params()?;
        if self.is_alarm() {
            if input.fields.len() > 48 || self.fields.len() != 2
                || self.fields.iter().any(|name| input.field_by_name(name).is_none_or(|f| f.data_type != DataType::Bool)) {
                return Err(SparrowError::new(ErrorCode::TypeMismatch,
                    "alarm requires enter/clear Bool fields and at most 48 scalar input fields"));
            }
            self.output_schema(input)?;
        }
        if let Some(timing) = &self.timing {
            if input.fields.len() > 60 || input.fields.iter().any(|f| !iot_value_type(&f.data_type)) {
                return Err(SparrowError::new(ErrorCode::FeatureUnavailable,
                    "timed IoT retains full rows and currently requires at most 60 flat scalar fields"));
            }
            if matches!(timing, IotTimingSpec::HoldFor { .. }) && (self.fields.len() != 1
                || input.field_by_name(&self.fields[0]).is_none_or(|f| f.data_type != DataType::Bool)) {
                return Err(SparrowError::new(ErrorCode::TypeMismatch, "hold_for requires one Bool condition field"));
            }
        }
        for name in &self.keys {
            let field = input.field_by_name(name).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("IoT key field '{name}' is missing from input schema"),
                )
            })?;
            if field.nullable {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("IoT key field '{name}' must be non-nullable"),
                ));
            }
            if !iot_key_type(&field.data_type) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!("IoT key field '{name}' has unsupported type {}", field.data_type),
                ));
            }
        }
        for name in &self.fields {
            let field = input.field_by_name(name).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("IoT value field '{name}' is missing from input schema"),
                )
            })?;
            if !iot_value_type(&field.data_type) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!("IoT value field '{name}' has unsupported type {}", field.data_type),
                ));
            }
        }
        if let Some(deadband) = &self.deadband {
            if self.fields.len() != 1 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "deadband requires exactly one value field",
                ));
            }
            let field = input.field_by_name(&self.fields[0]).expect("validated value field");
            if !matches!(
                field.data_type,
                DataType::Int64 | DataType::UInt64 | DataType::Float64
            ) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "deadband value field must be Int64, UInt64, or Float64",
                ));
            }
            deadband.validate()?;
        }
        if self.hysteresis.is_some() && (self.fields.len() != 1
            || !matches!(input.field_by_name(&self.fields[0]).expect("validated value field").data_type,
                DataType::Int64 | DataType::UInt64 | DataType::Float64)) {
            return Err(SparrowError::new(ErrorCode::TypeMismatch,
                "hysteresis requires exactly one numeric value field"));
        }
        Ok(())
    }

    /// Stable kind tag used by the checkpoint participant registry.  These
    /// values intentionally do not overlap the WindowKind tags.
    pub fn state_kind_tag(&self) -> u8 {
        if let Some(timing) = &self.timing { return timing.state_kind(); }
        let base = if self.hysteresis.is_some() { 6 } else if self.deadband.is_some() { 5 } else { 4 };
        if self.ttl_micros > 0 { base + 5 } else { base }
    }

    pub fn kind_name(&self) -> &'static str {
        if let Some(timing) = &self.timing { return timing.kind_name(); }
        if self.hysteresis.is_some() { "hysteresis" } else if self.deadband.is_some() { "deadband" } else { "change_detect" }
    }
}

fn validate_names(names: &[String], label: &str, max: usize) -> Result<()> {
    if names.is_empty() || names.len() > max {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("IoT {label} must contain 1..={max} fields"),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if name.is_empty() || !seen.insert(name) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("IoT {label} must contain unique non-empty field names"),
            ));
        }
    }
    Ok(())
}

fn iot_key_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Bool
            | DataType::Int64
            | DataType::UInt64
            | DataType::Utf8
            | DataType::Bytes
            | DataType::TimestampMicrosUTC
    )
}

fn iot_value_type(ty: &DataType) -> bool {
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
        if matches!(spec.kind, WindowKind::Count { .. }) { "count_start" } else { "window_start" },
        DataType::Int64,
        false,
    ));
    id += 1;
    fields.push(Field::new(
        FieldId::new(id),
        if matches!(spec.kind, WindowKind::Count { .. }) { "count_end" } else { "window_end" },
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

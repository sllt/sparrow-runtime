//! V0.2 stateful operator specs shared by Graph and SQL binders.

use serde::{Deserialize, Serialize};
use sparrow_expr::Expr;
use sparrow_model::{
    check_hop_overlap_bound, AggFn, DataType, ErrorCode, EventTimeBinding, Field, FieldId, Result,
    Scalar, Schema, SchemaId, SparrowError, WindowKind, DEFAULT_MAX_HOP_OVERLAP,
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
pub enum ProcessingTimePolicy {
    Paused,
}

/// Silence alone admits a non-durable live observation clock. Keep other
/// timing policies zero-sized so legacy Alarm/HoldFor futures do not grow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SilenceClockPolicy {
    Paused,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IotTimingSpec {
    /// Independent grid state; boxed to preserve the legacy specification size.
    Resample(Box<crate::ResampleSpec>),
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
    HoldFor {
        duration_micros: i64,
        clock: ProcessingTimePolicy,
    },
    /// A forced max-wait emission ends the current burst, as does quiet expiry.
    Debounce {
        quiet_micros: i64,
        max_wait_micros: i64,
        leading: bool,
        trailing: bool,
        reset_on_repeat: bool,
        clock: ProcessingTimePolicy,
    },
    /// Device silence: the state is judged from fresh source observations, not
    /// from the presence of telemetry rows. `registered_keys` holds the static
    /// extra key set, one row per entry in the declared `keys` order; it is
    /// boxed only so the largest variant (Alarm) still decides the enum layout.
    Silence {
        duration_micros: i64,
        max_observation_gap_micros: i64,
        #[serde(default)]
        registered_keys: Box<Vec<Vec<serde_json::Value>>>,
        clock: SilenceClockPolicy,
    },
}
impl IotTimingSpec {
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            Self::Silence {
                clock: SilenceClockPolicy::Live,
                ..
            }
        )
    }
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::HoldFor { .. } => "hold_for",
            Self::Debounce { .. } => "debounce",
            Self::Alarm { .. } => "alarm",
            Self::Silence { .. } => "silence",
            Self::Resample(_) => "resample",
        }
    }
    pub fn state_kind(&self) -> u8 {
        match self {
            Self::HoldFor { .. } => 7,
            Self::Debounce { .. } => 8,
            Self::Alarm { .. } => 11,
            Self::Silence { .. } => 12,
            Self::Resample(config) => config.mode.state_kind(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        let valid = match self {
            Self::Resample(config) => return config.validate(),
            Self::Alarm {
                activate_micros,
                resolve_micros,
                cooldown_micros,
                notification_max_age_micros,
                ..
            } => {
                *activate_micros >= 0
                    && *resolve_micros >= 0
                    && *cooldown_micros >= 0
                    && *notification_max_age_micros > 0
            }
            Self::HoldFor {
                duration_micros, ..
            } => *duration_micros > 0,
            Self::Debounce {
                quiet_micros,
                max_wait_micros,
                leading,
                trailing,
                ..
            } => *quiet_micros > 0 && *max_wait_micros >= *quiet_micros && (*leading || *trailing),
            // A silence window must cover at least two observation gaps
            // (`checked_mul` keeps an extreme gap from wrapping into a pass).
            Self::Silence {
                duration_micros,
                max_observation_gap_micros,
                ..
            } => {
                *duration_micros > 0
                    && *max_observation_gap_micros > 0
                    && max_observation_gap_micros
                        .checked_mul(2)
                        .is_some_and(|gap| gap <= *duration_micros)
            }
        };
        if !valid {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,
            "timed IoT requires positive durations; debounce max_wait >= quiet and leading or trailing; silence duration >= 2 * max_observation_gap"));
        }
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
    pub fn is_alarm(&self) -> bool {
        matches!(self.timing, Some(IotTimingSpec::Alarm { .. }))
    }

    pub fn is_silence(&self) -> bool {
        matches!(self.timing, Some(IotTimingSpec::Silence { .. }))
    }

    pub fn is_resample(&self) -> bool {
        matches!(self.timing, Some(IotTimingSpec::Resample(_)))
    }

    /// Alarm has an independently typed event schema. Legacy IoT stages keep
    /// their original row schema; callers must not silently append fields.
    /// Silence reports the lifecycle of a *missing* telemetry record, so it
    /// keeps only the configured keys and never fabricates the original values.
    pub fn output_schema(&self, input: &Schema) -> Result<Schema> {
        if let Some(IotTimingSpec::Resample(config)) = &self.timing {
            return config.output_schema(&self.keys, &self.fields, input);
        }
        if self.is_silence() {
            return self.silence_output_schema(input);
        }
        if !self.is_alarm() {
            return Ok(input.clone());
        }
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
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    "alarm output field collides with input",
                ));
            }
            id = id.checked_add(1).ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidSchema, "alarm field ID overflow")
            })?;
            fields.push(Field::new(FieldId::new(id), name, ty, false));
        }
        let schema_id = input.id.raw().checked_add(90).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidSchema, "alarm schema ID overflow")
        })?;
        Schema::new(SchemaId::new(schema_id), fields)
    }

    /// Silenced/never-seen devices have no telemetry row to forward, so the
    /// output keeps only the configured keys (same type, still non-nullable)
    /// plus the lifecycle columns.
    fn silence_output_schema(&self, input: &Schema) -> Result<Schema> {
        let mut fields = Vec::with_capacity(self.keys.len() + silence_event_fields().len());
        for name in &self.keys {
            let field = input.field_by_name(name).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("silence key field '{name}' is missing from input schema"),
                )
            })?;
            if field.nullable {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("silence key field '{name}' must be non-nullable"),
                ));
            }
            fields.push(field.clone());
        }
        let mut id = input.fields.iter().map(|f| f.id.raw()).max().unwrap_or(0);
        for (name, ty, nullable) in silence_event_fields() {
            if input.field_by_name(name).is_some() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    "silence output field collides with input",
                ));
            }
            id = id.checked_add(1).ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidSchema, "silence field ID overflow")
            })?;
            fields.push(Field::new(FieldId::new(id), name, ty, nullable));
        }
        let schema_id = input.id.raw().checked_add(91).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidSchema, "silence schema ID overflow")
        })?;
        Schema::new(SchemaId::new(schema_id), fields)
    }

    fn validate_params(&self) -> Result<()> {
        validate_names(&self.keys, "keys", 16)?;
        if self.is_silence() {
            // Silence reports a missing record: it keeps no telemetry value and
            // cannot silently drop an unusable one, because a dropped row would
            // replay as silence.
            if !self.fields.is_empty() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "silence requires empty fields; it retains keys only",
                ));
            }
            if self.invalid != InvalidValuePolicy::Error {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "silence requires invalid=error",
                ));
            }
        } else {
            validate_names(&self.fields, "fields", 16)?;
        }
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
            if self.ttl_micros != 0
                || self.emit_first
                || self.deadband.is_some()
                || self.hysteresis.is_some()
            {
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
        if let Some(IotTimingSpec::Resample(config)) = &self.timing {
            config.validate_schema(&self.keys, &self.fields, self.max_keys, input)?;
        }
        if self.is_alarm() {
            if input.fields.len() > 48
                || self.fields.len() != 2
                || self.fields.iter().any(|name| {
                    input
                        .field_by_name(name)
                        .is_none_or(|f| f.data_type != DataType::Bool)
                })
            {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "alarm requires enter/clear Bool fields and at most 48 scalar input fields",
                ));
            }
            self.output_schema(input)?;
        }
        if self.is_silence() {
            // The source fingerprint/journal keeps a conservative flat bound for
            // the new profile; the registry is schema-typed and bounded.
            if input.fields.len() > MAX_SILENCE_INPUT_FIELDS
                || input.fields.iter().any(|f| !iot_value_type(&f.data_type))
            {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    "silence requires at most 48 flat scalar input fields",
                ));
            }
            self.silence_registered_keys(input)?;
            self.output_schema(input)?;
        }
        if let Some(timing) = &self.timing {
            if input.fields.len() > 60 || input.fields.iter().any(|f| !iot_value_type(&f.data_type))
            {
                return Err(SparrowError::new(ErrorCode::FeatureUnavailable,
                    "timed IoT retains full rows and currently requires at most 60 flat scalar fields"));
            }
            if matches!(timing, IotTimingSpec::HoldFor { .. })
                && (self.fields.len() != 1
                    || input
                        .field_by_name(&self.fields[0])
                        .is_none_or(|f| f.data_type != DataType::Bool))
            {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "hold_for requires one Bool condition field",
                ));
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
                    format!(
                        "IoT key field '{name}' has unsupported type {}",
                        field.data_type
                    ),
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
                    format!(
                        "IoT value field '{name}' has unsupported type {}",
                        field.data_type
                    ),
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
            let field = input
                .field_by_name(&self.fields[0])
                .expect("validated value field");
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
        if self.hysteresis.is_some()
            && (self.fields.len() != 1
                || !matches!(
                    input
                        .field_by_name(&self.fields[0])
                        .expect("validated value field")
                        .data_type,
                    DataType::Int64 | DataType::UInt64 | DataType::Float64
                ))
        {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "hysteresis requires exactly one numeric value field",
            ));
        }
        Ok(())
    }

    /// Canonical registered key set of a silence configuration, typed by the
    /// declared key order and the input schema.
    ///
    /// Values are interpreted strictly: a JSON float never stands in for an
    /// integer key, NULL and nested/Dynamic keys are refused, `Bytes` is a byte
    /// array, and rows are de-duplicated on the canonical key encoding (not on
    /// their JSON spelling). The row count is bounded by
    /// `min(max_keys, 1024)` and the complete canonical encoding by 64 KiB;
    /// these bounds are independent of the overall computation descriptor
    /// bound, so a registry near its own limit can still be refused at plan
    /// admission rather than being shortened or hashed into another identity.
    ///
    /// This is a pure planner helper that performs its own bounds checks and
    /// allocates only its result; a runtime caller must account the returned
    /// rows in its bounded control workspace before publishing them.
    pub fn silence_registered_keys(&self, input: &Schema) -> Result<Vec<Vec<Scalar>>> {
        let Some(IotTimingSpec::Silence {
            registered_keys, ..
        }) = &self.timing
        else {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "silence registered keys require a silence timing configuration",
            ));
        };
        let limit = self.max_keys.min(MAX_SILENCE_REGISTERED_KEYS);
        if registered_keys.len() > limit {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "silence registered keys exceed min(max_keys, {MAX_SILENCE_REGISTERED_KEYS})"
                ),
            ));
        }
        let mut key_types = Vec::with_capacity(self.keys.len());
        for name in &self.keys {
            let field = input.field_by_name(name).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("silence key field '{name}' is missing from input schema"),
                )
            })?;
            if field.nullable || !iot_key_type(&field.data_type) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("silence key field '{name}' must be a non-nullable scalar key"),
                ));
            }
            key_types.push(field.data_type.clone());
        }
        let mut seen = std::collections::HashSet::new();
        let mut encoded_bytes = 0usize;
        let mut rows = Vec::with_capacity(registered_keys.len());
        for row in registered_keys.iter() {
            if row.len() != self.keys.len() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "each silence registered key row must follow the declared key order and arity",
                ));
            }
            // Bound the complete row BEFORE copying strings/byte arrays. A
            // later post-encoding check would allow a huge invalid row to be
            // materialised outside the caller's bounded workspace.
            let mut projected = encoded_bytes;
            for (value, ty) in row.iter().zip(&key_types) {
                projected = projected
                    .checked_add(silence_key_wire_len(value, ty)?)
                    .ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::BoundExceeded,
                            "silence registry encoding overflow",
                        )
                    })?;
                if projected > MAX_SILENCE_REGISTRY_BYTES {
                    return Err(SparrowError::new(
                        ErrorCode::BoundExceeded,
                        "silence registered keys exceed the canonical byte bound",
                    ));
                }
            }
            let mut values = Vec::with_capacity(row.len());
            for (value, ty) in row.iter().zip(&key_types) {
                values.push(silence_key_scalar(value, ty)?);
            }
            let mut encoded = Vec::new();
            for value in &values {
                value.encode_key(&mut encoded);
            }
            encoded_bytes = encoded_bytes.checked_add(encoded.len()).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "silence registry encoding overflow",
                )
            })?;
            if encoded_bytes > MAX_SILENCE_REGISTRY_BYTES {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    format!("silence registered keys exceed {MAX_SILENCE_REGISTRY_BYTES} canonical bytes"),
                ));
            }
            if !seen.insert(encoded) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "silence registered keys repeat one canonical key",
                ));
            }
            rows.push(values);
        }
        Ok(rows)
    }

    /// Resident JSON configuration retained by an operator. Legal registry
    /// values are scalar or flat byte arrays; invalid nested shapes are rejected
    /// by validation and are never recursively walked here.
    pub fn silence_registry_resident_bytes(&self) -> usize {
        use serde_json::Value;
        let Some(IotTimingSpec::Silence {
            registered_keys, ..
        }) = &self.timing
        else {
            return 0;
        };
        let mut bytes = 64usize.saturating_add(
            registered_keys
                .capacity()
                .saturating_mul(std::mem::size_of::<Vec<Value>>()),
        );
        for row in registered_keys.iter() {
            bytes = bytes
                .saturating_add(48)
                .saturating_add(row.capacity().saturating_mul(std::mem::size_of::<Value>()));
            for value in row {
                bytes = bytes.saturating_add(match value {
                    Value::String(text) => text.capacity().saturating_add(48),
                    Value::Array(values) => values
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Value>())
                        .saturating_add(48),
                    _ => 0,
                });
            }
        }
        bytes
    }

    /// Stable kind tag used by the checkpoint participant registry.  These
    /// values intentionally do not overlap the WindowKind tags.
    pub fn state_kind_tag(&self) -> u8 {
        if let Some(timing) = &self.timing {
            return timing.state_kind();
        }
        let base = if self.hysteresis.is_some() {
            6
        } else if self.deadband.is_some() {
            5
        } else {
            4
        };
        if self.ttl_micros > 0 {
            base + 5
        } else {
            base
        }
    }

    pub fn kind_name(&self) -> &'static str {
        if let Some(timing) = &self.timing {
            return timing.kind_name();
        }
        if self.hysteresis.is_some() {
            "hysteresis"
        } else if self.deadband.is_some() {
            "deadband"
        } else {
            "change_detect"
        }
    }
}

/// Registered silence keys are bounded per Job even when `max_keys` is larger.
const MAX_SILENCE_REGISTERED_KEYS: usize = 1024;
/// Canonical encoding of one silence state's registered key set.
const MAX_SILENCE_REGISTRY_BYTES: usize = 64 * 1024;
/// The new profile keeps the conservative time-journal input bound.
const MAX_SILENCE_INPUT_FIELDS: usize = 48;
/// Lifecycle columns of a silence event, in output order.
fn silence_event_fields() -> [(&'static str, DataType, bool); 7] {
    [
        ("sparrow_silence_event", DataType::Utf8, false),
        ("sparrow_silence_generation", DataType::Utf8, false),
        ("sparrow_silence_operator", DataType::UInt64, false),
        ("sparrow_silence_episode", DataType::UInt64, false),
        ("sparrow_silence_time", DataType::Int64, false),
        ("sparrow_silence_last_seen", DataType::Int64, true),
        ("sparrow_silence_never_seen", DataType::Bool, false),
    ]
}

/// One registered silence key value, typed by the declared key field.
///
/// The conversion is deliberately strict: no NULL, no JSON float for an
/// integer key, no string/array where a scalar was declared, and `Bytes` is an
/// array of u8 values.
fn silence_key_wire_len(value: &serde_json::Value, ty: &DataType) -> Result<usize> {
    use serde_json::Value;
    match (ty, value) {
        (DataType::Bool, Value::Bool(_)) => Ok(2),
        (DataType::Int64 | DataType::TimestampMicrosUTC, Value::Number(n)) if n.is_i64() => Ok(9),
        (DataType::UInt64, Value::Number(n)) if n.is_u64() => Ok(9),
        (DataType::Utf8, Value::String(s)) => Ok(s.len().saturating_add(5)),
        (DataType::Bytes, Value::Array(bytes)) => {
            if bytes.len() > MAX_SILENCE_REGISTRY_BYTES {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "silence registered byte key exceeds bound",
                ));
            }
            if bytes.iter().any(|b| b.as_u64().is_none_or(|v| v > 255)) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "silence registered byte key requires u8 values",
                ));
            }
            Ok(bytes.len().saturating_add(5))
        }
        _ => Err(SparrowError::new(
            ErrorCode::TypeMismatch,
            "silence registered key type mismatch",
        )),
    }
}

fn silence_key_scalar(value: &serde_json::Value, ty: &DataType) -> Result<Scalar> {
    use serde_json::Value;
    let expected = || {
        SparrowError::new(
            ErrorCode::TypeMismatch,
            format!("silence registered key value is not a valid {ty} key"),
        )
    };
    match (ty, value) {
        (DataType::Bool, Value::Bool(v)) => Ok(Scalar::Bool(*v)),
        (DataType::Int64, Value::Number(n)) if n.is_i64() => {
            Ok(Scalar::Int64(n.as_i64().unwrap_or_default()))
        }
        (DataType::UInt64, Value::Number(n)) if n.is_u64() => {
            Ok(Scalar::UInt64(n.as_u64().unwrap_or_default()))
        }
        (DataType::TimestampMicrosUTC, Value::Number(n)) if n.is_i64() => {
            Ok(Scalar::TimestampMicrosUTC(n.as_i64().unwrap_or_default()))
        }
        (DataType::Utf8, Value::String(v)) => Ok(Scalar::utf8(v)),
        (DataType::Bytes, Value::Array(items)) => {
            let mut bytes = Vec::with_capacity(items.len());
            for item in items {
                match item.as_u64().filter(|byte| *byte <= u8::MAX as u64) {
                    Some(byte) => bytes.push(byte as u8),
                    None => return Err(expected()),
                }
            }
            Ok(Scalar::bytes(bytes))
        }
        _ => Err(expected()),
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
    /// Buffered sliding/session families: per-key records AND pending triggers.
    /// This hard work/state bound complements, never replaces, the Job credits.
    pub max_buffered_rows: usize,
}

impl WindowSpec {
    pub fn has_extended_aggs(&self) -> bool {
        self.aggs.iter().any(|a| a.func.is_extended())
    }
    pub fn new(kind: WindowKind, keys: Vec<String>, aggs: Vec<AggCall>) -> Self {
        Self {
            kind,
            keys,
            aggs,
            event_time_field: None,
            lateness_micros: 0,
            max_overlap: DEFAULT_MAX_HOP_OVERLAP,
            max_future_skew_micros: None,
            max_buffered_rows: 1024,
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
        if self
            .aggs
            .iter()
            .any(|a| a.func.is_extended() && (a.input.is_none() || a.count_star))
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "extended aggregate requires exactly one expression, not star",
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
            }
            | WindowKind::HoppingProcessingTime {
                size_micros,
                slide_micros,
            } => {
                WindowKind::hopping_et(size_micros, slide_micros)?;
                check_hop_overlap_bound(size_micros, slide_micros, self.max_overlap)?;
            }
            WindowKind::SlidingCount { size, step } => {
                WindowKind::sliding_count(size, step)?;
                if size > self.max_buffered_rows as u64 {
                    return Err(SparrowError::new(
                        ErrorCode::BoundExceeded,
                        "sliding count size exceeds max_buffered_rows",
                    ));
                }
            }
            WindowKind::SlidingProcessingTime {
                size_micros,
                delay_micros,
            }
            | WindowKind::SlidingEventTime {
                size_micros,
                delay_micros,
            } => {
                WindowKind::sliding(size_micros, delay_micros, self.kind.uses_event_time())?;
            }
            WindowKind::SessionProcessingTime {
                gap_micros,
                max_duration_micros,
            }
            | WindowKind::SessionEventTime {
                gap_micros,
                max_duration_micros,
            } => {
                WindowKind::session(gap_micros, max_duration_micros, self.kind.uses_event_time())?;
                if self.lateness_micros != 0 {
                    return Err(SparrowError::new(ErrorCode::FeatureUnavailable,"Session v1 is final-only with lateness=0; emitted sessions cannot be merged"));
                }
            }
            _ => {}
        }
        if self.kind.is_buffered() && !(1..=16384).contains(&self.max_buffered_rows) {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "max_buffered_rows must be 1..16384",
            ));
        }
        if self.kind.is_buffered() && self.lateness_micros != 0 {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "buffered windows v1 require lateness=0; final outputs are not retracted",
            ));
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
        if self.kind.is_new_window() {
            if let Some(binding) = self.binding() {
                binding.validate()?;
            }
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
        if self.stream_keys.is_empty() || self.stream_keys.len() != self.table_keys.len() {
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
    if spec.kind.is_new_window() && spec.kind.uses_event_time() {
        let field = spec
            .event_time_field
            .as_deref()
            .and_then(|name| input.field_by_name(name))
            .ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "unknown event-time field for window",
                )
            })?;
        if !matches!(
            field.data_type,
            DataType::Int64 | DataType::TimestampMicrosUTC
        ) {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "event-time window requires Int64/TimestampMicrosUTC",
            ));
        }
    }
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
        if spec.kind.is_count() {
            "count_start"
        } else {
            "window_start"
        },
        DataType::Int64,
        false,
    ));
    id += 1;
    fields.push(Field::new(
        FieldId::new(id),
        if spec.kind.is_count() {
            "count_end"
        } else {
            "window_end"
        },
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
        AggFn::First | AggFn::Last => agg.input_type(schema)?,
        AggFn::VarPop | AggFn::VarSamp | AggFn::StddevPop | AggFn::StddevSamp => {
            if !matches!(
                agg.input_type(schema)?,
                DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Null
            ) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "variance/stddev require numeric input",
                ));
            }
            DataType::Float64
        }
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
                &AggCall::new(AggFn::Sum, Some(Expr::Column { name: "v".into() }), "s",),
                &s,
            )
            .unwrap(),
            DataType::Int64
        );
    }
}

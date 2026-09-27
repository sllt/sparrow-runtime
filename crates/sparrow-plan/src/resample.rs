//! Bounded processing-time resampling. This is not an event-time watermark.
use serde::{Deserialize, Serialize};
use sparrow_model::{DataType, ErrorCode, Field, FieldId, Result, Schema, SchemaId, SparrowError};

use crate::stateful::ProcessingTimePolicy;

pub const MAX_RESAMPLE_EMISSIONS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResampleMode {
    Last,
    Mean,
    Interpolate,
}

impl ResampleMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Last => "last",
            Self::Mean => "mean",
            Self::Interpolate => "interpolate",
        }
    }

    pub fn state_kind(self) -> u8 {
        match self {
            Self::Last => 13,
            Self::Mean => 14,
            Self::Interpolate => 15,
        }
    }
}

fn default_emissions() -> usize {
    256
}

/// Box this inside the timing enum: new configuration must not enlarge every
/// legacy IoT specification or stage future. Unused interpolation options are
/// rejected rather than silently accepted with no effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResampleSpec {
    pub mode: ResampleMode,
    pub period_micros: i64,
    #[serde(default)]
    pub max_wait_micros: i64,
    #[serde(default)]
    pub max_gap_micros: i64,
    #[serde(default = "default_emissions")]
    pub max_emissions_per_decision: usize,
    pub clock: ProcessingTimePolicy,
}

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message)
}

impl ResampleSpec {
    pub fn validate(&self) -> Result<()> {
        if self.period_micros <= 0
            || !(1..=MAX_RESAMPLE_EMISSIONS).contains(&self.max_emissions_per_decision)
        {
            return Err(invalid(
                "resample requires a positive period and 1..4096 emissions per decision",
            ));
        }
        match self.mode {
            ResampleMode::Interpolate => {
                if self.max_wait_micros <= 0
                    || self.max_wait_micros > self.period_micros
                    || self.max_gap_micros <= 0
                {
                    return Err(invalid(
                        "interpolate requires 0 < max_wait <= period and positive max_gap",
                    ));
                }
            }
            ResampleMode::Last | ResampleMode::Mean => {
                if self.max_wait_micros != 0 || self.max_gap_micros != 0 {
                    return Err(invalid(
                        "last/mean do not accept interpolation wait/gap options",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn validate_schema(
        &self,
        keys: &[String],
        values: &[String],
        max_keys: usize,
        input: &Schema,
    ) -> Result<()> {
        self.validate()?;
        let reserve = usize::from(self.mode == ResampleMode::Interpolate);
        if max_keys == 0
            || max_keys
                .checked_add(reserve)
                .is_none_or(|n| n > self.max_emissions_per_decision)
        {
            return Err(invalid(
                "resample emission cap must cover max_keys plus one interpolation input result",
            ));
        }
        if keys.is_empty()
            || keys.len() > 16
            || values.is_empty()
            || values.len() > 16
            || input.fields.len() > 48
            || input
                .fields
                .iter()
                .any(|field| field.data_type.is_nested() || field.data_type == DataType::Dynamic)
        {
            return Err(invalid("resample requires 1..16 keys, 1..16 numeric value fields and at most 48 flat input fields"));
        }
        for name in values {
            let field = input
                .field_by_name(name)
                .ok_or_else(|| invalid("resample value field is missing"))?;
            if keys.contains(name) {
                return Err(invalid("resample keys and value fields must be disjoint"));
            }
            if !matches!(
                field.data_type,
                DataType::Int64 | DataType::UInt64 | DataType::Float64
            ) {
                return Err(invalid("resample values must be Int64, UInt64 or Float64"));
            }
        }
        self.output_schema(keys, values, input)?;
        Ok(())
    }

    /// Last preserves the selected numeric types, including full-width integer
    /// precision. Mean/interpolation explicitly produce Float64. Missing is an
    /// all-NULL value vector; the key columns never become nullable.
    pub fn output_schema(
        &self,
        keys: &[String],
        values: &[String],
        input: &Schema,
    ) -> Result<Schema> {
        if keys.is_empty() || keys.len() > 16 || values.is_empty() || values.len() > 16 {
            return Err(invalid(
                "resample output requires bounded key/value arities",
            ));
        }
        let mut fields = Vec::with_capacity(keys.len() + values.len() + 7);
        for name in keys {
            let field = input
                .field_by_name(name)
                .ok_or_else(|| invalid("resample key is missing"))?;
            if field.nullable {
                return Err(invalid("resample keys must be non-nullable"));
            }
            fields.push(field.clone());
        }
        for name in values {
            let mut field = input
                .field_by_name(name)
                .ok_or_else(|| invalid("resample value is missing"))?
                .clone();
            field.nullable = true;
            if self.mode != ResampleMode::Last {
                field.data_type = DataType::Float64;
            }
            fields.push(field);
        }
        let mut id = input
            .fields
            .iter()
            .map(|field| field.id.raw())
            .max()
            .unwrap_or(0);
        for (name, ty) in [
            ("sparrow_resample_mode", DataType::Utf8),
            ("sparrow_resample_time", DataType::Int64),
            ("sparrow_resample_emitted_at", DataType::Int64),
            ("sparrow_resample_missing", DataType::Bool),
            ("sparrow_resample_samples", DataType::UInt64),
            ("sparrow_resample_generation", DataType::Utf8),
            ("sparrow_resample_operator", DataType::UInt64),
        ] {
            if input.field_by_name(name).is_some() {
                return Err(invalid("resample output field collides with input"));
            }
            id = id
                .checked_add(1)
                .ok_or_else(|| invalid("resample field ID overflow"))?;
            fields.push(Field::new(FieldId::new(id), name, ty, false));
        }
        let id = input
            .id
            .raw()
            .checked_add(92)
            .ok_or_else(|| invalid("resample schema ID overflow"))?;
        Schema::new(SchemaId::new(id), fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD_OR_CAP: &str =
        "resample requires a positive period and 1..4096 emissions per decision";
    const WAIT_GAP: &str = "interpolate requires 0 < max_wait <= period and positive max_gap";
    const EMISSION_CAP: &str =
        "resample emission cap must cover max_keys plus one interpolation input result";

    fn spec(
        mode: ResampleMode,
        period_micros: i64,
        max_wait_micros: i64,
        max_gap_micros: i64,
        max_emissions_per_decision: usize,
    ) -> ResampleSpec {
        ResampleSpec {
            mode,
            period_micros,
            max_wait_micros,
            max_gap_micros,
            max_emissions_per_decision,
            clock: ProcessingTimePolicy::Paused,
        }
    }

    fn assert_invalid<T: std::fmt::Debug>(result: Result<T>, message: &str) {
        assert_eq!(
            result.unwrap_err(),
            SparrowError::new(ErrorCode::InvalidArgument, message)
        );
    }

    #[test]
    fn validate_three_modes_and_period_emission_bounds() {
        assert_eq!(MAX_RESAMPLE_EMISSIONS, 4096);
        let accepted = [
            spec(ResampleMode::Last, 1, 0, 0, 1),
            spec(ResampleMode::Mean, 60_000_000, 0, 0, 4096),
            spec(ResampleMode::Interpolate, 1_000, 250, 10, 256),
        ];
        for spec in accepted {
            assert_eq!(spec.validate(), Ok(()));
        }
        let rejected = [
            spec(ResampleMode::Last, 0, 0, 0, 1),
            spec(ResampleMode::Mean, -1, 0, 0, 256),
            spec(ResampleMode::Last, 1, 0, 0, 0),
            spec(ResampleMode::Interpolate, 1, 1, 1, 4097),
        ];
        for spec in rejected {
            assert_invalid(spec.validate(), PERIOD_OR_CAP);
        }
    }

    #[test]
    fn validate_interpolate_wait_and_gap_bounds() {
        let accepted = [
            spec(ResampleMode::Interpolate, 10, 10, 1, 32),
            spec(ResampleMode::Interpolate, 10, 1, 1, 32),
        ];
        for spec in accepted {
            assert_eq!(spec.validate(), Ok(()));
        }
        let rejected = [
            spec(ResampleMode::Interpolate, 10, 11, 1, 32),
            spec(ResampleMode::Interpolate, 10, 0, 1, 32),
            spec(ResampleMode::Interpolate, 10, 10, 0, 32),
            spec(ResampleMode::Interpolate, 10, 10, -1, 32),
        ];
        for spec in rejected {
            assert_invalid(spec.validate(), WAIT_GAP);
        }
    }

    #[test]
    fn validate_schema_emission_cap_covers_max_keys() {
        let input = Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device", DataType::Utf8, false),
                Field::new(FieldId::new(2), "temp", DataType::Int64, false),
            ],
        )
        .unwrap();
        let keys = vec!["device".to_string()];
        let values = vec!["temp".to_string()];
        let cases = [
            (ResampleMode::Last, 8_usize, 8_usize, true),
            (ResampleMode::Last, 8, 9, false),
            (ResampleMode::Mean, 4096, 4096, true),
            (ResampleMode::Interpolate, 5, 4, true),
            (ResampleMode::Interpolate, 5, 5, false),
            (ResampleMode::Interpolate, 4096, usize::MAX, false),
        ];
        for (mode, emissions, max_keys, accept) in cases {
            let spec = match mode {
                ResampleMode::Interpolate => spec(mode, 100, 100, 1, emissions),
                ResampleMode::Last | ResampleMode::Mean => spec(mode, 100, 0, 0, emissions),
            };
            let result = spec.validate_schema(&keys, &values, max_keys, &input);
            if accept {
                assert_eq!(result, Ok(()));
            } else {
                assert_invalid(result, EMISSION_CAP);
            }
        }
    }

    fn output_schema(temp: DataType, hum: DataType, level: DataType) -> Schema {
        Schema::new(
            SchemaId::new(122),
            vec![
                Field::new(FieldId::new(4), "device", DataType::Utf8, false),
                Field::new(FieldId::new(9), "site", DataType::Int64, false),
                Field::new(FieldId::new(2), "temp", temp, true),
                Field::new(FieldId::new(7), "hum", hum, true),
                Field::new(FieldId::new(6), "level", level, true),
                Field::new(
                    FieldId::new(21),
                    "sparrow_resample_mode",
                    DataType::Utf8,
                    false,
                ),
                Field::new(
                    FieldId::new(22),
                    "sparrow_resample_time",
                    DataType::Int64,
                    false,
                ),
                Field::new(
                    FieldId::new(23),
                    "sparrow_resample_emitted_at",
                    DataType::Int64,
                    false,
                ),
                Field::new(
                    FieldId::new(24),
                    "sparrow_resample_missing",
                    DataType::Bool,
                    false,
                ),
                Field::new(
                    FieldId::new(25),
                    "sparrow_resample_samples",
                    DataType::UInt64,
                    false,
                ),
                Field::new(
                    FieldId::new(26),
                    "sparrow_resample_generation",
                    DataType::Utf8,
                    false,
                ),
                Field::new(
                    FieldId::new(27),
                    "sparrow_resample_operator",
                    DataType::UInt64,
                    false,
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn output_schema_three_modes_preserve_or_widen_values() {
        let input = Schema::new(
            SchemaId::new(30),
            vec![
                Field::new(FieldId::new(4), "device", DataType::Utf8, false),
                Field::new(FieldId::new(9), "site", DataType::Int64, false),
                Field::new(FieldId::new(2), "temp", DataType::Int64, false),
                Field::new(FieldId::new(7), "hum", DataType::UInt64, true),
                Field::new(FieldId::new(6), "level", DataType::Float64, false),
                Field::new(FieldId::new(20), "note", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let keys = vec!["device".to_string(), "site".to_string()];
        let values = vec!["temp".to_string(), "hum".to_string(), "level".to_string()];
        let last = spec(ResampleMode::Last, 1_000, 0, 0, 16);
        assert_eq!(
            last.output_schema(&keys, &values, &input).unwrap(),
            output_schema(DataType::Int64, DataType::UInt64, DataType::Float64)
        );
        let widened = output_schema(DataType::Float64, DataType::Float64, DataType::Float64);
        for spec in [
            spec(ResampleMode::Mean, 1_000, 0, 0, 16),
            spec(ResampleMode::Interpolate, 1_000, 1_000, 5, 16),
        ] {
            assert_eq!(spec.output_schema(&keys, &values, &input).unwrap(), widened);
        }
    }

    #[test]
    fn validate_schema_rejects_invalid_field_selections() {
        let input = Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device", DataType::Utf8, false),
                Field::new(FieldId::new(2), "temp", DataType::Int64, false),
                Field::new(FieldId::new(3), "label", DataType::Utf8, false),
                Field::new(FieldId::new(4), "nullable_device", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let spec = spec(ResampleMode::Last, 1_000, 0, 0, 16);
        let cases = [
            ("device", "absent", "resample value field is missing"),
            (
                "device",
                "label",
                "resample values must be Int64, UInt64 or Float64",
            ),
            (
                "temp",
                "temp",
                "resample keys and value fields must be disjoint",
            ),
            (
                "nullable_device",
                "temp",
                "resample keys must be non-nullable",
            ),
        ];
        for (key, value, message) in cases {
            let keys = vec![key.to_string()];
            let values = vec![value.to_string()];
            assert_invalid(spec.validate_schema(&keys, &values, 1, &input), message);
        }
    }

    #[test]
    fn output_schema_rejects_all_unselected_reserved_name_collisions() {
        let spec = spec(ResampleMode::Last, 1_000, 0, 0, 16);
        let keys = vec!["device".to_string()];
        let values = vec!["temp".to_string()];
        for name in [
            "sparrow_resample_mode",
            "sparrow_resample_time",
            "sparrow_resample_emitted_at",
            "sparrow_resample_missing",
            "sparrow_resample_samples",
            "sparrow_resample_generation",
            "sparrow_resample_operator",
        ] {
            let input = Schema::new(
                SchemaId::new(1),
                vec![
                    Field::new(FieldId::new(1), "device", DataType::Utf8, false),
                    Field::new(FieldId::new(2), "temp", DataType::Int64, false),
                    Field::new(FieldId::new(3), name, DataType::Utf8, false),
                ],
            )
            .unwrap();
            assert_invalid(
                spec.output_schema(&keys, &values, &input),
                "resample output field collides with input",
            );
        }
    }

    #[test]
    fn output_schema_id_addition_accepts_last_value_and_rejects_overflow() {
        let spec = spec(ResampleMode::Last, 1_000, 0, 0, 16);
        let keys = vec!["device".to_string()];
        let values = vec!["temp".to_string()];
        let input = |schema_id, device_id| {
            Schema::new(
                SchemaId::new(schema_id),
                vec![
                    Field::new(FieldId::new(device_id), "device", DataType::Utf8, false),
                    Field::new(FieldId::new(1), "temp", DataType::Int64, false),
                ],
            )
            .unwrap()
        };

        let field_limit = input(1, u16::MAX - 7);
        let output = spec.output_schema(&keys, &values, &field_limit).unwrap();
        assert_eq!(output.id, SchemaId::new(93));
        assert_eq!(
            output
                .fields
                .iter()
                .map(|field| field.id.raw())
                .collect::<Vec<_>>(),
            vec![
                u16::MAX - 7,
                1,
                u16::MAX - 6,
                u16::MAX - 5,
                u16::MAX - 4,
                u16::MAX - 3,
                u16::MAX - 2,
                u16::MAX - 1,
                u16::MAX,
            ]
        );
        assert_invalid(
            spec.output_schema(&keys, &values, &input(1, u16::MAX - 6)),
            "resample field ID overflow",
        );

        let schema_limit = input(u32::MAX - 92, 2);
        let output = spec.output_schema(&keys, &values, &schema_limit).unwrap();
        assert_eq!(output.id, SchemaId::new(u32::MAX));
        assert_eq!(
            output
                .fields
                .iter()
                .map(|field| field.id.raw())
                .collect::<Vec<_>>(),
            vec![2, 1, 3, 4, 5, 6, 7, 8, 9]
        );
        assert_invalid(
            spec.output_schema(&keys, &values, &input(u32::MAX - 91, 2)),
            "resample schema ID overflow",
        );
    }

    #[test]
    fn resample_serde_defaults_unknown_fields_and_timing_round_trip() {
        use crate::stateful::IotTimingSpec;

        let defaulted: ResampleSpec = serde_json::from_value(serde_json::json!({
            "mode": "mean",
            "period_micros": 1_000,
            "clock": "paused"
        }))
        .unwrap();
        assert_eq!(defaulted, spec(ResampleMode::Mean, 1_000, 0, 0, 256));

        let unknown = serde_json::from_value::<ResampleSpec>(serde_json::json!({
            "mode": "mean",
            "period_micros": 1_000,
            "clock": "paused",
            "unexpected": true
        }))
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown field `unexpected`"));

        let timing = IotTimingSpec::Resample(Box::new(defaulted));
        assert_eq!(timing.kind_name(), "resample");
        let encoded = serde_json::to_value(&timing).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({
                "kind": "resample",
                "mode": "mean",
                "period_micros": 1_000,
                "max_wait_micros": 0,
                "max_gap_micros": 0,
                "max_emissions_per_decision": 256,
                "clock": "paused"
            })
        );
        let decoded: IotTimingSpec = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, timing);
        match decoded {
            IotTimingSpec::Resample(config) => {
                assert_eq!(config.mode, ResampleMode::Mean);
                assert_eq!(config.max_wait_micros, 0);
                assert_eq!(config.max_gap_micros, 0);
                assert_eq!(config.max_emissions_per_decision, 256);
            }
            other => panic!("expected resample timing spec, got {other:?}"),
        }
    }
}

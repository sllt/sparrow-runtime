//! Explicit bounded multi-row/two-input operators. Fresh-only in this version.
use crate::expr_spec::ExprSpec;
use serde::{Deserialize, Serialize};
use sparrow_expr::{infer_type, Expr};
use sparrow_model::{DataType, ErrorCode, Field, Result, Schema, SparrowError};

fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}
fn rows() -> usize {
    1024
}
fn bytes() -> usize {
    1024 * 1024
}
fn item() -> String {
    "item".into()
}
fn left_prefix() -> String {
    "left_".into()
}
fn right_prefix() -> String {
    "right_".into()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnnestNodeSpec {
    pub expr: ExprSpec,
    #[serde(default = "item")]
    pub as_field: String,
    #[serde(default = "rows")]
    pub max_rows: usize,
    #[serde(default = "bytes")]
    pub max_bytes: usize,
}
#[derive(Clone, Debug, PartialEq)]
pub struct UnnestSpec {
    pub expr: Expr,
    pub as_field: String,
    pub max_rows: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinMode {
    Inner,
    Left,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamJoinSpec {
    pub left_input: u32,
    pub right_input: u32,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    pub left_time: String,
    pub right_time: String,
    pub mode: JoinMode,
    #[serde(default)]
    pub before_micros: Option<i64>,
    #[serde(default)]
    pub after_micros: Option<i64>,
    #[serde(default)]
    pub window_size_micros: Option<i64>,
    #[serde(default = "left_prefix")]
    pub left_prefix: String,
    #[serde(default = "right_prefix")]
    pub right_prefix: String,
    #[serde(default = "rows")]
    pub max_rows_per_side: usize,
    #[serde(default = "rows")]
    pub max_matches_per_row: usize,
    #[serde(default = "bytes")]
    pub max_output_bytes_per_row: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AnalysisPlan {
    Unnest {
        spec: UnnestSpec,
        input: Schema,
        output: Schema,
        element: DataType,
    },
    Join {
        spec: StreamJoinSpec,
        left: Schema,
        right: Schema,
        output: Schema,
    },
}
impl AnalysisPlan {
    pub fn input(&self) -> &Schema {
        match self {
            Self::Unnest { input, .. } => input,
            Self::Join { left, .. } => left,
        }
    }
    pub fn output(&self) -> &Schema {
        match self {
            Self::Unnest { output, .. } | Self::Join { output, .. } => output,
        }
    }
    pub fn is_join(&self) -> bool {
        matches!(self, Self::Join { .. })
    }
    pub fn validate(&self) -> Result<()> {
        let rebuilt = match self {
            Self::Unnest { spec, input, .. } => Self::unnest(spec.clone(), input.clone())?,
            Self::Join {
                spec, left, right, ..
            } => Self::join(spec.clone(), left.clone(), right.clone())?,
        };
        if &rebuilt != self {
            return Err(invalid("analysis output schema/spec mismatch"));
        }
        Ok(())
    }
    pub fn unnest(spec: UnnestSpec, input: Schema) -> Result<Self> {
        if !(1..=4096).contains(&spec.max_rows)
            || !(1..=16 * 1024 * 1024).contains(&spec.max_bytes)
            || spec.as_field.is_empty()
            || spec.as_field.len() > 128
            || input.fields.len() > 128
        {
            return Err(invalid("UNNEST requires <=128 input fields, a name, max_rows=1..4096 and max_bytes=1..16MiB"));
        }
        let element = match infer_type(&spec.expr, &input)? {
            DataType::Array(item) => *item,
            DataType::Dynamic | DataType::Null => DataType::Dynamic,
            _ => return Err(invalid("UNNEST expression must be Array or Dynamic")),
        };
        let mut fields = input.fields.clone();
        fields.push(Field::new(
            (fields.len() + 1) as u16,
            &spec.as_field,
            element.clone(),
            true,
        ));
        for name in ["unnest_source", "unnest_input", "unnest_ordinal"] {
            fields.push(Field::new(
                (fields.len() + 1) as u16,
                name,
                DataType::Int64,
                name == "unnest_source",
            ));
        }
        // Re-number inputs so appended field IDs cannot collide with custom IDs.
        for (i, f) in fields.iter_mut().enumerate() {
            f.id = ((i + 1) as u16).into();
        }
        let output = Schema::new(input.id.raw().saturating_add(80), fields)?;
        Ok(Self::Unnest {
            spec,
            input,
            output,
            element,
        })
    }
    pub fn join(spec: StreamJoinSpec, left: Schema, right: Schema) -> Result<Self> {
        if spec.left_input == spec.right_input
            || !(1..=8).contains(&spec.left_keys.len())
            || spec.left_keys.len() != spec.right_keys.len()
            || left.fields.len() + right.fields.len() > 128
            || spec.left_prefix.is_empty()
            || spec.right_prefix.is_empty()
            || spec.left_prefix.len() > 64
            || spec.right_prefix.len() > 64
            || !(1..=4096).contains(&spec.max_rows_per_side)
            || !(1..=4096).contains(&spec.max_matches_per_row)
            || !(1..=16 * 1024 * 1024).contains(&spec.max_output_bytes_per_row)
        {
            return Err(invalid(
                "invalid bounded Join ports, keys, fields or resource limits",
            ));
        }
        match (spec.window_size_micros,spec.before_micros,spec.after_micros) {
            (Some(size),None,None) if size>0=>{},
            (None,Some(before),Some(after)) if before>=0&&after>=0&&before.checked_add(after).is_some()=>{},
            _=>return Err(invalid("Join requires either positive window_size_micros or explicit nonnegative before/after bounds")),
        }
        if spec
            .left_keys
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != spec.left_keys.len()
            || spec
                .right_keys
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != spec.right_keys.len()
        {
            return Err(invalid("join keys must be distinct on each side"));
        }
        for (lk, rk) in spec.left_keys.iter().zip(&spec.right_keys) {
            let l = left
                .field_by_name(lk)
                .ok_or_else(|| invalid("unknown left join key"))?;
            let r = right
                .field_by_name(rk)
                .ok_or_else(|| invalid("unknown right join key"))?;
            if l.data_type != r.data_type
                || l.data_type.is_nested()
                || matches!(l.data_type, DataType::Dynamic | DataType::Null)
            {
                return Err(invalid("join keys require identical scalar types"));
            }
        }
        for (schema, name) in [(&left, &spec.left_time), (&right, &spec.right_time)] {
            let f = schema
                .field_by_name(name)
                .ok_or_else(|| invalid("unknown join timestamp"))?;
            if f.nullable || !matches!(f.data_type, DataType::Int64 | DataType::TimestampMicrosUTC)
            {
                return Err(invalid(
                    "join time requires non-null Int64/TimestampMicrosUTC",
                ));
            }
        }
        let mut fields = Vec::new();
        for (schema, prefix, is_right) in [
            (&left, &spec.left_prefix, false),
            (&right, &spec.right_prefix, true),
        ] {
            for f in &schema.fields {
                fields.push(Field::new(
                    (fields.len() + 1) as u16,
                    format!("{prefix}{}", f.name),
                    f.data_type.clone(),
                    f.nullable || (is_right && spec.mode == JoinMode::Left),
                ));
            }
        }
        for name in ["join_time", "join_left_ordinal", "join_right_ordinal"] {
            fields.push(Field::new(
                (fields.len() + 1) as u16,
                name,
                DataType::Int64,
                name == "join_right_ordinal" && spec.mode == JoinMode::Left,
            ));
        }
        let output = Schema::new(left.id.raw().max(right.id.raw()).saturating_add(81), fields)?;
        Ok(Self::Join {
            spec,
            left,
            right,
            output,
        })
    }
}

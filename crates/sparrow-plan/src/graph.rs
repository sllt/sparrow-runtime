//! Versioned GraphSpec JSON. Front-end only; the binder turns this into
//! the same [`crate::bound::BoundLogicalPlan`] that SQL produces.

use serde::{Deserialize, Serialize};

use crate::expr_spec::ExprSpec;
use crate::stateful::IotSpec;
use sparrow_model::error::{ErrorCode, Result, SparrowError};

pub const GRAPH_SPEC_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSpec {
    pub version: u32,
    pub pipeline_id: u64,
    pub revision_id: u64,
    #[serde(default)]
    pub catalog: Vec<CatalogTableSpec>,
    pub nodes: Vec<NodeSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogTableSpec {
    pub name: String,
    pub fields: Vec<FieldSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<sparrow_expr::plugins::extension::Binding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unnest: Option<crate::UnnestNodeSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_join: Option<crate::StreamJoinSpec>,
    pub id: u32,
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub predicate: Option<ExprSpec>,
    #[serde(default)]
    pub exprs: Option<Vec<NamedExprSpec>>,
    #[serde(default)]
    pub out: Vec<u32>,
    #[serde(default)]
    pub window: Option<WindowNodeSpec>,
    #[serde(default)]
    pub keys: Option<Vec<String>>,
    #[serde(default)]
    pub aggs: Option<Vec<AggNodeSpec>>,
    #[serde(default)]
    pub ttl_micros: Option<i64>,
    #[serde(default)]
    pub max_keys: Option<usize>,
    #[serde(default)]
    pub on: Option<Vec<JoinOnSpec>>,
    #[serde(default)]
    pub keep: Option<Vec<String>>,
    #[serde(default)]
    pub event_time_field: Option<String>,
    #[serde(default)]
    pub lateness_micros: Option<i64>,
    #[serde(default)]
    pub temporal: Option<bool>,
    #[serde(default)]
    pub as_of_field: Option<String>,
    /// Parameters for the first bounded IoT state operators.  Keeping this
    /// as an optional nested object preserves old linear GraphSpec JSON when
    /// no IoT node is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iot: Option<IotSpec>,
    /// Explicit ordered routing cases; destinations must occur in `out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routes: Option<Vec<RouteSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_mode: Option<RouteMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_out: Option<u32>,
    /// Loss is an edge contract, not an inference from a slow consumer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub best_effort: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_output: Option<SideOutputSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub out_of_orderness_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_future_skew_micros: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideOutputKind {
    DecodeError,
    Late,
    RuleReject,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SideOutputSpec {
    pub kind: SideOutputKind,
    pub to: u32,
    /// Explicit finite-queue policy: required backpressure, or lossy drop.
    pub full: SideOutputFull,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideOutputFull {
    Backpressure,
    Drop,
}

pub fn decode_error_schema() -> sparrow_model::Schema {
    use sparrow_model::{DataType, Field, Schema};
    Schema::new(
        0x44454345,
        vec![
            Field::new(1, "source_operator", DataType::UInt64, false),
            Field::new(2, "error_code", DataType::Utf8, false),
        ],
    )
    .expect("static decode error schema")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteMode {
    FirstMatch,
    AllMatch,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSpec {
    pub predicate: ExprSpec,
    pub to: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowNodeSpec {
    pub kind: String,
    #[serde(default)]
    pub size_micros: Option<i64>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub slide_micros: Option<i64>,
    #[serde(default)]
    pub event_time_field: Option<String>,
    #[serde(default)]
    pub lateness_micros: Option<i64>,
    #[serde(default)]
    pub max_overlap: Option<u32>,
    #[serde(default)]
    pub max_future_skew_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_buffered_rows: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggNodeSpec {
    #[serde(rename = "fn")]
    pub func: String,
    #[serde(default)]
    pub expr: Option<ExprSpec>,
    pub alias: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinOnSpec {
    pub stream: String,
    pub table: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedExprSpec {
    pub expr: ExprSpec,
    pub alias: String,
}

impl GraphSpec {
    pub fn from_json(text: &str) -> Result<Self> {
        if text.len() > 64 * 1024 {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                "GraphSpec exceeds 64 KiB",
            ));
        }
        let spec: Self = serde_json::from_str(text).map_err(|e| {
            SparrowError::new(ErrorCode::InvalidArgument, format!("GraphSpec JSON: {e}"))
        })?;
        if spec.version != GRAPH_SPEC_VERSION {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!(
                    "GraphSpec version {} is not supported (want {GRAPH_SPEC_VERSION})",
                    spec.version
                ),
            ));
        }
        Ok(spec)
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| {
            SparrowError::new(ErrorCode::Internal, format!("serialize GraphSpec: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a8_graph_and_node_spec_deny_unknown_fields() {
        let err = GraphSpec::from_json(
            r#"{
              "version": 1,
              "pipeline_id": 1,
              "revision_id": 1,
              "experimental": true,
              "nodes": []
            }"#,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(
            err.message.contains("unknown field") || err.message.contains("experimental"),
            "{}",
            err.message
        );

        let err = GraphSpec::from_json(
            r#"{
              "version": 1,
              "pipeline_id": 1,
              "revision_id": 1,
              "nodes": [
                {"id": 1, "kind": "memory_source", "table": "s", "mystery": 1}
              ]
            }"#,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(
            err.message.contains("unknown field") || err.message.contains("mystery"),
            "{}",
            err.message
        );
    }
}

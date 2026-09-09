//! Versioned GraphSpec JSON. Front-end only; the binder turns this into
//! the same [`crate::bound::BoundLogicalPlan`] that SQL produces.

use serde::{Deserialize, Serialize};

use crate::expr_spec::ExprSpec;
use sparrow_model::error::{ErrorCode, Result, SparrowError};

pub const GRAPH_SPEC_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphSpec {
    pub version: u32,
    pub pipeline_id: u64,
    pub revision_id: u64,
    #[serde(default)]
    pub catalog: Vec<CatalogTableSpec>,
    pub nodes: Vec<NodeSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogTableSpec {
    pub name: String,
    pub fields: Vec<FieldSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
pub struct NodeSpec {
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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedExprSpec {
    pub expr: ExprSpec,
    pub alias: String,
}

impl GraphSpec {
    pub fn from_json(text: &str) -> Result<Self> {
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

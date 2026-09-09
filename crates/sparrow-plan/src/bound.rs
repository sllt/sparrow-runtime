//! Bound logical plan. Graph and SQL both produce this IR.

use sparrow_expr::{infer_type, Expr};
use sparrow_model::{OperatorId, PipelineId, RevisionId, Schema};

#[derive(Clone, Debug, PartialEq)]
pub struct BoundLogicalPlan {
    pub pipeline: PipelineId,
    pub revision: RevisionId,
    pub nodes: Vec<BoundNode>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundNode {
    pub id: OperatorId,
    pub kind: BoundKind,
    pub downstream: Vec<OperatorId>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BoundKind {
    MemorySource {
        name: String,
        schema: Schema,
    },
    Filter {
        predicate: Expr,
        input: Schema,
    },
    Project {
        exprs: Vec<Expr>,
        input: Schema,
        output: Schema,
    },
    Map {
        exprs: Vec<Expr>,
        input: Schema,
        output: Schema,
    },
    CaptureSink {
        name: String,
        schema: Schema,
    },
}

impl BoundKind {
    pub fn output_schema(&self) -> &Schema {
        match self {
            Self::MemorySource { schema, .. } => schema,
            Self::Filter { input, .. } => input,
            Self::Project { output, .. } | Self::Map { output, .. } => output,
            Self::CaptureSink { schema, .. } => schema,
        }
    }
}

pub fn validate_predicate(pred: &Expr, schema: &Schema) -> sparrow_model::Result<()> {
    let ty = infer_type(pred, schema)?;
    if !matches!(ty, sparrow_model::DataType::Bool) {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::TypeMismatch,
            format!("filter predicate must be bool, got {ty}"),
        ));
    }
    Ok(())
}

impl BoundLogicalPlan {
    pub fn source_schema(&self) -> Option<&Schema> {
        self.nodes.iter().find_map(|n| match &n.kind {
            BoundKind::MemorySource { schema, .. } => Some(schema),
            _ => None,
        })
    }

    pub fn sink_schema(&self) -> Option<&Schema> {
        self.nodes.iter().rev().find_map(|n| match &n.kind {
            BoundKind::CaptureSink { schema, .. } => Some(schema),
            _ => None,
        })
    }
}

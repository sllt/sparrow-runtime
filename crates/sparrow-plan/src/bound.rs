//! Bound logical plan. Graph and SQL both produce this IR.

use sparrow_expr::{infer_type, Expr};
use sparrow_model::{OperatorId, PipelineId, RevisionId, Schema};

use crate::stateful::{DedupSpec, IotSpec, LookupSpec, WindowSpec};

#[derive(Clone, Debug, PartialEq)]
pub struct BoundLogicalPlan {
    pub pipeline: PipelineId,
    pub revision: RevisionId,
    pub nodes: Vec<BoundNode>,
    pub side_outputs: Vec<(OperatorId, crate::graph::SideOutputSpec)>,
    pub source_times: Vec<(OperatorId, sparrow_model::EventTimeBinding)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundNode {
    pub id: OperatorId,
    pub kind: BoundKind,
    pub downstream: Vec<OperatorId>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BoundKind {
    Branch { input: Schema, best_effort: Vec<OperatorId> },
    Route {
        input: Schema,
        mode: crate::graph::RouteMode,
        cases: Vec<(Expr, OperatorId)>,
        default: OperatorId,
        best_effort: Vec<OperatorId>,
    },
    UnionAll { input: Schema },
    BestEffortSink { name: String, schema: Schema },
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
    WindowAgg {
        spec: WindowSpec,
        input: Schema,
        output: Schema,
    },
    Deduplicate {
        spec: DedupSpec,
        input: Schema,
    },
    Lookup {
        spec: LookupSpec,
        input: Schema,
        output: Schema,
    },
    /// Bounded keyed IoT value state. This is deliberately separate from
    /// WindowAgg: it has no window timestamps or aggregate accumulator.
    Iot { spec: IotSpec, input: Schema, output: Schema },
}

impl BoundKind {
    pub fn output_schema(&self) -> &Schema {
        match self {
            Self::Branch { input, .. } | Self::Route { input, .. } | Self::UnionAll { input } => input,
            Self::BestEffortSink { schema, .. } => schema,
            Self::MemorySource { schema, .. } => schema,
            Self::Filter { input, .. } => input,
            Self::Project { output, .. } | Self::Map { output, .. } => output,
            Self::CaptureSink { schema, .. } => schema,
            Self::WindowAgg { output, .. } | Self::Lookup { output, .. } => output,
            Self::Deduplicate { input, .. } => input,
            Self::Iot { output, .. } => output,
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

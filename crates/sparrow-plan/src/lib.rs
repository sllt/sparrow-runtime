//! Logical and physical plan IR shared by SQL and Graph.
//!
//! M1: GraphSpec JSON and SQL bind to [`BoundLogicalPlan`], then one
//! [`physicalize`] path produces an ExecutionChain. Compact/Performance
//! are budgets, not alternate plan dialects.

use sparrow_expr::Expr;
use sparrow_model::{OperatorId, PipelineId, RevisionId, Schema};

pub mod bind;
pub mod bound;
pub mod catalog;
pub mod expr_spec;
pub mod graph;
pub mod physical;

pub use bind::{bind_graph, bind_linear};
pub use bound::{BoundKind, BoundLogicalPlan, BoundNode};
pub use catalog::Catalog;
pub use graph::{GraphSpec, GRAPH_SPEC_VERSION};
pub use physical::{physicalize, PhysicalPlan, PhysicalStage, PlanOptions, TransformStep};

/// M0 linear stub, still used by the sync `LinearExecutor`.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalOp {
    Source {
        operator: OperatorId,
        name: String,
        schema: Schema,
    },
    Filter {
        operator: OperatorId,
        predicate: Expr,
    },
    Project {
        operator: OperatorId,
        exprs: Vec<Expr>,
        output: Schema,
    },
    Sink {
        operator: OperatorId,
        name: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan {
    pub pipeline: PipelineId,
    pub revision: RevisionId,
    pub nodes: Vec<LogicalOp>,
}

impl LogicalPlan {
    pub fn linear(
        pipeline: PipelineId,
        revision: RevisionId,
        source: LogicalOp,
        filter: Option<Expr>,
        project: Option<(Vec<Expr>, Schema)>,
        sink: LogicalOp,
    ) -> Self {
        let mut nodes = vec![source];
        if let Some(predicate) = filter {
            nodes.push(LogicalOp::Filter {
                operator: OperatorId::new(2),
                predicate,
            });
        }
        if let Some((exprs, output)) = project {
            nodes.push(LogicalOp::Project {
                operator: OperatorId::new(3),
                exprs,
                output,
            });
        }
        nodes.push(sink);
        Self {
            pipeline,
            revision,
            nodes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_expr::{BinaryOp, Expr};
    use sparrow_model::{DataType, Field, FieldId, Scalar, SchemaId};

    fn sensor_catalog() -> Catalog {
        let mut c = Catalog::new();
        c.insert(
            "sensor_readings",
            Schema::new(
                SchemaId::new(1),
                vec![
                    Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                    Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
                ],
            )
            .unwrap(),
        );
        c
    }

    #[test]
    fn graph_binds_and_fuses() {
        let spec = GraphSpec::from_json(
            r#"{
              "version": 1,
              "pipeline_id": 1,
              "revision_id": 1,
              "nodes": [
                {"id": 1, "kind": "memory_source", "table": "sensor_readings", "out": [2]},
                {"id": 2, "kind": "filter",
                  "predicate": {"k": "bin", "op": "gt",
                    "left": {"k": "col", "name": "temperature"},
                    "right": {"k": "lit", "value": {"t": "float64", "v": 25.0}}},
                  "out": [3]},
                {"id": 3, "kind": "project",
                  "exprs": [
                    {"alias": "device_id", "expr": {"k": "col", "name": "device_id"}},
                    {"alias": "temperature", "expr": {"k": "col", "name": "temperature"}}
                  ],
                  "out": [4]},
                {"id": 4, "kind": "capture_sink", "name": "capture"}
              ]
            }"#,
        )
        .unwrap();
        let bound = bind_graph(&spec, &sensor_catalog()).unwrap();
        let fused = physicalize(&bound, &PlanOptions { fuse: true });
        let unfused = physicalize(&bound, &PlanOptions { fuse: false });
        assert!(fused.fused());
        assert!(!unfused.fused());
        assert_eq!(fused.mailbox_count(), 2);
        assert_eq!(unfused.mailbox_count(), 3);
    }

    #[test]
    fn rejects_join_kind() {
        let spec = GraphSpec::from_json(
            r#"{
              "version": 1, "pipeline_id": 1, "revision_id": 1,
              "nodes": [
                {"id": 1, "kind": "join", "out": []}
              ]
            }"#,
        )
        .unwrap();
        let err = bind_graph(&spec, &sensor_catalog()).unwrap_err();
        assert_eq!(err.code, sparrow_model::ErrorCode::FeatureUnavailable);
    }

    #[test]
    fn linear_plan_has_source_and_sink() {
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "x", DataType::Int64, true)],
        )
        .unwrap();
        let plan = LogicalPlan::linear(
            PipelineId::new(1),
            RevisionId::new(1),
            LogicalOp::Source {
                operator: OperatorId::new(1),
                name: "sensors".into(),
                schema,
            },
            Some(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column { name: "x".into() }),
                right: Box::new(Expr::Literal(Scalar::Int64(0))),
            }),
            None,
            LogicalOp::Sink {
                operator: OperatorId::new(9),
                name: "capture".into(),
            },
        );
        assert_eq!(plan.nodes.len(), 3);
    }
}

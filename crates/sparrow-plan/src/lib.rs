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
pub mod compat;
pub mod explain;
pub mod expr_spec;
pub mod graph;
pub mod physical;
pub mod stateful;

pub use bind::{
    bind_dedup_linear, bind_graph, bind_linear, bind_lookup_linear, bind_window_linear,
};
pub use bound::{validate_predicate, BoundKind, BoundLogicalPlan, BoundNode};
pub use catalog::Catalog;
pub use explain::{
    et_tumble_template, explain_bound, explain_graph, validate_graph, GraphExplain, REPLAY_UNBOUND,
};
pub use graph::{GraphSpec, GRAPH_SPEC_VERSION};
pub use physical::{physicalize, PhysicalPlan, PhysicalStage, PlanOptions, TransformStep};
pub use compat::{
    decide_state_reuse, expr_fingerprint, where_before_window, where_before_window_physical,
    PlanLayout, StateReuse,
};
pub use stateful::{
    agg_result_type, lookup_output_schema, window_output_schema, AggCall, DedupSpec, LookupSpec,
    WindowSpec,
};

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
    use crate::stateful::{AggCall, WindowSpec};

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
        let project = bound
            .nodes
            .iter()
            .find_map(|n| match &n.kind {
                BoundKind::Project { output, .. } => Some(output),
                _ => None,
            })
            .expect("project");
        assert!(
            !project.field_by_name("device_id").unwrap().nullable,
            "p3_46: non-null input column projected as itself stays non-null"
        );
        assert!(project.field_by_name("temperature").unwrap().nullable);
    }

    #[test]
    fn p3_46_project_nullable_not_always_true() {
        let spec = GraphSpec::from_json(
            r#"{
              "version": 1,
              "pipeline_id": 1,
              "revision_id": 1,
              "nodes": [
                {"id": 1, "kind": "memory_source", "table": "sensor_readings", "out": [2]},
                {"id": 2, "kind": "project",
                  "exprs": [
                    {"alias": "device_id", "expr": {"k": "col", "name": "device_id"}},
                    {"alias": "one", "expr": {"k": "lit", "value": {"t": "int64", "v": 1}}}
                  ],
                  "out": [3]},
                {"id": 3, "kind": "capture_sink", "name": "capture"}
              ]
            }"#,
        )
        .unwrap();
        let bound = bind_graph(&spec, &sensor_catalog()).unwrap();
        let project = bound
            .nodes
            .iter()
            .find_map(|n| match &n.kind {
                BoundKind::Project { output, .. } => Some(output),
                _ => None,
            })
            .unwrap();
        assert!(!project.field_by_name("device_id").unwrap().nullable);
        assert!(!project.field_by_name("one").unwrap().nullable);
    }

    #[test]
    fn binds_event_time_tumble() {
        let mut c = sensor_catalog();
        c.insert(
            "sensor_readings",
            Schema::new(
                SchemaId::new(1),
                vec![
                    Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                    Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
                    Field::new(FieldId::new(3), "ts", DataType::Int64, false),
                ],
            )
            .unwrap(),
        );
        let spec = GraphSpec::from_json(
            r#"{
              "version": 1, "pipeline_id": 1, "revision_id": 1,
              "nodes": [
                {"id": 1, "kind": "memory_source", "table": "sensor_readings", "out": [2]},
                {"id": 2, "kind": "window_agg",
                  "keys": ["device_id"],
                  "event_time_field": "ts",
                  "lateness_micros": 3000000,
                  "window": {"kind": "tumble_et", "size_micros": 10000000},
                  "aggs": [{"fn": "avg", "expr": {"k": "col", "name": "temperature"}, "alias": "avg_t"}],
                  "out": [3]},
                {"id": 3, "kind": "capture_sink"}
              ]
            }"#,
        )
        .unwrap();
        let bound = bind_graph(&spec, &c).unwrap();
        assert!(matches!(
            bound.nodes.iter().find(|n| matches!(n.kind, BoundKind::WindowAgg { .. })),
            Some(_)
        ));
    }

    #[test]
    fn rejects_hop_overlap() {
        let spec = WindowSpec::new(
            sparrow_model::WindowKind::hopping_et(90_000_000, 10_000_000).unwrap(),
            vec!["device_id".into()],
            vec![AggCall::count_star("n")],
        )
        .event_time("ts", 0);
        assert_eq!(
            spec.validate().unwrap_err().code,
            sparrow_model::ErrorCode::BoundExceeded
        );
    }

    #[test]
    fn rejects_session_and_join() {
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

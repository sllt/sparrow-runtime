//! Logical plan IR shared by SQL and Graph front-ends.
//!
//! M0 is a linear stub: Source → Filter → Project → Sink. The binder and
//! optimizer arrive after G0/G1a freeze. One engine; Compact/Performance
//! are budgets, not alternate plan dialects.

use sparrow_expr::Expr;
use sparrow_model::{OperatorId, PipelineId, RevisionId, Schema};

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
    use sparrow_model::{DataType, Field, FieldId, SchemaId};

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
            None,
            None,
            LogicalOp::Sink {
                operator: OperatorId::new(9),
                name: "capture".into(),
            },
        );
        assert_eq!(plan.nodes.len(), 2);
    }
}

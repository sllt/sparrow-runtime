//! Sync linear executor kept for the M0 smoke demo.
//!
//! Executes a linear Source → Filter → Project → Sink plan against
//! already-typed batches. Job failure is attributed with pipeline /
//! attempt / operator context. Not a distributed scheduler.

use std::sync::Arc;

use sparrow_expr::{bind, eval_bound, BoundExpr};
use sparrow_io::RecordSink;
use sparrow_model::{
    CreditKind, ErrorCode, JobAttemptId, MemoryOwner, OperatorId, PipelineId, Result, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SparrowError,
};
use sparrow_plan::{LogicalOp, LogicalPlan};

pub struct RuntimeConfig {
    pub pipeline: PipelineId,
    pub attempt: JobAttemptId,
    pub owner: Arc<MemoryOwner>,
    pub credit_kind: CreditKind,
}

pub struct LinearExecutor {
    cfg: RuntimeConfig,
    filter: Option<BoundExpr>,
    project: Option<(Vec<BoundExpr>, Schema)>,
    filter_op: OperatorId,
    project_op: OperatorId,
}

impl LinearExecutor {
    pub fn from_plan(cfg: RuntimeConfig, plan: &LogicalPlan) -> Result<Self> {
        let mut filter = None;
        let mut project = None;
        let mut filter_op = OperatorId::new(0);
        let mut project_op = OperatorId::new(0);
        let mut source_schema: Option<&Schema> = None;
        for node in &plan.nodes {
            match node {
                LogicalOp::Source { schema, .. } => {
                    source_schema = Some(schema);
                }
                LogicalOp::Filter {
                    operator,
                    predicate,
                } => {
                    let schema = source_schema.ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "filter requires a source schema to bind columns",
                        )
                    })?;
                    filter = Some(bind(predicate, schema)?);
                    filter_op = *operator;
                }
                LogicalOp::Project {
                    operator,
                    exprs,
                    output,
                } => {
                    let schema = source_schema.ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "project requires a source schema to bind columns",
                        )
                    })?;
                    let bound = exprs
                        .iter()
                        .map(|e| bind(e, schema))
                        .collect::<Result<Vec<_>>>()?;
                    project = Some((bound, output.clone()));
                    project_op = *operator;
                }
                LogicalOp::Sink { .. } => {}
            }
        }
        Ok(Self {
            cfg,
            filter,
            project,
            filter_op,
            project_op,
        })
    }

    pub fn run_batch(&self, input: RowBatch) -> Result<RowBatch> {
        let schema = input.schema().clone();
        let kept: Result<Vec<Row>> = input
            .rows()
            .iter()
            .filter_map(|row| match self.keep(&row.values) {
                Ok(true) => match self.project_row(&row.values) {
                    Ok(values) => Some(Ok(Row { values })),
                    Err(e) => Some(Err(e)),
                },
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            })
            .collect();
        let kept = kept?;
        let out_schema = self
            .project
            .as_ref()
            .map(|(_, s)| s.clone())
            .unwrap_or(schema);
        let max_rows = self.cfg.owner.budget().max_rows.max(1);
        let max_bytes = self
            .cfg
            .owner
            .budget()
            .cap(self.cfg.credit_kind)
            .min(input.tracked_bytes().saturating_mul(2).max(64));
        let mut builder = RowBatchBuilder::new(
            Arc::new(out_schema),
            Arc::clone(&self.cfg.owner),
            self.cfg.credit_kind,
            max_rows,
            max_bytes.max(64),
        )
        .map_err(|e| self.attr(e, self.project_op))?;
        for row in kept {
            builder.push(row).map_err(|e| self.attr(e, self.project_op))?;
        }
        builder.finish().map_err(|e| self.attr(e, self.project_op))
    }

    fn keep(&self, row: &[Scalar]) -> Result<bool> {
        let Some(pred) = &self.filter else {
            return Ok(true);
        };
        match eval_bound(pred, row).map_err(|e| self.attr(e, self.filter_op))? {
            Scalar::Bool(v) => Ok(v),
            Scalar::Null => Ok(false),
            other => Err(self.attr(
                SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!("filter must be bool, got {}", other.data_type()),
                ),
                self.filter_op,
            )),
        }
    }

    fn project_row(&self, row: &[Scalar]) -> Result<Vec<Scalar>> {
        let Some((exprs, _)) = &self.project else {
            return Ok(row.to_vec());
        };
        exprs
            .iter()
            .map(|e| eval_bound(e, row).map_err(|err| self.attr(err, self.project_op)))
            .collect()
    }

    fn attr(&self, err: SparrowError, operator: OperatorId) -> SparrowError {
        err.at_job(self.cfg.pipeline, self.cfg.attempt)
            .at_operator(operator)
    }
}

/// Drive batches through an executor into a sink.
pub fn drain<S: RecordSink>(
    exec: &LinearExecutor,
    batches: impl IntoIterator<Item = RowBatch>,
    sink: &mut S,
) -> Result<usize> {
    let mut rows = 0usize;
    for batch in batches {
        rows += batch.num_rows();
        let out = exec.run_batch(batch)?;
        sink.send(out)?;
    }
    sink.flush()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_expr::{BinaryOp, Expr};
    use sparrow_model::{
        DataType, DeliveryContract, Field, FieldId, MemoryOwner, ResourceBudget, RestoreClaim,
        RevisionId, SchemaId,
    };
    use sparrow_plan::LogicalPlan;

    struct VecSink(Vec<RowBatch>);
    impl RecordSink for VecSink {
        fn send(&mut self, batch: RowBatch) -> Result<()> {
            self.0.push(batch);
            Ok(())
        }
    }

    #[test]
    fn filter_project_and_reject_restore() {
        assert!(DeliveryContract::V0_1
            .validate_restore(&RestoreClaim::Checkpoint {
                snapshot_id: "snap-1".into()
            })
            .is_err());

        let in_schema = Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "temp", DataType::Float64, true),
                Field::new(FieldId::new(2), "id", DataType::Int64, false),
            ],
        )
        .unwrap();
        let out_schema = Schema::new(
            SchemaId::new(2),
            vec![Field::new(FieldId::new(2), "id", DataType::Int64, false)],
        )
        .unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut builder = RowBatchBuilder::new(
            Arc::new(in_schema.clone()),
            Arc::clone(&owner),
            CreditKind::Reservation,
            8,
            1024,
        )
        .unwrap();
        builder
            .push(Row {
                values: vec![Scalar::Float64(30.0), Scalar::Int64(1)],
            })
            .unwrap();
        builder
            .push(Row {
                values: vec![Scalar::Float64(10.0), Scalar::Int64(2)],
            })
            .unwrap();
        let batch = builder.finish().unwrap();

        let plan = LogicalPlan::linear(
            PipelineId::new(9),
            RevisionId::new(1),
            LogicalOp::Source {
                operator: OperatorId::new(1),
                name: "s".into(),
                schema: in_schema,
            },
            Some(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column {
                    name: "temp".into(),
                }),
                right: Box::new(Expr::Literal(Scalar::Float64(20.0))),
            }),
            Some((
                vec![Expr::Column { name: "id".into() }],
                out_schema,
            )),
            LogicalOp::Sink {
                operator: OperatorId::new(4),
                name: "c".into(),
            },
        );
        let exec = LinearExecutor::from_plan(
            RuntimeConfig {
                pipeline: PipelineId::new(9),
                attempt: JobAttemptId::new(1),
                owner,
                credit_kind: CreditKind::Reservation,
            },
            &plan,
        )
        .unwrap();
        let mut sink = VecSink(Vec::new());
        drain(&exec, [batch], &mut sink).unwrap();
        assert_eq!(sink.0[0].num_rows(), 1);
        assert_eq!(sink.0[0].rows()[0].values[0], Scalar::Int64(1));
    }
}

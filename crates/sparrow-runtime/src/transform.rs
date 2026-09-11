//! Shared Filter / Project / Map kernels used by fused and unfused stages.

use std::sync::Arc;

use sparrow_expr::{bind, eval_bound, BoundExpr};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryOwner, Result, Row, RowBatch, RowBatchBuilder, Scalar, Schema,
    SparrowError, WorkBudget,
};
use sparrow_plan::TransformStep;

pub fn apply_steps(
    batch: &RowBatch,
    steps: &[TransformStep],
    owner: &Arc<MemoryOwner>,
    work: &WorkBudget,
) -> Result<Option<RowBatch>> {
    CompiledTransform::new(steps)?.apply(batch, owner, work)
}

enum CompiledStep {
    Filter(BoundExpr),
    Project(Vec<BoundExpr>),
}

#[cfg(test)]
mod r3_tests {
    use super::*;
    use sparrow_model::{DataType, Field, FieldId, OperatorId, ResourceBudget};
    use sparrow_expr::Expr;

    #[test]
    fn source_batches_move_rows_share_schema_and_release_failed_leases() {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let schema = Arc::new(Schema::new(1, vec![
            Field::new(FieldId::new(1), "n", DataType::Int64, false),
        ]).unwrap());
        let rows: Vec<_> = (0..3).map(|n| Row { values: vec![Scalar::Int64(n)] }).collect();
        let pointers: Vec<_> = rows.iter().map(|r| r.values.as_ptr()).collect();
        let batches = build_source_batches_shared(schema.clone(), rows, &owner, 2).unwrap();
        assert_eq!(batches.iter().map(RowBatch::num_rows).collect::<Vec<_>>(), vec![2, 1]);
        for batch in &batches {
            assert!(std::ptr::eq(batch.schema(), schema.as_ref()));
        }
        assert_eq!(batches.iter().flat_map(|b| b.rows()).map(|r| r.values.as_ptr()).collect::<Vec<_>>(), pointers);
        drop(batches);
        assert_eq!(owner.usage().physical_bytes, 0);
        let rows = vec![Row { values: vec![Scalar::Int64(1)] }, Row { values: vec![Scalar::Bool(true)] }];
        assert!(build_source_batches_shared(schema, rows, &owner, 1).is_err());
        assert_eq!(owner.usage().physical_bytes, 0, "failed later batches must release earlier leases");
    }

    #[test]
    fn r3_projection_expansion_uses_budget_not_input_multiple() {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let input = Schema::new(1, vec![Field::new(FieldId::new(1), "n", DataType::Int64, false)]).unwrap();
        let output = Schema::new(2, vec![Field::new(FieldId::new(1), "s", DataType::Utf8, false)]).unwrap();
        let compiled = CompiledTransform::new(&[TransformStep::Project {
            operator: OperatorId::new(1), input: input.clone(), output,
            exprs: vec![Expr::Literal(Scalar::utf8("x".repeat(512)))],
        }]).unwrap();
        for _ in 0..3 {
            let batches = build_source_batches(input.clone(), vec![Row { values: vec![Scalar::Int64(1)] }], &owner, 1).unwrap();
            let out = compiled.apply(&batches[0], &owner, &WorkBudget::new(100)).unwrap().unwrap();
            assert_eq!(out.rows()[0].values[0], Scalar::utf8("x".repeat(512)));
        }
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

/// Construct once per stage, not once per batch. Fused steps retain only
/// one row of intermediate values, not a shadow batch for each step.
pub struct CompiledTransform {
    steps: Vec<CompiledStep>,
    output: Option<Arc<Schema>>,
}

impl CompiledTransform {
    pub fn work_units(&self, rows: usize) -> u64 {
        let per_row: u64 = self.steps.iter().map(|step| match step {
            CompiledStep::Filter(_) => 1,
            CompiledStep::Project(exprs) => exprs.len().max(1) as u64,
        }).sum();
        per_row.saturating_mul(rows as u64)
    }

    pub fn new(steps: &[TransformStep]) -> Result<Self> {
        let mut compiled = Vec::new();
        let mut output = None;
        for step in steps {
            match step {
                TransformStep::Filter { predicate, input, .. } => {
                    compiled.push(CompiledStep::Filter(bind(predicate, input)?));
                    output = Some(Arc::new(input.clone()));
                }
                TransformStep::Project { exprs, input, output: schema, .. }
                | TransformStep::Map { exprs, input, output: schema, .. } => {
                    compiled.push(CompiledStep::Project(exprs.iter().map(|e| bind(e, input)).collect::<Result<_>>()?));
                    output = Some(Arc::new(schema.clone()));
                }
            }
        }
        Ok(Self { steps: compiled, output })
    }

    pub fn apply(&self, batch: &RowBatch, owner: &Arc<MemoryOwner>, work: &WorkBudget) -> Result<Option<RowBatch>> {
        let Some(schema) = &self.output else { return Ok(None); };
        let mut builder = RowBatchBuilder::new(Arc::clone(schema), Arc::clone(owner),
            CreditKind::Reservation, owner.budget().max_rows,
            owner.budget().reservation_bytes)?;
        'rows: for row in batch.rows() {
            let mut values = std::borrow::Cow::Borrowed(row.values.as_slice());
            for step in &self.steps {
                match step {
                    CompiledStep::Filter(predicate) => {
                        work.consume(1)?;
                        match eval_bound(predicate, &values)? {
                            Scalar::Bool(true) => {}
                            Scalar::Bool(false) | Scalar::Null => continue 'rows,
                            other => return Err(SparrowError::new(ErrorCode::TypeMismatch,
                                format!("filter must be bool, got {}", other.data_type()))),
                        }
                    }
                    CompiledStep::Project(exprs) => {
                        work.consume(exprs.len().max(1) as u64)?;
                        let next = exprs.iter().map(|e| eval_bound(e, &values)).collect::<Result<Vec<_>>>()?;
                        values = std::borrow::Cow::Owned(next);
                    }
                }
            }
            builder.push(Row { values: values.into_owned() })?;
        }
        finish_step(builder)
    }
}

fn finish_step(b: RowBatchBuilder) -> Result<Option<RowBatch>> {
    if b.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(b.finish()?))
}

pub fn build_source_batches(
    schema: Schema,
    rows: Vec<Row>,
    owner: &Arc<MemoryOwner>,
    rows_per_batch: usize,
) -> Result<Vec<RowBatch>> {
    build_source_batches_shared(Arc::new(schema), rows, owner, rows_per_batch)
}

pub(crate) fn build_source_batches_shared(
    schema: Arc<Schema>,
    rows: Vec<Row>,
    owner: &Arc<MemoryOwner>,
    rows_per_batch: usize,
) -> Result<Vec<RowBatch>> {
    if rows_per_batch == 0 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "rows_per_batch must be > 0",
        ));
    }
    let mut out = Vec::new();
    let mut rows = rows.into_iter();
    while rows.len() > 0 {
        let chunk_len = rows.len().min(rows_per_batch);
        let mut b = RowBatchBuilder::new(
            Arc::clone(&schema),
            Arc::clone(owner),
            CreditKind::Reservation,
            chunk_len,
            owner
                .budget()
                .cap(CreditKind::Reservation)
                .min(64 * 1024)
                .max(64),
        )?;
        for row in rows.by_ref().take(chunk_len) {
            b.push(row)?;
        }
        out.push(b.finish()?);
    }
    Ok(out)
}

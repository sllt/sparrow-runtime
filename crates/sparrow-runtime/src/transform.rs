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

/// Width of a whole-chain positional identity projection, if any.
///
/// Every step must be a Project of the same non-zero width, and every
/// expression must read the column at its own output position, so applying the
/// chain is exactly a re-labelling of the row values. Filters, computed
/// expressions, dropped/reordered columns and mixed widths are not identity.
/// This is a static property of the compiled chain; the batch helper only
/// re-checks shape and ownership.
fn identity_projection_width(steps: &[CompiledStep]) -> Option<usize> {
    let mut width = None;
    for step in steps {
        let CompiledStep::Project(exprs) = step else {
            return None;
        };
        if exprs.is_empty() || *width.get_or_insert(exprs.len()) != exprs.len() {
            return None;
        }
        for (position, expr) in exprs.iter().enumerate() {
            match expr {
                BoundExpr::Column { index } if *index == position => {}
                _ => return None,
            }
        }
    }
    width
}

#[cfg(test)]
mod r3_tests {
    use super::*;
    use sparrow_expr::Expr;
    use sparrow_model::{DataType, Field, FieldId, OperatorId, ResourceBudget};

    #[test]
    fn r10_fused_wide_shared_projection_has_linear_scratch() {
        let owner = MemoryOwner::new(ResourceBudget {
            reservation_bytes: 64 * 1024,
            ..ResourceBudget::compact()
        });
        let schema = Schema::new(
            1,
            (0..20)
                .map(|i| Field::new((i + 1) as u16, format!("v{i}"), DataType::Utf8, false))
                .collect(),
        )
        .unwrap();
        let exprs: Vec<_> = (0..20)
            .map(|i| Expr::Column {
                name: format!("v{i}"),
            })
            .collect();
        let project = TransformStep::Project {
            operator: 1.into(),
            input: schema.clone(),
            output: schema.clone(),
            exprs,
        };
        let compiled = CompiledTransform::new(&[project.clone(), project]).unwrap();
        let batches = build_source_batches(
            schema,
            vec![Row {
                values: (0..20).map(|_| Scalar::utf8("shared-text")).collect(),
            }],
            &owner,
            1,
        )
        .unwrap();
        let out = compiled
            .apply(&batches[0], &owner, &WorkBudget::new(1000))
            .expect("fused projections must not multiply row bytes by width at every step")
            .unwrap();
        assert_eq!(out.rows(), batches[0].rows());
    }

    #[test]
    fn production_evaluator_scratch_rejects_before_large_literal_projection() {
        let schema = Schema::new(1, vec![Field::new(1, "n", DataType::Int64, false)]).unwrap();
        let owner = MemoryOwner::new(ResourceBudget {
            reservation_bytes: 4096,
            ..ResourceBudget::compact()
        });
        let input = build_source_batches(
            schema.clone(),
            vec![Row {
                values: vec![Scalar::Int64(1)],
            }],
            &owner,
            1,
        )
        .unwrap();
        let plan = CompiledTransform::new(&[TransformStep::Project {
            operator: OperatorId::new(1),
            input: schema,
            output: Schema::new(2, vec![Field::new(1, "s", DataType::Utf8, false)]).unwrap(),
            exprs: vec![Expr::Literal(Scalar::utf8("x".repeat(16 * 1024)))],
        }])
        .unwrap();
        let before = owner.usage().physical_bytes;
        assert_eq!(
            plan.apply(&input[0], &owner, &WorkBudget::new(100))
                .unwrap_err()
                .code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(owner.usage().physical_bytes, before);
        drop(input);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn source_batches_move_rows_share_schema_and_release_failed_leases() {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let schema = Arc::new(
            Schema::new(
                1,
                vec![Field::new(FieldId::new(1), "n", DataType::Int64, false)],
            )
            .unwrap(),
        );
        let rows: Vec<_> = (0..3)
            .map(|n| Row {
                values: vec![Scalar::Int64(n)],
            })
            .collect();
        let pointers: Vec<_> = rows.iter().map(|r| r.values.as_ptr()).collect();
        let batches = build_source_batches_shared(schema.clone(), rows, &owner, 2).unwrap();
        assert_eq!(
            batches.iter().map(RowBatch::num_rows).collect::<Vec<_>>(),
            vec![2, 1]
        );
        for batch in &batches {
            assert!(std::ptr::eq(batch.schema(), schema.as_ref()));
        }
        assert_eq!(
            batches
                .iter()
                .flat_map(|b| b.rows())
                .map(|r| r.values.as_ptr())
                .collect::<Vec<_>>(),
            pointers
        );
        drop(batches);
        assert_eq!(owner.usage().physical_bytes, 0);
        let rows = vec![
            Row {
                values: vec![Scalar::Int64(1)],
            },
            Row {
                values: vec![Scalar::Bool(true)],
            },
        ];
        assert!(build_source_batches_shared(schema, rows, &owner, 1).is_err());
        assert_eq!(
            owner.usage().physical_bytes,
            0,
            "failed later batches must release earlier leases"
        );
    }

    #[test]
    fn r3_projection_expansion_uses_budget_not_input_multiple() {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let input = Schema::new(
            1,
            vec![Field::new(FieldId::new(1), "n", DataType::Int64, false)],
        )
        .unwrap();
        let output = Schema::new(
            2,
            vec![Field::new(FieldId::new(1), "s", DataType::Utf8, false)],
        )
        .unwrap();
        let compiled = CompiledTransform::new(&[TransformStep::Project {
            operator: OperatorId::new(1),
            input: input.clone(),
            output,
            exprs: vec![Expr::Literal(Scalar::utf8("x".repeat(512)))],
        }])
        .unwrap();
        for _ in 0..3 {
            let batches = build_source_batches(
                input.clone(),
                vec![Row {
                    values: vec![Scalar::Int64(1)],
                }],
                &owner,
                1,
            )
            .unwrap();
            let out = compiled
                .apply(&batches[0], &owner, &WorkBudget::new(100))
                .unwrap()
                .unwrap();
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
    scratch: Vec<Vec<sparrow_expr::allocation::AllocationBound>>,
    max_width: usize,
    /// Set only when the whole chain is a positional identity projection, i.e.
    /// the shared-batch path may re-label the rows instead of re-encoding them.
    identity_width: Option<usize>,
}

impl CompiledTransform {
    pub fn work_units(&self, rows: usize) -> u64 {
        let per_row: u64 = self
            .steps
            .iter()
            .map(|step| match step {
                CompiledStep::Filter(_) => 1,
                CompiledStep::Project(exprs) => exprs.len().max(1) as u64,
            })
            .sum();
        per_row.saturating_mul(rows as u64)
    }

    pub fn new(steps: &[TransformStep]) -> Result<Self> {
        let mut compiled = Vec::new();
        let mut output = None;
        for step in steps {
            match step {
                TransformStep::Filter {
                    predicate, input, ..
                } => {
                    compiled.push(CompiledStep::Filter(bind(predicate, input)?));
                    output = Some(Arc::new(input.clone()));
                }
                TransformStep::Project {
                    exprs,
                    input,
                    output: schema,
                    ..
                }
                | TransformStep::Map {
                    exprs,
                    input,
                    output: schema,
                    ..
                } => {
                    compiled.push(CompiledStep::Project(
                        exprs
                            .iter()
                            .map(|e| bind(e, input))
                            .collect::<Result<_>>()?,
                    ));
                    output = Some(Arc::new(schema.clone()));
                }
            }
        }
        let scratch = compiled
            .iter()
            .map(|step| match step {
                CompiledStep::Filter(e) => {
                    vec![sparrow_expr::allocation::AllocationBound::for_expr(e)]
                }
                CompiledStep::Project(exprs) => exprs
                    .iter()
                    .map(sparrow_expr::allocation::AllocationBound::for_expr)
                    .collect(),
            })
            .collect();
        let max_width = compiled
            .iter()
            .filter_map(|step| match step {
                CompiledStep::Project(e) => Some(e.len()),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        // The compiled chain proves every expression reads its own position;
        // the declared step schemas must also agree that no column is dropped,
        // reordered or added at any step, so a narrow Project over a wider input
        // is not a complete positional projection. Cold construction only.
        let identity_width = identity_projection_width(&compiled).filter(|width| {
            steps.iter().all(|step| match step {
                TransformStep::Filter { .. } => false,
                TransformStep::Project { input, output, .. }
                | TransformStep::Map { input, output, .. } => {
                    input.fields.len() == *width && output.fields.len() == *width
                }
            })
        });
        Ok(Self {
            steps: compiled,
            output,
            scratch,
            max_width,
            identity_width,
        })
    }

    pub fn apply(
        &self,
        batch: &RowBatch,
        owner: &Arc<MemoryOwner>,
        work: &WorkBudget,
    ) -> Result<Option<RowBatch>> {
        let Some(schema) = &self.output else {
            return Ok(None);
        };
        // A whole-chain positional identity projection of a non-empty batch only
        // re-labels the rows: values, layout and the input lease are reused
        // instead of re-encoded. The helper re-checks shape/owner only, and the
        // work quantum below is exactly the generic path's per-row/per-step
        // quantum, so previews, remaining and failure semantics do not change.
        if let Some(width) = self.identity_width {
            if batch.num_rows() > 0
                && width == batch.schema().fields.len()
                && width == schema.fields.len()
            {
                if let Some(shared) = batch.try_identity_projection(Arc::clone(schema), owner) {
                    for _row in batch.rows() {
                        for _step in &self.steps {
                            work.consume(width.max(1) as u64)?;
                        }
                    }
                    return Ok(Some(shared));
                }
            }
        }
        // One row of intermediates at a time, but admit before evaluator Vec,
        // CAST/case-mapping allocations. Credit survives builder adoption.
        let width = self.max_width.max(batch.schema().fields.len());
        let metadata = width
            .saturating_mul(2 * std::mem::size_of::<usize>())
            .saturating_add(128);
        let mut scratch = owner.acquire(CreditKind::Reservation, metadata)?;
        let mut columns = Vec::with_capacity(width);
        columns.resize(batch.schema().fields.len(), 0);
        for row in batch.rows() {
            for (size, value) in columns.iter_mut().zip(&row.values) {
                *size = (*size).max(value.resident_bytes());
            }
        }
        let mut next = Vec::with_capacity(width);
        let mut previous = 0usize;
        let mut peak = 0usize;
        for (step, bounds) in self.steps.iter().zip(&self.scratch) {
            let mut allocated = 0usize;
            next.clear();
            for bound in bounds {
                let e = bound.estimate(&columns);
                allocated = allocated.saturating_add(e.allocated);
                next.push(e.value);
            }
            match step {
                CompiledStep::Filter(_) => peak = peak.max(previous.saturating_add(allocated)),
                CompiledStep::Project(_) => {
                    let size = next.iter().fold(64usize, |n, v| n.saturating_add(*v));
                    peak = peak.max(previous.saturating_add(size).saturating_add(allocated));
                    previous = size;
                    std::mem::swap(&mut columns, &mut next);
                }
            }
        }
        scratch.grow_to(metadata.saturating_add(peak))?;
        let mut builder = RowBatchBuilder::new(
            Arc::clone(schema),
            Arc::clone(owner),
            CreditKind::Reservation,
            owner.budget().max_rows,
            owner.budget().reservation_bytes,
        )?;
        'rows: for row in batch.rows() {
            let mut values = std::borrow::Cow::Borrowed(row.values.as_slice());
            for step in &self.steps {
                match step {
                    CompiledStep::Filter(predicate) => {
                        work.consume(1)?;
                        match eval_bound(predicate, &values)? {
                            Scalar::Bool(true) => {}
                            Scalar::Bool(false) | Scalar::Null => continue 'rows,
                            other => {
                                return Err(SparrowError::new(
                                    ErrorCode::TypeMismatch,
                                    format!("filter must be bool, got {}", other.data_type()),
                                ))
                            }
                        }
                    }
                    CompiledStep::Project(exprs) => {
                        work.consume(exprs.len().max(1) as u64)?;
                        let next = exprs
                            .iter()
                            .map(|e| eval_bound(e, &values))
                            .collect::<Result<Vec<_>>>()?;
                        values = std::borrow::Cow::Owned(next);
                    }
                }
            }
            let row = Row {
                values: values.into_owned(),
            };
            let bytes = row.resident_bytes().saturating_add(64);
            builder.push_accounted(row, bytes)?;
        }
        Ok(finish_step(builder)?.map(|out| out.with_origin(batch.origin())))
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

#[cfg(test)]
#[path = "identity_projection_tests.rs"]
mod identity_projection_tests;

//! Shared Filter / Project / Map kernels used by fused and unfused stages.

use std::sync::Arc;

use sparrow_expr::{eval, filter_mask};
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
    if steps.is_empty() {
        return Ok(None);
    }
    // Each step builds into a charged [`RowBatchBuilder`]. No unaccounted
    // `to_vec()` of the whole batch (A2 / P1-16).
    let mut working: Option<RowBatch> = None;
    for step in steps {
        match step {
            TransformStep::Filter {
                predicate, input, ..
            } => {
                let src = working.as_ref().unwrap_or(batch);
                work.consume(src.num_rows() as u64)?;
                let mask = filter_mask(predicate, input, src.rows())?;
                let mut b = step_builder(input.clone(), owner, src.num_rows(), src.tracked_bytes())?;
                for (row, keep) in src.rows().iter().zip(mask) {
                    if keep {
                        b.push(row.clone())?;
                    }
                }
                working = finish_step(b)?;
                if working.is_none() {
                    return Ok(None);
                }
            }
            TransformStep::Project {
                exprs,
                input,
                output,
                ..
            }
            | TransformStep::Map {
                exprs,
                input,
                output,
                ..
            } => {
                let src = working.as_ref().unwrap_or(batch);
                work.consume(src.num_rows() as u64 * exprs.len().max(1) as u64)?;
                let mut b = step_builder(
                    output.clone(),
                    owner,
                    src.num_rows(),
                    src.tracked_bytes().saturating_mul(2).max(64),
                )?;
                for row in src.rows() {
                    let values: Result<Vec<Scalar>> =
                        exprs.iter().map(|e| eval(e, input, &row.values)).collect();
                    b.push(Row { values: values? })?;
                }
                working = finish_step(b)?;
                if working.is_none() {
                    return Ok(None);
                }
            }
        }
    }
    Ok(working)
}

fn step_builder(
    schema: Schema,
    owner: &Arc<MemoryOwner>,
    rows_hint: usize,
    bytes_hint: usize,
) -> Result<RowBatchBuilder> {
    let max_rows = owner.budget().max_rows.max(rows_hint).max(1);
    let max_bytes = owner
        .budget()
        .cap(CreditKind::Reservation)
        .min(bytes_hint.saturating_mul(2).max(64));
    RowBatchBuilder::new(
        Arc::new(schema),
        Arc::clone(owner),
        CreditKind::Reservation,
        max_rows,
        max_bytes.max(64),
    )
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
    if rows_per_batch == 0 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "rows_per_batch must be > 0",
        ));
    }
    let schema = Arc::new(schema);
    let mut out = Vec::new();
    for chunk in rows.chunks(rows_per_batch) {
        let mut b = RowBatchBuilder::new(
            Arc::clone(&schema),
            Arc::clone(owner),
            CreditKind::Reservation,
            chunk.len().max(1),
            owner
                .budget()
                .cap(CreditKind::Reservation)
                .min(64 * 1024)
                .max(64),
        )?;
        for row in chunk {
            b.push(row.clone())?;
        }
        out.push(b.finish()?);
    }
    Ok(out)
}

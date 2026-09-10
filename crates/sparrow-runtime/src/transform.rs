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
    let mut working: Option<Vec<Row>> = None;
    let mut schema = batch.schema().clone();
    for step in steps {
        match step {
            TransformStep::Filter {
                predicate, input, ..
            } => {
                let src = working.as_deref().unwrap_or(batch.rows());
                work.consume(src.len() as u64)?;
                let mask = filter_mask(predicate, input, src)?;
                let next: Vec<Row> = src
                    .iter()
                    .zip(mask)
                    .filter_map(|(r, keep)| keep.then(|| r.clone()))
                    .collect();
                working = Some(next);
                schema = input.clone();
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
                let src = working.as_deref().unwrap_or(batch.rows());
                work.consume(src.len() as u64 * exprs.len().max(1) as u64)?;
                let mut next = Vec::with_capacity(src.len());
                for row in src {
                    let values: Result<Vec<Scalar>> =
                        exprs.iter().map(|e| eval(e, input, &row.values)).collect();
                    next.push(Row { values: values? });
                }
                working = Some(next);
                schema = output.clone();
            }
        }
    }
    let Some(rows) = working else {
        return Ok(None);
    };
    if rows.is_empty() {
        return Ok(None);
    }
    build_batch(schema, rows, owner)
}

fn build_batch(schema: Schema, rows: Vec<Row>, owner: &Arc<MemoryOwner>) -> Result<Option<RowBatch>> {
    let max_rows = owner.budget().max_rows.max(rows.len()).max(1);
    let bytes: usize = rows.iter().map(Row::tracked_bytes).sum();
    let max_bytes = owner
        .budget()
        .cap(CreditKind::Reservation)
        .min(bytes.saturating_mul(2).max(64));
    let mut b = RowBatchBuilder::new(
        Arc::new(schema),
        Arc::clone(owner),
        CreditKind::Reservation,
        max_rows,
        max_bytes.max(64),
    )?;
    for row in rows {
        b.push(row)?;
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

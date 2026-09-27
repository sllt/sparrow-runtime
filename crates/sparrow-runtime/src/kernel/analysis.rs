//! Cold, credit-owned continuation paths for expansion and two-input joins.
use super::*;
use crate::bounded_join::BoundedJoin;
use sparrow_model::{CreditKind, DataType, DynamicValue, Scalar};
use sparrow_plan::AnalysisPlan;

async fn charge(ctx: &JobCtx, mut units: u64) -> Result<()> {
    while units > 0 {
        if ctx.cancel.is_cancelled() {
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "analysis cancelled",
            ));
        }
        let n = units.min(ctx.work.cap().max(1));
        consume_work(ctx, n).await?;
        units -= n;
    }
    Ok(())
}
pub(super) fn unnest_task<'a>(
    ctx: &'a JobCtx,
    plan: Box<AnalysisPlan>,
    rx: &'a mut MailboxRx,
    tx: &'a MailboxTx,
) -> Result<ChargedWindowFuture<impl std::future::Future<Output = Result<usize>> + 'a>> {
    let future = unnest(ctx, plan, rx, tx);
    let credit = ctx.owner.acquire(
        CreditKind::Reservation,
        std::mem::size_of_val(&future).saturating_add(64),
    )?;
    Ok(ChargedWindowFuture {
        future: Box::pin(future),
        _credit: credit,
    })
}
fn scalar(value: &DynamicValue, ty: &DataType) -> Scalar {
    match value {
        DynamicValue::Null => Scalar::Null,
        _ if *ty == DataType::Dynamic => Scalar::Dynamic(value.detach_copy()),
        DynamicValue::Bool(v) => Scalar::Bool(*v),
        DynamicValue::Int64(v) if *ty == DataType::TimestampMicrosUTC => {
            Scalar::TimestampMicrosUTC(*v)
        }
        DynamicValue::Int64(v) => Scalar::Int64(*v),
        DynamicValue::UInt64(v) => Scalar::UInt64(*v),
        DynamicValue::Float64(v) => Scalar::Float64(*v),
        DynamicValue::Utf8(v) => Scalar::utf8(v),
        DynamicValue::Bytes(v) => Scalar::bytes(v),
        _ => Scalar::Dynamic(value.detach_copy()),
    }
}
async fn unnest(
    ctx: &JobCtx,
    plan: Box<AnalysisPlan>,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
) -> Result<usize> {
    let AnalysisPlan::Unnest {
        spec,
        input,
        output,
        element,
    } = *plan
    else {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "Join requires two graph input ports",
        ));
    };
    let expr = sparrow_expr::bind(&spec.expr, &input)?;
    let allocation = sparrow_expr::allocation::AllocationBound::for_expr(&expr);
    let _metadata = ctx.owner.acquire(CreditKind::Retention, 64 * 512)?;
    let mut sequences = std::collections::BTreeMap::<Option<sparrow_model::OperatorId>, i64>::new();
    let mut eof = false;
    loop {
        let env = tokio::select! {biased;_=ctx.cancel.cancelled()=>break,env=rx.recv()=>env?};
        let Some(mut env) = env else {
            if ctx.graph_mode && !eof && !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "UNNEST input closed without EOF",
                ));
            }
            break;
        };
        let (batch, control) = env.take();
        if let Some(control) = control {
            match control {
                StreamControl::CheckpointBarrier { .. }
                | StreamControl::ProcessingTime { .. }
                | StreamControl::FeedObservation { .. }
                | StreamControl::GraphProgress { .. }
                | StreamControl::GraphRoundEnd { .. }
                | StreamControl::LiveFeedStart { .. }
                | StreamControl::LiveFeedEnd { .. } => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "UNNEST durable controls are not supported",
                    ))
                }
                StreamControl::EndOfInput => {
                    if !ctx.graph_mode || eof {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "invalid UNNEST EOF",
                        ));
                    }
                    eof = true;
                }
                _ => {}
            }
            if !tx.send_control(control).await? {
                break;
            }
        }
        if let Some(batch) = batch {
            if eof {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "UNNEST data after EOF",
                ));
            }
            for row in batch.rows() {
                charge(ctx, 1).await?;
                if !sequences.contains_key(&batch.source_operator()) && sequences.len() >= 64 {
                    return Err(SparrowError::new(
                        ErrorCode::BoundExceeded,
                        "UNNEST source identity limit",
                    ));
                }
                let sequence = sequences.entry(batch.source_operator()).or_default();
                *sequence = sequence.checked_add(1).ok_or_else(|| {
                    SparrowError::new(ErrorCode::IntegerOverflow, "UNNEST input ordinal exhausted")
                })?;
                let sequence = *sequence;
                let mut scratch = ctx.owner.acquire(
                    CreditKind::Reservation,
                    row.values.len().saturating_mul(8).saturating_add(128),
                )?;
                let columns: Vec<_> = row.values.iter().map(Scalar::resident_bytes).collect();
                let estimate = allocation.estimate(&columns);
                scratch.grow_to(
                    scratch
                        .bytes()
                        .saturating_add(estimate.allocated)
                        .saturating_add(estimate.value.saturating_mul(2)),
                )?;
                let array = sparrow_expr::eval_bound(&expr, &row.values)?;
                let items = match &array {
                    Scalar::Null | Scalar::Dynamic(DynamicValue::Null) => continue,
                    Scalar::Dynamic(DynamicValue::Array(items)) => items,
                    _ => {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            "UNNEST expression did not return an array",
                        ))
                    }
                };
                if items.len() > spec.max_rows {
                    return Err(SparrowError::new(
                        ErrorCode::BoundExceeded,
                        "UNNEST expansion row limit",
                    ));
                }
                let total = items.iter().fold(0usize, |n, v| {
                    n.saturating_add(row.resident_bytes())
                        .saturating_add(Scalar::Dynamic(v.clone()).resident_bytes())
                        .saturating_add(256)
                });
                if total > spec.max_bytes {
                    return Err(SparrowError::new(
                        ErrorCode::BoundExceeded,
                        "UNNEST per-input output byte limit",
                    ));
                }
                for (ordinal, value) in items.iter().enumerate() {
                    charge(ctx, (row.values.len() + 1) as u64).await?;
                    let _output = ctx.owner.acquire(
                        CreditKind::Reservation,
                        row.resident_bytes()
                            .saturating_add(Scalar::Dynamic(value.clone()).resident_bytes())
                            .saturating_add(512)
                            .saturating_mul(3),
                    )?;
                    let mut values = Vec::with_capacity(output.fields.len());
                    values.extend(row.values.iter().map(Scalar::detach_copy));
                    values.push(scalar(value, &element));
                    values.extend([
                        batch
                            .source_operator()
                            .map_or(Scalar::Null, |id| Scalar::Int64(id.raw() as i64)),
                        Scalar::Int64(sequence),
                        Scalar::Int64((ordinal + 1) as i64),
                    ]);
                    let out = crate::window::finish_rows_metered(
                        &output,
                        vec![Row { values }],
                        &ctx.owner,
                    )?
                    .unwrap()
                    .with_origin(batch.origin())
                    .with_source_operator(batch.source_operator());
                    drop(_output);
                    if !tx.send(out).await? {
                        return Ok(0);
                    }
                }
            }
        }
    }
    Ok(0)
}

pub(super) fn join_task<'a>(
    ctx: &'a JobCtx,
    plan: Box<AnalysisPlan>,
    inputs: Vec<MailboxRx>,
    tx: MailboxTx,
) -> Result<ChargedWindowFuture<impl std::future::Future<Output = Result<usize>> + 'a>> {
    let future = join(ctx, plan, inputs, tx);
    let credit = ctx.owner.acquire(
        CreditKind::Reservation,
        std::mem::size_of_val(&future).saturating_add(64),
    )?;
    Ok(ChargedWindowFuture {
        future: Box::pin(future),
        _credit: credit,
    })
}
async fn drain_join(
    ctx: &JobCtx,
    op: &mut BoundedJoin,
    tx: &MailboxTx,
    last: &mut Option<i64>,
) -> Result<bool> {
    while let Some((side, id)) = op.expired() {
        charge(ctx, op.work()).await?;
        if let Some(out) = op.close(side, id)? {
            if !tx.send(out).await? {
                return Ok(false);
            }
        }
    }
    let (keys, bytes) = op.state();
    ctx.metrics.record_state(keys as u64, bytes as u64);
    if let Some(wm) = op.progress() {
        if last.is_none_or(|old| wm > old) {
            if !tx
                .send_control(StreamControl::Watermark {
                    input: 0,
                    wm_micros: wm,
                })
                .await?
            {
                return Ok(false);
            }
            *last = Some(wm);
        }
    }
    Ok(true)
}
async fn join(
    ctx: &JobCtx,
    plan: Box<AnalysisPlan>,
    mut inputs: Vec<MailboxRx>,
    tx: MailboxTx,
) -> Result<usize> {
    let AnalysisPlan::Join {
        spec, left, right, ..
    } = *plan
    else {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "expected Join plan",
        ));
    };
    let mut op = BoundedJoin::new(spec, left, right, ctx.owner.clone(), ctx.max_state_keys)?;
    let mut eof = [false; 2];
    let mut last = None;
    loop {
        if eof == [true, true] {
            tx.send_control(StreamControl::EndOfInput).await?;
            break;
        }
        let (l, r) = inputs.split_at_mut(1);
        let (side, env) = tokio::select! {
            _=ctx.cancel.cancelled()=>break,
            e=l[0].recv(),if !eof[0]=>(0,e?),
            e=r[0].recv(),if !eof[1]=>(1,e?),
        };
        let Some(mut env) = env else {
            if ctx.cancel.is_cancelled() {
                break;
            }
            return Err(SparrowError::new(
                ErrorCode::JobFailed,
                "join input closed without permanent EOF",
            ));
        };
        let (batch, control) = env.take();
        if let Some(control) = control {
            match control {
                StreamControl::Watermark { wm_micros, .. } => op.advance(side, wm_micros)?,
                StreamControl::EndOfInput => {
                    eof[side] = true;
                    op.advance(side, i64::MAX)?;
                }
                // Idle is not a proof of absence. Do not propagate idle while
                // retained rows could later produce an older unmatched output.
                StreamControl::Idle { .. } | StreamControl::Active { .. } => {}
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "Join durable controls are not supported",
                    ))
                }
            }
            if !drain_join(ctx, &mut op, &tx, &mut last).await? {
                break;
            }
        }
        if let Some(batch) = batch {
            if eof[side] {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "join data after EOF",
                ));
            }
            for row in batch.rows() {
                charge(ctx, op.work()).await?;
                let Some(mut pending) = op.prepare(side, row, ctx.clock.now_micros())? else {
                    ctx.metrics.record_future_dropped(1);
                    continue;
                };
                op.check_fanout(side, &pending)?;
                let mut cursor = 0;
                while let Some(id) = op.next_match(side, &pending, cursor) {
                    charge(ctx, 1).await?;
                    cursor = id;
                    if !tx.send(op.emit_match(side, &mut pending, id)?).await? {
                        return Ok(0);
                    }
                }
                op.insert(side, pending);
                if !drain_join(ctx, &mut op, &tx, &mut last).await? {
                    return Ok(0);
                }
            }
        }
    }
    Ok(0)
}

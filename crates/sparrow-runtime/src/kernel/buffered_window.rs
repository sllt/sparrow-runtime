//! Cold executor: bounded recomputation does not enlarge the legacy window task.
use super::*;
use crate::buffered_window::{BufferedWindow, Ingest};

#[cold]
#[allow(clippy::too_many_arguments)]
pub(super) fn task<'a>(
    ctx: &'a JobCtx,
    operator: sparrow_model::OperatorId,
    prepared: Option<BufferedWindow>,
    spec: sparrow_plan::WindowSpec,
    input: sparrow_model::Schema,
    rx: &'a mut MailboxRx,
    tx: &'a MailboxTx,
    capture: &'a SharedCapture,
) -> Result<ChargedWindowFuture<impl std::future::Future<Output = Result<usize>> + 'a>> {
    // Only a prepared codec 4 participant (v31/v32 sliding count, v33 ET,
    // v34/v35 PT on the durable clock) runs aligned.
    let pt_ordered = ctx.ordered_time && prepared.as_ref().is_some_and(|op| op.is_pt() && op.is_durable());
    if (ctx.ordered_time && !pt_ordered)
        || ctx.aligned.is_some() != prepared.is_some()
        || prepared.as_ref().is_some_and(|op| op.is_pt() && !ctx.ordered_time)
    {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "buffered windows other than v31/v32 sliding count, v33 ET sliding/session and v34/v35 PT sliding/session require restart_fresh without a durable clock",
        ));
    }
    if pt_ordered {
        let op = prepared.expect("checked");
        let future = ordered_run(ctx, operator, op, rx, tx);
        let credit = ctx.owner.acquire(
            sparrow_model::CreditKind::Reservation,
            std::mem::size_of_val(&future).saturating_add(64),
        )?;
        return Ok(ChargedWindowFuture {
            future: Box::pin(OrderedOrLive::Ordered(Box::pin(future))),
            _credit: credit,
        });
    }
    let future = run(ctx, operator, prepared, spec, input, rx, tx, capture);
    let credit = ctx.owner.acquire(
        sparrow_model::CreditKind::Reservation,
        std::mem::size_of_val(&future).saturating_add(64),
    )?;
    Ok(ChargedWindowFuture {
        future: Box::pin(OrderedOrLive::Live(Box::pin(future))),
        _credit: credit,
    })
}

/// One concrete future type for both executors.
enum OrderedOrLive<A, B> {
    Ordered(std::pin::Pin<Box<A>>),
    Live(std::pin::Pin<Box<B>>),
}

impl<A, B> std::future::Future for OrderedOrLive<A, B>
where
    A: std::future::Future<Output = Result<usize>>,
    B: std::future::Future<Output = Result<usize>>,
{
    type Output = Result<usize>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            OrderedOrLive::Ordered(f) => f.as_mut().poll(cx),
            OrderedOrLive::Live(f) => f.as_mut().poll(cx),
        }
    }
}

/// v34/v35 PT sliding/session on the durable logical clock (v16/v17
/// contract): never samples or sleeps on a host clock. A `ProcessingTime`
/// control is forwarded first, then due outputs are drained in stable
/// (deadline, key) order, then later input/barriers are handled. Restart and
/// replay of the uncommitted suffix never advance time; there is no catch-up.
async fn ordered_run(
    ctx: &JobCtx,
    operator: sparrow_model::OperatorId,
    mut op: BufferedWindow,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
) -> Result<usize> {
    let mut now = ctx.clock.now_micros();
    // The prepared operator is already bound to the restored/initial cut.
    if op.now(now)? != now {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "PT buffered clock differs from the processing-time cut",
        ));
    }
    let mut n = 0usize;
    let mut timers = TimerReporter { ctx, live: 0, cancelled: 0 };
    crate::process_fault::pt_clock_log("start", now, 0);
    ctx.metrics.record_state(op.key_count() as u64, op.retention_bytes() as u64);
    loop {
        timers.sample(op.timers(), 0);
        let envelope = tokio::select! { biased; _ = ctx.cancel.cancelled() => break, e = rx.recv() => e? };
        let Some(mut envelope) = envelope else {
            if !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "ordered time window input closed without shutdown",
                ));
            }
            break;
        };
        let (batch, control) = envelope.take();
        if let Some(batch) = batch {
            for row in batch.rows() {
                charge(ctx, op.work_bound()).await?;
                let started = std::time::Instant::now();
                let result = op.push(row, now);
                if let Some(obs) = &ctx.observation {
                    obs.record(Latency::Window, started.elapsed());
                }
                match result? {
                    Ingest::Accepted(Some(out)) => {
                        n += out.num_rows();
                        if !tx.send(out).await? {
                            return Ok(n);
                        }
                    }
                    Ingest::Accepted(None) => {}
                    Ingest::Future | Ingest::Late => {
                        return Err(SparrowError::new(
                            ErrorCode::Internal,
                            "PT buffered window classified an arrival as late/future",
                        ))
                    }
                }
            }
            crate::process_fault::pt_clock_log("rows", now, batch.num_rows());
            crate::process_fault::window_rows_applied(batch.num_rows());
            ctx.metrics.record_state(op.key_count() as u64, op.retention_bytes() as u64);
        }
        if let Some(control) = control {
            match control {
                StreamControl::ProcessingTime { micros } => {
                    if micros < now {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "processing time cannot move backwards",
                        ));
                    }
                    now = op.now(micros)?;
                    crate::process_fault::pt_clock_log("tick", now, 0);
                    if !tx.send_control(StreamControl::ProcessingTime { micros }).await? {
                        break;
                    }
                    // Process-test hook: the tick is applied, its due outputs
                    // are not yet emitted (timer-driven cuts).
                    crate::process_fault::pt_time_applied(op.due(now), op.deadline().map(|d| d - now));
                    if !drain(ctx, &mut op, tx, now, &mut n).await? {
                        break;
                    }
                }
                StreamControl::CheckpointBarrier { checkpoint_id } => {
                    let aligned = ctx.aligned.as_ref().expect("ordered admission");
                    if aligned.acks.is_active(checkpoint_id) {
                        let frozen = crate::barrier::EncodedFreeze::from_buffered(
                            &op, operator, &ctx.owner, ctx.max_state_keys);
                        aligned.acks.state_frozen(checkpoint_id, operator, frozen).await;
                    }
                    if !tx.send_control(StreamControl::CheckpointBarrier { checkpoint_id }).await? {
                        break;
                    }
                }
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "PT buffered window on the durable clock accepts only time and barrier controls",
                    ))
                }
            }
        }
    }
    drop(op);
    timers.sample(0, 0);
    Ok(n)
}

async fn charge(ctx: &JobCtx, mut units: u64) -> Result<()> {
    while units > 0 {
        if ctx.cancel.is_cancelled() {
            return Err(SparrowError::new(ErrorCode::Cancelled, "window cancelled"));
        }
        let part = units.min(ctx.work.cap().max(1));
        consume_work(ctx, part).await?;
        units -= part;
    }
    Ok(())
}

async fn drain(
    ctx: &JobCtx,
    op: &mut BufferedWindow,
    tx: &MailboxTx,
    time: i64,
    n: &mut usize,
) -> Result<bool> {
    while op.due(time) {
        charge(ctx, op.work_bound()).await?;
        if let Some(batch) = op.take_due(time)? {
            *n += batch.num_rows();
            if !tx.send(batch).await? {
                return Ok(false);
            }
        }
    }
    ctx.metrics
        .record_state(op.key_count() as u64, op.retention_bytes() as u64);
    Ok(true)
}

async fn progress(
    ctx: &JobCtx,
    op: &mut BufferedWindow,
    tx: &MailboxTx,
    last: &mut Option<i64>,
    n: &mut usize,
) -> Result<bool> {
    if let Some(wm) = op.progress() {
        if !drain(ctx, op, tx, wm, n).await? {
            return Ok(false);
        }
        if last.is_none_or(|old| wm > old) {
            if !tx
                .send_control(StreamControl::Watermark {
                    input: sparrow_model::InputId::SINGLE.raw(),
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

#[allow(clippy::too_many_arguments)]
async fn run(
    ctx: &JobCtx,
    operator: sparrow_model::OperatorId,
    prepared: Option<BufferedWindow>,
    spec: sparrow_plan::WindowSpec,
    input: sparrow_model::Schema,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
    capture: &SharedCapture,
) -> Result<usize> {
    let mut op = match prepared {
        Some(op) => op,
        None => BufferedWindow::new(
            spec,
            input.clone(),
            ctx.owner.clone(),
            ctx.max_state_keys,
            ctx.max_timers,
            ctx.graph_mode,
        )?,
    };
    ctx.metrics
        .record_state(op.key_count() as u64, op.retention_bytes() as u64);
    let mut timers = TimerReporter {
        ctx,
        live: 0,
        cancelled: 0,
    };
    let (mut closed, mut eof, mut deferred_eof) = (false, false, false);
    let (mut n, mut last_watermark) = (0, None);
    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        if op.is_pt() {
            let now = op.now(ctx.clock.now_micros())?;
            if !drain(ctx, &mut op, tx, now, &mut n).await? {
                break;
            }
        }
        timers.sample(op.timers(), 0);
        let deadline = op.deadline();
        if closed {
            if !op.is_pt() || deadline.is_none() {
                break;
            }
            tokio::select! { biased; _ = ctx.cancel.cancelled() => break, _ = ctx.clock.sleep_until(deadline) => {} }
            continue;
        }
        let env = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => break,
            env = rx.recv() => env?,
            _ = ctx.clock.sleep_until(deadline) => continue,
        };
        let Some(mut env) = env else {
            if ctx.graph_mode && !eof && !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "window input closed without EOF",
                ));
            }
            closed = true;
            if op.is_et() && !ctx.graph_mode {
                if !drain(ctx, &mut op, tx, i64::MAX, &mut n).await? {
                    break;
                }
            }
            continue;
        };
        let (batch, control) = env.take();
        if let Some(control) = control {
            match control.clone() {
                StreamControl::Watermark { input, wm_micros } => {
                    op.watermark(sparrow_model::InputId(input), wm_micros)?
                }
                StreamControl::Idle { input } => {
                    op.activity(sparrow_model::InputId(input), false)?
                }
                StreamControl::Active { input } => {
                    op.activity(sparrow_model::InputId(input), true)?
                }
                StreamControl::EndOfInput => {
                    if !ctx.graph_mode || eof {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "invalid/duplicate window EOF",
                        ));
                    }
                    eof = true;
                    if op.is_pt() {
                        closed = true;
                        deferred_eof = true;
                    } else {
                        if op.is_et() && !drain(ctx, &mut op, tx, i64::MAX, &mut n).await? {
                            break;
                        }
                        if !tx.send_control(StreamControl::EndOfInput).await? {
                            break;
                        }
                    }
                }
                StreamControl::CheckpointBarrier { checkpoint_id } if op.is_durable() => {
                    // Rows of every earlier envelope are fully applied; the
                    // frame is the exact state at this barrier.
                    if let Some(aj) = &ctx.aligned {
                        if aj.acks.is_active(checkpoint_id) {
                            let frozen = crate::barrier::EncodedFreeze::from_buffered(
                                &op, operator, &ctx.owner, ctx.max_state_keys);
                            aj.acks.state_frozen(checkpoint_id, operator, frozen).await;
                        }
                    }
                    if !tx.send_control(StreamControl::CheckpointBarrier { checkpoint_id }).await? {
                        break;
                    }
                }
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "buffered window does not accept checkpoint or durable-clock controls",
                    ))
                }
            }
            if !eof && !progress(ctx, &mut op, tx, &mut last_watermark, &mut n).await? {
                break;
            }
            if matches!(
                control,
                StreamControl::Idle { .. } | StreamControl::Active { .. }
            ) && !tx.send_control(control.clone()).await?
            {
                break;
            }
            if let Some((side, drop_when_full)) = &ctx.side_output {
                graph::publish_control(ctx, side, *drop_when_full, control).await?;
            }
        }
        if let Some(batch) = batch {
            if eof {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "window data after EOF",
                ));
            }
            for row in batch.rows() {
                charge(ctx, op.work_bound()).await?;
                let now = op.now(ctx.clock.now_micros())?;
                if op.is_pt() && !drain(ctx, &mut op, tx, now, &mut n).await? {
                    return Ok(n);
                }
                let started = std::time::Instant::now();
                let result = op.push(row, now);
                if let Some(obs) = &ctx.observation {
                    obs.record(Latency::Window, started.elapsed());
                }
                match result? {
                    Ingest::Accepted(Some(out)) => {
                        n += out.num_rows();
                        if !tx.send(out).await? {
                            return Ok(n);
                        }
                    }
                    Ingest::Accepted(None) => {}
                    Ingest::Future => ctx.metrics.record_future_dropped(1),
                    Ingest::Late => {
                        capture.push_late(std::slice::from_ref(row));
                        if ctx.side_output.is_some() {
                            let _scratch = ctx.owner.acquire(
                                sparrow_model::CreditKind::Reservation,
                                row.resident_bytes().saturating_mul(2).saturating_add(128),
                            )?;
                            let late = Row {
                                values: row
                                    .values
                                    .iter()
                                    .map(sparrow_model::Scalar::detach_copy)
                                    .collect(),
                            };
                            let out =
                                crate::window::finish_rows_metered(&input, vec![late], &ctx.owner)?
                                    .expect("one late row");
                            drop(_scratch);
                            graph::publish_side(ctx, out).await?;
                        }
                    }
                }
                if !progress(ctx, &mut op, tx, &mut last_watermark, &mut n).await? {
                    return Ok(n);
                }
            }
            // Process-test hook (feature process-fault-pause): input_after cut.
            crate::process_fault::window_rows_applied(batch.num_rows());
            ctx.metrics
                .record_state(op.key_count() as u64, op.retention_bytes() as u64);
        }
    }
    if deferred_eof && !ctx.cancel.is_cancelled() {
        tx.send_control(StreamControl::EndOfInput).await?;
    }
    drop(op);
    timers.sample(0, 0);
    Ok(n)
}

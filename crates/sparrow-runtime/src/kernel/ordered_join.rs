//! v38 Join: collect bounded rounds from both ports, then execute in fixed
//! left/right order. A barrier never crosses an incomplete match continuation.
use super::*;
use crate::{
    analysis_state::PreparedAnalysis,
    bounded_join::BoundedJoin,
    graph_cut::{invalid, Progress},
};
use sparrow_model::CreditKind;
use std::collections::VecDeque;

enum Item {
    Data(RowBatch),
    Progress(Progress),
}

async fn charge(ctx: &JobCtx, mut units: u64) -> Result<()> {
    while units != 0 {
        if ctx.cancel.is_cancelled() {
            return Err(invalid("Join cancelled"));
        }
        let n = units.min(ctx.work.cap().max(1));
        consume_work(ctx, n).await?;
        units -= n;
    }
    Ok(())
}

async fn drain(
    ctx: &JobCtx,
    op: &mut BoundedJoin,
    states: &[Progress; 2],
    emitted: &mut Progress,
    out: &MailboxTx,
) -> Result<bool> {
    while let Some((side, id)) = op.expired() {
        charge(ctx, op.work()).await?;
        if let Some(batch) = op.close(side, id)? {
            if !out.send(batch).await? {
                return Ok(false);
            }
        }
    }
    let (keys, bytes) = op.state();
    ctx.metrics.record_state(keys as u64, bytes as u64);
    // Idle never establishes absence. Keep the Join active until BOTH inputs
    // have permanent EOF, so downstream fan-in cannot bypass pending left rows.
    let eof = states.iter().all(|p| p.eof);
    let next = Progress {
        watermark: op.progress(),
        idle: eof,
        eof,
    };
    if emitted
        .watermark
        .is_some_and(|old| next.watermark.is_none_or(|n| n < old))
    {
        return Err(invalid("Join output watermark moved backwards"));
    }
    if next != *emitted {
        if !out.send_control(next.control()).await? {
            return Ok(false);
        }
        *emitted = next;
    }
    Ok(true)
}

pub(super) async fn run(
    ctx: &JobCtx,
    operator: sparrow_model::OperatorId,
    mut inputs: Vec<MailboxRx>,
    output: MailboxTx,
) -> Result<usize> {
    let graph = ctx
        .graph_time
        .as_ref()
        .ok_or_else(|| invalid("Join requires logged graph decisions"))?;
    if inputs.len() != 2 || !graph.event_time {
        return Err(invalid("Join requires two event-time inputs"));
    }
    let prepared = ctx.aligned.as_ref().and_then(|a| {
        a.analysis
            .lock()
            .expect("prepared analysis")
            .remove(&operator)
    });
    let Some(PreparedAnalysis::Join {
        mut op,
        inputs: mut states,
        mut emitted,
        _credit,
    }) = prepared
    else {
        return Err(invalid("Join lacks prepared recovery state"));
    };
    let _credit = _credit;
    let mut sequence = graph
        .initial
        .sequence
        .checked_add(1)
        .ok_or_else(|| invalid("Join decision sequence exhausted"))?;
    let mut previous_time = graph.initial.micros;
    let mut times = [None; 2];
    let mut done = [false; 2];
    let mut barriers = [None; 2];
    let mut buffers: [VecDeque<Item>; 2] = std::array::from_fn(|_| VecDeque::new());
    let mut workspace: Option<sparrow_model::MemoryLease> = None;
    let (mut items, mut rows, mut bytes, mut cursor) = (0usize, 0usize, 0usize, 0usize);
    loop {
        let (side, env) = tokio::select! { biased;
            _ = ctx.cancel.cancelled() => return Ok(0),
            e = std::future::poll_fn(|cx| {
                for offset in 0..2 { let i = (cursor + offset) % 2;
                    if !done[i] && barriers[i].is_none() {
                        if let std::task::Poll::Ready(e) = inputs[i].poll_recv(cx) { return std::task::Poll::Ready((i, e)); }
                    }
                }
                std::task::Poll::Pending
            }) => e,
        };
        cursor = (side + 1) % 2;
        let Some(mut env) = env else {
            return Err(invalid("ordered Join input closed without cancellation"));
        };
        let (batch, control) = env.take();
        let mut item = None;
        if let Some(batch) = batch {
            if times[side].is_none() || barriers.iter().any(Option::is_some) {
                return Err(invalid("Join data outside decision"));
            }
            rows = rows.saturating_add(batch.num_rows());
            bytes = bytes.saturating_add(batch.lease().bytes());
            if rows > ctx.owner.budget().max_rows
                || bytes > ctx.owner.budget().reservation_bytes / 2
            {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "Join round buffer bound exceeded",
                ));
            }
            item = Some(Item::Data(batch));
        }
        if let Some(control) = control {
            match control {
                StreamControl::ProcessingTime { micros } => {
                    if times[side].is_some()
                        || micros < previous_time
                        || times.iter().flatten().any(|v| *v != micros)
                        || barriers.iter().any(Option::is_some)
                    {
                        return Err(invalid("conflicting Join time decision"));
                    }
                    times[side] = Some(micros);
                }
                StreamControl::GraphProgress {
                    watermark_micros,
                    flags,
                } => {
                    if times[side].is_none() {
                        return Err(invalid("Join progress outside decision"));
                    }
                    item = Some(Item::Progress(Progress::from_control(
                        watermark_micros,
                        flags,
                    )?));
                }
                StreamControl::GraphRoundEnd { sequence: got } => {
                    if got != sequence || times[side].is_none() {
                        return Err(invalid("Join round boundary mismatch"));
                    }
                    done[side] = true;
                }
                StreamControl::CheckpointBarrier { checkpoint_id } => {
                    let acks = &ctx.aligned.as_ref().expect("prepared Join").acks;
                    if !acks.is_active(checkpoint_id) {
                        continue;
                    }
                    if times.iter().any(Option::is_some)
                        || items != 0
                        || barriers.iter().flatten().any(|id| *id != checkpoint_id)
                    {
                        return Err(invalid("Join barrier crossed incomplete decision"));
                    }
                    barriers[side] = Some(checkpoint_id);
                    inputs[side].barrier_blocked(true);
                    if barriers.iter().all(Option::is_some) {
                        let frozen = crate::barrier::EncodedFreeze::from_analysis(
                            &ctx.owner,
                            op.state().1,
                            |out| {
                                op.encode_state(
                                    operator,
                                    &states,
                                    &emitted,
                                    out,
                                    ctx.max_state_keys,
                                )
                            },
                        );
                        acks.analysis_frozen(checkpoint_id, operator, frozen).await;
                        if !output.send_control(control).await? {
                            return Ok(0);
                        }
                        barriers.fill(None);
                        for input in &inputs {
                            input.barrier_blocked(false);
                        }
                    }
                }
                _ => return Err(invalid("legacy controls cannot enter a durable Join")),
            }
        }
        if let Some(item) = item {
            items += 1;
            if items > ctx.owner.budget().max_rows.saturating_add(8) {
                return Err(invalid("Join round control count bound"));
            }
            let size = items
                .saturating_mul(2 * std::mem::size_of::<Item>())
                .saturating_add(256);
            match &mut workspace {
                Some(c) => c.grow_to(size)?,
                None => workspace = Some(ctx.owner.acquire(CreditKind::Reservation, size)?),
            }
            buffers[side].push_back(item);
        }
        if done.iter().all(|v| *v) {
            let micros = times[0].expect("complete round");
            if !output
                .send_control(StreamControl::ProcessingTime { micros })
                .await?
            {
                return Ok(0);
            }
            for (side, buffer) in buffers.iter_mut().enumerate() {
                while let Some(item) = buffer.pop_front() {
                    match item {
                        Item::Progress(next) => {
                            let old = &states[side];
                            if (old.eof && !next.eof)
                                || old
                                    .watermark
                                    .is_some_and(|v| next.watermark.is_none_or(|wm| wm < v))
                            {
                                return Err(invalid("Join source progress moved backwards"));
                            }
                            if let Some(wm) = if next.eof {
                                Some(i64::MAX)
                            } else {
                                next.watermark
                            } {
                                op.advance(side, wm)?;
                            }
                            states[side] = next;
                        }
                        Item::Data(batch) => {
                            if states[side].eof {
                                return Err(invalid("Join data after EOF"));
                            }
                            for row in batch.rows() {
                                charge(ctx, op.work()).await?;
                                let Some(mut pending) =
                                    op.prepare(side, row, graph.observed_micros())?
                                else {
                                    ctx.metrics.record_future_dropped(1);
                                    continue;
                                };
                                op.check_fanout(side, &pending)?;
                                let mut after = 0;
                                while let Some(id) = op.next_match(side, &pending, after) {
                                    charge(ctx, 1).await?;
                                    after = id;
                                    if !output.send(op.emit_match(side, &mut pending, id)?).await? {
                                        return Ok(0);
                                    }
                                }
                                op.insert(side, pending);
                                if !drain(ctx, &mut op, &states, &mut emitted, &output).await? {
                                    return Ok(0);
                                }
                            }
                        }
                    }
                    if !drain(ctx, &mut op, &states, &mut emitted, &output).await? {
                        return Ok(0);
                    }
                }
                *buffer = VecDeque::new();
            }
            workspace = None;
            items = 0;
            rows = 0;
            bytes = 0;
            if !output
                .send_control(StreamControl::GraphRoundEnd { sequence })
                .await?
            {
                return Ok(0);
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("Join decision sequence exhausted"))?;
            previous_time = micros;
            times.fill(None);
            done.fill(false);
        }
    }
}

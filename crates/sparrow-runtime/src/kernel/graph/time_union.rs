//! Deterministic bounded per-decision fan-in. Drain every ready port into
//! charged buffers (never block one fanout leg while awaiting another), then
//! replay those buffers in physical-edge order. No scheduling-order promise is
//! retroactively imposed on the legacy Union path.
use super::*;
use crate::graph_cut::{invalid, Progress, UnionProgress};
use std::collections::VecDeque;

enum Item {
    Data(RowBatch),
    Progress(Progress),
}

fn merged(state: &UnionProgress, event_time: bool) -> Progress {
    let eof = state.inputs.iter().all(|p| p.eof);
    let idle = state.inputs.iter().all(|p| p.idle);
    let mut watermark = state.emitted.watermark;
    if event_time && eof {
        watermark = Some(i64::MAX);
    } else if event_time
        && !idle
        && !state
            .inputs
            .iter()
            .any(|p| !p.idle && p.watermark.is_none())
    {
        if let Some(next) = state
            .inputs
            .iter()
            .filter(|p| !p.idle)
            .filter_map(|p| p.watermark)
            .min()
        {
            watermark = Some(watermark.map_or(next, |old| old.max(next)));
        }
    }
    Progress {
        watermark,
        idle,
        eof,
    }
}

pub(super) async fn run(
    ctx: &JobCtx,
    operator: u32,
    mut inputs: Vec<MailboxRx>,
    output: MailboxTx,
) -> Result<()> {
    let graph = ctx.graph_time.as_ref().expect("durable graph");
    let mut state = graph
        .initial
        .unions
        .get(&operator)
        .ok_or_else(|| invalid("missing restored Union"))?
        .clone();
    let n = inputs.len();
    if state.inputs.len() != n {
        return Err(invalid("Union port count changed"));
    }
    let mut sequence = graph
        .initial
        .sequence
        .checked_add(1)
        .ok_or_else(|| invalid("decision sequence exhausted"))?;
    let mut time = vec![None; n];
    let mut done = vec![false; n];
    let mut barriers = vec![None; n];
    let mut buffers = (0..n).map(|_| VecDeque::<Item>::new()).collect::<Vec<_>>();
    let mut workspace: Option<sparrow_model::MemoryLease> = None;
    let mut items = 0usize;
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let mut cursor = 0usize;
    let mut previous_time = graph.initial.micros;
    loop {
        let (port, envelope) = tokio::select! {biased;
            _=ctx.cancel.cancelled()=>return Ok(()),
            value=std::future::poll_fn(|cx| {
                for offset in 0..n {let i=(cursor+offset)%n;if !done[i] && barriers[i].is_none() {
                    if let Poll::Ready(e)=inputs[i].poll_recv(cx) {return Poll::Ready((i,e));}
                }}Poll::Pending
            })=>value,
        };
        cursor = (port + 1) % n;
        let Some(mut envelope) = envelope else {
            return Err(invalid("ordered Union input closed without cancellation"));
        };
        let (batch, control) = envelope.take();
        let mut item = None;
        if let Some(batch) = batch {
            if time[port].is_none() || barriers.iter().any(Option::is_some) {
                return Err(invalid("Union data outside a decision"));
            }
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or_else(|| invalid("round row overflow"))?;
            bytes = bytes
                .checked_add(batch.lease().bytes())
                .ok_or_else(|| invalid("round byte overflow"))?;
            if rows > ctx.owner.budget().max_rows
                || bytes > ctx.owner.budget().reservation_bytes / 2
            {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "durable Union decision buffer bound exceeded",
                ));
            }
            item = Some(Item::Data(batch));
        }
        if let Some(control) = control {
            match control {
                StreamControl::ProcessingTime { micros } => {
                    if time[port].is_some()
                        || micros < previous_time
                        || barriers.iter().any(Option::is_some)
                        || time.iter().flatten().any(|t| *t != micros)
                    {
                        return Err(invalid("conflicting Union time decision"));
                    }
                    time[port] = Some(micros);
                }
                StreamControl::GraphProgress {
                    watermark_micros,
                    flags,
                } => {
                    if time[port].is_none() {
                        return Err(invalid("Union progress outside a decision"));
                    }
                    item = Some(Item::Progress(Progress::from_control(
                        watermark_micros,
                        flags,
                    )?));
                }
                StreamControl::GraphRoundEnd { sequence: got } => {
                    if got != sequence || time[port].is_none() {
                        return Err(invalid("Union round boundary mismatch"));
                    }
                    done[port] = true;
                }
                StreamControl::CheckpointBarrier { checkpoint_id } => {
                    let acks = &ctx.aligned.as_ref().expect("durable graph ACKs").acks;
                    if !acks.is_active(checkpoint_id) {
                        continue;
                    }
                    if time.iter().any(Option::is_some)
                        || items != 0
                        || barriers.iter().flatten().any(|id| *id != checkpoint_id)
                    {
                        return Err(invalid("barrier crossed an incomplete graph decision"));
                    }
                    barriers[port] = Some(checkpoint_id);
                    inputs[port].barrier_blocked(true);
                    if barriers.iter().all(Option::is_some) {
                        graph.record_union(operator, checkpoint_id, state.clone())?;
                        if !output
                            .send_control(StreamControl::CheckpointBarrier { checkpoint_id })
                            .await?
                        {
                            return Ok(());
                        }
                        barriers.fill(None);
                        for input in &inputs {
                            input.barrier_blocked(false);
                        }
                    }
                }
                _ => {
                    return Err(invalid(
                        "legacy time controls cannot enter a durable graph decision",
                    ))
                }
            }
        }
        if let Some(item) = item {
            items = items
                .checked_add(1)
                .ok_or_else(|| invalid("round item overflow"))?;
            if items > ctx.owner.budget().max_rows.saturating_add(n * 4) {
                return Err(invalid("round control count bound"));
            }
            // Keep capacity credit until ALL per-port VecDeque allocations are
            // released, not merely until each buffered item is popped.
            let size = items
                .saturating_mul(2 * std::mem::size_of::<Item>())
                .saturating_add(n * 128);
            match &mut workspace {
                Some(credit) => credit.grow_to(size)?,
                None => workspace = Some(ctx.owner.acquire(CreditKind::Reservation, size)?),
            }
            buffers[port].push_back(item);
        }
        if done.iter().all(|v| *v) {
            let micros = time[0].expect("complete round time");
            if !output
                .send_control(StreamControl::ProcessingTime { micros })
                .await?
            {
                return Ok(());
            }
            for (i, buffer) in buffers.iter_mut().enumerate() {
                while let Some(item) = buffer.pop_front() {
                    consume_work(ctx, 1).await?;
                    match item {
                        Item::Data(batch) => {
                            if graph.event_time && state.inputs[i].eof {
                                return Err(invalid("ET row after permanent EOF"));
                            }
                            if !output.send(batch).await? {
                                return Ok(());
                            }
                        }
                        Item::Progress(next) => {
                            let previous = &state.inputs[i];
                            if (previous.eof && !next.eof)
                                || previous
                                    .watermark
                                    .is_some_and(|old| next.watermark.is_none_or(|wm| wm < old))
                            {
                                return Err(invalid("graph input progress moved backwards"));
                            }
                            state.inputs[i] = next;
                            let next = merged(&state, graph.event_time);
                            if next != state.emitted {
                                if !output.send_control(next.control()).await? {
                                    return Ok(());
                                }
                                state.emitted = next;
                            }
                        }
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
                return Ok(());
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("decision sequence exhausted"))?;
            previous_time = micros;
            time.fill(None);
            done.fill(false);
        }
    }
}

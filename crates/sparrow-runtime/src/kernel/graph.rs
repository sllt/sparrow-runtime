//! Explicit physical edges. Every edge has an independent bounded mailbox;
//! sources, transforms and windows reuse the linear kernels and Job owner.
use super::*;
use sparrow_model::{CreditKind, InputId, RowBatchBuilder, Scalar};
use sparrow_plan::graph::RouteMode;
use sparrow_plan::physical::PhysicalEdge;
use std::collections::BTreeSet;
use std::task::Poll;

#[derive(Default)]
pub struct GraphInput {
    pub rows: Vec<Row>,
    /// Ordered data/control channel. EndOfInput declares permanent data EOF;
    /// an unmarked channel close is a failure, never a final watermark.
    pub events: Option<ObservedReceiver<IngressEvent>>,
    pub live: Option<ObservedReceiver<Row>>,
    pub budgeted: Option<ObservedReceiver<sparrow_model::QueuedRow>>,
    pub trailing_controls: Vec<StreamControl>,
}

pub struct GraphOutput {
    pub capture: SharedCapture,
    pub live: Option<ObservedSender<RowBatch>>,
    pub outbox: Option<Arc<sparrow_model::InflightCounter>>,
}

pub(super) struct SourceTime {
    binding: sparrow_model::EventTimeBinding,
    last: std::sync::atomic::AtomicI64,
    active: std::sync::atomic::AtomicBool,
}
pub(super) fn source_activity(ctx: &JobCtx, control: &StreamControl) {
    if let Some(time) = &ctx.source_time {
        match control {
            StreamControl::Idle { .. } => time.active.store(false, Ordering::Relaxed),
            StreamControl::Active { .. } => time.active.store(true, Ordering::Relaxed),
            _ => {}
        }
    }
}
pub(super) async fn send_source(ctx: &JobCtx, tx: &MailboxTx, batch: RowBatch) -> Result<bool> {
    if ctx
        .source_time
        .as_ref()
        .is_some_and(|t| !t.active.swap(true, Ordering::Relaxed))
    {
        tx.send_control(StreamControl::Active { input: 0 }).await?;
    }
    let watermark = if let Some(time) = &ctx.source_time {
        let index = batch
            .schema()
            .index_of_name(&time.binding.field)
            .ok_or_else(|| invalid("source event-time field missing"))?;
        let mut next = time.last.load(Ordering::Relaxed);
        for row in batch.rows() {
            let timestamp = row.values[index]
                .as_event_time_micros()
                .filter(|v| *v >= 0)
                .ok_or_else(|| invalid("invalid source event time"))?;
            if time
                .binding
                .max_future_skew_micros
                .is_some_and(|skew| timestamp > ctx.clock.now_micros().saturating_add(skew))
            {
                continue;
            }
            next = next.max(
                timestamp
                    .saturating_sub(time.binding.out_of_orderness_micros)
                    .max(0),
            );
        }
        let previous = time.last.swap(next, Ordering::Relaxed);
        (next > previous).then_some(next)
    } else {
        None
    };
    if !tx.send(batch).await? {
        return Ok(false);
    }
    if let Some(wm_micros) = watermark {
        tx.send_control(StreamControl::Watermark {
            input: 0,
            wm_micros,
        })
        .await?;
    }
    Ok(true)
}

fn invalid(message: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message)
}

/// Conservative working reservation for graph IR, compiled expressions,
/// schemas, task contexts and bounded routing/alignment bookkeeping. Shared
/// literals may be overcharged; no allocation is exempt because it is metadata.
pub(super) fn metadata_bytes(plan: &PhysicalPlan) -> usize {
    use sparrow_expr::Expr;
    use sparrow_model::{DataType, Schema};
    fn ty(value: &DataType) -> usize {
        match value {
            DataType::Array(v) => 64usize.saturating_add(ty(v)),
            DataType::Map { key, value } => {
                128usize.saturating_add(ty(key)).saturating_add(ty(value))
            }
            DataType::Struct(fields) => fields.iter().fold(
                fields
                    .capacity()
                    .saturating_mul(std::mem::size_of::<sparrow_model::Field>()),
                |n, f| {
                    n.saturating_add(f.name.capacity())
                        .saturating_add(ty(&f.data_type))
                },
            ),
            _ => 0,
        }
    }
    fn schema(s: &Schema) -> usize {
        s.fields.iter().fold(
            128usize.saturating_add(
                s.fields
                    .capacity()
                    .saturating_mul(std::mem::size_of::<sparrow_model::Field>()),
            ),
            |n, f| {
                n.saturating_add(f.name.capacity())
                    .saturating_add(ty(&f.data_type))
            },
        )
    }
    fn expr(e: &Expr) -> usize {
        128usize.saturating_add(match e {
            Expr::Column { name } => name.capacity(),
            Expr::Literal(v) => v.resident_bytes(),
            Expr::Cast { expr: e, target } | Expr::TryCast { expr: e, target } => {
                expr(e).saturating_add(ty(target))
            }
            Expr::Binary { left, right, .. } => expr(left).saturating_add(expr(right)),
            Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::Not(e) => expr(e),
            Expr::Call { name, args } => args
                .iter()
                .fold(name.capacity(), |n, e| n.saturating_add(expr(e))),
            Expr::DynamicGet { expr: e, key } => expr(e).saturating_add(key.capacity()),
        })
    }
    let mut bytes = plan.mailbox_count().saturating_mul(256);
    for stage in &plan.stages {
        bytes = bytes
            .saturating_add(1024)
            .saturating_add(stage_schema(stage, false).map(schema).unwrap_or(0))
            .saturating_add(stage_schema(stage, true).map(schema).unwrap_or(0));
        let expressions = match stage {
            PhysicalStage::Route { cases, .. } => cases
                .iter()
                .fold(0usize, |n, (e, _)| n.saturating_add(expr(e))),
            PhysicalStage::Transform { steps } => steps.iter().fold(0usize, |n, s| {
                n.saturating_add(match s {
                    sparrow_plan::TransformStep::Filter { predicate, .. } => expr(predicate),
                    sparrow_plan::TransformStep::Project { exprs, .. }
                    | sparrow_plan::TransformStep::Map { exprs, .. } => {
                        exprs.iter().fold(0usize, |n, e| n.saturating_add(expr(e)))
                    }
                })
            }),
            PhysicalStage::WindowAgg { spec, .. } => spec.aggs.iter().fold(
                spec.keys.iter().map(String::capacity).sum::<usize>(),
                |n, a| {
                    n.saturating_add(a.alias.capacity())
                        .saturating_add(a.input.as_ref().map(expr).unwrap_or(0))
                },
            ),
            PhysicalStage::Iot { spec, .. } => spec.keys.iter().chain(&spec.fields)
                .fold(512usize, |n, field| n.saturating_add(field.capacity()).saturating_add(64)),
            _ => 0,
        };
        bytes = bytes.saturating_add(expressions);
    }
    bytes.saturating_mul(4)
}

pub(super) fn validate_request(req: &JobRequest) -> Result<()> {
    let Some(edges) = &req.plan.edges else {
        if !req.graph_inputs.is_empty()
            || !req.graph_outputs.is_empty()
            || !req.plan.side_outputs.is_empty()
            || !req.plan.source_times.is_empty()
            || req.plan.stages.iter().any(|s| {
                matches!(
                    s,
                    PhysicalStage::Branch { .. }
                        | PhysicalStage::Route { .. }
                        | PhysicalStage::UnionAll { .. }
                        | PhysicalStage::BestEffortSink { .. }
                )
            })
        {
            return Err(invalid("graph I/O requires an explicit graph topology"));
        }
        return Ok(());
    };
    let stages = &req.plan.stages;
    if stages.len() > 64 || edges.len() > 128 || stages.len() < 2 {
        return Err(invalid("graph exceeds 64 chains / 128 edges"));
    }
    let mut incoming = vec![0; stages.len()];
    let mut outgoing = vec![0; stages.len()];
    let mut seen_edges = BTreeSet::new();
    let mut operators = BTreeSet::new();
    for stage in stages {
        let ids = match stage {
            PhysicalStage::Transform { steps } => steps
                .iter()
                .map(|step| match step {
                    sparrow_plan::TransformStep::Filter { operator, .. }
                    | sparrow_plan::TransformStep::Project { operator, .. }
                    | sparrow_plan::TransformStep::Map { operator, .. } => *operator,
                })
                .collect::<Vec<_>>(),
            PhysicalStage::MemorySource { operator, .. }
            | PhysicalStage::CaptureSink { operator, .. }
            | PhysicalStage::BestEffortSink { operator, .. }
            | PhysicalStage::Branch { operator, .. }
            | PhysicalStage::Route { operator, .. }
            | PhysicalStage::UnionAll { operator, .. }
            | PhysicalStage::WindowAgg { operator, .. }
            | PhysicalStage::Iot { operator, .. }
            | PhysicalStage::Deduplicate { operator, .. }
            | PhysicalStage::Lookup { operator, .. } => vec![*operator],
        };
        if ids.is_empty() || ids.iter().any(|id| !operators.insert(*id)) {
            return Err(invalid("empty chain or duplicate graph operator identity"));
        }
        if let PhysicalStage::Transform { steps } = stage {
            CompiledTransform::new(steps)?;
        }
        if let PhysicalStage::Iot { spec, input, .. } = stage {
            spec.validate(input)?;
        }
    }
    let mut side_stages = BTreeSet::new();
    for (i, side) in &req.plan.side_outputs {
        if *i >= stages.len()
            || !side_stages.insert(*i)
            || !edges.iter().any(|e| {
                e.from == *i
                    && e.port.raw() == side.to
                    && e.best_effort == (side.full == sparrow_plan::graph::SideOutputFull::Drop)
            })
        {
            return Err(invalid("invalid/duplicate graph side port"));
        }
        let valid = match side.kind {
            sparrow_plan::graph::SideOutputKind::DecodeError => {
                matches!(stages[*i], PhysicalStage::MemorySource { .. })
            }
            sparrow_plan::graph::SideOutputKind::RuleReject => {
                matches!(stages[*i], PhysicalStage::Route { .. })
            }
            sparrow_plan::graph::SideOutputKind::Late => {
                matches!(&stages[*i],PhysicalStage::WindowAgg {spec,..} if spec.kind.uses_event_time())
            }
        };
        if !valid {
            return Err(invalid(
                "side output attached to incompatible graph operator",
            ));
        }
    }
    for edge in edges {
        if edge.from >= stages.len()
            || edge.to >= stages.len()
            || edge.from == edge.to
            || !seen_edges.insert((edge.from, edge.to))
        {
            return Err(invalid("duplicate, self or out-of-range graph edge"));
        }
        incoming[edge.to] += 1;
        outgoing[edge.from] += 1;
        let from_schema = if let Some((_, side)) = req
            .plan
            .side_outputs
            .iter()
            .find(|(i, s)| *i == edge.from && s.to == edge.port.raw())
        {
            if side.kind == sparrow_plan::graph::SideOutputKind::DecodeError {
                sparrow_plan::graph::decode_error_schema()
            } else {
                stage_schema(&stages[edge.from], false)?.clone()
            }
        } else {
            stage_schema(&stages[edge.from], true)?.clone()
        };
        if from_schema.fields != stage_schema(&stages[edge.to], false)?.fields {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "graph edge schema mismatch",
            ));
        }
        if edge.best_effort
            && !matches!(
                stages[edge.from],
                PhysicalStage::Branch { .. } | PhysicalStage::Route { .. }
            )
            && !req
                .plan
                .side_outputs
                .iter()
                .any(|(i, s)| *i == edge.from && s.to == edge.port.raw())
        {
            return Err(invalid("best-effort edges require an explicit router"));
        }
    }
    let mut sources = BTreeSet::new();
    let mut sinks = BTreeSet::new();
    for (i, stage) in stages.iter().enumerate() {
        let side = usize::from(req.plan.side_outputs.iter().any(|(index, _)| *index == i));
        let valid = match stage {
            PhysicalStage::MemorySource { operator, .. } => {
                sources.insert(*operator) && incoming[i] == 0 && outgoing[i] == 1 + side
            }
            PhysicalStage::CaptureSink { operator, .. }
            | PhysicalStage::BestEffortSink { operator, .. } => {
                sinks.insert(*operator) && incoming[i] == 1 && outgoing[i] == 0
            }
            PhysicalStage::Branch { .. } | PhysicalStage::Route { .. } => {
                incoming[i] == 1 && (1..=16).contains(&outgoing[i])
            }
            PhysicalStage::UnionAll { .. } => (2..=16).contains(&incoming[i]) && outgoing[i] == 1,
            _ => incoming[i] == 1 && outgoing[i] == 1 + side,
        };
        if !valid {
            return Err(invalid(format!(
                "invalid graph chain {i} input/output arity"
            )));
        }
    }
    let mut ready: Vec<_> = (0..stages.len()).filter(|i| incoming[*i] == 0).collect();
    let mut visited = 0;
    let mut lossy = vec![false; stages.len()];
    while let Some(i) = ready.pop() {
        visited += 1;
        if lossy[i]
            && matches!(
                stages[i],
                PhysicalStage::CaptureSink { .. } | PhysicalStage::UnionAll { .. }
            )
        {
            return Err(invalid(
                "lossy branch cannot rejoin or reach a required sink",
            ));
        }
        if !lossy[i] && matches!(stages[i], PhysicalStage::BestEffortSink { .. }) {
            return Err(invalid("best_effort_sink requires an explicit lossy edge"));
        }
        for edge in edges.iter().filter(|e| e.from == i) {
            lossy[edge.to] |= lossy[i] || edge.best_effort;
            incoming[edge.to] -= 1;
            if incoming[edge.to] == 0 {
                ready.push(edge.to);
            }
        }
    }
    if visited != stages.len() {
        return Err(invalid("physical graph contains a cycle"));
    }
    if sources.is_empty() || sinks.is_empty() {
        return Err(invalid("graph requires sources and sinks"));
    }
    let mut time_sources = BTreeSet::new();
    for (source, time) in &req.plan.source_times {
        time.validate()?;
        if !sources.contains(source) || !time_sources.insert(*source) {
            return Err(invalid("unknown/duplicate event-time source"));
        }
        let schema = stages
            .iter()
            .find_map(|s| match s {
                PhysicalStage::MemorySource {
                    operator, schema, ..
                } if operator == source => Some(schema),
                _ => None,
            })
            .unwrap();
        let index = schema
            .index_of_name(&time.field)
            .ok_or_else(|| invalid("source event-time column absent"))?;
        if schema.fields[index].nullable
            || !matches!(
                schema.fields[index].data_type,
                sparrow_model::DataType::Int64 | sparrow_model::DataType::TimestampMicrosUTC
            )
        {
            return Err(invalid("invalid source event-time column type"));
        }
    }
    if req.plan.has_event_time_window() && time_sources != sources {
        return Err(invalid(
            "all event-time graph sources require explicit time bindings",
        ));
    }
    if req.graph_inputs.is_empty() {
        if sources.len() != 1 {
            return Err(invalid("each graph source requires its own input binding"));
        }
    } else if req.graph_inputs.keys().copied().collect::<BTreeSet<_>>() != sources
        || req.live_in.is_some()
        || req.budgeted_in.is_some()
        || req.live_events.is_some()
        || !req.rows.is_empty()
    {
        return Err(invalid(
            "graph input bindings must exactly match source IDs, without legacy input",
        ));
    }
    for input in req.graph_inputs.values() {
        let channels = usize::from(input.events.is_some())
            + usize::from(input.live.is_some())
            + usize::from(input.budgeted.is_some());
        if channels > 1
            || (channels > 0 && (!input.rows.is_empty() || !input.trailing_controls.is_empty()))
        {
            return Err(invalid(
                "each graph input must choose one ordered live or finite memory producer",
            ));
        }
    }
    // Checkpoint participants must remain available after logical EOF so a
    // later cut can still cross every source. Finite/raw row producers cannot
    // receive ordered barriers, even when their data happens to have drained.
    if req.aligned.is_some()
        && if req.graph_inputs.is_empty() {
            req.live_events.is_none() || !req.rows.is_empty()
                || req.live_in.is_some() || req.budgeted_in.is_some()
        } else {
            req.graph_inputs.values().any(|input| input.events.is_none())
        }
    {
        return Err(invalid("aligned graph sources require ordered event inputs"));
    }
    if req.graph_outputs.keys().any(|id| !sinks.contains(id))
        || (req.live_out.is_some() && (sinks.len() != 1 || !req.graph_outputs.is_empty()))
    {
        return Err(invalid("graph output binding is unknown or ambiguous"));
    }
    if req.aligned.is_some()
        && req
            .graph_outputs
            .values()
            .any(|s| s.live.is_some() && s.outbox.is_none())
    {
        return Err(invalid(
            "aligned graph live sinks require individual flush counters",
        ));
    }
    Ok(())
}

fn stage_schema(stage: &PhysicalStage, output: bool) -> Result<&sparrow_model::Schema> {
    Ok(match stage {
        PhysicalStage::MemorySource { schema, .. }
        | PhysicalStage::CaptureSink { schema, .. }
        | PhysicalStage::BestEffortSink { schema, .. } => schema,
        PhysicalStage::Branch { input, .. }
        | PhysicalStage::Route { input, .. }
        | PhysicalStage::UnionAll { input, .. }
        | PhysicalStage::Iot { input, .. }
        | PhysicalStage::Deduplicate { input, .. } => input,
        PhysicalStage::WindowAgg {
            input,
            output: schema,
            ..
        }
        | PhysicalStage::Lookup {
            input,
            output: schema,
            ..
        } => {
            if output {
                schema
            } else {
                input
            }
        }
        PhysicalStage::Transform { steps } => {
            let step = if output { steps.last() } else { steps.first() }
                .ok_or_else(|| invalid("empty graph chain"))?;
            match step {
                sparrow_plan::TransformStep::Filter { input, .. } => input,
                sparrow_plan::TransformStep::Project {
                    input,
                    output: schema,
                    ..
                }
                | sparrow_plan::TransformStep::Map {
                    input,
                    output: schema,
                    ..
                } => {
                    if output {
                        schema
                    } else {
                        input
                    }
                }
            }
        }
    })
}

pub(super) async fn run(ctx: JobCtx, mut req: JobRequest) -> Result<JobStats> {
    let edges = req.plan.edges.take().expect("validated graph");
    let mut cancellations = vec![ctx.cancel.clone(); req.plan.stages.len()];
    let mut optional = vec![false; req.plan.stages.len()];
    let mut incoming = vec![0usize; req.plan.stages.len()];
    for edge in &edges {
        incoming[edge.to] += 1;
    }
    let mut ready: Vec<_> = (0..incoming.len()).filter(|i| incoming[*i] == 0).collect();
    while let Some(from) = ready.pop() {
        for edge in edges.iter().filter(|e| e.from == from) {
            if edge.best_effort {
                cancellations[edge.to] = cancellations[from].child_token();
                optional[edge.to] = true;
            } else if optional[from] {
                cancellations[edge.to] = cancellations[from].clone();
                optional[edge.to] = true;
            }
            incoming[edge.to] -= 1;
            if incoming[edge.to] == 0 {
                ready.push(edge.to);
            }
        }
    }
    let mut inputs: Vec<Vec<MailboxRx>> = (0..req.plan.stages.len()).map(|_| vec![]).collect();
    let mut outputs: Vec<Vec<(PhysicalEdge, MailboxTx)>> =
        (0..req.plan.stages.len()).map(|_| vec![]).collect();
    for (i, edge) in edges.into_iter().enumerate() {
        let (tx, rx) = channel_observed(
            ctx.mailbox,
            cancellations[edge.to].clone(),
            ctx.mailboxes.edge(i),
        )?;
        inputs[edge.to].push(rx);
        outputs[edge.from].push((edge, tx));
    }
    ctx.mailboxes.initialized();
    let mut set = JoinSet::new();
    for (i, stage) in req.plan.stages.into_iter().enumerate() {
        let mut child = clone_ctx(&ctx);
        child.side_port = req
            .plan
            .side_outputs
            .iter()
            .find(|(index, _)| *index == i)
            .map(|(_, side)| side.to);
        child.graph_mode = true;
        child.cancel = cancellations[i].clone();
        child.optional_branch = optional[i];
        let mut input = std::mem::take(&mut inputs[i]);
        let mut output = std::mem::take(&mut outputs[i]);
        match stage {
            PhysicalStage::Branch { operator, .. }
            | PhysicalStage::Route { operator, .. }
            | PhysicalStage::UnionAll { operator, .. } => {
                ctx.live.fetch_add(1, Ordering::SeqCst);
                let guard = LiveTaskGuard {
                    live: ctx.live.clone(),
                };
                let inject_panic = req.inject_panic;
                set.spawn(async move {
                    let _guard = guard;
                    if inject_panic {
                        panic!("injected graph stage panic");
                    }
                    let result = if matches!(stage, PhysicalStage::UnionAll { .. }) {
                        union(&child, input, output.remove(0).1).await
                    } else {
                        router(&child, stage, input.remove(0), output).await
                    };
                    if child.optional_branch && result.is_err() {
                        child.cancel.cancel();
                        child
                            .metrics
                            .graph_branch_failures
                            .fetch_add(1, Ordering::Relaxed);
                        Ok(0)
                    } else {
                        result.map(|_| 0).map_err(|e| e.at_operator(operator))
                    }
                });
            }
            stage => {
                if let Some((_, side)) = req.plan.side_outputs.iter().find(|(index, _)| *index == i)
                {
                    let index = output
                        .iter()
                        .position(|(e, _)| e.port.raw() == side.to)
                        .ok_or_else(|| invalid("side port missing"))?;
                    let (edge, tx) = output.remove(index);
                    child.side_output = Some((tx, edge.best_effort));
                }
                let is_source = matches!(stage, PhysicalStage::MemorySource { .. });
                let source_id = match &stage {
                    PhysicalStage::MemorySource { operator, .. } => Some(*operator),
                    _ => None,
                };
                let sink_id = match &stage {
                    PhysicalStage::CaptureSink { operator, .. }
                    | PhysicalStage::BestEffortSink { operator, .. } => Some(*operator),
                    _ => None,
                };
                child.source_operator = source_id;
                child.source_time = source_id
                    .and_then(|id| {
                        req.plan
                            .source_times
                            .iter()
                            .find(|(source, _)| *source == id)
                    })
                    .map(|(_, binding)| {
                        Arc::new(SourceTime {
                            binding: binding.clone(),
                            last: std::sync::atomic::AtomicI64::new(-1),
                            active: std::sync::atomic::AtomicBool::new(true),
                        })
                    });
                let binding = source_id.and_then(|id| req.graph_inputs.remove(&id));
                let (rows, events, controls, source_live, source_budgeted) =
                    if let Some(binding) = binding {
                        (
                            binding.rows,
                            binding.events,
                            binding.trailing_controls,
                            binding.live,
                            binding.budgeted,
                        )
                    } else if is_source {
                        (
                            std::mem::take(&mut req.rows),
                            req.live_events.take(),
                            std::mem::take(&mut req.trailing_controls),
                            req.live_in.take(),
                            req.budgeted_in.take(),
                        )
                    } else {
                        (vec![], None, vec![], None, None)
                    };
                let sink = sink_id.and_then(|id| req.graph_outputs.remove(&id));
                let (capture, live) = if let Some(sink) = sink {
                    child.sink_outbox = sink.outbox;
                    (sink.capture, sink.live)
                } else {
                    (
                        req.capture.clone(),
                        if sink_id.is_some() {
                            req.live_out.take()
                        } else {
                            None
                        },
                    )
                };
                spawn_stage(
                    &mut set,
                    child,
                    stage,
                    input.pop(),
                    output.pop().map(|(_, tx)| tx),
                    rows,
                    capture,
                    source_live,
                    source_budgeted,
                    live,
                    if is_source {
                        req.live_ctrl.take()
                    } else {
                        None
                    },
                    events,
                    controls,
                    req.inject_panic && !is_source,
                );
            }
        }
    }
    let mut ingested = 0;
    let mut primary = None;
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(rows)) => ingested += rows,
            other => {
                ctx.cancel.cancel();
                let error = match other {
                    Ok(Err(e)) => e,
                    Err(e) => SparrowError::new(
                        ErrorCode::JobFailed,
                        format!("graph stage panicked: {e}"),
                    ),
                    _ => unreachable!(),
                };
                if error.code != ErrorCode::Cancelled && primary.is_none() {
                    primary = Some(error);
                }
            }
        }
    }
    if let Some(error) = primary {
        ctx.metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
        return Err(error.at_job(ctx.pipeline, ctx.attempt));
    }
    Ok(JobStats {
        attempt: ctx.attempt,
        pipeline: ctx.pipeline,
        ingested_rows: ingested,
        captured_rows: req.capture.row_count(),
        cancelled: ctx.cancel.is_cancelled(),
        remaining_work: ctx.min_remaining_work.load(Ordering::Relaxed),
        live_tasks_after: ctx.live.load(Ordering::SeqCst),
        state_keys: ctx.metrics.state_keys.load(Ordering::Relaxed) as usize,
        state_bytes: ctx.metrics.state_bytes.load(Ordering::Relaxed) as usize,
        timers_live: ctx.timers.live.load(Ordering::Relaxed),
        timers_cancelled: ctx.timers.cancelled.load(Ordering::Relaxed),
    })
}

async fn publish(ctx: &JobCtx, edge: &PhysicalEdge, tx: &MailboxTx, batch: RowBatch) -> Result<()> {
    if edge.best_effort {
        let rows = batch.num_rows();
        match tx.try_send(batch).await {
            Ok(true) => {}
            Ok(false) => {
                ctx.metrics
                    .graph_dropped_rows
                    .fetch_add(rows as u64, Ordering::Relaxed);
            }
            Err(e) if e.code == ErrorCode::Cancelled => {
                ctx.metrics
                    .graph_dropped_rows
                    .fetch_add(rows as u64, Ordering::Relaxed);
            }
            Err(e) => return Err(e),
        }
    } else {
        tx.send(batch).await?;
    }
    Ok(())
}

pub(super) async fn publish_side(ctx: &JobCtx, batch: RowBatch) -> Result<()> {
    let Some((tx, drop_when_full)) = &ctx.side_output else {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "decode error has no explicit side port",
        ));
    };
    let rows = batch.num_rows() as u64;
    ctx.metrics
        .graph_side_rows
        .fetch_add(rows, Ordering::Relaxed);
    let sent = if *drop_when_full {
        tx.try_send(batch).await?
    } else {
        tx.send(batch).await?
    };
    if !sent && !ctx.cancel.is_cancelled() {
        ctx.metrics
            .graph_dropped_rows
            .fetch_add(rows, Ordering::Relaxed);
    }
    Ok(())
}

/// Controls are never silently dropped. A lossy branch unable to accept one
/// is detached for this attempt, so it cannot continue with stale time/cuts.
pub(super) async fn publish_control(
    ctx: &JobCtx,
    tx: &MailboxTx,
    best_effort: bool,
    control: StreamControl,
) -> Result<()> {
    if best_effort {
        if !matches!(tx.try_send_control(control).await, Ok(true)) {
            if tx.cancel_path() {
                ctx.metrics
                    .graph_detached_branches
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    } else {
        tx.send_control(control).await?;
    }
    Ok(())
}

async fn router(
    ctx: &JobCtx,
    stage: PhysicalStage,
    mut rx: MailboxRx,
    output: Vec<(PhysicalEdge, MailboxTx)>,
) -> Result<()> {
    let route = if let PhysicalStage::Route {
        input,
        mode,
        cases,
        default,
        ..
    } = stage
    {
        let compiled = cases
            .iter()
            .map(|(expr, dest)| {
                let bound = sparrow_expr::bind(expr, &input)?;
                let allocation = sparrow_expr::allocation::AllocationBound::for_expr(&bound);
                let port = output
                    .iter()
                    .position(|(e, _)| e.port == *dest)
                    .ok_or_else(|| invalid("route port not wired"))?;
                Ok((bound, allocation, port))
            })
            .collect::<Result<Vec<_>>>()?;
        let default = output
            .iter()
            .position(|(e, _)| e.port == default)
            .ok_or_else(|| invalid("route default not wired"))?;
        Some((mode, compiled, default))
    } else {
        None
    };
    let mut ended = false;
    while let Some(mut envelope) = rx.recv().await? {
        let (batch, control) = envelope.take();
        if matches!(control, Some(StreamControl::EndOfInput)) {
            ended = true;
        }
        if ended && batch.is_some() {
            return Err(invalid("router data after EOF"));
        }
        if let Some(batch) = batch {
            if let Some((mode, cases, default)) = &route {
                let metadata = batch
                    .num_rows()
                    .saturating_mul(2)
                    .saturating_add(batch.schema().fields.len().saturating_mul(8))
                    .saturating_add(128);
                let mut scratch = ctx.owner.acquire(CreditKind::Reservation, metadata)?;
                let mut columns = vec![0usize; batch.schema().fields.len()];
                let mut row_bytes = 0;
                for row in batch.rows() {
                    row_bytes = row_bytes.max(row.resident_bytes());
                    for (size, value) in columns.iter_mut().zip(&row.values) {
                        *size = (*size).max(value.resident_bytes());
                    }
                }
                let expression_bytes = cases
                    .iter()
                    .map(|(_, bound, _)| bound.estimate(&columns).allocated)
                    .max()
                    .unwrap_or(0);
                scratch.grow_to(
                    metadata
                        .saturating_add(expression_bytes)
                        .saturating_add(row_bytes.saturating_mul(2)),
                )?;
                let mut masks = Vec::with_capacity(batch.num_rows());
                for row in batch.rows() {
                    consume_work(ctx, cases.len() as u64).await?;
                    let mut mask = 0u16;
                    for (expr, _, port) in cases {
                        if matches!(
                            sparrow_expr::eval_bound(expr, &row.values)?,
                            Scalar::Bool(true)
                        ) {
                            mask |= 1 << port;
                            if *mode == RouteMode::FirstMatch {
                                break;
                            }
                        }
                    }
                    if mask == 0 {
                        mask = 1 << default;
                    }
                    masks.push(mask);
                }
                for (port, (edge, tx)) in output.iter().enumerate() {
                    if ctx.cancel.is_cancelled() {
                        return Ok(());
                    }
                    let mut builder = RowBatchBuilder::new(
                        batch.schema_arc(),
                        ctx.owner.clone(),
                        CreditKind::Reservation,
                        batch.num_rows().min(ctx.owner.budget().max_rows),
                        ctx.mailbox
                            .max_bytes
                            .min(ctx.owner.budget().reservation_bytes),
                    )?;
                    for (row, mask) in batch.rows().iter().zip(&masks) {
                        if mask & (1 << port) != 0 {
                            builder.push_accounted(row.clone(), row.resident_bytes())?;
                        }
                    }
                    if builder.num_rows() != 0 {
                        if ctx.side_port == Some(edge.port.raw()) {
                            ctx.metrics
                                .graph_side_rows
                                .fetch_add(builder.num_rows() as u64, Ordering::Relaxed);
                        }
                        let routed = builder
                            .finish()?
                            .with_origin(batch.origin())
                            .with_source_operator(batch.source_operator());
                        publish(ctx, edge, tx, routed).await?;
                    }
                }
            } else {
                // Continuation is the bounded output index. Share payload, hold
                // the input lease until every required branch has accepted it.
                for (edge, tx) in &output {
                    if ctx.cancel.is_cancelled() {
                        return Ok(());
                    }
                    publish(ctx, edge, tx, batch.share()).await?;
                }
            }
        }
        if let Some(control) = control {
            if let StreamControl::CheckpointBarrier { checkpoint_id } = control {
                if !ctx
                    .aligned
                    .as_ref()
                    .is_some_and(|a| a.acks.is_active(checkpoint_id))
                {
                    continue;
                }
            }
            for (edge, tx) in &output {
                publish_control(ctx, tx, edge.best_effort, control.clone()).await?;
            }
        }
    }
    if !ended && !ctx.cancel.is_cancelled() {
        return Err(SparrowError::new(
            ErrorCode::JobFailed,
            "router input closed without EOF",
        ));
    }
    Ok(())
}

async fn union(ctx: &JobCtx, mut inputs: Vec<MailboxRx>, output: MailboxTx) -> Result<()> {
    let mut hub = crate::WatermarkHub::with_capacity(inputs.len() as u16);
    for i in 0..inputs.len() {
        hub.register(InputId(i as u16))?;
    }
    let mut closed = vec![false; inputs.len()];
    let mut ended = vec![false; inputs.len()];
    let mut cursor = 0;
    let mut last = None;
    let mut idle = false;
    let mut blocked = vec![false; inputs.len()];
    let mut aligning = None;
    let mut completed = 0;
    loop {
        let abandoned =
            aligning.and_then(|id| ctx.aligned.as_ref().and_then(|a| a.acks.abandonment(id)));
        if aligning.is_some() && abandoned.is_none() {
            blocked.fill(false);
            for input in &inputs {
                input.barrier_blocked(false);
            }
            aligning = None;
        }
        let next = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Ok(()),
            _ = async { match abandoned { Some(token) => token.cancelled().await, None => std::future::pending().await } } => {
                blocked.fill(false); for input in &inputs {input.barrier_blocked(false);} aligning = None; continue;
            },
            event = std::future::poll_fn(|cx| {
                for offset in 0..inputs.len() {
                    let i = (cursor + offset) % inputs.len();
                    if !closed[i] && !blocked[i] {
                        if let Poll::Ready(envelope) = inputs[i].poll_recv(cx) { return Poll::Ready((i, envelope)); }
                    }
                }
                Poll::Pending
            }) => event,
        };
        let (i, envelope) = next;
        cursor = (i + 1) % inputs.len();
        if let Some(mut envelope) = envelope {
            let (batch, control) = envelope.take();
            if let Some(batch) = batch {
                if ended[i] {
                    return Err(invalid("UnionAll data after EOF"));
                }
                hub.mark_active(InputId(i as u16))?;
                if idle {
                    output
                        .send_control(StreamControl::Active { input: 0 })
                        .await?;
                    idle = false;
                }
                if !output.send(batch).await? {
                    return Ok(());
                }
            }
            if let Some(control) = control {
                match control {
                    StreamControl::ProcessingTime { .. } => return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"ordered processing time is not enabled for DAG Union")),
                    StreamControl::EndOfInput => {
                        ended[i] = true;
                        hub.mark_idle(InputId(i as u16))?;
                        if ended.iter().all(|e| *e) {
                            if idle {
                                output
                                    .send_control(StreamControl::Active { input: 0 })
                                    .await?;
                                idle = false;
                            }
                            output
                                .send_control(StreamControl::Watermark {
                                    input: 0,
                                    wm_micros: i64::MAX,
                                })
                                .await?;
                            output.send_control(StreamControl::EndOfInput).await?;
                            last = Some(i64::MAX);
                        }
                    }
                    StreamControl::Watermark { wm_micros, .. } => {
                        hub.set_watermark(InputId(i as u16), wm_micros)?;
                    }
                    StreamControl::Idle { .. } => {
                        hub.mark_idle(InputId(i as u16))?;
                    }
                    StreamControl::Active { .. } => {
                        hub.mark_active(InputId(i as u16))?;
                    }
                    StreamControl::CheckpointBarrier { checkpoint_id } => {
                        if checkpoint_id <= completed
                            || !ctx
                                .aligned
                                .as_ref()
                                .is_some_and(|a| a.acks.is_active(checkpoint_id))
                        {
                            continue;
                        }
                        if aligning.is_some_and(|id| id != checkpoint_id) {
                            return Err(invalid("conflicting active graph barrier"));
                        }
                        if closed.iter().any(|closed| *closed) {
                            return Err(invalid("UnionAll closed input cannot acknowledge a barrier"));
                        }
                        aligning = Some(checkpoint_id);
                        blocked[i] = true;
                        inputs[i].barrier_blocked(true);
                        if blocked.iter().all(|b| *b) {
                            output
                                .send_control(StreamControl::CheckpointBarrier { checkpoint_id })
                                .await?;
                            completed = checkpoint_id;
                            aligning = None;
                            blocked.fill(false);
                            for input in &inputs {
                                input.barrier_blocked(false);
                            }
                        }
                    }
                }
            }
        } else {
            if aligning.is_some() && !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "UnionAll input closed during barrier alignment",
                ));
            }
            if !ended[i] && !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "UnionAll input closed without EOF",
                ));
            }
            closed[i] = true;
            hub.mark_idle(InputId(i as u16))?;
            if closed.iter().all(|c| *c) {
                return Ok(());
            }
        }
        if ended.iter().all(|e| *e) {
            continue;
        }
        let now_idle = hub.all_idle();
        if now_idle != idle {
            output
                .send_control(if now_idle {
                    StreamControl::Idle { input: 0 }
                } else {
                    StreamControl::Active { input: 0 }
                })
                .await?;
            idle = now_idle;
        }
        if let Some(wm) = hub.progress() {
            if last.is_none_or(|previous| wm > previous) {
                if !output
                    .send_control(StreamControl::Watermark {
                        input: 0,
                        wm_micros: wm,
                    })
                    .await?
                {
                    return Ok(());
                }
                last = Some(wm);
            }
        }
        tokio::task::yield_now().await;
    }
}

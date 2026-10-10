//! K5.4 bounded Preview with a controlled clock.
//!
//! A single synchronous run of an existing linear physical plan on the real
//! Kernel. Time never comes from the host: the job uses a virtual clock pinned
//! at `start_micros`, and processing time moves only through in-band
//! `ProcessingTime` controls, just like the paused-time production profile.
//! Each event is followed by an in-memory checkpoint barrier. Its participant
//! acknowledgement is the step boundary: every stage has applied the event
//! (and any timers it made due) before the next event is sent. No sleeps and
//! no scheduling guesses. Nothing is persisted. The barrier is only used as a
//! synchronisation point and snapshots are never encoded.
use crate::{
    AlignedAcks, AlignedJob, IngressEvent, JobRequest, Kernel, KernelOptions, MailboxConfig,
    PipelineRestore, SharedCapture, StreamControl,
};
use sparrow_model::{
    ErrorCode, InflightCounter, OutputSequence, ResourceBudget, Result, Row, RowBatch, Schema,
    SharedVirtualClock, SparrowError,
};
use sparrow_plan::{CheckpointPlan, PhysicalPlan, PhysicalStage};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub enum PreviewEvent {
    Data(Row),
    /// Absolute logical processing time; must not move backwards.
    AdvanceClock(i64),
    /// Event-time watermark; must not move backwards.
    Watermark(i64),
    /// Permanent end of input. Nothing may follow.
    Eof,
}

#[derive(Clone, Copy, Debug)]
pub struct PreviewLimits {
    pub events: usize,
    pub output_rows: usize,
    pub output_bytes: usize,
    pub timeout_ms: u64,
    pub work_units: u64,
}
impl Default for PreviewLimits {
    fn default() -> Self {
        Self { events: 256, output_rows: 1000, output_bytes: 1024 * 1024, timeout_ms: 5000, work_units: 100_000 }
    }
}
impl PreviewLimits {
    pub const MAX: Self = Self { events: 512, output_rows: 5000, output_bytes: 2 * 1024 * 1024, timeout_ms: 10_000, work_units: 1_000_000 };
    pub fn validate(&self) -> Result<()> {
        let m = Self::MAX;
        if self.events == 0 || self.events > m.events || self.output_rows == 0 || self.output_rows > m.output_rows
            || self.output_bytes == 0 || self.output_bytes > m.output_bytes || self.timeout_ms == 0
            || self.timeout_ms > m.timeout_ms || self.work_units == 0 || self.work_units > m.work_units
        {
            return Err(bound("preview limits exceed the hard caps"));
        }
        Ok(())
    }
}

pub struct PreviewStep {
    pub clock_micros: i64,
    /// -1 means no watermark yet.
    pub watermark_micros: i64,
    pub batches: Vec<RowBatch>,
}
pub struct PreviewResult {
    pub schema: Schema,
    pub steps: Vec<PreviewStep>,
    pub input_rows: usize,
    pub output_rows: usize,
    pub eof: bool,
    pub future_dropped: u64,
}

fn bound(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::BoundExceeded, s)
}
fn unsupported(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::FeatureUnavailable, s)
}

/// Shape gate: one memory source, linear transforms/windows/IoT, one capture.
/// Anything that could touch I/O, external state or untrusted code is refused.
pub fn check_plan(plan: &PhysicalPlan) -> Result<()> {
    if plan.edges.is_some() || !plan.side_outputs.is_empty() || !plan.source_times.is_empty() {
        return Err(unsupported("preview supports linear plans only (no DAG, side outputs or per-source time bindings)"));
    }
    let n = plan.stages.len();
    if !(2..=32).contains(&n)
        || !matches!(plan.stages.first(), Some(PhysicalStage::MemorySource { .. }))
        || !matches!(plan.stages.last(), Some(PhysicalStage::CaptureSink { .. }))
    {
        return Err(unsupported("preview needs exactly one source and one sink"));
    }
    for s in &plan.stages[1..n - 1] {
        if !matches!(s, PhysicalStage::Transform { .. } | PhysicalStage::WindowAgg { .. } | PhysicalStage::Iot { .. }) {
            return Err(unsupported("preview supports Filter/Project, windows and IoT operators only; Lookup, Join, analysis and plugins are not executed"));
        }
    }
    let mut unsafe_plugin = false;
    plan.visit_plugins(&mut |f| unsafe_plugin |= !f.is_preemptible());
    if unsafe_plugin || plan.has_external_plugins() {
        return Err(unsupported("preview rejects native/external/non-preemptible functions"));
    }
    // Steps are synchronised by in-memory barriers, so the plan must have a
    // checkpoint shape (scalar source columns, supported state kinds).
    CheckpointPlan::from_physical(plan)
        .map(|_| ())
        .map_err(|e| unsupported(&format!("preview cannot step this plan: {}", e.message)))
}

/// Blocking boundary: call on a bounded worker thread.
pub fn execute(
    plan: PhysicalPlan,
    start_micros: i64,
    events: Vec<PreviewEvent>,
    limits: PreviewLimits,
    cancel: CancellationToken,
) -> Result<PreviewResult> {
    limits.validate()?;
    check_plan(&plan)?;
    if events.is_empty() || events.len() > limits.events {
        return Err(bound("preview event count out of range"));
    }
    if start_micros < 0 {
        return Err(SparrowError::new(ErrorCode::InvalidArgument, "start_micros must be >= 0"));
    }
    // Validate the whole timeline before running anything.
    let (mut clock, mut wm, mut eof) = (start_micros, -1i64, false);
    for e in &events {
        if eof {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, "no events may follow eof"));
        }
        match e {
            PreviewEvent::AdvanceClock(t) if *t < clock => {
                return Err(SparrowError::new(ErrorCode::InvalidArgument, "advance_clock cannot move time backwards"))
            }
            PreviewEvent::AdvanceClock(t) => clock = *t,
            PreviewEvent::Watermark(t) if *t < wm => {
                return Err(SparrowError::new(ErrorCode::InvalidArgument, "watermark cannot move backwards"))
            }
            PreviewEvent::Watermark(t) => wm = *t,
            PreviewEvent::Eof => eof = true,
            PreviewEvent::Data(_) => {}
        }
    }
    let input_rows = events.iter().filter(|e| matches!(e, PreviewEvent::Data(_))).count();
    let event_time = plan.has_event_time_window();
    let schema = match plan.stages.last() {
        Some(PhysicalStage::CaptureSink { schema, .. }) => schema.clone(),
        _ => unreachable!(),
    };
    let manifest = Arc::new(CheckpointPlan::from_physical(&plan)?);
    let budget = ResourceBudget { max_rows: 256, max_state_keys: 1024, work_units: 10_000, ..ResourceBudget::compact() };
    let kernel = Kernel::new_with_job_budget(
        KernelOptions { budget, mailbox: MailboxConfig { max_items: 2, max_bytes: 64 * 1024 }, worker_threads: 1, rows_per_batch: 8 },
        budget,
    )?;
    let deadline = std::time::Instant::now() + Duration::from_millis(limits.timeout_ms);
    let mut result = kernel.block_on(async {
        let acks = AlignedAcks::default().with_output_sequence(OutputSequence::new([0x50; 16], 1)?)?;
        let (tx, rx) = sparrow_io::observed::channel(4);
        let (out, mut received) = sparrow_io::observed::channel::<RowBatch>(4);
        let inflight = Arc::new(InflightCounter::new());
        let mut request = JobRequest::new(plan, vec![], SharedCapture::disabled())
            .with_clock(crate::RuntimeClock::virtual_clock(SharedVirtualClock::new(start_micros)))
            .with_live_events(rx)
            .with_live_out(out)
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    analysis: Vec::new(), buffered: Vec::new(), sink: None, plan: manifest,
                    generation: [0x50; 16], restore: None, iot: Vec::new(),
                }),
                acks: acks.clone(),
                outbox: inflight.clone(),
            });
        request.work_lifetime = Some(limits.work_units);
        request.script_deadline = Some(deadline);
        let job = kernel.submit(request)?;
        let fail = |e: SparrowError| e;
        let mut steps = Vec::with_capacity(events.len());
        let (mut clock, mut wm) = (start_micros, -1i64);
        let (mut rows_out, mut bytes_out, mut eof) = (0usize, 0usize, false);
        let run = async {
            for (i, event) in events.into_iter().enumerate() {
                if cancel.is_cancelled() {
                    return Err(SparrowError::new(ErrorCode::Cancelled, "preview cancelled"));
                }
                let control = match event {
                    PreviewEvent::Data(row) => Some(IngressEvent::Row(row)),
                    PreviewEvent::AdvanceClock(t) => { clock = t; Some(IngressEvent::Control(StreamControl::ProcessingTime { micros: t })) }
                    PreviewEvent::Watermark(t) => { wm = t; Some(IngressEvent::Control(StreamControl::Watermark { input: 0, wm_micros: t })) }
                    PreviewEvent::Eof => {
                        eof = true;
                        // Finite end: event-time state closes at +inf, the
                        // same rule as /v1/query. PT timers stay where they are.
                        if event_time {
                            wm = i64::MAX;
                            Some(IngressEvent::Control(StreamControl::Watermark { input: 0, wm_micros: i64::MAX }))
                        } else {
                            None
                        }
                    }
                };
                let id = i as u64 + 1;
                let pending = acks.begin(id)?;
                let send = async {
                    if let Some(control) = control {
                        tx.send(control).await.map_err(|_| SparrowError::new(ErrorCode::JobFailed, "preview job stopped"))?;
                    }
                    tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier { checkpoint_id: id }))
                        .await
                        .map_err(|_| SparrowError::new(ErrorCode::JobFailed, "preview job stopped"))
                };
                let mut batches = Vec::new();
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(SparrowError::new(ErrorCode::Cancelled, "preview deadline exceeded"));
                }
                // Drain output while waiting so a full sink can never stall the barrier.
                let wait = pending.wait_participants(remaining);
                tokio::pin!(send, wait);
                let mut sent = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Err(SparrowError::new(ErrorCode::Cancelled, "preview cancelled")),
                        r = &mut send, if !sent => { r?; sent = true; }
                        b = received.recv() => match b {
                            Some(b) => { inflight.ack(); push(&mut batches, b, &mut rows_out, &mut bytes_out, &limits)?; }
                            None => return Err(SparrowError::new(ErrorCode::JobFailed, "preview output closed early")),
                        },
                        r = &mut wait, if sent => { r?; break; }
                    }
                }
                while let Ok(b) = received.try_recv() {
                    inflight.ack();
                    push(&mut batches, b, &mut rows_out, &mut bytes_out, &limits)?;
                }
                steps.push(PreviewStep { clock_micros: clock, watermark_micros: wm, batches });
            }
            Ok(())
        };
        let outcome = run.await;
        drop(tx);
        let stop = job.stop().await;
        while received.try_recv().is_ok() { inflight.ack(); }
        // A job failure closes the output first; report the job's own error.
        if let (Err(o), Err(e)) = (&outcome, &stop) {
            if o.code == ErrorCode::JobFailed && e.code != ErrorCode::Cancelled {
                return Err(e.clone());
            }
        }
        outcome.map_err(fail)?;
        if let Err(e) = stop {
            if e.code != ErrorCode::Cancelled { return Err(e); }
        }
        Ok(PreviewResult { schema, steps, input_rows, output_rows: rows_out, eof, future_dropped: 0 })
    })?;
    result.future_dropped = kernel.metrics.snapshot().future_dropped;
    Ok(result)
}

fn push(into: &mut Vec<RowBatch>, b: RowBatch, rows: &mut usize, bytes: &mut usize, l: &PreviewLimits) -> Result<()> {
    *rows = rows.saturating_add(b.num_rows());
    *bytes = bytes.saturating_add(b.tracked_bytes());
    if *rows > l.output_rows || *bytes > l.output_bytes {
        return Err(bound("preview output rows/bytes exceeded; no partial successful response"));
    }
    into.push(b);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{AggFn, DataType, Field, Scalar, WindowKind};
    use sparrow_plan::{AggCall, WindowSpec};

    fn schema() -> Schema {
        Schema::new(1, vec![Field::new(1, "id", DataType::Utf8, false), Field::new(2, "v", DataType::Int64, true)]).unwrap()
    }
    fn plan(kind: WindowKind) -> PhysicalPlan {
        let input = schema();
        let spec = WindowSpec::new(kind, vec!["id".into()], vec![AggCall::new(AggFn::Sum, Some(sparrow_expr::Expr::Column { name: "v".into() }), "v")]);
        let output = sparrow_plan::window_output_schema(&input, &spec).unwrap();
        PhysicalPlan {
            pipeline: 9.into(), revision: 1.into(), edges: None, source_times: vec![], side_outputs: vec![],
            stages: vec![
                PhysicalStage::MemorySource { operator: 1.into(), name: "s".into(), schema: input.clone() },
                PhysicalStage::WindowAgg { operator: 10.into(), spec, input, output: output.clone() },
                PhysicalStage::CaptureSink { operator: 30.into(), name: "out".into(), schema: output },
            ],
        }
    }
    fn row(v: i64) -> PreviewEvent { PreviewEvent::Data(Row { values: vec![Scalar::utf8("a"), Scalar::Int64(v)] }) }
    fn counts(r: &PreviewResult) -> Vec<usize> { r.steps.iter().map(|s| s.batches.iter().map(RowBatch::num_rows).sum()).collect() }

    #[test]
    fn pt_tumbling_fires_exactly_on_the_advance_step() {
        let ev = vec![row(2), PreviewEvent::AdvanceClock(50), row(3), PreviewEvent::AdvanceClock(99), PreviewEvent::AdvanceClock(100), row(7), PreviewEvent::AdvanceClock(200)];
        for _ in 0..3 {
            let r = execute(plan(WindowKind::TumblingProcessingTime { size_micros: 100 }), 0, ev.clone(), PreviewLimits::default(), CancellationToken::new()).unwrap();
            assert_eq!(counts(&r), vec![0, 0, 0, 0, 1, 0, 1]);
            let idx = r.schema.index_of_name("v").unwrap();
            assert_eq!(r.steps[4].batches[0].rows()[0].values[idx], Scalar::Int64(5));
            assert_eq!(r.steps[6].clock_micros, 200);
        }
    }
    #[test]
    fn count_window_and_timeline_rules() {
        let r = execute(plan(WindowKind::Count { size: 2 }), 0, vec![row(1), row(2), row(3), PreviewEvent::Eof], PreviewLimits::default(), CancellationToken::new()).unwrap();
        assert_eq!(counts(&r)[..3], [0, 1, 0]);
        assert!(r.eof);
        let p = || plan(WindowKind::Count { size: 2 });
        let e = |ev| execute(p(), 10, ev, PreviewLimits::default(), CancellationToken::new()).err().unwrap().message;
        assert!(e(vec![PreviewEvent::AdvanceClock(5)]).contains("backwards"));
        assert!(e(vec![PreviewEvent::Watermark(5), PreviewEvent::Watermark(4)]).contains("backwards"));
        assert!(e(vec![PreviewEvent::Eof, row(1)]).contains("eof"));
        let tight = PreviewLimits { output_rows: 1, ..Default::default() };
        let err = execute(p(), 0, vec![row(1), row(2), row(3), row(4)], tight, CancellationToken::new()).err().unwrap();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        assert!(PreviewLimits { events: 513, ..Default::default() }.validate().is_err());
    }
}

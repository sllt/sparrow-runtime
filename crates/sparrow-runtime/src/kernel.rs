//! Job supervisor and ExecutionChain. One Tokio task per physical stage.
//! Stop cancels every mailbox send/recv; live task count must return to 0.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use std::collections::HashMap;

use sparrow_model::{
    DeliveryContract, ErrorCode, JobAttemptId, MemoryOwner, PipelineId, ResourceBudget, RestoreClaim,
    Result, Row, RowBatch, SparrowError, WorkBudget,
};
use sparrow_plan::{PhysicalPlan, PhysicalStage};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::capture::SharedCapture;
use crate::clock::RuntimeClock;
use crate::dedup::DedupOperator;
use crate::lookup::{LookupOperator, ReferenceTable};
use crate::mailbox::{channel, MailboxConfig, MailboxRx, MailboxTx};
use crate::transform::{apply_steps, build_source_batches};
use crate::window::WindowOperator;

#[derive(Clone, Debug)]
pub struct KernelOptions {
    pub budget: ResourceBudget,
    pub mailbox: MailboxConfig,
    pub worker_threads: usize,
    pub rows_per_batch: usize,
}

impl Default for KernelOptions {
    fn default() -> Self {
        Self {
            budget: ResourceBudget::compact(),
            mailbox: MailboxConfig::default(),
            worker_threads: 2,
            rows_per_batch: 8,
        }
    }
}

/// Kernel job. Live I/O is optional channels so MQTT/HTTP stay out of this crate.
pub struct JobRequest {
    pub plan: PhysicalPlan,
    pub rows: Vec<Row>,
    pub capture: SharedCapture,
    /// Live ingress. When set, `MemorySource` reads rows here instead of `rows`.
    pub live_in: Option<tokio::sync::mpsc::Receiver<Row>>,
    /// Live egress. `CaptureSink` also forwards batches here (bounded backpressure).
    pub live_out: Option<tokio::sync::mpsc::Sender<RowBatch>>,
    /// Process clock. Virtual clocks are required for deterministic PT windows.
    pub clock: RuntimeClock,
    /// Static reference-table snapshots frozen at submit. Running jobs keep these Arcs.
    pub tables: HashMap<String, std::sync::Arc<ReferenceTable>>,
}

impl JobRequest {
    pub fn new(plan: PhysicalPlan, rows: Vec<Row>, capture: SharedCapture) -> Self {
        Self {
            plan,
            rows,
            capture,
            live_in: None,
            live_out: None,
            clock: RuntimeClock::wall(),
            tables: HashMap::new(),
        }
    }

    pub fn with_clock(mut self, clock: RuntimeClock) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_tables(mut self, tables: HashMap<String, std::sync::Arc<ReferenceTable>>) -> Self {
        self.tables = tables;
        self
    }

    pub fn with_live_io(
        mut self,
        live_in: tokio::sync::mpsc::Receiver<Row>,
        live_out: tokio::sync::mpsc::Sender<RowBatch>,
    ) -> Self {
        self.live_in = Some(live_in);
        self.live_out = Some(live_out);
        self
    }
}

#[derive(Clone, Debug)]
pub struct JobStats {
    pub attempt: JobAttemptId,
    pub pipeline: PipelineId,
    pub ingested_rows: usize,
    pub captured_rows: usize,
    pub cancelled: bool,
    pub remaining_work: u64,
    pub live_tasks_after: usize,
    pub state_keys: usize,
    pub state_bytes: usize,
    pub timers_live: usize,
    pub timers_cancelled: u64,
}

pub struct Kernel {
    rt: tokio::runtime::Runtime,
    opts: KernelOptions,
    next_attempt: AtomicU64,
    live_tasks: Arc<AtomicUsize>,
}

impl Kernel {
    pub fn new(opts: KernelOptions) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(opts.worker_threads.max(2))
            .enable_all()
            .thread_name("sparrow-kernel")
            .build()
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("tokio runtime: {e}")))?;
        Ok(Self {
            rt,
            opts,
            next_attempt: AtomicU64::new(1),
            live_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn live_tasks(&self) -> usize {
        self.live_tasks.load(Ordering::SeqCst)
    }

    /// Tokio handle for composition-root connector tasks (MQTT/HTTP).
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }

    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    pub fn submit(&self, req: JobRequest) -> Result<JobHandle> {
        DeliveryContract::V0_2.validate_restore(&RestoreClaim::None)?;
        admit(&self.opts, &req.plan)?;
        let attempt = JobAttemptId::new(self.next_attempt.fetch_add(1, Ordering::SeqCst));
        let cancel = CancellationToken::new();
        let owner = MemoryOwner::new(self.opts.budget);
        let work = Arc::new(WorkBudget::new(self.opts.budget.work_units));
        let ctx = JobCtx {
            pipeline: req.plan.pipeline,
            attempt,
            owner,
            work,
            cancel: cancel.clone(),
            live: Arc::clone(&self.live_tasks),
            mailbox: self.opts.mailbox,
            rows_per_batch: self.opts.rows_per_batch,
            clock: req.clock.clone(),
            tables: req.tables.clone(),
            max_state_keys: self.opts.budget.max_state_keys,
            max_timers: self.opts.budget.max_timers,
        };
        let handle = self.rt.spawn(run_job(ctx, req));
        Ok(JobHandle {
            attempt,
            cancel,
            handle,
            live: Arc::clone(&self.live_tasks),
        })
    }

    pub fn run(&self, req: JobRequest) -> Result<JobStats> {
        let h = self.submit(req)?;
        self.block_on(h.wait())
    }
}

fn admit(opts: &KernelOptions, plan: &PhysicalPlan) -> Result<()> {
    let mailboxes = plan.mailbox_count().max(1);
    let need = opts.mailbox.max_bytes.saturating_mul(mailboxes);
    if need > opts.budget.queue_bytes {
        return Err(SparrowError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "queue admission: {mailboxes} mailboxes × {}B > queue cap {}",
                opts.mailbox.max_bytes, opts.budget.queue_bytes
            ),
        )
        .retryable(true));
    }
    Ok(())
}

struct JobCtx {
    pipeline: PipelineId,
    attempt: JobAttemptId,
    owner: Arc<MemoryOwner>,
    work: Arc<WorkBudget>,
    cancel: CancellationToken,
    live: Arc<AtomicUsize>,
    mailbox: MailboxConfig,
    rows_per_batch: usize,
    clock: RuntimeClock,
    tables: HashMap<String, std::sync::Arc<ReferenceTable>>,
    max_state_keys: usize,
    max_timers: usize,
}

pub struct JobHandle {
    pub attempt: JobAttemptId,
    cancel: CancellationToken,
    handle: JoinHandle<Result<JobStats>>,
    live: Arc<AtomicUsize>,
}

impl JobHandle {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn wait(self) -> Result<JobStats> {
        match self.handle.await {
            Ok(r) => r,
            Err(e) => Err(SparrowError::new(
                ErrorCode::JobFailed,
                format!("job task panicked: {e}"),
            )),
        }
    }

    pub async fn stop(self) -> Result<JobStats> {
        self.cancel.cancel();
        let mut stats = self.wait().await?;
        stats.cancelled = true;
        Ok(stats)
    }

    pub fn live_tasks(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

async fn run_job(ctx: JobCtx, req: JobRequest) -> Result<JobStats> {
    let n = req.plan.stages.len();
    if n < 2 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "physical plan needs source and sink",
        ));
    }

    let mut txs = Vec::new();
    let mut rxs: Vec<Option<MailboxRx>> = Vec::new();
    for _ in 0..n.saturating_sub(1) {
        let (tx, rx) = channel(ctx.mailbox, ctx.cancel.clone())?;
        txs.push(tx);
        rxs.push(Some(rx));
    }

    let JobRequest {
        plan,
        rows,
        capture,
        mut live_in,
        live_out,
        clock: _,
        tables: _,
    } = req;

    let mut joins = Vec::new();
    for (i, stage) in plan.stages.iter().cloned().enumerate() {
        let tx = if i + 1 < n {
            Some(txs[i].clone())
        } else {
            None
        };
        let rx = if i == 0 {
            None
        } else {
            rxs[i - 1].take()
        };
        let ctx = clone_ctx(&ctx);
        let is_source = matches!(stage, PhysicalStage::MemorySource { .. });
        let is_sink = matches!(stage, PhysicalStage::CaptureSink { .. });
        let rows = if is_source { rows.clone() } else { Vec::new() };
        let capture = capture.clone();
        let stage_live_in = if is_source { live_in.take() } else { None };
        let stage_live_out = if is_sink { live_out.clone() } else { None };
        joins.push(spawn_stage(
            ctx,
            stage,
            rx,
            tx,
            rows,
            capture,
            stage_live_in,
            stage_live_out,
        ));
    }
    drop(txs);

    let mut ingested = 0usize;
    let mut first_err = None;
    for j in joins {
        match j.await {
            Ok(Ok(n)) => ingested += n,
            Ok(Err(e)) => {
                ctx.cancel.cancel();
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                ctx.cancel.cancel();
                if first_err.is_none() {
                    first_err = Some(SparrowError::new(
                        ErrorCode::JobFailed,
                        format!("stage panicked: {e}"),
                    ));
                }
            }
        }
    }
    if let Some(e) = first_err {
        if e.code != ErrorCode::Cancelled && !ctx.cancel.is_cancelled() {
            return Err(e.at_job(ctx.pipeline, ctx.attempt));
        }
    }
    Ok(JobStats {
        attempt: ctx.attempt,
        pipeline: ctx.pipeline,
        ingested_rows: ingested,
        captured_rows: capture.row_count(),
        cancelled: ctx.cancel.is_cancelled(),
        remaining_work: ctx.work.remaining(),
        live_tasks_after: ctx.live.load(Ordering::SeqCst),
        state_keys: 0,
        state_bytes: 0,
        timers_live: 0,
        timers_cancelled: 0,
    })
}

fn clone_ctx(ctx: &JobCtx) -> JobCtx {
    JobCtx {
        pipeline: ctx.pipeline,
        attempt: ctx.attempt,
        owner: Arc::clone(&ctx.owner),
        work: Arc::clone(&ctx.work),
        cancel: ctx.cancel.clone(),
        live: Arc::clone(&ctx.live),
        mailbox: ctx.mailbox,
        rows_per_batch: ctx.rows_per_batch,
        clock: ctx.clock.clone(),
        tables: ctx.tables.clone(),
        max_state_keys: ctx.max_state_keys,
        max_timers: ctx.max_timers,
    }
}

fn spawn_stage(
    ctx: JobCtx,
    stage: PhysicalStage,
    rx: Option<MailboxRx>,
    tx: Option<MailboxTx>,
    rows: Vec<Row>,
    capture: SharedCapture,
    live_in: Option<tokio::sync::mpsc::Receiver<Row>>,
    live_out: Option<tokio::sync::mpsc::Sender<RowBatch>>,
) -> JoinHandle<Result<usize>> {
    ctx.live.fetch_add(1, Ordering::SeqCst);
    let live = Arc::clone(&ctx.live);
    tokio::spawn(async move {
        let result = stage_loop(ctx, stage, rx, tx, rows, capture, live_in, live_out).await;
        live.fetch_sub(1, Ordering::SeqCst);
        result
    })
}

async fn stage_loop(
    ctx: JobCtx,
    stage: PhysicalStage,
    mut rx: Option<MailboxRx>,
    tx: Option<MailboxTx>,
    rows: Vec<Row>,
    capture: SharedCapture,
    live_in: Option<tokio::sync::mpsc::Receiver<Row>>,
    live_out: Option<tokio::sync::mpsc::Sender<RowBatch>>,
) -> Result<usize> {
    match stage {
        PhysicalStage::MemorySource { schema, .. } => {
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "source missing mailbox")
            })?;
            if let Some(live_in) = live_in {
                return live_source(ctx, schema, tx, live_in).await;
            }
            let batches = build_source_batches(schema, rows, &ctx.owner, ctx.rows_per_batch)?;
            let mut n = 0usize;
            for b in batches {
                n += b.num_rows();
                if !tx.send(b).await? {
                    return Ok(n);
                }
            }
            Ok(n)
        }
        PhysicalStage::Transform { steps } => {
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "transform missing tx")
            })?;
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "transform missing rx")
            })?;
            while let Some(env) = rx.recv().await? {
                if let Some(out) = apply_steps(&env.batch, &steps, &ctx.owner, &ctx.work)? {
                    if !tx.send(out).await? {
                        return Ok(0);
                    }
                }
            }
            Ok(0)
        }
        PhysicalStage::CaptureSink { schema, .. } => {
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "sink missing rx")
            })?;
            while let Some(env) = rx.recv().await? {
                capture.stall.wait_if_stalled(&ctx.cancel).await;
                if ctx.cancel.is_cancelled() {
                    break;
                }
                capture.push(&schema, env.batch.rows());
                if let Some(out) = &live_out {
                    tokio::select! {
                        biased;
                        _ = ctx.cancel.cancelled() => break,
                        sent = out.send(env.batch.share()) => {
                            if sent.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            Ok(0)
        }
        PhysicalStage::WindowAgg {
            operator,
            spec,
            input,
            output: _,
        } => {
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "window missing tx")
            })?;
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "window missing rx")
            })?;
            let mut op = WindowOperator::new(
                operator,
                spec,
                input,
                Arc::clone(&ctx.owner),
                ctx.max_state_keys,
                ctx.max_timers,
            )?;
            window_stage(&ctx, &mut op, rx, &tx).await
        }
        PhysicalStage::Deduplicate {
            operator,
            spec,
            input,
        } => {
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "dedup missing tx")
            })?;
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "dedup missing rx")
            })?;
            let mut op = DedupOperator::new(operator, spec, input, Arc::clone(&ctx.owner))?;
            while let Some(env) = rx.recv().await? {
                ctx.work.consume(env.batch.num_rows() as u64)?;
                let rows = op.on_batch(&env.batch, ctx.clock.now_micros())?;
                if let Some(out) = op.build_batch(rows)? {
                    if !tx.send(out).await? {
                        break;
                    }
                }
            }
            op.cleanup();
            Ok(0)
        }
        PhysicalStage::Lookup {
            spec,
            input,
            output: _,
            ..
        } => {
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "lookup missing tx")
            })?;
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "lookup missing rx")
            })?;
            let table = ctx.tables.get(&spec.table).cloned().ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "reference table '{}' was not attached to this Job (static snapshot required)",
                        spec.table
                    ),
                )
            })?;
            let op = LookupOperator::new(spec, table, input, Arc::clone(&ctx.owner))?;
            while let Some(env) = rx.recv().await? {
                ctx.work.consume(env.batch.num_rows() as u64)?;
                let rows = op.on_batch(&env.batch)?;
                if let Some(out) = op.build_batch(rows)? {
                    if !tx.send(out).await? {
                        break;
                    }
                }
            }
            Ok(0)
        }
    }
}

async fn window_stage(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
) -> Result<usize> {
    let mut input_closed = false;
    let mut n = 0usize;
    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        let deadline = op.peek_deadline();
        if input_closed {
            if deadline.is_none() {
                break;
            }
            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => break,
                _ = ctx.clock.sleep_until(deadline) => {}
            }
            let rows = op.fire_due(ctx.clock.now_micros())?;
            n += rows.len();
            if let Some(out) = op.build_batch(rows)? {
                if !tx.send(out).await? {
                    break;
                }
            }
            continue;
        }
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => break,
            env = rx.recv() => {
                match env? {
                    Some(env) => {
                        ctx.work.consume(env.batch.num_rows() as u64)?;
                        let rows = op.on_batch(&env.batch, ctx.clock.now_micros())?;
                        n += rows.len();
                        if let Some(out) = op.build_batch(rows)? {
                            if !tx.send(out).await? {
                                input_closed = true;
                            }
                        }
                    }
                    None => input_closed = true,
                }
            }
            _ = ctx.clock.sleep_until(deadline) => {
                let rows = op.fire_due(ctx.clock.now_micros())?;
                n += rows.len();
                if let Some(out) = op.build_batch(rows)? {
                    if !tx.send(out).await? {
                        break;
                    }
                }
            }
        }
    }
    op.cleanup();
    Ok(n)
}

async fn live_source(
    ctx: JobCtx,
    schema: sparrow_model::Schema,
    tx: MailboxTx,
    mut live_in: tokio::sync::mpsc::Receiver<Row>,
) -> Result<usize> {
    let mut n = 0usize;
    loop {
        let first = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Ok(n),
            row = live_in.recv() => row,
        };
        let Some(first) = first else {
            return Ok(n);
        };
        let mut buf = vec![first];
        while buf.len() < ctx.rows_per_batch {
            match live_in.try_recv() {
                Ok(row) => buf.push(row),
                Err(_) => break,
            }
        }
        let batches = build_source_batches(schema.clone(), buf, &ctx.owner, ctx.rows_per_batch)?;
        for b in batches {
            n += b.num_rows();
            if !tx.send(b).await? {
                return Ok(n);
            }
        }
    }
}

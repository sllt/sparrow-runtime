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
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::barrier::{wait_outbox, AlignedAck, AlignedJob};
use crate::capture::SharedCapture;
use crate::clock::RuntimeClock;
use crate::dedup::DedupOperator;
use crate::lookup::{LookupOperator, ReferenceTable, VersionedReferenceTable};
use crate::mailbox::{channel, MailboxConfig, MailboxRx, MailboxTx, StreamControl};
use crate::transform::{CompiledTransform, build_source_batches};
use crate::metrics::RuntimeMetrics;
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
    /// V0.3 versioned tables (as-of event time). Running jobs share the handle.
    pub versioned_tables: HashMap<String, VersionedReferenceTable>,
    /// Punctuation sent after memory-source rows (watermarks / idle / active).
    pub trailing_controls: Vec<StreamControl>,
    /// Live watermark / idle marks (optional; alongside `live_in`).
    pub live_ctrl: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    /// Single ordered ingress (row | punctuation). Preferred over split channels.
    pub live_events: Option<tokio::sync::mpsc::Receiver<IngressEvent>>,
    /// Test-only: panic inside the first non-source stage.
    pub inject_panic: bool,
    /// Production aligned recovery hooks (Kernel path; not AlignedSession).
    pub aligned: Option<AlignedJob>,
}

/// Ordered live ingress envelope (R18).
#[derive(Debug)]
pub enum IngressEvent {
    Row(Row),
    Control(StreamControl),
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
            versioned_tables: HashMap::new(),
            trailing_controls: Vec::new(),
            live_ctrl: None,
            live_events: None,
            inject_panic: false,
            aligned: None,
        }
    }

    pub fn with_aligned(mut self, aligned: AlignedJob) -> Self {
        self.aligned = Some(aligned);
        self
    }

    pub fn with_inject_panic(mut self) -> Self {
        self.inject_panic = true;
        self
    }

    pub fn with_live_events(
        mut self,
        live_events: tokio::sync::mpsc::Receiver<IngressEvent>,
    ) -> Self {
        self.live_events = Some(live_events);
        self
    }

    pub fn with_clock(mut self, clock: RuntimeClock) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_tables(mut self, tables: HashMap<String, std::sync::Arc<ReferenceTable>>) -> Self {
        self.tables = tables;
        self
    }

    pub fn with_versioned_tables(
        mut self,
        tables: HashMap<String, VersionedReferenceTable>,
    ) -> Self {
        self.versioned_tables = tables;
        self
    }

    pub fn with_controls(mut self, controls: Vec<StreamControl>) -> Self {
        self.trailing_controls = controls;
        self
    }

    pub fn with_live_ctrl(
        mut self,
        live_ctrl: tokio::sync::mpsc::Receiver<StreamControl>,
    ) -> Self {
        self.live_ctrl = Some(live_ctrl);
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

    pub fn with_live_out(mut self, live_out: tokio::sync::mpsc::Sender<RowBatch>) -> Self {
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

#[derive(Default)]
struct JobTimers {
    live: AtomicUsize,
    cancelled: AtomicU64,
}

struct TimerReporter<'a> {
    ctx: &'a JobCtx,
    live: usize,
    cancelled: u64,
}

impl TimerReporter<'_> {
    fn sample(&mut self, live: usize, cancelled: u64) {
        if live >= self.live {
            self.ctx.timers.live.fetch_add(live - self.live, Ordering::Relaxed);
            self.ctx.metrics.timers_live.fetch_add((live - self.live) as u64, Ordering::Relaxed);
        } else {
            self.ctx.timers.live.fetch_sub(self.live - live, Ordering::Relaxed);
            self.ctx.metrics.timers_live.fetch_sub((self.live - live) as u64, Ordering::Relaxed);
        }
        let delta = cancelled.saturating_sub(self.cancelled);
        self.ctx.timers.cancelled.fetch_add(delta, Ordering::Relaxed);
        self.ctx.metrics.timers_cancelled.fetch_add(delta, Ordering::Relaxed);
        self.live = live;
        self.cancelled = cancelled;
    }
}

impl Drop for TimerReporter<'_> {
    fn drop(&mut self) {
        self.sample(0, self.cancelled.saturating_add(self.live as u64));
    }
}

/// Process-wide queue reservation released when the job task ends.
struct QueueAdmit {
    reserved: Arc<AtomicUsize>,
    jobs: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for QueueAdmit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reserved.fetch_sub(self.bytes, Ordering::SeqCst);
        }
        self.jobs.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct Kernel {
    rt: Option<tokio::runtime::Runtime>,
    opts: KernelOptions,
    job_budget: ResourceBudget,
    admit_lock: std::sync::Mutex<()>,
    next_attempt: AtomicU64,
    live_tasks: Arc<AtomicUsize>,
    /// Shared by every job. N jobs must not each get an independent
    /// compact [`ResourceBudget`] (P1-20).
    process_owner: Arc<MemoryOwner>,
    queue_reserved: Arc<AtomicUsize>,
    admitted_jobs: Arc<AtomicUsize>,
    pub metrics: Arc<RuntimeMetrics>,
}

impl Kernel {
    pub fn new(opts: KernelOptions) -> Result<Self> {
        let mut job = opts.budget;
        // Default four-way byte isolation. Embedders can explicitly choose
        // process and job budgets through new_with_job_budget.
        job.reservation_bytes = (job.reservation_bytes / 4).max(1);
        job.retention_bytes = (job.retention_bytes / 4).max(1);
        Self::new_with_job_budget(opts, job)
    }

    pub fn new_with_job_budget(opts: KernelOptions, job_budget: ResourceBudget) -> Result<Self> {
        for kind in [sparrow_model::CreditKind::Reservation, sparrow_model::CreditKind::Retention, sparrow_model::CreditKind::Queue] {
            if job_budget.cap(kind) == 0 || job_budget.cap(kind) > opts.budget.cap(kind) {
                return Err(SparrowError::new(ErrorCode::InvalidArgument, "job quota must fit process budget"));
            }
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(opts.worker_threads.max(2))
            .enable_all()
            .thread_name("sparrow-kernel")
            .build()
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("tokio runtime: {e}")))?;
        let process_owner = MemoryOwner::new(opts.budget);
        Ok(Self {
            rt: Some(rt),
            opts,
            job_budget,
            admit_lock: std::sync::Mutex::new(()),
            next_attempt: AtomicU64::new(1),
            live_tasks: Arc::new(AtomicUsize::new(0)),
            process_owner,
            queue_reserved: Arc::new(AtomicUsize::new(0)),
            admitted_jobs: Arc::new(AtomicUsize::new(0)),
            metrics: RuntimeMetrics::new(),
        })
    }

    pub fn live_tasks(&self) -> usize {
        self.live_tasks.load(Ordering::SeqCst)
    }

    pub fn budget(&self) -> ResourceBudget {
        self.opts.budget
    }

    pub fn job_budget(&self) -> ResourceBudget { self.job_budget }

    /// Process-wide memory owner shared by every job on this kernel (P1-20).
    pub fn process_owner(&self) -> &Arc<MemoryOwner> {
        &self.process_owner
    }

    pub fn admitted_jobs(&self) -> usize {
        self.admitted_jobs.load(Ordering::SeqCst)
    }

    pub fn queue_reserved(&self) -> usize {
        self.queue_reserved.load(Ordering::SeqCst)
    }

    /// Tokio handle for composition-root connector tasks (MQTT/HTTP).
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.as_ref().expect("live kernel runtime").handle().clone()
    }

    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.as_ref().expect("live kernel runtime").block_on(f)
    }

    pub fn submit(&self, req: JobRequest) -> Result<JobHandle> {
        let _gate = self.admit_lock.lock().expect("job admission");
        DeliveryContract::V0_3.validate_restore(&RestoreClaim::None)?;
        let job_queue_need = self.opts.mailbox.max_bytes.saturating_mul(req.plan.mailbox_count().max(1));
        if job_queue_need > self.job_budget.queue_bytes {
            return Err(SparrowError::new(ErrorCode::ResourceExhausted,
                format!("job queue quota: plan needs {job_queue_need}B > {}B; reduce mailboxes or increase job budget",
                    self.job_budget.queue_bytes)).retryable(false));
        }
        let queue_need = admit_process(
            &self.opts,
            &req.plan,
            &self.process_owner,
            &self.queue_reserved,
        )?;
        // Reserve each admitted job's quota so a growing job cannot consume
        // a sibling's allowance. Physical allocations still bill the parent.
        let jobs = self.admitted_jobs.load(Ordering::SeqCst).saturating_add(1);
        if jobs.saturating_mul(self.job_budget.retention_bytes) > self.opts.budget.retention_bytes
            || jobs.saturating_mul(self.job_budget.reservation_bytes) > self.opts.budget.reservation_bytes {
            return Err(SparrowError::new(ErrorCode::ResourceExhausted,
                format!("capacity: {}/{} jobs admitted; retry later", jobs - 1,
                    (self.opts.budget.retention_bytes / self.job_budget.retention_bytes.max(1))
                        .min(self.opts.budget.reservation_bytes / self.job_budget.reservation_bytes.max(1))))
                .retryable(true).context("admission", "capacity"));
        }
        self.queue_reserved.fetch_add(queue_need, Ordering::SeqCst);
        self.admitted_jobs.fetch_add(1, Ordering::SeqCst);
        let admit = QueueAdmit {
            reserved: Arc::clone(&self.queue_reserved),
            jobs: Arc::clone(&self.admitted_jobs),
            bytes: queue_need,
        };
        self.metrics
            .jobs_started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let attempt = JobAttemptId::new(self.next_attempt.fetch_add(1, Ordering::SeqCst));
        let cancel = CancellationToken::new();
        let owner = MemoryOwner::child(Arc::clone(&self.process_owner), self.job_budget,
            format!("pipeline={} attempt={}", req.plan.pipeline.raw(), attempt.raw()));
        let work = Arc::new(WorkBudget::new(self.job_budget.work_units));
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
            versioned_tables: req.versioned_tables.clone(),
            max_state_keys: self.job_budget.max_state_keys,
            max_timers: self.job_budget.max_timers,
            timers: Arc::new(JobTimers::default()),
            min_remaining_work: Arc::new(AtomicU64::new(self.job_budget.work_units)),
            metrics: Arc::clone(&self.metrics),
            aligned: req.aligned.clone(),
        };
        let handle = self.rt.as_ref().expect("live kernel runtime").spawn(run_job(ctx, req, admit));
        Ok(JobHandle {
            attempt,
            cancel,
            handle,
            live: Arc::clone(&self.live_tasks),
            metrics: Arc::clone(&self.metrics),
        })
    }

    pub fn run(&self, req: JobRequest) -> Result<JobStats> {
        let h = self.submit(req)?;
        self.block_on(h.wait())
    }
}

impl Drop for Kernel {
    fn drop(&mut self) {
        if let Some(runtime) = self.rt.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                // A failed async boot may own the final Arc. Dropping Tokio's
                // runtime synchronously here would panic and hide the error.
                runtime.shutdown_background();
            } else {
                drop(runtime);
            }
        }
    }
}

fn admit_process(
    opts: &KernelOptions,
    plan: &PhysicalPlan,
    process: &MemoryOwner,
    queue_reserved: &AtomicUsize,
) -> Result<usize> {
    let mailboxes = plan.mailbox_count().max(1);
    let need = opts.mailbox.max_bytes.saturating_mul(mailboxes);
    if need > opts.budget.queue_bytes {
        return Err(SparrowError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "queue admission: {mailboxes} mailboxes × {}B > queue cap {}",
                opts.mailbox.max_bytes, opts.budget.queue_bytes
            ),
        ).retryable(false));
    }
    let live_q = queue_reserved.load(Ordering::SeqCst);
    if live_q.saturating_add(need) > opts.budget.queue_bytes {
        return Err(SparrowError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "capacity: process queue reserved {live_q}B + job {need}B > queue cap {}; retry later",
                opts.budget.queue_bytes
            ),
        )
        .retryable(true).context("admission", "capacity"));
    }
    let usage = process.usage();
    let live_held = usage.retention_bytes.saturating_add(usage.queue_bytes);
    let process_cap = opts
        .budget
        .retention_bytes
        .saturating_add(opts.budget.queue_bytes);
    if live_held.saturating_add(need) > process_cap {
        return Err(SparrowError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "process memory admit: live retained+queue {live_held}B + job mailbox {need}B > process cap {process_cap}B"
            ),
        )
        .retryable(true).context("admission", "capacity"));
    }
    Ok(need)
}

struct JobCtx {
    timers: Arc<JobTimers>,
    min_remaining_work: Arc<AtomicU64>,
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
    versioned_tables: HashMap<String, VersionedReferenceTable>,
    max_state_keys: usize,
    max_timers: usize,
    metrics: Arc<RuntimeMetrics>,
    aligned: Option<AlignedJob>,
}

pub struct JobHandle {
    pub attempt: JobAttemptId,
    cancel: CancellationToken,
    handle: JoinHandle<Result<JobStats>>,
    live: Arc<AtomicUsize>,
    metrics: Arc<RuntimeMetrics>,
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
        self.metrics
            .jobs_stopped
            .fetch_add(1, Ordering::Relaxed);
        match self.wait().await {
            Ok(mut stats) => {
                stats.cancelled = true;
                Ok(stats)
            }
            Err(e) if e.code == ErrorCode::Cancelled => Err(e),
            Err(e) => Err(e),
        }
    }

    pub fn live_tasks(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

async fn run_job(ctx: JobCtx, req: JobRequest, _admit: QueueAdmit) -> Result<JobStats> {
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
        versioned_tables: _,
        trailing_controls,
        mut live_ctrl,
        mut live_events,
        inject_panic,
        aligned: _,
    } = req;

    let mut set = JoinSet::new();
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
        let stage_live_ctrl = if is_source { live_ctrl.take() } else { None };
        let stage_live_events = if is_source { live_events.take() } else { None };
        let stage_controls = if is_source {
            trailing_controls.clone()
        } else {
            Vec::new()
        };
        spawn_stage(
            &mut set,
            ctx,
            stage,
            rx,
            tx,
            rows,
            capture,
            stage_live_in,
            stage_live_out,
            stage_live_ctrl,
            stage_live_events,
            stage_controls,
            inject_panic && !is_source,
        );
    }
    drop(txs);

    let mut ingested = 0usize;
    let mut primary_err = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(n)) => ingested += n,
            Ok(Err(e)) => {
                ctx.cancel.cancel();
                if e.code != ErrorCode::Cancelled && primary_err.is_none() {
                    primary_err = Some(e);
                }
            }
            Err(e) => {
                ctx.cancel.cancel();
                if primary_err.is_none() {
                    primary_err = Some(SparrowError::new(
                        ErrorCode::JobFailed,
                        format!("stage panicked: {e}"),
                    ));
                }
            }
        }
    }
    if let Some(e) = primary_err {
        ctx.metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
        return Err(e.at_job(ctx.pipeline, ctx.attempt));
    }
    ctx.metrics.record_ingest(ingested as u64);
    Ok(JobStats {
        attempt: ctx.attempt,
        pipeline: ctx.pipeline,
        ingested_rows: ingested,
        captured_rows: capture.row_count(),
        cancelled: ctx.cancel.is_cancelled(),
        remaining_work: ctx.min_remaining_work.load(Ordering::Relaxed),
        live_tasks_after: ctx.live.load(Ordering::SeqCst),
        state_keys: ctx.metrics.state_keys.load(Ordering::Relaxed) as usize,
        state_bytes: ctx.metrics.state_bytes.load(Ordering::Relaxed) as usize,
        timers_live: ctx.timers.live.load(Ordering::Relaxed),
        timers_cancelled: ctx.timers.cancelled.load(Ordering::Relaxed),
    })
}

fn clone_ctx(ctx: &JobCtx) -> JobCtx {
    JobCtx {
        timers: Arc::clone(&ctx.timers),
        min_remaining_work: Arc::clone(&ctx.min_remaining_work),
        pipeline: ctx.pipeline,
        attempt: ctx.attempt,
        owner: Arc::clone(&ctx.owner),
        work: Arc::new(WorkBudget::new(ctx.work.cap())),
        cancel: ctx.cancel.clone(),
        live: Arc::clone(&ctx.live),
        mailbox: ctx.mailbox,
        rows_per_batch: ctx.rows_per_batch,
        clock: ctx.clock.clone(),
        tables: ctx.tables.clone(),
        versioned_tables: ctx.versioned_tables.clone(),
        max_state_keys: ctx.max_state_keys,
        max_timers: ctx.max_timers,
        metrics: Arc::clone(&ctx.metrics),
        aligned: ctx.aligned.clone(),
    }
}

async fn consume_work(ctx: &JobCtx, units: u64) -> Result<()> {
    if ctx.work.would_exhaust(units) {
        tokio::task::yield_now().await;
        ctx.work.begin_quantum();
    }
    ctx.work.consume(units)
}

struct LiveTaskGuard {
    live: Arc<AtomicUsize>,
}

impl Drop for LiveTaskGuard {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

fn spawn_stage(
    set: &mut JoinSet<Result<usize>>,
    ctx: JobCtx,
    stage: PhysicalStage,
    rx: Option<MailboxRx>,
    tx: Option<MailboxTx>,
    rows: Vec<Row>,
    capture: SharedCapture,
    live_in: Option<tokio::sync::mpsc::Receiver<Row>>,
    live_out: Option<tokio::sync::mpsc::Sender<RowBatch>>,
    live_ctrl: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    live_events: Option<tokio::sync::mpsc::Receiver<IngressEvent>>,
    trailing_controls: Vec<StreamControl>,
    inject_panic: bool,
) {
    ctx.live.fetch_add(1, Ordering::SeqCst);
    let guard = LiveTaskGuard {
        live: Arc::clone(&ctx.live),
    };
    set.spawn(async move {
        let _guard = guard;
        if inject_panic {
            panic!("injected stage panic");
        }
        let work = Arc::clone(&ctx.work);
        let min_remaining = Arc::clone(&ctx.min_remaining_work);
        let result = stage_loop(
            ctx,
            stage,
            rx,
            tx,
            rows,
            capture,
            live_in,
            live_out,
            live_ctrl,
            live_events,
            trailing_controls,
        )
        .await;
        min_remaining.fetch_min(work.remaining(), Ordering::Relaxed);
        result
    });
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
    live_ctrl: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    live_events: Option<tokio::sync::mpsc::Receiver<IngressEvent>>,
    trailing_controls: Vec<StreamControl>,
) -> Result<usize> {
    match stage {
        PhysicalStage::MemorySource { schema, .. } => {
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "source missing mailbox")
            })?;
            if let Some(events) = live_events {
                return live_source(ctx, schema, tx, LiveInput::Ordered(events)).await;
            }
            if let Some(live_in) = live_in {
                return live_source(ctx, schema, tx, LiveInput::Split { rows: live_in, controls: live_ctrl }).await;
            }
            let batches = build_source_batches(schema, rows, &ctx.owner, ctx.rows_per_batch)?;
            let mut n = 0usize;
            for b in batches {
                n += b.num_rows();
                if !tx.send(b).await? {
                    return Ok(n);
                }
            }
            for c in trailing_controls {
                if !tx.send_control(c).await? {
                    return Ok(n);
                }
            }
            Ok(n)
        }
        PhysicalStage::Transform { steps } => {
            let compiled = CompiledTransform::new(&steps)?;
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "transform missing tx")
            })?;
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "transform missing rx")
            })?;
            while let Some(mut env) = rx.recv().await? {
                let (batch, ctrl) = env.take();
                if let Some(ctrl) = ctrl {
                    if !tx.send_control(ctrl).await? {
                        return Ok(0);
                    }
                }
                if let Some(batch) = batch {
                    if ctx.work.would_exhaust(compiled.work_units(batch.num_rows())) {
                        tokio::task::yield_now().await;
                        ctx.work.begin_quantum();
                    }
                    if let Some(out) = compiled.apply(&batch, &ctx.owner, &ctx.work)? {
                        if !tx.send(out).await? {
                            return Ok(0);
                        }
                    }
                }
            }
            Ok(0)
        }
        PhysicalStage::CaptureSink { schema, .. } => {
            let rx = rx.as_mut().ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "sink missing rx")
            })?;
            while let Some(mut env) = rx.recv().await? {
                capture.stall.wait_if_stalled(&ctx.cancel).await;
                if ctx.cancel.is_cancelled() {
                    break;
                }
                let (batch, ctrl) = env.take();
                if let Some(batch) = batch {
                    ctx.metrics.record_emit(batch.num_rows() as u64);
                    capture.push(&schema, batch.rows());
                    if let Some(out) = &live_out {
                        if let Some(aj) = &ctx.aligned {
                            aj.outbox.enqueue();
                        }
                        tokio::select! {
                            biased;
                            _ = ctx.cancel.cancelled() => break,
                            sent = out.send(batch.share()) => {
                                if sent.is_err() {
                                    if let Some(aj) = &ctx.aligned {
                                        aj.outbox.fail();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                if let Some(StreamControl::CheckpointBarrier { checkpoint_id }) = ctrl {
                    if let Some(aj) = &ctx.aligned {
                        // Timeout or drop is a failed flush, not a job death.
                        // The supervisor refuses commit; late acks keep their id.
                        if !aj.acks.is_active(checkpoint_id) { continue; }
                        let flush = tokio::select! {
                            _ = ctx.cancel.cancelled() => break,
                            flush = wait_outbox(&aj.outbox, std::time::Duration::from_secs(5)) => flush,
                        };
                        // Loss is sticky for this attempt: retrying a barrier
                        // cannot retroactively deliver a dropped batch.
                        if flush.ok && flush.dropped == 0 {
                            aj.outbox.mark();
                        }
                        let _ = aj
                            .acks
                            .send(AlignedAck::SinkFlushed {
                                checkpoint_id,
                                ok: flush.ok,
                                dropped: flush.dropped,
                            })
                            .await;
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
            if let Some(freeze) = ctx.aligned.as_ref().and_then(|a| a.restore.as_ref()) {
                op.restore_freeze(freeze)?;
            }
            window_stage(&ctx, &mut op, rx, &tx, &capture).await
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
            while let Some(mut env) = rx.recv().await? {
                let (batch, ctrl) = env.take();
                if let Some(ctrl) = ctrl {
                    if !tx.send_control(ctrl).await? {
                        break;
                    }
                }
                if let Some(batch) = batch {
                    consume_work(&ctx, batch.num_rows() as u64).await?;
                    let rows = op.on_batch(&batch, ctx.clock.now_micros())?;
                    if let Some(out) = op.build_batch(rows)? {
                        if !tx.send(out).await? {
                            break;
                        }
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
            let op = if let Some(ver) = ctx.versioned_tables.get(&spec.table).cloned() {
                LookupOperator::new_versioned(spec, ver, input, Arc::clone(&ctx.owner))?
            } else {
                let table = ctx.tables.get(&spec.table).cloned().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!(
                            "reference table '{}' was not attached to this Job (static or versioned snapshot required)",
                            spec.table
                        ),
                    )
                })?;
                LookupOperator::new(spec, table, input, Arc::clone(&ctx.owner))?
            };
            while let Some(mut env) = rx.recv().await? {
                let (batch, ctrl) = env.take();
                if let Some(ctrl) = ctrl {
                    if !tx.send_control(ctrl).await? {
                        break;
                    }
                }
                if let Some(batch) = batch {
                    consume_work(&ctx, batch.num_rows() as u64).await?;
                    let rows = op.on_batch(&batch)?;
                    if let Some(out) = op.build_batch(rows)? {
                        if !tx.send(out).await? {
                            break;
                        }
                    }
                }
            }
            Ok(0)
        }
    }
}

async fn emit_window(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    tx: &MailboxTx,
    capture: &SharedCapture,
    emission: crate::window::WindowEmission,
) -> Result<bool> {
    if emission.future_dropped > 0 {
        ctx.metrics.record_future_dropped(emission.future_dropped);
    }
    if !emission.lates.is_empty() {
        capture.push_late(&emission.lates);
    }
    if !send_rows_chunked(ctx, op, tx, emission.finals).await? {
        return Ok(false);
    }
    if let Some(wm) = emission.pending_close {
        loop {
            let chunk = op.take_closed_chunk(wm, ctx.mailbox.max_items.max(1), ctx.mailbox.max_bytes)?;
            if chunk.is_empty() {
                break;
            }
            if !send_rows_chunked(ctx, op, tx, chunk).await? {
                return Ok(false);
            }
        }
        op.advance_holdback(wm)?;
        if !tx
            .send_control(StreamControl::Watermark {
                input: sparrow_model::InputId::SINGLE.raw(),
                wm_micros: wm,
            })
            .await?
        {
            return Ok(false);
        }
    }
    ctx.metrics.record_state(
        ctx.owner.usage().live_handles as u64,
        ctx.owner.usage().retention_bytes as u64,
    );
    if let (Some(wm_in), Some(wm_out)) = (op.wm_in(), op.wm_out()) {
        ctx.metrics.record_watermark_lag(wm_in.saturating_sub(wm_out));
    }
    Ok(true)
}

async fn send_rows_chunked(
    ctx: &JobCtx,
    op: &WindowOperator,
    tx: &MailboxTx,
    rows: Vec<Row>,
) -> Result<bool> {
    let max_rows = ctx.mailbox.max_items.max(1);
    let max_bytes = ctx.mailbox.max_bytes.max(1);
    let mut chunk = Vec::new();
    let mut bytes = 0usize;
    for row in rows {
        let sz = row.tracked_bytes().max(1);
        if !chunk.is_empty() && (chunk.len() >= max_rows || bytes.saturating_add(sz) > max_bytes) {
            if let Some(out) = op.build_batch(std::mem::take(&mut chunk))? {
                ctx.metrics.record_emit(out.num_rows() as u64);
                if !tx.send(out).await? {
                    return Ok(false);
                }
            }
            bytes = 0;
        }
        bytes = bytes.saturating_add(sz);
        chunk.push(row);
    }
    if let Some(out) = op.build_batch(chunk)? {
        ctx.metrics.record_emit(out.num_rows() as u64);
        if !tx.send(out).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn window_stage(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
    capture: &SharedCapture,
) -> Result<usize> {
    let mut timer_reporter = TimerReporter { ctx, live: 0, cancelled: 0 };
    let mut input_closed = false;
    let mut n = 0usize;
    loop {
        timer_reporter.sample(op.live_timers(), op.cancelled_timers());
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
            if !emit_window(
                ctx,
                op,
                tx,
                capture,
                crate::window::WindowEmission {
                    finals: rows,
                    ..crate::window::WindowEmission::default()
                },
            )
            .await?
            {
                break;
            }
            continue;
        }
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => break,
            env = rx.recv() => {
                match env? {
                    Some(mut env) => {
                        let (batch, ctrl) = env.take();
                        if let Some(ctrl) = ctrl {
                            let emission = match ctrl {
                                StreamControl::Watermark { input, wm_micros } => {
                                    op.observe_watermark(sparrow_model::InputId(input), wm_micros)?
                                }
                                StreamControl::Idle { input } => {
                                    op.mark_idle(sparrow_model::InputId(input))?
                                }
                                StreamControl::Active { input } => {
                                    op.mark_active(sparrow_model::InputId(input))?
                                }
                                StreamControl::CheckpointBarrier { checkpoint_id } => {
                                    if let Some(aj) = &ctx.aligned {
                                        if aj.acks.is_active(checkpoint_id) {
                                            let ack = match crate::barrier::EncodedFreeze::from_operator(&op, &ctx.owner, ctx.max_state_keys) {
                                                Ok(freeze) => AlignedAck::WindowFrozen { checkpoint_id, freeze },
                                                Err(error) => AlignedAck::FreezeFailed { checkpoint_id, error },
                                            };
                                            aj.acks.send(ack).await;
                                        }
                                    }
                                    if !tx
                                        .send_control(StreamControl::CheckpointBarrier {
                                            checkpoint_id,
                                        })
                                        .await?
                                    {
                                        return Ok(n);
                                    }
                                    crate::window::WindowEmission::default()
                                }
                            };
                            n += emission.finals.len();
                            if !emit_window(ctx, op, tx, capture, emission).await? {
                                input_closed = true;
                            }
                        }
                        if let Some(batch) = batch {
                            consume_work(ctx, batch.num_rows() as u64).await?;
                            let emission = op.on_batch(&batch, ctx.clock.now_micros())?;
                            n += emission.finals.len();
                            if !emit_window(ctx, op, tx, capture, emission).await? {
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
                if !emit_window(
                    ctx,
                    op,
                    tx,
                    capture,
                    crate::window::WindowEmission {
                        finals: rows,
                        ..crate::window::WindowEmission::default()
                    },
                )
                .await?
                {
                    break;
                }
            }
        }
    }
    op.cleanup();
    timer_reporter.sample(op.live_timers(), op.cancelled_timers());
    Ok(n)
}

/// Legacy split channels are adapted at the boundary; batching, accounting,
/// cancellation and punctuation forwarding have one implementation.
enum LiveInput {
    Ordered(tokio::sync::mpsc::Receiver<IngressEvent>),
    Split {
        rows: tokio::sync::mpsc::Receiver<Row>,
        controls: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    },
}

impl LiveInput {
    async fn recv(&mut self) -> Option<IngressEvent> {
        match self {
            Self::Ordered(rx) => rx.recv().await,
            Self::Split { rows, controls } => loop {
                tokio::select! {
                    biased;
                    row = rows.recv() => return row.map(IngressEvent::Row),
                    control = async {
                        match controls.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    } => match control {
                        Some(control) => return Some(IngressEvent::Control(control)),
                        None => *controls = None,
                    }
                }
            },
        }
    }

    fn try_recv(&mut self) -> std::result::Result<IngressEvent, tokio::sync::mpsc::error::TryRecvError> {
        match self {
            Self::Ordered(rx) => rx.try_recv(),
            Self::Split { rows, .. } => rows.try_recv().map(IngressEvent::Row),
        }
    }

    fn ready_control(&mut self) -> Option<StreamControl> {
        match self {
            Self::Split { controls, .. } => controls.as_mut().and_then(|rx| rx.try_recv().ok()),
            Self::Ordered(_) => None,
        }
    }
}

async fn live_source(
    ctx: JobCtx,
    schema: sparrow_model::Schema,
    tx: MailboxTx,
    mut events: LiveInput,
) -> Result<usize> {
    let mut n = 0usize;
    loop {
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Ok(n),
            ev = events.recv() => {
                let Some(ev) = ev else {
                    return Ok(n);
                };
                match ev {
                    IngressEvent::Row(first) => {
                        let mut buf = vec![first];
                        while buf.len() < ctx.rows_per_batch {
                            match events.try_recv() {
                                Ok(IngressEvent::Row(row)) => buf.push(row),
                                Ok(IngressEvent::Control(c)) => {
                                    let batches = build_source_batches(
                                        schema.clone(),
                                        std::mem::take(&mut buf),
                                        &ctx.owner,
                                        ctx.rows_per_batch,
                                    )?;
                                    for b in batches {
                                        n += b.num_rows();
                                        ctx.metrics.record_ingest(b.num_rows() as u64);
                                        if !tx.send(b).await? {
                                            return Ok(n);
                                        }
                                    }
                                    if !tx.send_control(c).await? {
                                        return Ok(n);
                                    }
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        if !buf.is_empty() {
                            let batches = build_source_batches(
                                schema.clone(),
                                buf,
                                &ctx.owner,
                                ctx.rows_per_batch,
                            )?;
                            for b in batches {
                                n += b.num_rows();
                                ctx.metrics.record_ingest(b.num_rows() as u64);
                                if !tx.send(b).await? {
                                    return Ok(n);
                                }
                            }
                        }
                    }
                    IngressEvent::Control(c) => {
                        if !tx.send_control(c).await? {
                            return Ok(n);
                        }
                    }
                }
                while let Some(control) = events.ready_control() {
                    if !tx.send_control(control).await? { return Ok(n); }
                }
            }
        }
    }
}

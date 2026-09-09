//! Job supervisor and ExecutionChain. One Tokio task per physical stage.
//! Stop cancels every mailbox send/recv; live task count must return to 0.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use sparrow_model::{
    DeliveryContract, ErrorCode, JobAttemptId, MemoryOwner, PipelineId, ResourceBudget, RestoreClaim,
    Result, Row, SparrowError, WorkBudget,
};
use sparrow_plan::{PhysicalPlan, PhysicalStage};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::capture::SharedCapture;
use crate::mailbox::{channel, MailboxConfig, MailboxRx, MailboxTx};
use crate::transform::{apply_steps, build_source_batches};

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

pub struct JobRequest {
    pub plan: PhysicalPlan,
    pub rows: Vec<Row>,
    pub capture: SharedCapture,
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

    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    pub fn submit(&self, req: JobRequest) -> Result<JobHandle> {
        DeliveryContract::V0_1.validate_restore(&RestoreClaim::None)?;
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

    let mut joins = Vec::new();
    for (i, stage) in req.plan.stages.iter().cloned().enumerate() {
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
        let rows = if matches!(stage, PhysicalStage::MemorySource { .. }) {
            req.rows.clone()
        } else {
            Vec::new()
        };
        let capture = req.capture.clone();
        joins.push(spawn_stage(ctx, stage, rx, tx, rows, capture));
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
        captured_rows: req.capture.row_count(),
        cancelled: ctx.cancel.is_cancelled(),
        remaining_work: ctx.work.remaining(),
        live_tasks_after: ctx.live.load(Ordering::SeqCst),
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
    }
}

fn spawn_stage(
    ctx: JobCtx,
    stage: PhysicalStage,
    rx: Option<MailboxRx>,
    tx: Option<MailboxTx>,
    rows: Vec<Row>,
    capture: SharedCapture,
) -> JoinHandle<Result<usize>> {
    ctx.live.fetch_add(1, Ordering::SeqCst);
    let live = Arc::clone(&ctx.live);
    tokio::spawn(async move {
        let result = stage_loop(ctx, stage, rx, tx, rows, capture).await;
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
) -> Result<usize> {
    match stage {
        PhysicalStage::MemorySource { schema, .. } => {
            let batches = build_source_batches(schema, rows, &ctx.owner, ctx.rows_per_batch)?;
            let tx = tx.ok_or_else(|| {
                SparrowError::new(ErrorCode::Internal, "source missing mailbox")
            })?;
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
            }
            Ok(0)
        }
    }
}

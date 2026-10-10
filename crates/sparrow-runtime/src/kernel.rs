//! Job supervisor and ExecutionChain. One Tokio task per physical stage.
//! Stop cancels every mailbox send/recv; live task count must return to 0.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use std::collections::HashMap;

use sparrow_io::observed::{Payload, Receiver as ObservedReceiver, Sender as ObservedSender};
use sparrow_model::observation::{FlowObservation, Latency, OriginSpan};
use sparrow_model::{
    DeliveryContract, ErrorCode, JobAttemptId, MemoryOwner, PipelineId, ResourceBudget,
    RestoreClaim, Result, Row, RowBatch, SparrowError, WorkBudget,
};
use sparrow_plan::{PhysicalPlan, PhysicalStage};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::barrier::AlignedJob;
use crate::capture::SharedCapture;
use crate::clock::RuntimeClock;
use crate::dedup::DedupOperator;
use crate::external_lookup::{ExternalLookupBinding, ExternalLookupOperator, LookupDiagnostics};
use crate::lookup::{LiveReferenceTable, LookupOperator, ReferenceTable, VersionedReferenceTable};
use crate::mailbox::{channel_observed, MailboxConfig, MailboxRx, MailboxTx, StreamControl};
use crate::mailbox_observe::{JobMailboxObserver, JobMailboxSnapshot};
use crate::metrics::RuntimeMetrics;
use crate::transform::{build_source_batches, build_source_batches_shared, CompiledTransform};
use crate::window::WindowOperator;

mod analysis;
mod buffered_window;
mod graph;
mod live_silence;
pub use graph::{GraphInput, GraphOutput};

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
    /// Optional finite-query aggregate work limit; normal live jobs are unlimited.
    pub work_lifetime: Option<u64>,
    /// Absolute deadline for finite-query script calls, shared by all stages.
    pub script_deadline: Option<std::time::Instant>,
    live_silence: Option<Arc<live_silence::Config>>,
    pub graph_inputs: HashMap<sparrow_model::OperatorId, GraphInput>,
    pub graph_outputs: HashMap<sparrow_model::OperatorId, GraphOutput>,
    /// Optional connector bootstrap reservation, minted by this Kernel only.
    pub source_admission: Option<SourceAdmission>,
    pub plan: PhysicalPlan,
    pub rows: Vec<Row>,
    pub capture: SharedCapture,
    /// Live ingress. When set, `MemorySource` reads rows here instead of `rows`.
    pub live_in: Option<ObservedReceiver<Row>>,
    pub budgeted_in: Option<ObservedReceiver<sparrow_model::QueuedRow>>,
    /// Live egress. `CaptureSink` also forwards batches here (bounded backpressure).
    pub live_out: Option<ObservedSender<RowBatch>>,
    pub observation: Option<Arc<FlowObservation>>,
    /// Process clock. Virtual clocks are required for deterministic PT windows.
    pub clock: RuntimeClock,
    /// Static reference-table snapshots frozen at submit. Running jobs keep these Arcs.
    pub tables: HashMap<String, std::sync::Arc<ReferenceTable>>,
    /// V0.3 versioned tables (as-of event time). Running jobs share the handle.
    pub versioned_tables: HashMap<String, VersionedReferenceTable>,
    /// Hot-follow snapshots: one revision per batch, restart-fresh only.
    pub live_tables: HashMap<String, LiveReferenceTable>,
    /// Bounded read-only external providers, restart-fresh only.
    pub external_lookups: HashMap<String, ExternalLookupBinding>,
    /// Punctuation sent after memory-source rows (watermarks / idle / active).
    pub trailing_controls: Vec<StreamControl>,
    /// Live watermark / idle marks (optional; alongside `live_in`).
    pub live_ctrl: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    /// Single ordered ingress (row | punctuation). Preferred over split channels.
    pub live_events: Option<ObservedReceiver<IngressEvent>>,
    /// Test-only: panic inside the first non-source stage.
    pub inject_panic: bool,
    /// Production aligned recovery hooks (Kernel path; not AlignedSession).
    pub aligned: Option<AlignedJob>,
    /// Restore memory already reserved on the admitted Job owner by the
    /// Store's owned decode. Consumed by the pre-spawn participant restore.
    restore_credit: Option<crate::checkpoint::RestoreCredit>,
}

/// Ordered live ingress envelope (R18).
#[derive(Debug)]
pub enum IngressEvent {
    LiveFeed(sparrow_io::live_feed::LiveFeedEvent),
    /// Only an explicitly wired decode-error side port may accept this marker.
    DecodeError,
    Row(Row),
    /// Pre-admitted decoded rows, ordered with barriers on the same channel.
    Batch(BoxedIngressBatch),
    Control(StreamControl),
}

/// Keep the optional reliable variant as small as a Row, so existing File
/// event channels and bulk copies do not inherit a whole inline RowBatch.
/// Field order deliberately frees the Box before refunding its metadata lease.
#[derive(Debug)]
pub struct BoxedIngressBatch {
    batch: Box<RowBatch>,
    metadata: sparrow_model::MemoryLease,
}
impl BoxedIngressBatch {
    fn into_batch(self) -> RowBatch {
        self.batch.share()
    }
}
impl IngressEvent {
    pub fn admitted_batch(batch: RowBatch) -> Result<Self> {
        if batch.output_sequence().is_some() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "final output identity cannot be injected at source ingress",
            ));
        }
        let metadata = batch.lease().owner().acquire(
            sparrow_model::CreditKind::Reservation,
            std::mem::size_of::<RowBatch>(),
        )?;
        Ok(Self::Batch(BoxedIngressBatch {
            batch: Box::new(batch),
            metadata,
        }))
    }
}
impl Payload for IngressEvent {
    fn rows(&self) -> usize {
        match self {
            Self::LiveFeed(event) => event.rows(),
            Self::DecodeError => 1,
            Self::Row(_) => 1,
            Self::Batch(batch) => batch.batch.num_rows(),
            Self::Control(_) => 0,
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::LiveFeed(event) => event.bytes(),
            Self::DecodeError => 1,
            Self::Row(row) => row.resident_bytes(),
            Self::Batch(batch) => batch
                .batch
                .tracked_bytes()
                .saturating_add(batch.metadata.bytes()),
            Self::Control(_) => 1,
        }
    }
}

impl JobRequest {
    pub fn new(plan: PhysicalPlan, rows: Vec<Row>, capture: SharedCapture) -> Self {
        Self {
            live_silence: None,
            work_lifetime: None,
            script_deadline: None,
            graph_inputs: HashMap::new(),
            graph_outputs: HashMap::new(),
            source_admission: None,
            plan,
            rows,
            capture,
            live_in: None,
            budgeted_in: None,
            live_out: None,
            observation: None,
            clock: RuntimeClock::wall(),
            tables: HashMap::new(),
            versioned_tables: HashMap::new(),
            live_tables: HashMap::new(),
            external_lookups: HashMap::new(),
            trailing_controls: Vec::new(),
            live_ctrl: None,
            live_events: None,
            inject_panic: false,
            aligned: None,
            restore_credit: None,
        }
    }

    /// Attach Store restore credit. It must belong to this request's
    /// SourceAdmission owner; otherwise admission rejects the restore.
    pub fn with_restore_credit(mut self, credit: crate::checkpoint::RestoreCredit) -> Self {
        self.restore_credit = Some(credit);
        self
    }

    pub fn with_aligned(mut self, aligned: AlignedJob) -> Self {
        self.aligned = Some(aligned);
        self
    }

    pub fn with_source_admission(mut self, admission: SourceAdmission) -> Self {
        self.source_admission = Some(admission);
        self
    }

    pub fn with_observation(mut self, observation: Arc<FlowObservation>) -> Self {
        self.observation = Some(observation);
        self
    }
    pub fn with_budgeted_live_io(
        mut self,
        input: impl Into<ObservedReceiver<sparrow_model::QueuedRow>>,
        output: impl Into<ObservedSender<RowBatch>>,
    ) -> Self {
        self.budgeted_in = Some(input.into());
        self.live_out = Some(output.into());
        self
    }

    pub fn with_inject_panic(mut self) -> Self {
        self.inject_panic = true;
        self
    }

    pub fn with_live_events(
        mut self,
        live_events: impl Into<ObservedReceiver<IngressEvent>>,
    ) -> Self {
        self.live_events = Some(live_events.into());
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

    pub fn with_live_tables(mut self, tables: HashMap<String, LiveReferenceTable>) -> Self {
        self.live_tables = tables;
        self
    }

    pub fn with_external_lookups(
        mut self,
        lookups: HashMap<String, ExternalLookupBinding>,
    ) -> Self {
        self.external_lookups = lookups;
        self
    }

    pub fn with_controls(mut self, controls: Vec<StreamControl>) -> Self {
        self.trailing_controls = controls;
        self
    }

    pub fn with_live_ctrl(mut self, live_ctrl: tokio::sync::mpsc::Receiver<StreamControl>) -> Self {
        self.live_ctrl = Some(live_ctrl);
        self
    }

    pub fn with_live_io(
        mut self,
        live_in: impl Into<ObservedReceiver<Row>>,
        live_out: impl Into<ObservedSender<RowBatch>>,
    ) -> Self {
        self.live_in = Some(live_in.into());
        self.live_out = Some(live_out.into());
        self
    }

    pub fn with_live_out(mut self, live_out: impl Into<ObservedSender<RowBatch>>) -> Self {
        self.live_out = Some(live_out.into());
        self
    }
}

/// Derive the exact immutable table identities attached to a request. The map
/// key is part of the admission contract: accepting a table under a different
/// map key could make a physical Lookup resolve a different object than the
/// checkpoint manifest describes.
fn verified_reference_dependencies(
    tables: &HashMap<String, Arc<ReferenceTable>>,
) -> Result<Vec<sparrow_plan::ReferenceTableDependency>> {
    let mut dependencies = Vec::with_capacity(tables.len());
    for (map_name, table) in tables {
        if map_name != &table.name {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "reference table attachment key does not match the table identity",
            ));
        }
        dependencies.push(table.verified_dependency()?);
    }
    dependencies.sort_by(|a, b| a.name.cmp(&b.name));
    if dependencies
        .windows(2)
        .any(|pair| pair[0].name == pair[1].name)
    {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "duplicate reference table dependency identity",
        ));
    }
    Ok(dependencies)
}

/// Validate each static Lookup against the attached snapshot, rather than
/// relying solely on dependency names in the checkpoint manifest. The plan
/// crate validates the dependency set and stage shape; this short-lived
/// operator additionally proves the table schema, key order, keep/output
/// schema and table CRC that the runtime will execute.
fn validate_static_lookup_bindings(
    plan: &PhysicalPlan,
    tables: &HashMap<String, Arc<ReferenceTable>>,
    owner: &Arc<MemoryOwner>,
) -> Result<()> {
    for stage in &plan.stages {
        let PhysicalStage::Lookup {
            operator,
            spec,
            input,
            output,
        } = stage
        else {
            continue;
        };
        if spec.temporal || spec.as_of_field.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "aligned reference-table profile excludes temporal Lookup",
            )
            .at_operator(*operator));
        }
        let table = tables.get(&spec.table).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("static Lookup table '{}' is not attached", spec.table),
            )
            .at_operator(*operator)
        })?;
        let lookup = LookupOperator::new(
            spec.clone(),
            Arc::clone(table),
            input.clone(),
            Arc::clone(owner),
        )
        .map_err(|error| error.at_operator(*operator))?;
        if lookup.output_schema() != output {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "static Lookup output schema does not match the physical plan",
            )
            .at_operator(*operator));
        }
    }
    Ok(())
}

/// Resolve the same physical Lookup contract for every table family BEFORE
/// starting any task or connector I/O. A same-name fallback is never allowed.
fn validate_lookup_request(req: &JobRequest, owner: &Arc<MemoryOwner>) -> Result<()> {
    if req.aligned.is_some()
        && (!req.live_tables.is_empty()
            || !req.external_lookups.is_empty()
            || !req.versioned_tables.is_empty())
    {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "hot-follow, versioned and external lookups require restart_fresh",
        ));
    }
    let invalid = if req.aligned.is_some() {
        ErrorCode::UnsupportedRestore
    } else {
        ErrorCode::InvalidArgument
    };
    let attachments = req
        .tables
        .len()
        .saturating_add(req.versioned_tables.len())
        .saturating_add(req.live_tables.len())
        .saturating_add(req.external_lookups.len());
    if attachments > 16 {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "Job exceeds 16 reference attachments",
        ));
    }
    let mut names = std::collections::HashSet::new();
    for name in req
        .tables
        .keys()
        .chain(req.versioned_tables.keys())
        .chain(req.live_tables.keys())
        .chain(req.external_lookups.keys())
    {
        if name.is_empty() || name.len() > 128 || !names.insert(name.as_str()) {
            return Err(SparrowError::new(
                invalid,
                "reference attachments require unique bounded names across all table families",
            ));
        }
    }
    for (name, table) in &req.tables {
        if name != &table.name {
            return Err(SparrowError::new(
                invalid,
                "static reference attachment name mismatch",
            ));
        }
    }
    for (name, table) in &req.versioned_tables {
        if name != table.name() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "versioned reference attachment name mismatch",
            ));
        }
    }
    for (name, table) in &req.live_tables {
        if name != table.name() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "hot-follow reference attachment name mismatch",
            ));
        }
    }
    for stage in &req.plan.stages {
        let PhysicalStage::Lookup {
            operator,
            spec,
            input,
            output,
        } = stage
        else {
            continue;
        };
        let resolved = if let Some(binding) = req.external_lookups.get(&spec.table) {
            ExternalLookupOperator::validate_binding(spec, binding, input)
                .map(|(_, _, _, schema)| schema)
        } else if let Some(table) = req.live_tables.get(&spec.table) {
            if spec.temporal || spec.as_of_field.is_some() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "hot-follow lookup cannot use temporal/as-of semantics",
                )
                .at_operator(*operator));
            }
            // Ownership is checked against the admitting Job owner below.
            LookupOperator::new(
                spec.clone(),
                table.snapshot()?,
                input.clone(),
                Arc::clone(owner),
            )
            .map(|op| op.output_schema().clone())
        } else if let Some(table) = req.versioned_tables.get(&spec.table) {
            LookupOperator::new_versioned(
                spec.clone(),
                table.clone(),
                input.clone(),
                Arc::clone(owner),
            )
            .map(|op| op.output_schema().clone())
        } else if let Some(table) = req.tables.get(&spec.table) {
            LookupOperator::new(
                spec.clone(),
                Arc::clone(table),
                input.clone(),
                Arc::clone(owner),
            )
            .map(|op| op.output_schema().clone())
        } else {
            return Err(
                SparrowError::new(invalid, "Lookup has no declared reference attachment")
                    .at_operator(*operator),
            );
        }?;
        if &resolved != output {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "Lookup attachment does not match the physical output schema",
            )
            .at_operator(*operator));
        }
    }
    Ok(())
}

/// One shared lease covers request/context/handle Lookup map clones, including
/// their names, bucket capacity and diagnostics. The default path allocates
/// nothing: an empty optional attachment is not a new per-job memory tax.
pub(crate) fn lookup_metadata_bytes(req: &JobRequest) -> usize {
    if req.live_tables.is_empty() && req.external_lookups.is_empty() {
        return 0;
    }
    fn buckets<V>(capacity: usize) -> usize {
        capacity
            .saturating_mul(std::mem::size_of::<(String, V)>().saturating_add(64))
            .saturating_mul(2)
            .saturating_add(128)
    }
    fn map<V>(values: &HashMap<String, V>) -> usize {
        if values.is_empty() {
            return 0;
        }
        values
            .keys()
            .fold(buckets::<V>(values.capacity()), |n, name| {
                n.saturating_add(name.capacity())
            })
    }
    let diag_map = if req.external_lookups.is_empty() {
        0
    } else {
        req.external_lookups.keys().fold(
            buckets::<Arc<LookupDiagnostics>>(req.external_lookups.capacity()),
            |n, name| n.saturating_add(name.capacity()),
        )
    };
    let maps = map(&req.live_tables)
        .saturating_add(map(&req.external_lookups))
        .saturating_add(diag_map)
        .saturating_add(std::mem::size_of::<JobCtx>())
        .saturating_add(256);
    let descriptors = req.external_lookups.values().fold(0usize, |n, binding| {
        binding.provider.keys().iter().fold(
            n.saturating_add(crate::lookup::schema_resident_bytes(
                binding.provider.schema(),
            ))
            .saturating_add(std::mem::size_of::<LookupDiagnostics>())
            .saturating_add(512),
            |n, key| {
                n.saturating_add(std::mem::size_of::<String>())
                    .saturating_add(key.capacity())
            },
        )
    });
    maps.saturating_mul(req.plan.stages.len().saturating_add(4))
        .saturating_add(descriptors.saturating_mul(2))
        .saturating_add(graph::metadata_bytes(&req.plan))
        .saturating_add(1024)
}

fn clone_lookup_map<V: Clone>(values: &HashMap<String, V>) -> HashMap<String, V> {
    // An embedding caller can provide an empty map with spare capacity; do
    // not clone that allocation while taking the no-dynamic-lookup fast path.
    if values.is_empty() {
        HashMap::new()
    } else {
        values.clone()
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
            self.ctx
                .timers
                .live
                .fetch_add(live - self.live, Ordering::Relaxed);
            self.ctx
                .metrics
                .timers_live
                .fetch_add((live - self.live) as u64, Ordering::Relaxed);
        } else {
            self.ctx
                .timers
                .live
                .fetch_sub(self.live - live, Ordering::Relaxed);
            self.ctx
                .metrics
                .timers_live
                .fetch_sub((self.live - live) as u64, Ordering::Relaxed);
        }
        let delta = cancelled.saturating_sub(self.cancelled);
        self.ctx
            .timers
            .cancelled
            .fetch_add(delta, Ordering::Relaxed);
        self.ctx
            .metrics
            .timers_cancelled
            .fetch_add(delta, Ordering::Relaxed);
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
    _slot: Arc<SourceSlot>,
    bytes: usize,
}

impl Drop for QueueAdmit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reserved.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }
}

struct SourceSlot {
    jobs: Arc<AtomicUsize>,
}
impl Drop for SourceSlot {
    fn drop(&mut self) {
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

/// Single-use bridge for async connector setup BEFORE Kernel input activation.
/// Holding this does not bypass job admission or give the connector a second
/// independent quota. A token cannot be cloned or moved to another Kernel/plan.
#[must_use]
pub struct SourceAdmission {
    process: Arc<MemoryOwner>,
    owner: Arc<MemoryOwner>,
    pipeline: PipelineId,
    attempt: JobAttemptId,
    slot: Arc<SourceSlot>,
}
impl SourceAdmission {
    pub fn owner(&self) -> Arc<MemoryOwner> {
        self.owner.clone()
    }
    pub fn attempt(&self) -> JobAttemptId {
        self.attempt
    }
    /// The connector keeps this through actual shutdown, not just Kernel task
    /// completion. Otherwise a closing SDK could spend a replacement's quota.
    pub fn lifecycle_guard(&self) -> Arc<dyn Send + Sync> {
        self.slot.clone()
    }
}

impl Kernel {
    pub fn prepare_source_admission(&self, pipeline: PipelineId) -> Result<SourceAdmission> {
        let _gate = self.admit_lock.lock().expect("source admission");
        self.source_admission_locked(pipeline)
    }

    fn source_admission_locked(&self, pipeline: PipelineId) -> Result<SourceAdmission> {
        let jobs = self.admitted_jobs.load(Ordering::SeqCst).saturating_add(1);
        let max = (self.opts.budget.retention_bytes / self.job_budget.retention_bytes.max(1))
            .min(self.opts.budget.reservation_bytes / self.job_budget.reservation_bytes.max(1));
        if jobs > max {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                format!("capacity: {}/{} jobs admitted; retry later", jobs - 1, max),
            )
            .retryable(true)
            .context("admission", "capacity"));
        }
        self.admitted_jobs.fetch_add(1, Ordering::SeqCst);
        let slot = Arc::new(SourceSlot {
            jobs: self.admitted_jobs.clone(),
        });
        let attempt = JobAttemptId::new(self.next_attempt.fetch_add(1, Ordering::SeqCst));
        Ok(SourceAdmission {
            process: self.process_owner.clone(),
            owner: MemoryOwner::child(
                self.process_owner.clone(),
                self.job_budget,
                format!("pipeline={} attempt={}", pipeline.raw(), attempt.raw()),
            ),
            pipeline,
            attempt,
            slot,
        })
    }
    pub fn new(opts: KernelOptions) -> Result<Self> {
        let mut job = opts.budget;
        // Default four-way byte isolation. Embedders can explicitly choose
        // process and job budgets through new_with_job_budget.
        job.reservation_bytes = (job.reservation_bytes / 4).max(1);
        job.retention_bytes = (job.retention_bytes / 4).max(1);
        Self::new_with_job_budget(opts, job)
    }

    pub fn new_with_job_budget(opts: KernelOptions, job_budget: ResourceBudget) -> Result<Self> {
        for kind in [
            sparrow_model::CreditKind::Reservation,
            sparrow_model::CreditKind::Retention,
            sparrow_model::CreditKind::Queue,
        ] {
            if job_budget.cap(kind) == 0 || job_budget.cap(kind) > opts.budget.cap(kind) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "job quota must fit process budget",
                ));
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

    pub fn job_budget(&self) -> ResourceBudget {
        self.job_budget
    }
    pub fn ingress_row_limit(&self) -> usize {
        self.opts
            .mailbox
            .max_bytes
            .min(self.job_budget.reservation_bytes)
    }
    pub fn ingress_batch_rows(&self) -> usize {
        self.opts.rows_per_batch
    }

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
        self.rt
            .as_ref()
            .expect("live kernel runtime")
            .handle()
            .clone()
    }

    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.as_ref().expect("live kernel runtime").block_on(f)
    }

    pub fn submit(&self, mut req: JobRequest) -> Result<JobHandle> {
        validate_lookup_request(&req, &self.process_owner)?;
        let mut plugin_count = 0usize;
        req.plan.visit_plugins(&mut |_| plugin_count += 1);
        if plugin_count > 64 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "job exceeds 64 plugin call sites",
            ));
        }
        if plugin_count > 0 && req.aligned.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "native plugin functions require restart_fresh",
            ));
        }
        if req.plan.has_analysis() && req.aligned.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "analysis is restart_fresh only",
            ));
        }
        // Extended aggregates are recoverable only through the strict
        // participant profile (codec 3, v29/v30); never legacy AlignedSession.
        if req.plan.has_extended_aggs()
            && req.aligned.as_ref().is_some_and(|aligned| {
                aligned.pipeline.is_none()
                    || sparrow_plan::CheckpointPlan::from_physical(&req.plan).is_err()
            })
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "extended aggregates require the strict linear v29/v30 participant checkpoint profile",
            ));
        }
        for stage in &req.plan.stages {
            if let PhysicalStage::Analysis { plan, .. } = stage {
                plan.validate()?;
                if plan.is_join() && req.plan.edges.is_none() {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "Join requires an explicit two-input graph",
                    ));
                }
            }
        }
        // Only a strict v31/v32/v33 participant manifest (codec 4 buffered window)
        // may run a new-kind window aligned; the rest stay restart_fresh.
        if req.plan.has_new_windows()
            && req.aligned.as_ref().is_some_and(|aligned| {
                aligned.pipeline.as_ref().is_none_or(|p| !p.plan.has_buffered_state())
                    || !sparrow_plan::CheckpointPlan::from_physical(&req.plan)
                        .is_ok_and(|live| live.has_buffered_state())
            })
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "new hopping-PT / PT sliding/session windows are restart_fresh only (sliding count requires v31/v32, ET sliding/session require File v33)",
            ));
        }
        graph::validate_request(&req)?;
        let graph_time = req
            .aligned
            .as_ref()
            .and_then(|a| a.acks.graph_time())
            .cloned();
        let durable_graph = req.aligned.is_some()
            && req.plan.edges.is_some()
            && (req.plan.has_processing_time_state() || req.plan.has_event_time_window());
        if req.live_silence.is_some() {
            live_silence::validate_request(&req)?;
        }
        let ordered_time = req.live_silence.is_none()
            && (durable_graph
                || req.plan.has_timed_iot()
                || (req.aligned.is_some() && req.plan.has_processing_time_state()));
        if ordered_time {
            let manifest = sparrow_plan::CheckpointPlan::from_physical(&req.plan)?;
            let ingress = if durable_graph {
                graph_time.as_ref().is_some_and(|g| {
                    g.check_plan(&req.plan).is_ok() && g.initial.micros == req.clock.now_micros()
                }) && req.live_events.is_none()
                    && req.graph_inputs.len() == manifest.source_ids().len()
                    && req.graph_inputs.values().all(|s| {
                        s.events.is_some()
                            && s.rows.is_empty()
                            && s.live.is_none()
                            && s.budgeted.is_none()
                            && s.trailing_controls.is_empty()
                    })
                    && req.graph_outputs.len() == manifest.sink_ids().len()
                    && req
                        .graph_outputs
                        .values()
                        .all(|s| s.live.is_some() && s.outbox.is_some())
            } else {
                req.live_events.is_some() && graph_time.is_none()
            };
            if !req.clock.is_virtual()
                || req.clock.now_micros() < 0
                || !ingress
                || req.live_ctrl.is_some()
                || req.live_in.is_some()
                || req.budgeted_in.is_some()
                || !req.rows.is_empty()
                || !req.trailing_controls.is_empty()
                || req.aligned.as_ref().is_none_or(|a| {
                    a.pipeline.is_none() || (!durable_graph && a.acks.output_sequence().is_none())
                })
            {
                return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                    "time recovery requires ordered ingress, an initial logical clock and complete aligned output identity"));
            }
            manifest.validate()?;
        }
        if graph_time.is_some() && !durable_graph {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "graph time context requires a durable time graph",
            ));
        }
        for stage in &req.plan.stages {
            if let PhysicalStage::Iot {
                operator,
                spec,
                input,
                output,
            } = stage
            {
                spec.validate(input).map_err(|e| e.at_operator(*operator))?;
                if spec.output_schema(input)?.fields != output.fields {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidSchema,
                        "IoT output schema mismatch",
                    )
                    .at_operator(*operator));
                }
                if spec.ttl_micros > 0 && self.job_budget.max_timers == 0 {
                    return Err(SparrowError::new(
                        ErrorCode::ResourceExhausted,
                        "IoT TTL requires a bounded timer slot",
                    )
                    .at_operator(*operator));
                }
            }
        }
        if req.source_admission.as_ref().is_some_and(|a| {
            !Arc::ptr_eq(&a.process, &self.process_owner) || a.pipeline != req.plan.pipeline
        }) {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "source admission belongs to another Kernel or pipeline",
            ));
        }
        if req.plan.stages.len() < 2 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "physical plan needs source and sink",
            ));
        }
        if let Some(aligned) = &req.aligned {
            if !req.versioned_tables.is_empty() {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "aligned recovery does not support versioned reference tables",
                ));
            }
            if req.plan.edges.is_some() && aligned.acks.output_sequence().is_some() {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "graph state is not enabled for the JetStream output profile",
                ));
            }
            if aligned.acks.output_sequence().is_some()
                && (req.live_out.is_none() || aligned.pipeline.is_none())
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "reliable output requires a live required sink and participant checkpoint",
                ));
            }
            if req.plan.has_iot()
                && aligned
                    .acks
                    .output_sequence()
                    .zip(
                        aligned
                            .pipeline
                            .as_ref()
                            .map(|pipeline| pipeline.generation),
                    )
                    .is_some_and(|(output, generation)| output.epoch() != generation)
            {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "reliable IoT output epoch differs from the state generation",
                ));
            }
            if let Some(pipeline) = &aligned.pipeline {
                if pipeline.sink.is_some() {
                    // A physical MemorySource is also the File connector's
                    // ingress stage. Require the admitted ordered connector
                    // path, not memory rows or a synthetic JS source cursor.
                    if req.source_admission.is_none()
                        || req.live_events.is_none()
                        || req.live_out.is_none()
                        || !req.rows.is_empty()
                        || req.live_in.is_some()
                        || req.budgeted_in.is_some()
                        || req.live_ctrl.is_some()
                        || !req.trailing_controls.is_empty()
                        || !req.graph_inputs.is_empty()
                        || !req.graph_outputs.is_empty()
                        || req.plan.edges.is_some()
                        || aligned.restore.is_some()
                        || aligned.acks.output_sequence().is_some()
                    {
                        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                            "File/JetStream sink checkpoint requires an admitted ordered File connector, not Memory or another profile"));
                    }
                    pipeline.check_compatible(
                        &sparrow_plan::CheckpointPlan::from_physical(&req.plan)?,
                        &req.source_admission.as_ref().expect("validated File admission").owner,
                    )?;
                }
                // The source connector is not started yet, but the profile is
                // already unambiguous from the physical topology and whether
                // a reliable output cursor was provisioned.  Select it before
                // owner activation so v10/v13 cannot defer an epoch mismatch
                // until the first checkpoint.
                let source_kind = if durable_graph {
                    crate::graph_cut::KIND
                } else if req.plan.has_silence() {
                    // Select the observed-clock family, not legacy PTC1.
                    // The actor/store validates the concrete File/JetStream
                    // member; both require the same epoch/shape at this point.
                    crate::observed_cut::FILE_KIND
                } else if ordered_time {
                    crate::processing_cut::FILE_KIND
                } else if req.plan.edges.is_some() {
                    "file-dag-v1"
                } else if aligned.acks.output_sequence().is_some() {
                    "jetstream-v1"
                } else {
                    "file"
                };
                let profile = if let Some(binding) = &pipeline.sink {
                    crate::pipeline_checkpoint::sink_snapshot_version_for(
                        &pipeline.plan, source_kind, binding.live.identity(),
                    )?
                } else {
                    crate::pipeline_checkpoint::snapshot_version_for(&pipeline.plan, source_kind)?
                };
                if matches!(
                    profile,
                    crate::pipeline_checkpoint::REFERENCE_RELIABLE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::RELIABLE_IOT_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::PAUSED_FILE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::PAUSED_COMBINED_FILE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::OBSERVED_FILE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::RESAMPLE_FILE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::EXT_AGG_RELIABLE_SNAPSHOT_VERSION
                        | crate::pipeline_checkpoint::SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION
                ) && aligned
                    .acks
                    .output_sequence()
                    .is_some_and(|output| output.epoch() != pipeline.generation)
                {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "reliable output epoch differs from the state generation",
                    ));
                }
                if pipeline.plan.has_references() {
                    let dependencies = verified_reference_dependencies(&req.tables)?;
                    if dependencies != pipeline.plan.reference_tables {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "attached reference tables do not exactly match the checkpoint dependencies",
                        ));
                    }
                    // Validate the physical Lookup schema/key/output contract
                    // against the attached table; name/revision alone is not
                    // a restore proof.
                    validate_static_lookup_bindings(&req.plan, &req.tables, &self.process_owner)?;
                    let live = sparrow_plan::CheckpointPlan::from_physical_with_references(
                        &req.plan,
                        dependencies,
                    )?;
                    pipeline.plan.check_compatible(&live)?;
                } else {
                    if !req.tables.is_empty() {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "reference tables require a dependency-bearing checkpoint plan",
                        ));
                    }
                    if pipeline.sink.is_none() {
                        pipeline.plan.check_compatible(
                            &sparrow_plan::CheckpointPlan::from_physical(&req.plan)?,
                        )?;
                    }
                }
            } else {
                if !req.tables.is_empty() {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "reference tables are not supported by the legacy aligned checkpoint path",
                    ));
                }
                sparrow_plan::PlanLayout::from_physical(&req.plan)?;
            }
        }
        let _gate = self.admit_lock.lock().expect("job admission");
        DeliveryContract::V0_3.validate_restore(&RestoreClaim::None)?;
        let mut boundary_queues: Vec<_> = [
            req.live_in.as_ref().and_then(|r| r.observer()),
            req.budgeted_in.as_ref().and_then(|r| r.observer()),
            req.live_events.as_ref().and_then(|r| r.observer()),
            req.live_out.as_ref().and_then(|t| t.observer()),
        ]
        .into_iter()
        .flatten()
        .collect();
        for input in req.graph_inputs.values() {
            if let Some(observer) = input.events.as_ref().and_then(|r| r.observer()) {
                boundary_queues.push(observer);
            }
            if let Some(observer) = input.live.as_ref().and_then(|r| r.observer()) {
                boundary_queues.push(observer);
            }
            if let Some(observer) = input.budgeted.as_ref().and_then(|r| r.observer()) {
                boundary_queues.push(observer);
            }
        }
        for output in req.graph_outputs.values() {
            if let Some(observer) = output.live.as_ref().and_then(|t| t.observer()) {
                boundary_queues.push(observer);
            }
        }
        let ingress_metadata = req
            .budgeted_in
            .as_ref()
            .filter(|r| r.observer().is_none())
            .map(|rx| sparrow_model::QueuedRow::channel_budget(rx.max_capacity()))
            .unwrap_or(0)
            .saturating_add(
                req.graph_inputs
                    .values()
                    .filter_map(|input| input.budgeted.as_ref())
                    .filter(|rx| rx.observer().is_none())
                    .map(|rx| sparrow_model::QueuedRow::channel_budget(rx.max_capacity()))
                    .sum::<usize>(),
            );
        let boundary_metadata = boundary_queues
            .iter()
            .map(|o| o.metadata_bytes())
            .sum::<usize>();
        let observation_metadata =
            JobMailboxObserver::metadata_bytes(self.opts.mailbox, req.plan.mailbox_count())?;
        let metadata_need = ingress_metadata
            .saturating_add(observation_metadata)
            .saturating_add(boundary_metadata)
            .saturating_add(if req.observation.is_some() {
                FlowObservation::metadata_bytes()
            } else {
                0
            });
        let job_queue_need = self
            .opts
            .mailbox
            .max_bytes
            .saturating_mul(req.plan.mailbox_count().max(1))
            .saturating_add(metadata_need);
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
            metadata_need,
        )?;
        // A prepared connector already owns its job quota. Ordinary jobs
        // acquire the same slot here; neither path charges admission twice.
        let admission = match req.source_admission.take() {
            Some(admission) => admission,
            None => self.source_admission_locked(req.plan.pipeline)?,
        };
        self.queue_reserved.fetch_add(queue_need, Ordering::SeqCst);
        let admit = QueueAdmit {
            reserved: Arc::clone(&self.queue_reserved),
            _slot: admission.slot,
            bytes: queue_need,
        };
        let attempt = admission.attempt;
        let cancel = CancellationToken::new();
        let owner = admission.owner;
        for table in req.live_tables.values() {
            if !table.is_owned_by(&owner) {
                return Err(SparrowError::new(
                    ErrorCode::PolicyDenied,
                    "hot-follow reference table is not retained by this Job owner",
                ));
            }
        }
        let lookup_memory = match lookup_metadata_bytes(&req) {
            0 => None,
            bytes => Some(Arc::new(
                owner.acquire(sparrow_model::CreditKind::Reservation, bytes)?,
            )),
        };
        let lookup_diagnostics: HashMap<String, Arc<LookupDiagnostics>> = req
            .external_lookups
            .keys()
            .map(|name| {
                (
                    name.clone(),
                    Arc::new(LookupDiagnostics::owned(Arc::clone(
                        lookup_memory
                            .as_ref()
                            .expect("dynamic lookup has metadata lease"),
                    ))),
                )
            })
            .collect();
        if req
            .aligned
            .as_ref()
            .and_then(|job| job.pipeline.as_ref())
            .is_some_and(|pipeline| pipeline.plan.has_references())
        {
            for table in req.tables.values() {
                if !table.is_owned_by(&owner) {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        format!(
                            "aligned reference table '{}' is not retained by this Job owner",
                            table.name
                        ),
                    ));
                }
            }
        }
        let ingress_memory = if ingress_metadata > 0 {
            Some(Arc::new(
                owner
                    .acquire(sparrow_model::CreditKind::Queue, ingress_metadata)
                    .map_err(|e| e.retryable(true).context("admission", "capacity"))?,
            ))
        } else {
            None
        };
        for q in &boundary_queues {
            q.initialize(&owner)?;
        }
        if let Some(obs) = &req.observation {
            obs.initialize(&owner)?;
        }
        let mailboxes =
            JobMailboxObserver::new(&req.plan, attempt.raw(), self.opts.mailbox, &owner)
                .map_err(|e| e.retryable(true).context("admission", "capacity"))?;
        let work = Arc::new(WorkBudget::new(self.job_budget.work_units));
        let mut script_plugins = false;
        req.plan
            .visit_plugins(&mut |f| script_plugins |= f.is_preemptible());
        let ctx = JobCtx {
            script_plugins,
            script_deadline: req.script_deadline,
            query_work: req
                .work_lifetime
                .map(|limit| Arc::new(AtomicU64::new(limit))),
            live_silence: req.live_silence.clone(),
            ordered_time,
            graph_time: graph_time.clone(),
            graph_memory: if req.plan.edges.is_some() || req.plan.has_analysis() || plugin_count > 0
            {
                Some(Arc::new(owner.acquire(
                    sparrow_model::CreditKind::Reservation,
                    graph::metadata_bytes(&req.plan),
                )?))
            } else {
                None
            },
            side_port: None,
            graph_mode: false,
            source_time: None,
            side_output: None,
            optional_branch: false,
            source_operator: None,
            sink_outbox: None,
            pipeline: req.plan.pipeline,
            attempt,
            owner: Arc::clone(&owner),
            mailboxes: mailboxes.clone(),
            ingress_memory,
            work,
            cancel: cancel.clone(),
            live: Arc::clone(&self.live_tasks),
            mailbox: self.opts.mailbox,
            rows_per_batch: self.opts.rows_per_batch,
            clock: req.clock.clone(),
            tables: req.tables.clone(),
            versioned_tables: req.versioned_tables.clone(),
            live_tables: clone_lookup_map(&req.live_tables),
            external_lookups: clone_lookup_map(&req.external_lookups),
            lookup_diagnostics: clone_lookup_map(&lookup_diagnostics),
            max_state_keys: self.job_budget.max_state_keys,
            max_timers: self.job_budget.max_timers,
            timers: Arc::new(JobTimers::default()),
            min_remaining_work: Arc::new(AtomicU64::new(self.job_budget.work_units)),
            metrics: Arc::clone(&self.metrics),
            aligned: req
                .aligned
                .take()
                .map(|job| {
                    if graph_time.as_ref().is_some_and(|g| {
                        !g.belongs_to(&owner)
                            || job
                                .pipeline
                                .as_ref()
                                .is_none_or(|p| p.generation != g.generation)
                    }) {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "graph control state belongs to another owner or generation",
                        ));
                    }
                    crate::barrier::RuntimeAligned::prepare_with_credit(
                        job,
                        &req.plan,
                        &owner,
                        attempt.raw(),
                        self.job_budget.max_state_keys,
                        self.job_budget.max_timers,
                        (ordered_time && !req.plan.has_event_time_window())
                            .then(|| req.clock.now_micros()),
                        req.restore_credit.take(),
                    )
                })
                .transpose()?,
            observation: req.observation.clone(),
            _lookup_memory: lookup_memory.clone(),
        };
        let plugin_pins = if plugin_count > 0 {
            let credit = owner.acquire(
                sparrow_model::CreditKind::Reservation,
                plugin_count.saturating_mul(32).saturating_add(128),
            )?;
            Some((req.plan.plugin_functions(), credit))
        } else {
            None
        };
        self.metrics.jobs_started.fetch_add(1, Ordering::Relaxed);
        let live_tables = clone_lookup_map(&req.live_tables);
        let handle = self
            .rt
            .as_ref()
            .expect("live kernel runtime")
            .spawn(async move {
                let _pins = plugin_pins;
                run_job(ctx, req, admit).await
            });
        Ok(JobHandle {
            attempt,
            owner,
            mailboxes,
            cancel,
            handle,
            live: Arc::clone(&self.live_tasks),
            metrics: Arc::clone(&self.metrics),
            live_tables,
            lookup_diagnostics,
            _lookup_memory: lookup_memory,
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
    ingress_metadata: usize,
) -> Result<usize> {
    let mailboxes = plan.mailbox_count().max(1);
    let need = opts
        .mailbox
        .max_bytes
        .saturating_mul(mailboxes)
        .saturating_add(ingress_metadata);
    if need > opts.budget.queue_bytes {
        return Err(SparrowError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "queue admission: {mailboxes} mailboxes × {}B > queue cap {}",
                opts.mailbox.max_bytes, opts.budget.queue_bytes
            ),
        )
        .retryable(false));
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
    script_plugins: bool,
    script_deadline: Option<std::time::Instant>,
    query_work: Option<Arc<AtomicU64>>,
    live_silence: Option<Arc<live_silence::Config>>,
    ordered_time: bool,
    graph_time: Option<Arc<crate::graph_cut::GraphRuntime>>,
    graph_memory: Option<Arc<sparrow_model::MemoryLease>>,
    side_port: Option<u32>,
    graph_mode: bool,
    source_time: Option<Arc<graph::SourceTime>>,
    optional_branch: bool,
    side_output: Option<(MailboxTx, bool)>,
    source_operator: Option<sparrow_model::OperatorId>,
    sink_outbox: Option<Arc<sparrow_model::InflightCounter>>,
    observation: Option<Arc<FlowObservation>>,
    mailboxes: Arc<JobMailboxObserver>,
    timers: Arc<JobTimers>,
    min_remaining_work: Arc<AtomicU64>,
    pipeline: PipelineId,
    attempt: JobAttemptId,
    owner: Arc<MemoryOwner>,
    ingress_memory: Option<Arc<sparrow_model::MemoryLease>>,
    work: Arc<WorkBudget>,
    cancel: CancellationToken,
    live: Arc<AtomicUsize>,
    mailbox: MailboxConfig,
    rows_per_batch: usize,
    clock: RuntimeClock,
    tables: HashMap<String, std::sync::Arc<ReferenceTable>>,
    versioned_tables: HashMap<String, VersionedReferenceTable>,
    live_tables: HashMap<String, LiveReferenceTable>,
    external_lookups: HashMap<String, ExternalLookupBinding>,
    lookup_diagnostics: HashMap<String, Arc<LookupDiagnostics>>,
    max_state_keys: usize,
    max_timers: usize,
    metrics: Arc<RuntimeMetrics>,
    aligned: Option<Arc<crate::barrier::RuntimeAligned>>,
    // Declared after maps: refund only after all owned map/context fields drop.
    _lookup_memory: Option<Arc<sparrow_model::MemoryLease>>,
}

pub struct JobHandle {
    pub attempt: JobAttemptId,
    owner: Arc<MemoryOwner>,
    mailboxes: Arc<JobMailboxObserver>,
    cancel: CancellationToken,
    handle: JoinHandle<Result<JobStats>>,
    live: Arc<AtomicUsize>,
    metrics: Arc<RuntimeMetrics>,
    live_tables: HashMap<String, LiveReferenceTable>,
    lookup_diagnostics: HashMap<String, Arc<LookupDiagnostics>>,
    _lookup_memory: Option<Arc<sparrow_model::MemoryLease>>,
}

impl JobHandle {
    pub fn live_reference_tables(&self) -> HashMap<String, LiveReferenceTable> {
        clone_lookup_map(&self.live_tables)
    }

    pub fn external_lookup_diagnostics(&self) -> HashMap<String, Arc<LookupDiagnostics>> {
        clone_lookup_map(&self.lookup_diagnostics)
    }
    pub fn mailbox_snapshot(&self) -> JobMailboxSnapshot {
        self.mailboxes.snapshot()
    }
    pub fn mailbox_observer(&self) -> Arc<JobMailboxObserver> {
        self.mailboxes.clone()
    }
    pub fn memory_owner(&self) -> Arc<MemoryOwner> {
        Arc::clone(&self.owner)
    }
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
        self.metrics.jobs_stopped.fetch_add(1, Ordering::Relaxed);
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
    if req.plan.edges.is_some() {
        return graph::run(ctx, req).await;
    }
    let n = req.plan.stages.len();
    if n < 2 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "physical plan needs source and sink",
        ));
    }

    let mut txs = Vec::new();
    let mut rxs: Vec<Option<MailboxRx>> = Vec::new();
    for i in 0..n.saturating_sub(1) {
        let (tx, rx) = channel_observed(ctx.mailbox, ctx.cancel.clone(), ctx.mailboxes.edge(i))?;
        txs.push(tx);
        rxs.push(Some(rx));
    }
    ctx.mailboxes.initialized();

    let JobRequest {
        work_lifetime: _,
        script_deadline: _,
        live_silence: _,
        plan,
        rows,
        capture,
        mut live_in,
        mut budgeted_in,
        live_out,
        clock: _,
        tables: _,
        versioned_tables: _,
        live_tables: _,
        external_lookups: _,
        trailing_controls,
        restore_credit: _,
        mut live_ctrl,
        mut live_events,
        inject_panic,
        aligned: _,
        observation: _,
        source_admission: _,
        graph_inputs: _,
        graph_outputs: _,
    } = req;

    let mut set = JoinSet::new();
    for (i, stage) in plan.stages.iter().cloned().enumerate() {
        let tx = if i + 1 < n {
            Some(txs[i].clone())
        } else {
            None
        };
        let rx = if i == 0 { None } else { rxs[i - 1].take() };
        let ctx = clone_ctx(&ctx);
        let is_source = matches!(stage, PhysicalStage::MemorySource { .. });
        let is_sink = matches!(stage, PhysicalStage::CaptureSink { .. });
        let rows = if is_source { rows.clone() } else { Vec::new() };
        let capture = capture.clone();
        let stage_live_in = if is_source { live_in.take() } else { None };
        let stage_budgeted_in = if is_source { budgeted_in.take() } else { None };
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
            stage_budgeted_in,
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
    // Source paths already count batches as they enter the graph. Completing
    // or stopping a live job must not add its entire input a second time.
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
        script_plugins: ctx.script_plugins,
        script_deadline: ctx.script_deadline,
        query_work: ctx.query_work.clone(),
        live_silence: ctx.live_silence.clone(),
        ordered_time: ctx.ordered_time,
        graph_time: ctx.graph_time.clone(),
        graph_memory: ctx.graph_memory.clone(),
        side_port: ctx.side_port,
        graph_mode: ctx.graph_mode,
        source_time: ctx.source_time.clone(),
        optional_branch: ctx.optional_branch,
        side_output: ctx.side_output.clone(),
        source_operator: ctx.source_operator,
        sink_outbox: ctx.sink_outbox.clone(),
        observation: ctx.observation.clone(),
        mailboxes: ctx.mailboxes.clone(),
        timers: Arc::clone(&ctx.timers),
        min_remaining_work: Arc::clone(&ctx.min_remaining_work),
        pipeline: ctx.pipeline,
        attempt: ctx.attempt,
        owner: Arc::clone(&ctx.owner),
        ingress_memory: ctx.ingress_memory.clone(),
        work: Arc::new(WorkBudget::new(ctx.work.cap())),
        cancel: ctx.cancel.clone(),
        live: Arc::clone(&ctx.live),
        mailbox: ctx.mailbox,
        rows_per_batch: ctx.rows_per_batch,
        clock: ctx.clock.clone(),
        tables: ctx.tables.clone(),
        versioned_tables: ctx.versioned_tables.clone(),
        live_tables: clone_lookup_map(&ctx.live_tables),
        external_lookups: clone_lookup_map(&ctx.external_lookups),
        lookup_diagnostics: clone_lookup_map(&ctx.lookup_diagnostics),
        max_state_keys: ctx.max_state_keys,
        max_timers: ctx.max_timers,
        metrics: Arc::clone(&ctx.metrics),
        aligned: ctx.aligned.clone(),
        _lookup_memory: ctx._lookup_memory.clone(),
    }
}

fn consume_query_work(ctx: &JobCtx, units: u64) -> Result<()> {
    if let Some(limit) = &ctx.query_work {
        limit
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(units)
            })
            .map_err(|_| {
                SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "finite query total work limit exceeded",
                )
            })?;
    }
    Ok(())
}
async fn consume_work(ctx: &JobCtx, units: u64) -> Result<()> {
    consume_query_work(ctx, units)?;
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
    live_in: Option<ObservedReceiver<Row>>,
    budgeted_in: Option<ObservedReceiver<sparrow_model::QueuedRow>>,
    live_out: Option<ObservedSender<RowBatch>>,
    live_ctrl: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    live_events: Option<ObservedReceiver<IngressEvent>>,
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
        let is_source = matches!(&stage, PhysicalStage::MemorySource { .. });
        let optional = ctx.optional_branch;
        let local_cancel = ctx.cancel.clone();
        let branch_metrics = ctx.metrics.clone();
        let script_plugins = ctx.script_plugins;
        let script_work = ctx.query_work.clone();
        let script_deadline = ctx.script_deadline;
        let future = stage_loop(
            ctx,
            stage,
            rx,
            tx,
            rows,
            capture,
            live_in,
            budgeted_in,
            live_out,
            live_ctrl,
            live_events,
            trailing_controls,
        );
        let result = if script_plugins {
            sparrow_expr::plugins::script::scope_with_budget(
                local_cancel.clone(),
                script_work,
                script_deadline,
                future,
            )
            .await
        } else {
            future.await
        };
        min_remaining.fetch_min(work.remaining(), Ordering::Relaxed);
        // Window stages return their output count. Do not add intermediate
        // emissions to JobStats.ingested_rows when joining all stage tasks.
        if optional && result.is_err() {
            local_cancel.cancel();
            branch_metrics
                .graph_branch_failures
                .fetch_add(1, Ordering::Relaxed);
            Ok(0)
        } else {
            result.map(|rows| if is_source { rows } else { 0 })
        }
    });
}

async fn stage_loop(
    ctx: JobCtx,
    stage: PhysicalStage,
    mut rx: Option<MailboxRx>,
    tx: Option<MailboxTx>,
    rows: Vec<Row>,
    capture: SharedCapture,
    live_in: Option<ObservedReceiver<Row>>,
    budgeted_in: Option<ObservedReceiver<sparrow_model::QueuedRow>>,
    live_out: Option<ObservedSender<RowBatch>>,
    live_ctrl: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    live_events: Option<ObservedReceiver<IngressEvent>>,
    trailing_controls: Vec<StreamControl>,
) -> Result<usize> {
    match stage {
        PhysicalStage::Analysis { operator, plan } => {
            let tx =
                tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "analysis output absent"))?;
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "analysis input absent"))?;
            let result = analysis::unnest_task(&ctx, plan, rx, &tx)?
                .await
                .map_err(|e| e.at_operator(operator));
            result
        }
        PhysicalStage::Branch { .. }
        | PhysicalStage::Route { .. }
        | PhysicalStage::UnionAll { .. } => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "graph operator requires explicit topology",
        )),
        PhysicalStage::MemorySource { schema, .. } => {
            let tx =
                tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "source missing mailbox"))?;
            if let Some(input) = budgeted_in {
                return budgeted_live_source(ctx, schema, tx, input).await;
            }
            if let Some(events) = live_events {
                if ctx.live_silence.is_some() {
                    return Box::pin(live_silence::source(ctx, schema, tx, events)).await;
                }
                return if ctx.graph_mode {
                    live_source::<true>(ctx, schema, tx, LiveInput::Ordered(events)).await
                } else {
                    live_source::<false>(ctx, schema, tx, LiveInput::Ordered(events)).await
                };
            }
            if let Some(live_in) = live_in {
                let input = LiveInput::Split {
                    rows: live_in,
                    controls: live_ctrl,
                };
                return if ctx.graph_mode {
                    live_source::<true>(ctx, schema, tx, input).await
                } else {
                    live_source::<false>(ctx, schema, tx, input).await
                };
            }
            let batches = build_source_batches(schema, rows, &ctx.owner, ctx.rows_per_batch)?;
            let mut n = 0usize;
            for b in batches {
                n += b.num_rows();
                ctx.metrics.record_ingest(b.num_rows() as u64);
                if let Some(obs) = &ctx.observation {
                    obs.runtime_rows(true, b.num_rows());
                }
                if !graph::send_source(&ctx, &tx, b.with_source_operator(ctx.source_operator))
                    .await?
                {
                    return Ok(n);
                }
            }
            for c in trailing_controls {
                if !publish_source_control(&ctx, &tx, c).await? {
                    return Ok(n);
                }
            }
            if ctx.graph_mode {
                publish_source_control(&ctx, &tx, StreamControl::EndOfInput).await?;
            }
            Ok(n)
        }
        PhysicalStage::Transform { steps } => {
            let compiled = CompiledTransform::new(&steps)?;
            let tx =
                tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "transform missing tx"))?;
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "transform missing rx"))?;
            while let Some(mut env) = rx.recv().await? {
                let (batch, ctrl) = env.take();
                if let Some(ctrl) = ctrl {
                    if !tx.send_control(ctrl).await? {
                        return Ok(0);
                    }
                }
                if let Some(batch) = batch {
                    if ctx
                        .work
                        .would_exhaust(compiled.work_units(batch.num_rows()))
                    {
                        tokio::task::yield_now().await;
                        ctx.work.begin_quantum();
                    }
                    consume_query_work(&ctx, compiled.work_units(batch.num_rows()))?;
                    let started = std::time::Instant::now();
                    let result = compiled.apply(&batch, &ctx.owner, &ctx.work);
                    if let Some(obs) = &ctx.observation {
                        obs.record(Latency::Transform, started.elapsed());
                    }
                    if let (Some(obs), Ok(output)) = (&ctx.observation, &result) {
                        obs.filtered(
                            batch
                                .num_rows()
                                .saturating_sub(output.as_ref().map_or(0, |b| b.num_rows())),
                        );
                    }
                    if let Some(out) = result? {
                        let out = out
                            .with_origin(batch.origin())
                            .with_source_operator(batch.source_operator());
                        if !tx.send(out).await? {
                            return Ok(0);
                        }
                    }
                }
            }
            Ok(0)
        }
        PhysicalStage::CaptureSink {
            schema, operator, ..
        }
        | PhysicalStage::BestEffortSink {
            schema, operator, ..
        } => {
            let mut ended = false;
            let sink_outbox = ctx
                .sink_outbox
                .as_ref()
                .or_else(|| ctx.aligned.as_ref().map(|a| &a.outbox));
            let mut next_output = if let Some(graph) = &ctx.graph_time {
                Some(graph.sink_sequence(operator.raw())?)
            } else {
                ctx.aligned.as_ref().and_then(|a| a.acks.output_sequence())
            };
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "sink missing rx"))?;
            loop {
                let envelope = if !ctx.graph_mode {
                    rx.recv().await?
                } else {
                    tokio::select! {
                        biased;
                        _=ctx.cancel.cancelled()=>None,
                        _=async {let counter=sink_outbox.expect("graph live sink counter");while counter.failed()==0 && live_out.as_ref().is_none_or(|out|!out.is_closed()){tokio::time::sleep(std::time::Duration::from_millis(10)).await;}},if ctx.graph_mode&&sink_outbox.is_some()=>{
                            return Err(SparrowError::new(ErrorCode::JobFailed,"graph sink reported failed output").at_operator(operator));
                        },
                        result=rx.recv()=>result?,
                    }
                };
                let Some(mut env) = envelope else {
                    if ctx.graph_mode && !ended && !ctx.cancel.is_cancelled() {
                        return Err(SparrowError::new(
                            ErrorCode::JobFailed,
                            "graph sink input closed without EOF",
                        )
                        .at_operator(operator));
                    }
                    break;
                };
                capture.stall.wait_if_stalled(&ctx.cancel).await;
                if ctx.cancel.is_cancelled() {
                    break;
                }
                let (batch, ctrl) = env.take();
                if ctx.graph_mode {
                    if matches!(ctrl, Some(StreamControl::EndOfInput)) {
                        ended = true;
                    }
                    if ended && batch.is_some() {
                        return Err(SparrowError::new(
                            ErrorCode::JobFailed,
                            "graph sink received data after EOF",
                        )
                        .at_operator(operator));
                    }
                }
                if let Some(batch) = batch {
                    let batch = if let Some(position) = next_output {
                        let following = position.advance(batch.num_rows())?;
                        let batch = batch.with_output_sequence(position)?;
                        next_output = Some(following);
                        batch
                    } else {
                        batch
                    };
                    // Count final plan output once, not every intermediate
                    // window emission (which may still be filtered downstream).
                    ctx.metrics.record_emit(batch.num_rows() as u64);
                    if let Some(obs) = &ctx.observation {
                        obs.runtime_rows(false, batch.num_rows());
                    }
                    capture.push(&schema, batch.rows());
                    if let Some(out) = &live_out {
                        if let Some(outbox) = sink_outbox {
                            outbox.enqueue();
                        }
                        tokio::select! {
                            biased;
                            _ = ctx.cancel.cancelled() => {
                                if let Some(outbox) = sink_outbox { outbox.fail(); }
                                break;
                            },
                            sent = out.send(batch.share()) => {
                                if sent.is_err() {
                                    if let Some(outbox) = sink_outbox {
                                        outbox.fail();
                                    }
                                    if ctx.graph_mode && !ctx.optional_branch {
                                        return Err(SparrowError::new(ErrorCode::JobFailed,"required graph sink closed before accepting output").at_operator(operator));
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
                        if !aj.acks.is_active(checkpoint_id) {
                            continue;
                        }
                        let flush = tokio::select! {
                            _ = ctx.cancel.cancelled() => break,
                            flush = aj.acks.wait_for_flush(checkpoint_id, sink_outbox.expect("aligned outbox")) => flush,
                        };
                        let Some(flush) = flush else {
                            continue;
                        };
                        // Loss is sticky for this attempt: retrying a barrier
                        // cannot retroactively deliver a dropped batch.
                        if flush.ok && flush.dropped == 0 {
                            sink_outbox.expect("aligned outbox").mark();
                        }
                        aj.acks
                            .sink_flushed_for(checkpoint_id, flush, next_output, Some(operator))
                            .await;
                    }
                }
            }
            if ctx.graph_mode && !ctx.cancel.is_cancelled() {
                if let Some(counter) = sink_outbox {
                    let flush = tokio::select! {biased;_=ctx.cancel.cancelled()=>return Ok(0),flush=crate::barrier::wait_outbox(counter,std::time::Duration::from_secs(30))=>flush};
                    if !flush.ok || flush.timed_out || flush.dropped != 0 {
                        return Err(SparrowError::new(
                            ErrorCode::JobFailed,
                            "required graph sink final flush failed",
                        )
                        .at_operator(operator));
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
            let tx =
                tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "window missing tx"))?;
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "window missing rx"))?;
            if spec.kind.is_buffered() {
                let prepared = ctx.aligned.as_ref().and_then(|a| {
                    a.buffered.lock().expect("prepared buffered windows").remove(&operator)
                });
                return buffered_window::task(&ctx, operator, prepared, spec, input, rx, &tx, &capture)?
                    .await
                    .map_err(|e| e.at_operator(operator));
            }
            let prepared = ctx.aligned.as_ref().and_then(|a| {
                a.windows
                    .lock()
                    .expect("prepared windows")
                    .remove(&operator)
            });
            if prepared.is_none() && ctx.aligned.as_ref().is_some_and(|a| a.participant_mode) {
                return Err(SparrowError::new(
                    ErrorCode::Internal,
                    "prepared checkpoint participant missing",
                ));
            }
            let mut op = if let Some(window) = prepared {
                window
            } else {
                WindowOperator::new(
                    operator,
                    spec,
                    input,
                    Arc::clone(&ctx.owner),
                    ctx.max_state_keys,
                    ctx.max_timers,
                )?
            };
            if ctx.graph_mode {
                op.use_external_watermarks();
            }
            let restore = ctx
                .aligned
                .as_ref()
                .and_then(|a| a.restore.lock().expect("restore slot").take());
            if let Some(restored) = restore {
                op.restore_freeze(&restored.freeze)?;
            }
            if ctx.graph_mode {
                window_stage::<true>(&ctx, &mut op, rx, &tx, &capture).await
            } else {
                window_stage::<false>(&ctx, &mut op, rx, &tx, &capture).await
            }
        }
        PhysicalStage::Iot {
            operator,
            spec,
            input,
            ..
        } => {
            let tx = tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "IoT missing tx"))?;
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "IoT missing rx"))?;
            let prepared = ctx
                .aligned
                .as_ref()
                .and_then(|a| a.iot.lock().expect("prepared IoT state").remove(&operator));
            if prepared.is_none() && ctx.aligned.as_ref().is_some_and(|a| a.participant_mode) {
                return Err(SparrowError::new(
                    ErrorCode::Internal,
                    "prepared IoT checkpoint participant missing",
                )
                .at_operator(operator));
            }
            let mut op = match prepared {
                Some(op) => op,
                None => crate::iot::IotOperator::new(operator, spec, input, ctx.owner.clone())
                    .map_err(|error| error.at_operator(operator))?,
            };
            iot_stage(&ctx, &mut op, rx, &tx)
                .await
                .map_err(|e| e.at_operator(operator))
        }
        PhysicalStage::Deduplicate {
            operator,
            spec,
            input,
        } => {
            let tx =
                tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "dedup missing tx"))?;
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "dedup missing rx"))?;
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
                    let scratch_bytes = batch
                        .rows()
                        .iter()
                        .map(Row::resident_bytes)
                        .sum::<usize>()
                        .saturating_mul(8)
                        .saturating_add(1024);
                    let scratch = ctx
                        .owner
                        .acquire(sparrow_model::CreditKind::Reservation, scratch_bytes)?;
                    let started = std::time::Instant::now();
                    let result = op.on_batch(&batch, ctx.clock.now_micros());
                    if let Some(obs) = &ctx.observation {
                        obs.record(Latency::Dedup, started.elapsed());
                    }
                    let rows = result?;
                    let out = op.build_metered_batch(rows)?;
                    drop(scratch);
                    if let Some(out) = out {
                        if !tx
                            .send(
                                out.with_origin(batch.origin())
                                    .with_source_operator(batch.source_operator()),
                            )
                            .await?
                        {
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
            let tx =
                tx.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "lookup missing tx"))?;
            let rx = rx
                .as_mut()
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "lookup missing rx"))?;
            let mut external = if let Some(binding) = ctx.external_lookups.get(&spec.table).cloned()
            {
                Some(ExternalLookupOperator::with_diagnostics(
                    spec.clone(),
                    binding,
                    input.clone(),
                    Arc::clone(&ctx.owner),
                    ctx.lookup_diagnostics
                        .get(&spec.table)
                        .cloned()
                        .ok_or_else(|| {
                            SparrowError::new(
                                ErrorCode::Internal,
                                "external lookup diagnostics missing",
                            )
                        })?,
                )?)
            } else {
                None
            };
            let op = if external.is_some() {
                None
            } else if let Some(table) = ctx.live_tables.get(&spec.table).cloned() {
                Some(LookupOperator::new_live(
                    spec,
                    table,
                    input,
                    Arc::clone(&ctx.owner),
                )?)
            } else if let Some(ver) = ctx.versioned_tables.get(&spec.table).cloned() {
                Some(LookupOperator::new_versioned(
                    spec,
                    ver,
                    input,
                    Arc::clone(&ctx.owner),
                )?)
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
                Some(LookupOperator::new(
                    spec,
                    table,
                    input,
                    Arc::clone(&ctx.owner),
                )?)
            };
            while let Some(mut env) = rx.recv().await? {
                let (batch, ctrl) = env.take();
                if let Some(batch) = batch {
                    consume_work(&ctx, batch.num_rows() as u64).await?;
                    let started = std::time::Instant::now();
                    let result = if let Some(external) = &mut external {
                        external_lookup_batch(&ctx.owner, external, &batch, ctx.cancel.clone())?
                            .await
                    } else {
                        op.as_ref()
                            .expect("resolved local lookup")
                            .on_batch_into(&batch)
                    };
                    if let Some(obs) = &ctx.observation {
                        obs.record(Latency::Lookup, started.elapsed());
                    }
                    if let Some(out) = result? {
                        if !tx
                            .send(
                                out.with_origin(batch.origin())
                                    .with_source_operator(batch.source_operator()),
                            )
                            .await?
                        {
                            break;
                        }
                    }
                }
                if let Some(ctrl) = ctrl {
                    if (external.is_some() || !ctx.live_tables.is_empty())
                        && matches!(
                            ctrl,
                            StreamControl::CheckpointBarrier { .. }
                                | StreamControl::ProcessingTime { .. }
                                | StreamControl::FeedObservation { .. }
                                | StreamControl::GraphProgress { .. }
                                | StreamControl::GraphRoundEnd { .. }
                        )
                    {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "hot-follow and external lookups reject durable/checkpoint controls",
                        ));
                    }
                    if !tx.send_control(ctrl).await? {
                        break;
                    }
                }
            }
            Ok(0)
        }
    }
}

struct IotReporter {
    metrics: Arc<RuntimeMetrics>,
    previous: crate::iot::IotStats,
    keys: u64,
    bytes: u64,
}

impl IotReporter {
    fn sample(&mut self, op: &crate::iot::IotOperator) {
        let current = op.stats();
        for (counter, value, previous) in [
            (
                &self.metrics.iot_input_rows,
                current.input_rows,
                self.previous.input_rows,
            ),
            (
                &self.metrics.iot_emitted_rows,
                current.emitted_rows,
                self.previous.emitted_rows,
            ),
            (
                &self.metrics.iot_filtered_rows,
                current.filtered_rows,
                self.previous.filtered_rows,
            ),
            (
                &self.metrics.iot_invalid_rows,
                current.invalid_rows,
                self.previous.invalid_rows,
            ),
            (
                &self.metrics.iot_expired_keys,
                current.expired_keys,
                self.previous.expired_keys,
            ),
            (
                &self.metrics.alarm_notifications_expired,
                current.notifications_expired,
                self.previous.notifications_expired,
            ),
            (
                &self.metrics.alarm_notifications_cancelled,
                current.notifications_cancelled,
                self.previous.notifications_cancelled,
            ),
            (
                &self.metrics.alarm_notifications_deferred,
                current.notifications_deferred,
                self.previous.notifications_deferred,
            ),
        ] {
            counter.fetch_add(value.saturating_sub(previous), Ordering::Relaxed);
        }
        for (counter, previous, value) in [
            (
                &self.metrics.iot_state_keys,
                &mut self.keys,
                op.key_count() as u64,
            ),
            (
                &self.metrics.iot_state_bytes,
                &mut self.bytes,
                op.retention_bytes() as u64,
            ),
        ] {
            if value >= *previous {
                counter.fetch_add(value - *previous, Ordering::Relaxed);
            } else {
                counter.fetch_sub(*previous - value, Ordering::Relaxed);
            }
            *previous = value;
        }
        self.previous = current;
    }
}

impl Drop for IotReporter {
    fn drop(&mut self) {
        self.metrics
            .iot_state_keys
            .fetch_sub(self.keys, Ordering::Relaxed);
        self.metrics
            .iot_state_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Live TTL uses local monotonic idle time. Aligned time profiles instead use
/// only persisted, FIFO time controls and never wake on this local timer path.
async fn iot_stage(
    ctx: &JobCtx,
    op: &mut crate::iot::IotOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
) -> Result<usize> {
    if ctx.live_silence.is_some() {
        return Box::pin(live_silence::stage(ctx, op, rx, tx)).await;
    }
    if ctx.ordered_time {
        return timed_iot_stage(ctx, op, rx, tx).await;
    }
    let started = tokio::time::Instant::now();
    let now = || {
        if ctx.clock.is_virtual() {
            ctx.clock.now_micros()
        } else {
            started.elapsed().as_micros().min(i64::MAX as u128) as i64
        }
    };
    let mut reporter = IotReporter {
        metrics: ctx.metrics.clone(),
        previous: Default::default(),
        keys: 0,
        bytes: 0,
    };
    let mut timers = TimerReporter {
        ctx,
        live: 0,
        cancelled: 0,
    };
    let mut ended = false;
    loop {
        reporter.sample(op);
        let deadline = op.next_deadline();
        timers.sample(op.pending_timers(), 0);
        let envelope = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => break,
            _ = async {
                if ctx.clock.is_virtual() { ctx.clock.sleep_until(deadline).await; }
                else if let Some(at) = deadline.and_then(|at| started.checked_add(std::time::Duration::from_micros(at.max(0) as u64))) {
                    tokio::time::sleep_until(at).await;
                } else { std::future::pending::<()>().await; }
            }, if deadline.is_some() => { op.expire(now()); continue; },
            envelope = rx.recv() => envelope?,
        };
        let Some(mut envelope) = envelope else {
            if ctx.graph_mode && !ended && !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "IoT input closed without EOF",
                ));
            }
            break;
        };
        let (batch, control) = envelope.take();
        if let Some(batch) = batch {
            if ended {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "IoT data after EOF",
                ));
            }
            consume_work(ctx, batch.num_rows() as u64).await?;
            let output = op.on_batch(&batch, now());
            reporter.sample(op);
            if let Some(output) = output? {
                if !tx
                    .send(
                        output
                            .with_origin(batch.origin())
                            .with_source_operator(batch.source_operator()),
                    )
                    .await?
                {
                    break;
                }
            }
        }
        if let Some(control) = control {
            match &control {
                StreamControl::CheckpointBarrier { checkpoint_id } => {
                    if let Some(aligned) = &ctx.aligned {
                        if !aligned.acks.is_active(*checkpoint_id) {
                            continue;
                        }
                        let freeze = crate::barrier::EncodedFreeze::from_iot(
                            op,
                            &ctx.owner,
                            ctx.max_state_keys,
                        );
                        aligned
                            .acks
                            .iot_frozen(*checkpoint_id, op.operator_id(), freeze)
                            .await;
                    }
                }
                StreamControl::EndOfInput => ended = true,
                _ => {}
            }
            if !tx.send_control(control).await? {
                break;
            }
        }
    }
    reporter.sample(op);
    op.cleanup();
    Ok(0)
}

/// Time decisions and data share one FIFO. Timers are completely drained at
/// each durable decision before the following data or checkpoint barrier.
/// Silence is the explicit exception: time only opens its decision, input
/// updates last-seen, and the following source fact authorizes timer draining.
async fn timed_iot_stage(
    ctx: &JobCtx,
    op: &mut crate::iot::IotOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
) -> Result<usize> {
    let mut now = ctx.clock.now_micros();
    op.validate_processing_cut(now)?;
    let mut reporter = IotReporter {
        metrics: ctx.metrics.clone(),
        previous: Default::default(),
        keys: 0,
        bytes: 0,
    };
    let mut timers = TimerReporter {
        ctx,
        live: 0,
        cancelled: 0,
    };
    loop {
        reporter.sample(op);
        timers.sample(op.pending_timers(), 0);
        op.report_resample_metrics(&ctx.metrics);
        let envelope = tokio::select! { biased; _=ctx.cancel.cancelled()=>break, e=rx.recv()=>e? };
        let Some(mut envelope) = envelope else {
            if !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "ordered processing-time source closed without shutdown",
                ));
            }
            break;
        };
        let (batch, control) = envelope.take();
        if let Some(batch) = batch {
            consume_work(ctx, batch.num_rows() as u64).await?;
            if let Some(output) = op.on_batch(&batch, now)? {
                if !tx.send(output).await? {
                    break;
                }
            }
        }
        if let Some(control) = control {
            match &control {
                StreamControl::ProcessingTime { micros } => {
                    op.set_processing_time(*micros)?;
                    now = *micros;
                    // Publish the cut before timer-derived rows. Every downstream
                    // stage expires its old state at this cut before accepting
                    // upstream timer output at the same cut (half-open boundary).
                    if !tx.send_control(control.clone()).await? {
                        break;
                    }
                    while op.next_deadline().is_some_and(|deadline| deadline <= now) {
                        consume_work(ctx, 1).await?;
                        if let Some(output) = op.take_timed_due(now)? {
                            if !tx.send(output).await? {
                                op.cleanup();
                                return Ok(0);
                            }
                        }
                    }
                    continue;
                }
                StreamControl::FeedObservation { coverage_since } if op.is_silence() => {
                    if *coverage_since < -1 {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "invalid feed coverage sentinel",
                        ));
                    }
                    op.observe_feed((*coverage_since != -1).then_some(*coverage_since))?;
                    while op.next_deadline().is_some_and(|deadline| deadline <= now) {
                        consume_work(ctx, 1).await?;
                        if let Some(output) = op.take_timed_due(now)? {
                            if !tx.send(output).await? {
                                op.cleanup();
                                return Ok(0);
                            }
                        }
                    }
                }
                StreamControl::CheckpointBarrier { checkpoint_id } => {
                    let aligned = ctx.aligned.as_ref().expect("timed admission");
                    if !aligned.acks.is_active(*checkpoint_id) {
                        continue;
                    }
                    let freeze =
                        crate::barrier::EncodedFreeze::from_iot(op, &ctx.owner, ctx.max_state_keys);
                    aligned
                        .acks
                        .iot_frozen(*checkpoint_id, op.operator_id(), freeze)
                        .await;
                }
                StreamControl::GraphProgress {
                    watermark_micros,
                    flags,
                } if ctx.graph_time.is_some() => {
                    crate::graph_cut::Progress::from_control(*watermark_micros, *flags)?;
                }
                StreamControl::GraphRoundEnd { .. } if ctx.graph_time.is_some() => {}
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "timed IoT accepts only ordered time and checkpoint controls",
                    ))
                }
            }
            if !tx.send_control(control).await? {
                break;
            }
        }
    }
    reporter.sample(op);
    op.report_resample_metrics(&ctx.metrics);
    op.cleanup();
    Ok(0)
}

async fn emit_window(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    tx: &MailboxTx,
    capture: &SharedCapture,
    emission: crate::window::WindowEmission,
    scratch: Option<sparrow_model::MemoryLease>,
) -> Result<bool> {
    if emission.future_dropped > 0 {
        ctx.metrics.record_future_dropped(emission.future_dropped);
    }
    if !emission.lates.is_empty() {
        capture.push_late(&emission.lates);
    }
    let batches = build_rows_chunked(ctx, op, emission.finals)?;
    if ctx.side_output.is_some() && !emission.lates.is_empty() {
        if let Some(batch) = op.build_late_batch(emission.lates)? {
            graph::publish_side(ctx, batch).await?;
        }
    } else {
        drop(emission.lates);
    }
    drop(scratch); // all raw outputs now have their own resident-sized leases
    for out in batches {
        if !tx.send(out).await? {
            return Ok(false);
        }
    }
    if let Some(wm) = emission.pending_close {
        loop {
            let Some(out) =
                op.take_closed_batch(wm, ctx.mailbox.max_items, ctx.mailbox.max_bytes)?
            else {
                break;
            };
            if ctx.graph_time.is_some() {
                consume_work(ctx, out.num_rows() as u64).await?;
            }
            if !tx.send(out).await? {
                return Ok(false);
            }
        }
        if ctx.graph_time.is_some() && wm == i64::MAX {
            op.advance_graph_final();
        } else {
            op.advance_holdback(wm)?;
        }
        if ctx.graph_time.is_none()
            && !tx
                .send_control(StreamControl::Watermark {
                    input: sparrow_model::InputId::SINGLE.raw(),
                    wm_micros: wm,
                })
                .await?
        {
            return Ok(false);
        }
    }
    ctx.metrics
        .record_state(op.key_count() as u64, op.retention_bytes() as u64);
    if let (Some(wm_in), Some(wm_out)) = (op.wm_in(), op.wm_out()) {
        ctx.metrics
            .record_watermark_lag(wm_in.saturating_sub(wm_out));
    }
    Ok(true)
}

fn build_rows_chunked(ctx: &JobCtx, op: &WindowOperator, rows: Vec<Row>) -> Result<Vec<RowBatch>> {
    let max_rows = ctx.mailbox.max_items.max(1);
    let max_bytes = ctx.mailbox.max_bytes.max(1);
    let mut chunk = Vec::new();
    let mut bytes = 0usize;
    let mut batches = Vec::new();
    for row in rows {
        let sz = row.resident_bytes().saturating_add(64);
        if !chunk.is_empty() && (chunk.len() >= max_rows || bytes.saturating_add(sz) > max_bytes) {
            if let Some(out) = op.build_metered_batch(std::mem::take(&mut chunk))? {
                batches.push(out);
            }
            bytes = 0;
        }
        bytes = bytes.saturating_add(sz);
        chunk.push(row);
    }
    if let Some(out) = op.build_metered_batch(chunk)? {
        batches.push(out);
    }
    Ok(batches)
}

async fn emit_due(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    tx: &MailboxTx,
    now: i64,
    n: &mut usize,
) -> Result<bool> {
    if !op.begin_due(now) {
        return Ok(true);
    }
    while let Some(out) = op.take_closed_batch(now, ctx.mailbox.max_items, ctx.mailbox.max_bytes)? {
        *n += out.num_rows();
        if !tx.send(out).await? {
            return Ok(false);
        }
    }
    ctx.metrics
        .record_state(op.key_count() as u64, op.retention_bytes() as u64);
    Ok(true)
}

async fn window_stage<const GRAPH: bool>(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
    capture: &SharedCapture,
) -> Result<usize> {
    if ctx.ordered_time {
        return ordered_window_task(ctx, op, rx, tx, capture)?.await;
    }
    let mut timer_reporter = TimerReporter {
        ctx,
        live: 0,
        cancelled: 0,
    };
    let mut input_closed = false;
    let mut eof_received = false;
    let mut deferred_eof = false;
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
            if !emit_due(ctx, op, tx, ctx.clock.now_micros(), &mut n).await? {
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
                            let forward_activity=matches!(ctrl,StreamControl::Idle {..}|StreamControl::Active {..}).then(||ctrl.clone());
                            let side_control=ctrl.clone();
                            // Freeze has its own pre-admitted encoding lease.
                            // A failed checkpoint must abort that request, not
                            // fail the live job while reserving unused output.
                            let emission = match ctrl {
                                StreamControl::LiveFeedStart {..} | StreamControl::LiveFeedEnd {..} | StreamControl::ProcessingTime { .. } | StreamControl::FeedObservation {..} | StreamControl::GraphProgress {..} | StreamControl::GraphRoundEnd {..} => return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"ordered processing time is not enabled for windows")),
                                StreamControl::EndOfInput=>{
                                    if !GRAPH{return Err(SparrowError::new(ErrorCode::InvalidArgument,"explicit EOF belongs to graph ingress"));}
                                    eof_received=true;
                                    if op.is_processing_time(){input_closed=true;deferred_eof=true;crate::window::WindowEmission::default()}
                                    else{op.finish_input()?}
                                },
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
                                            let frozen = crate::barrier::EncodedFreeze::from_operator(&op, &ctx.owner, ctx.max_state_keys);
                                            aj.acks.state_frozen(checkpoint_id,op.operator_id(),frozen).await;
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
                            if !emit_window(ctx, op, tx, capture, emission,None).await? {
                                input_closed = true;
                            }
                            if let Some(control)=forward_activity { if !tx.send_control(control).await? {return Ok(n);} }
                            if matches!(side_control,StreamControl::EndOfInput)&&!deferred_eof {if !tx.send_control(StreamControl::EndOfInput).await?{return Ok(n);}}
                            if let Some((side,drop_when_full))=&ctx.side_output {graph::publish_control(ctx,side,*drop_when_full,side_control).await?;}
                        }
                        if let Some(batch) = batch {
                            if GRAPH&&eof_received {return Err(SparrowError::new(ErrorCode::JobFailed,"window data after EOF").at_operator(op.operator_id()));}
                            consume_work(ctx, batch.num_rows() as u64).await?;
                            let now=ctx.clock.now_micros();
                            if !emit_due(ctx,op,tx,now,&mut n).await? {break;}
                            let scratch = op.working_credit(Some(&batch))?;
                            let started=std::time::Instant::now();
                            let result=op.on_batch_without_timers(&batch,now);
                            if let Some(obs)=&ctx.observation {obs.record(Latency::Window,started.elapsed());}
                            let emission=result?;
                            n += emission.finals.len();
                            if !emit_window(ctx, op, tx, capture, emission,Some(scratch)).await? {
                                input_closed = true;
                            }
                        }
                    }
                    None => {
                        if GRAPH&&!eof_received&&!ctx.cancel.is_cancelled(){return Err(SparrowError::new(ErrorCode::JobFailed,"window input closed without EOF").at_operator(op.operator_id()));}
                        input_closed=true;
                    },
                }
            }
            _ = ctx.clock.sleep_until(deadline) => {
                if !emit_due(ctx,op,tx,ctx.clock.now_micros(),&mut n).await? {
                    break;
                }
            }
        }
    }
    if deferred_eof && !ctx.cancel.is_cancelled() {
        tx.send_control(StreamControl::EndOfInput).await?;
    }
    op.cleanup();
    timer_reporter.sample(op.live_timers(), op.cancelled_timers());
    Ok(n)
}

/// The durable branch is cold for ordinary Count/live windows. Do not embed
/// its complete async state in every legacy window task. Field drop order
/// keeps the reservation until the boxed future (including cancellation) drops.
struct ChargedWindowFuture<F> {
    future: std::pin::Pin<Box<F>>,
    _credit: sparrow_model::MemoryLease,
}
impl<F: std::future::Future> std::future::Future for ChargedWindowFuture<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

/// Keep the external request/timeout/reorder continuation off every legacy
/// stage's generator. Acquire exact frame credit BEFORE allocating its Box;
/// field order keeps that credit through cancellation and future destruction.
#[cold]
#[inline(never)]
fn external_lookup_batch<'a>(
    owner: &Arc<MemoryOwner>,
    op: &'a mut ExternalLookupOperator,
    batch: &'a RowBatch,
    cancel: CancellationToken,
) -> Result<ChargedWindowFuture<impl std::future::Future<Output = Result<Option<RowBatch>>> + 'a>> {
    let future = op.on_batch_into(batch, cancel);
    let credit = owner.acquire(
        sparrow_model::CreditKind::Reservation,
        std::mem::size_of_val(&future).saturating_add(64),
    )?;
    Ok(ChargedWindowFuture {
        future: Box::pin(future),
        _credit: credit,
    })
}

#[cfg(test)]
pub(crate) fn external_lookup_future_sizes() -> (usize, usize) {
    fn bytes<F>(
        _: impl FnOnce(&'static mut ExternalLookupOperator, &'static RowBatch, CancellationToken) -> F,
    ) -> (usize, usize) {
        (
            std::mem::size_of::<F>(),
            std::mem::size_of::<ChargedWindowFuture<F>>(),
        )
    }
    // Type inspection only: do not create fake references or execute requests.
    bytes(ExternalLookupOperator::on_batch_into)
}

#[cfg(test)]
pub(crate) fn external_lookup_charged_for_test<'a>(
    owner: &Arc<MemoryOwner>,
    op: &'a mut ExternalLookupOperator,
    batch: &'a RowBatch,
    cancel: CancellationToken,
) -> Result<impl std::future::Future<Output = Result<Option<RowBatch>>> + 'a> {
    external_lookup_batch(owner, op, batch, cancel)
}

#[cold]
fn ordered_window_task<'a>(
    ctx: &'a JobCtx,
    op: &'a mut WindowOperator,
    rx: &'a mut MailboxRx,
    tx: &'a MailboxTx,
    capture: &'a SharedCapture,
) -> Result<ChargedWindowFuture<impl std::future::Future<Output = Result<usize>> + 'a>> {
    // Keep construction outside the parent async body: otherwise even a
    // subsequently boxed local can retain its large generator storage there.
    let future = ordered_window_stage(ctx, op, rx, tx, capture);
    let credit = ctx.owner.acquire(
        sparrow_model::CreditKind::Reservation,
        std::mem::size_of_val(&future).saturating_add(64),
    )?;
    Ok(ChargedWindowFuture {
        future: Box::pin(future),
        _credit: credit,
    })
}

#[cfg(test)]
pub(crate) fn time_graph_window_future_sizes() -> (usize, usize) {
    fn bytes<F>(
        _: impl FnOnce(
            &'static JobCtx,
            &'static mut WindowOperator,
            &'static mut MailboxRx,
            &'static MailboxTx,
            &'static SharedCapture,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    // Type inspection only: no references are constructed and neither async
    // function is called. Keep cold durable futures off the legacy task layout.
    (bytes(window_stage::<false>), bytes(ordered_window_stage))
}

/// PT/Count stages in a durable clock pipeline never sample or wake on a host
/// clock. A time control is forwarded before derived rows, just as for IoT.
async fn ordered_window_stage(
    ctx: &JobCtx,
    op: &mut WindowOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
    capture: &SharedCapture,
) -> Result<usize> {
    let mut now = ctx.clock.now_micros();
    if !op.is_event_time() {
        op.validate_processing_cut(now)?;
    }
    let mut n = 0;
    let mut timers = TimerReporter {
        ctx,
        live: 0,
        cancelled: 0,
    };
    loop {
        timers.sample(op.live_timers(), op.cancelled_timers());
        let envelope = tokio::select! { biased; _=ctx.cancel.cancelled()=>break, e=rx.recv()=>e? };
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
            consume_work(ctx, batch.num_rows() as u64).await?;
            let scratch = op.working_credit(Some(&batch))?;
            let observation = ctx
                .graph_time
                .as_ref()
                .filter(|g| g.event_time)
                .map_or(now, |g| g.observed_micros());
            let emission = op.on_batch_without_timers(&batch, observation)?;
            n += emission.finals.len();
            if !emit_window(ctx, op, tx, capture, emission, Some(scratch)).await? {
                break;
            }
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
                    now = micros;
                    if !tx
                        .send_control(StreamControl::ProcessingTime { micros })
                        .await?
                    {
                        break;
                    }
                    if op.is_event_time() {
                        continue;
                    }
                    op.begin_due(now);
                    while let Some(out) =
                        op.take_closed_batch(now, ctx.mailbox.max_items, ctx.mailbox.max_bytes)?
                    {
                        consume_work(ctx, out.num_rows() as u64).await?;
                        n += out.num_rows();
                        if !tx.send(out).await? {
                            op.cleanup();
                            return Ok(n);
                        }
                    }
                    ctx.metrics
                        .record_state(op.key_count() as u64, op.retention_bytes() as u64);
                }
                StreamControl::CheckpointBarrier { checkpoint_id } => {
                    let aligned = ctx.aligned.as_ref().expect("ordered admission");
                    if !aligned.acks.is_active(checkpoint_id) {
                        continue;
                    }
                    let freeze = crate::barrier::EncodedFreeze::from_operator(
                        op,
                        &ctx.owner,
                        ctx.max_state_keys,
                    );
                    aligned
                        .acks
                        .state_frozen(checkpoint_id, op.operator_id(), freeze)
                        .await;
                    if !tx
                        .send_control(StreamControl::CheckpointBarrier { checkpoint_id })
                        .await?
                    {
                        break;
                    }
                }
                StreamControl::GraphProgress {
                    watermark_micros,
                    flags,
                } if ctx.graph_time.is_some() => {
                    let mut p = crate::graph_cut::Progress::from_control(watermark_micros, flags)?;
                    if op.is_event_time() {
                        let emission = if p.eof {
                            let mut emission = op.finish_input()?;
                            emission.pending_close = Some(i64::MAX);
                            emission
                        } else if p.idle {
                            op.mark_idle(sparrow_model::InputId::SINGLE)?
                        } else {
                            op.mark_active(sparrow_model::InputId::SINGLE)?;
                            match p.watermark {
                                Some(wm) => {
                                    op.observe_watermark(sparrow_model::InputId::SINGLE, wm)?
                                }
                                None => Default::default(),
                            }
                        };
                        n += emission.finals.len();
                        if !emit_window(ctx, op, tx, capture, emission, None).await? {
                            break;
                        }
                        p.watermark = op.wm_out();
                    }
                    if !tx.send_control(p.control()).await? {
                        break;
                    }
                }
                StreamControl::GraphRoundEnd { sequence } if ctx.graph_time.is_some() => {
                    if !tx
                        .send_control(StreamControl::GraphRoundEnd { sequence })
                        .await?
                    {
                        break;
                    }
                }
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "ordered windows accept only time and checkpoint controls",
                    ))
                }
            }
        }
    }
    op.cleanup();
    Ok(n)
}

/// Legacy split channels are adapted at the boundary; batching, accounting,
/// cancellation and punctuation forwarding have one implementation.
enum LiveInput {
    Ordered(ObservedReceiver<IngressEvent>),
    Split {
        rows: ObservedReceiver<Row>,
        controls: Option<tokio::sync::mpsc::Receiver<StreamControl>>,
    },
}

impl LiveInput {
    fn last_origin(&self) -> OriginSpan {
        match self {
            Self::Ordered(rx) => rx.last_origin(),
            Self::Split { rows, .. } => rows.last_origin(),
        }
    }
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

    fn try_recv_many(&mut self, out: &mut Vec<(IngressEvent, OriginSpan)>, limit: usize) {
        match self {
            Self::Ordered(rx) => {
                rx.try_recv_many(out, limit);
            }
            Self::Split { rows, .. } => {
                for _ in 0..limit {
                    let Ok(row) = rows.try_recv() else {
                        break;
                    };
                    out.push((IngressEvent::Row(row), rows.last_origin()));
                }
            }
        }
    }

    fn ready_control(&mut self) -> Option<StreamControl> {
        match self {
            Self::Split { controls, .. } => controls.as_mut().and_then(|rx| rx.try_recv().ok()),
            Self::Ordered(_) => None,
        }
    }
}

async fn budgeted_live_source(
    ctx: JobCtx,
    schema: sparrow_model::Schema,
    tx: MailboxTx,
    mut rx: ObservedReceiver<sparrow_model::QueuedRow>,
) -> Result<usize> {
    let schema = Arc::new(schema);
    let mut n = 0;
    let mut carry = None;
    let cap = ctx
        .mailbox
        .max_bytes
        .min(ctx.owner.budget().reservation_bytes);
    loop {
        if ctx.cancel.is_cancelled() {
            return Ok(n);
        }
        let first = match carry.take() {
            Some(row) => Some(row),
            None => tokio::select! { biased;
                _ = ctx.cancel.cancelled() => return Ok(n),
                row = rx.recv() => row.map(|r|(r,rx.last_origin())),
            },
        };
        let Some((first, mut origin)) = first else {
            if ctx.graph_mode && !ctx.cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    "live graph input closed without EOF",
                ));
            }
            return Ok(n);
        };
        let mut builder = sparrow_model::RowBatchBuilder::new(
            schema.clone(),
            ctx.owner.clone(),
            sparrow_model::CreditKind::Reservation,
            ctx.rows_per_batch,
            cap,
        )?;
        first.push_into(&mut builder)?;
        while builder.num_rows() < ctx.rows_per_batch {
            match rx.try_recv() {
                Ok(row) if builder.current_bytes().saturating_add(row.bytes()) <= cap => {
                    origin.merge(rx.last_origin());
                    row.push_into(&mut builder)?;
                }
                Ok(row) => {
                    carry = Some((row, rx.last_origin()));
                    break;
                }
                Err(_) => break,
            }
        }
        let batch = builder
            .finish()?
            .with_origin(origin)
            .with_source_operator(ctx.source_operator);
        n += batch.num_rows();
        ctx.metrics.record_ingest(batch.num_rows() as u64);
        if let Some(obs) = &ctx.observation {
            obs.runtime_rows(true, batch.num_rows());
        }
        if !graph::send_source(&ctx, &tx, batch).await? {
            return Ok(n);
        }
    }
}

async fn live_source<const GRAPH: bool>(
    ctx: JobCtx,
    schema: sparrow_model::Schema,
    tx: MailboxTx,
    mut events: LiveInput,
) -> Result<usize> {
    let schema = Arc::new(schema);
    let mut n = 0usize;
    let mut ended = false;
    let mut ready = Vec::new();
    loop {
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Ok(n),
            ev = events.recv() => {
                let Some(ev) = ev else {
                    if GRAPH&&!ended&&!ctx.cancel.is_cancelled(){return Err(SparrowError::new(ErrorCode::JobFailed,"ordered graph input closed without explicit EOF"));}
                    return Ok(n);
                };
                ready.push((ev, events.last_origin()));
                events.try_recv_many(&mut ready, ctx.rows_per_batch.saturating_sub(1));
                let mut rows = Vec::new();
                let mut origin = OriginSpan::default();
                for (event, at) in ready.drain(..) {
                    if GRAPH {
                        if ended&&!matches!(event,IngressEvent::Control(StreamControl::CheckpointBarrier {..})|IngressEvent::Control(StreamControl::EndOfInput)){
                            return Err(SparrowError::new(ErrorCode::JobFailed,"graph source data/control after EOF"));
                        }
                        if matches!(event,IngressEvent::Control(StreamControl::EndOfInput)){ended=true;}
                    }
                    match event {
                        IngressEvent::LiveFeed(_) => {
                            return Err(SparrowError::new(ErrorCode::InvalidArgument,"live feed event outside the live silence profile"));
                        }
                        IngressEvent::DecodeError => {
                            if !publish_live_rows(&ctx,&schema,&tx,&mut rows,origin,&mut n).await? {return Ok(n);}
                            origin=OriginSpan::default();
                            let source=ctx.source_operator.ok_or_else(||SparrowError::new(ErrorCode::InvalidArgument,"decode side output requires graph source identity"))?;
                            let _scratch=ctx.owner.acquire(sparrow_model::CreditKind::Reservation,512)?;
                            let mut builder=sparrow_model::RowBatchBuilder::new(Arc::new(sparrow_plan::graph::decode_error_schema()),ctx.owner.clone(),sparrow_model::CreditKind::Reservation,1,512)?;
                            builder.push(Row {values:vec![sparrow_model::Scalar::UInt64(source.raw() as u64),sparrow_model::Scalar::utf8("CodecViolation")]})?;
                            graph::publish_side(&ctx,builder.finish()?.with_source_operator(Some(source)).with_origin(at)).await?;
                        }
                        IngressEvent::Row(row) => { rows.push(row); origin.merge(at); }
                        IngressEvent::Batch(batch) => {
                            let batch=batch.into_batch();
                            if !Arc::ptr_eq(batch.lease().owner(),&ctx.owner) {
                                return Err(SparrowError::new(ErrorCode::PolicyDenied,"ingress batch belongs to another Job memory owner"));
                            }
                            if batch.schema()!=schema.as_ref() || batch.num_rows()>ctx.rows_per_batch {
                                return Err(SparrowError::new(ErrorCode::InvalidSchema,"ingress batch schema/row bound differs from live source"));
                            }
                            if !publish_live_rows(&ctx,&schema,&tx,&mut rows,origin,&mut n).await? {return Ok(n);}
                            origin=OriginSpan::default();
                            let count=batch.num_rows();
                            ctx.metrics.record_ingest(count as u64);
                            if let Some(obs)=&ctx.observation {obs.runtime_rows(true,count);}
                            n+=count;
                            if !graph::send_source(&ctx,&tx,batch.with_origin(at).with_source_operator(ctx.source_operator)).await? {return Ok(n);}
                        }
                        IngressEvent::Control(control) => {
                            if !publish_live_rows(&ctx, &schema, &tx, &mut rows, origin, &mut n).await?
                                || !publish_source_control(&ctx,&tx,control).await? { return Ok(n); }
                            origin = OriginSpan::default();
                        }
                    }
                }
                if !publish_live_rows(&ctx, &schema, &tx, &mut rows, origin, &mut n).await? {
                    return Ok(n);
                }
                while let Some(control) = events.ready_control() {
                    if !publish_source_control(&ctx,&tx,control).await? { return Ok(n); }
                }
            }
        }
    }
}

async fn publish_live_rows(
    ctx: &JobCtx,
    schema: &Arc<sparrow_model::Schema>,
    tx: &MailboxTx,
    rows: &mut Vec<Row>,
    origin: OriginSpan,
    n: &mut usize,
) -> Result<bool> {
    if rows.is_empty() {
        return Ok(true);
    }
    for batch in build_source_batches_shared(
        schema.clone(),
        std::mem::take(rows),
        &ctx.owner,
        ctx.rows_per_batch,
    )? {
        *n += batch.num_rows();
        ctx.metrics.record_ingest(batch.num_rows() as u64);
        if let Some(obs) = &ctx.observation {
            obs.runtime_rows(true, batch.num_rows());
        }
        if !graph::send_source(
            ctx,
            tx,
            batch
                .with_origin(origin)
                .with_source_operator(ctx.source_operator),
        )
        .await?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn publish_source_control(
    ctx: &JobCtx,
    tx: &MailboxTx,
    control: StreamControl,
) -> Result<bool> {
    graph::source_activity(ctx, &control);
    if let Some((side, drop_when_full)) = &ctx.side_output {
        graph::publish_control(ctx, side, *drop_when_full, control.clone()).await?;
    }
    let checkpoint_id = match &control {
        StreamControl::CheckpointBarrier { checkpoint_id } => Some(*checkpoint_id),
        _ => None,
    };
    if !tx.send_control(control).await? {
        return Ok(false);
    }
    if let (Some(id), Some(aligned)) = (checkpoint_id, &ctx.aligned) {
        if let Some(source) = ctx.source_operator {
            aligned.acks.source_cut_for(id, source).await;
        } else {
            aligned.acks.source_cut(id).await;
        }
    }
    Ok(true)
}

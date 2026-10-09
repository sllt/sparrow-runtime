//! Converges catalog desired state to in-process actual jobs.
//! Catalog commit never waits for MQTT/HTTP connect.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "demo-io")]
use sparrow_connectors::{publish_qos0, sensor_json, EmbeddedBroker, HttpCapture};
use sparrow_connectors::{
    FileReplayConfig, FileReplaySource, HttpPollSource, HttpPushSource, HttpSink, IoDiagnostics,
    LogSink, MqttSink, MqttSource,
};
use sparrow_io::observed;
use sparrow_io::ReplayableSource;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{InflightCounter, RecoveryPolicy, ResourceBudget, Result, SparrowError};
use sparrow_plan::{PhysicalPlan, CheckpointPlan};
#[cfg(test)]
use sparrow_plan::PlanLayout;
use sparrow_runtime::{
    AlignedAcks, AlignedJob, PipelineSnapshot, PipelineRestore, CheckpointStore, IngressEvent, JobHandle,
    JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture, SourceAdmission,
    StreamControl,
};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::checkpoint::{CheckpointAdmission, CheckpointControl, CheckpointSpec};
use crate::store::{ActualState, Store};
use crate::validate::{
    bind_plan_with_store, http_config, http_poll_config, http_push_config, mqtt_config,
    mqtt_sink_config, store_policy, stream_to_schema, validate_aligned_plan, validate_io_with_plan,
    DemoEndpoints, StoreSecrets,
};

/// Cap on **consecutive** start failures. Lifetime `attempt_id` still
/// increments on start/stop/running/failed; it must not trigger this hold.
pub(crate) const MAX_PIPELINE_ATTEMPTS: u64 = 16;

#[cfg(feature="jetstream")]
mod jetstream_source;
mod graph;
mod paused_time;
mod paused_time_log;
mod observed_time;
mod live_silence;
mod observed_time_log;
mod graph_time;
mod graph_time_log;
mod prepared_sink;

/// Per-pipeline backoff, capped at 32 seconds; never sleep in converge.
/// A successful launch is not a stable recovery: reset after 30s running.
pub(crate) fn retry_backoff(consecutive_failures: u64) -> Duration {
    let shift = consecutive_failures.min(7) as u32;
    Duration::from_millis(250u64.saturating_mul(1u64 << shift))
}

pub(crate) fn capacity_backoff(attempt: u32) -> Duration {
    Duration::from_millis((500u64 << attempt.saturating_sub(1).min(4)).min(5_000))
}

fn is_capacity_wait(error: &SparrowError) -> bool {
    error.retryable
        && error.code == sparrow_model::ErrorCode::ResourceExhausted
        && error
            .context
            .iter()
            .any(|(key, value)| key == "admission" && value == "capacity")
}

#[cfg(feature = "demo-io")]
pub struct DemoHarness {
    pub broker: EmbeddedBroker,
    pub http: HttpCapture,
}

#[cfg(feature = "demo-io")]
impl DemoHarness {
    pub async fn start() -> Result<Self> {
        let broker = EmbeddedBroker::start().await.map_err(SparrowError::from)?;
        let http = HttpCapture::start().await.map_err(SparrowError::from)?;
        Ok(Self { broker, http })
    }

    pub fn endpoints(&self) -> DemoEndpoints {
        DemoEndpoints {
            mqtt_host: self.broker.host(),
            mqtt_port: self.broker.port(),
            http_url: self.http.url(),
            http_port: self.http.port(),
        }
    }

    pub async fn publish_fixture(&self) -> Result<()> {
        let events = [
            ("edge-a", 18.5, 41.0, 1_700_000_000_000_000i64, false),
            ("edge-a", 26.2, 39.0, 1_700_000_001_000_000, false),
            ("edge-b", 31.0, 55.0, 1_700_000_002_000_000, true),
            ("edge-b", 22.0, 48.0, 1_700_000_003_000_000, false),
            ("edge-c", 29.4, 33.0, 1_700_000_004_000_000, true),
            ("edge-c", 12.0, 70.0, 1_700_000_005_000_000, false),
        ];
        for (i, (id, temp, hum, ts, alert)) in events.into_iter().enumerate() {
            publish_qos0(
                &self.broker.host(),
                self.broker.port(),
                &format!("m3-pub-{i}"),
                "sensors/json",
                sensor_json(id, temp, hum, ts, alert),
            )
            .await
            .map_err(SparrowError::from)?;
        }
        Ok(())
    }
}

enum AlignedCmd {
    Checkpoint {
        reply: tokio::sync::oneshot::Sender<Result<u64>>,
        admission: CheckpointAdmission,
    },
}

async fn request_checkpoint(
    cmd: &tokio::sync::mpsc::Sender<AlignedCmd>,
    control: &Arc<CheckpointControl>,
    cancel: &CancellationToken,
    trigger: &'static str,
) -> Result<u64> {
    if cancel.is_cancelled() {
        return Err(SparrowError::new(
            sparrow_model::ErrorCode::Cancelled,
            "job stopping",
        ));
    }
    let admission = control.begin(trigger)?;
    request_checkpoint_with_admission(cmd, control, cancel, admission).await
}

async fn request_checkpoint_with_admission(
    cmd: &tokio::sync::mpsc::Sender<AlignedCmd>,
    control: &Arc<CheckpointControl>,
    cancel: &CancellationToken,
    admission: CheckpointAdmission,
) -> Result<u64> {
    let deadline = admission.deadline;
    let (reply, wait) = tokio::sync::oneshot::channel();
    cmd.try_send(AlignedCmd::Checkpoint { reply, admission })
        .map_err(|_| {
            SparrowError::new(
                sparrow_model::ErrorCode::Cancelled,
                "checkpoint actor unavailable",
            )
        })?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(SparrowError::new(sparrow_model::ErrorCode::Cancelled, "job stopping")),
        reply = wait => reply.map_err(|_| SparrowError::new(sparrow_model::ErrorCode::Cancelled, "checkpoint reply dropped"))?,
        _ = tokio::time::sleep_until(deadline) => {
            control.waiter_timeout();
            Err(SparrowError::new(sparrow_model::ErrorCode::ResourceExhausted,
                "checkpoint wait timed out; a blocking durable commit may still finish, inspect status before retry").retryable(true))
        }
    }
}

async fn periodic_checkpoints(
    cmd: tokio::sync::mpsc::Sender<AlignedCmd>,
    control: Arc<CheckpointControl>,
    cancel: CancellationToken,
) {
    let Some(ms) = control.policy.interval_ms else {
        return;
    };
    let period = Duration::from_millis(ms);
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { biased; _ = cancel.cancelled() => break, _ = tick.tick() => {} }
        if request_checkpoint(&cmd, &control, &cancel, "periodic")
            .await
            .is_err()
            && cmd.is_closed()
        {
            break;
        }
    }
}

enum RunningKind {
    Live {
        handle: JobHandle,
        source: JoinHandle<Result<()>>,
        sink: JoinHandle<()>,
    },
    Aligned {
        cancel: CancellationToken,
        cmd: tokio::sync::mpsc::Sender<AlignedCmd>,
        handle: JobHandle,
        source: JoinHandle<Result<()>>,
        sink: JoinHandle<()>,
        scheduler: Option<JoinHandle<()>>,
        checkpoint: Arc<CheckpointControl>,
    },
}

const STABLE_RUN: Duration = Duration::from_secs(30);

#[cfg(test)]
mod production_checkpoint_tests {
    use super::*;
    #[test]
    fn self_review_cancelled_stop_keeps_transition_until_children_finish() {
        let kernel = Arc::new(compact_kernel().unwrap());
        kernel.block_on(async {
            for all in [false,true] {
                let store=Arc::new(Store::open_memory().unwrap());
                let sup=Supervisor::new(store,kernel.clone(),false,None).unwrap();
                let schema=sparrow_model::Schema::new(1,vec![sparrow_model::Field::new(1,"v",sparrow_model::DataType::Int64,false)]).unwrap();
                let mut catalog=sparrow_plan::Catalog::new();catalog.insert("s",schema);
                let plan=sparrow_sql::bind_sql("SELECT v FROM s",&catalog,1.into(),1.into()).unwrap();
                let handle=kernel.submit(JobRequest::new(sparrow_plan::physicalize(&plan,&Default::default()),vec![],SharedCapture::disabled())).unwrap();
                let lease=kernel.process_owner().acquire(sparrow_model::CreditKind::Retention,128).unwrap();
                let (release,wait)=tokio::sync::oneshot::channel();
                let source=kernel.handle().spawn(async move {let _lease=lease;let _=wait.await;Ok(())});
                let sink=kernel.handle().spawn(async {});
                sup.running.lock().await.insert("delayed".into(),RunningJob {
                    graph_ports:None,
                    source_kind:"file",sink_kind:"log",started_at:Instant::now(),stable:false,revision:1,
                    diag:IoDiagnostics::new(),kind:RunningKind::Live{handle,source,sink},
                });
                let s=sup.clone();
                let waiter=tokio::spawn(async move {if all {s.stop_all().await;}else{s.kill_named("delayed").await.unwrap();}});
                tokio::time::timeout(Duration::from_secs(2),async {
                    while sup.running.lock().await.contains_key("delayed") {tokio::task::yield_now().await;}
                }).await.unwrap();
                waiter.abort();let _=waiter.await;
                let retained=sup.transition.try_lock().is_err();
                release.send(()).unwrap();
                sup.stop_all().await;
                assert!(retained,"cancelled waiter released transition while old source was still alive; stop_all={all}");
                assert_eq!(kernel.process_owner().usage().physical_bytes,0);
            }
        });
    }
    #[tokio::test(start_paused = true)]
    async fn production_periodic_scheduler_has_no_overlap_or_detached_tick_tasks() {
        let control = crate::checkpoint::tests::control();
        let cancel = CancellationToken::new();
        let (cmd, mut rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(periodic_checkpoints(cmd, control.clone(), cancel.clone()));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        let AlignedCmd::Checkpoint {
            reply,
            mut admission,
        } = rx.recv().await.unwrap();
        tokio::time::advance(Duration::from_millis(200)).await;
        tokio::task::yield_now().await;
        assert!(reply.is_closed());
        assert!(control.snapshot().active);
        assert_eq!(control.snapshot().started, 1);
        assert!(rx.try_recv().is_err());
        admission.finish(&Ok(1), None);
        drop(admission);
        drop(reply);
        cancel.cancel();
        task.await.unwrap();
        let snapshot = control.snapshot();
        assert_eq!(
            (snapshot.started, snapshot.succeeded, snapshot.failed),
            (1, 1, 0)
        );
        assert!(snapshot.timed_out_waiters > 0);
        assert!(!snapshot.active);
    }
    #[tokio::test(start_paused = true)]
    async fn production_queued_manual_timeout_keeps_gate_until_actor_discards() {
        let control = crate::checkpoint::tests::control();
        let cancel = CancellationToken::new();
        let (cmd, mut rx) = tokio::sync::mpsc::channel(1);
        let c = control.clone();
        let stop = cancel.clone();
        let call = tokio::spawn(async move { request_checkpoint(&cmd, &c, &stop, "manual").await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(101)).await;
        assert!(call.await.unwrap().is_err());
        assert!(control.begin("periodic").is_err());
        let AlignedCmd::Checkpoint { reply, admission } = rx.recv().await.unwrap();
        assert!(reply.is_closed());
        drop(admission);
        let s = control.snapshot();
        assert_eq!((s.started, s.failed, s.active), (1, 1, false));
    }
}

struct RunningJob {
    graph_ports: Option<Arc<GraphPortDiagnostics>>,
    source_kind: &'static str,
    sink_kind: &'static str,
    started_at: Instant,
    stable: bool,
    kind: RunningKind,
    revision: u64,
    diag: Arc<IoDiagnostics>,
}

pub struct PipelineMailboxSnapshot {
    pub running_revision: u64,
    pub finished: bool,
    pub cancel_requested: bool,
    pub runtime: sparrow_runtime::mailbox_observe::JobMailboxSnapshot,
}
pub struct PipelineFlowSnapshot {
    pub graph_ports: Option<Arc<GraphPortDiagnostics>>,
    pub running_revision: u64,
    pub runtime_attempt_id: u64,
    pub finished: bool,
    pub cancel_requested: bool,
    pub source_kind: &'static str,
    pub sink_kind: &'static str,
    pub diagnostics: Arc<IoDiagnostics>,
}

pub struct GraphPortDiagnostics {
    pub sources: std::collections::BTreeMap<u32, Arc<IoDiagnostics>>,
    pub sinks: std::collections::BTreeMap<u32, Arc<IoDiagnostics>>,
}

/// One verified, job-owned reference snapshot plus the exact dependency
/// identities used by the checkpoint participant manifest.  The control
/// plane keeps this bundle together so an aligned layout can never be built
/// from a spec-only placeholder CRC and then paired with different tables.
struct PreparedReferenceTables {
    tables: HashMap<String, Arc<sparrow_runtime::ReferenceTable>>,
    dependencies: Vec<sparrow_plan::ReferenceTableDependency>,
}

impl GraphPortDiagnostics {
    pub fn snapshot(&self)->sparrow_connectors::IoSnapshot {
        let mut snapshot=sparrow_connectors::IoSnapshot::default();
        for port in self.sources.values().chain(self.sinks.values()){snapshot.add_assign(&port.snapshot());}
        snapshot
    }
}
pub struct PipelineCheckpointInventory {
    pub revision: u64,
    pub attempt: Option<u64>,
    pub scope: &'static str,
    pub storage_sample: &'static str,
    pub storage: sparrow_runtime::checkpoint::CheckpointInventory,
}
/// A JetStream Sink that could not confirm a row, a DataBus Sink that could
/// not attach, or a WebSocket / TCP Sink out of reconnect attempts fails the job
/// (fail closed).
fn sink_failed_closed(diags: &[Arc<IoDiagnostics>]) -> Result<()> {
    if diags.iter().any(|d| {
        d.jetstream_sink_fatal
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    }) {
        return Err(SparrowError::new(
            sparrow_model::ErrorCode::JobFailed,
            "JetStream Sink failed closed (stream validation or an unconfirmed PubAck); inspect sink health and jetstream_sink_*; aligned restore replays from the last checkpoint",
        ));
    }
    if diags.iter().any(|d| {
        d.databus_sink_fatal
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    }) {
        return Err(SparrowError::new(
            sparrow_model::ErrorCode::JobFailed,
            "DataBus Sink could not register its publisher (runtime publisher limit); inspect databus_sink_fatal",
        ));
    }
    if diags.iter().any(|d| {
        d.websocket_sink_fatal
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    }) {
        return Err(SparrowError::new(
            sparrow_model::ErrorCode::JobFailed,
            "WebSocket Sink exhausted its reconnect attempts; inspect sink health and websocket_sink_*",
        )
        .retryable(true));
    }
    if diags
        .iter()
        .any(|d| d.tcp_sink_fatal.load(std::sync::atomic::Ordering::Relaxed) > 0)
    {
        return Err(SparrowError::new(
            sparrow_model::ErrorCode::JobFailed,
            "TCP Sink exhausted its reconnect attempts; inspect sink health and tcp_sink_*",
        )
        .retryable(true));
    }
    Ok(())
}

fn observed_sink_kind(spec: &crate::spec::PipelineSpec) -> &'static str {
    match spec.sink.kind.as_str() {
        "log" => "log",
        "mqtt" => "mqtt",
        "nats" => "nats",
        "websocket" => "websocket",
        "tcp" => "tcp",
        "jetstream" => "jetstream",
        "databus" => "databus",
        "file" => "file",
        "plugin" => "plugin",
        _ => "http",
    }
}

impl RunningJob {
    fn flow_snapshot(&self) -> PipelineFlowSnapshot {
        PipelineFlowSnapshot {
            graph_ports: self.graph_ports.clone(),
            running_revision: self.revision,
            runtime_attempt_id: self.handle().attempt.raw(),
            finished: self.is_finished(),
            cancel_requested: self.handle().cancellation().is_cancelled(),
            source_kind: self.source_kind,
            sink_kind: self.sink_kind,
            diagnostics: self.diag.clone(),
        }
    }
    fn handle(&self) -> &JobHandle {
        match &self.kind {
            RunningKind::Live { handle, .. } | RunningKind::Aligned { handle, .. } => handle,
        }
    }
    fn is_finished(&self) -> bool {
        match &self.kind {
            RunningKind::Live { handle, .. } => handle.is_finished(),
            RunningKind::Aligned {
                handle,
                source,
                scheduler,
                ..
            } => {
                handle.is_finished()
                    || source.is_finished()
                    || scheduler.as_ref().is_some_and(|s| s.is_finished())
            }
        }
    }
}

pub struct Supervisor {
    pub(crate) checkpoint_timeout: Duration,
    pub(crate) stable_run: Duration,
    store: Arc<Store>,
    kernel: Arc<Kernel>,
    running: Mutex<HashMap<String, RunningJob>>,
    transition: Mutex<()>,
    transition_work: Arc<tokio::sync::Semaphore>,
    shutdown: CancellationToken,
    /// When a failed pipeline may be retried. Checked with `continue`;
    /// the shared converge loop must not sleep on this map.
    next_retry_at: Mutex<HashMap<String, Instant>>,
    capacity_retries: Mutex<HashMap<String, u32>>,
    wake: Notify,
    safe_mode: bool,
    #[cfg(feature = "demo-io")]
    demo: Option<Arc<DemoHarness>>,
    secrets: StoreSecrets,
    /// In-process topic bus shared by every pipeline of this runtime.
    databus: Arc<sparrow_connectors::DataBus>,
}

/// Demo handle passed into [`Supervisor::new`]. `()` when built without `demo-io`.
#[cfg(feature = "demo-io")]
pub type DemoIo = Arc<DemoHarness>;
#[cfg(not(feature = "demo-io"))]
pub type DemoIo = ();

impl Supervisor {
    pub fn new(
        store: Arc<Store>,
        kernel: Arc<Kernel>,
        safe_mode: bool,
        #[cfg(feature = "demo-io")] demo: Option<DemoIo>,
        #[cfg(not(feature = "demo-io"))] _demo: Option<DemoIo>,
    ) -> Result<Arc<Self>> {
        store.reset_actual_after_process_restart()?;
        let secrets = StoreSecrets::new(Arc::clone(&store));
        Ok(Arc::new(Self {
            checkpoint_timeout: Duration::from_secs(5),
            stable_run: STABLE_RUN,
            store,
            kernel,
            running: Mutex::new(HashMap::new()),
            transition: Mutex::new(()),
            transition_work: Arc::new(tokio::sync::Semaphore::new(128)),
            shutdown: CancellationToken::new(),
            next_retry_at: Mutex::new(HashMap::new()),
            capacity_retries: Mutex::new(HashMap::new()),
            wake: Notify::new(),
            safe_mode,
            #[cfg(feature = "demo-io")]
            demo,
            secrets,
            databus: sparrow_connectors::DataBus::new(),
        }))
    }

    /// The runtime's Local DataBus (live registrations, for status/tests).
    pub fn databus(&self) -> &Arc<sparrow_connectors::DataBus> {
        &self.databus
    }

    #[cfg(feature = "demo-io")]
    pub fn demo(&self) -> Option<Arc<DemoHarness>> {
        self.demo.clone()
    }

    pub fn demo_endpoints(&self) -> Option<DemoEndpoints> {
        #[cfg(feature = "demo-io")]
        {
            self.demo.as_ref().map(|d| d.endpoints())
        }
        #[cfg(not(feature = "demo-io"))]
        {
            None
        }
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub fn kernel(&self) -> &Kernel {
        &self.kernel
    }

    pub async fn run_loop(self: Arc<Self>) {
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }
            if let Err(error) = self.converge_once().await {
                tracing::error!(code = error.code.as_str(), error = %error, "supervisor_converge_failed");
            }
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    }

    pub async fn shutdown(self: &Arc<Self>) {
        self.begin_shutdown();
        self.stop_all().await;
    }

    pub fn begin_shutdown(&self) {
        self.shutdown.cancel();
    }
    pub fn is_draining(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    pub fn checkpoint_snapshot(&self, name: &str) -> Result<Option<(u64, Arc<CheckpointControl>)>> {
        let jobs = self.running.try_lock().map_err(|_| {
            SparrowError::new(sparrow_model::ErrorCode::ResourceExhausted, "registry busy")
        })?;
        Ok(jobs.get(name).and_then(|job| match &job.kind {
            RunningKind::Aligned { checkpoint, .. } => Some((job.revision, checkpoint.clone())),
            _ => None,
        }))
    }

    pub async fn checkpoint_inventory(&self, name: &str) -> Result<PipelineCheckpointInventory> {
        let active = {
            let jobs = self.running.lock().await;
            jobs.get(name).and_then(|job| match &job.kind {
                RunningKind::Aligned { checkpoint, .. } => {
                    Some((checkpoint.clone(), job.revision, checkpoint.attempt))
                }
                _ => None,
            })
        };
        if let Some((control, revision, attempt)) = active {
            // A status reader must not retain the writer's FileLock after
            // stop/join and accidentally reject the next attempt's admission.
            let storage = control.snapshot().storage.ok_or_else(|| {
                SparrowError::new(
                    sparrow_model::ErrorCode::Internal,
                    "checkpoint storage sample unavailable",
                )
            })?;
            return Ok(PipelineCheckpointInventory {
                revision,
                attempt: Some(attempt),
                scope: "running_attempt",
                storage_sample: "cached_at_start_or_last_completed_commit",
                storage,
            });
        }
        let name = name.to_owned();
        self.catalog(move |store| {
            let row = store.get_pipeline(&name)?;
            if row.spec.recovery != "aligned"
                || !matches!(
                    row.spec.source.kind.as_str(),
                    "file" | "file_replay" | "replay" | "jetstream"
                )
            {
                return Err(SparrowError::new(
                    sparrow_model::ErrorCode::FeatureUnavailable,
                    "aligned replayable source configuration required",
                ));
            }
            let path = row.spec.checkpoint_dir.clone().unwrap_or_else(|| {
                format!(
                    "{}.sparrow-chk",
                    row.spec.source.path.as_deref().unwrap_or("")
                )
            });
            let path = std::path::Path::new(&path);
            sparrow_connectors::check_data_path(path)?;
            let storage = if path.exists() {
                CheckpointStore::open_readonly(path)?.inventory()?
            } else {
                sparrow_runtime::checkpoint::CheckpointInventory {
                    current: None,
                    pinned: None,
                    current_error: None,
                    generations: Vec::new(),
                    bytes: 0,
                    maintenance_error: None,
                    state_generation_marker: None,
                    marker_error: None,
                }
            };
            Ok(PipelineCheckpointInventory {
                revision: row.latest_revision,
                attempt: None,
                scope: "stored_latest_revision",
                storage_sample: "live_disk_listing",
                storage,
            })
        })
        .await
    }

    async fn catalog<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> Result<T> + Send + 'static,
    {
        let store = self.store.clone();
        self.store.run_blocking(move || f(&store)).await
    }

    pub async fn io_snapshot(&self) -> sparrow_connectors::IoSnapshot {
        let g = self.running.lock().await;
        let mut acc = sparrow_connectors::IoSnapshot::default();
        for j in g.values() {
            let snapshot = if let Some(ports) = &j.graph_ports {
                let mut snapshot = ports.snapshot();
                // Refresh failures belong to the root Job, not a connector
                // port. Merge only that counter: adding the complete root
                // snapshot would duplicate graph connector accounting.
                snapshot.lookup_update_failed += j
                    .diag
                    .lookup_update_failed
                    .load(std::sync::atomic::Ordering::Relaxed);
                snapshot
            } else {
                j.diag.snapshot()
            };
            acc.add_assign(&snapshot);
        }
        acc
    }
    pub fn flow_snapshot(&self, name: &str) -> Result<Option<PipelineFlowSnapshot>> {
        let jobs = self.running.try_lock().map_err(|_| {
            SparrowError::new(
                sparrow_model::ErrorCode::ResourceExhausted,
                "runtime observation registry busy",
            )
        })?;
        Ok(jobs.get(name).map(RunningJob::flow_snapshot))
    }
    pub async fn flow_snapshots(&self) -> Vec<(String, PipelineFlowSnapshot)> {
        let jobs = self.running.lock().await;
        jobs.iter()
            .map(|(name, job)| (name.clone(), job.flow_snapshot()))
            .collect()
    }

    /// Nonblocking lookup for synchronous status builders. Contention is unknown,
    /// not an empty/healthy pipeline; callers must expose this distinction.
    pub fn mailbox_snapshot(&self, name: &str) -> Result<Option<PipelineMailboxSnapshot>> {
        let held = {
            let jobs = self.running.try_lock().map_err(|_| {
                SparrowError::new(
                    sparrow_model::ErrorCode::ResourceExhausted,
                    "runtime observation registry busy",
                )
            })?;
            jobs.get(name).map(|job| {
                (
                    job.revision,
                    job.handle().is_finished(),
                    job.handle().cancellation().is_cancelled(),
                    job.handle().mailbox_observer(),
                )
            })
        };
        Ok(
            held.map(|(running_revision, finished, cancel_requested, observer)| {
                PipelineMailboxSnapshot {
                    running_revision,
                    finished,
                    cancel_requested,
                    runtime: observer.snapshot(),
                }
            }),
        )
    }

    pub async fn mailbox_snapshots(&self) -> Vec<(String, PipelineMailboxSnapshot)> {
        let held: Vec<_> = {
            let jobs = self.running.lock().await;
            jobs.iter()
                .map(|(name, job)| {
                    (
                        name.clone(),
                        job.revision,
                        job.handle().is_finished(),
                        job.handle().cancellation().is_cancelled(),
                        job.handle().mailbox_observer(),
                    )
                })
                .collect()
        };
        held.into_iter()
            .map(
                |(name, running_revision, finished, cancel_requested, observer)| {
                    (
                        name,
                        PipelineMailboxSnapshot {
                            running_revision,
                            finished,
                            cancel_requested,
                            runtime: observer.snapshot(),
                        },
                    )
                },
            )
            .collect()
    }

    /// Once admitted, a lifecycle operation owns its guard and children even
    /// if the HTTP/embedding caller stops waiting. Bound detached waiters too.
    async fn transition_operation<T, F, Fut>(
        self: &Arc<Self>,
        wait_for_capacity: bool,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        let permit = if wait_for_capacity {
            self.transition_work
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| {
                    SparrowError::new(
                        sparrow_model::ErrorCode::Cancelled,
                        "lifecycle capacity closed",
                    )
                })?
        } else {
            self.transition_work
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    SparrowError::new(
                        sparrow_model::ErrorCode::ResourceExhausted,
                        "lifecycle operation capacity exhausted",
                    )
                    .retryable(true)
                })?
        };
        let supervisor = self.clone();
        self.kernel
            .handle()
            .spawn(async move {
                let _permit = permit;
                let _transition = supervisor.transition.lock().await;
                operation(supervisor.clone()).await
            })
            .await
            .map_err(|e| {
                SparrowError::new(
                    sparrow_model::ErrorCode::Internal,
                    format!("lifecycle worker: {e}"),
                )
            })?
    }

    pub async fn converge_once(self: &Arc<Self>) -> Result<()> {
        self.transition_operation(false, |sup| async move { sup.converge_inner().await })
            .await
    }

    async fn converge_inner(&self) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Ok(());
        }
        self.reap_finished().await;
        self.refresh_live_lookups().await?;
        let stable_names: Vec<String> = {
            let mut jobs = self.running.lock().await;
            jobs.iter_mut()
                .filter_map(|(name, job)| {
                    if !job.stable
                        && !job.is_finished()
                        && job.started_at.elapsed() >= self.stable_run
                        // Uptime alone can repeatedly reset a slow poison loop.
                        // Reliable attempts need durable input progress as well.
                        && (job.source_kind!="jetstream" || matches!(&job.kind,
                            RunningKind::Aligned{checkpoint,..} if checkpoint.snapshot().reliable_source
                                .is_some_and(|s|s.committed_cut>s.restored_cut)))
                    {
                        job.stable = true;
                        Some(name.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        for name in stable_names {
            self.catalog(move |s| s.reset_consecutive_failures(&name))
                .await?;
        }
        let desired = self.catalog(|s| s.list_desired()).await?;
        for d in desired {
            if d.status == "running" {
                // Read actual state once and fail closed for this pipeline only.
                // A persistent bad row must not starve healthy siblings' start/stop.
                let a = match self.actual_if_start_allowed(&d.name).await {
                    Ok(Some(actual)) => actual,
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::error!(pipeline = %d.name, code = error.code.as_str(),
                            error = %error, "pipeline_converge_skipped");
                        continue;
                    }
                };
                if a.status == "completed" && a.revision == d.revision {
                    continue;
                }
                if (a.status == "failed" || a.status == "waiting") && a.revision == d.revision {
                    // N8: per-pipeline due time. Never sleep here —
                    // a failed sibling must not stall healthy start/stop.
                    if !self.retry_is_due(&d.name, a.consecutive_failures).await {
                        continue;
                    }
                } else {
                    self.clear_retry(&d.name).await;
                }
                let rev = d.revision.unwrap_or(1);
                let already = {
                    let g = self.running.lock().await;
                    g.get(&d.name).map(|j| j.revision == rev).unwrap_or(false)
                };
                if already {
                    continue;
                }
                if let Err(e) = self.start_named(&d.name, rev).await {
                    let name = d.name.clone();
                    let msg = e.message.clone();
                    let waiting = is_capacity_wait(&e);
                    let _ = self
                        .catalog({
                            let name = name.clone();
                            move |s| {
                                let previous = s.actual(&name)?;
                                let still_waiting = waiting
                                    && previous.status == "waiting"
                                    && previous.revision == Some(rev);
                                let attempt = previous
                                    .attempt_id
                                    .saturating_add(u64::from(!still_waiting));
                                if waiting {
                                    s.set_waiting_failure(&name, rev, attempt, &msg)?;
                                    if !still_waiting {
                                        s.insert_attempt(&name, rev, "waiting", Some(&msg))?;
                                    }
                                } else {
                                    s.set_actual(&name, "failed", Some(rev), attempt, Some(&msg))?;
                                    s.insert_attempt(&name, rev, "failed", Some(&msg))?;
                                }
                                Ok(())
                            }
                        })
                        .await;
                    self.schedule_retry(&name).await;
                } else {
                    self.clear_retry(&d.name).await;
                }
            } else {
                self.clear_retry(&d.name).await;
                let job = self.running.lock().await.remove(&d.name);
                if let Some(job) = job {
                    self.stop_job(job).await;
                }
                let name = d.name.clone();
                let revision = d.revision;
                let _ = self
                    .catalog(move |s| {
                        if s.actual(&name)?.status == "stopped" {
                            return Ok(());
                        }
                        let attempt = s
                            .actual(&name)
                            .map(|a| a.attempt_id.saturating_add(1))
                            .unwrap_or(1);
                        s.set_actual(&name, "stopped", revision, attempt, None)
                    })
                    .await;
            }
        }
        Ok(())
    }

    async fn reap_finished(&self) {
        let names: Vec<String> = {
            let g = self.running.lock().await;
            g.iter()
                .filter(|(_, j)| j.is_finished())
                .map(|(n, _)| n.clone())
                .collect()
        };
        for name in names {
            let Some(job) = self.running.lock().await.remove(&name) else {
                continue;
            };
            let rev = job.revision;
            let outcome = self.join_job(job).await;
            match outcome {
                Ok(()) => {
                    let n = name.clone();
                    let _ = self
                        .catalog(move |s| {
                            let attempt = s
                                .actual(&n)
                                .map(|a| a.attempt_id.saturating_add(1))
                                .unwrap_or(1);
                            s.set_actual(&n, "completed", Some(rev), attempt, None)?;
                            s.insert_attempt(&n, rev, "completed", None)
                        })
                        .await;
                }
                Err(e) => {
                    let n = name.clone();
                    let msg = e.message.clone();
                    let _ = self
                        .catalog({
                            let n = n.clone();
                            move |s| {
                                let attempt = s
                                    .actual(&n)
                                    .map(|a| a.attempt_id.saturating_add(1))
                                    .unwrap_or(1);
                                s.set_actual(&n, "failed", Some(rev), attempt, Some(&msg))?;
                                s.insert_attempt(&n, rev, "failed", Some(&msg))
                            }
                        })
                        .await;
                    self.schedule_retry(&n).await;
                }
            }
        }
    }

    /// No detached refresh loop or database I/O under the running registry.
    /// The transition worker owns this bounded operation through completion.
    async fn refresh_live_lookups(&self)->Result<()> {
        let updates={
            let jobs=self.running.lock().await;
            jobs.values().filter(|job|!job.is_finished()).filter_map(|job| {
                let tables=job.handle().live_reference_tables();
                (!tables.is_empty()).then(||(tables,job.handle().memory_owner(),job.handle().cancellation(),job.diag.clone()))
            }).collect::<Vec<_>>()
        };
        if updates.is_empty(){return Ok(());}
        let store=self.store.clone();
        self.store.run_blocking(move || {
            for (tables,owner,cancel,diag) in updates {
                for (name,live) in tables {
                    if cancel.is_cancelled(){break;}
                    let current=live.current();
                    let update:Result<()>=(|| {
                        if let Some(next)=store.reference_update_after(&name,current.version)? {
                            let next=crate::lookup::prepare_update(next,&current,&owner)?;
                            if !cancel.is_cancelled(){live.publish(next)?;}
                        }
                        Ok(())
                    })();
                    if let Err(error)=update {
                        diag.lookup_update_failed.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                        diag.observation.health(false,HealthState::Failed,"lookup_refresh_failed",Some(error.code));
                        live.fail(error);cancel.cancel();break;
                    }
                }
            }
            Ok(())
        }).await
    }

    pub fn lookup_snapshot(&self,name:&str)->Result<serde_json::Value> {
        let jobs=self.running.try_lock().map_err(|_|SparrowError::new(sparrow_model::ErrorCode::ResourceExhausted,"runtime lookup registry busy"))?;
        let Some(job)=jobs.get(name) else{return Ok(serde_json::json!({"available":false}));};
        let tables=job.handle().live_reference_tables().into_iter().map(|(name,live)| {
            let table=live.current();
            let digest=table.canonical_sha256().map(|d|d.iter().map(|b|format!("{b:02x}")).collect::<String>());
            (name,serde_json::json!({"observed_revision":table.version,"sha256":digest,"failed":live.snapshot().is_err()}))
        }).collect::<std::collections::BTreeMap<_,_>>();
        let external=job.handle().external_lookup_diagnostics().into_iter().map(|(name,diag)|(name,diag.snapshot())).collect::<std::collections::BTreeMap<_,_>>();
        Ok(serde_json::json!({"available":true,"attempt":job.handle().attempt.raw(),"live_tables":tables,"external":external,"replay":"unsupported"}))
    }

    async fn retry_is_due(&self, name: &str, consecutive_failures: u64) -> bool {
        let mut map = self.next_retry_at.lock().await;
        if consecutive_failures == 0 && !map.contains_key(name) {
            return true;
        }
        let due = *map
            .entry(name.to_string())
            .or_insert_with(|| Instant::now() + retry_backoff(consecutive_failures));
        Instant::now() >= due
    }

    async fn schedule_retry(&self, name: &str) {
        let actual = self
            .catalog({
                let name = name.to_string();
                move |s| s.actual(&name)
            })
            .await;
        let delay = if actual.as_ref().is_ok_and(|a| a.status == "waiting") {
            let mut retries = self.capacity_retries.lock().await;
            let n = retries.entry(name.to_string()).or_default();
            *n = n.saturating_add(1);
            capacity_backoff(*n)
        } else {
            self.capacity_retries.lock().await.remove(name);
            retry_backoff(actual.map(|a| a.consecutive_failures).unwrap_or(1).max(1))
        };
        let due = Instant::now() + delay;
        self.next_retry_at
            .lock()
            .await
            .insert(name.to_string(), due);
    }

    pub(crate) async fn clear_retry(&self, name: &str) {
        self.next_retry_at.lock().await.remove(name);
        self.capacity_retries.lock().await.remove(name);
    }

    /// None means held. A read/write error is not permission to start the job.
    async fn actual_if_start_allowed(&self, name: &str) -> Result<Option<ActualState>> {
        let name = name.to_string();
        let safe = self.safe_mode;
        self.catalog(move |s| {
            let a = s.actual(&name)?;
            if a.restart_blocked {
                let desired=s.desired(&name)?;
                let fixed=desired.revision.map(|r|s.get_pipeline_revision(&name,r))
                    .transpose()?.is_some_and(|r|r.spec.requires_explicit_restart());
                if fixed {
                    let msg = "held: fixed snapshot, external plugin or live Lookup requires explicit start after failure or process restart";
                    if !a.last_error.as_deref().unwrap_or("").starts_with("held:") {
                        s.set_last_error(&name, Some(&format!("{msg}; last: {}", a.last_error.as_deref().unwrap_or("unknown"))))?;
                    }
                    return Ok(None);
                }
            }
            if a.status != "running"
                && a.status != "waiting"
                && a.consecutive_failures >= MAX_PIPELINE_ATTEMPTS
            {
                let msg = format!(
                    "held: consecutive_failures {} reached cap {MAX_PIPELINE_ATTEMPTS}; last: {}",
                    a.consecutive_failures,
                    a.last_error.as_deref().unwrap_or("unknown")
                );
                if !a.last_error.as_deref().unwrap_or("").starts_with("held:") {
                    s.set_last_error(&name, Some(&msg))?;
                }
                return Ok(None);
            }
            if safe && a.restart_blocked {
                let msg = "held: safe-mode and last attempt failed";
                if !a.last_error.as_deref().unwrap_or("").starts_with("held:") {
                    let detail = format!(
                        "{msg}; last: {}",
                        a.last_error.as_deref().unwrap_or("unknown")
                    );
                    s.set_last_error(&name, Some(&detail))?;
                }
                return Ok(None);
            }
            Ok(Some(a))
        })
        .await
    }

    async fn start_named(&self, name: &str, revision: u64) -> Result<()> {
        let name_owned = name.to_string();
        let demo = self.demo_endpoints();
        let secrets = StoreSecrets::new(self.store.clone());
        let (spec, schema, plan, policy) = self
            .catalog({
                let name = name_owned.clone();
                let demo = demo.clone();
                move |s| {
                    let row = s.get_pipeline_revision(&name, revision)?;
                    let stream = s.get_stream(&row.spec.stream)?;
                    let schema = stream_to_schema(&stream)?;
                    let policy = store_policy(s, demo.as_ref())?;
                    let plan = bind_plan_with_store(s, &row.spec, &name, revision)?;
                    validate_aligned_plan(&row.spec, &plan)?;
                    validate_io_with_plan(
                        &row.spec,
                        &schema,
                        &plan,
                        &secrets,
                        &policy,
                        demo.as_ref(),
                    )?;
                    Ok((row.spec, schema, plan, policy))
                }
            })
            .await?;

        // Replacement is stop/join then start, never two writers/readers of
        // one source/checkpoint directory. Static validation above leaves a
        // healthy old revision untouched when the new spec is invalid.
        let old = self.running.lock().await.remove(name);
        if let Some(old) = old {
            self.stop_job(old).await;
        }

        let note = if RecoveryPolicy::parse(&spec.recovery)?.is_aligned() {
            "aligned; not exactly-once"
        } else {
            "restart_fresh"
        };
        let name_s = name.to_string();
        let note_s = note.to_string();
        self.catalog({
            let name = name_s.clone();
            let note = note_s.clone();
            move |s| {
                let previous = s.actual(&name)?;
                // Preserve the waiting state until admission succeeds. Otherwise
                // every capacity poll writes starting + waiting and defeats dedup.
                if previous.status == "waiting" && previous.revision == Some(revision) {
                    return Ok(());
                }
                let attempt = previous.attempt_id.saturating_add(1);
                s.set_actual(&name, "starting", Some(revision), attempt, None)?;
                s.insert_attempt(&name, revision, "starting", Some(&note))?;
                Ok(())
            }
        })
        .await?;
        let recovery = RecoveryPolicy::parse(&spec.recovery)?;

        let job = if plan.has_silence() && spec.source.kind == "mqtt" {
            self.start_live_silence(name, &spec, schema, plan, demo.as_ref(), &policy).await?
        } else if plan.has_silence() {
            self.start_observed_time(&spec,schema,plan,&policy).await?
        } else if spec.recovery=="aligned" && plan.edges.is_some() && (plan.has_processing_time_state() || plan.has_event_time_window()) {
            self.start_time_graph(&spec,plan,&policy).await?
        } else if plan.has_timed_iot() || (spec.recovery == "aligned" && plan.has_processing_time_state()) {
            self.start_paused_time(&spec,schema,plan,&policy).await?
        } else if spec.graph_io.is_some() {
            self.start_graph(&spec, plan, &policy).await?
        } else { match spec.source.kind.as_str() {
            #[cfg(feature="jetstream")]
            "jetstream" => self.start_jetstream(&spec,schema,plan,&policy).await?,
            "file" | "file_replay" | "replay" => {
                self.start_file(name, &spec, schema, plan, recovery, &policy)
                    .await?
            }
            kind => {
                self.start_live(name, &spec, schema, plan, kind, demo, &policy)
                    .await?
            }
        }};

        let old = {
            let mut g = self.running.lock().await;
            g.insert(name.to_string(), {
                let mut j = job;
                j.revision = revision;
                j
            })
        };
        if let Some(old) = old {
            self.stop_job(old).await;
        }
        let name_s = name.to_string();
        let note_s = note.to_string();
        self.catalog(move |s| {
            let previous = s.actual(&name_s)?;
            let attempt = previous
                .attempt_id
                .saturating_add(u64::from(previous.status == "waiting"));
            s.set_actual(&name_s, "running", Some(revision), attempt, None)?;
            s.insert_attempt(&name_s, revision, "running", Some(&note_s))
        })
        .await?;
        Ok(())
    }

    /// Load fixed catalog revisions before source activation and charge the
    /// detached table to the very same owner Kernel will use for this attempt.
    async fn attach_reference_tables(
        &self,
        spec: &crate::spec::PipelineSpec,
        request: JobRequest,
    ) -> Result<JobRequest> {
        if spec.reference_tables.is_empty() {
            return self.attach_lookup_bindings(spec,request);
        }
        let (admission, prepared) = self
            .prepare_reference_tables(spec, request.plan.pipeline)
            .await?;
        self.attach_lookup_bindings(spec,request
            .with_tables(prepared.tables)
            .with_source_admission(admission))
    }

    /// Live content is resolved before source activation, never in the hot
    /// expression evaluator. The initial immutable catalog pin stays intact.
    pub(super) fn attach_lookup_bindings(&self,spec:&crate::PipelineSpec,mut request:JobRequest)->Result<JobRequest> {
        let mut live=HashMap::new();
        for (name,binding) in &spec.reference_tables {
            if binding.follow_latest {
                let table=request.tables.remove(name).ok_or_else(||SparrowError::new(sparrow_model::ErrorCode::InvalidArgument,"live Lookup initial snapshot missing"))?;
                live.insert(name.clone(),sparrow_runtime::lookup::LiveReferenceTable::new(table)?);
            }
        }
        let mut external=HashMap::new();
        if !spec.external_lookups.is_empty() {
            let policy=store_policy(&self.store,self.demo_endpoints().as_ref())?;
            for (name,binding) in &spec.external_lookups {external.insert(name.clone(),binding.provider(name,&self.secrets,&policy)?);}
        }
        Ok(request.with_live_tables(live).with_external_lookups(external))
    }

    /// Resolve immutable catalog revisions and build verified, job-owned
    /// snapshots before any connector/source activation.  The blocking
    /// closure retains the lifecycle guard so a cancelled caller cannot free
    /// the source-admission slot while decode/snapshot work is still running.
    async fn prepare_reference_tables(
        &self,
        spec: &crate::spec::PipelineSpec,
        pipeline: sparrow_model::PipelineId,
    ) -> Result<(SourceAdmission, PreparedReferenceTables)> {
        let admission = self.kernel.prepare_source_admission(pipeline)?;
        self.prepare_reference_tables_with_admission(spec, admission)
            .await
    }

    /// Resolve references while retaining an already-acquired source
    /// admission.  Graph and JetStream startup acquire their admission before
    /// this helper because their source/bootstrap code also needs the same
    /// owner; acquiring a second token here would double-count one Job.
    async fn prepare_reference_tables_with_admission(
        &self,
        spec: &crate::spec::PipelineSpec,
        admission: SourceAdmission,
    ) -> Result<(SourceAdmission, PreparedReferenceTables)> {
        if spec.reference_tables.is_empty() {
            return Err(SparrowError::new(
                sparrow_model::ErrorCode::InvalidArgument,
                "reference table preparation requires at least one binding",
            ));
        }
        let owner = admission.owner();
        let lifecycle_guard = admission.lifecycle_guard();
        let store = self.store.clone();
        let spec = spec.clone();
        let (prepared, _lifecycle_guard) = self
            .store
            .run_blocking(move || {
                let revisions = store.reference_bindings(&spec)?;
                let mut tables = HashMap::with_capacity(revisions.len());
                let mut dependencies = Vec::with_capacity(revisions.len());
                for revision in revisions {
                    // Catalog JSON is independently bounded; reserve decoded
                    // scalar/vector scratch before materializing detached rows.
                    let scratch_bytes = usize::try_from(revision.payload_bytes)
                        .ok()
                        .and_then(|n| n.checked_mul(64))
                        .and_then(|n| n.checked_add(64 * 1024))
                        .ok_or_else(|| {
                            SparrowError::new(
                                sparrow_model::ErrorCode::BoundExceeded,
                                "reference table decode scratch overflow",
                            )
                        })?;
                    let _scratch = owner.acquire(
                        sparrow_model::CreditKind::Reservation,
                        scratch_bytes,
                    )?;
                    let canonical_sha256 =
                        crate::validate::decode_reference_sha256(&revision.sha256)?;
                    let table = sparrow_runtime::ReferenceTable::snapshot_owned_verified(
                        revision.name.clone(),
                        revision.revision,
                        revision.table.schema(&revision.name)?,
                        revision.table.keys.clone(),
                        revision.table.rows()?,
                        crate::reference_table::MAX_REFERENCE_TABLE_ROWS,
                        owner.budget().retention_bytes,
                        canonical_sha256,
                        &owner,
                    )?;
                    let dependency = table.verified_dependency()?;
                    tables.insert(revision.name, table);
                    dependencies.push(dependency);
                }
                crate::validate::validate_reference_dependencies(&spec, &dependencies)?;
                // Fail before I/O if the latest live revision changed schema,
                // is corrupt or cannot fit beside the current owned snapshot.
                for (name,binding) in &spec.reference_tables {
                    if binding.follow_latest {
                        if let Some(next)=store.reference_update_after(name,binding.revision)? {
                            let old=tables.get(name).expect("resolved initial table");
                            let next=crate::lookup::prepare_update(next,old,&owner)?;
                            tables.insert(name.clone(),next);
                        }
                    }
                }
                // Cancellation of the waiter does not stop a blocking worker.
                // Keep its admission slot through construction and through
                // the returned snapshots until the waiter takes the result.
                Ok((
                    PreparedReferenceTables {
                        tables,
                        dependencies,
                    },
                    lifecycle_guard,
                ))
            })
            .await?;
        Ok((admission, prepared))
    }

    async fn start_live(
        &self,
        name: &str,
        spec: &crate::spec::PipelineSpec,
        schema: sparrow_model::Schema,
        plan: PhysicalPlan,
        kind: &str,
        demo: Option<DemoEndpoints>,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<RunningJob> {
        let diag = IoDiagnostics::new();
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox = spec.sink.outbox_capacity.max(1);
        let (tx_out, rx_out) = observed::channel(outbox);
        diag.observe_sink(&tx_out);
        let capture = SharedCapture::disabled();
        let mut request =
            JobRequest::new(plan, Vec::new(), capture).with_observation(diag.observation.clone());
        let mut tx_plugin=None;
        let (tx_in, tx_budgeted) = if kind == "plugin" {
            let (tx,rx)=observed::channel(inbox);
            diag.observe_source(&tx);
            request=request.with_live_events(rx).with_live_out(tx_out);
            tx_plugin=Some(tx);
            (None,None)
        } else if matches!(
            kind,
            "mqtt" | "http_poll" | "nats" | "databus" | "websocket" | "tcp"
        ) {
            let (tx, rx) = observed::channel(inbox);
            diag.observe_source(&tx);
            request = request.with_budgeted_live_io(rx, tx_out);
            (None, Some(tx))
        } else {
            let (tx, rx) = observed::channel(inbox);
            diag.observe_source(&tx);
            request = request.with_live_io(rx, tx_out);
            (Some(tx), None)
        };
        let request = self.attach_reference_tables(spec, request).await?;
        let job = self.kernel.submit(request)?;
        let cancel = job.cancellation();
        let source_result: Result<JoinHandle<Result<()>>> = async {
            Ok(match kind {
                "plugin" => {
                    let binding=spec.source.plugin.as_ref().expect("validated plugin source");
                    let extension=crate::plugins::manager(&self.store)?.resolve_extension(binding,sparrow_expr::plugins::extension::Role::Source)?;
                    crate::plugin_io::source(self.kernel.handle(),extension,binding.config.clone(),schema,tx_plugin.expect("plugin ingress"),cancel.clone(),job.memory_owner(),diag.clone(),false,self.kernel.ingress_batch_rows())
                }
                "http_push" => {
                    let cfg = http_push_config(&spec.source, schema)?;
                    let push = HttpPushSource::bind(cfg, &self.secrets, policy, Arc::clone(&diag))
                        .await
                        .map_err(SparrowError::from)?;
                    let cancel_src = cancel.clone();
                    self.kernel.handle().spawn(async move {
                        push.run(tx_in.expect("HTTP ingress"), cancel_src).await;
                        Ok(())
                    })
                }
                "http_poll" => {
                    let cfg =
                        http_poll_config(&spec.source, schema, spec.effective_fail_on_decode())?;
                    cfg.check_inbox_budget(self.kernel.job_budget().queue_bytes)
                        .map_err(SparrowError::from)?;
                    cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)
                        .map_err(SparrowError::from)?;
                    let poll = HttpPollSource::bind(cfg, &self.secrets, policy, Arc::clone(&diag))
                        .map_err(SparrowError::from)?;
                    let cancel_job = cancel.clone();
                    let owner = job.memory_owner();
                    let max_row_bytes = self.kernel.ingress_row_limit();
                    self.kernel.handle().spawn(async move {
                        let r = poll
                            .run_budgeted(
                                tx_budgeted.expect("HTTP poll ingress"),
                                cancel_job.clone(),
                                owner,
                                max_row_bytes,
                            )
                            .await
                            .map_err(SparrowError::from);
                        if r.is_err() {
                            cancel_job.cancel();
                        }
                        r
                    })
                }
                #[cfg(feature = "websocket")]
                "websocket" => {
                    let cfg = crate::validate::websocket_source_config(
                        &spec.source,
                        schema,
                        spec.effective_fail_on_decode(),
                    )?;
                    cfg.check_inbox_budget(self.kernel.job_budget().queue_bytes)?;
                    cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                    let source = sparrow_connectors::WebSocketSource::bind(
                        cfg,
                        &self.secrets,
                        policy,
                        Arc::clone(&diag),
                    )?;
                    let cancel_job = cancel.clone();
                    let owner = job.memory_owner();
                    let max_row_bytes = self.kernel.ingress_row_limit();
                    self.kernel.handle().spawn(async move {
                        let r = source
                            .run_budgeted(
                                tx_budgeted.expect("WebSocket ingress"),
                                cancel_job.clone(),
                                owner,
                                max_row_bytes,
                            )
                            .await;
                        if r.is_err() {
                            cancel_job.cancel();
                        }
                        r
                    })
                }
                "tcp" => {
                    let cfg = crate::validate::tcp_source_config(
                        &spec.source,
                        schema,
                        spec.effective_fail_on_decode(),
                    )?;
                    cfg.check_inbox_budget(self.kernel.job_budget().queue_bytes)?;
                    cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                    let source =
                        sparrow_connectors::TcpSource::bind(cfg, policy, Arc::clone(&diag))?;
                    let cancel_job = cancel.clone();
                    let owner = job.memory_owner();
                    let max_row_bytes = self.kernel.ingress_row_limit();
                    self.kernel.handle().spawn(async move {
                        let r = source
                            .run_budgeted(
                                tx_budgeted.expect("TCP ingress"),
                                cancel_job.clone(),
                                owner,
                                max_row_bytes,
                            )
                            .await;
                        if r.is_err() {
                            cancel_job.cancel();
                        }
                        r
                    })
                }
                #[cfg(feature = "nats")]
                "nats" => {
                    let cfg = crate::validate::nats_source_config(
                        &spec.source,
                        schema,
                        spec.effective_fail_on_decode(),
                    )?;
                    cfg.check_inbox_budget(self.kernel.job_budget().queue_bytes)?;
                    cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                    let source = sparrow_connectors::NatsSource::bind(
                        cfg,
                        &self.secrets,
                        policy,
                        Arc::clone(&diag),
                    )?;
                    let cancel_job = cancel.clone();
                    let owner = job.memory_owner();
                    let max_row_bytes = self.kernel.ingress_row_limit();
                    self.kernel.handle().spawn(async move {
                        let r = source
                            .run_budgeted(
                                tx_budgeted.expect("NATS ingress"),
                                cancel_job.clone(),
                                owner,
                                max_row_bytes,
                            )
                            .await;
                        if r.is_err() {
                            cancel_job.cancel();
                        }
                        r
                    })
                }
                "databus" => {
                    let cfg = crate::validate::databus_source_config(
                        &spec.source,
                        schema,
                        spec.effective_fail_on_decode(),
                    )?;
                    cfg.check_inbox_budget(self.kernel.job_budget().queue_bytes)?;
                    cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                    let source = sparrow_connectors::DataBusSource::bind(
                        cfg,
                        self.databus.clone(),
                        Arc::clone(&diag),
                    )?;
                    let cancel_job = cancel.clone();
                    let owner = job.memory_owner();
                    let max_row_bytes = self.kernel.ingress_row_limit();
                    self.kernel.handle().spawn(async move {
                        let r = source
                            .run_budgeted(
                                tx_budgeted.expect("DataBus ingress"),
                                cancel_job.clone(),
                                owner,
                                max_row_bytes,
                            )
                            .await;
                        if r.is_err() {
                            cancel_job.cancel();
                        }
                        r
                    })
                }
                "mqtt" => {
                    let mut mqtt_cfg = mqtt_config(&spec.source, schema, demo.as_ref(), name)?;
                    mqtt_cfg.fail_on_decode = spec.effective_fail_on_decode();
                    mqtt_cfg
                        .check_inbox_budget(self.kernel.job_budget().queue_bytes)
                        .map_err(SparrowError::from)?;
                    let mqtt = MqttSource::bind(mqtt_cfg, &self.secrets, policy, Arc::clone(&diag))
                        .map_err(SparrowError::from)?;
                    let cancel_job = cancel.clone();
                    let owner = job.memory_owner();
                    let max_row_bytes = self.kernel.ingress_row_limit();
                    self.kernel.handle().spawn(async move {
                        let r = mqtt
                            .run_budgeted(
                                tx_budgeted.expect("MQTT ingress"),
                                cancel_job.clone(),
                                owner,
                                max_row_bytes,
                            )
                            .await
                            .map_err(SparrowError::from);
                        if r.is_err() {
                            cancel_job.cancel();
                        }
                        r
                    })
                }
                other => {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::FeatureUnavailable,
                        format!("source kind `{other}` is not wired on the live path"),
                    ));
                }
            })
        }
        .await;
        let source = match source_result {
            Ok(source) => source,
            Err(error) => {
                let _ = job.stop().await;
                return Err(error);
            }
        };
        let sink_result = self.spawn_sink(
            job.memory_owner(),
            spec,
            rx_out,
            cancel.clone(),
            Arc::clone(&diag),
            demo.as_ref(),
            policy,
            None,
        );
        let sink = match sink_result {
            Ok(sink) => sink,
            Err(error) => {
                cancel.cancel();
                let _ = job.stop().await;
                let _ = source.await;
                return Err(error);
            }
        };
        Ok(RunningJob {
            graph_ports: None,
            source_kind: match kind {"mqtt"=>"mqtt","plugin"=>"plugin","http_poll"=>"http_poll","nats"=>"nats","databus"=>"databus","websocket"=>"websocket","tcp"=>"tcp",_=>"http_push"},
            sink_kind: observed_sink_kind(spec),
            started_at: Instant::now(),
            stable: false,
            kind: RunningKind::Live {
                handle: job,
                source,
                sink,
            },
            revision: 0,
            diag,
        })
    }

    async fn start_file(
        &self,
        name: &str,
        spec: &crate::spec::PipelineSpec,
        schema: sparrow_model::Schema,
        plan: PhysicalPlan,
        recovery: RecoveryPolicy,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<RunningJob> {
        let path = spec.source.path.clone().ok_or_else(|| {
            SparrowError::new(
                sparrow_model::ErrorCode::InvalidArgument,
                "file source requires source.path",
            )
        })?;
        if recovery.is_aligned() {
            return self
                .start_file_aligned(name, spec, schema, plan, path, policy)
                .await;
        }
        let contract = crate::validate::resolve_file_contract(spec, recovery)?;
        let fail_on_decode = spec.effective_fail_on_decode();
        let mut cfg = FileReplayConfig::new(&path, schema.clone());
        cfg.contract = contract;
        cfg.fail_on_decode = fail_on_decode;
        cfg.format = spec.source.payload_format()?;
        let src = self
            .store
            .run_blocking(move || {
                FileReplaySource::open(&cfg).map_err(|e| SparrowError::new(e.code(), e.to_string()))
            })
            .await?;
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox = spec.sink.outbox_capacity.max(1);
        let diag = IoDiagnostics::new();
        let (tx_ev, rx_ev) = observed::channel(inbox);
        let (tx_out, rx_out) = observed::channel(outbox);
        diag.observe_source(&tx_ev);
        diag.observe_sink(&tx_out);
        let capture = SharedCapture::disabled();
        let request = self.attach_reference_tables(spec,
            JobRequest::new(plan, Vec::new(), capture)
                .with_observation(diag.observation.clone())
                .with_live_events(rx_ev)
                .with_live_out(tx_out),
        ).await?;
        let job = self.kernel.submit(request)?;
        let cancel = job.cancellation();
        let diag_src = Arc::clone(&diag);
        let source = self.kernel.handle().spawn({
            let cancel = cancel.clone();
            async move {
                let r = crate::file_source::run_file_source(
                    src,
                    contract,
                    tx_ev,
                    cancel.clone(),
                    diag_src,
                    None,
                    None,
                    fail_on_decode,
                )
                .await;
                if r.is_err() {
                    cancel.cancel();
                }
                r
            }
        });
       let sink_result = self.spawn_sink(
            job.memory_owner(),
           spec,
            rx_out,
            cancel.clone(),
            Arc::clone(&diag),
            self.demo_endpoints().as_ref(),
            policy,
            None,
        );
        let sink = match sink_result {
            Ok(sink) => sink,
            Err(error) => {
                cancel.cancel();
                let _ = job.stop().await;
                let _ = source.await;
                return Err(error);
            }
        };
        Ok(RunningJob {
            graph_ports: None,
            source_kind: "file",
            sink_kind: observed_sink_kind(spec),
            started_at: Instant::now(),
            stable: false,
            kind: RunningKind::Live {
                handle: job,
                source,
                sink,
            },
            revision: 0,
            diag,
        })
    }

    async fn start_file_aligned(
        &self,
        _name: &str,
        spec: &crate::spec::PipelineSpec,
        schema: sparrow_model::Schema,
        plan: PhysicalPlan,
        path: String,
        target_policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<RunningJob> {
        crate::validate::validate_aligned_plan(spec, &plan)?;
        // Reference-dependent aligned recovery must resolve and materialize
        // the exact table revisions before touching checkpoint history.  The
        // no-reference path intentionally retains the old v3 behavior.
        let prepared_references = if spec.reference_tables.is_empty() {
            None
        } else {
            Some(self.prepare_reference_tables(spec, plan.pipeline).await?)
        };
        let layout = Arc::new(if let Some((_, prepared)) = &prepared_references {
            crate::validate::checkpoint_plan_with_references(
                spec,
                &plan,
                &prepared.dependencies,
            )?
        } else {
            CheckpointPlan::from_physical(&plan)?
        });
        let checkpoint_revision = plan.revision.raw();
        let chk = spec
            .checkpoint_dir
            .clone()
            .unwrap_or_else(|| format!("{path}.sparrow-chk"));
        sparrow_connectors::policy::check_data_path(std::path::Path::new(&path))?;
        sparrow_connectors::policy::check_data_path(std::path::Path::new(&chk))?;
        let policy = spec.checkpoint.clone().unwrap_or_else(|| CheckpointSpec {
            timeout_ms: self.checkpoint_timeout.as_millis() as u64,
            ..CheckpointSpec::default()
        });
        let fail_on_decode = spec.effective_fail_on_decode();
        let mut cfg = FileReplayConfig::new(&path, schema.clone());
        cfg.recovery = RecoveryPolicy::Aligned;
        cfg.restore = spec.restore_claim()?;
        cfg.fail_on_decode = fail_on_decode;
        cfg.format = spec.source.payload_format()?;
        // Aligned growing files default to AppendOnly (N5): EOF polls, no
        // terminal MAX watermark. Finite fixtures set source.file_contract=sealed.
        let contract = crate::validate::resolve_file_contract(spec, RecoveryPolicy::Aligned)?;
        cfg.contract = contract;
        let selected = spec.restore.clone();
        let restore_layout = layout.clone();
        let restore_policy = policy.clone();
        let max_keys = self.kernel.job_budget().max_state_keys;
        let iot_profile = plan.has_iot();
        let reference_profile = prepared_references.is_some();
        let diag = IoDiagnostics::new();
        // A required durable target must be resolved before opening/advancing
        // source history. Reuse this admitted session for the running Sink.
        let mut prepared_sink = prepared_sink::PreparedSink::prepare(
            self, spec, &plan, target_policy, Arc::clone(&diag),
        ).await?;
        let sink_identity = prepared_sink.identity();
        let restore_sink = sink_identity.clone();
        let source_guard = prepared_sink.lifecycle_guard();
        let restore_guard = source_guard.clone();
        let profile_specific = reference_profile || layout.has_hysteresis() || sink_identity.is_some();
        // Fingerprinting, bounded snapshot reads and cursor verification are
        // cold filesystem work; never block a Tokio executor worker on them.
        let restored = self
            .store
            .run_blocking(move || {
                // Dropping the awaiting start future cannot release the Job
                // slot while this non-cancellable restore still holds credit.
                let _restore_guard = restore_guard;
                let mut store = if let Some(sink) = &restore_sink {
                    CheckpointStore::open_file_jetstream_sink_exclusive(
                        std::path::Path::new(&chk), max_keys, restore_policy.retention(),
                        &restore_layout, Arc::clone(sink),
                    )?
                } else if profile_specific {
                    CheckpointStore::open_for_plan_exclusive(
                        std::path::Path::new(&chk),
                        max_keys,
                        restore_policy.retention(),
                        restore_layout.as_ref(),
                        "file",
                    )?
                } else if iot_profile {
                    CheckpointStore::open_iot_exclusive(
                        std::path::Path::new(&chk),
                        max_keys,
                        restore_policy.retention(),
                    )?
                } else {
                    CheckpointStore::open_pipeline_exclusive(
                        std::path::Path::new(&chk),
                        max_keys,
                        restore_policy.retention(),
                    )?
                };
                let mut source = FileReplaySource::open(&cfg)
                    .map_err(|e| SparrowError::new(e.code(), e.to_string()))?;
                let inventory = store.inventory()?;
                let restore = matches!(
                    selected.as_ref().map(|r| r.kind.as_str()),
                    Some("checkpoint")
                ) || (restore_policy.resume_latest
                    && (inventory.current.is_some() || inventory.current_error.is_some()));
                if profile_specific && !restore
                    && (inventory.current.is_some() || inventory.current_error.is_some()
                        || !inventory.generations.is_empty())
                {
                    let message = if restore_sink.is_some() {
                        "required sink checkpoint history requires resume_latest or an explicit checkpoint restore; use a new directory for fresh replay"
                    } else if reference_profile {
                        "reference checkpoint history requires resume_latest or an explicit checkpoint restore; use a new directory for fresh replay"
                    } else {
                        "hysteresis checkpoint history requires resume_latest or an explicit checkpoint restore; use a new directory for fresh replay"
                    };
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::UnsupportedRestore,
                        message,
                    ));
                }
                if restore_policy.resume_latest && !restore && !inventory.generations.is_empty() {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::UnsupportedRestore,
                        "checkpoint history exists without CURRENT; refusing automatic fresh start",
                    ));
                }
                let (restore_freeze, restore_iot, ingested0, restored_from, state_generation, downstream_changed, saved_plan, saved_sink) = if restore {
                    let requested = selected
                        .as_ref()
                        .and_then(|s| s.snapshot_id.as_deref())
                        .filter(|s| !s.is_empty() && *s != "aligned");
                    let snap = if let Some(id) = requested {
                        store.recover_pipeline_id(id.parse().map_err(|_| {
                            SparrowError::new(
                                sparrow_model::ErrorCode::InvalidArgument,
                                "snapshot_id must be aligned or an integer",
                            )
                        })?)?
                    } else {
                        store.recover_pipeline_required()?
                    };
                    let saved_sink = if let Some(sink) = &restore_sink {
                        snap.check_compatible_with_sink(&restore_layout, sink.identity())?;
                        Some(sparrow_io::OwnedSinkIdentity::new(
                            snap.sink_identity.clone().expect("checked required sink identity"),
                            sink.owner(),
                        )?)
                    } else {
                        snap.check_compatible(&restore_layout)?;
                        None
                    };
                    source.seek(&snap.source)?;
                    if requested.is_some() {
                        store.pin_recovery_point(snap.checkpoint_id)?;
                    }
                    (
                        Some(snap.windows),
                        snap.iot,
                        snap.ingested_rows,
                        Some(snap.checkpoint_id),
                        snap.generation,
                        snap.plan.semantics != restore_layout.semantics,
                        Arc::new(snap.plan),
                        saved_sink,
                    )
                } else {
                    use ring::rand::{SecureRandom,SystemRandom};
                    let mut generation=[0u8;16];
                    SystemRandom::new().fill(&mut generation).map_err(|_|SparrowError::new(sparrow_model::ErrorCode::Internal,"state generation randomness unavailable"))?;
                    (None, Vec::new(), 0, None, generation, false, Arc::clone(&restore_layout), None)
                };
                // Durable before Kernel/source activation. Fresh/reset gets a new
                // random 128-bit identity; compatible recovery preserves its ID.
                store.activate_state_generation(state_generation)?;
                let inventory = store.inventory()?;
                Ok((
                    source,
                    store,
                    restore_freeze,
                    restore_iot,
                    ingested0,
                    restored_from,
                    inventory,
                    state_generation,
                    downstream_changed,
                    saved_plan,
                    saved_sink,
                ))
            })
            .await;
        let (source, store, restore_freeze, restore_iot, ingested0, restored_from, inventory, state_generation, downstream_changed, saved_plan, saved_sink) =
            match restored {
                Ok(restored) => restored,
                Err(error) => return Err(prepared_sink.cleanup_error(error).await),
            };
        let mut source = source;
        let next_chk = store.next_checkpoint_id();
        let ingested = Arc::new(std::sync::atomic::AtomicU64::new(ingested0));
        let next_id = Arc::new(std::sync::atomic::AtomicU64::new(next_chk));
        let store = Arc::new(std::sync::Mutex::new(store));
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox_n = spec.sink.outbox_capacity.max(1);
        let (tx_ev, rx_ev) = observed::channel(inbox);
        let (tx_out, rx_out) = observed::channel(outbox_n);
        let acks = AlignedAcks::default();
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<AlignedCmd>(1);
        let outbox = Arc::new(InflightCounter::new());
        diag.observe_source(&tx_ev);
        diag.observe_sink(&tx_out);
        let diag_src = Arc::clone(&diag);
        let metrics = Arc::clone(&self.kernel.metrics);
        let mut request = JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
            .with_observation(diag.observation.clone())
            .with_live_events(rx_ev)
            .with_live_out(tx_out)
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    // v27/v28 independently verify the saved full semantics at
                    // Kernel admission, rather than comparing live to itself.
                    plan: if sink_identity.is_some() { saved_plan } else { layout.clone() },
                    generation:state_generation,restore:restore_freeze,iot:restore_iot,
                    sink: sink_identity.as_ref().map(|live| match saved_sink {
                        Some(saved) => sparrow_runtime::SinkRestoreBinding::restored(Arc::clone(live), saved),
                        None => sparrow_runtime::SinkRestoreBinding::fresh(Arc::clone(live)),
                    }),
                }),
                acks: acks.clone(),
                outbox: Arc::clone(&outbox),
            });
        if let Some((admission, prepared)) = prepared_references {
            request = request
                .with_tables(prepared.tables)
                .with_source_admission(admission);
        }
        if let Some(admission) = prepared_sink.take_admission() {
            request = request.with_source_admission(admission);
        }
        let job = match self.kernel.submit(request) {
            Ok(job) => job,
            Err(error) => return Err(prepared_sink.cleanup_error(error).await),
        };
        let cancel = job.cancellation();
        let child = cancel.clone();
        let ingested_r = Arc::clone(&ingested);
        let next_r = Arc::clone(&next_id);
        let store_r = Arc::clone(&store);
        let layout_r = Arc::clone(&layout);
        let checkpoint_timeout = Duration::from_millis(policy.timeout_ms);
        let checkpoint_owner = job.memory_owner();
        let checkpoint = CheckpointControl::new(policy.clone(), job.attempt.raw(), inventory);
        checkpoint.restored_from(restored_from);
        checkpoint.state_generation(state_generation);
        checkpoint.downstream_changed(downstream_changed);
        let scheduler = policy.interval_ms.map(|_| {
            self.kernel.handle().spawn(periodic_checkpoints(
                cmd_tx.clone(),
                checkpoint.clone(),
                cancel.clone(),
            ))
        });
        let source_task = self.kernel.handle().spawn(async move {
            let _lifecycle=diag_src.observation.lifecycle(true);
            diag_src.observation.health(true,HealthState::Ready,"file_open",None);
            source.set_diagnostics(diag_src.clone());
            let mut terminal_sent = false;
            let mut next_file_poll = None;
            // Independent polling is important: publishing a file batch may
            // itself wait for mailbox capacity. A future only polled at the
            // top-level select could starve while that branch is awaiting.
            let checkpoint_cancel = child.child_token();
            let mut workers = tokio::task::JoinSet::new();
            let source_result: Result<()> = async {
            loop {
                if child.is_cancelled() {
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    _ = child.cancelled() => return Ok(()),
                    Some(joined) = workers.join_next(), if !workers.is_empty() => {
                        joined.map_err(|e| SparrowError::new(sparrow_model::ErrorCode::Internal, format!("checkpoint task: {e}")))?;
                    }
                    cmd = cmd_rx.recv() => {
                        let Some(AlignedCmd::Checkpoint { reply, mut admission }) = cmd else {
                            return Ok(());
                        };
                        if reply.is_closed() || tokio::time::Instant::now() >= admission.deadline { continue; }
                        admission.phase("aligning");
                        // Every attempt gets a fresh id, including encode/commit failures.
                        let id = next_r.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Cut is the source cursor when the barrier is injected.
                        // Refresh the identity at the consumed cut (including
                        // append after an empty open) before injecting the barrier.
                        let (returned,position)=crate::file_source::checkpoint_file_position(source).await?;
                        source=returned;
                        let cut_source=match position {
                            Ok(position)=>position,
                            Err(error)=>{
                                metrics.record_checkpoint_abort();admission.finish(&Err(error.clone()),None);
                                let _=reply.send(Err(error));continue;
                            }
                        };
                        let cut_ingested =
                            ingested_r.load(std::sync::atomic::Ordering::SeqCst);
                        // The timeout includes barrier injection. Dropping this future
                        // drops its request inbox, so late encoded ACKs release leases.
                        let inject = async {
                            let request = acks.begin_with_deadline(id, admission.deadline)?;
                            tx_ev.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                                checkpoint_id: id,
                            })).await.map_err(|_| SparrowError::new(
                                sparrow_model::ErrorCode::Cancelled, "aligned source ended before barrier"))?;
                            Ok::<_, SparrowError>(request)
                        };
                        let result = tokio::select! {
                            _ = child.cancelled() => {metrics.record_checkpoint_abort();return Ok(());},
                            result = tokio::time::timeout_at(admission.deadline, inject) =>
                                result.unwrap_or_else(|_| Err(SparrowError::new(
                                    sparrow_model::ErrorCode::ResourceExhausted, "aligned checkpoint timed out"))),
                        };
                        let request = match result {
                            Ok(a) => a,
                            Err(e) => {
                                // Abandon this id so late freeze/flush cannot
                                // satisfy the next checkpoint_named.
                                metrics.record_checkpoint_abort();
                                admission.finish(&Err(e.clone()), None);
                                let _ = reply.send(Err(e));
                                continue;
                            }
                        };
                        let layout = Arc::clone(&layout_r);
                        let store = Arc::clone(&store_r);
                        let owner = checkpoint_owner.clone();
                        let sink_identity = sink_identity.clone();
                        let checkpoint_guard = source_guard.clone();
                        let metrics = metrics.clone();
                        let checkpoint_cancel = checkpoint_cancel.clone();
                        // Cut and barrier publication above remain inline and
                        // ordered. Later rows may now flow, but publication still
                        // requires every frozen participant and the real Sink ACK.
                        workers.spawn(async move {
                        let aligned = tokio::select! {
                            _ = checkpoint_cancel.cancelled() => {metrics.record_checkpoint_abort();return;},
                            result = tokio::time::timeout_at(admission.deadline, request.wait_participants(checkpoint_timeout)) =>
                                result.unwrap_or_else(|_| Err(SparrowError::new(sparrow_model::ErrorCode::ResourceExhausted, "aligned checkpoint timed out"))),
                        };
                        let acks = match aligned {
                            Ok(acks) => acks,
                            Err(error) => {
                                metrics.record_checkpoint_abort();
                                admission.finish(&Err(error.clone()), None);
                                let _ = reply.send(Err(error));
                                return;
                            }
                        };
                        if checkpoint_cancel.is_cancelled() { metrics.record_checkpoint_abort();return; }
                        admission.phase("committing");
                        let committed = tokio::task::spawn_blocking(move || {
                            // Kernel/SDK shutdown may finish first. Retain
                            // admission until this durable write actually ends.
                            let _checkpoint_guard = checkpoint_guard;
                            let started = std::time::Instant::now();
                            let mut store = store.lock().expect("store");
                            let payload = if let Some(sink) = &sink_identity {
                                PipelineSnapshot::encode_frozen_with_sink(
                                    id, &cut_source, cut_ingested, checkpoint_revision, &layout,
                                    sink.identity(), acks, &owner, max_keys)?
                            } else {
                                PipelineSnapshot::encode_frozen(
                                    id, &cut_source, cut_ingested, checkpoint_revision, &layout, acks, &owner, max_keys)?
                            };
                            let payload_len = payload.bytes().len() as u64;
                            let id = store.commit_prepared(&payload)?;
                            let storage = store.inventory().ok();
                            Ok::<_, SparrowError>((id, payload_len, started.elapsed(), storage))
                        })
                        .await
                        .map_err(|e| {
                            SparrowError::new(
                                sparrow_model::ErrorCode::Internal,
                                format!("checkpoint worker: {e}"),
                            )
                        });
                        match committed {
                            Ok(Ok((cid, payload_len, duration, storage))) => {
                                metrics.record_checkpoint(duration, payload_len);
                                tracing::info!(
                                    checkpoint_id = cid,
                                    bytes = payload_len,
                                    duration_micros = duration.as_micros() as u64,
                                    "checkpoint_commit"
                                );
                                admission.finish(&Ok(cid), storage);
                                let _ = reply.send(Ok(cid));
                            }
                            Ok(Err(e)) => {
                                metrics.record_checkpoint_abort();
                                admission.finish(&Err(e.clone()), None);
                                let _ = reply.send(Err(e));
                            }
                            Err(e) => {
                                metrics.record_checkpoint_abort();
                                admission.finish(&Err(e.clone()), None);
                                let _ = reply.send(Err(e));
                            }
                        }
                        });
                    }
                    _ = crate::file_source::wait_for_file_poll(next_file_poll) => {
                        next_file_poll = None;
                        let started=std::time::Instant::now();
                        let result=crate::file_source::take_file_batch(source).await;
                        diag_src.observation.record(Latency::FileReadDecode,started.elapsed());
                        let (src, polls) = result.map_err(|e|{diag_src.observation.health(true,HealthState::Failed,"file_read_failed",Some(e.code));e})?;
                        let batch_ready=crate::file_source::observe_file_batch(&diag_src,&polls);
                        source = src;
                            match crate::file_source::apply_file_batch(
                                polls,
                                contract,
                                &tx_ev,
                                &diag_src,
                                &mut terminal_sent,
                                Some(&ingested_r),
                                fail_on_decode,
                                batch_ready,
                                &child,
                            )
                            .await?
                            {
                                crate::file_source::FileProgress::Done => return Ok(()),
                                crate::file_source::FileProgress::Continue => {}
                                crate::file_source::FileProgress::Wait => {
                                    next_file_poll = Some(tokio::time::Instant::now()
                                        + crate::file_source::FILE_EOF_POLL);
                                }
                            }
                    }
                }
            }
            }.await;
            if source_result.is_err() || child.is_cancelled() { checkpoint_cancel.cancel(); }
            // Never detach a blocking commit on EOF, read failure or stop. Its
            // admission permit, frozen leases and exclusive store live through
            // durable completion, including after an HTTP waiter times out.
            while let Some(joined) = workers.join_next().await {
                joined.map_err(|e| SparrowError::new(sparrow_model::ErrorCode::Internal, format!("checkpoint task: {e}")))?;
            }
            source_result
        });
       let mut rx_out = Some(rx_out);
       let sink_result = if let Some(sink) = prepared_sink.spawn(&self.kernel, &mut rx_out, cancel.clone(), Arc::clone(&outbox)) {
           Ok(sink)
       } else { self.spawn_sink(
            job.memory_owner(),
           spec,
            rx_out.take().expect("unprepared sink input"),
            cancel.clone(),
            Arc::clone(&diag),
            self.demo_endpoints().as_ref(),
            target_policy,
            Some(Arc::clone(&outbox)),
        ) };
        let sink = match sink_result {
            Ok(sink) => sink,
            Err(error) => {
                cancel.cancel();
                let _ = job.stop().await;
                let _ = source_task.await;
                if let Some(scheduler) = scheduler {
                    let _ = scheduler.await;
                }
                return Err(error);
            }
        };
        Ok(RunningJob {
            graph_ports: None,
            source_kind: "file",
            sink_kind: observed_sink_kind(spec),
            started_at: Instant::now(),
            stable: false,
            kind: RunningKind::Aligned {
                cancel,
                cmd: cmd_tx,
                handle: job,
                source: source_task,
                sink,
                scheduler,
                checkpoint,
            },
            revision: 0,
            diag,
        })
    }

    fn spawn_sink(
        &self,
        owner: Arc<sparrow_model::MemoryOwner>,
        spec: &crate::spec::PipelineSpec,
        rx_out: observed::Receiver<sparrow_model::RowBatch>,
        cancel: CancellationToken,
        diag: Arc<IoDiagnostics>,
        demo: Option<&DemoEndpoints>,
        policy: &sparrow_connectors::TargetPolicy,
        outbox: Option<Arc<InflightCounter>>,
    ) -> Result<JoinHandle<()>> {
        Ok(match spec.sink.kind.as_str() {
            "plugin" => {
                let binding=spec.sink.plugin.as_ref().expect("validated plugin sink");
                let extension=crate::plugins::manager(&self.store)?.resolve_extension(binding,sparrow_expr::plugins::extension::Role::Sink)?;
                crate::plugin_io::sink(self.kernel.handle(),extension,binding.config.clone(),rx_out,cancel,owner,diag,outbox)
            }
            "file" => {
                let config=crate::validate::file_sink_config(&spec.sink)?;
                config.validate().map_err(SparrowError::from)?;
                let sink=sparrow_connectors::file_sink::FileSink {config,diag,action:spec.sink.action.as_deref().cloned().unwrap_or_default()};
                self.kernel.handle().spawn(sink.run(rx_out,cancel,outbox))
            }
            "log" => {
                let log = LogSink::unbuffered(diag).with_action(spec.sink.action.clone());
                self.kernel.handle().spawn(log.run(rx_out, cancel, outbox))
            }
            #[cfg(feature = "websocket")]
            "websocket" => {
                let cfg = crate::validate::websocket_sink_config(&spec.sink)?;
                cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                let sink = sparrow_connectors::WebSocketSink::bind(
                    cfg,
                    &self.secrets,
                    policy,
                    owner,
                    diag,
                )?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            "tcp" => {
                let cfg = crate::validate::tcp_sink_config(&spec.sink)?;
                cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                let sink = sparrow_connectors::TcpSink::bind(cfg, policy, owner, diag)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            #[cfg(feature = "nats")]
            "nats" => {
                let cfg = crate::validate::nats_sink_config(&spec.sink)?;
                let sink =
                    sparrow_connectors::NatsSink::bind(cfg, &self.secrets, policy, owner, diag)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            #[cfg(feature = "jetstream")]
            "jetstream" => {
                let cfg = crate::validate::jetstream_sink_config(&spec.sink)?;
                cfg.check_reservation_budget(self.kernel.job_budget().reservation_bytes)?;
                let sink = sparrow_connectors::jetstream::JetStreamSink::bind(
                    cfg,
                    &self.secrets,
                    policy,
                    owner,
                    diag,
                )?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            "databus" => {
                let cfg = crate::validate::databus_sink_config(&spec.sink)?;
                let sink =
                    sparrow_connectors::DataBusSink::bind(cfg, self.databus.clone(), owner, diag)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            "mqtt" => {
                let cfg = mqtt_sink_config(&spec.sink, demo)?;
                let sink =
                    MqttSink::bind(cfg, &self.secrets, policy, diag).and_then(|sink|sink.with_action(spec.sink.action.clone())).map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            _ => {
                let http_cfg = http_config(&spec.sink, demo)?;
                let sink = HttpSink::bind(http_cfg, &self.secrets, policy, diag).and_then(|sink|sink.with_action(spec.sink.action.clone(),policy))
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
        })
    }

    async fn stop_job(&self, job: RunningJob) {
        match job.kind {
            RunningKind::Live {
                handle,
                source,
                sink,
            } => {
                let _ = handle.stop().await;
                let _ = source.await;
                let _ = sink.await;
            }
            RunningKind::Aligned {
                cancel,
                handle,
                source,
                sink,
                scheduler,
                ..
            } => {
                cancel.cancel();
                let _ = handle.stop().await;
                let _ = source.await;
                let _ = sink.await;
                if let Some(scheduler) = scheduler {
                    let _ = scheduler.await;
                }
            }
        }
    }

    async fn join_job(&self, job: RunningJob) -> Result<()> {
        let lookup_diag=job.diag.clone();
        let file_diag=(job.graph_ports.is_none() && job.sink_kind=="file").then(||job.diag.clone());
        let plugin_diags=job.graph_ports.as_ref().map_or_else(||vec![job.diag.clone()],|ports|ports.sources.values().chain(ports.sinks.values()).cloned().collect::<Vec<_>>());
        match job.kind {
            RunningKind::Live {
                handle,
                source,
                sink,
            } => {
                let r = handle.wait().await;
                let src = match source.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(SparrowError::new(
                        sparrow_model::ErrorCode::JobFailed,
                        format!("source task panicked: {e}"),
                    )),
                };
                if sink.await.is_err() {return Err(SparrowError::new(sparrow_model::ErrorCode::JobFailed,"sink actor panicked"));}
                if lookup_diag.lookup_update_failed.load(std::sync::atomic::Ordering::Relaxed)>0 {
                    return Err(SparrowError::new(sparrow_model::ErrorCode::JobFailed,"live Lookup refresh failed; no stale fallback or automatic restart"));
                }
                if plugin_diags.iter().any(|d|d.plugin_failed.load(std::sync::atomic::Ordering::Relaxed)>0) {
                    return Err(SparrowError::new(sparrow_model::ErrorCode::JobFailed,"external plugin failed; inspect connector health; no automatic replay"));
                }
                if file_diag.as_ref().is_some_and(|d|d.file_failed.load(std::sync::atomic::Ordering::Relaxed)>0) {
                    return Err(SparrowError::new(sparrow_model::ErrorCode::JobFailed,"File Sink failed; inspect sink health and file_failed; no automatic rollback or replay"));
                }
                sink_failed_closed(&plugin_diags)?;
                match (r, src) {
                    (_, Err(e)) => Err(e),
                    (Err(e), _) => Err(e),
                    (Ok(_), Ok(())) => Ok(()),
                }
            }
            RunningKind::Aligned {
                handle,
                source,
                sink,
                scheduler,
                cancel,
                ..
            } => {
                // A scheduler panic must not leave the source waiting forever
                // while reap holds the lifecycle transition gate.
                if scheduler.as_ref().is_some_and(|s| s.is_finished())
                    && !source.is_finished()
                    && !cancel.is_cancelled()
                {
                    cancel.cancel();
                }
                let r = handle.wait().await;
                let src = match source.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(SparrowError::new(
                        sparrow_model::ErrorCode::JobFailed,
                        format!("aligned source panicked: {e}"),
                    )),
                };
                let _ = sink.await;
                cancel.cancel();
                if let Some(scheduler) = scheduler {
                    scheduler.await.map_err(|e| {
                        SparrowError::new(
                            sparrow_model::ErrorCode::JobFailed,
                            format!("checkpoint scheduler: {e}"),
                        )
                    })?;
                }
                // An unconfirmed JetStream publish cancels the job; report it
                // instead of the consequential Cancelled.
                sink_failed_closed(&plugin_diags)?;
                // Source failures may cancel Kernel to unblock its bounded
                // inbox. Preserve that root cause instead of replacing poison,
                // retention or ACK errors with consequential Cancelled.
                match (r,src) {
                    (_,Err(e))=>Err(e),
                    (Err(e),_)=>Err(e),
                    (Ok(_),Ok(()))=>Ok(()),
                }
            }
        }
    }

    pub async fn checkpoint_named(&self, name: &str) -> Result<u64> {
        let (cmd, control, cancel) = {
            let g = self.running.lock().await;
            match g.get(name) {
                Some(RunningJob {
                    kind:
                        RunningKind::Aligned {
                            cmd,
                            checkpoint,
                            cancel,
                            ..
                        },
                    ..
                }) => (cmd.clone(), checkpoint.clone(), cancel.clone()),
                Some(_) => {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::FeatureUnavailable,
                        "checkpoint is only available for aligned File/replay or JetStream jobs",
                    ));
                }
                None => {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::InvalidArgument,
                        format!("pipeline `{name}` is not running"),
                    ));
                }
            }
        };
        request_checkpoint(&cmd, &control, &cancel, "manual").await
    }

    /// Exercise a real coordinator timeout without shortening the running
    /// attempt's policy (including its subsequent checkpoint requests).
    #[cfg(all(test, feature = "demo-io"))]
    pub(crate) async fn checkpoint_named_with_timeout_for_test(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<u64> {
        let (cmd, control, cancel) = {
            let jobs = self.running.lock().await;
            match jobs.get(name).map(|job| &job.kind) {
                Some(RunningKind::Aligned {
                    cmd,
                    checkpoint,
                    cancel,
                    ..
                }) => (cmd.clone(), checkpoint.clone(), cancel.clone()),
                _ => {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::InvalidArgument,
                        format!("pipeline `{name}` is not a running aligned job"),
                    ));
                }
            }
        };
        if cancel.is_cancelled() {
            return Err(SparrowError::new(
                sparrow_model::ErrorCode::Cancelled,
                "job stopping",
            ));
        }
        let mut admission = control.begin("manual")?;
        admission.deadline = tokio::time::Instant::now() + timeout;
        request_checkpoint_with_admission(&cmd, &control, &cancel, admission).await
    }

    pub async fn kill_named(self: &Arc<Self>, name: &str) -> Result<()> {
        let name = name.to_owned();
        self.transition_operation(false, move |sup| async move { sup.kill_inner(&name).await })
            .await
    }

    async fn kill_inner(&self, name: &str) -> Result<()> {
        let job = {
            let mut g = self.running.lock().await;
            g.remove(name)
        };
        if let Some(job) = job {
            self.stop_job(job).await;
            let n = name.to_string();
            let _ = self
                .catalog(move |s| {
                    let attempt = s
                        .actual(&n)
                        .map(|a| a.attempt_id.saturating_add(1))
                        .unwrap_or(1);
                    s.set_actual(&n, "stopped", None, attempt, Some("killed"))
                })
                .await;
        }
        Ok(())
    }

    pub async fn stop_all(self: &Arc<Self>) {
        if let Err(error) = self
            .transition_operation(true, |sup| async move {
                sup.stop_all_inner().await;
                Ok(())
            })
            .await
        {
            tracing::error!(code=error.code.as_str(),error=%error,"stop_all_failed");
        }
    }

    async fn stop_all_inner(&self) {
        let mut g = self.running.lock().await;
        let jobs: Vec<_> = g.drain().collect();
        drop(g);
        for (name, job) in jobs {
            self.stop_job(job).await;
            let n = name.clone();
            let _ = self
                .catalog(move |s| {
                    let attempt = s
                        .actual(&n)
                        .map(|a| a.attempt_id.saturating_add(1))
                        .unwrap_or(1);
                    s.set_actual(&n, "stopped", None, attempt, None)
                })
                .await;
        }
    }
}

/// Test / compact process: 2 async workers. Production catalog/checkpoint/file
/// I/O must use `Store::run_blocking` / `spawn_blocking` so these workers are
/// not blocked. See `docs/RUNTIME.md`.
pub fn compact_kernel() -> Result<Kernel> {
    Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 4,
    })
}

/// Host-sized kernel for sparrow-server. SQLite/checkpoint/file still run on
/// the blocking pool; these workers stay for mailbox/stage futures.
pub fn host_kernel() -> Result<Kernel> {
    let max_jobs = match std::env::var("SPARROW_MAX_JOBS") {
        Ok(value) => parse_max_jobs(&value)?,
        Err(std::env::VarError::NotPresent) => 16,
        Err(_) => {
            return Err(SparrowError::new(
                sparrow_model::ErrorCode::InvalidArgument,
                "SPARROW_MAX_JOBS must be an integer in 1..=256",
            ))
        }
    };
    host_kernel_with_max_jobs(max_jobs)
}

pub fn parse_max_jobs(value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .ok()
        .filter(|n| (1..=256).contains(n))
        .ok_or_else(|| {
            SparrowError::new(
                sparrow_model::ErrorCode::InvalidArgument,
                "max_jobs must be an integer in 1..=256 (--max-jobs / SPARROW_MAX_JOBS)",
            )
        })
}

/// Scale the three process memory caps by the configured number of Compact
/// jobs. These are admission/credit limits, not eagerly allocated memory.
pub fn host_kernel_with_max_jobs(max_jobs: usize) -> Result<Kernel> {
    if !(1..=256).contains(&max_jobs) {
        return Err(SparrowError::new(
            sparrow_model::ErrorCode::InvalidArgument,
            "max_jobs must be in 1..=256",
        ));
    }
    let job = ResourceBudget::compact();
    let budget = ResourceBudget {
        reservation_bytes: job.reservation_bytes * max_jobs,
        retention_bytes: job.retention_bytes * max_jobs,
        queue_bytes: job.queue_bytes * max_jobs,
        ..ResourceBudget::performance()
    };
    let n = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
        .clamp(4, 16);
    Kernel::new_with_job_budget(
        KernelOptions {
            budget,
            mailbox: MailboxConfig {
                max_items: 32,
                max_bytes: 256 * 1024,
            },
            worker_threads: n,
            rows_per_batch: 8,
        },
        job,
    )
}

#[cfg(test)]
pub(crate) fn layout_from_physical(plan: &PhysicalPlan) -> Result<PlanLayout> {
    PlanLayout::from_physical(plan)
}

/// Request start: write desired state and return immediately.
pub fn request_start(store: &Store, name: &str, actor: &str) -> Result<()> {
    request_start_at(store, name, actor, None)
}

pub fn request_start_at(
    store: &Store,
    name: &str,
    actor: &str,
    revision: Option<u64>,
) -> Result<()> {
    let row = store.get_pipeline(name)?;
    let rev = match revision {
        Some(r) => {
            let _ = store.get_pipeline_revision(name, r)?;
            r
        }
        None => row.latest_revision,
    };
    store.request_start_revision(name, rev)?;
    store.audit(
        actor,
        "start",
        Some(name),
        Some(&format!(
            "desired=running; revision={rev}; catalog committed before I/O"
        )),
        "accepted",
    )?;
    Ok(())
}

pub fn request_stop(store: &Store, name: &str, actor: &str) -> Result<()> {
    let _ = store.get_pipeline(name)?;
    store.set_desired(name, "stopped", None)?;
    store.audit(
        actor,
        "stop",
        Some(name),
        Some("desired=stopped"),
        "accepted",
    )?;
    Ok(())
}

#[cfg(test)]
mod r4_retry_tests {
    use super::*;

    #[test]
    fn r10_fixed_restore_failure_is_held_even_without_safe_mode() {
        let kernel = Arc::new(host_kernel_with_max_jobs(1).unwrap());
        kernel.block_on(async {
            let store = Arc::new(Store::open_memory().unwrap());
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let spec = crate::PipelineSpec::from_json(br#"{"stream":"sensors","sql":"SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)","source":{"kind":"file","path":"unused.ndjson"},"sink":{"kind":"log"},"recovery":"aligned","checkpoint_dir":"unused-checkpoints","restore":{"kind":"checkpoint","snapshot_id":"1"},"checkpoint":{"resume_latest":true,"interval_ms":100}}"#).unwrap();
            assert!(!spec.checkpoint_warnings().is_empty());
            store.put_pipeline_and_start("fixed", &spec, None).unwrap();
            assert!(sup.actual_if_start_allowed("fixed").await.unwrap().is_some());
            store.set_actual("fixed", "failed", Some(1), 1, Some("sink failed")).unwrap();
            for _ in 0..3 {
                assert!(sup.actual_if_start_allowed("fixed").await.unwrap().is_none());
            }
            assert_eq!(store.actual("fixed").unwrap().consecutive_failures, 1);
            assert!(store.actual("fixed").unwrap().last_error.unwrap().contains("fixed snapshot"));
            request_start(&store, "fixed", "test").unwrap();
            assert!(sup.actual_if_start_allowed("fixed").await.unwrap().is_some());
            assert_eq!(store.get_pipeline("fixed").unwrap().spec.fixed_snapshot_id(), Some(1));
            sup.shutdown().await;
        });
    }

    #[test]
    fn r5_only_explicit_capacity_admission_enters_waiting() {
        use sparrow_model::ErrorCode;
        let plain = SparrowError::new(ErrorCode::ResourceExhausted, "memory allocation failed");
        assert!(plain.retryable);
        assert!(!is_capacity_wait(&plain));
        assert!(!is_capacity_wait(
            &plain.clone().context("admission", "configuration")
        ));
        let capacity = plain.context("admission", "capacity");
        assert!(is_capacity_wait(&capacity));
        assert!(!is_capacity_wait(&capacity.retryable(false)));
        assert!(!is_capacity_wait(
            &SparrowError::new(ErrorCode::InvalidArgument, "bad plan")
                .context("admission", "capacity")
                .retryable(true)
        ));
    }

    #[test]
    fn r4_capacity_retry_grows_independently_of_crash_count_and_resets() {
        let kernel = Arc::new(host_kernel_with_max_jobs(1).unwrap());
        kernel.block_on(async {
            let store = Arc::new(Store::open_memory().unwrap());
            let spec = crate::PipelineSpec::from_json(br#"{"stream":"sensors","sql":"SELECT v FROM sensors","source":{"kind":"file","path":"unused.ndjson"},"sink":{"kind":"log"}}"#).unwrap();
            store.put_pipeline("wait", &spec, None).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            store.set_waiting_failure("wait", 1, 1, "capacity").unwrap();
            for (i, expected_ms) in [500, 1000, 2000, 4000, 5000, 5000].into_iter().enumerate() {
                sup.schedule_retry("wait").await;
                assert_eq!(capacity_backoff(i as u32 + 1), Duration::from_millis(expected_ms));
                assert_eq!(sup.capacity_retries.lock().await["wait"], i as u32 + 1);
                assert!(sup.next_retry_at.lock().await.contains_key("wait"));
                assert_eq!(store.actual("wait").unwrap().consecutive_failures, 0);
            }
            sup.clear_retry("wait").await;
            sup.schedule_retry("wait").await;
            assert_eq!(sup.capacity_retries.lock().await["wait"], 1);
            store.set_actual("wait", "failed", Some(1), 2, Some("crash")).unwrap();
            sup.schedule_retry("wait").await;
            assert!(!sup.capacity_retries.lock().await.contains_key("wait"));
            assert_eq!(store.actual("wait").unwrap().consecutive_failures, 1);
        });
    }
}

#[cfg(all(test, feature = "demo-io"))]
mod a6_tests {
    #[test]
    fn a6_demo_harness_type_is_exported() {
        fn assert_exported(_: Option<super::DemoHarness>) {}
        assert_exported(None);
    }
}

//! Converges catalog desired state to in-process actual jobs.
//! Catalog commit never waits for MQTT/HTTP connect.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sparrow_connectors::{
    FileReplayConfig, FileReplaySource, HttpPushSource, HttpSink, IoDiagnostics, LogSink, MqttSink,
    MqttSource, publish_qos0, sensor_json, EmbeddedBroker, HttpCapture,
};
use sparrow_io::{RecordSource, ReplayableSource};
use sparrow_model::{RecoveryPolicy, ResourceBudget, Result, SparrowError};
use sparrow_plan::{PhysicalPlan, PhysicalStage};
use sparrow_runtime::{
    AlignedSession, CheckpointStore, JobHandle, JobRequest, Kernel, KernelOptions, MailboxConfig,
    SharedCapture,
};
use tokio_util::sync::CancellationToken;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::store::Store;
use crate::validate::{
    bind_plan, binder_catalog, http_config, http_push_config, mqtt_config, mqtt_sink_config,
    store_policy, stream_to_schema, validate_io, DemoEndpoints, StoreSecrets,
};

pub struct DemoHarness {
    pub broker: EmbeddedBroker,
    pub http: HttpCapture,
}

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
    },
}

enum RunningKind {
    Live {
        handle: JobHandle,
        source: JoinHandle<()>,
        sink: JoinHandle<()>,
    },
    Aligned {
        cancel: CancellationToken,
        cmd: tokio::sync::mpsc::Sender<AlignedCmd>,
        task: JoinHandle<Result<()>>,
        sink: JoinHandle<()>,
    },
}

struct RunningJob {
    kind: RunningKind,
    revision: u64,
}

impl RunningJob {
    fn is_finished(&self) -> bool {
        match &self.kind {
            RunningKind::Live { handle, .. } => handle.is_finished(),
            RunningKind::Aligned { task, .. } => task.is_finished(),
        }
    }
}

pub struct Supervisor {
    store: Arc<Store>,
    kernel: Arc<Kernel>,
    running: Mutex<HashMap<String, RunningJob>>,
    wake: Notify,
    safe_mode: bool,
    demo: Option<Arc<DemoHarness>>,
    secrets: StoreSecrets,
}

impl Supervisor {
    pub fn new(
        store: Arc<Store>,
        kernel: Arc<Kernel>,
        safe_mode: bool,
        demo: Option<Arc<DemoHarness>>,
    ) -> Result<Arc<Self>> {
        store.reset_actual_after_process_restart()?;
        let secrets = StoreSecrets::new(Arc::clone(&store));
        Ok(Arc::new(Self {
            store,
            kernel,
            running: Mutex::new(HashMap::new()),
            wake: Notify::new(),
            safe_mode,
            demo,
            secrets,
        }))
    }

    pub fn demo(&self) -> Option<Arc<DemoHarness>> {
        self.demo.clone()
    }

    pub fn demo_endpoints(&self) -> Option<DemoEndpoints> {
        self.demo.as_ref().map(|d| d.endpoints())
    }

    pub fn wake(&self) {
        self.wake.notify_waiters();
    }

    pub fn kernel(&self) -> &Kernel {
        &self.kernel
    }

    pub async fn run_loop(self: Arc<Self>) {
        loop {
            let _ = self.converge_once().await;
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    }

    pub async fn converge_once(&self) -> Result<()> {
        self.reap_finished().await;
        let desired = self.store.list_desired()?;
        for d in desired {
            if d.status == "running" {
                if self.should_hold_failed(&d.name)? {
                    continue;
                }
                if let Ok(a) = self.store.actual(&d.name) {
                    if a.status == "completed" && a.revision == d.revision {
                        continue;
                    }
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
                    let _ = self.store.set_actual(
                        &d.name,
                        "failed",
                        Some(rev),
                        self.next_attempt(&d.name),
                        Some(&e.message),
                    );
                    let _ = self.store.insert_attempt(&d.name, rev, "failed", Some(&e.message));
                }
            } else if let Some(job) = self.running.lock().await.remove(&d.name) {
                self.stop_job(job).await;
                let _ = self.store.set_actual(&d.name, "stopped", d.revision, self.next_attempt(&d.name), None);
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
                    let _ = self.store.set_actual(
                        &name,
                        "completed",
                        Some(rev),
                        self.next_attempt(&name),
                        None,
                    );
                    let _ = self.store.insert_attempt(&name, rev, "completed", None);
                }
                Err(e) => {
                    let _ = self.store.set_actual(
                        &name,
                        "failed",
                        Some(rev),
                        self.next_attempt(&name),
                        Some(&e.message),
                    );
                    let _ = self.store.insert_attempt(&name, rev, "failed", Some(&e.message));
                }
            }
        }
    }

    fn should_hold_failed(&self, name: &str) -> Result<bool> {
        if !self.safe_mode {
            return Ok(false);
        }
        if let Some(last) = self.store.last_attempt(name)? {
            if last.outcome == "failed" {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn next_attempt(&self, name: &str) -> u64 {
        self.store
            .actual(name)
            .map(|a| a.attempt_id.saturating_add(1))
            .unwrap_or(1)
    }

    async fn start_named(&self, name: &str, revision: u64) -> Result<()> {
        let row = self.store.get_pipeline_revision(name, revision)?;
        let spec = row.spec;
        let stream = self.store.get_stream(&spec.stream)?;
        let schema = stream_to_schema(&stream)?;
        let catalog = binder_catalog(&self.store)?;
        let demo = self.demo_endpoints();
        let policy = store_policy(&self.store, demo.as_ref())?;
        validate_io(&spec, &schema, &self.secrets, &policy, demo.as_ref())?;
        let plan = bind_plan(&spec, &catalog, name, revision)?;

        let attempt = self.next_attempt(name);
        self.store.set_actual(name, "starting", Some(revision), attempt, None)?;
        let recovery = RecoveryPolicy::parse(&spec.recovery)?;
        let note = if recovery.is_aligned() {
            "aligned; not exactly-once"
        } else {
            "restart_fresh"
        };
        self.store.insert_attempt(name, revision, "starting", Some(note))?;

        let job = match spec.source.kind.as_str() {
            "file" | "file_replay" | "replay" => {
                self.start_file(name, &spec, schema, plan, recovery).await?
            }
            kind => self.start_live(name, &spec, schema, plan, kind, demo, &policy).await?,
        };

        if let Some(old) = self.running.lock().await.insert(name.to_string(), {
            let mut j = job;
            j.revision = revision;
            j
        }) {
            self.stop_job(old).await;
        }
        self.store.set_actual(name, "running", Some(revision), attempt, None)?;
        self.store.insert_attempt(name, revision, "running", Some(note))?;
        Ok(())
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
        let (tx_in, rx_in) = tokio::sync::mpsc::channel(inbox);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let capture = SharedCapture::disabled();
        let job = self.kernel.submit(
            JobRequest::new(plan, Vec::new(), capture).with_live_io(rx_in, tx_out),
        )?;
        let cancel = job.cancellation();
        let source = match kind {
            "http_push" => {
                let cfg = http_push_config(&spec.source, schema)?;
                let push = HttpPushSource::bind(cfg, &self.secrets, policy, Arc::clone(&diag))
                    .await
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(push.run(tx_in, cancel.clone()))
            }
            "mqtt" => {
                let mqtt_cfg = mqtt_config(&spec.source, schema, demo.as_ref(), name)?;
                let mqtt = MqttSource::bind(mqtt_cfg, &self.secrets, policy, Arc::clone(&diag))
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(mqtt.run(tx_in, cancel.clone()))
            }
            other => {
                return Err(SparrowError::new(
                    sparrow_model::ErrorCode::FeatureUnavailable,
                    format!("source kind `{other}` is not wired on the live path"),
                ));
            }
        };
        let sink = self.spawn_sink(spec, rx_out, cancel, diag, demo.as_ref(), policy)?;
        Ok(RunningJob {
            kind: RunningKind::Live {
                handle: job,
                source,
                sink,
            },
            revision: 0,
        })
    }

    async fn start_file(
        &self,
        name: &str,
        spec: &crate::spec::PipelineSpec,
        schema: sparrow_model::Schema,
        plan: PhysicalPlan,
        recovery: RecoveryPolicy,
    ) -> Result<RunningJob> {
        let path = spec.source.path.clone().ok_or_else(|| {
            SparrowError::new(
                sparrow_model::ErrorCode::InvalidArgument,
                "file source requires source.path",
            )
        })?;
        if recovery.is_aligned() {
            return self.start_file_aligned(name, spec, schema, plan, path).await;
        }
        let cfg = FileReplayConfig::new(&path, schema.clone());
        let mut src = FileReplaySource::open(&cfg).map_err(|e| {
            SparrowError::new(e.code(), e.to_string())
        })?;
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox = spec.sink.outbox_capacity.max(1);
        let (tx_in, rx_in) = tokio::sync::mpsc::channel(inbox);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let capture = SharedCapture::disabled();
        let job = self.kernel.submit(
            JobRequest::new(plan, Vec::new(), capture).with_live_io(rx_in, tx_out),
        )?;
        let cancel = job.cancellation();
        let source = self.kernel.handle().spawn({
            let cancel = cancel.clone();
            async move {
                loop {
                    if cancel.is_cancelled() {
                        break;
                    }
                    match src.next_frame() {
                        Ok(Some(frame)) => {
                            if let Ok(Some(row)) = src.decode_frame(&frame) {
                                if tx_in.send(row).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(40)).await;
                        }
                        Err(_) => break,
                    }
                }
            }
        });
        let diag = IoDiagnostics::new();
        let sink = self.spawn_sink(spec, rx_out, cancel, diag, self.demo_endpoints().as_ref(), &store_policy(&self.store, self.demo_endpoints().as_ref())?)?;
        Ok(RunningJob {
            kind: RunningKind::Live {
                handle: job,
                source,
                sink,
            },
            revision: 0,
        })
    }

    async fn start_file_aligned(
        &self,
        _name: &str,
        spec: &crate::spec::PipelineSpec,
        schema: sparrow_model::Schema,
        plan: PhysicalPlan,
        path: String,
    ) -> Result<RunningJob> {
        let (op, win, input) = window_from_plan(&plan)?;
        let chk = spec
            .checkpoint_dir
            .clone()
            .unwrap_or_else(|| format!("{path}.sparrow-chk"));
        let store = CheckpointStore::open(std::path::Path::new(&chk))?;
        let cfg = FileReplayConfig::new(&path, schema.clone());
        let mut cfg = cfg;
        cfg.recovery = RecoveryPolicy::Aligned;
        cfg.restore = spec.restore_claim()?;
        let mut source = FileReplaySource::open(&cfg).map_err(|e| {
            SparrowError::new(e.code(), e.to_string())
        })?;
        let metrics = Arc::clone(&self.kernel.metrics);
        let mut session = if matches!(
            spec.restore.as_ref().map(|r| r.kind.as_str()),
            Some("checkpoint")
        ) {
            AlignedSession::restore(store, win, input, op, ResourceBudget::compact(), &mut source)?
        } else {
            AlignedSession::open(
                store,
                win,
                input,
                op,
                ResourceBudget::compact(),
                source.position(),
            )?
        };
        session.metrics = metrics;
        let outbox = spec.sink.outbox_capacity.max(1);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<AlignedCmd>(4);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let task = self.kernel.handle().spawn(async move {
            let tx_out = tx_out;
            loop {
                if child.is_cancelled() {
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    _ = child.cancelled() => return Ok(()),
                    cmd = cmd_rx.recv() => {
                        if let Some(AlignedCmd::Checkpoint { reply }) = cmd {
                            // R11: wait until the sink has drained the outbox
                            // (closed-window outputs) before committing the cut.
                            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                            let flush_ok = loop {
                                if tx_out.capacity() >= tx_out.max_capacity() {
                                    break true;
                                }
                                if tokio::time::Instant::now() >= deadline {
                                    break false;
                                }
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            };
                            let r = if flush_ok {
                                session.checkpoint_barrier_after_flush(|| Ok(()))
                            } else {
                                Err(SparrowError::new(
                                    sparrow_model::ErrorCode::ResourceExhausted,
                                    "sink flush timeout before checkpoint commit",
                                ))
                            };
                            let _ = reply.send(r);
                        } else {
                            return Ok(());
                        }
                    }
                    _ = tokio::task::yield_now() => {
                        match source.next_frame() {
                            Ok(Some(frame)) => {
                                if let Ok(Some(row)) = source.decode_frame(&frame) {
                                    let pos = source.position();
                                    let emission = session.ingest_rows(&[row], 0, pos)?;
                                    let mut rows = emission.finals;
                                    if let Some(wm) = emission.pending_close {
                                        loop {
                                            let chunk = session.operator.take_closed_chunk(
                                                wm,
                                                8,
                                                64 * 1024,
                                            )?;
                                            if chunk.is_empty() {
                                                break;
                                            }
                                            rows.extend(chunk);
                                        }
                                        session.operator.advance_holdback(wm)?;
                                    }
                                    if !rows.is_empty() {
                                        if let Some(b) = session.operator.build_batch(rows)? {
                                            if tx_out.send(b).await.is_err() {
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                tokio::time::sleep(Duration::from_millis(40)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        });
        let diag = IoDiagnostics::new();
        let sink = self.spawn_sink(
            spec,
            rx_out,
            cancel.clone(),
            diag,
            self.demo_endpoints().as_ref(),
            &store_policy(&self.store, self.demo_endpoints().as_ref())?,
        )?;
        Ok(RunningJob {
            kind: RunningKind::Aligned {
                cancel,
                cmd: cmd_tx,
                task,
                sink,
            },
            revision: 0,
        })
    }

    fn spawn_sink(
        &self,
        spec: &crate::spec::PipelineSpec,
        rx_out: tokio::sync::mpsc::Receiver<sparrow_model::RowBatch>,
        cancel: CancellationToken,
        diag: Arc<IoDiagnostics>,
        demo: Option<&DemoEndpoints>,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<JoinHandle<()>> {
        Ok(match spec.sink.kind.as_str() {
            "log" => {
                let log = LogSink::new(diag, 64);
                self.kernel.handle().spawn(log.run(rx_out, cancel))
            }
            "mqtt" => {
                let cfg = mqtt_sink_config(&spec.sink, demo)?;
                let sink = MqttSink::bind(cfg, &self.secrets, policy, diag)
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel))
            }
            _ => {
                let http_cfg = http_config(&spec.sink, demo)?;
                let sink = HttpSink::bind(http_cfg, &self.secrets, policy, diag)
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel))
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
                task,
                sink,
                ..
            } => {
                cancel.cancel();
                let _ = task.await;
                let _ = sink.await;
            }
        }
    }

    async fn join_job(&self, job: RunningJob) -> Result<()> {
        match job.kind {
            RunningKind::Live {
                handle,
                source,
                sink,
            } => {
                let r = handle.wait().await;
                let _ = source.await;
                let _ = sink.await;
                r.map(|_| ())
            }
            RunningKind::Aligned { task, sink, .. } => {
                let r = task.await.map_err(|e| {
                    SparrowError::new(
                        sparrow_model::ErrorCode::JobFailed,
                        format!("aligned task panicked: {e}"),
                    )
                })?;
                let _ = sink.await;
                r
            }
        }
    }

    pub async fn checkpoint_named(&self, name: &str) -> Result<u64> {
        let cmd = {
            let g = self.running.lock().await;
            match g.get(name) {
                Some(RunningJob {
                    kind: RunningKind::Aligned { cmd, .. },
                    ..
                }) => cmd.clone(),
                Some(_) => {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::FeatureUnavailable,
                        "checkpoint is only available for aligned File/replay jobs",
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
        let (tx, rx) = tokio::sync::oneshot::channel();
        cmd.send(AlignedCmd::Checkpoint { reply: tx })
            .await
            .map_err(|_| {
                SparrowError::new(sparrow_model::ErrorCode::Cancelled, "aligned job ended")
            })?;
        rx.await.map_err(|_| {
            SparrowError::new(sparrow_model::ErrorCode::Cancelled, "checkpoint reply dropped")
        })?
    }

    pub async fn kill_named(&self, name: &str) -> Result<()> {
        if let Some(job) = self.running.lock().await.remove(name) {
            self.stop_job(job).await;
            let _ = self.store.set_actual(name, "stopped", None, self.next_attempt(name), Some("killed"));
        }
        Ok(())
    }

    pub async fn stop_all(&self) {
        let mut g = self.running.lock().await;
        let jobs: Vec<_> = g.drain().collect();
        drop(g);
        for (name, job) in jobs {
            self.stop_job(job).await;
            let _ = self.store.set_actual(&name, "stopped", None, self.next_attempt(&name), None);
        }
    }
}

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

fn window_from_plan(
    plan: &PhysicalPlan,
) -> Result<(
    sparrow_model::OperatorId,
    sparrow_plan::WindowSpec,
    sparrow_model::Schema,
)> {
    for s in &plan.stages {
        if let PhysicalStage::WindowAgg {
            operator,
            spec,
            input,
            ..
        } = s
        {
            return Ok((*operator, spec.clone(), input.clone()));
        }
    }
    Err(SparrowError::new(
        sparrow_model::ErrorCode::FeatureUnavailable,
        "aligned recovery requires a window operator in the plan",
    ))
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
    store.set_desired(name, "running", Some(rev))?;
    let attempt = store.actual(name).map(|a| a.attempt_id).unwrap_or(0);
    store.set_actual(name, "stopped", None, attempt, None)?;
    store.audit(
        actor,
        "start",
        Some(name),
        Some(&format!("desired=running; revision={rev}; catalog committed before I/O")),
        "accepted",
    )?;
    Ok(())
}

pub fn request_stop(store: &Store, name: &str, actor: &str) -> Result<()> {
    let _ = store.get_pipeline(name)?;
    store.set_desired(name, "stopped", None)?;
    store.audit(actor, "stop", Some(name), Some("desired=stopped"), "accepted")?;
    Ok(())
}

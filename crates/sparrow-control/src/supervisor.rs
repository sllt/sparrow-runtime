//! Converges catalog desired state to in-process actual jobs.
//! Catalog commit never waits for MQTT/HTTP connect.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sparrow_connectors::{
    FileReplayConfig, FileReplaySource, HttpPushSource, HttpSink, IoDiagnostics, LogSink, MqttSink,
    MqttSource,
};
#[cfg(feature = "demo-io")]
use sparrow_connectors::{publish_qos0, sensor_json, EmbeddedBroker, HttpCapture};
use sparrow_io::ReplayableSource;
use sparrow_model::{
    InflightCounter, RecoveryPolicy, ResourceBudget, Result, SparrowError, StateSlotId,
};
use sparrow_plan::{PhysicalPlan, PhysicalStage, PlanLayout};
use sparrow_runtime::{
    AlignedAcks, AlignedJob, CheckpointSnapshot, CheckpointStore, IngressEvent,
    JobHandle, JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture, StreamControl,
};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::store::{ActualState, Store};
use crate::validate::{
    bind_plan, binder_catalog, http_config, http_push_config, mqtt_config, mqtt_sink_config,
    store_policy, stream_to_schema, validate_aligned_plan, validate_io, DemoEndpoints,
    StoreSecrets,
};

/// Cap on **consecutive** start failures. Lifetime `attempt_id` still
/// increments on start/stop/running/failed; it must not trigger this hold.
pub(crate) const MAX_PIPELINE_ATTEMPTS: u64 = 16;

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
    error.retryable && error.code == sparrow_model::ErrorCode::ResourceExhausted
        && error.context.iter().any(|(key, value)| key == "admission" && value == "capacity")
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
    },
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
    },
}

const STABLE_RUN: Duration = Duration::from_secs(30);

struct RunningJob {
    started_at: Instant,
    stable: bool,
    kind: RunningKind,
    revision: u64,
    diag: Arc<IoDiagnostics>,
}

impl RunningJob {
    fn is_finished(&self) -> bool {
        match &self.kind {
            RunningKind::Live { handle, .. } => handle.is_finished(),
            RunningKind::Aligned { handle, source, .. } => {
                handle.is_finished() || source.is_finished()
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
    /// When a failed pipeline may be retried. Checked with `continue`;
    /// the shared converge loop must not sleep on this map.
    next_retry_at: Mutex<HashMap<String, Instant>>,
    capacity_retries: Mutex<HashMap<String, u32>>,
    wake: Notify,
    safe_mode: bool,
    #[cfg(feature = "demo-io")]
    demo: Option<Arc<DemoHarness>>,
    secrets: StoreSecrets,
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
            next_retry_at: Mutex::new(HashMap::new()),
            capacity_retries: Mutex::new(HashMap::new()),
            wake: Notify::new(),
            safe_mode,
            #[cfg(feature = "demo-io")]
            demo,
            secrets,
        }))
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
        self.wake.notify_waiters();
    }

    pub fn kernel(&self) -> &Kernel {
        &self.kernel
    }

    pub async fn run_loop(self: Arc<Self>) {
        loop {
            if let Err(error) = self.converge_once().await {
                tracing::error!(code = error.code.as_str(), error = %error, "supervisor_converge_failed");
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
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
            acc.add_assign(&j.diag.snapshot());
        }
        acc
    }

    pub async fn converge_once(&self) -> Result<()> {
        self.reap_finished().await;
        let stable_names: Vec<String> = {
            let mut jobs = self.running.lock().await;
            jobs.iter_mut().filter_map(|(name, job)| {
                if !job.stable && !job.is_finished() && job.started_at.elapsed() >= self.stable_run {
                    job.stable = true;
                    Some(name.clone())
                } else { None }
            }).collect()
        };
        for name in stable_names {
            self.catalog(move |s| s.reset_consecutive_failures(&name)).await?;
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
                    if !self
                        .retry_is_due(&d.name, a.consecutive_failures)
                        .await
                    {
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
                                let still_waiting = waiting && previous.status == "waiting"
                                    && previous.revision == Some(rev);
                                let attempt = previous.attempt_id.saturating_add(u64::from(!still_waiting));
                                if waiting {
                                    s.set_waiting_failure(&name, rev, attempt, &msg)?;
                                    if !still_waiting { s.insert_attempt(&name, rev, "waiting", Some(&msg))?; }
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
                if let Some(job) = job { self.stop_job(job).await; }
                let name = d.name.clone();
                let revision = d.revision;
                let _ = self
                    .catalog(move |s| {
                        if s.actual(&name)?.status == "stopped" { return Ok(()); }
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

    async fn retry_is_due(&self, name: &str, consecutive_failures: u64) -> bool {
        let mut map = self.next_retry_at.lock().await;
        if consecutive_failures == 0 && !map.contains_key(name) { return true; }
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
        self.next_retry_at.lock().await.insert(name.to_string(), due);
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
            if a.status != "running" && a.status != "waiting" && a.consecutive_failures >= MAX_PIPELINE_ATTEMPTS {
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
                    let detail = format!("{msg}; last: {}", a.last_error.as_deref().unwrap_or("unknown"));
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
        let (spec, schema, catalog, policy) = self
            .catalog({
                let name = name_owned.clone();
                let demo = demo.clone();
                move |s| {
                    let row = s.get_pipeline_revision(&name, revision)?;
                    let stream = s.get_stream(&row.spec.stream)?;
                    let schema = stream_to_schema(&stream)?;
                    let catalog = binder_catalog(s)?;
                    let policy = store_policy(s, demo.as_ref())?;
                    Ok((row.spec, schema, catalog, policy))
                }
            })
            .await?;
        validate_io(&spec, &schema, &self.secrets, &policy, demo.as_ref())?;
        let plan = bind_plan(&spec, &catalog, name, revision)?;
        validate_aligned_plan(&spec, &plan)?;

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

        let job = match spec.source.kind.as_str() {
            "file" | "file_replay" | "replay" => {
                self.start_file(name, &spec, schema, plan, recovery).await?
            }
            kind => {
                self.start_live(name, &spec, schema, plan, kind, demo, &policy)
                    .await?
            }
        };

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
            let attempt = previous.attempt_id.saturating_add(u64::from(previous.status == "waiting"));
            s.set_actual(&name_s, "running", Some(revision), attempt, None)?;
            s.insert_attempt(&name_s, revision, "running", Some(&note_s))
        })
        .await?;
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
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let capture = SharedCapture::disabled();
        let mut request = JobRequest::new(plan, Vec::new(), capture);
        let (tx_in, tx_budgeted) = if kind == "mqtt" {
            let (tx, rx) = tokio::sync::mpsc::channel(inbox);
            request = request.with_budgeted_live_io(rx, tx_out);
            (None, Some(tx))
        } else {
            let (tx, rx) = tokio::sync::mpsc::channel(inbox);
            request = request.with_live_io(rx, tx_out);
            (Some(tx), None)
        };
        let job = self.kernel.submit(request)?;
        let cancel = job.cancellation();
        let source = match kind {
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
                    let r = mqtt.run_budgeted(tx_budgeted.expect("MQTT ingress"), cancel_job.clone(), owner, max_row_bytes).await.map_err(SparrowError::from);
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
        };
        let sink = self.spawn_sink(
            spec,
            rx_out,
            cancel,
            Arc::clone(&diag),
            demo.as_ref(),
            policy,
            None,
        )?;
        Ok(RunningJob {
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
    ) -> Result<RunningJob> {
        let path = spec.source.path.clone().ok_or_else(|| {
            SparrowError::new(
                sparrow_model::ErrorCode::InvalidArgument,
                "file source requires source.path",
            )
        })?;
        if recovery.is_aligned() {
            return self
                .start_file_aligned(name, spec, schema, plan, path)
                .await;
        }
        let contract = crate::validate::resolve_file_contract(spec, recovery)?;
        let fail_on_decode = spec.effective_fail_on_decode();
        let mut cfg = FileReplayConfig::new(&path, schema.clone());
        cfg.contract = contract;
        cfg.fail_on_decode = fail_on_decode;
        let src =
            FileReplaySource::open(&cfg).map_err(|e| SparrowError::new(e.code(), e.to_string()))?;
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox = spec.sink.outbox_capacity.max(1);
        let (tx_ev, rx_ev) = tokio::sync::mpsc::channel(inbox);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let capture = SharedCapture::disabled();
        let job = self.kernel.submit(
            JobRequest::new(plan, Vec::new(), capture)
                .with_live_events(rx_ev)
                .with_live_out(tx_out),
        )?;
        let cancel = job.cancellation();
        let diag = IoDiagnostics::new();
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
        let sink = self.spawn_sink(
            spec,
            rx_out,
            cancel,
            Arc::clone(&diag),
            self.demo_endpoints().as_ref(),
            &store_policy(&self.store, self.demo_endpoints().as_ref())?,
            None,
        )?;
        Ok(RunningJob {
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
    ) -> Result<RunningJob> {
        crate::validate::validate_aligned_plan(spec, &plan)?;
        let layout = layout_from_physical(&plan)?;
        let chk = spec
            .checkpoint_dir
            .clone()
            .unwrap_or_else(|| format!("{path}.sparrow-chk"));
        sparrow_connectors::policy::check_data_path(std::path::Path::new(&path))?;
        sparrow_connectors::policy::check_data_path(std::path::Path::new(&chk))?;
        let store = CheckpointStore::open_with_max_state_keys(
            std::path::Path::new(&chk),
            self.kernel.job_budget().max_state_keys,
        )?;
        let fail_on_decode = spec.effective_fail_on_decode();
        let mut cfg = FileReplayConfig::new(&path, schema.clone());
        cfg.recovery = RecoveryPolicy::Aligned;
        cfg.restore = spec.restore_claim()?;
        cfg.fail_on_decode = fail_on_decode;
        // Aligned growing files default to AppendOnly (N5): EOF polls, no
        // terminal MAX watermark. Finite fixtures set source.file_contract=sealed.
        let contract = crate::validate::resolve_file_contract(spec, RecoveryPolicy::Aligned)?;
        cfg.contract = contract;
        let mut source =
            FileReplaySource::open(&cfg).map_err(|e| SparrowError::new(e.code(), e.to_string()))?;
        let restore = matches!(
            spec.restore.as_ref().map(|r| r.kind.as_str()),
            Some("checkpoint")
        );
        let (restore_freeze, ingested0, next_chk) = if restore {
            let snap = store.recover_required()?;
            snap.check_compatible(&layout)?;
            source.seek(&snap.source)?;
            (
                Some(snap.window.clone()),
                snap.ingested_rows,
                snap.checkpoint_id.saturating_add(1),
            )
        } else {
            (None, 0, 1)
        };
        let pos = Arc::new(std::sync::Mutex::new(source.position()));
        let ingested = Arc::new(std::sync::atomic::AtomicU64::new(ingested0));
        let next_id = Arc::new(std::sync::atomic::AtomicU64::new(next_chk));
        let store = Arc::new(std::sync::Mutex::new(store));
        let layout = Arc::new(layout);
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox_n = spec.sink.outbox_capacity.max(1);
        let (tx_ev, rx_ev) = tokio::sync::mpsc::channel(inbox);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox_n);
        let acks = AlignedAcks::default();
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<AlignedCmd>(4);
        let outbox = Arc::new(InflightCounter::new());
        let diag = IoDiagnostics::new();
        let diag_src = Arc::clone(&diag);
        let metrics = Arc::clone(&self.kernel.metrics);
        let job = self.kernel.submit(
            JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
                .with_live_events(rx_ev)
                .with_live_out(tx_out)
                .with_aligned(AlignedJob {
                    restore: restore_freeze,
                    acks: acks.clone(),
                    outbox: Arc::clone(&outbox),
                }),
        )?;
        let cancel = job.cancellation();
        let child = cancel.clone();
        let pos_r = Arc::clone(&pos);
        let ingested_r = Arc::clone(&ingested);
        let next_r = Arc::clone(&next_id);
        let store_r = Arc::clone(&store);
        let layout_r = Arc::clone(&layout);
        let checkpoint_timeout = self.checkpoint_timeout;
        let source_task = self.kernel.handle().spawn(async move {
            let mut terminal_sent = false;
            let mut next_file_poll = None;
            loop {
                if child.is_cancelled() {
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    _ = child.cancelled() => return Ok(()),
                    cmd = cmd_rx.recv() => {
                        let Some(AlignedCmd::Checkpoint { reply }) = cmd else {
                            return Ok(());
                        };
                        // Every attempt gets a fresh id, including encode/commit failures.
                        let id = next_r.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Cut is the source cursor when the barrier is injected.
                        // Do not later stitch a stale freeze onto a newer pos_r.
                        let cut_source = pos_r.lock().expect("pos").clone();
                        let cut_ingested =
                            ingested_r.load(std::sync::atomic::Ordering::SeqCst);
                        // The timeout includes barrier injection. Dropping this future
                        // drops its request inbox, so late encoded ACKs release leases.
                        let aligned = async {
                            let request = acks.begin(id)?;
                            tx_ev.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                                checkpoint_id: id,
                            })).await.map_err(|_| SparrowError::new(
                                sparrow_model::ErrorCode::Cancelled, "aligned source ended before barrier"))?;
                            request.wait(checkpoint_timeout).await
                        };
                        let result = tokio::select! {
                            _ = child.cancelled() => return Ok(()),
                            result = tokio::time::timeout(checkpoint_timeout, aligned) =>
                                result.unwrap_or_else(|_| Err(SparrowError::new(
                                    sparrow_model::ErrorCode::ResourceExhausted, "aligned checkpoint timed out"))),
                        };
                        let acks = match result {
                            Ok(a) => a,
                            Err(e) => {
                                // Abandon this id so late freeze/flush cannot
                                // satisfy the next checkpoint_named.
                                metrics.record_checkpoint_abort();
                                let _ = reply.send(Err(e));
                                continue;
                            }
                        };
                        let freeze = acks.freeze.expect("aligned freeze");
                        let layout = Arc::clone(&layout_r);
                        let store = Arc::clone(&store_r);
                        let committed = tokio::task::spawn_blocking(move || {
                            let started = std::time::Instant::now();
                            let mut store = store.lock().expect("store");
                            let payload = CheckpointSnapshot::encode_frozen(
                                id, &cut_source, cut_ingested, &layout, None, freeze)?;
                            let payload_len = payload.bytes.len() as u64;
                            let id = store.commit_encoded(id, &payload.bytes)?;
                            Ok((id, payload_len, started.elapsed()))
                        })
                        .await
                        .map_err(|e| {
                            SparrowError::new(
                                sparrow_model::ErrorCode::Internal,
                                format!("checkpoint worker: {e}"),
                            )
                        });
                        match committed {
                            Ok(Ok((cid, payload_len, duration))) => {
                                metrics.record_checkpoint(duration, payload_len);
                                tracing::info!(
                                    checkpoint_id = cid,
                                    bytes = payload_len,
                                    duration_micros = duration.as_micros() as u64,
                                    "checkpoint_commit"
                                );
                                let _ = reply.send(Ok(cid));
                            }
                            Ok(Err(e)) => {
                                metrics.record_checkpoint_abort();
                                let _ = reply.send(Err(e));
                            }
                            Err(e) => {
                                metrics.record_checkpoint_abort();
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    _ = crate::file_source::wait_for_file_poll(next_file_poll) => {
                        next_file_poll = None;
                        let (src, polls) = crate::file_source::take_file_batch(source).await?;
                        source = src;
                        *pos_r.lock().expect("pos") = source.position();
                        for poll in polls {
                            match crate::file_source::apply_file_poll(
                                poll,
                                contract,
                                &tx_ev,
                                &diag_src,
                                &mut terminal_sent,
                                Some(&ingested_r),
                                fail_on_decode,
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
            }
        });
        let sink = self.spawn_sink(
            spec,
            rx_out,
            cancel.clone(),
            Arc::clone(&diag),
            self.demo_endpoints().as_ref(),
            &store_policy(&self.store, self.demo_endpoints().as_ref())?,
            Some(Arc::clone(&outbox)),
        )?;
        Ok(RunningJob {
            started_at: Instant::now(),
            stable: false,
            kind: RunningKind::Aligned {
                cancel,
                cmd: cmd_tx,
                handle: job,
                source: source_task,
                sink,
            },
            revision: 0,
            diag,
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
        outbox: Option<Arc<InflightCounter>>,
    ) -> Result<JoinHandle<()>> {
        Ok(match spec.sink.kind.as_str() {
            "log" => {
                let log = LogSink::new(diag, 64);
                self.kernel.handle().spawn(log.run(rx_out, cancel, outbox))
            }
            "mqtt" => {
                let cfg = mqtt_sink_config(&spec.sink, demo)?;
                let sink =
                    MqttSink::bind(cfg, &self.secrets, policy, diag).map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel, outbox))
            }
            _ => {
                let http_cfg = http_config(&spec.sink, demo)?;
                let sink = HttpSink::bind(http_cfg, &self.secrets, policy, diag)
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
                ..
            } => {
                cancel.cancel();
                let _ = handle.stop().await;
                let _ = source.await;
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
                let src = match source.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(SparrowError::new(
                        sparrow_model::ErrorCode::JobFailed,
                        format!("source task panicked: {e}"),
                    )),
                };
                let _ = sink.await;
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
                ..
            } => {
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
                r.map(|_| ()).and(src)
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
            SparrowError::new(
                sparrow_model::ErrorCode::Cancelled,
                "checkpoint reply dropped",
            )
        })?
    }

    pub async fn kill_named(&self, name: &str) -> Result<()> {
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

    pub async fn stop_all(&self) {
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
        Err(_) => return Err(SparrowError::new(sparrow_model::ErrorCode::InvalidArgument,
            "SPARROW_MAX_JOBS must be an integer in 1..=256")),
    };
    host_kernel_with_max_jobs(max_jobs)
}

pub fn parse_max_jobs(value: &str) -> Result<usize> {
    value.parse::<usize>().ok().filter(|n| (1..=256).contains(n))
        .ok_or_else(|| SparrowError::new(sparrow_model::ErrorCode::InvalidArgument,
            "max_jobs must be an integer in 1..=256 (--max-jobs / SPARROW_MAX_JOBS)"))
}

/// Scale the three process memory caps by the configured number of Compact
/// jobs. These are admission/credit limits, not eagerly allocated memory.
pub fn host_kernel_with_max_jobs(max_jobs: usize) -> Result<Kernel> {
    if !(1..=256).contains(&max_jobs) {
        return Err(SparrowError::new(sparrow_model::ErrorCode::InvalidArgument,
            "max_jobs must be in 1..=256"));
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
    Kernel::new_with_job_budget(KernelOptions {
        budget,
        mailbox: MailboxConfig {
            max_items: 32,
            max_bytes: 256 * 1024,
        },
        worker_threads: n,
        rows_per_batch: 8,
    }, job)
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

pub(crate) fn layout_from_physical(plan: &PhysicalPlan) -> Result<PlanLayout> {
    let (operator, spec, _) = window_from_plan(plan)?;
    let pred = sparrow_plan::where_before_window_physical(plan);
    Ok(PlanLayout::from_window(operator, StateSlotId::new(1), &spec).with_where(pred))
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
    fn r5_only_explicit_capacity_admission_enters_waiting() {
        use sparrow_model::ErrorCode;
        let plain = SparrowError::new(ErrorCode::ResourceExhausted, "memory allocation failed");
        assert!(plain.retryable);
        assert!(!is_capacity_wait(&plain));
        assert!(!is_capacity_wait(&plain.clone().context("admission", "configuration")));
        let capacity = plain.context("admission", "capacity");
        assert!(is_capacity_wait(&capacity));
        assert!(!is_capacity_wait(&capacity.retryable(false)));
        assert!(!is_capacity_wait(&SparrowError::new(ErrorCode::InvalidArgument, "bad plan")
            .context("admission", "capacity").retryable(true)));
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

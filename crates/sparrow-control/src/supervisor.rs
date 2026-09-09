//! Converges catalog desired state to in-process actual jobs.
//! Catalog commit never waits for MQTT/HTTP connect.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sparrow_connectors::{
    HttpPushSource, HttpSink, IoDiagnostics, LogSink, MqttSink, MqttSource, publish_qos0,
    sensor_json, EmbeddedBroker, HttpCapture,
};
use sparrow_model::{ResourceBudget, Result, SparrowError};
use sparrow_runtime::{JobHandle, JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture};
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

struct RunningJob {
    handle: JobHandle,
    mqtt: JoinHandle<()>,
    sink: JoinHandle<()>,
    revision: u64,
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
        let desired = self.store.list_desired()?;
        for d in desired {
            if d.status == "running" {
                if self.should_hold_failed(&d.name)? {
                    continue;
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
        let row = self.store.get_pipeline(name)?;
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
        self.store.insert_attempt(name, revision, "starting", Some("restart_fresh"))?;

        let diag = IoDiagnostics::new();
        let inbox = spec.source.inbox_capacity.max(1);
        let outbox = spec.sink.outbox_capacity.max(1);
        let (tx_in, rx_in) = tokio::sync::mpsc::channel(inbox);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let capture = SharedCapture::new();
        let job = self.kernel.submit(
            JobRequest::new(plan, Vec::new(), capture).with_live_io(rx_in, tx_out),
        )?;
        let cancel = job.cancellation();
        let mqtt_task = match spec.source.kind.as_str() {
            "http_push" => {
                let cfg = http_push_config(&spec.source, schema)?;
                let push = HttpPushSource::bind(cfg, &self.secrets, &policy, Arc::clone(&diag))
                    .await
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(push.run(tx_in, cancel.clone()))
            }
            _ => {
                let mqtt_cfg = mqtt_config(&spec.source, schema, demo.as_ref())?;
                let mqtt = MqttSource::bind(mqtt_cfg, &self.secrets, &policy, Arc::clone(&diag))
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(mqtt.run(tx_in, cancel.clone()))
            }
        };
        let sink_task = match spec.sink.kind.as_str() {
            "log" => {
                let log = LogSink::new(diag, 64);
                self.kernel.handle().spawn(log.run(rx_out, cancel))
            }
            "mqtt" => {
                let cfg = mqtt_sink_config(&spec.sink, demo.as_ref())?;
                let sink = MqttSink::bind(cfg, &self.secrets, &policy, diag)
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel))
            }
            _ => {
                let http_cfg = http_config(&spec.sink, demo.as_ref())?;
                let sink = HttpSink::bind(http_cfg, &self.secrets, &policy, diag)
                    .map_err(SparrowError::from)?;
                self.kernel.handle().spawn(sink.run(rx_out, cancel))
            }
        };

        if let Some(old) = self.running.lock().await.insert(
            name.to_string(),
            RunningJob {
                handle: job,
                mqtt: mqtt_task,
                sink: sink_task,
                revision,
            },
        ) {
            self.stop_job(old).await;
        }
        self.store.set_actual(name, "running", Some(revision), attempt, None)?;
        self.store.insert_attempt(name, revision, "running", Some("live_best_effort; restart_fresh"))?;
        Ok(())
    }

    async fn stop_job(&self, job: RunningJob) {
        let _ = job.handle.stop().await;
        let _ = job.mqtt.await;
        let _ = job.sink.await;
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

/// Request start: write desired state and return immediately.
pub fn request_start(store: &Store, name: &str, actor: &str) -> Result<()> {
    let row = store.get_pipeline(name)?;
    store.set_desired(name, "running", Some(row.latest_revision))?;
    // Clear a previous failed actual so converge will retry.
    let attempt = store.actual(name).map(|a| a.attempt_id).unwrap_or(0);
    store.set_actual(name, "stopped", None, attempt, None)?;
    store.audit(actor, "start", Some(name), Some("desired=running; catalog committed before I/O"), "accepted")?;
    Ok(())
}

pub fn request_stop(store: &Store, name: &str, actor: &str) -> Result<()> {
    let _ = store.get_pipeline(name)?;
    store.set_desired(name, "stopped", None)?;
    store.audit(actor, "stop", Some(name), Some("desired=stopped"), "accepted")?;
    Ok(())
}

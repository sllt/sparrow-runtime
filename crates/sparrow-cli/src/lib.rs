//! Composition root for M2. MQTT/HTTP crates are dependencies of *this*
//! crate, not of `sparrow-runtime`.

use sparrow_connectors::{
    HttpSink, HttpSinkConfig, IoDiagnostics, MapSecretResolver, MqttSource, MqttSourceConfig,
    TargetPolicy,
};
#[cfg(feature = "demo-io")]
use sparrow_connectors::{sensor_json, EmbeddedBroker, HttpCapture};
use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::{
    DataType, Field, FieldId, PipelineId, ResourceBudget, Result, RevisionId, Scalar, Schema,
    SchemaId, SparrowError,
};
use sparrow_plan::{bind_linear, physicalize, PhysicalPlan, PlanOptions};
use sparrow_runtime::{JobHandle, JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture};
use sparrow_testkit::{sensor_fixture, sensor_schema};
use std::sync::Arc;
use std::time::Duration;

pub fn projected_sensor_schema() -> Result<Schema> {
    Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(4), "ts", DataType::TimestampMicrosUTC, false),
        ],
    )
}

pub fn hot_sensor_plan() -> Result<PhysicalPlan> {
    let in_schema = sensor_schema();
    let out_schema = projected_sensor_schema()?;
    let bound = bind_linear(
        PipelineId::new(2),
        RevisionId::new(1),
        "sensors".into(),
        in_schema,
        Some(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temperature".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
        }),
        Some((
            vec![
                Expr::Column {
                    name: "device_id".into(),
                },
                Expr::Column {
                    name: "temperature".into(),
                },
                Expr::Column { name: "ts".into() },
            ],
            out_schema,
        )),
        None,
        "capture".into(),
    )?;
    Ok(physicalize(&bound, &PlanOptions { fuse: true }))
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

#[cfg(feature = "demo-io")]
pub struct LiveLoop {
    pub broker: EmbeddedBroker,
    pub http: HttpCapture,
    pub job: JobHandle,
    pub mqtt_task: tokio::task::JoinHandle<()>,
    pub http_task: tokio::task::JoinHandle<()>,
    pub diag: Arc<IoDiagnostics>,
    pub capture: SharedCapture,
    pub policy: TargetPolicy,
}

#[cfg(feature = "demo-io")]
impl LiveLoop {
    pub fn start(kernel: &Kernel, inbox: usize, outbox: usize) -> Result<Self> {
        kernel.block_on(Self::start_async(kernel, inbox, outbox))
    }

    async fn start_async(kernel: &Kernel, inbox: usize, outbox: usize) -> Result<Self> {
        let broker = EmbeddedBroker::start()
            .await
            .map_err(SparrowError::from)?;
        let http = HttpCapture::start().await.map_err(SparrowError::from)?;
        let policy = TargetPolicy::allow(broker.host(), broker.port())
            .with_allow("127.0.0.1", http.port());
        let secrets = MapSecretResolver::empty();
        let diag = IoDiagnostics::new();

        let mut mqtt_cfg = MqttSourceConfig::demo(broker.host(), broker.port(), sensor_schema());
        mqtt_cfg.inbox_capacity = inbox;
        let mqtt = MqttSource::bind(mqtt_cfg, &secrets, &policy, Arc::clone(&diag))
            .map_err(SparrowError::from)?;

        let mut http_cfg = HttpSinkConfig::demo(http.url());
        http_cfg.outbox_capacity = outbox;
        let sink = HttpSink::bind(http_cfg, &secrets, &policy, Arc::clone(&diag))
            .map_err(SparrowError::from)?;

        let (tx_in, rx_in) = tokio::sync::mpsc::channel(inbox);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel(outbox);
        let capture = SharedCapture::new();
        let job = kernel.submit(
            JobRequest::new(hot_sensor_plan()?, Vec::new(), capture.clone())
                .with_live_io(rx_in, tx_out),
        )?;
        let cancel = job.cancellation();
        let mqtt_task = kernel.handle().spawn(mqtt.run(tx_in, cancel.clone()));
        let http_task = kernel.handle().spawn(sink.run(rx_out, cancel, None));
        // Give the subscriber time to CONNECT/SUBSCRIBE before publishes.
        tokio::time::sleep(Duration::from_millis(80)).await;
        Ok(Self {
            broker,
            http,
            job,
            mqtt_task,
            http_task,
            diag,
            capture,
            policy,
        })
    }

    pub fn publish_fixture(&self, kernel: &Kernel) -> Result<()> {
        kernel.block_on(self.publish_fixture_async())
    }

    async fn publish_fixture_async(&self) -> Result<()> {
        for (i, rec) in sensor_fixture().into_iter().enumerate() {
            sparrow_connectors::publish_qos0(
                &self.broker.host(),
                self.broker.port(),
                &format!("pub-{i}"),
                "sensors/json",
                sensor_json(
                    &rec.device_id,
                    rec.temperature,
                    rec.humidity,
                    rec.ts,
                    rec.alert,
                ),
            )
            .await
            .map_err(SparrowError::from)?;
        }
        Ok(())
    }

    pub fn publish_flood(&self, kernel: &Kernel, n: usize, temp: f64) -> Result<()> {
        kernel.block_on(self.publish_flood_async(n, temp))
    }

    async fn publish_flood_async(&self, n: usize, temp: f64) -> Result<()> {
        let payloads = (0..n)
            .map(|i| {
                sensor_json(
                    "edge-flood",
                    temp,
                    40.0,
                    1_700_000_100_000_000 + i as i64,
                    true,
                )
            })
            .collect();
        sparrow_connectors::publish_qos0_many(
            &self.broker.host(),
            self.broker.port(),
            "flood",
            "sensors/json",
            payloads,
        )
        .await
        .map_err(SparrowError::from)
    }

    pub fn wait_http_at_least(
        &self,
        kernel: &Kernel,
        n: usize,
        timeout: Duration,
    ) -> Result<Vec<String>> {
        kernel.block_on(async {
            let start = std::time::Instant::now();
            loop {
                let bodies = self.http.body_strings();
                if bodies.len() >= n {
                    return Ok(bodies);
                }
                if start.elapsed() > timeout {
                    return Err(SparrowError::new(
                        sparrow_model::ErrorCode::Internal,
                        format!(
                            "HTTP capture has {} bodies, expected >= {n} ({})",
                            bodies.len(),
                            self.diag.snapshot()
                        ),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    }

    pub fn stop(self, kernel: &Kernel) -> Result<()> {
        kernel.block_on(async {
            let stats = self.job.stop().await?;
            let _ = self.mqtt_task.await;
            let _ = self.http_task.await;
            self.broker.stop().await;
            self.http.stop().await;
            if stats.live_tasks_after != 0 || kernel.live_tasks() != 0 {
                return Err(SparrowError::new(
                    sparrow_model::ErrorCode::Internal,
                    format!(
                        "orphan kernel tasks after stop: job={} kernel={}",
                        stats.live_tasks_after,
                        kernel.live_tasks()
                    ),
                ));
            }
            Ok(())
        })
    }
}

pub fn rss_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

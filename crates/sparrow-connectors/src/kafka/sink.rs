//! Kafka producer Sink: one record per row to a static topic.
//!
//! - Default: idempotent producer (`enable.idempotence=true`, `acks=all`);
//!   librdkafka retries within `delivery_timeout` (`message.timeout.ms`)
//!   without duplicates or reordering per partition. With idempotence off,
//!   `acks` is `all` or `leader` and retries are disabled
//!   (`message.send.max.retries=0`) so a retry can never duplicate.
//! - A batch is acknowledged to the outbox only after the delivery report of
//!   every row came back successful. In flight (sent, report pending) is
//!   bounded by `max_in_flight` and librdkafka's queue by `queue_bytes`.
//! - A failed delivery report fails the Sink closed (health `Failed`;
//!   this and every later batch fails its receipt): with idempotence the
//!   partition order after a timed-out message is no longer guaranteed.
//! - Rows that cannot be encoded (or exceed `max_message_bytes`) are dropped
//!   and counted and the batch receipt fails, as for the other sinks.
//! - Stop: `flush_timeout` bounds the rest of the current batch, batches
//!   already queued and their delivery reports; on expiry librdkafka's queue
//!   and in-flight requests are purged. A purged in-flight request may still
//!   have been written by the broker.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer, PurgeConfig};
use rdkafka::ClientContext;
use sparrow_formats::PayloadFormat;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, RestoreClaim, Result, Row,
    RowBatch, Scalar,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::client::{check_advertised, millis, KafkaClientConfig};
use super::{check_name, error};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::TargetPolicy;

const MAX_OUTBOX: usize = 4096;
const CLIENT_OVERHEAD: usize = 256 * 1024;
const QUEUE_FULL_RETRY: Duration = Duration::from_millis(5);
const MAX_KEY_BYTES: usize = 1024;
const REPORT_OVERHEAD: usize = 1024;

/// The SDK owns the context until its final native client is destroyed,
/// including clones in abandoned metadata requests and background cleanup.
struct ChargedContext {
    _lease: Arc<MemoryLease>,
}
impl ClientContext for ChargedContext {}
type KafkaProducer = FutureProducer<ChargedContext>;
type PendingReport =
    BoxFuture<'static, (<DeliveryFuture as std::future::Future>::Output, MemoryLease)>;

/// Even task abort must not destroy a native client on a Tokio worker.
struct DeferredProducer(Option<KafkaProducer>);
impl std::ops::Deref for DeferredProducer {
    type Target = KafkaProducer;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("live producer")
    }
}
impl Drop for DeferredProducer {
    fn drop(&mut self) {
        if let Some(producer) = self.0.take() {
            tokio::task::spawn_blocking(move || drop(producer));
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaAcks {
    /// All in-sync replicas (`acks=all`).
    All,
    /// Partition leader only (`acks=1`); only with idempotence off.
    Leader,
}

#[derive(Clone, Debug)]
pub struct KafkaSinkConfig {
    pub client: KafkaClientConfig,
    pub topic: String,
    /// Optional record key column (Utf8 or Bytes; NULL produces no key).
    pub key_column: Option<String>,
    pub idempotence: bool,
    pub acks: KafkaAcks,
    /// Records sent and awaiting a delivery report (1..=1024, default 128).
    pub max_in_flight: usize,
    /// librdkafka producer queue bound (64 KiB..=16 MiB, default 1 MiB).
    pub queue_bytes: usize,
    /// librdkafka `linger.ms` (0..=1 s, default 5 ms).
    pub linger: Duration,
    /// librdkafka `message.timeout.ms`: bound on a record's delivery
    /// including retries (1..=300 s, default 30 s).
    pub delivery_timeout: Duration,
    /// Largest encoded value (1..=1 MiB, default 64 KiB, <= queue_bytes / 2).
    pub max_message_bytes: usize,
    /// Stop deadline for queued batches and outstanding reports.
    pub flush_timeout: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
    pub payload_format: PayloadFormat,
}

impl KafkaSinkConfig {
    pub fn new(brokers: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            client: KafkaClientConfig::new(brokers),
            topic: topic.into(),
            key_column: None,
            idempotence: true,
            acks: KafkaAcks::All,
            max_in_flight: 128,
            queue_bytes: 1024 * 1024,
            linger: Duration::from_millis(5),
            delivery_timeout: Duration::from_secs(30),
            max_message_bytes: 64 * 1024,
            flush_timeout: Duration::from_secs(5),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
            payload_format: PayloadFormat::Json,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::KAFKA_SINK
    }

    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        check_name("topic", &self.topic, 249)?;
        if self.idempotence && self.acks != KafkaAcks::All {
            return Err(error(
                ErrorCode::InvalidArgument,
                "Kafka idempotence requires acks=all",
            ));
        }
        if let Some(key) = &self.key_column {
            if key.is_empty() || key.len() > 256 {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "Kafka key_column must be 1..=256 bytes",
                ));
            }
        }
        let ms = Duration::from_millis;
        let s = Duration::from_secs;
        if !(1..=1024).contains(&self.max_in_flight)
            || !(64 * 1024..=16 * 1024 * 1024).contains(&self.queue_bytes)
            || self.linger > s(1)
            || !(s(1)..=s(300)).contains(&self.delivery_timeout)
            || !(1..=1024 * 1024).contains(&self.max_message_bytes)
            || self.max_message_bytes > self.queue_bytes / 2
            || !(ms(100)..=s(60)).contains(&self.flush_timeout)
            || !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "Kafka sink bounds: max_in_flight 1..=1024; queue_bytes 64KiB..=16MiB; linger <= 1s; delivery_timeout 1..=300s; max_message_bytes 1..=1MiB and <= queue_bytes/2; flush_timeout 100ms..=60s; outbox_capacity 1..=4096",
            ));
        }
        self.client.validate(policy)
    }

    /// librdkafka-held memory: the producer queue (records are copied into
    /// it), one encoded record per side of the copy and fixed buffers.
    pub fn reservation(&self) -> usize {
        self.queue_bytes
            .saturating_add(self.max_message_bytes.saturating_mul(2))
            .saturating_add(CLIENT_OVERHEAD)
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "Kafka sink buffers (queue + 2*message + client) exceed half the job reservation budget",
            ));
        }
        Ok(())
    }

    pub(crate) fn producer_config(&self) -> ClientConfig {
        let mut c = self.client.base();
        // Record overhead on top of the value (key, headers, framing).
        let message_max = (self.max_message_bytes + 1024).max(1000).to_string();
        c.set(
            "enable.idempotence",
            if self.idempotence { "true" } else { "false" },
        )
        .set(
            "acks",
            match self.acks {
                KafkaAcks::All => "all",
                KafkaAcks::Leader => "1",
            },
        )
        .set("message.timeout.ms", millis(self.delivery_timeout))
        .set("linger.ms", millis(self.linger))
        .set(
            "queue.buffering.max.kbytes",
            (self.queue_bytes / 1024).to_string(),
        )
        .set(
            "queue.buffering.max.messages",
            self.max_in_flight.to_string(),
        )
        .set("message.max.bytes", &message_max)
        .set("batch.size", &message_max)
        .set("compression.type", "none");
        if !self.idempotence {
            c.set("message.send.max.retries", "0");
        }
        c
    }
}

pub struct KafkaSink {
    pub config: KafkaSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    policy: TargetPolicy,
    owner: Arc<MemoryOwner>,
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("brokers", &self.config.client.brokers.len())
            .field("topic", &self.config.topic)
            .field("idempotence", &self.config.idempotence)
            .finish_non_exhaustive()
    }
}

/// Outbox receipt: ack only if every row of the batch was delivered.
struct BatchReceipt(Option<Arc<InflightCounter>>);
impl BatchReceipt {
    fn ack(&mut self) {
        if let Some(outbox) = self.0.take() {
            outbox.ack();
        }
    }
}
impl Drop for BatchReceipt {
    fn drop(&mut self) {
        if let Some(outbox) = self.0.take() {
            outbox.fail();
        }
    }
}

/// Stop deadline shared by everything after cancellation.
struct Stop<'a> {
    cancel: &'a CancellationToken,
    timeout: Duration,
    deadline: Option<Instant>,
}

impl Stop<'_> {
    fn arm(&mut self) {
        if self.deadline.is_none() {
            self.deadline = Some(Instant::now() + self.timeout);
        }
    }
    fn expired(&mut self) -> bool {
        if self.cancel.is_cancelled() {
            self.arm();
        }
        self.deadline.is_some_and(|at| Instant::now() >= at)
    }
    /// Wait for `fut`, arming the deadline on cancellation; `None` past it.
    async fn wait<F: std::future::Future>(&mut self, fut: F) -> Option<F::Output> {
        tokio::pin!(fut);
        loop {
            if self.expired() {
                return None;
            }
            match self.deadline {
                Some(at) => return tokio::time::timeout_at(at, fut.as_mut()).await.ok(),
                None => {
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => self.arm(),
                        out = fut.as_mut() => return Some(out),
                    }
                }
            }
        }
    }
}

enum Batch {
    Delivered,
    /// Some rows were dropped (encode/oversize); the receipt failed.
    Partial,
    /// A delivery failed or could not be confirmed: fail closed.
    Failed,
    /// The stop deadline expired.
    Expired,
}

impl KafkaSink {
    pub fn bind(
        config: KafkaSinkConfig,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(policy)?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let mut budget = owner.budget();
        budget.reservation_bytes /= 2;
        let owner = MemoryOwner::child(owner, budget, "kafka-sink");
        Ok(Self {
            config,
            diag,
            policy: policy.clone(),
            owner,
        })
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx = rx.into();
        let _lifecycle = self.diag.observation.lifecycle(false);
        let outbox = outbox.as_ref();
        let Ok(static_lease) = self
            .owner
            .acquire(CreditKind::Reservation, self.config.reservation())
        else {
            self.fail(ErrorCode::BoundExceeded, "kafka_sink_budget");
            self.fail_closed(&mut rx, &cancel, outbox).await;
            return;
        };
        self.diag.observation.health(
            false,
            HealthState::Connecting,
            "kafka_sink_connecting",
            None,
        );
        let mut producer = match self
            .config
            .producer_config()
            .create_with_context::<_, KafkaProducer>(ChargedContext {
                _lease: Arc::new(static_lease),
            }) {
            Ok(p) => DeferredProducer(Some(p)),
            Err(_) => {
                self.fail(ErrorCode::JobFailed, "kafka_sink_create_failed");
                self.fail_closed(&mut rx, &cancel, outbox).await;
                return;
            }
        };
        let mut stop = Stop {
            cancel: &cancel,
            timeout: self.config.flush_timeout,
            deadline: None,
        };
        let failed = match self.preflight(&producer, &mut stop).await {
            Ok(true) => {
                self.diag
                    .observation
                    .health(false, HealthState::Ready, "kafka_sink_ready", None);
                !self.pump(&producer, &mut rx, &mut stop, outbox).await
            }
            Ok(false) => false, // stopped before ready
            Err(code) => {
                self.fail(code, "kafka_sink_preflight_failed");
                true
            }
        };
        // Dropping purges and flushes (bounded 500 ms) and destroys the
        // client, which may block: keep it off the runtime workers.
        if failed {
            // Stop the source/job promptly, before any blocking SDK cleanup.
            self.fail_closed(&mut rx, &cancel, outbox).await;
        }
        stop.arm();
        let native = producer.0.take();
        let _ = stop
            .wait(tokio::task::spawn_blocking(move || drop(native)))
            .await;
        if !failed {
            self.discard(&mut rx, outbox);
            self.diag
                .observation
                .health(false, HealthState::Stopped, "kafka_sink_stopped", None);
        }
    }

    async fn fail_closed(
        &self,
        rx: &mut ObservedReceiver<RowBatch>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) {
        self.diag.kafka.sink_fatal.fetch_add(1, Ordering::Relaxed);
        cancel.cancel();
        self.discard(rx, outbox);
    }

    fn fail(&self, code: ErrorCode, reason: &'static str) {
        self.diag
            .observation
            .health(false, HealthState::Failed, reason, Some(code));
    }

    /// Topic exists and every advertised broker is allowed. Retries
    /// transient metadata failures until stop; `Err` is fail-closed.
    async fn preflight(
        &self,
        producer: &KafkaProducer,
        stop: &mut Stop<'_>,
    ) -> std::result::Result<bool, ErrorCode> {
        let mut backoff = Duration::from_millis(100);
        loop {
            let p = producer.clone();
            let topic = self.config.topic.clone();
            let timeout = self.config.client.request_timeout;
            let fetched = stop
                .wait(tokio::task::spawn_blocking(move || {
                    p.client().fetch_metadata(Some(&topic), timeout)
                }))
                .await;
            let Some(fetched) = fetched else {
                return Ok(false);
            };
            match fetched {
                Ok(Ok(metadata)) => {
                    check_advertised(&metadata, &self.policy).map_err(|e| e.code)?;
                    let exists = metadata.topics().iter().any(|t| {
                        t.name() == self.config.topic
                            && t.error().is_none()
                            && !t.partitions().is_empty()
                    });
                    if !exists {
                        return Err(ErrorCode::InvalidArgument);
                    }
                    return Ok(true);
                }
                Ok(Err(_)) | Err(_) => {
                    self.diag.observation.health(
                        false,
                        HealthState::Reconnecting,
                        "kafka_sink_metadata_failed",
                        Some(ErrorCode::JobFailed),
                    );
                    if stop.wait(tokio::time::sleep(backoff)).await.is_none() {
                        return Ok(false);
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    async fn pump(
        &self,
        producer: &KafkaProducer,
        rx: &mut ObservedReceiver<RowBatch>,
        stop: &mut Stop<'_>,
        outbox: Option<&Arc<InflightCounter>>,
    ) -> bool {
        let mut last_policy = std::time::Instant::now();
        loop {
            let batch = if stop.deadline.is_none() && !stop.cancel.is_cancelled() {
                tokio::select! {
                    biased;
                    _ = stop.cancel.cancelled() => {
                        stop.arm();
                        continue;
                    }
                    b = rx.recv() => b,
                }
            } else {
                // Stopping: only batches already queued, within the deadline.
                stop.arm();
                rx.close();
                rx.try_recv().ok()
            };
            let Some(batch) = batch else { return true };
            if last_policy.elapsed() >= self.config.client.policy_check_interval {
                last_policy = std::time::Instant::now();
                let checked = stop.wait(self.policy_check(producer)).await;
                if checked.is_none() {
                    drop(BatchReceipt(outbox.cloned()));
                    self.diag
                        .kafka
                        .sink_discarded_on_close
                        .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    return true;
                }
                if let Some(Err(code)) = checked {
                    self.fail(code, "kafka_sink_broker_denied");
                    drop(BatchReceipt(outbox.cloned()));
                    self.diag
                        .kafka
                        .sink_discarded_on_close
                        .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    return false;
                }
            }
            match self.send_batch(producer, &batch, stop, outbox).await {
                Batch::Delivered | Batch::Partial => {}
                Batch::Failed => {
                    self.fail(ErrorCode::JobFailed, "kafka_sink_delivery_failed");
                    return false;
                }
                Batch::Expired => {
                    producer.purge(PurgeConfig::default().queue().inflight());
                    return true;
                }
            }
        }
    }

    async fn policy_check(&self, producer: &KafkaProducer) -> std::result::Result<(), ErrorCode> {
        let p = producer.clone();
        let topic = self.config.topic.clone();
        let timeout = self.config.client.request_timeout;
        match tokio::task::spawn_blocking(move || p.client().fetch_metadata(Some(&topic), timeout))
            .await
        {
            Ok(Ok(metadata)) => check_advertised(&metadata, &self.policy).map_err(|e| e.code),
            _ => Ok(()), // transient: retried at the next interval
        }
    }

    fn key<'r>(
        &self,
        schema_index: Option<usize>,
        row: &'r Row,
    ) -> std::result::Result<Option<&'r [u8]>, ()> {
        let Some(i) = schema_index else {
            return Ok(None);
        };
        match row.values.get(i) {
            Some(Scalar::Utf8(s)) => Ok(Some(s.as_bytes())),
            Some(Scalar::Bytes(b)) => Ok(Some(b)),
            Some(Scalar::Null) | None => Ok(None),
            Some(_) => Err(()),
        }
    }

    async fn send_batch(
        &self,
        producer: &KafkaProducer,
        batch: &RowBatch,
        stop: &mut Stop<'_>,
        outbox: Option<&Arc<InflightCounter>>,
    ) -> Batch {
        let k = &self.diag.kafka;
        let mut receipt = BatchReceipt(outbox.cloned());
        if batch.output_sequence().is_some() {
            self.fail(
                ErrorCode::UnsupportedRestore,
                "kafka_reliable_output_rejected",
            );
            return Batch::Failed;
        }
        let mut delivered = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        let schema = batch.schema();
        let key_index = match &self.config.key_column {
            Some(name) => match schema.index_of_name(name) {
                Some(i) => Some(i),
                None => {
                    k.sink_dropped_bad
                        .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    return Batch::Partial;
                }
            },
            None => None,
        };
        let format = &self.config.payload_format;
        let limit = self.config.max_message_bytes;
        let mut pending: FuturesUnordered<PendingReport> = FuturesUnordered::new();
        let mut all = true;
        let rows = batch.rows();
        for (i, row) in rows.iter().enumerate() {
            if i % 64 == 0 {
                tokio::task::yield_now().await;
            }
            if stop.expired() {
                k.sink_discarded_on_close
                    .fetch_add((rows.len() - i + pending.len()) as u64, Ordering::Relaxed);
                return Batch::Expired;
            }
            let Ok(key) = self.key(key_index, row) else {
                all = false;
                k.sink_dropped_bad.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if key.is_some_and(|k| k.len() > MAX_KEY_BYTES) {
                all = false;
                k.sink_dropped_oversize.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let started = std::time::Instant::now();
            let scratch = format.encode_scratch(schema, row);
            let encoded = (|| {
                // Charged before encoding; the output grows under the lease.
                let mut lease = self.owner.acquire(CreditKind::Reservation, scratch)?;
                let body = format.encode_row_bounded_with_capacity(schema, row, limit, |cap| {
                    lease.grow_to(scratch.saturating_add(cap))
                })?;
                Ok::<_, sparrow_model::SparrowError>((body, lease))
            })();
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            let (body, lease) = match encoded {
                Ok(v) => v,
                Err(e) if e.code == ErrorCode::BoundExceeded => {
                    all = false;
                    k.sink_dropped_oversize.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(e) => {
                    all = false;
                    if e.code == ErrorCode::ResourceExhausted {
                        k.sink_failed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.diag.format_encode_error(format);
                        k.sink_dropped_bad.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };
            // Bounded in flight: wait for a report before sending more.
            while pending.len() >= self.config.max_in_flight {
                match self.next_report(&mut pending, stop).await {
                    Some(true) => {}
                    Some(false) => return Batch::Failed,
                    None => return self.expired(rows.len() - i, &pending),
                }
            }
            // FutureProducer detaches failed messages into the delivery
            // future. That copy can outlive librdkafka's queue accounting.
            let report_bytes = body
                .len()
                .saturating_add(key.map_or(0, |k| k.len()))
                .saturating_add(REPORT_OVERHEAD);
            if report_bytes
                > self
                    .owner
                    .budget()
                    .reservation_bytes
                    .saturating_sub(self.config.reservation())
                    .saturating_sub(lease.bytes())
            {
                all = false;
                k.sink_failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let report_credit = loop {
                if let Ok(credit) = self.owner.acquire(CreditKind::Reservation, report_bytes) {
                    break credit;
                }
                let progress = if pending.is_empty() {
                    stop.wait(tokio::time::sleep(QUEUE_FULL_RETRY))
                        .await
                        .map(|_| true)
                } else {
                    self.next_report(&mut pending, stop).await
                };
                match progress {
                    Some(true) => {}
                    Some(false) => return Batch::Failed,
                    None => return self.expired(rows.len() - i, &pending),
                }
            };
            let mut record = FutureRecord::<[u8], [u8]>::to(&self.config.topic).payload(&body[..]);
            if let Some(key) = key {
                record = record.key(key);
            }
            loop {
                if stop.expired() {
                    return self.expired(rows.len() - i, &pending);
                }
                match producer.send_result(record) {
                    Ok(report) => {
                        pending.push(Box::pin(async move { (report.await, report_credit) }));
                        break;
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                        record = back;
                        k.sink_queue_full_waits.fetch_add(1, Ordering::Relaxed);
                        let progressed = if pending.is_empty() {
                            stop.wait(tokio::time::sleep(QUEUE_FULL_RETRY))
                                .await
                                .map(|_| true)
                        } else {
                            self.next_report(&mut pending, stop).await
                        };
                        match progressed {
                            Some(true) => {}
                            Some(false) => return Batch::Failed,
                            None => return self.expired(rows.len() - i, &pending),
                        }
                    }
                    Err((
                        KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
                        _,
                    )) => {
                        all = false;
                        k.sink_dropped_oversize.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => {
                        k.sink_failed.fetch_add(1, Ordering::Relaxed);
                        return Batch::Failed;
                    }
                }
            }
        }
        while !pending.is_empty() {
            match self.next_report(&mut pending, stop).await {
                Some(true) => {}
                Some(false) => return Batch::Failed,
                None => return self.expired(0, &pending),
            }
        }
        if all {
            delivered.complete();
            receipt.ack();
            Batch::Delivered
        } else {
            Batch::Partial
        }
    }

    fn expired(&self, unsent: usize, pending: &FuturesUnordered<PendingReport>) -> Batch {
        self.diag
            .kafka
            .sink_discarded_on_close
            .fetch_add((unsent + pending.len()) as u64, Ordering::Relaxed);
        Batch::Expired
    }

    /// One delivery report: `Some(true)` acked, `Some(false)` failed,
    /// `None` stop deadline expired.
    async fn next_report(
        &self,
        pending: &mut FuturesUnordered<PendingReport>,
        stop: &mut Stop<'_>,
    ) -> Option<bool> {
        let k = &self.diag.kafka;
        match stop.wait(pending.next()).await? {
            Some((Ok(Ok(_)), _credit)) => {
                k.sink_acked.fetch_add(1, Ordering::Relaxed);
                self.diag.observation.progress(false, 1);
                Some(true)
            }
            Some(_) | None => {
                k.sink_failed.fetch_add(1, Ordering::Relaxed);
                Some(false)
            }
        }
    }

    fn discard(&self, rx: &mut ObservedReceiver<RowBatch>, outbox: Option<&Arc<InflightCounter>>) {
        rx.close();
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .kafka
                .sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
        }
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use sparrow_model::{
        DataType, Field, FieldId, OutputSequence, ResourceBudget, RowBatchBuilder, Schema, SchemaId,
    };

    #[tokio::test(start_paused = true)]
    async fn kafka_expired_stop_refuses_ready_work() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stop = Stop {
            cancel: &cancel,
            timeout: Duration::from_millis(100),
            deadline: None,
        };
        assert_eq!(stop.wait(std::future::ready(1)).await, Some(1));
        let deadline = stop.deadline;
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(stop.wait(std::future::ready(2)).await, None);
        assert_eq!(stop.deadline, deadline);
    }

    #[tokio::test]
    async fn kafka_reliable_sequence_is_refused_without_sending() {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let sink = KafkaSink::bind(
            KafkaSinkConfig::new(vec!["127.0.0.1:9092".into()], "t"),
            &TargetPolicy::allow("127.0.0.1", 9092),
            owner.clone(),
            IoDiagnostics::new(),
        )
        .unwrap();
        let lease = owner.acquire(CreditKind::Reservation, 1).unwrap();
        let producer: KafkaProducer = ClientConfig::new()
            .create_with_context(ChargedContext {
                _lease: Arc::new(lease),
            })
            .unwrap();
        let schema = Arc::new(
            Schema::new(
                SchemaId::new(1),
                vec![Field::new(FieldId::new(1), "v", DataType::Int64, false)],
            )
            .unwrap(),
        );
        let mut b =
            RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 1, 1024).unwrap();
        b.push(Row {
            values: vec![Scalar::Int64(1)],
        })
        .unwrap();
        let batch = b
            .finish()
            .unwrap()
            .with_output_sequence(OutputSequence::new([1; 16], 1).unwrap())
            .unwrap();
        let outbox = Arc::new(InflightCounter::new());
        outbox.enqueue();
        let cancel = CancellationToken::new();
        let mut stop = Stop {
            cancel: &cancel,
            timeout: Duration::from_secs(1),
            deadline: None,
        };
        assert!(matches!(
            sink.send_batch(&producer, &batch, &mut stop, Some(&outbox))
                .await,
            Batch::Failed
        ));
        assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
        tokio::task::spawn_blocking(move || drop(producer))
            .await
            .unwrap();
    }
}

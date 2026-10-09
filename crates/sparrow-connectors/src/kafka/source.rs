//! Kafka consumer-group Source.
//!
//! A dedicated thread owns the librdkafka consumer (`BaseConsumer`): every
//! blocking call (poll, rebalance callbacks, synchronous commits, metadata
//! and committed-offset lookups) runs there, never on a runtime worker. The
//! thread hands out one message at a time and waits for its outcome before
//! polling again, so when a rebalance callback runs nothing is in flight.
//!
//! Offsets (`next = offset + 1`) are recorded per partition only when the
//! async side reports the message *processed*: its row was admitted into the
//! job inbox, or it was skipped as poison under the skip policy. Commits
//! carry only processed offsets: periodically (async), synchronously before
//! a partition is revoked, and synchronously on stop. A message that was
//! handed out but not processed when the job stops is redelivered.
//!
//! Admission never drops for lack of credit: payload copy, decode scratch,
//! the working row and the inbox slot are all waited for (cancel-aware).
//! What can never be admitted (over `max_message_bytes`, a decode scratch or
//! row above the job's bounds, an undecodable payload) is poison and follows
//! `fail_on_decode`: skip (counted, committed past) or fail the Source
//! (nothing committed past it, so it is redelivered on restart).
//!
//! Every commit carries an identity string (cluster id, topic, group,
//! partition count, payload format). On assignment the committed offsets
//! are read back first; an offset written under another identity, or by a
//! non-Sparrow client (no identity), is refused with `unsupported_restore`
//! before any message is handed out.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::config::{ClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use sparrow_formats::{JsonLimits, PayloadFormat};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, QueuedRow, RestoreClaim, Result, Row, Schema,
    SparrowError,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::client::{check_advertised, millis, KafkaClientConfig};
use super::{check_name, error};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::TargetPolicy;

pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
pub const DEFAULT_FETCH_MAX_BYTES: usize = 256 * 1024;
pub const DEFAULT_PREFETCH_BYTES: usize = 512 * 1024;
const MAX_INBOX: usize = 4096;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
/// librdkafka per-client fixed buffers outside the fetch bounds (estimate).
const CLIENT_OVERHEAD: usize = 256 * 1024;
const IDENTITY_PREFIX: &str = "sparrow-kafka-v1:";
const POLL: Duration = Duration::from_millis(100);
const CREDIT_RETRY: Duration = Duration::from_millis(2);

/// `auto.offset.reset`: where a partition starts when the group has no
/// committed offset for it (or the committed offset is out of range).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetReset {
    Earliest,
    Latest,
    /// Fail the Source instead of choosing a position.
    Error,
}

impl OffsetReset {
    fn as_str(self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug)]
pub struct KafkaSourceConfig {
    pub client: KafkaClientConfig,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: OffsetReset,
    /// Largest message value accepted (1..=1 MiB, default 64 KiB); the
    /// payload format's own limit applies too. Larger messages are poison.
    pub max_message_bytes: usize,
    /// librdkafka `fetch.max.bytes` = `max.partition.fetch.bytes`
    /// (64 KiB..=4 MiB, default 256 KiB, >= max_message_bytes + 1 KiB).
    pub fetch_max_bytes: usize,
    /// librdkafka prefetch queue bound `queued.max.messages.kbytes`
    /// (64 KiB..=16 MiB, default 512 KiB, >= fetch_max_bytes).
    pub prefetch_bytes: usize,
    /// Periodic async commit of processed offsets (100 ms..=60 s, default 1 s).
    pub commit_interval: Duration,
    pub session_timeout: Duration,
    pub max_poll_interval: Duration,
    /// Bound on the final commit and consumer close at stop (default 5 s).
    pub stop_timeout: Duration,
    pub inbox_capacity: usize,
    pub inbox_bytes: usize,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    /// Poison policy: `false` skips (counted, committed past), `true` fails
    /// the Source without committing past the message.
    pub fail_on_decode: bool,
    pub restore: RestoreClaim,
    pub payload_format: PayloadFormat,
}

impl KafkaSourceConfig {
    pub fn new(
        brokers: Vec<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
        auto_offset_reset: OffsetReset,
        schema: Schema,
    ) -> Self {
        Self {
            client: KafkaClientConfig::new(brokers),
            topic: topic.into(),
            group_id: group_id.into(),
            auto_offset_reset,
            max_message_bytes: 64 * 1024,
            fetch_max_bytes: DEFAULT_FETCH_MAX_BYTES,
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
            commit_interval: Duration::from_secs(1),
            session_timeout: Duration::from_secs(10),
            max_poll_interval: Duration::from_secs(300),
            stop_timeout: Duration::from_secs(5),
            inbox_capacity: 16,
            inbox_bytes: DEFAULT_INBOX_BYTES,
            schema,
            json_limits: JsonLimits::default(),
            fail_on_decode: false,
            restore: RestoreClaim::None,
            payload_format: PayloadFormat::Json,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::KAFKA_SOURCE
    }

    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        check_name("topic", &self.topic, 249)?;
        check_name("group_id", &self.group_id, 255)?;
        let within = |d: Duration, lo: Duration, hi: Duration| (lo..=hi).contains(&d);
        let ms = Duration::from_millis;
        let s = Duration::from_secs;
        if !(1..=MAX_MESSAGE_BYTES).contains(&self.max_message_bytes)
            || !(64 * 1024..=4 * 1024 * 1024).contains(&self.fetch_max_bytes)
            || self.fetch_max_bytes < self.max_message_bytes.saturating_add(1024)
            || !(64 * 1024..=16 * 1024 * 1024).contains(&self.prefetch_bytes)
            || self.prefetch_bytes < self.fetch_max_bytes
            || !within(self.commit_interval, ms(100), s(60))
            || !within(self.session_timeout, s(6), s(300))
            || !within(self.max_poll_interval, self.session_timeout, s(3600))
            || !within(self.stop_timeout, ms(100), s(60))
            || !(1..=MAX_INBOX).contains(&self.inbox_capacity)
            || self.json_limits.max_bytes > 64 * 1024
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "Kafka source bounds: max_message_bytes 1..=1MiB; fetch_max_bytes 64KiB..=4MiB and >= max_message_bytes+1KiB; prefetch_bytes 64KiB..=16MiB and >= fetch_max_bytes; commit_interval 100ms..=60s; session_timeout 6..=300s; max_poll_interval session_timeout..=3600s; stop_timeout 100ms..=60s; inbox_capacity 1..=4096",
            ));
        }
        self.check_inbox_budget(sparrow_model::ResourceBudget::compact().queue_bytes)?;
        self.client.validate(policy)
    }

    /// Static reservation for librdkafka-held memory: the prefetch queue,
    /// one fetch response plus its decompression working copy and fixed
    /// client buffers (the payload copy handed to decoding is charged per
    /// message). librdkafka allocates outside
    /// the job's accounting, so this configured bound is charged up front.
    pub fn reservation(&self) -> usize {
        Self::reservation_for(self.prefetch_bytes, self.fetch_max_bytes)
    }

    pub fn reservation_for(prefetch_bytes: usize, fetch_max_bytes: usize) -> usize {
        prefetch_bytes
            .saturating_add(fetch_max_bytes.saturating_mul(2))
            .saturating_add(CLIENT_OVERHEAD)
    }

    /// One connector may use at most half the job reservation budget.
    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "Kafka source buffers (prefetch + 2*fetch + client) exceed half the job reservation budget",
            ));
        }
        Ok(())
    }

    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        if self.inbox_bytes == 0
            || self
                .inbox_bytes
                .saturating_add(QueuedRow::channel_budget(self.inbox_capacity))
                > queue_budget.min(4 * 1024 * 1024)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "Kafka inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
            ));
        }
        Ok(())
    }

    /// librdkafka consumer settings (explicit; nothing left to defaults that
    /// changes delivery semantics).
    pub(crate) fn consumer_config(&self) -> ClientConfig {
        let mut c = self.client.base();
        let fetch = self.fetch_max_bytes.to_string();
        c.set("group.id", &self.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", self.auto_offset_reset.as_str())
            .set("enable.partition.eof", "false")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("session.timeout.ms", millis(self.session_timeout))
            .set("heartbeat.interval.ms", millis(self.session_timeout / 3))
            .set("max.poll.interval.ms", millis(self.max_poll_interval))
            .set(
                "queued.max.messages.kbytes",
                (self.prefetch_bytes / 1024).to_string(),
            )
            .set("queued.min.messages", "1")
            .set("fetch.max.bytes", &fetch)
            .set("max.partition.fetch.bytes", &fetch)
            .set("message.max.bytes", &fetch)
            .set(
                "receive.message.max.bytes",
                (self.fetch_max_bytes + 512).to_string(),
            )
            .set("check.crcs", "true")
            .set("isolation.level", "read_committed");
        c
    }

    /// Identity carried as commit metadata: cluster id, topic, group,
    /// partition count and the payload format (with its options).
    pub fn identity(&self, cluster_id: &str, partitions: usize) -> String {
        use ring::digest::{Context, SHA256};
        let mut h = Context::new(&SHA256);
        let mut put = |b: &[u8]| {
            h.update(&(b.len() as u64).to_le_bytes());
            h.update(b);
        };
        put(IDENTITY_PREFIX.as_bytes());
        put(cluster_id.as_bytes());
        put(self.topic.as_bytes());
        put(self.group_id.as_bytes());
        put(&(partitions as u64).to_le_bytes());
        put(self.payload_format.name().as_bytes());
        put(&self.payload_format.identity_bytes().unwrap_or_default());
        let digest = h.finish();
        let mut out = String::from(IDENTITY_PREFIX);
        for b in digest.as_ref() {
            use std::fmt::Write;
            let _ = write!(out, "{b:02x}");
        }
        out
    }
}

pub struct KafkaSource {
    pub config: KafkaSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    policy: TargetPolicy,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("brokers", &self.config.client.brokers.len())
            .field("topic", &self.config.topic)
            .field("group_id", &self.config.group_id)
            .finish_non_exhaustive()
    }
}

/// What the async side decided about the message handed to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// Admitted downstream or skipped as poison: its offset may be committed.
    Processed,
    /// Not processed (stop or fatal): never committed past.
    Unprocessed,
}

struct Delivery {
    /// `None`: over `max_message_bytes` (never copied).
    payload: Option<(Vec<u8>, MemoryLease)>,
    received_at: Instant,
}

/// State shared between the consumer thread's poll loop and the rebalance
/// callbacks (which run inside `poll` on the same thread).
#[derive(Default)]
struct Tracked {
    identity: String,
    partitions: usize,
    assigned: BTreeSet<i32>,
    /// Next offset to commit per assigned partition (processed + 1).
    processed: BTreeMap<i32, i64>,
    /// Offsets the coordinator confirmed (sync result or async callback).
    /// Anything processed beyond it is committed again until confirmed, so
    /// a failed async commit is retried rather than forgotten.
    confirmed: BTreeMap<i32, i64>,
    fatal: Option<SparrowError>,
}

struct Ctx {
    topic: String,
    request_timeout: Duration,
    diag: Arc<IoDiagnostics>,
    tracked: Mutex<Tracked>,
}

impl Ctx {
    fn lock(&self) -> std::sync::MutexGuard<'_, Tracked> {
        self.tracked.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Commit list for `only` (or every partition processed beyond its
    /// confirmed commit), tagged with the identity.
    fn commit_list(&self, only: Option<&[i32]>) -> KafkaResult<(TopicPartitionList, Vec<i32>)> {
        let t = self.lock();
        let mut list = TopicPartitionList::new();
        let mut parts = Vec::new();
        for (&p, &next) in &t.processed {
            let wanted = match only {
                Some(only) => only.contains(&p),
                None => t.confirmed.get(&p) != Some(&next),
            };
            if wanted {
                let mut e = list.add_partition(&self.topic, p);
                e.set_offset(Offset::Offset(next))?;
                e.set_metadata(&t.identity);
                parts.push(p);
            }
        }
        Ok((list, parts))
    }

    fn confirm(&self, list: &TopicPartitionList) {
        let mut t = self.lock();
        for e in list.elements_for_topic(&self.topic) {
            if let Offset::Offset(o) = e.offset() {
                let c = t.confirmed.entry(e.partition()).or_insert(o);
                *c = (*c).max(o);
            }
        }
    }

    fn commit(&self, consumer: &BaseConsumer<Ctx>, only: Option<&[i32]>, mode: CommitMode) {
        let (list, parts) = match self.commit_list(only) {
            Ok(v) => v,
            Err(_) => {
                self.diag
                    .kafka
                    .source_commit_failed
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if parts.is_empty() {
            return;
        }
        self.diag
            .kafka
            .source_commits
            .fetch_add(1, Ordering::Relaxed);
        match consumer.commit(&list, mode) {
            Ok(()) => {
                // Async: enqueued only; confirmed in `commit_callback`.
                if matches!(mode, CommitMode::Sync) {
                    self.confirm(&list);
                }
            }
            Err(_) => {
                self.diag
                    .kafka
                    .source_commit_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Read the committed offsets of a new assignment back and refuse any
    /// written under another identity (or by a non-Sparrow client).
    fn check_assignment(&self, consumer: &BaseConsumer<Ctx>, parts: &[i32]) -> Result<()> {
        let (identity, partitions) = {
            let t = self.lock();
            (t.identity.clone(), t.partitions)
        };
        let metadata = consumer
            .fetch_metadata(Some(&self.topic), self.request_timeout)
            .map_err(|e| kafka_error("Kafka metadata on assignment", &e).retryable(true))?;
        let now = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .map_or(0, |t| t.partitions().len());
        if now != partitions {
            return Err(error(
                ErrorCode::UnsupportedRestore,
                format!(
                    "Kafka topic partition count changed ({partitions} -> {now}); committed offsets are bound to it, use a new group_id"
                ),
            ));
        }
        let mut query = TopicPartitionList::new();
        for &p in parts {
            query.add_partition(&self.topic, p);
        }
        let committed = consumer
            .committed_offsets(query, self.request_timeout)
            .map_err(|e| kafka_error("Kafka committed offsets", &e).retryable(true))?;
        for e in committed.elements() {
            if let Offset::Offset(_) = e.offset() {
                if e.metadata() == identity {
                    continue;
                }
                let what = if e.metadata().starts_with(IDENTITY_PREFIX) {
                    "was committed under a different cluster/topic/group/partition-count/format identity"
                } else {
                    "was committed by a client that is not this Sparrow source (no identity)"
                };
                return Err(error(
                    ErrorCode::UnsupportedRestore,
                    format!(
                        "Kafka group offset for partition {} {what}; refusing to resume from it (use a new group_id)",
                        e.partition()
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl ClientContext for Ctx {}

impl ConsumerContext for Ctx {
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let k = &self.diag.kafka;
        match rebalance {
            Rebalance::Revoke(list) => {
                let parts: Vec<i32> = list
                    .elements_for_topic(&self.topic)
                    .iter()
                    .map(|e| e.partition())
                    .collect();
                // Nothing is in flight here (the poll thread waits for each
                // outcome), so the processed offsets are final: commit them
                // before the partitions move. A lost assignment cannot commit.
                if consumer.assignment_lost() {
                    k.source_lost.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.commit(consumer, Some(&parts), CommitMode::Sync);
                }
                let mut t = self.lock();
                for p in &parts {
                    t.assigned.remove(p);
                    t.processed.remove(p);
                    t.confirmed.remove(p);
                }
                k.source_revoked
                    .fetch_add(parts.len() as u64, Ordering::Relaxed);
            }
            Rebalance::Assign(list) => {
                let parts: Vec<i32> = list
                    .elements_for_topic(&self.topic)
                    .iter()
                    .map(|e| e.partition())
                    .collect();
                if parts.is_empty() {
                    return;
                }
                match self.check_assignment(consumer, &parts) {
                    Ok(()) => {
                        self.lock().assigned.extend(parts.iter().copied());
                        k.source_assigned
                            .fetch_add(parts.len() as u64, Ordering::Relaxed);
                        self.diag.observation.health(
                            true,
                            HealthState::Ready,
                            "kafka_assigned",
                            None,
                        );
                    }
                    Err(e) => {
                        self.lock().fatal.get_or_insert(e);
                    }
                }
            }
            Rebalance::Error(_) => {
                k.source_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        // librdkafka reports every commit here (sync ones too; confirming
        // twice is harmless).
        match result {
            Ok(()) => self.confirm(offsets),
            Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
            Err(_) => {
                self.diag
                    .kafka
                    .source_commit_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn kafka_error(what: &str, e: &KafkaError) -> SparrowError {
    error(ErrorCode::JobFailed, format!("{what}: {e}"))
}

struct Thread {
    config: KafkaSourceConfig,
    policy: TargetPolicy,
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    stop: Arc<AtomicBool>,
    feed: mpsc::Sender<Delivery>,
    outcomes: std::sync::mpsc::Receiver<Outcome>,
    /// librdkafka's buffers stay charged until the consumer is destroyed on
    /// this thread, also when a stop timeout detached it.
    _static: MemoryLease,
}

impl Thread {
    fn run(self) -> Result<()> {
        let ctx = Ctx {
            topic: self.config.topic.clone(),
            request_timeout: self.config.client.request_timeout,
            diag: self.diag.clone(),
            tracked: Mutex::new(Tracked::default()),
        };
        let consumer: BaseConsumer<Ctx> =
            BaseConsumer::from_config_and_context(&self.config.consumer_config(), ctx)
                .map_err(|e| kafka_error("Kafka consumer create", &e))?;
        let result = self.consume(&consumer);
        // Commit what was processed (also after a poison/admission failure:
        // those offsets were admitted). Skipped when identity checks failed
        // (nothing was handed out) or the assignment is lost.
        let ctx = consumer.context();
        if !consumer.assignment_lost() {
            ctx.commit(&consumer, None, CommitMode::Sync);
        }
        consumer.unsubscribe();
        drop(consumer); // closes: leaves the group (bounded by stop_timeout on the async side)
        result
    }

    fn consume(&self, consumer: &BaseConsumer<Ctx>) -> Result<()> {
        let cfg = &self.config;
        let ctx = consumer.context();
        let timeout = cfg.client.request_timeout;
        let metadata = consumer
            .fetch_metadata(Some(&cfg.topic), timeout)
            .map_err(|e| kafka_error("Kafka metadata", &e).retryable(true))?;
        check_advertised(&metadata, &self.policy)?;
        let partitions = metadata
            .topics()
            .iter()
            .find(|t| t.name() == cfg.topic && t.error().is_none())
            .map_or(0, |t| t.partitions().len());
        if partitions == 0 {
            return Err(error(
                ErrorCode::InvalidArgument,
                format!("Kafka topic {:?} does not exist", cfg.topic),
            ));
        }
        let cluster = consumer.client().fetch_cluster_id(timeout).ok_or_else(|| {
            error(ErrorCode::JobFailed, "Kafka cluster id unavailable").retryable(true)
        })?;
        {
            let mut t = ctx.lock();
            t.identity = cfg.identity(&cluster, partitions);
            t.partitions = partitions;
        }
        consumer
            .subscribe(&[&cfg.topic])
            .map_err(|e| kafka_error("Kafka subscribe", &e))?;
        let mut last_commit = Instant::now();
        let mut last_policy = Instant::now();
        let k = &self.diag.kafka;
        loop {
            if self.stop.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(e) = ctx.lock().fatal.take() {
                return Err(e);
            }
            if last_commit.elapsed() >= cfg.commit_interval {
                last_commit = Instant::now();
                ctx.commit(consumer, None, CommitMode::Async);
            }
            if last_policy.elapsed() >= cfg.client.policy_check_interval {
                last_policy = Instant::now();
                // Transient metadata failures are retried next interval; an
                // advertised broker outside the allowlist fails closed.
                if let Ok(m) = consumer.fetch_metadata(Some(&cfg.topic), timeout) {
                    check_advertised(&m, &self.policy)?;
                }
            }
            let message = match consumer.poll(POLL) {
                None => continue,
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    classify(&e)?;
                    k.source_errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            if let Some(e) = ctx.lock().fatal.take() {
                return Err(e);
            }
            let partition = message.partition();
            let offset = message.offset();
            if message.topic() != cfg.topic || !ctx.lock().assigned.contains(&partition) {
                continue; // stale fetch for a partition no longer ours
            }
            k.source_received.fetch_add(1, Ordering::Relaxed);
            let received_at = Instant::now();
            let len = message.payload_len();
            let payload = if len > cfg.max_message_bytes {
                None
            } else {
                // Charge the copy before allocating it.
                let Some(lease) = self.wait_credit(len.max(1)) else {
                    return Ok(());
                };
                Some((message.payload().unwrap_or_default().to_vec(), lease))
            };
            drop(message);
            let delivery = Delivery {
                payload,
                received_at,
            };
            if self.feed.blocking_send(delivery).is_err() {
                return Ok(());
            }
            match self.outcomes.recv() {
                Ok(Outcome::Processed) => {
                    let mut t = ctx.lock();
                    if t.assigned.contains(&partition) {
                        let next = offset
                            .checked_add(1)
                            .ok_or_else(|| error(ErrorCode::JobFailed, "Kafka offset overflow"))?;
                        t.processed.insert(partition, next);
                    }
                }
                Ok(Outcome::Unprocessed) | Err(_) => return Ok(()),
            }
        }
    }

    fn wait_credit(&self, bytes: usize) -> Option<MemoryLease> {
        let mut waited = false;
        loop {
            if let Ok(lease) = self.owner.acquire(CreditKind::Reservation, bytes) {
                return Some(lease);
            }
            if self.stop.load(Ordering::Acquire) {
                return None;
            }
            if !waited {
                waited = true;
                self.diag
                    .kafka
                    .source_budget_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            std::thread::sleep(CREDIT_RETRY);
        }
    }
}

/// Consumer errors: fatal ones end the Source, the rest are transient
/// (librdkafka retries them itself) and only counted.
fn classify(e: &KafkaError) -> Result<()> {
    match e {
        KafkaError::PartitionEOF(_) => Ok(()),
        KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset) => Err(error(
            ErrorCode::UnsupportedRestore,
            "Kafka partition has no usable committed offset and auto_offset_reset is error",
        )),
        KafkaError::MessageConsumptionFatal(_) => Err(kafka_error("Kafka fatal consumer error", e)),
        KafkaError::MessageConsumption(
            RDKafkaErrorCode::Fatal
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed,
        ) => Err(kafka_error("Kafka consumer error", e)),
        KafkaError::Global(RDKafkaErrorCode::Fatal) => Err(kafka_error("Kafka fatal error", e)),
        _ => Ok(()),
    }
}

struct Ingress<'a> {
    tx: &'a ObservedSender<QueuedRow>,
    owner: &'a Arc<MemoryOwner>,
    queue: &'a Arc<MemoryOwner>,
    max_row_bytes: usize,
}

impl KafkaSource {
    pub fn bind(
        config: KafkaSourceConfig,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(policy)?;
        Ok(Self {
            config,
            diag,
            policy: policy.clone(),
        })
    }

    /// Consume until cancelled. Requires the byte-accounted ingress.
    pub async fn run_budgeted(
        self,
        tx: impl Into<ObservedSender<QueuedRow>>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        let tx = tx.into();
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(error(
                ErrorCode::InvalidArgument,
                "Kafka inbox_capacity does not match channel",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes)?;
        // librdkafka's buffers are charged until the consumer thread ends.
        let static_lease = owner
            .acquire(CreditKind::Reservation, self.config.reservation())
            .map_err(|_| {
                error(
                    ErrorCode::BoundExceeded,
                    "Kafka source buffers do not fit the job reservation",
                )
            })?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "kafka-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let _lifecycle = self.diag.observation.lifecycle(true);
        self.diag
            .observation
            .health(true, HealthState::Connecting, "kafka_connecting", None);
        let stop = Arc::new(AtomicBool::new(false));
        let (feed_tx, mut feed) = mpsc::channel(1);
        let (outcome_tx, outcomes) = std::sync::mpsc::channel();
        let (done_tx, done) = oneshot::channel();
        let thread = Thread {
            config: self.config.clone(),
            policy: self.policy.clone(),
            diag: self.diag.clone(),
            owner: owner.clone(),
            stop: stop.clone(),
            feed: feed_tx,
            outcomes,
            _static: static_lease,
        };
        std::thread::Builder::new()
            .name("sparrow-kafka-consumer".into())
            .spawn(move || {
                let _ = done_tx.send(thread.run());
            })
            .map_err(|e| error(ErrorCode::JobFailed, format!("Kafka consumer thread: {e}")))?;
        let child = cancel.child_token();
        let _cancel_on_drop = child.clone().drop_guard();
        let result = loop {
            let delivery = tokio::select! {
                biased;
                _ = child.cancelled() => break Ok(()),
                d = feed.recv() => d,
            };
            let Some(delivery) = delivery else {
                break Ok(()); // the thread ended; its result follows
            };
            let outcome = self.ingest(delivery, &ingress, &child).await;
            let reply = match outcome {
                Ok(true) => Outcome::Processed,
                Ok(false) | Err(_) => Outcome::Unprocessed,
            };
            let _ = outcome_tx.send(reply);
            match outcome {
                Ok(true) => {}
                Ok(false) => break Ok(()),
                Err(e) => break Err(e),
            }
        };
        // Stop: the thread commits processed offsets and closes. The deadline
        // covers both; past it the thread is detached (counted).
        stop.store(true, Ordering::Release);
        drop(outcome_tx);
        feed.close();
        let thread_result = match tokio::time::timeout(self.config.stop_timeout, done).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(error(
                ErrorCode::JobFailed,
                "Kafka consumer thread panicked",
            )),
            Err(_) => {
                self.diag
                    .kafka
                    .source_stop_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        };
        let result = result.and(thread_result);
        if let Err(e) = &result {
            self.diag
                .kafka
                .source_errors
                .fetch_add(1, Ordering::Relaxed);
            self.diag.observation.health(
                true,
                HealthState::Failed,
                "kafka_source_failed",
                Some(e.code),
            );
        }
        result
    }

    /// Poison handling: skip (processed) or fail (not committed past).
    fn poison(&self, code: ErrorCode, what: &str) -> Result<bool> {
        if self.config.fail_on_decode {
            return Err(error(
                code,
                format!("Kafka poison message: {what} (fail_on_decode)"),
            ));
        }
        self.diag
            .kafka
            .source_poison_skipped
            .fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    async fn ingest(
        &self,
        delivery: Delivery,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
    ) -> Result<bool> {
        let k = &self.diag.kafka;
        let format = &self.config.payload_format;
        let Some((payload, _copy)) = delivery.payload else {
            k.source_oversize.fetch_add(1, Ordering::Relaxed);
            return self.poison(ErrorCode::MaxRecordSize, "value over max_message_bytes");
        };
        if payload.len() > format.max_message_bytes(&self.config.json_limits) {
            k.source_oversize.fetch_add(1, Ordering::Relaxed);
            return self.poison(ErrorCode::MaxRecordSize, "value over the format limit");
        }
        // Decode scratch is charged before decoding; waits, never drops.
        let estimate = format.decode_scratch(&self.config.schema, payload.len());
        // What could never be granted while the static charge is held is
        // poison rather than an endless wait.
        let available = ingress
            .owner
            .budget()
            .reservation_bytes
            .saturating_sub(self.config.reservation());
        if estimate.saturating_add(payload.len()) > available {
            k.source_oversize.fetch_add(1, Ordering::Relaxed);
            return self.poison(
                ErrorCode::MaxRecordSize,
                "decode scratch over the job bound",
            );
        }
        let Some(_scratch) = self.wait_credit(ingress.owner, estimate, cancel).await else {
            return Ok(false);
        };
        let started = Instant::now();
        let decoded = format.decode_row(
            &self.config.schema,
            &payload,
            &self.config.json_limits,
            None,
        );
        self.diag
            .observation
            .record(Latency::Decode, started.elapsed());
        let row = match decoded {
            Ok(row) => row,
            Err(e) => {
                self.diag.format_decode_error(format, &e);
                self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                return self.poison(e.code, "undecodable value");
            }
        };
        drop(payload);
        self.diag.observation.progress(true, 1);
        self.admit(row, ingress, cancel, OriginSpan::at(delivery.received_at))
            .await
    }

    async fn wait_credit(
        &self,
        owner: &Arc<MemoryOwner>,
        bytes: usize,
        cancel: &CancellationToken,
    ) -> Option<MemoryLease> {
        let mut waited = false;
        loop {
            if let Ok(lease) = owner.acquire(CreditKind::Reservation, bytes) {
                return Some(lease);
            }
            if !waited {
                waited = true;
                self.diag
                    .kafka
                    .source_budget_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return None,
                _ = tokio::time::sleep(CREDIT_RETRY) => {}
            }
        }
    }

    /// Cancel-aware wait for working credit, Queue credit and a channel slot.
    async fn admit(
        &self,
        row: Row,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        origin: OriginSpan,
    ) -> Result<bool> {
        let _admission = self.diag.observation.timer(Latency::SourceAdmission);
        let k = &self.diag.kafka;
        let bytes = QueuedRow::accounted_bytes(&row);
        if bytes
            > ingress
                .queue
                .budget()
                .queue_bytes
                .min(ingress.max_row_bytes)
        {
            k.source_oversize.fetch_add(1, Ordering::Relaxed);
            return self.poison(ErrorCode::MaxRecordSize, "row over the inbox bound");
        }
        let Some(_working) = self.wait_credit(ingress.owner, bytes, cancel).await else {
            return Ok(false);
        };
        let mut row = Some(row);
        let mut observed_wait: Option<sparrow_io::observed::Wait<'_>> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let budget_full = match ingress.tx.try_reserve() {
                Ok(permit) => match QueuedRow::try_new(
                    row.take().expect("pending row"),
                    ingress.queue,
                    &k.source_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        k.source_rows.fetch_add(1, Ordering::Relaxed);
                        return Ok(true);
                    }
                    Err((r, _)) => {
                        row = Some(r);
                        true
                    }
                },
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => false,
            };
            if observed_wait.is_none() {
                observed_wait = Some(ingress.tx.observe_wait());
                k.source_backpressure_waits.fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(false),
                _ = async {
                    if budget_full {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        let _ = ingress.tx.reserve().await;
                    }
                } => {}
            }
        }
    }
}

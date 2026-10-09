//! Kafka unit tests plus opt-in real-broker tests. Broker tests are
//! `#[ignore]`d and need `SPARROW_KAFKA_HOME` / `SPARROW_JAVA_HOME` from
//! `scripts/kafka-broker.sh` (pinned, checksum-verified Kafka 4.3.1 and
//! Temurin 21). Each test owns an isolated single-node KRaft broker child.

use super::broker::KafkaSandbox;
use super::*;
use crate::{IoDiagnostics, TargetPolicy};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use sparrow_formats::{CsvRole, PayloadFormat};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, InflightCounter, MemoryOwner, QueuedRow,
    ResourceBudget, RestoreClaim, Result, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn csv_format(role: CsvRole) -> PayloadFormat {
    PayloadFormat::csv(
        sparrow_formats::CsvOptions::default()
            .compile(role)
            .unwrap(),
    )
}

fn protobuf_format(role: CsvRole) -> PayloadFormat {
    crate::protobuf_test_support::format(role, |_| {})
}

// ------------------------------------------------------------------ unit

#[test]
fn kafka_source_config_bounds_identity_and_client_settings() {
    let p = TargetPolicy::allow("127.0.0.1", 9092);
    let base = KafkaSourceConfig::new(
        vec!["127.0.0.1:9092".into()],
        "readings",
        "sparrow-g1",
        OffsetReset::Earliest,
        schema(),
    );
    base.validate(&p).unwrap();
    type M = fn(&mut KafkaSourceConfig);
    let cases: Vec<(M, ErrorCode)> = vec![
        (|c| c.topic = "bad topic".into(), ErrorCode::InvalidArgument),
        (|c| c.topic = "..".into(), ErrorCode::InvalidArgument),
        (|c| c.topic = "t".repeat(250), ErrorCode::InvalidArgument),
        (|c| c.group_id = String::new(), ErrorCode::InvalidArgument),
        (|c| c.max_message_bytes = 0, ErrorCode::BoundExceeded),
        (
            |c| c.max_message_bytes = usize::MAX,
            ErrorCode::BoundExceeded,
        ),
        (|c| c.fetch_max_bytes = usize::MAX, ErrorCode::BoundExceeded),
        // fetch must hold one max message plus framing.
        (
            |c| c.max_message_bytes = 256 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (|c| c.prefetch_bytes = 128 * 1024, ErrorCode::BoundExceeded),
        (|c| c.prefetch_bytes = usize::MAX, ErrorCode::BoundExceeded),
        (
            |c| c.commit_interval = Duration::from_millis(99),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.session_timeout = Duration::from_secs(5),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.max_poll_interval = Duration::from_secs(9),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.stop_timeout = Duration::from_secs(61),
            ErrorCode::BoundExceeded,
        ),
        (|c| c.inbox_capacity = 0, ErrorCode::BoundExceeded),
        (|c| c.inbox_bytes = usize::MAX, ErrorCode::BoundExceeded),
        (|c| c.inbox_bytes = 0, ErrorCode::BoundExceeded),
        (
            |c| c.json_limits.max_bytes = 128 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.client.brokers = vec!["127.0.0.1:9093".into()],
            ErrorCode::PolicyDenied,
        ),
        (
            |c| {
                c.restore = RestoreClaim::Checkpoint {
                    snapshot_id: "1".into(),
                }
            },
            ErrorCode::UnsupportedRestore,
        ),
    ];
    for (i, (mutate, code)) in cases.iter().enumerate() {
        let mut c = base.clone();
        mutate(&mut c);
        assert_eq!(c.validate(&p).unwrap_err().code, *code, "case {i}");
    }
    // Reservation: prefetch + 2*fetch + client, checked against half the job.
    assert_eq!(base.reservation(), 512 * 1024 + 2 * 256 * 1024 + 256 * 1024);
    base.check_reservation_budget(ResourceBudget::compact().reservation_bytes)
        .unwrap();
    assert_eq!(
        base.check_reservation_budget(2 * base.reservation() - 1)
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
    let mut huge = base.clone();
    huge.prefetch_bytes = usize::MAX;
    huge.fetch_max_bytes = usize::MAX;
    assert_eq!(huge.reservation(), usize::MAX, "saturates, never wraps");
    assert!(huge.check_reservation_budget(usize::MAX).is_err());

    // Identity binds cluster, topic, group, partition count and format.
    let id = base.identity("cluster-a", 4);
    assert!(id.starts_with("sparrow-kafka-v1:") && id.len() == 17 + 64);
    assert_eq!(id, base.identity("cluster-a", 4));
    assert_ne!(id, base.identity("cluster-b", 4));
    assert_ne!(id, base.identity("cluster-a", 5));
    let mut other = base.clone();
    other.topic = "readings2".into();
    assert_ne!(id, other.identity("cluster-a", 4));
    let mut other = base.clone();
    other.group_id = "sparrow-g2".into();
    assert_ne!(id, other.identity("cluster-a", 4));
    let mut csv = base.clone();
    csv.payload_format = csv_format(CsvRole::Decode);
    assert_ne!(id, csv.identity("cluster-a", 4));
    let mut pb = base.clone();
    pb.payload_format = protobuf_format(CsvRole::Decode);
    assert_ne!(csv.identity("cluster-a", 4), pb.identity("cluster-a", 4));
    let mut pb2 = base.clone();
    pb2.payload_format = crate::protobuf_test_support::format(CsvRole::Decode, |o| {
        o.fields.remove("v");
    });
    assert_ne!(pb.identity("cluster-a", 4), pb2.identity("cluster-a", 4));

    // Delivery-relevant librdkafka settings are explicit.
    let c = base.consumer_config();
    for (k, v) in [
        ("enable.auto.commit", "false"),
        ("enable.auto.offset.store", "false"),
        ("auto.offset.reset", "earliest"),
        ("partition.assignment.strategy", "cooperative-sticky"),
        ("queued.max.messages.kbytes", "512"),
        ("fetch.max.bytes", "262144"),
        ("max.partition.fetch.bytes", "262144"),
        ("isolation.level", "read_committed"),
        ("security.protocol", "plaintext"),
    ] {
        assert_eq!(c.get(k), Some(v), "{k}");
    }
    let mut err = base.clone();
    err.auto_offset_reset = OffsetReset::Error;
    assert_eq!(
        err.consumer_config().get("auto.offset.reset"),
        Some("error")
    );
}

#[test]
fn kafka_sink_config_bounds_and_producer_settings() {
    let p = TargetPolicy::allow("127.0.0.1", 9092);
    let base = KafkaSinkConfig::new(vec!["127.0.0.1:9092".into()], "out");
    base.validate(&p).unwrap();
    type M = fn(&mut KafkaSinkConfig);
    let cases: Vec<(M, ErrorCode)> = vec![
        (|c| c.topic = "a/b".into(), ErrorCode::InvalidArgument),
        (|c| c.acks = KafkaAcks::Leader, ErrorCode::InvalidArgument),
        (
            |c| c.key_column = Some(String::new()),
            ErrorCode::InvalidArgument,
        ),
        (|c| c.max_in_flight = 0, ErrorCode::BoundExceeded),
        (|c| c.max_in_flight = 1025, ErrorCode::BoundExceeded),
        (|c| c.queue_bytes = usize::MAX, ErrorCode::BoundExceeded),
        (|c| c.queue_bytes = 1024, ErrorCode::BoundExceeded),
        (
            |c| c.linger = Duration::from_millis(1001),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.delivery_timeout = Duration::from_millis(999),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.max_message_bytes = usize::MAX,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.max_message_bytes = 600 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.flush_timeout = Duration::from_millis(99),
            ErrorCode::BoundExceeded,
        ),
        (|c| c.outbox_capacity = 0, ErrorCode::BoundExceeded),
        (
            |c| c.client.brokers = vec!["http://127.0.0.1:9092".into()],
            ErrorCode::InvalidArgument,
        ),
        (
            |c| {
                c.restore = RestoreClaim::Checkpoint {
                    snapshot_id: "1".into(),
                }
            },
            ErrorCode::UnsupportedRestore,
        ),
    ];
    for (i, (mutate, code)) in cases.iter().enumerate() {
        let mut c = base.clone();
        mutate(&mut c);
        assert_eq!(c.validate(&p).unwrap_err().code, *code, "case {i}");
    }
    let c = base.producer_config();
    for (k, v) in [
        ("enable.idempotence", "true"),
        ("acks", "all"),
        ("message.timeout.ms", "30000"),
        ("queue.buffering.max.kbytes", "1024"),
        ("queue.buffering.max.messages", "128"),
    ] {
        assert_eq!(c.get(k), Some(v), "{k}");
    }
    assert_eq!(c.get("message.send.max.retries"), None);
    let mut plain = base.clone();
    plain.idempotence = false;
    plain.acks = KafkaAcks::Leader;
    plain.validate(&p).unwrap();
    let c = plain.producer_config();
    assert_eq!(c.get("enable.idempotence"), Some("false"));
    assert_eq!(c.get("acks"), Some("1"));
    assert_eq!(c.get("message.send.max.retries"), Some("0"));
    assert_eq!(base.reservation(), 1024 * 1024 + 2 * 64 * 1024 + 256 * 1024);
    let mut huge = base.clone();
    huge.queue_bytes = usize::MAX;
    assert_eq!(huge.reservation(), usize::MAX);
    assert!(huge.check_reservation_budget(usize::MAX).is_err());
}

// --------------------------------------------------------------- helpers

fn policy(broker: &KafkaSandbox) -> TargetPolicy {
    TargetPolicy::allow("127.0.0.1", broker.port)
}

fn source_config(
    broker: &KafkaSandbox,
    topic: &str,
    group: &str,
    reset: OffsetReset,
) -> KafkaSourceConfig {
    let mut c = KafkaSourceConfig::new(vec![broker.bootstrap()], topic, group, reset, schema());
    c.commit_interval = Duration::from_millis(200);
    c.session_timeout = Duration::from_secs(6);
    c.client.policy_check_interval = Duration::from_secs(1);
    c
}

struct Run {
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    rx: sparrow_io::observed::Receiver<QueuedRow>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<Result<()>>,
}

fn start_source(config: KafkaSourceConfig, policy: &TargetPolicy) -> Run {
    let diag = IoDiagnostics::new();
    let capacity = config.inbox_capacity;
    let source = KafkaSource::bind(config, policy, diag.clone()).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    diag.observation.initialize(&owner).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(source.run_budgeted(tx, cancel.clone(), owner.clone(), 64 * 1024));
    Run {
        diag,
        owner,
        rx,
        cancel,
        task,
    }
}

fn value(q: QueuedRow, owner: &Arc<MemoryOwner>) -> i64 {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        1,
        1 << 16,
    )
    .unwrap();
    q.push_into(&mut builder).unwrap();
    let batch = builder.finish().unwrap();
    match batch.rows()[0].values[1] {
        Scalar::Int64(v) => v,
        ref other => panic!("{other:?}"),
    }
}

impl Run {
    async fn take(&mut self, n: usize) -> Vec<i64> {
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(30), async {
            while got.len() < n {
                let q = self.rx.recv().await.expect("source closed");
                got.push(value(q, &self.owner));
            }
        })
        .await
        .unwrap_or_else(|_| panic!("rows deadline: {got:?} {:?}", self.diag.snapshot().kafka));
        got
    }

    /// Nothing more arrives within `quiet`.
    async fn idle(&mut self, quiet: Duration) {
        if let Ok(Some(q)) = tokio::time::timeout(quiet, self.rx.recv()).await {
            panic!("unexpected row {}", value(q, &self.owner));
        }
    }

    async fn assigned(&self) {
        until(Duration::from_secs(30), || {
            self.diag.snapshot().kafka.source_assigned > 0
        })
        .await;
    }

    async fn stop(self) -> Result<()> {
        self.cancel.cancel();
        let r = tokio::time::timeout(Duration::from_secs(15), self.task)
            .await
            .expect("Kafka source must stop within its deadline")
            .unwrap();
        drop(self.rx);
        r
    }

    async fn failed(self) -> sparrow_model::SparrowError {
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("source should fail")
            .unwrap()
            .unwrap_err()
    }
}

async fn until(deadline: Duration, mut f: impl FnMut() -> bool) {
    tokio::time::timeout(deadline, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition deadline");
}

fn json(v: i64) -> Vec<u8> {
    format!(r#"{{"device_id":"d","v":{v}}}"#).into_bytes()
}

async fn produce(broker: &KafkaSandbox, topic: &str, messages: Vec<(i32, Vec<u8>)>) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker.bootstrap())
        .set("message.max.bytes", "2000000")
        .create()
        .unwrap();
    for (partition, payload) in messages {
        producer
            .send(
                FutureRecord::<(), [u8]>::to(topic)
                    .partition(partition)
                    .payload(&payload[..]),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
}

/// Committed (offset, metadata) per partition, as the group coordinator has it.
async fn committed(
    broker: &KafkaSandbox,
    group: &str,
    topic: &str,
    partitions: i32,
) -> Vec<(i64, String)> {
    let (bootstrap, group, topic) = (broker.bootstrap(), group.to_string(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .create()
            .unwrap();
        let mut list = TopicPartitionList::new();
        for p in 0..partitions {
            list.add_partition(&topic, p);
        }
        consumer
            .committed_offsets(list, Duration::from_secs(10))
            .unwrap()
            .elements()
            .iter()
            .map(|e| match e.offset() {
                Offset::Offset(o) => (o, e.metadata().to_string()),
                _ => (-1, String::new()),
            })
            .collect()
    })
    .await
    .unwrap()
}

/// A plain (non-Sparrow) client commits an offset without identity.
async fn foreign_commit(broker: &KafkaSandbox, group: &str, topic: &str, offset: i64) {
    let (bootstrap, group, topic) = (broker.bootstrap(), group.to_string(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .create()
            .unwrap();
        let mut list = TopicPartitionList::new();
        list.add_partition_offset(&topic, 0, Offset::Offset(offset))
            .unwrap();
        consumer.commit(&list, CommitMode::Sync).unwrap();
    })
    .await
    .unwrap();
}

/// Every record of the topic (key, value), read from the beginning.
async fn read_topic(
    broker: &KafkaSandbox,
    topic: &str,
    partitions: i32,
    n: usize,
) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    let (bootstrap, topic) = (broker.bootstrap(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", "test-reader")
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        let mut list = TopicPartitionList::new();
        for p in 0..partitions {
            list.add_partition_offset(&topic, p, Offset::Beginning)
                .unwrap();
        }
        consumer.assign(&list).unwrap();
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while out.len() < n && std::time::Instant::now() < deadline {
            if let Some(Ok(m)) = consumer.poll(Duration::from_millis(200)) {
                out.push((
                    m.key().map(<[u8]>::to_vec),
                    m.payload().unwrap_or_default().to_vec(),
                ));
            }
        }
        // Nothing beyond n.
        let extra_deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < extra_deadline {
            if let Some(Ok(m)) = consumer.poll(Duration::from_millis(100)) {
                out.push((
                    m.key().map(<[u8]>::to_vec),
                    m.payload().unwrap_or_default().to_vec(),
                ));
            }
        }
        out
    })
    .await
    .unwrap()
}

fn batch(owner: &Arc<MemoryOwner>, from: i64, to: i64) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        (to - from + 1) as usize,
        1 << 16,
    )
    .unwrap();
    for v in from..=to {
        builder
            .push(Row {
                values: vec![Scalar::utf8(format!("k{v}")), Scalar::Int64(v)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

struct SinkRun {
    owner: Arc<MemoryOwner>,
    diag: Arc<IoDiagnostics>,
    tx: sparrow_io::observed::Sender<RowBatch>,
    outbox: Arc<InflightCounter>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

fn start_sink(config: KafkaSinkConfig, policy: &TargetPolicy) -> SinkRun {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    diag.observation.initialize(&owner).unwrap();
    let capacity = config.outbox_capacity;
    let sink = KafkaSink::bind(config, policy, owner.clone(), diag.clone()).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let outbox = Arc::new(InflightCounter::new());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
    SinkRun {
        owner,
        diag,
        tx,
        outbox,
        cancel,
        task,
    }
}

impl SinkRun {
    async fn send(&self, from: i64, to: i64) {
        self.outbox.enqueue();
        self.tx.send(batch(&self.owner, from, to)).await.unwrap();
    }

    async fn settled(&self, acked: u64, failed: u64) {
        until(Duration::from_secs(40), || {
            self.outbox.acked() == acked && self.outbox.failed() == failed
        })
        .await;
    }

    async fn stop(self) {
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(15), self.task)
            .await
            .expect("Kafka sink must stop within its deadline")
            .unwrap();
    }
}

fn health(diag: &IoDiagnostics, source: bool) -> sparrow_model::observation::HealthState {
    let (s, k) = diag.observation.endpoints().expect("endpoints");
    if source {
        s.state
    } else {
        k.state
    }
}

// ----------------------------------------------------------- broker tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_source_commits_admitted_rows_and_resumes_after_restart() {
    let broker = KafkaSandbox::start();
    broker.create_topic("in", 2).await;
    produce(
        &broker,
        "in",
        (1..=6).map(|v| ((v % 2) as i32, json(v))).collect(),
    )
    .await;
    let p = policy(&broker);
    let mut run = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    let mut got = run.take(6).await;
    got.sort_unstable();
    assert_eq!(got, (1..=6).collect::<Vec<_>>());
    run.stop().await.unwrap();
    let commits = committed(&broker, "g", "in", 2).await;
    assert_eq!(commits.iter().map(|c| c.0).sum::<i64>(), 6, "{commits:?}");
    assert!(commits.iter().all(|c| c.1.starts_with("sparrow-kafka-v1:")));

    // Restart resumes from the committed offsets: no redelivery, no loss.
    produce(
        &broker,
        "in",
        (7..=10).map(|v| ((v % 2) as i32, json(v))).collect(),
    )
    .await;
    let mut run = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    let mut got = run.take(4).await;
    got.sort_unstable();
    assert_eq!(got, vec![7, 8, 9, 10]);
    run.idle(Duration::from_secs(1)).await;
    run.stop().await.unwrap();

    // Backpressure: only admitted rows are committed; the rest are
    // redelivered to the next run. The inbox holds 1 row and is not drained.
    produce(&broker, "in", (11..=20).map(|v| (0, json(v))).collect()).await;
    let mut config = source_config(&broker, "in", "g", OffsetReset::Earliest);
    config.inbox_capacity = 1;
    let run = start_source(config, &p);
    until(Duration::from_secs(30), || {
        run.diag.snapshot().kafka.source_backpressure_waits > 0
    })
    .await;
    let admitted = run.diag.snapshot().kafka.source_rows;
    assert_eq!(admitted, 1, "one row fills the inbox");
    run.stop().await.unwrap();
    let commits = committed(&broker, "g", "in", 2).await;
    assert_eq!(
        commits[0].0,
        5 + 1,
        "p0: 5 earlier + 1 admitted: {commits:?}"
    );
    let mut run = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    assert_eq!(run.take(9).await, (12..=20).collect::<Vec<_>>());
    run.idle(Duration::from_secs(1)).await;
    let snap = run.diag.snapshot().kafka;
    run.stop().await.unwrap();
    assert_eq!(snap.source_poison_skipped, 0);
    assert!(snap.source_commit_failed == 0, "{snap:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_source_refuses_foreign_or_mismatched_commits() {
    let broker = KafkaSandbox::start();
    broker.create_topic("in", 2).await;
    produce(&broker, "in", vec![(0, json(1)), (1, json(2))]).await;
    let p = policy(&broker);
    let mut run = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    run.take(2).await;
    run.stop().await.unwrap();

    // Same group, different format: refused before anything is delivered.
    let mut csv = source_config(&broker, "in", "g", OffsetReset::Earliest);
    csv.payload_format = csv_format(CsvRole::Decode);
    let run = start_source(csv, &p);
    let e = run.failed().await;
    assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{e}");

    // A commit written by a plain Kafka client (no identity) is refused.
    foreign_commit(&broker, "foreign", "in", 0).await;
    let run = start_source(
        source_config(&broker, "in", "foreign", OffsetReset::Earliest),
        &p,
    );
    let e = run.failed().await;
    assert_eq!(e.code, ErrorCode::UnsupportedRestore);
    assert!(e.to_string().contains("not this Sparrow source"), "{e}");

    // Partition count changed under the group: refused.
    broker.add_partitions("in", 3).await;
    let run = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    let e = run.failed().await;
    assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{e}");
    // The original commits were not overwritten by the refused runs.
    let commits = committed(&broker, "g", "in", 2).await;
    assert_eq!(commits.iter().map(|c| c.0).collect::<Vec<_>>(), vec![1, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_source_auto_offset_reset_is_explicit() {
    let broker = KafkaSandbox::start();
    broker.create_topic("in", 1).await;
    produce(&broker, "in", (1..=3).map(|v| (0, json(v))).collect()).await;
    let p = policy(&broker);

    let mut run = start_source(
        source_config(&broker, "in", "early", OffsetReset::Earliest),
        &p,
    );
    assert_eq!(run.take(3).await, vec![1, 2, 3]);
    run.stop().await.unwrap();

    let mut run = start_source(
        source_config(&broker, "in", "late", OffsetReset::Latest),
        &p,
    );
    run.assigned().await;
    run.idle(Duration::from_secs(2)).await;
    produce(&broker, "in", vec![(0, json(4))]).await;
    assert_eq!(run.take(1).await, vec![4]);
    run.stop().await.unwrap();

    let run = start_source(
        source_config(&broker, "in", "strict", OffsetReset::Error),
        &p,
    );
    let e = run.failed().await;
    assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{e}");
    // With a committed position `error` resumes normally.
    let mut run = start_source(
        source_config(&broker, "in", "early", OffsetReset::Error),
        &p,
    );
    produce(&broker, "in", vec![(0, json(5))]).await;
    assert_eq!(run.take(2).await, vec![4, 5]);
    run.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_source_poison_policy_skip_or_fail() {
    let broker = KafkaSandbox::start();
    broker.create_topic("in", 1).await;
    produce(
        &broker,
        "in",
        vec![
            (0, json(1)),
            (0, b"not json".to_vec()),
            (0, vec![b' '; 100 * 1024]), // over max_message_bytes (64 KiB)
            (0, json(2)),
        ],
    )
    .await;
    let p = policy(&broker);
    let mut run = start_source(
        source_config(&broker, "in", "skip", OffsetReset::Earliest),
        &p,
    );
    assert_eq!(run.take(2).await, vec![1, 2]);
    until(Duration::from_secs(5), || {
        run.diag.snapshot().kafka.source_poison_skipped == 2
    })
    .await;
    let snap = run.diag.snapshot();
    run.stop().await.unwrap();
    assert_eq!(snap.kafka.source_oversize, 1);
    assert_eq!(snap.decode_errors, 1);
    assert_eq!(
        committed(&broker, "skip", "in", 1).await[0].0,
        4,
        "skipped poison is committed past"
    );

    let mut config = source_config(&broker, "in", "fail", OffsetReset::Earliest);
    config.fail_on_decode = true;
    let mut run = start_source(config, &p);
    assert_eq!(run.take(1).await, vec![1]);
    let e = run.failed().await;
    assert_eq!(e.code, ErrorCode::CodecViolation, "{e}");
    assert_eq!(
        committed(&broker, "fail", "in", 1).await[0].0,
        1,
        "never committed past the poison message"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_source_rebalance_between_two_consumers_loses_and_repeats_nothing() {
    let broker = KafkaSandbox::start();
    broker.create_topic("in", 4).await;
    let p = policy(&broker);
    let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
    let collect = |mut run: Run, seen: Arc<Mutex<Vec<i64>>>| {
        let diag = run.diag.clone();
        let cancel = run.cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    q = run.rx.recv() => match q {
                        Some(q) => seen.lock().unwrap().push(value(q, &run.owner)),
                        None => break,
                    },
                    _ = run.cancel.cancelled() => break,
                }
            }
            run.stop().await
        });
        (diag, cancel, handle)
    };
    let produce_range = |from: i64, to: i64| {
        (from..=to)
            .map(|v| ((v % 4) as i32, json(v)))
            .collect::<Vec<_>>()
    };
    let a = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    a.assigned().await;
    let (a_diag, a_cancel, a_task) = collect(a, seen.clone());
    produce(&broker, "in", produce_range(1, 40)).await;
    until(Duration::from_secs(30), || seen.lock().unwrap().len() == 40).await;

    // B joins: cooperative-sticky moves partitions from A to B. A commits
    // the revoked partitions' processed offsets before they move.
    let b = start_source(source_config(&broker, "in", "g", OffsetReset::Earliest), &p);
    let (b_diag, b_cancel, b_task) = collect(b, seen.clone());
    until(Duration::from_secs(40), || {
        a_diag.snapshot().kafka.source_revoked > 0 && b_diag.snapshot().kafka.source_assigned > 0
    })
    .await;
    produce(&broker, "in", produce_range(41, 80)).await;
    until(Duration::from_secs(30), || seen.lock().unwrap().len() >= 80).await;
    assert!(
        b_diag.snapshot().kafka.source_rows > 0,
        "B received its share"
    );

    // A leaves: its partitions move to B, which resumes from A's commits.
    a_cancel.cancel();
    a_task.await.unwrap().unwrap();
    until(Duration::from_secs(40), || {
        b_diag.snapshot().kafka.source_assigned >= 4
    })
    .await;
    produce(&broker, "in", produce_range(81, 100)).await;
    until(Duration::from_secs(30), || {
        seen.lock().unwrap().len() >= 100
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    b_cancel.cancel();
    b_task.await.unwrap().unwrap();
    let all = seen.lock().unwrap().clone();
    let unique: BTreeSet<i64> = all.iter().copied().collect();
    assert_eq!(unique, (1..=100).collect::<BTreeSet<_>>(), "no loss");
    assert_eq!(all.len(), 100, "no duplicates across graceful rebalances");
    assert_eq!(a_diag.snapshot().kafka.source_lost, 0);
    let commits = committed(&broker, "g", "in", 4).await;
    assert_eq!(commits.iter().map(|c| c.0).sum::<i64>(), 100, "{commits:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_sink_awaits_acks_and_formats_round_trip_through_the_source() {
    let broker = KafkaSandbox::start();
    let p = policy(&broker);
    for (i, (enc, dec)) in [
        (PayloadFormat::Json, PayloadFormat::Json),
        (csv_format(CsvRole::Encode), csv_format(CsvRole::Decode)),
        (
            protobuf_format(CsvRole::Encode),
            protobuf_format(CsvRole::Decode),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let topic = format!("fmt{i}");
        broker.create_topic(&topic, 2).await;
        let mut config = KafkaSinkConfig::new(vec![broker.bootstrap()], &topic);
        config.payload_format = enc;
        config.key_column = Some("device_id".into());
        if i == 1 {
            // Non-idempotent leader acks (no retries) also delivers.
            config.idempotence = false;
            config.acks = KafkaAcks::Leader;
        }
        config.max_in_flight = 2; // exercise the in-flight bound
        let sink = start_sink(config, &p);
        sink.send(1, 5).await;
        sink.send(6, 6).await;
        sink.settled(2, 0).await;
        let snap = sink.diag.snapshot();
        assert_eq!(snap.kafka.sink_acked, 6);
        assert_eq!(snap.kafka.sink_failed, 0);
        sink.stop().await;
        let records = read_topic(&broker, &topic, 2, 6).await;
        assert_eq!(records.len(), 6);
        let keys: BTreeSet<Vec<u8>> = records.iter().map(|r| r.0.clone().unwrap()).collect();
        assert_eq!(
            keys,
            (1..=6).map(|v| format!("k{v}").into_bytes()).collect()
        );

        let mut config = source_config(&broker, &topic, "g", OffsetReset::Earliest);
        config.payload_format = dec;
        let mut run = start_source(config, &p);
        let mut got = run.take(6).await;
        got.sort_unstable();
        assert_eq!(got, (1..=6).collect::<Vec<_>>(), "format {i}");
        run.stop().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_producer_and_consumer_survive_a_broker_restart() {
    let mut broker = KafkaSandbox::start();
    broker.create_topic("t", 2).await;
    let p = policy(&broker);
    let sink = start_sink(KafkaSinkConfig::new(vec![broker.bootstrap()], "t"), &p);
    let mut run = start_source(source_config(&broker, "t", "g", OffsetReset::Earliest), &p);
    run.assigned().await;
    sink.send(1, 5).await;
    sink.settled(1, 0).await;
    let mut got = run.take(5).await;
    // Resume across the restart is from the committed position.
    let mut commits = Vec::new();
    for _ in 0..100 {
        commits = committed(&broker, "g", "t", 2).await;
        if commits.iter().map(|c| c.0.max(0)).sum::<i64>() == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        commits.iter().map(|c| c.0.max(0)).sum::<i64>(),
        5,
        "{commits:?}"
    );

    let broker = tokio::task::spawn_blocking(move || {
        broker.restart();
        broker
    })
    .await
    .unwrap();
    sink.send(6, 10).await;
    sink.settled(2, 0).await;
    got.extend(run.take(5).await);
    got.sort_unstable();
    assert_eq!(got, (1..=10).collect::<Vec<_>>(), "no loss, no duplicates");
    run.idle(Duration::from_secs(1)).await;
    run.stop().await.unwrap();
    sink.stop().await;
    let records = read_topic(&broker, "t", 2, 10).await;
    assert_eq!(records.len(), 10, "idempotent producer wrote each row once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_sink_fails_closed_on_delivery_timeout_and_stop_deadline_bounds_it() {
    let mut broker = KafkaSandbox::start();
    broker.create_topic("t", 1).await;
    let p = policy(&broker);
    let mut config = KafkaSinkConfig::new(vec![broker.bootstrap()], "t");
    config.delivery_timeout = Duration::from_secs(2);
    let sink = start_sink(config.clone(), &p);
    sink.send(1, 1).await;
    sink.settled(1, 0).await;
    let mut slow = config.clone();
    slow.delivery_timeout = Duration::from_secs(120);
    slow.flush_timeout = Duration::from_millis(500);
    let stuck = start_sink(slow, &p);
    stuck.send(1, 1).await;
    stuck.settled(1, 0).await;

    let mut broker = tokio::task::spawn_blocking(move || {
        broker.stop();
        broker
    })
    .await
    .unwrap();
    // Unconfirmed delivery: the batch fails and the sink fails closed.
    sink.send(2, 3).await;
    sink.settled(1, 1).await;
    assert_eq!(
        health(&sink.diag, false),
        sparrow_model::observation::HealthState::Failed
    );
    sink.send(4, 4).await;
    sink.settled(1, 2).await;
    assert!(sink.diag.snapshot().kafka.sink_failed >= 1);
    assert_eq!(
        sink.diag.snapshot().kafka.sink_fatal,
        1,
        "supervisor fails the job"
    );
    sink.stop().await;

    // Stop deadline covers outstanding reports: a sink whose deliveries
    // cannot complete stops within flush_timeout, failing the batch.
    stuck.send(2, 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let outbox = stuck.outbox.clone();
    let diag = stuck.diag.clone();
    let started = std::time::Instant::now();
    stuck.stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(outbox.failed(), 1);
    assert!(diag.snapshot().kafka.sink_discarded_on_close >= 1);

    // Source stop deadline with the broker down: bounded.
    broker = tokio::task::spawn_blocking(move || {
        broker.restart();
        broker
    })
    .await
    .unwrap();
    let mut config = source_config(&broker, "t", "g", OffsetReset::Earliest);
    config.stop_timeout = Duration::from_secs(1);
    let mut run = start_source(config, &p);
    run.take(1).await;
    let broker = tokio::task::spawn_blocking(move || {
        let mut broker = broker;
        broker.stop();
        broker
    })
    .await
    .unwrap();
    let started = std::time::Instant::now();
    run.stop().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    drop(broker);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
async fn kafka_advertised_brokers_outside_the_allowlist_fail_closed() {
    let broker = KafkaSandbox::start();
    broker.create_topic("t", 1).await;
    // Bootstrap through an allowed name; the broker advertises 127.0.0.1,
    // which this policy does not allow.
    let only_localhost = TargetPolicy::allow("localhost", broker.port);
    let mut config = source_config(&broker, "t", "g", OffsetReset::Earliest);
    config.client.brokers = vec![format!("localhost:{}", broker.port)];
    let run = start_source(config, &only_localhost);
    assert_eq!(run.failed().await.code, ErrorCode::PolicyDenied);

    let sink = start_sink(
        KafkaSinkConfig::new(vec![format!("localhost:{}", broker.port)], "t"),
        &only_localhost,
    );
    sink.send(1, 1).await;
    sink.settled(0, 1).await;
    assert_eq!(
        health(&sink.diag, false),
        sparrow_model::observation::HealthState::Failed
    );
    sink.stop().await;
    // Missing topic: the source refuses to start.
    let run = start_source(
        source_config(&broker, "absent", "g", OffsetReset::Earliest),
        &policy(&broker),
    );
    assert_eq!(run.failed().await.code, ErrorCode::InvalidArgument);
}

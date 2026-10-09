//! JetStream Sink unit tests plus opt-in real-broker tests (`#[ignore]`,
//! `SPARROW_NATS_SERVER`, isolated child broker with JetStream enabled).

use super::sink::{JetStreamSink, JetStreamSinkConfig};
use crate::nats::common::{check_stream_name, subject_matches};
use crate::{IoDiagnostics, MapSecretResolver, TargetPolicy};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, InflightCounter, MemoryOwner, ResourceBudget,
    Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_testkit::nats::{jetstream, NatsSandbox};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "event_id", DataType::Utf8, true),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn secrets() -> MapSecretResolver {
    MapSecretResolver::new(HashMap::from([(
        "nats.token".to_string(),
        "t0ken-secret-value".to_string(),
    )]))
}

const LOCAL: &str = "nats://127.0.0.1:4222";

// ------------------------------------------------------------------ unit

#[test]
fn jetstream_sink_config_bounds_semantics_and_redaction() {
    let s = secrets();
    let p = TargetPolicy::allow("127.0.0.1", 4222);
    let base = JetStreamSinkConfig::new(vec![LOCAL.into()], "OUT", "out.rows");
    base.validate(&s, &p).unwrap();
    let caps = JetStreamSinkConfig::capabilities();
    assert_eq!(caps.kind, "jetstream_sink");
    assert_eq!(caps.replay, crate::ReplaySupport::Unsupported);
    assert_eq!(
        caps.delivery,
        sparrow_model::DeliveryGuarantee::CheckpointedAtLeastOnce
    );
    type M = fn(&mut JetStreamSinkConfig);
    let cases: Vec<(M, ErrorCode)> = vec![
        (|c| c.stream = "".into(), ErrorCode::InvalidArgument),
        (|c| c.stream = "a.b".into(), ErrorCode::InvalidArgument),
        (|c| c.stream = "a b".into(), ErrorCode::InvalidArgument),
        (|c| c.stream = "x".repeat(256), ErrorCode::InvalidArgument),
        (|c| c.subject = "out.*".into(), ErrorCode::InvalidArgument),
        (|c| c.subject = "out.>".into(), ErrorCode::InvalidArgument),
        (|c| c.outbox_capacity = 0, ErrorCode::BoundExceeded),
        (
            |c| c.ack_timeout = Duration::from_millis(99),
            ErrorCode::BoundExceeded,
        ),
        (|c| c.max_inflight_acks = 0, ErrorCode::BoundExceeded),
        (|c| c.max_inflight_acks = 257, ErrorCode::BoundExceeded),
        (|c| c.max_retries = 21, ErrorCode::BoundExceeded),
        (
            |c| c.flush_timeout = Duration::from_secs(61),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.msg_id_column = Some(String::new()),
            ErrorCode::InvalidArgument,
        ),
        (
            |c| c.client.reconnect_attempts = 0,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.client.token_secret = Some("nats.token".into()),
            ErrorCode::PolicyDenied,
        ),
        (
            |c| c.client.servers = vec!["nats://10.1.2.3:4222".into()],
            ErrorCode::PolicyDenied,
        ),
    ];
    for (i, (mutate, code)) in cases.iter().enumerate() {
        let mut c = base.clone();
        mutate(&mut c);
        assert_eq!(c.validate(&s, &p).unwrap_err().code, *code, "case {i}");
    }

    // Retained payloads are charged on top of the client buffers.
    let compact = ResourceBudget::compact().reservation_bytes;
    assert!(base.sdk_reservation() > base.client.sdk_reservation());
    base.check_reservation_budget(compact).unwrap();
    let mut big = base.clone();
    big.max_inflight_acks = 64;
    assert_eq!(
        big.check_reservation_budget(compact).unwrap_err().code,
        ErrorCode::BoundExceeded
    );

    // msg_id_column must exist and be utf8/integer.
    let mut with_id = base.clone();
    with_id.msg_id_column = Some("event_id".into());
    with_id.check_schema(&schema()).unwrap();
    with_id.msg_id_column = Some("missing".into());
    assert_eq!(
        with_id.check_schema(&schema()).unwrap_err().code,
        ErrorCode::InvalidSchema
    );
    let floats = Schema::new(
        SchemaId::new(2),
        vec![Field::new(FieldId::new(1), "f", DataType::Float64, false)],
    )
    .unwrap();
    with_id.msg_id_column = Some("f".into());
    assert_eq!(
        with_id.check_schema(&floats).unwrap_err().code,
        ErrorCode::InvalidSchema
    );

    // Token resolved at bind, never printed.
    let mut tls = base.clone();
    tls.client.servers = vec!["tls://127.0.0.1:4222".into()];
    tls.client.token_secret = Some("nats.token".into());
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let sink = JetStreamSink::bind(tls, &s, &p, owner, IoDiagnostics::new()).unwrap();
    let printed = format!("{sink:?}");
    assert!(printed.contains("<redacted>"), "{printed}");
    assert!(!printed.contains("t0ken"), "{printed}");
}

#[test]
fn jetstream_subject_and_stream_name_grammar() {
    assert!(subject_matches("out.>", "out.a.b"));
    assert!(subject_matches("out.*", "out.a"));
    assert!(!subject_matches("out.*", "out.a.b"));
    assert!(!subject_matches("out.>", "out"));
    assert!(subject_matches("out.rows", "out.rows"));
    assert!(!subject_matches("out.rows", "out.rowsx"));
    assert!(subject_matches(">", "x"));
    check_stream_name("OUT_1-a").unwrap();
    for bad in ["", "a.b", "a*", "a>", "a/b", "a\\b", "a b"] {
        assert!(check_stream_name(bad).is_err(), "{bad:?}");
    }
}

// ---------------------------------------------------------- broker harness

fn policy(broker: &NatsSandbox) -> TargetPolicy {
    TargetPolicy::allow("127.0.0.1", broker.port)
}

async fn create_stream(broker: &NatsSandbox, name: &str, subjects: &[&str]) -> jetstream::Context {
    let context = broker.context().await;
    context
        .create_stream(jetstream::stream::Config {
            name: name.into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            max_bytes: 16 * 1024 * 1024,
            storage: jetstream::stream::StorageType::File,
            num_replicas: 1,
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .unwrap();
    context
}

/// (v -> occurrences) and total messages stored in the stream.
async fn stream_rows(context: &jetstream::Context, name: &str) -> (BTreeMap<i64, usize>, u64) {
    let mut stream = context.get_stream(name).await.unwrap();
    let state = stream.info().await.unwrap().state.clone();
    let mut seen = BTreeMap::new();
    for seq in state.first_sequence..=state.last_sequence {
        if state.messages == 0 {
            break;
        }
        let m = stream.get_raw_message(seq).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
        *seen.entry(v["v"].as_i64().unwrap()).or_insert(0) += 1;
    }
    (seen, state.messages)
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
                values: vec![Scalar::utf8(format!("e-{v}")), Scalar::Int64(v)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

async fn until(deadline: Duration, mut f: impl FnMut() -> bool) {
    tokio::time::timeout(deadline, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition deadline");
}

struct Running {
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    tx: Option<sparrow_io::observed::Sender<RowBatch>>,
    outbox: Arc<InflightCounter>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

fn start(broker: &NatsSandbox, mutate: impl FnOnce(&mut JetStreamSinkConfig)) -> Running {
    let mut config = JetStreamSinkConfig::new(vec![broker.url()], "OUT", "out.rows");
    config.client.reconnect_attempts = 20;
    mutate(&mut config);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let sink = JetStreamSink::bind(
        config.clone(),
        &secrets(),
        &policy(broker),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(config.outbox_capacity);
    let cancel = CancellationToken::new();
    let outbox = Arc::new(InflightCounter::new());
    let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
    Running {
        diag,
        owner,
        tx: Some(tx),
        outbox,
        cancel,
        task,
    }
}

impl Running {
    async fn ready(&self) {
        until(Duration::from_secs(10), || {
            self.diag.snapshot().jetstream_sink_sessions == 1
        })
        .await;
    }
    async fn send(&self, from: i64, to: i64) {
        self.outbox.enqueue();
        self.tx
            .as_ref()
            .unwrap()
            .send(batch(&self.owner, from, to))
            .await
            .unwrap();
    }
    async fn settled(&self, batches: u64) {
        until(Duration::from_secs(30), || {
            self.outbox.acked() + self.outbox.failed() >= batches
        })
        .await;
    }
    async fn finish(mut self) -> (Arc<IoDiagnostics>, Arc<InflightCounter>, bool) {
        drop(self.tx.take());
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("JetStream sink must stop within its bounded budgets")
            .unwrap();
        let cancelled = self.cancel.is_cancelled();
        until(Duration::from_secs(10), || {
            self.owner.usage().reservation_bytes == 0
        })
        .await;
        (self.diag, self.outbox, cancelled)
    }
}

// ----------------------------------------------------------- broker tests

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_acks_batches_only_after_pub_acks() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |_| {});
    sink.ready().await;
    for i in 0..10 {
        sink.send(i * 10 + 1, i * 10 + 10).await;
    }
    sink.settled(10).await;
    assert_eq!(sink.outbox.acked(), 10);
    let (diag, outbox, cancelled) = sink.finish().await;
    let (seen, messages) = stream_rows(&context, "OUT").await;
    assert_eq!(messages, 100);
    assert_eq!(seen.len(), 100);
    assert!(seen.values().all(|n| *n == 1));
    let s = diag.snapshot();
    assert_eq!(
        (s.jetstream_sink_acked, s.jetstream_sink_batches),
        (100, 10)
    );
    assert_eq!(s.jetstream_sink_fatal, 0, "{s:?}");
    assert_eq!(s.jetstream_sink_inflight, 0);
    assert_eq!(outbox.failed(), 0);
    assert!(!cancelled, "EOF is a clean end");
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_refuses_missing_stream_unbound_subject_and_never_creates() {
    let broker = NatsSandbox::start().await;
    // Missing stream: fatal at start, job cancelled, nothing created.
    let sink = start(&broker, |c| c.max_retries = 1);
    let (diag, _, cancelled) = sink.finish().await;
    assert!(cancelled, "a missing stream fails the job");
    assert_eq!(diag.snapshot().jetstream_sink_fatal, 1);
    assert_eq!(diag.snapshot().jetstream_sink_sessions, 0);
    let context = broker.context().await;
    assert!(
        context.get_stream("OUT").await.is_err(),
        "sink must never auto-create"
    );
    // Stream exists but does not bind the subject.
    create_stream(&broker, "OUT", &["other.>"]).await;
    let sink = start(&broker, |c| c.max_retries = 1);
    let (diag, _, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!(diag.snapshot().jetstream_sink_fatal, 1);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_ack_timeout_retries_dedups_and_then_fails_closed() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    // Short pause: PubAcks time out, retries with the same Nats-Msg-Id land
    // once the broker resumes; the stream window drops the duplicates.
    let sink = start(&broker, |c| {
        c.ack_timeout = Duration::from_millis(200);
        c.max_retries = 10;
        c.msg_id_column = Some("event_id".into());
    });
    sink.ready().await;
    broker.pause();
    sink.send(1, 5).await;
    until(Duration::from_secs(10), || {
        sink.diag.snapshot().jetstream_sink_ack_timeouts >= 1
    })
    .await;
    assert_eq!(sink.outbox.acked(), 0, "no ack before PubAck");
    broker.resume();
    sink.settled(1).await;
    assert_eq!(sink.outbox.acked(), 1);
    let (diag, _, cancelled) = sink.finish().await;
    assert!(!cancelled);
    let s = diag.snapshot();
    assert!(s.jetstream_sink_retries >= 1, "{s:?}");
    let (seen, messages) = stream_rows(&context, "OUT").await;
    assert_eq!(messages, 5, "Nats-Msg-Id dedups retried publishes");
    assert_eq!(
        seen.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );

    // Long pause: retries are bounded; the batch is failed, never acked,
    // and the job is cancelled (fail closed).
    let sink = start(&broker, |c| {
        c.ack_timeout = Duration::from_millis(100);
        c.max_retries = 1;
    });
    sink.ready().await;
    broker.pause();
    sink.send(10, 12).await;
    sink.settled(1).await;
    broker.resume();
    let (diag, outbox, cancelled) = sink.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert!(cancelled);
    let s = diag.snapshot();
    assert_eq!(s.jetstream_sink_fatal, 1, "{s:?}");
    assert!(s.jetstream_sink_failed >= 1 && s.jetstream_sink_ack_timeouts >= 2);
}

async fn restart_mid_publish(msg_id: bool) {
    let mut broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| {
        c.max_retries = 15;
        c.ack_timeout = Duration::from_millis(500);
        if msg_id {
            c.msg_id_column = Some("event_id".into());
        }
    });
    sink.ready().await;
    for i in 0..20 {
        sink.send(i * 5 + 1, i * 5 + 5).await;
    }
    sink.settled(20).await;
    // Freeze the broker with publishes in flight, then kill and relaunch
    // it: those PubAcks never arrive and the sink must retry them.
    broker.pause();
    for i in 20..24 {
        sink.send(i * 5 + 1, i * 5 + 5).await;
    }
    until(Duration::from_secs(10), || {
        sink.diag.snapshot().jetstream_sink_ack_timeouts >= 1
    })
    .await;
    broker.restart().await;
    for i in 24..40 {
        sink.send(i * 5 + 1, i * 5 + 5).await;
    }
    sink.settled(40).await;
    assert_eq!(sink.outbox.acked(), 40, "{:?}", sink.diag.snapshot());
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(!cancelled);
    assert_eq!(outbox.failed(), 0);
    let s = diag.snapshot();
    assert!(s.jetstream_sink_reconnects >= 1, "{s:?}");
    assert!(s.jetstream_sink_retries >= 1, "{s:?}");
    eprintln!(
        "restart msg_id={msg_id}: retries={} ack_timeouts={} duplicates={} reconnects={}",
        s.jetstream_sink_retries,
        s.jetstream_sink_ack_timeouts,
        s.jetstream_sink_duplicates,
        s.jetstream_sink_reconnects
    );
    let context = if msg_id {
        broker.context().await
    } else {
        context
    };
    let (seen, messages) = stream_rows(&context, "OUT").await;
    assert_eq!(seen.len(), 200, "no loss across the restart");
    assert_eq!(seen.keys().next(), Some(&1));
    assert_eq!(seen.keys().last(), Some(&200));
    if msg_id {
        assert_eq!(messages, 200, "msg-id dedup: exactly one copy each");
    } else {
        // Duplicates are allowed (lost PubAck → retry) and visible.
        assert!(messages >= 200);
        assert_eq!(
            messages - 200,
            seen.values().map(|n| (*n - 1) as u64).sum::<u64>()
        );
    }
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_server_restart_mid_publish_retries_without_loss() {
    restart_mid_publish(false).await;
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_server_restart_with_msg_id_has_no_duplicates() {
    restart_mid_publish(true).await;
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_flushes_queued_batches_on_stop() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |_| {});
    sink.ready().await;
    // Queue without waiting, then stop: shutdown confirms what is queued.
    for i in 0..8 {
        sink.outbox.enqueue();
        sink.tx
            .as_ref()
            .unwrap()
            .try_send(batch(&sink.owner, i * 3 + 1, i * 3 + 3))
            .unwrap();
    }
    sink.cancel.cancel();
    let (diag, outbox, _) = sink.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (8, 0));
    let s = diag.snapshot();
    assert_eq!(s.jetstream_sink_discarded_on_close, 0);
    assert_eq!(s.jetstream_sink_fatal, 0, "{s:?}");
    let (seen, messages) = stream_rows(&context, "OUT").await;
    assert_eq!((seen.len(), messages), (24, 24));
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_oversize_row_fails_closed() {
    let broker = NatsSandbox::start().await;
    create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| c.client.max_payload_bytes = 1024);
    sink.ready().await;
    sink.outbox.enqueue();
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        sink.owner.clone(),
        CreditKind::Reservation,
        1,
        1 << 16,
    )
    .unwrap();
    builder
        .push(Row {
            values: vec![Scalar::utf8("x".repeat(2000)), Scalar::Int64(1)],
        })
        .unwrap();
    sink.tx
        .as_ref()
        .unwrap()
        .send(builder.finish().unwrap())
        .await
        .unwrap();
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!(outbox.failed(), 1);
    let s = diag.snapshot();
    assert_eq!(
        (s.jetstream_sink_dropped_oversize, s.jetstream_sink_fatal),
        (1, 1)
    );
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_csv_stores_one_record_per_message() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| {
        c.payload_format = sparrow_formats::PayloadFormat::csv(
            sparrow_formats::CsvOptions::default()
                .compile(sparrow_formats::CsvRole::Encode)
                .unwrap(),
        );
    });
    sink.ready().await;
    sink.send(1, 2).await;
    sink.settled(1).await;
    let (diag, outbox, _) = sink.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0));
    let stream = context.get_stream("OUT").await.unwrap();
    let mut payloads = Vec::new();
    for seq in 1..=2 {
        let m = stream.get_raw_message(seq).await.unwrap();
        payloads.push(String::from_utf8(m.payload.to_vec()).unwrap());
    }
    assert_eq!(payloads, ["event_id,v\ne-1,1\n", "event_id,v\ne-2,2\n"]);
    assert_eq!(diag.snapshot().csv_encode_errors, 0);
}

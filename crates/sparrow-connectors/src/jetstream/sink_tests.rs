//! JetStream Sink unit tests plus opt-in real-broker tests (`#[ignore]`,
//! `SPARROW_NATS_SERVER`, isolated child broker with JetStream enabled).

use super::sink::{JetStreamSink, JetStreamSinkConfig};
use crate::nats::common::{check_stream_name, subject_matches};
use crate::{IoDiagnostics, MapSecretResolver, TargetPolicy};
use sparrow_model::{
    CreditKind, DataType, DynamicValue, ErrorCode, Field, FieldId, InflightCounter, MemoryOwner,
    ResourceBudget, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_testkit::nats::{jetstream, NatsSandbox};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
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
    assert_eq!(
        base.client.capacity,
        JetStreamSinkConfig::DEFAULT_CLIENT_CAPACITY
    );
    assert_eq!(base.client.capacity, 4);
    assert_eq!(base.max_inflight_acks, 8);
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
    // Reservation estimation must not overflow before config validation.
    let mut saturated = base.clone();
    saturated.max_inflight_acks = usize::MAX;
    assert_eq!(saturated.sdk_reservation(), usize::MAX);
    assert_eq!(
        saturated
            .check_reservation_budget(compact)
            .unwrap_err()
            .code,
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

#[test]
fn jetstream_sink_encode_acquires_scratch_before_id_headers_or_codec() {
    let budget = ResourceBudget::compact();
    let owner = MemoryOwner::new(budget);
    let diag = IoDiagnostics::new();
    let mut config = JetStreamSinkConfig::new(vec![LOCAL.into()], "OUT", "out.rows");
    config.msg_id_column = Some("event_id".into());
    let sink = JetStreamSink::bind(
        config,
        &secrets(),
        &TargetPolicy::allow("127.0.0.1", 4222),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let row = Row {
        values: vec![Scalar::utf8("invalid\nid"), Scalar::Int64(1)],
    };
    let held = owner
        .acquire(CreditKind::Reservation, budget.reservation_bytes - 8192)
        .unwrap();
    let error = sink.encode(&schema(), &row, Some(0), 1024).unwrap_err();
    assert_eq!(error.code, ErrorCode::ResourceExhausted);
    assert_eq!(
        diag.snapshot().jetstream_sink_dropped_bad,
        0,
        "id validation must not run without scratch credit"
    );
    assert_eq!(diag.snapshot().jetstream_sink_dropped_oversize, 0);
    assert_eq!(owner.usage().reservation_bytes, held.bytes());
    drop(held);
    // With credit available, the same row reaches id validation and fails
    // there instead; both failure paths return the temporary scratch lease.
    assert_eq!(
        sink.encode(&schema(), &row, Some(0), 1024)
            .unwrap_err()
            .code,
        ErrorCode::InvalidArgument
    );
    assert_eq!(diag.snapshot().jetstream_sink_dropped_bad, 1);
    assert_eq!(owner.usage().reservation_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(owner.accounting_errors_total(), 0);
}

#[test]
fn jetstream_sink_wide_bytes_dynamic_codec_scratch_is_charged_and_returned() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let config = JetStreamSinkConfig::new(vec![LOCAL.into()], "OUT", "out.rows");
    let retained = owner
        .acquire(CreditKind::Reservation, config.sdk_reservation())
        .unwrap();
    let sink = JetStreamSink::bind(
        config,
        &secrets(),
        &TargetPolicy::allow("127.0.0.1", 4222),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let mut fields = Vec::new();
    let mut values = Vec::new();
    for index in 0..15u16 {
        fields.push(Field::new(
            FieldId::new(index + 1),
            format!("f{index}"),
            DataType::Int64,
            false,
        ));
        values.push(Scalar::Int64(i64::from(index)));
    }
    fields.push(Field::new(
        FieldId::new(16),
        "bytes",
        DataType::Bytes,
        false,
    ));
    values.push(Scalar::Bytes(Arc::from(b"abc".as_slice())));
    fields.push(Field::new(
        FieldId::new(17),
        "dynamic",
        DataType::Dynamic,
        false,
    ));
    values.push(Scalar::Dynamic(DynamicValue::object(vec![(
        "message",
        DynamicValue::utf8("quoted \" value\n"),
    )])));
    let output = Schema::new(SchemaId::new(2), fields).unwrap();
    let row = Row { values };
    let before = owner.usage().physical_bytes;
    let (body, id) = sink.encode(&output, &row, None, 1024).unwrap();
    assert!(id.is_none());
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(decoded["bytes"], "YWJj");
    assert_eq!(decoded["dynamic"]["message"], "quoted \" value\n");
    assert_eq!(decoded["f14"], 14);
    assert!(body.len() <= 1024);
    assert_eq!(
        owner.usage().physical_bytes,
        before,
        "scratch must be released; encoded bytes use the existing retained reservation"
    );
    assert!(
        owner.usage().peak_physical_bytes > before,
        "the temporary codec scratch must have been charged"
    );
    drop(body);
    drop(retained);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(owner.accounting_errors_total(), 0);
    assert_eq!(diag.snapshot().jetstream_sink_dropped_bad, 0);
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

fn bound_sink(
    broker: &NatsSandbox,
    mutate: impl FnOnce(&mut JetStreamSinkConfig),
) -> (JetStreamSink, Arc<MemoryOwner>, Arc<IoDiagnostics>) {
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
    (sink, owner, diag)
}

fn start(broker: &NatsSandbox, mutate: impl FnOnce(&mut JetStreamSinkConfig)) -> Running {
    let (sink, owner, diag) = bound_sink(broker, mutate);
    let (tx, rx) = sparrow_io::observed::channel(sink.config.outbox_capacity);
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

async fn start_prepared(
    broker: &NatsSandbox,
    mutate: impl FnOnce(&mut JetStreamSinkConfig),
) -> Running {
    let (sink, owner, diag) = bound_sink(broker, mutate);
    let (tx, rx) = sparrow_io::observed::channel(sink.config.outbox_capacity);
    let cancel = CancellationToken::new();
    let prepared = sink
        .prepare_aligned(cancel.clone(), Arc::new(()))
        .await
        .unwrap()
        .expect("prepared File/Limits sink");
    assert!(prepared.identity().belongs_to(&owner));
    let outbox = Arc::new(InflightCounter::new());
    let task = tokio::spawn(prepared.run(rx, cancel.clone(), Some(outbox.clone())));
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
        self.send_batch(batch(&self.owner, from, to)).await;
    }
    async fn send_batch(&self, batch: RowBatch) {
        self.outbox.enqueue();
        self.tx.as_ref().unwrap().send(batch).await.unwrap();
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
        assert_eq!(self.owner.usage().physical_bytes, 0);
        assert_eq!(self.owner.accounting_errors_total(), 0);
        (self.diag, self.outbox, cancelled)
    }
}

// ----------------------------------------------------------- broker tests

struct ProbeGuard(Arc<AtomicBool>);
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_prepared_probe_close_joins_the_original_sdk_and_guard() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let (sink, owner, diag) = bound_sink(&broker, |_| {});
    let released = Arc::new(AtomicBool::new(false));
    let guard = Arc::new(ProbeGuard(released.clone()));
    let weak = Arc::downgrade(&guard);
    let prepared = sink
        .prepare_aligned(CancellationToken::new(), guard.clone())
        .await
        .unwrap()
        .unwrap();
    let identity = prepared.identity();
    assert!(identity.belongs_to(&owner));
    assert_eq!(identity.identity().stream, "OUT");
    assert_eq!(identity.identity().subject, "out.rows");
    assert_eq!(identity.identity().encoding, sparrow_io::SinkEncoding::Json);
    assert_eq!(
        identity.identity().created_nanos,
        context
            .get_stream("OUT")
            .await
            .unwrap()
            .cached_info()
            .created
            .unix_timestamp_nanos()
    );
    assert_eq!(diag.snapshot().jetstream_sink_sessions, 1);
    assert_eq!(
        diag.snapshot().jetstream_sink_acked,
        0,
        "probe publishes no source rows"
    );
    assert_eq!(stream_rows(&context, "OUT").await.1, 0);
    drop(guard);
    assert!(
        weak.upgrade().is_some(),
        "the SDK must own the lifecycle guard"
    );
    assert!(!released.load(Ordering::SeqCst));
    prepared.close().await.unwrap();
    drop(identity);
    until(Duration::from_secs(10), || {
        owner.usage().physical_bytes == 0 && weak.upgrade().is_none()
    })
    .await;
    assert!(released.load(Ordering::SeqCst));
    assert_eq!(owner.accounting_errors_total(), 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_prepared_csv_identity_binds_effective_encode_options() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    for (options, expected) in [
        (
            sparrow_formats::CsvOptions::default(),
            sparrow_io::CsvEncodeIdentity::new(b',', b'"', true, "").unwrap(),
        ),
        (
            sparrow_formats::CsvOptions {
                delimiter: ";".into(),
                quote: "'".into(),
                header: false,
                null_value: "NIL".into(),
                ..Default::default()
            },
            sparrow_io::CsvEncodeIdentity::new(b';', b'\'', false, "NIL").unwrap(),
        ),
    ] {
        let (sink, owner, diag) = bound_sink(&broker, |config| {
            config.payload_format = sparrow_formats::PayloadFormat::csv(
                options.compile(sparrow_formats::CsvRole::Encode).unwrap(),
            );
        });
        let prepared = sink
            .prepare_aligned(CancellationToken::new(), Arc::new(()))
            .await
            .unwrap()
            .unwrap();
        let identity = prepared.identity();
        assert!(identity.belongs_to(&owner));
        assert_eq!(
            identity.identity().encoding,
            sparrow_io::SinkEncoding::Csv(expected)
        );
        assert_eq!(identity.identity().stream, "OUT");
        assert_eq!(identity.identity().subject, "out.rows");
        assert_eq!(
            identity.identity().created_nanos,
            context
                .get_stream("OUT")
                .await
                .unwrap()
                .cached_info()
                .created
                .unix_timestamp_nanos()
        );
        assert_eq!(
            diag.snapshot().jetstream_sink_acked,
            0,
            "identity probe must not publish"
        );
        assert_eq!(stream_rows(&context, "OUT").await.1, 0);
        prepared.close().await.unwrap();
        drop(identity);
        until(Duration::from_secs(10), || {
            owner.usage().physical_bytes == 0
        })
        .await;
        assert_eq!(owner.accounting_errors_total(), 0);
    }
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_prepared_run_reuses_the_probe_session() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start_prepared(&broker, |_| {}).await;
    sink.send(1, 5).await;
    sink.settled(1).await;
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(!cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0));
    assert_eq!(
        diag.snapshot().jetstream_sink_sessions,
        1,
        "run must not reopen a second SDK session"
    );
    assert_eq!(diag.snapshot().jetstream_sink_acked, 5);
    assert_eq!(stream_rows(&context, "OUT").await.1, 5);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_prepared_refuses_memory_interest_and_no_ack_streams() {
    let broker = NatsSandbox::start().await;
    let context = broker.context().await;
    for (storage, retention, no_ack, expected) in [
        (
            jetstream::stream::StorageType::Memory,
            jetstream::stream::RetentionPolicy::Limits,
            false,
            ErrorCode::UnsupportedRestore,
        ),
        (
            jetstream::stream::StorageType::File,
            jetstream::stream::RetentionPolicy::Interest,
            false,
            ErrorCode::UnsupportedRestore,
        ),
        (
            jetstream::stream::StorageType::File,
            jetstream::stream::RetentionPolicy::Limits,
            true,
            ErrorCode::InvalidArgument,
        ),
    ] {
        context
            .create_stream(jetstream::stream::Config {
                name: "OUT".into(),
                subjects: vec!["out.>".into()],
                storage,
                retention,
                no_ack,
                max_bytes: 16 * 1024 * 1024,
                ..Default::default()
            })
            .await
            .unwrap();
        let (sink, owner, diag) = bound_sink(&broker, |_| {});
        let error = sink
            .prepare_aligned(CancellationToken::new(), Arc::new(()))
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(diag.snapshot().jetstream_sink_sessions, 0);
        assert_eq!(diag.snapshot().jetstream_sink_acked, 0);
        assert_eq!(diag.snapshot().jetstream_sink_fatal, 1);
        until(Duration::from_secs(10), || {
            owner.usage().physical_bytes == 0
        })
        .await;
        assert_eq!(owner.accounting_errors_total(), 0);
        assert_eq!(stream_rows(&context, "OUT").await.1, 0);
        context.delete_stream("OUT").await.unwrap();
    }
    // Non-aligned PubAck publishing deliberately keeps its weaker contract.
    context
        .create_stream(jetstream::stream::Config {
            name: "OUT".into(),
            subjects: vec!["out.>".into()],
            storage: jetstream::stream::StorageType::Memory,
            max_bytes: 16 * 1024 * 1024,
            ..Default::default()
        })
        .await
        .unwrap();
    let sink = start(&broker, |_| {});
    sink.ready().await;
    sink.send(1, 1).await;
    sink.settled(1).await;
    let (_, outbox, cancelled) = sink.finish().await;
    assert!(!cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0));
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_prepared_rejects_same_named_recreation_before_receipt() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let old_created = context
        .get_stream("OUT")
        .await
        .unwrap()
        .cached_info()
        .created;
    let sink = start_prepared(&broker, |_| {}).await;
    context.delete_stream("OUT").await.unwrap();
    create_stream(&broker, "OUT", &["out.>"]).await;
    assert_ne!(
        context
            .get_stream("OUT")
            .await
            .unwrap()
            .cached_info()
            .created,
        old_created
    );
    sink.send(1, 1).await;
    sink.settled(1).await;
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert_eq!(diag.snapshot().jetstream_sink_acked, 0);
    assert_eq!(diag.snapshot().jetstream_sink_fatal, 1);
    assert_eq!(diag.snapshot().jetstream_sink_retries, 0);
    assert_eq!(stream_rows(&context, "OUT").await.1, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_prepared_rejects_config_drift_before_receipt() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start_prepared(&broker, |_| {}).await;
    let stream = context.get_stream("OUT").await.unwrap();
    let created = stream.cached_info().created;
    let mut changed = stream.cached_info().config.clone();
    changed.max_bytes += 1024;
    context.update_stream(changed).await.unwrap();
    assert_eq!(
        context
            .get_stream("OUT")
            .await
            .unwrap()
            .cached_info()
            .created,
        created
    );
    sink.send(1, 1).await;
    sink.settled(1).await;
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert_eq!(diag.snapshot().jetstream_sink_acked, 0);
    assert_eq!(diag.snapshot().jetstream_sink_fatal, 1);
    assert_eq!(stream_rows(&context, "OUT").await.1, 0);
}

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
async fn jetstream_sink_stop_bounds_the_inflight_batch_and_queued_receipts() {
    let broker = NatsSandbox::start().await;
    create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| {
        c.ack_timeout = Duration::from_secs(5);
        c.max_retries = 20;
        c.max_inflight_acks = 1;
        c.flush_timeout = Duration::from_millis(100);
    });
    sink.ready().await;
    broker.pause();
    sink.send(1, 1).await;
    until(Duration::from_secs(2), || {
        sink.diag.snapshot().jetstream_sink_inflight == 1
    })
    .await;
    sink.send(2, 2).await;
    assert_eq!(sink.outbox.pending(), 2);
    sink.cancel.cancel();
    // A 100ms flush must not wait for even the first 5s PubAck timeout,
    // let alone give every retry / queued batch a fresh flush budget.
    until(Duration::from_secs(2), || sink.outbox.failed() == 2).await;
    let snapshot = sink.diag.snapshot();
    assert_eq!((sink.outbox.acked(), sink.outbox.pending()), (0, 0));
    assert_eq!(snapshot.jetstream_sink_inflight, 0, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_fatal, 1, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_failed, 1, "{snapshot:?}");
    assert_eq!(
        snapshot.jetstream_sink_discarded_on_close, 1,
        "{snapshot:?}"
    );
    assert_eq!(snapshot.jetstream_sink_retries, 0, "{snapshot:?}");
    // Closing is still real SDK lifecycle work, not an early ledger refund.
    // Some SDKs can flush their socket and exit without a broker response.
    if !sink.task.is_finished() {
        assert!(sink.owner.usage().reservation_bytes > 0);
    }
    broker.resume();
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (0, 2));
    assert_eq!(diag.snapshot().jetstream_sink_inflight, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_subject_rebinding_fails_without_retry_or_foreign_publish() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| c.max_retries = 20);
    sink.ready().await;
    context.delete_stream("OUT").await.unwrap();
    create_stream(&broker, "OTHER", &["out.>"]).await;
    sink.send(1, 1).await;
    until(Duration::from_secs(2), || {
        sink.outbox.acked() + sink.outbox.failed() == 1
    })
    .await;
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    let snapshot = diag.snapshot();
    assert_eq!(snapshot.jetstream_sink_acked, 0, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_fatal, 1, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_failed, 1, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_retries, 0, "{snapshot:?}");
    assert_eq!(stream_rows(&context, "OTHER").await.1, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_refuses_no_ack_stream_before_publishing() {
    let broker = NatsSandbox::start().await;
    broker
        .context()
        .await
        .create_stream(jetstream::stream::Config {
            name: "OUT".into(),
            subjects: vec!["out.>".into()],
            no_ack: true,
            storage: jetstream::stream::StorageType::File,
            max_bytes: 16 * 1024 * 1024,
            ..Default::default()
        })
        .await
        .unwrap();
    let sink = start(&broker, |c| c.max_retries = 20);
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (0, 0));
    let snapshot = diag.snapshot();
    assert_eq!(snapshot.jetstream_sink_sessions, 0, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_fatal, 1, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_ack_timeouts, 0, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_retries, 0, "{snapshot:?}");
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_bounded_encoder_rejects_json_escape_expansion() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| c.client.max_payload_bytes = 1024);
    sink.ready().await;
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        sink.owner.clone(),
        CreditKind::Reservation,
        1,
        1 << 16,
    )
    .unwrap();
    // Only 300 source bytes, but each NUL expands to six JSON bytes.
    builder
        .push(Row {
            values: vec![Scalar::utf8("\0".repeat(300)), Scalar::Int64(1)],
        })
        .unwrap();
    sink.send_batch(builder.finish().unwrap()).await;
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    let snapshot = diag.snapshot();
    assert_eq!(snapshot.jetstream_sink_dropped_oversize, 1, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_dropped_bad, 0, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_acked, 0, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_fatal, 1, "{snapshot:?}");
    assert_eq!(stream_rows(&context, "OUT").await.1, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_payload_limit_includes_expected_stream_and_msg_id_headers() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let sink = start(&broker, |c| {
        c.client.max_payload_bytes = 1024;
        c.msg_id_column = Some("event_id".into());
    });
    sink.ready().await;
    let id = "i".repeat(super::sink::MAX_MSG_ID_BYTES);
    let header_bytes =
        format!("NATS/1.0\r\nNats-Expected-Stream: OUT\r\nNats-Msg-Id: {id}\r\n\r\n").len();
    let empty_body = serde_json::json!({"event_id": id, "v": 1, "blob": ""});
    let blob_bytes = 1024 - header_bytes - empty_body.to_string().len();
    let mut fields = schema().fields;
    fields.push(Field::new(FieldId::new(3), "blob", DataType::Utf8, false));
    let output = Arc::new(Schema::new(SchemaId::new(2), fields).unwrap());
    let owner = sink.owner.clone();
    let make = |extra: usize| {
        let mut builder = RowBatchBuilder::new(
            output.clone(),
            owner.clone(),
            CreditKind::Reservation,
            1,
            1 << 16,
        )
        .unwrap();
        builder
            .push(Row {
                values: vec![
                    Scalar::utf8(&id),
                    Scalar::Int64(1),
                    Scalar::utf8("x".repeat(blob_bytes + extra)),
                ],
            })
            .unwrap();
        builder.finish().unwrap()
    };
    sink.send_batch(make(0)).await;
    sink.settled(1).await;
    assert_eq!((sink.outbox.acked(), sink.outbox.failed()), (1, 0));
    let stream = context.get_stream("OUT").await.unwrap();
    assert_eq!(
        stream.get_raw_message(1).await.unwrap().payload.len() + header_bytes,
        1024
    );
    // The body still fits alone, but one extra byte no longer fits with the
    // exact Expected-Stream plus msg-id header block.
    sink.send_batch(make(1)).await;
    let (diag, outbox, cancelled) = sink.finish().await;
    assert!(cancelled);
    assert_eq!((outbox.acked(), outbox.failed()), (1, 1));
    let snapshot = diag.snapshot();
    assert_eq!(snapshot.jetstream_sink_dropped_oversize, 1, "{snapshot:?}");
    assert_eq!(snapshot.jetstream_sink_acked, 1, "{snapshot:?}");
    assert_eq!(stream_rows(&context, "OUT").await.1, 1);
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

/// `event_id` (nullable) <- oneof `text` (presence), `v` <- `seq`.
fn protobuf_format() -> sparrow_formats::PayloadFormat {
    crate::protobuf_test_support::format(sparrow_formats::CsvRole::Encode, |o| {
        o.fields = [("event_id", "text"), ("v", "seq")]
            .iter()
            .map(|(c, p)| (c.to_string(), p.to_string()))
            .collect();
    })
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn jetstream_sink_protobuf_stores_one_message_per_row_and_refuses_aligned() {
    let broker = NatsSandbox::start().await;
    let context = create_stream(&broker, "OUT", &["out.>"]).await;
    let (sink, _owner, _diag) = bound_sink(&broker, |c| c.payload_format = protobuf_format());
    let err = sink
        .prepare_aligned(CancellationToken::new(), Arc::new(()))
        .await
        .expect_err("aligned protobuf has no sink identity");
    assert_eq!(err.code, ErrorCode::UnsupportedRestore);

    let sink = start(&broker, |c| c.payload_format = protobuf_format());
    sink.ready().await;
    sink.send(1, 2).await;
    sink.settled(1).await;
    let (diag, outbox, _) = sink.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0));
    let stream = context.get_stream("OUT").await.unwrap();
    for seq in 1..=2u64 {
        let m = stream.get_raw_message(seq).await.unwrap();
        // text = 18 (tag 0x92 0x01), seq = 2 (tag 0x10).
        let mut want = vec![0x10, seq as u8, 0x92, 0x01, 3];
        want.extend_from_slice(format!("e-{seq}").as_bytes());
        assert_eq!(m.payload.to_vec(), want);
    }
    assert_eq!(diag.snapshot().protobuf_encode_errors, 0);
}

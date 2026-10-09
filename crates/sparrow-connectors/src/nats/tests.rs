//! NATS Core unit tests plus opt-in real-broker tests. Broker tests follow
//! the K2 convention: `#[ignore]`, `SPARROW_NATS_SERVER` points at the pinned
//! official nats-server binary, and each test owns an isolated child broker
//! (`sparrow_testkit::nats::NatsSandbox`); no external service is touched.

use super::*;
use crate::{IoDiagnostics, MapSecretResolver, TargetPolicy};
use futures_util::StreamExt;
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, QueuedRow, ResourceBudget,
    RestoreClaim, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_testkit::nats::{async_nats, NatsSandbox};
use std::collections::HashMap;
use std::sync::Arc;
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

fn secrets() -> MapSecretResolver {
    MapSecretResolver::new(HashMap::from([(
        "nats.token".to_string(),
        "t0ken-secret-value".to_string(),
    )]))
}

const LOCAL: &str = "nats://127.0.0.1:4222";

fn local_policy() -> TargetPolicy {
    TargetPolicy::allow("127.0.0.1", 4222)
}

// ------------------------------------------------------------------ unit

#[test]
fn nats_source_config_validation_and_semantics_gate() {
    let s = secrets();
    let p = local_policy();
    let base = NatsSourceConfig::new(vec![LOCAL.into()], "sensors.>", schema());
    base.validate(&s, &p).unwrap();
    assert_eq!(NatsSourceConfig::capabilities().kind, "nats");
    assert_eq!(
        NatsSourceConfig::capabilities().replay,
        crate::ReplaySupport::Unsupported
    );
    type M = fn(&mut NatsSourceConfig);
    let cases: Vec<(M, ErrorCode)> = vec![
        (|c| c.subject = "a..b".into(), ErrorCode::InvalidArgument),
        (|c| c.subject = "a.>.b".into(), ErrorCode::InvalidArgument),
        (
            |c| c.queue_group = Some("bad group".into()),
            ErrorCode::InvalidArgument,
        ),
        (|c| c.inbox_capacity = 0, ErrorCode::BoundExceeded),
        (|c| c.inbox_capacity = 4097, ErrorCode::BoundExceeded),
        (
            |c| c.inbox_bytes = 4 * 1024 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.json_limits.max_bytes = 128 * 1024,
            ErrorCode::BoundExceeded,
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
            |c| c.client.servers = vec!["nats://127.0.0.1:4223".into()],
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
        assert_eq!(c.validate(&s, &p).unwrap_err().code, *code, "case {i}");
    }
    let mut grouped = base.clone();
    grouped.queue_group = Some("workers".into());
    grouped.validate(&s, &p).unwrap();
}

#[test]
fn nats_sink_config_validation_and_semantics_gate() {
    let s = secrets();
    let p = local_policy();
    let base = NatsSinkConfig::new(vec![LOCAL.into()], "out.rows");
    base.validate(&s, &p).unwrap();
    assert_eq!(NatsSinkConfig::capabilities().kind, "nats_sink");
    type M = fn(&mut NatsSinkConfig);
    let cases: Vec<(M, ErrorCode)> = vec![
        (|c| c.subject = "out.*".into(), ErrorCode::InvalidArgument),
        (|c| c.subject = "out.>".into(), ErrorCode::InvalidArgument),
        (|c| c.outbox_capacity = 0, ErrorCode::BoundExceeded),
        (
            |c| c.publish_timeout = Duration::from_millis(9),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.flush_timeout = Duration::from_secs(31),
            ErrorCode::BoundExceeded,
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
        assert_eq!(c.validate(&s, &p).unwrap_err().code, *code, "case {i}");
    }
    // Sink bind checks the SDK charge against the job reservation.
    let mut big = base.clone();
    big.client.max_payload_bytes = 1024 * 1024;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    assert_eq!(
        NatsSink::bind(big, &s, &p, owner, IoDiagnostics::new())
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
}

#[test]
fn nats_token_is_resolved_at_bind_and_never_printed() {
    let s = secrets();
    let p = local_policy();
    let mut src = NatsSourceConfig::new(vec!["tls://127.0.0.1:4222".into()], "a", schema());
    src.client.token_secret = Some("nats.token".into());
    let source = NatsSource::bind(src, &s, &p, IoDiagnostics::new()).unwrap();
    let printed = format!("{source:?}");
    assert!(printed.contains("<redacted>"), "{printed}");
    assert!(!printed.contains("t0ken-secret-value"), "{printed}");
    let mut snk = NatsSinkConfig::new(vec!["tls://127.0.0.1:4222".into()], "b");
    snk.client.token_secret = Some("nats.token".into());
    let sink = NatsSink::bind(
        snk,
        &s,
        &p,
        MemoryOwner::new(ResourceBudget::compact()),
        IoDiagnostics::new(),
    )
    .unwrap();
    let printed = format!("{sink:?} {:?}", sink.config);
    assert!(!printed.contains("t0ken-secret-value"), "{printed}");
    // A missing SecretRef fails bind without echoing anything secret.
    let mut missing = NatsSinkConfig::new(vec!["tls://127.0.0.1:4222".into()], "b");
    missing.client.token_secret = Some("absent".into());
    let e = NatsSink::bind(
        missing,
        &s,
        &p,
        MemoryOwner::new(ResourceBudget::compact()),
        IoDiagnostics::new(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::SecretMissing);
}

// ---------------------------------------------------------- broker harness

fn policy(broker: &NatsSandbox) -> TargetPolicy {
    TargetPolicy::allow("127.0.0.1", broker.port)
}

struct RunningSource {
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    rx: sparrow_io::observed::Receiver<QueuedRow>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<sparrow_model::Result<()>>,
}

fn start_source(broker: &NatsSandbox, mutate: impl FnOnce(&mut NatsSourceConfig)) -> RunningSource {
    start_source_url(broker.url(), broker.port, mutate)
}

fn start_source_url(
    url: String,
    port: u16,
    mutate: impl FnOnce(&mut NatsSourceConfig),
) -> RunningSource {
    let mut config = NatsSourceConfig::new(vec![url], "in.>", schema());
    config.client.reconnect_attempts = 20;
    mutate(&mut config);
    start_source_config(config, TargetPolicy::allow("127.0.0.1", port))
}

fn start_source_config(config: NatsSourceConfig, allowed: TargetPolicy) -> RunningSource {
    let diag = IoDiagnostics::new();
    let capacity = config.inbox_capacity;
    let source = NatsSource::bind(config, &secrets(), &allowed, diag.clone()).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    diag.observation.initialize(&owner).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(source.run_budgeted(tx, cancel.clone(), owner.clone(), 64 * 1024));
    RunningSource {
        diag,
        owner,
        rx,
        cancel,
        task,
    }
}

impl RunningSource {
    async fn ready(&self) {
        until(Duration::from_secs(5), || {
            self.diag
                .observation
                .endpoints()
                .is_some_and(|(source, _)| {
                    source.state == sparrow_model::observation::HealthState::Ready
                })
        })
        .await;
    }

    async fn take(&mut self, n: usize) -> Vec<Vec<Scalar>> {
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while got.len() < n {
                got.push(self.rx.recv().await.expect("source closed"));
            }
        })
        .await
        .expect("rows deadline");
        let mut builder = RowBatchBuilder::new(
            Arc::new(schema()),
            self.owner.clone(),
            CreditKind::Reservation,
            got.len().max(1),
            1 << 20,
        )
        .unwrap();
        for q in got {
            q.push_into(&mut builder).unwrap();
        }
        builder
            .finish()
            .unwrap()
            .rows()
            .iter()
            .map(|r| r.values.clone())
            .collect()
    }

    async fn stop(self) -> sparrow_model::Result<()> {
        self.cancel.cancel();
        let r = tokio::time::timeout(Duration::from_secs(8), self.task)
            .await
            .expect("NATS source must stop promptly")
            .unwrap();
        drop(self.rx);
        r
    }
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

async fn raw_client(broker: &NatsSandbox) -> async_nats::Client {
    async_nats::connect(broker.url()).await.unwrap()
}

async fn publish_rows(client: &async_nats::Client, subject: &str, from: i64, to: i64) {
    for v in from..=to {
        client
            .publish(
                subject.to_string(),
                format!(r#"{{"device_id":"d","v":{v}}}"#).into(),
            )
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
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
                values: vec![Scalar::utf8("s"), Scalar::Int64(v)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

async fn collect(sub: &mut async_nats::Subscriber, n: usize) -> Vec<i64> {
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while out.len() < n {
            let m = sub.next().await.expect("subscriber closed");
            let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
            out.push(v["v"].as_i64().unwrap());
        }
    })
    .await
    .expect("subscriber deadline");
    out
}

fn ints(rows: &[Vec<Scalar>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r[1] {
            Scalar::Int64(v) => v,
            ref other => panic!("{other:?}"),
        })
        .collect()
}

// ----------------------------------------------------------- broker tests

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_source_wildcard_decode_and_drop_counters() {
    let broker = NatsSandbox::start().await;
    let mut run = start_source(&broker, |c| c.json_limits.max_bytes = 1024);
    run.ready().await;
    let client = raw_client(&broker).await;
    publish_rows(&client, "in.a", 1, 3).await;
    client.publish("in.b.c", "not json".into()).await.unwrap();
    client
        .publish("in.b", r#"{"device_id":"d"}"#.into())
        .await
        .unwrap();
    client
        .publish("in.big", vec![b' '; 2048].into())
        .await
        .unwrap();
    client
        .publish("other", r#"{"device_id":"d","v":9}"#.into())
        .await
        .unwrap();
    publish_rows(&client, "in.z", 4, 4).await;
    let rows = run.take(4).await;
    assert_eq!(ints(&rows), vec![1, 2, 3, 4]);
    until(Duration::from_secs(2), || {
        run.diag.snapshot().nats_source_received == 7
    })
    .await;
    let snap = run.diag.snapshot();
    assert_eq!(snap.nats_source_rows, 4);
    assert_eq!(snap.nats_source_dropped_bad, 2, "{snap:?}");
    assert_eq!(snap.decode_errors, 2);
    assert_eq!(snap.nats_source_dropped_oversize, 1);
    assert_eq!(snap.nats_source_slow_consumer, 0);
    run.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_fail_on_decode_fails_the_source() {
    let broker = NatsSandbox::start().await;
    let run = start_source(&broker, |c| c.fail_on_decode = true);
    run.ready().await;
    let client = raw_client(&broker).await;
    client.publish("in.x", "{".into()).await.unwrap();
    client.flush().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), run.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::CodecViolation);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_queue_group_splits_and_plain_subscriber_sees_all() {
    let broker = NatsSandbox::start().await;
    let mut a = start_source(&broker, |c| {
        c.queue_group = Some("workers".into());
        c.inbox_capacity = 256;
    });
    let mut b = start_source(&broker, |c| {
        c.queue_group = Some("workers".into());
        c.inbox_capacity = 256;
    });
    let mut all = start_source(&broker, |c| c.inbox_capacity = 256);
    a.ready().await;
    b.ready().await;
    all.ready().await;
    let client = raw_client(&broker).await;
    // Paced so no subscriber hits its wire prefetch bound (that path is the
    // slow-consumer test); the server picks a random member per message.
    for chunk in 0..25 {
        publish_rows(&client, "in.q", chunk * 8 + 1, chunk * 8 + 8).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(ints(&all.take(200).await), (1..=200).collect::<Vec<_>>());
    until(Duration::from_secs(3), || {
        a.diag.snapshot().nats_source_rows + b.diag.snapshot().nats_source_rows == 200
    })
    .await;
    let (na, nb) = (
        a.diag.snapshot().nats_source_rows as usize,
        b.diag.snapshot().nats_source_rows as usize,
    );
    assert!(na > 0 && nb > 0, "both members receive: {na}/{nb}");
    let mut seen = ints(&a.take(na).await);
    seen.extend(ints(&b.take(nb).await));
    seen.sort_unstable();
    assert_eq!(
        seen,
        (1..=200).collect::<Vec<_>>(),
        "each message exactly one member"
    );
    for run in [a, b, all] {
        run.stop().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_slow_consumer_drops_are_counted_and_memory_bounded() {
    let broker = NatsSandbox::start().await;
    let run = start_source(&broker, |c| {
        c.inbox_capacity = 2;
        c.client.capacity = 4;
    });
    run.ready().await;
    let client = raw_client(&broker).await;
    publish_rows(&client, "in.burst", 1, 500).await;
    // Inbox (2) + one waiting row + wire prefetch (4); the rest is explicitly
    // dropped by the actor, with no lossy SDK event channel involved.
    until(Duration::from_secs(5), || {
        let s = run.diag.snapshot();
        s.nats_source_slow_consumer > 0 && s.nats_source_backpressure_waits >= 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let snap = run.diag.snapshot();
    assert_eq!(snap.nats_source_rows, 2, "{snap:?}");
    assert!(snap.nats_source_inbox_items <= 2);
    assert!(snap.nats_source_received <= 3, "{snap:?}");
    assert_eq!(
        snap.nats_source_slow_consumer, 493,
        "all actor drops are counted: {snap:?}"
    );
    let queue = run.owner.usage().queue_bytes;
    let reservation = run.owner.usage().reservation_bytes;
    publish_rows(&client, "in.burst", 501, 1000).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(run.owner.usage().queue_bytes, queue, "queue credit bounded");
    assert_eq!(run.owner.usage().reservation_bytes, reservation);
    // Stop while blocked on admission: prompt, and all credit released.
    let owner = run.owner.clone();
    run.stop().await.unwrap();
    until(Duration::from_secs(5), || {
        owner.usage().reservation_bytes == 0
    })
    .await;
    assert_eq!(owner.usage().queue_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_server_restart_reconnects_source_and_sink() {
    let mut broker = NatsSandbox::start().await;
    let mut run = start_source(&broker, |_| {});
    run.ready().await;
    let sink_owner = MemoryOwner::new(ResourceBudget::compact());
    let sink_diag = IoDiagnostics::new();
    let mut sink_cfg = NatsSinkConfig::new(vec![broker.url()], "out.rows");
    sink_cfg.client.reconnect_attempts = 20;
    let sink = NatsSink::bind(
        sink_cfg,
        &secrets(),
        &policy(&broker),
        sink_owner.clone(),
        sink_diag.clone(),
    )
    .unwrap();
    let (out_tx, out_rx) = sparrow_io::observed::channel(8);
    let sink_cancel = CancellationToken::new();
    let sink_task = tokio::spawn(sink.run(out_rx, sink_cancel.clone(), None));
    until(Duration::from_secs(5), || {
        sink_diag.snapshot().nats_sink_sessions == 1
    })
    .await;

    publish_rows(&raw_client(&broker).await, "in.r", 1, 1).await;
    assert_eq!(ints(&run.take(1).await), vec![1]);

    broker.restart().await;
    until(Duration::from_secs(10), || {
        run.diag.snapshot().nats_source_reconnects >= 1
            && sink_diag.snapshot().nats_sink_reconnects >= 1
    })
    .await;
    assert!(run.diag.snapshot().nats_source_disconnects >= 1);
    assert!(sink_diag.snapshot().nats_sink_disconnects >= 1);
    // The source actor resubscribed and the sink SDK reconnected; no replay.
    let client = raw_client(&broker).await;
    let mut sub = client.subscribe("out.rows").await.unwrap();
    client.flush().await.unwrap();
    publish_rows(&client, "in.r", 2, 3).await;
    assert_eq!(ints(&run.take(2).await), vec![2, 3]);
    out_tx.send(batch(&sink_owner, 10, 12)).await.unwrap();
    assert_eq!(collect(&mut sub, 3).await, vec![10, 11, 12]);
    assert_eq!(
        sink_diag.snapshot().nats_sink_sessions,
        1,
        "SDK reconnect, not a new session"
    );
    run.stop().await.unwrap();
    sink_cancel.cancel();
    tokio::time::timeout(Duration::from_secs(8), sink_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_bounded_reconnect_gives_up_retryably() {
    let mut broker = NatsSandbox::start().await;
    let run = start_source(&broker, |c| c.client.reconnect_attempts = 2);
    run.ready().await;
    broker.stop_broker();
    // 2 attempts at 100ms/200ms backoff plus connect timeouts: well under 10s.
    let result = tokio::time::timeout(Duration::from_secs(10), run.task)
        .await
        .expect("bounded reconnect must give up")
        .unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.code, ErrorCode::JobFailed);
    assert!(error.retryable, "{error:?}");
    assert!(run.diag.snapshot().nats_source_disconnects >= 1);
    until(Duration::from_secs(5), || {
        run.owner.usage().reservation_bytes == 0
    })
    .await;
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_source_refuses_server_payload_above_its_bound() {
    let broker = NatsSandbox::start().await; // max_payload 65536
    let run = start_source(&broker, |c| c.client.max_payload_bytes = 32 * 1024);
    let result = tokio::time::timeout(Duration::from_secs(5), run.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::BoundExceeded);
    until(Duration::from_secs(5), || {
        run.owner.usage().reservation_bytes == 0
    })
    .await;
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_sink_publishes_and_flushes_queued_batches_on_stop() {
    let broker = NatsSandbox::start().await;
    let client = raw_client(&broker).await;
    let mut sub = client.subscribe("out.rows").await.unwrap();
    client.flush().await.unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let sink = NatsSink::bind(
        NatsSinkConfig::new(vec![broker.url()], "out.rows"),
        &secrets(),
        &policy(&broker),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(8);
    let cancel = CancellationToken::new();
    let outbox = Arc::new(sparrow_model::InflightCounter::new());
    let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
    until(Duration::from_secs(5), || {
        diag.snapshot().nats_sink_sessions == 1
    })
    .await;
    tx.send(batch(&owner, 1, 2)).await.unwrap();
    assert_eq!(collect(&mut sub, 2).await, vec![1, 2]);
    // Queue several batches and stop immediately: shutdown publishes what
    // is already queued, then flushes, within flush_timeout.
    for i in 0..5 {
        outbox.enqueue();
        tx.try_send(batch(&owner, 100 + 10 * i, 100 + 10 * i + 2))
            .unwrap();
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let got = collect(&mut sub, 15).await;
    assert_eq!(got.len(), 15);
    let snap = diag.snapshot();
    assert_eq!(snap.nats_sink_published, 17, "{snap:?}");
    assert_eq!(snap.nats_sink_flushes, 1);
    assert_eq!(snap.nats_sink_discarded_on_close, 0);
    assert_eq!(outbox.failed(), 0);
    drop(tx);
    until(Duration::from_secs(5), || {
        owner.usage().reservation_bytes == 0
    })
    .await;
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_sink_counts_oversize_and_ends_on_input_close() {
    let broker = NatsSandbox::start().await;
    let client = raw_client(&broker).await;
    let mut sub = client.subscribe("out.big").await.unwrap();
    client.flush().await.unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let wide = Arc::new(
        Schema::new(
            SchemaId::new(2),
            vec![Field::new(FieldId::new(1), "s", DataType::Utf8, false)],
        )
        .unwrap(),
    );
    let mut cfg = NatsSinkConfig::new(vec![broker.url()], "out.big");
    cfg.client.max_payload_bytes = 1024;
    let sink = NatsSink::bind(
        cfg,
        &secrets(),
        &policy(&broker),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(2);
    let task = tokio::spawn(sink.run(rx, CancellationToken::new(), None));
    let mut builder =
        RowBatchBuilder::new(wide, owner.clone(), CreditKind::Reservation, 2, 1 << 16).unwrap();
    builder
        .push(Row {
            values: vec![Scalar::utf8("x".repeat(2000))],
        })
        .unwrap();
    builder
        .push(Row {
            values: vec![Scalar::utf8("ok")],
        })
        .unwrap();
    tx.send(builder.finish().unwrap()).await.unwrap();
    drop(tx); // EOF: flush and return without cancellation.
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.payload[..], br#"{"s":"ok"}"#);
    let snap = diag.snapshot();
    assert_eq!(
        (snap.nats_sink_published, snap.nats_sink_dropped_oversize),
        (1, 1)
    );
    assert_eq!(snap.nats_sink_flushes, 1);
}

// Scripted peers assert protocol order directly; no sleep/poll is used as a
// substitute for the subscription PONG or pre-reconnect payload gate.
async fn peer_line(socket: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut line = Vec::new();
    loop {
        let mut byte = [0; 1];
        socket.read_exact(&mut byte).await.unwrap();
        line.push(byte[0]);
        assert!(line.len() <= 32 * 1024);
        if line.ends_with(b"\r\n") {
            return String::from_utf8(line).unwrap();
        }
    }
}

#[tokio::test]
async fn nats_core_source_ready_requires_this_subscriptions_pong() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
    let (pong_tx, pong_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket
            .write_all(b"INFO {\"max_payload\":65536}\r\n")
            .await
            .unwrap();
        let connect = peer_line(&mut socket).await;
        let options: serde_json::Value =
            serde_json::from_str(connect.trim().strip_prefix("CONNECT ").unwrap()).unwrap();
        assert_eq!(options["headers"], false);
        assert_eq!(peer_line(&mut socket).await, "SUB in.> 1\r\n");
        assert_eq!(peer_line(&mut socket).await, "PING\r\n");
        barrier_tx.send(()).unwrap();
        pong_rx.await.unwrap();
        socket.write_all(b"PONG\r\n").await.unwrap();
        let body = br#"{"device_id":"d","v":1}"#;
        socket
            .write_all(format!("MSG in.a 1 {}\r\n", body.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(body).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        finish_rx.await.unwrap();
    });
    let mut run = start_source_url(format!("nats://127.0.0.1:{port}"), port, |_| {});
    barrier_rx.await.unwrap();
    assert_ne!(
        run.diag.observation.endpoints().unwrap().0.state,
        sparrow_model::observation::HealthState::Ready
    );
    pong_tx.send(()).unwrap();
    run.ready().await;
    assert_eq!(ints(&run.take(1).await), vec![1]);
    run.stop().await.unwrap();
    finish_tx.send(()).unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn nats_core_source_payload_gate_precedes_connect_and_sub() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket
            .write_all(b"INFO {\"max_payload\":131072}\r\n")
            .await
            .unwrap();
        let mut byte = [0; 1];
        assert_eq!(
            socket.read(&mut byte).await.unwrap(),
            0,
            "no CONNECT/SUB may escape the rejected INFO gate"
        );
    });
    let run = start_source_url(format!("nats://127.0.0.1:{port}"), port, |_| {});
    let result = tokio::time::timeout(Duration::from_secs(2), run.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::BoundExceeded);
    assert_eq!(run.owner.usage().reservation_bytes, 0);
    peer.await.unwrap();
}

#[tokio::test]
async fn nats_core_source_refuses_unnegotiated_header_frame_before_body_allocation() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket
            .write_all(b"INFO {\"max_payload\":65536}\r\n")
            .await
            .unwrap();
        assert!(peer_line(&mut socket).await.starts_with("CONNECT "));
        assert_eq!(peer_line(&mut socket).await, "SUB in.> 1\r\n");
        assert_eq!(peer_line(&mut socket).await, "PING\r\n");
        socket
            .write_all(b"PONG\r\nHMSG in.a 1 60000 60030\r\n")
            .await
            .unwrap();
        // No header/body bytes are necessary for the bounded rejection.
    });
    let run = start_source_url(format!("nats://127.0.0.1:{port}"), port, |_| {});
    let result = tokio::time::timeout(Duration::from_secs(2), run.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::CodecViolation);
    assert_eq!(run.owner.usage().reservation_bytes, 0);
    peer.await.unwrap();
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_reconnect_payload_gate_runs_while_ingress_is_blocked() {
    let mut small = NatsSandbox::start().await;
    let mut large = NatsSandbox::start().await;
    let path = large.root.join("nats.conf");
    let config = std::fs::read_to_string(&path)
        .unwrap()
        .replace("max_payload: 65536", "max_payload: 131072");
    std::fs::write(&path, config).unwrap();
    large.restart().await;
    let mut config = NatsSourceConfig::new(vec![small.url(), large.url()], "in.>", schema());
    config.inbox_capacity = 1;
    config.client.capacity = 4;
    config.client.reconnect_attempts = 2;
    let run = start_source_config(config, policy(&small).with_allow("127.0.0.1", large.port));
    run.ready().await;
    publish_rows(&raw_client(&small).await, "in.a", 1, 20).await;
    until(Duration::from_secs(2), || {
        run.diag.snapshot().nats_source_backpressure_waits > 0
    })
    .await;
    small.stop_broker();
    let result = tokio::time::timeout(Duration::from_secs(3), run.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::BoundExceeded);
    assert_eq!(
        run.diag.snapshot().nats_source_reconnects,
        0,
        "rejected INFO must not report a successful reconnect"
    );
    assert_eq!(run.owner.usage().reservation_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_header_storm_is_not_negotiated_or_materialized() {
    let broker = NatsSandbox::start().await;
    let mut run = start_source(&broker, |_| {});
    run.ready().await;
    let before = run.owner.usage().reservation_bytes;
    let client = raw_client(&broker).await;
    let mut headers = async_nats::HeaderMap::new();
    for _ in 0..10000 {
        headers.append("X", "v");
    }
    client
        .publish_with_headers(
            "in.a",
            headers,
            br#"{"device_id":"d","v":42}"#.as_slice().into(),
        )
        .await
        .unwrap();
    assert_eq!(
        ints(&run.take(1).await),
        vec![42],
        "broker must strip headers for CONNECT headers=false"
    );
    assert_eq!(run.diag.snapshot().nats_source_dropped_bad, 0);
    assert_eq!(run.owner.usage().reservation_bytes, before);
    run.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_sink_cancel_bounds_active_and_queued_batches_by_one_deadline() {
    let mut broker = NatsSandbox::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    diag.observation.initialize(&owner).unwrap();
    let mut config = NatsSinkConfig::new(vec![broker.url()], "out.rows");
    config.client.capacity = 1;
    config.client.reconnect_attempts = 4;
    config.client.connect_timeout = Duration::from_millis(100);
    config.publish_timeout = Duration::from_secs(30);
    config.flush_timeout = Duration::from_millis(100);
    let sink = NatsSink::bind(
        config,
        &secrets(),
        &policy(&broker),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(8);
    let cancel = CancellationToken::new();
    let outbox = Arc::new(sparrow_model::InflightCounter::new());
    let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
    until(Duration::from_secs(2), || {
        diag.snapshot().nats_sink_sessions == 1
    })
    .await;
    broker.stop_broker();
    until(Duration::from_secs(2), || {
        diag.snapshot().nats_sink_disconnects == 1
    })
    .await;
    outbox.enqueue();
    tx.send(batch(&owner, 1, 64)).await.unwrap();
    outbox.enqueue();
    tx.send(batch(&owner, 100, 101)).await.unwrap();
    until(Duration::from_secs(1), || {
        diag.snapshot().nats_sink_published == 1
    })
    .await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let snapshot = diag.snapshot();
    assert_eq!(snapshot.nats_sink_published, 1);
    assert_eq!(
        snapshot.nats_sink_failed, 1,
        "only the blocked in-flight publish expires"
    );
    assert_eq!(
        snapshot.nats_sink_discarded_on_close, 64,
        "remaining 62 active + 2 queued rows share the expired deadline"
    );
    assert_eq!(
        (outbox.pending(), outbox.acked(), outbox.failed()),
        (0, 0, 2)
    );
    assert_eq!(owner.usage().reservation_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_sdk_exit_does_not_refund_a_live_core_client() {
    let mut broker = NatsSandbox::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let mut config = NatsClientConfig::new(vec![broker.url()]);
    config.capacity = 1;
    config.reconnect_attempts = 1;
    let expected = config.sdk_reservation();
    let client =
        super::client::CoreClient::open(&config, None, &owner, &diag, super::client::Role::Sink)
            .await
            .unwrap();
    broker.stop_broker();
    until(Duration::from_secs(2), || client.is_closed()).await;
    assert_eq!(owner.usage().reservation_bytes, expected);
    client.close(false).await.unwrap();
    assert_eq!(owner.usage().reservation_bytes, 0);
}

#[tokio::test]
async fn nats_core_sink_flush_is_only_a_local_socket_flush_without_server_pong() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket
            .write_all(b"INFO {\"max_payload\":65536}\r\n")
            .await
            .unwrap();
        assert!(peer_line(&mut socket).await.starts_with("CONNECT "));
        assert_eq!(peer_line(&mut socket).await, "PING\r\n");
        socket.write_all(b"PONG\r\n").await.unwrap();
        // Deliberately never read any PUB or reply to any later PING.
        done_rx.await.unwrap();
    });
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let sink = NatsSink::bind(
        NatsSinkConfig::new(vec![format!("nats://127.0.0.1:{port}")], "out.rows"),
        &secrets(),
        &TargetPolicy::allow("127.0.0.1", port),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(1);
    tx.send(batch(&owner, 1, 1)).await.unwrap();
    drop(tx);
    tokio::time::timeout(
        Duration::from_secs(2),
        sink.run(rx, CancellationToken::new(), None),
    )
    .await
    .unwrap();
    assert_eq!(diag.snapshot().nats_sink_published, 1);
    assert_eq!(
        diag.snapshot().nats_sink_flushes,
        1,
        "socket flush can succeed without broker receipt/PONG"
    );
    assert_eq!(owner.usage().reservation_bytes, 0);
    done_tx.send(()).unwrap();
    peer.await.unwrap();
}

fn csv_format(role: sparrow_formats::CsvRole) -> sparrow_formats::PayloadFormat {
    sparrow_formats::PayloadFormat::csv(
        sparrow_formats::CsvOptions::default()
            .compile(role)
            .unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_csv_source_and_sink_round_trip_through_the_broker() {
    let broker = NatsSandbox::start().await;
    let mut run = start_source(&broker, |c| {
        c.payload_format = csv_format(sparrow_formats::CsvRole::Decode);
    });
    run.ready().await;
    let client = raw_client(&broker).await;
    for payload in [
        "device_id,v\nd,1\n",
        // Reordered header, quoted delimiter, CRLF, BOM.
        "\u{feff}v,device_id\r\n2,\"x,y\"\r\n",
        // Missing header: the record is read as a header and refused.
        "d,3\n",
        // Two records in one message are malformed.
        "device_id,v\nd,4\nd,5\n",
        // Type error.
        "device_id,v\nd,six\n",
    ] {
        client.publish("in.csv", payload.into()).await.unwrap();
    }
    client.flush().await.unwrap();
    // Sink: one CSV message per row, header first, through the same broker
    // into the CSV source.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let mut config = NatsSinkConfig::new(vec![broker.url()], "in.sink");
    config.payload_format = csv_format(sparrow_formats::CsvRole::Encode);
    let mut sub = client.subscribe("in.sink").await.unwrap();
    client.flush().await.unwrap();
    let sink = NatsSink::bind(
        config,
        &secrets(),
        &policy(&broker),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(8);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(sink.run(rx, cancel.clone(), None));
    until(Duration::from_secs(5), || {
        diag.snapshot().nats_sink_sessions == 1
    })
    .await;
    tx.send(batch(&owner, 7, 8)).await.unwrap();
    let raw = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&raw.payload[..], b"device_id,v\ns,7\n");
    let rows = run.take(4).await;
    assert_eq!(
        rows,
        vec![
            vec![Scalar::utf8("d"), Scalar::Int64(1)],
            vec![Scalar::utf8("x,y"), Scalar::Int64(2)],
            vec![Scalar::utf8("s"), Scalar::Int64(7)],
            vec![Scalar::utf8("s"), Scalar::Int64(8)],
        ]
    );
    let snap = run.diag.snapshot();
    assert_eq!(snap.nats_source_dropped_bad, 3, "{snap:?}");
    assert_eq!(
        (
            snap.csv_header_errors,
            snap.csv_malformed,
            snap.csv_type_errors
        ),
        (1, 1, 1)
    );
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    drop(tx);
    run.stop().await.unwrap();
}

fn protobuf_format(
    role: sparrow_formats::CsvRole,
    tune: impl FnOnce(&mut sparrow_formats::ProtobufOptions),
) -> sparrow_formats::PayloadFormat {
    crate::protobuf_test_support::format(role, tune)
}

#[test]
fn nats_protobuf_formats_resolve_the_schema() {
    for role in [sparrow_formats::CsvRole::Decode, sparrow_formats::CsvRole::Encode] {
        assert!(protobuf_format(role, |_| {}).check_schema(&schema()).is_ok());
    }
    // An unmapped schema column with no same-named field is refused.
    let format = protobuf_format(sparrow_formats::CsvRole::Decode, |o| {
        o.fields.remove("v");
    });
    assert_eq!(
        format.check_schema(&schema()).unwrap_err().code,
        ErrorCode::InvalidSchema
    );
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn nats_core_protobuf_source_and_sink_round_trip_through_the_broker() {
    let broker = NatsSandbox::start().await;
    let mut run = start_source(&broker, |c| {
        c.payload_format = protobuf_format(sparrow_formats::CsvRole::Decode, |o| {
            o.unknown_fields = sparrow_formats::UnknownFields::Error;
            o.max_message_bytes = Some(16);
        });
    });
    run.ready().await;
    let client = raw_client(&broker).await;
    for payload in [
        // device "d", seq 1.
        vec![0x0a, 0x01, b'd', 0x10, 0x01],
        // seq absent: proto3 implicit presence decodes to 0, not NULL.
        vec![0x0a, 0x01, b'e'],
        // Truncated string.
        vec![0x0a, 0x05, b'd'],
        // Unknown field 111 under unknown_fields=error.
        vec![0x0a, 0x01, b'd', 0xf8, 0x06, 0x01],
        // 17 bytes > max_message_bytes 16.
        [vec![0x0a, 0x0f], vec![b'x'; 15]].concat(),
        // Invalid UTF-8 in a declared string.
        vec![0x0a, 0x01, 0xff],
    ] {
        client.publish("in.pb", payload.into()).await.unwrap();
    }
    client.flush().await.unwrap();
    // Sink: one protobuf message per row, through the same broker into the
    // protobuf source.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let mut config = NatsSinkConfig::new(vec![broker.url()], "in.sink");
    config.payload_format = protobuf_format(sparrow_formats::CsvRole::Encode, |_| {});
    let mut sub = client.subscribe("in.sink").await.unwrap();
    client.flush().await.unwrap();
    let sink = NatsSink::bind(
        config,
        &secrets(),
        &policy(&broker),
        owner.clone(),
        diag.clone(),
    )
    .unwrap();
    let (tx, rx) = sparrow_io::observed::channel(8);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(sink.run(rx, cancel.clone(), None));
    until(Duration::from_secs(5), || {
        diag.snapshot().nats_sink_sessions == 1
    })
    .await;
    tx.send(batch(&owner, 0, 300)).await.unwrap();
    let raw = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    // seq 0 is the implicit default and is omitted, like protoc.
    assert_eq!(&raw.payload[..], [0x0a, 0x01, b's']);
    let rows = run.take(2 + 301).await;
    assert_eq!(rows[0], vec![Scalar::utf8("d"), Scalar::Int64(1)]);
    assert_eq!(rows[1], vec![Scalar::utf8("e"), Scalar::Int64(0)]);
    for (i, row) in rows[2..].iter().enumerate() {
        assert_eq!(*row, vec![Scalar::utf8("s"), Scalar::Int64(i as i64)]);
    }
    let snap = run.diag.snapshot();
    // Over max_message_bytes is refused by length before any charge/decode.
    assert_eq!(snap.nats_source_dropped_oversize, 1, "{snap:?}");
    assert_eq!(snap.nats_source_dropped_bad, 3, "{snap:?}");
    assert_eq!(
        (
            snap.protobuf_malformed,
            snap.protobuf_unknown_fields,
            snap.protobuf_type_errors,
        ),
        (2, 1, 0),
        "{snap:?}"
    );
    assert_eq!(snap.csv_malformed, 0);
    assert_eq!(diag.snapshot().protobuf_encode_errors, 0);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    drop(tx);
    run.stop().await.unwrap();
}

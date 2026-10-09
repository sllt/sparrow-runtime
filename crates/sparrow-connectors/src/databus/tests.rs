//! DataBus unit tests: topic grammar, overflow policies, fan-out, slow
//! subscriber isolation, lifecycle cleanup, memory refusal, and a real
//! Sink -> bus -> Source run through the actors.

use super::*;
use sparrow_model::{
    CreditKind, DataType, Field, FieldId, QueuedRow, ResourceBudget, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SchemaId,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

fn owner() -> Arc<MemoryOwner> {
    MemoryOwner::new(ResourceBudget::compact())
}

fn sub_config(pattern: &str, capacity: usize, overflow: Overflow) -> SubscriptionConfig {
    SubscriptionConfig {
        pattern: pattern.into(),
        capacity,
        max_bytes: 64 * 1024,
        overflow,
        block_timeout: Duration::from_millis(50),
    }
}

fn msg(i: i64) -> Arc<[u8]> {
    Arc::from(format!(r#"{{"v":{i}}}"#).into_bytes())
}

fn value(m: &[u8]) -> i64 {
    serde_json::from_slice::<serde_json::Value>(m).unwrap()["v"]
        .as_i64()
        .unwrap()
}

async fn drain(sub: &Subscriber) -> Vec<i64> {
    let mut out = Vec::new();
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(20), sub.recv()).await {
        out.push(value(&m));
    }
    out
}

#[test]
fn databus_topic_grammar_matching_and_overlap() {
    for ok in ["a", "a.b", "sensor-1.temp_c", "A.B.C"] {
        check_topic(ok, false).unwrap();
    }
    for bad in [
        "",
        "a..b",
        ".a",
        "a.",
        "a b",
        "a/b",
        "a.*",
        "a.>",
        &"x".repeat(129),
    ] {
        assert!(check_topic(bad, false).is_err(), "{bad:?}");
    }
    let deep = vec!["t"; 17].join(".");
    assert!(check_topic(&deep, true).is_err());
    for ok in ["a.*", "a.>", "*", ">", "*.b.>"] {
        check_topic(ok, true).unwrap();
    }
    for bad in ["a.>.b", "a.b*", "a.>x"] {
        assert!(check_topic(bad, true).is_err(), "{bad:?}");
    }
    assert!(topic_matches("a.*", "a.b"));
    assert!(!topic_matches("a.*", "a.b.c"));
    assert!(topic_matches("a.>", "a.b.c"));
    assert!(!topic_matches("a.>", "a"));
    assert!(topic_matches("a.b", "a.b"));
    assert!(!topic_matches("a.b", "a.c"));
    assert!(patterns_overlap("a.*", "a.b"));
    assert!(patterns_overlap("a.>", "*.x.y"));
    assert!(!patterns_overlap("a.*", "b.c"));
    assert!(!patterns_overlap("a.b", "a.b.c"));
    assert_eq!(Overflow::parse("block").unwrap(), Overflow::Block);
    assert!(Overflow::parse("drop").is_err());
    assert_eq!(Overflow::default().as_str(), "drop_oldest");
    let mut c = sub_config("a", 0, Overflow::Block);
    assert_eq!(c.validate().unwrap_err().code, ErrorCode::BoundExceeded);
    c.capacity = 1;
    c.block_timeout = Duration::ZERO;
    assert_eq!(c.validate().unwrap_err().code, ErrorCode::BoundExceeded);
}

#[tokio::test]
async fn databus_fan_out_preserves_order_and_no_subscriber_is_counted() {
    let bus = DataBus::new();
    let owner = owner();
    let publisher = bus.publisher("plant.line1").unwrap();
    // Nobody listening: offered to zero subscribers, nothing retained.
    assert_eq!(publisher.publish(msg(0)).await.matched, 0);
    let (da, db) = (IoDiagnostics::new(), IoDiagnostics::new());
    let a = bus
        .subscribe(
            sub_config("plant.line1", 64, Overflow::DropOldest),
            &owner,
            da.clone(),
        )
        .unwrap();
    let b = bus
        .subscribe(
            sub_config("plant.>", 64, Overflow::DropNewest),
            &owner,
            db.clone(),
        )
        .unwrap();
    let other = bus
        .subscribe(
            sub_config("other.*", 64, Overflow::Block),
            &owner,
            IoDiagnostics::new(),
        )
        .unwrap();
    for i in 1..=20 {
        let out = publisher.publish(msg(i)).await;
        assert_eq!((out.matched, out.accepted, out.blocked), (2, 2, false));
    }
    let expected: Vec<i64> = (1..=20).collect();
    assert_eq!(drain(&a).await, expected);
    assert_eq!(drain(&b).await, expected);
    assert!(drain(&other).await.is_empty());
    assert_eq!(da.snapshot().databus_source_received, 20);
    assert_eq!(db.snapshot().databus_source_dropped_newest, 0);
}

#[tokio::test]
async fn databus_overflow_policies_count_and_bound_buffers() {
    let bus = DataBus::new();
    let owner = owner();
    let publisher = bus.publisher("t").unwrap();
    let (d_old, d_new, d_block) = (
        IoDiagnostics::new(),
        IoDiagnostics::new(),
        IoDiagnostics::new(),
    );
    let oldest = bus
        .subscribe(
            sub_config("t", 4, Overflow::DropOldest),
            &owner,
            d_old.clone(),
        )
        .unwrap();
    let newest = bus
        .subscribe(
            sub_config("t", 4, Overflow::DropNewest),
            &owner,
            d_new.clone(),
        )
        .unwrap();
    let block = bus
        .subscribe(sub_config("t", 4, Overflow::Block), &owner, d_block.clone())
        .unwrap();
    for i in 1..=10 {
        publisher.publish(msg(i)).await;
    }
    let s = d_old.snapshot();
    assert_eq!(
        (
            s.databus_source_buffer_items,
            s.databus_source_dropped_oldest
        ),
        (4, 6)
    );
    assert_eq!(drain(&oldest).await, vec![7, 8, 9, 10], "freshest kept");
    assert_eq!(d_new.snapshot().databus_source_dropped_newest, 6);
    assert_eq!(drain(&newest).await, vec![1, 2, 3, 4], "first kept");
    // Block: the publisher waited 50ms per overflowing message, then dropped
    // it for this subscriber only.
    assert_eq!(d_block.snapshot().databus_source_block_timeouts, 6);
    assert_eq!(drain(&block).await, vec![1, 2, 3, 4]);
    assert_eq!(d_old.snapshot().databus_source_buffer_items, 0);
    assert_eq!(d_old.snapshot().databus_source_buffer_bytes, 0);

    // Byte bound: 1 KiB buffer holds two ~400 B messages.
    let d_bytes = IoDiagnostics::new();
    let mut cfg = sub_config("t", 100, Overflow::DropNewest);
    cfg.max_bytes = 1024;
    let small = bus.subscribe(cfg, &owner, d_bytes.clone()).unwrap();
    let big: Arc<[u8]> = Arc::from(vec![b' '; 400]);
    for _ in 0..3 {
        publisher.publish(big.clone()).await;
    }
    let s = d_bytes.snapshot();
    assert_eq!(
        (s.databus_source_buffer_items, s.databus_source_buffer_bytes),
        (2, 800)
    );
    assert_eq!(s.databus_source_dropped_newest, 1);
    publisher.publish(Arc::from(vec![b' '; 2048])).await;
    assert_eq!(d_bytes.snapshot().databus_source_dropped_oversize, 1);
    drop(small);
}

#[tokio::test]
async fn databus_block_policy_backpressures_without_loss_while_consumed() {
    let bus = DataBus::new();
    let owner = owner();
    let publisher = bus.publisher("t").unwrap();
    let diag = IoDiagnostics::new();
    let mut cfg = sub_config("t", 2, Overflow::Block);
    cfg.block_timeout = Duration::from_secs(5);
    let sub = Arc::new(bus.subscribe(cfg, &owner, diag.clone()).unwrap());
    let reader = {
        let sub = sub.clone();
        tokio::spawn(async move {
            let mut got = Vec::new();
            while got.len() < 200 {
                got.push(value(&sub.recv().await.unwrap()));
                if got.len() % 10 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            got
        })
    };
    let mut blocked = 0;
    for i in 0..200 {
        let out = publisher.publish(msg(i)).await;
        assert_eq!(out.accepted, 1);
        blocked += out.blocked as usize;
    }
    assert_eq!(reader.await.unwrap(), (0..200).collect::<Vec<_>>());
    assert!(
        blocked > 0,
        "a 2-slot buffer must have made the publisher wait"
    );
    let s = diag.snapshot();
    assert_eq!(
        (s.databus_source_block_timeouts, s.databus_source_received),
        (0, 200)
    );
}

#[tokio::test]
async fn databus_slow_subscriber_only_stalls_publishers_under_block() {
    let bus = DataBus::new();
    let owner = owner();
    let publisher = bus.publisher("t").unwrap();
    let fast_diag = IoDiagnostics::new();
    let fast = bus
        .subscribe(
            sub_config("t", 1024, Overflow::DropNewest),
            &owner,
            fast_diag.clone(),
        )
        .unwrap();
    // A subscriber that never reads, with a drop policy.
    let slow = bus
        .subscribe(
            sub_config("t", 2, Overflow::DropOldest),
            &owner,
            IoDiagnostics::new(),
        )
        .unwrap();
    let started = Instant::now();
    for i in 0..500 {
        assert!(!publisher.publish(msg(i)).await.blocked);
    }
    assert!(started.elapsed() < Duration::from_secs(2), "never stalled");
    assert_eq!(
        drain(&fast).await.len(),
        500,
        "fast subscriber got everything"
    );
    drop(slow);
    // Same with a block subscriber: the publisher (only) is slowed by its
    // timeout, other subscribers still get every message, first.
    let mut cfg = sub_config("t", 1, Overflow::Block);
    cfg.block_timeout = Duration::from_millis(20);
    let _stuck = bus.subscribe(cfg, &owner, IoDiagnostics::new()).unwrap();
    let started = Instant::now();
    for i in 0..5 {
        publisher.publish(msg(i)).await;
    }
    assert!(started.elapsed() >= Duration::from_millis(80));
    assert_eq!(drain(&fast).await, vec![0, 1, 2, 3, 4]);
    assert_eq!(fast_diag.snapshot().databus_source_dropped_newest, 0);
}

#[tokio::test]
async fn databus_lifecycle_releases_topics_memory_and_wakes_blocked_publishers() {
    let bus = DataBus::new();
    let owner = owner();
    let diag = IoDiagnostics::new();
    let publisher = bus.publisher("x.y").unwrap();
    let second = bus.publisher("x.y").unwrap();
    let mut cfg = sub_config("x.*", 2, Overflow::Block);
    cfg.block_timeout = Duration::from_secs(10);
    let sub = bus.subscribe(cfg.clone(), &owner, diag.clone()).unwrap();
    assert_eq!(owner.usage().reservation_bytes, cfg.reservation());
    let snapshot = bus.snapshot();
    assert_eq!(snapshot.topics.len(), 2);
    let find = |t: &str| {
        snapshot
            .topics
            .iter()
            .find(|x| x.topic == t)
            .unwrap()
            .clone()
    };
    assert_eq!(find("x.y").publishers, 2);
    assert_eq!(find("x.*").subscribers, 1);
    publisher.publish(msg(1)).await;
    publisher.publish(msg(2)).await;
    // The third publish blocks; detaching the subscriber must release it.
    let blocked = tokio::spawn(async move {
        let out = second.publish(msg(3)).await;
        drop(second);
        out
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!blocked.is_finished());
    drop(sub);
    let out = tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .expect("detach wakes blocked publishers")
        .unwrap();
    assert_eq!(out.accepted, 0);
    assert_eq!(diag.snapshot().databus_source_discarded_on_close, 2);
    assert_eq!(owner.usage().reservation_bytes, 0, "buffer credit released");
    drop(publisher);
    assert!(bus.snapshot().topics.is_empty(), "no leaked topics");
    // Re-attach after restart: a new subscriber sees only new messages.
    let publisher = bus.publisher("x.y").unwrap();
    let sub = bus
        .subscribe(
            sub_config("x.y", 8, Overflow::DropOldest),
            &owner,
            diag.clone(),
        )
        .unwrap();
    publisher.publish(msg(9)).await;
    assert_eq!(drain(&sub).await, vec![9]);
}

#[test]
fn databus_buffer_reservation_is_refused_when_it_does_not_fit() {
    let bus = DataBus::new();
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = 128 * 1024;
    let small = MemoryOwner::new(budget);
    let mut cfg = sub_config("t", 16, Overflow::DropOldest);
    cfg.max_bytes = 256 * 1024;
    let e = bus
        .subscribe(cfg.clone(), &small, IoDiagnostics::new())
        .err()
        .unwrap();
    assert_eq!(e.code, ErrorCode::BoundExceeded);
    assert!(bus.snapshot().topics.is_empty());
    assert_eq!(small.usage().reservation_bytes, 0);
    let mut source = DataBusSourceConfig::new("t", schema());
    source.subscription = cfg;
    assert_eq!(
        source
            .check_reservation_budget(budget.reservation_bytes)
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
    source.subscription.max_bytes = 8 * 1024 * 1024;
    assert_eq!(
        source.validate().unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    let mut sink = DataBusSinkConfig::new("t.*");
    assert!(sink.validate().is_err(), "sink topic is literal");
    sink.topic = "t".into();
    sink.validate().unwrap();
    source.subscription.max_bytes = 64 * 1024;
    source.restore = sparrow_model::RestoreClaim::Checkpoint {
        snapshot_id: "1".into(),
    };
    assert!(source.validate().is_err(), "no durable restore");
    assert_eq!(DataBusSourceConfig::capabilities().kind, "databus");
    assert_eq!(DataBusSinkConfig::capabilities().kind, "databus_sink");
}

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
                values: vec![Scalar::utf8("d"), Scalar::Int64(v)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

#[tokio::test]
async fn databus_sink_to_source_actors_exact_rows_and_cleanup() {
    let bus = DataBus::new();
    let source_owner = owner();
    let source_diag = IoDiagnostics::new();
    source_diag.observation.initialize(&source_owner).unwrap();
    // Observation state holds its own fixed credit.
    let baseline = source_owner.usage();
    let config = DataBusSourceConfig::new("rows.*", schema());
    let source = DataBusSource::bind(config, bus.clone(), source_diag.clone()).unwrap();
    let (tx, mut rx) = sparrow_io::observed::channel::<QueuedRow>(16);
    let source_cancel = CancellationToken::new();
    let source_task = tokio::spawn(source.run_budgeted(
        tx,
        source_cancel.clone(),
        source_owner.clone(),
        64 * 1024,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while bus.snapshot().topics.is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();

    let sink_owner = owner();
    let sink_diag = IoDiagnostics::new();
    let sink = DataBusSink::bind(
        DataBusSinkConfig::new("rows.a"),
        bus.clone(),
        sink_diag.clone(),
    )
    .unwrap();
    let (out_tx, out_rx) = sparrow_io::observed::channel(4);
    let sink_cancel = CancellationToken::new();
    let outbox = Arc::new(sparrow_model::InflightCounter::new());
    let sink_task = tokio::spawn(sink.run(out_rx, sink_cancel.clone(), Some(outbox.clone())));
    for (from, to) in [(1, 10), (11, 50), (51, 100)] {
        outbox.enqueue();
        out_tx.send(batch(&sink_owner, from, to)).await.unwrap();
    }
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while got.len() < 100 {
            got.push(rx.recv().await.unwrap());
        }
    })
    .await
    .expect("100 rows through the bus");
    assert_eq!(got.len(), 100);
    drop(out_tx);
    tokio::time::timeout(Duration::from_secs(2), sink_task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        outbox.pending(),
        0,
        "every batch acked after the bus accepted it"
    );
    let s = sink_diag.snapshot();
    assert_eq!(
        (s.databus_sink_published, s.databus_sink_deliveries),
        (100, 100)
    );
    assert_eq!(
        (s.databus_sink_batches, s.databus_sink_no_subscribers),
        (3, 0)
    );
    let s = source_diag.snapshot();
    assert_eq!(
        (s.databus_source_received, s.databus_source_rows),
        (100, 100)
    );
    source_cancel.cancel();
    source_task.await.unwrap().unwrap();
    drop(got);
    assert!(
        bus.snapshot().topics.is_empty(),
        "stop detaches publisher and subscriber"
    );
    assert_eq!(
        source_owner.usage().reservation_bytes,
        baseline.reservation_bytes
    );
    assert_eq!(source_owner.usage().queue_bytes, baseline.queue_bytes);
}

#[tokio::test]
async fn databus_sink_registration_failure_is_fatal() {
    let bus = DataBus::new();
    let held: Vec<_> = (0..MAX_PUBLISHERS)
        .map(|i| bus.publisher(&format!("p.{i}")).unwrap())
        .collect();
    let diag = IoDiagnostics::new();
    let sink = DataBusSink::bind(DataBusSinkConfig::new("p.x"), bus.clone(), diag.clone()).unwrap();
    let (_tx, rx) = sparrow_io::observed::channel::<RowBatch>(4);
    let cancel = CancellationToken::new();
    sink.run(rx, cancel.clone(), None).await;
    assert_eq!(diag.snapshot().databus_sink_fatal, 1);
    assert!(cancel.is_cancelled(), "fatal cancels the job");
    drop(held);
    assert!(bus.snapshot().topics.is_empty());
}

//! JetStream Sink through the real control path: spec matrix, validation, and
//! opt-in broker tests (`SPARROW_NATS_SERVER`, JetStream-enabled child broker):
//! NATS source -> SQL filter -> JetStream sink, missing stream, and the aligned
//! File profile whose checkpoints wait for every pre-barrier PubAck.

use crate::PipelineSpec;
use serde_json::{json, Value};

#[cfg(feature = "jetstream")]
const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

#[cfg(feature = "jetstream")]
const EVENTS_SCHEMA: &str = r#"{"fields":[
  {"name":"id","type":"int64","nullable":false},
  {"name":"v","type":"int64","nullable":false}
]}"#;

fn live_spec(url: &str) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 0",
        "source": {
            "kind": "nats",
            "inbox_capacity": 8,
            "nats": {"servers": [url], "subject": "telemetry.>"}
        },
        "sink": {
            "kind": "jetstream",
            "outbox_capacity": 8,
            "jetstream": {"servers": [url], "stream": "ALERTS", "subject": "alerts.hot"}
        }
    })
}

#[cfg(feature = "jetstream")]
fn aligned_spec(url: &str, path: &std::path::Path, dir: &std::path::Path) -> Value {
    json!({
        "version": 1,
        "stream": "events",
        "sql": "SELECT id, v FROM events WHERE v > 0",
        "source": {"kind": "file", "path": path, "file_contract": "append_only", "inbox_capacity": 4},
        "sink": {
            "kind": "jetstream",
            "outbox_capacity": 4,
            "jetstream": {"servers": [url], "stream": "OUT", "subject": "out.events",
                "msg_id_column": "id", "ack_timeout_ms": 3000}
        },
        "recovery": "aligned",
        "checkpoint_dir": dir,
        "checkpoint": {"timeout_ms": 2000, "resume_latest": true}
    })
}

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

#[cfg(not(feature = "jetstream"))]
#[test]
fn jetstream_sink_spec_without_the_feature_is_feature_unavailable() {
    let mut v = live_spec("nats://127.0.0.1:4222");
    v["source"] = json!({"kind": "mqtt"});
    let err = parse(&v).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::FeatureUnavailable);
}

#[cfg(feature = "jetstream")]
type Mutation = Box<dyn Fn(&mut Value)>;

#[cfg(feature = "jetstream")]
#[test]
fn jetstream_sink_spec_matrix_mixed_fields_and_aligned_profile() {
    use sparrow_model::ErrorCode::*;
    let base = live_spec("nats://127.0.0.1:4222");
    parse(&base).unwrap();
    let aligned = aligned_spec(
        "nats://127.0.0.1:4222",
        std::path::Path::new("/tmp/unused.ndjson"),
        std::path::Path::new("/tmp/unused-checkpoints"),
    );
    parse(&aligned).unwrap();
    let mutations: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "missing block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("jetstream");
            }),
            InvalidArgument,
        ),
        (
            "block on log",
            Box::new(|v| v["sink"]["kind"] = json!("log")),
            InvalidArgument,
        ),
        (
            "nats block too",
            Box::new(|v| {
                v["sink"]["nats"] = json!({"servers": ["nats://127.0.0.1:4222"], "subject": "x"})
            }),
            InvalidArgument,
        ),
        (
            "url field",
            Box::new(|v| v["sink"]["url"] = json!("http://127.0.0.1:1/")),
            InvalidArgument,
        ),
        (
            "mqtt topic",
            Box::new(|v| v["sink"]["topic"] = json!("t")),
            InvalidArgument,
        ),
        (
            "unknown field",
            Box::new(|v| v["sink"]["jetstream"]["create_stream"] = json!(true)),
            InvalidArgument,
        ),
        (
            "aligned with a NATS Core source",
            Box::new(|v| v["recovery"] = json!("aligned")),
            UnsupportedRestore,
        ),
    ];
    for (name, mutate, code) in mutations {
        let mut v = base.clone();
        mutate(&mut v);
        let err = parse(&v).expect_err(name);
        assert_eq!(err.code, code, "{name}: {}", err.message);
    }
    // Aligned/checkpoint claims are only admitted with a File source.
    let mut v = aligned.clone();
    v["source"] = json!({"kind": "mqtt"});
    assert_eq!(parse(&v).unwrap_err().code, UnsupportedRestore);
    let mut v = base.clone();
    v["source"] = json!({"kind": "mqtt"});
    v["checkpoint_dir"] = json!("/tmp/x");
    assert_eq!(parse(&v).unwrap_err().code, UnsupportedRestore);
    let stored = serde_json::to_value(parse(&aligned).unwrap()).unwrap();
    assert!(stored["sink"]["jetstream"].get("max_retries").is_none());
    assert!(stored["sink"].get("nats").is_none());
    assert_eq!(parse(&stored).unwrap(), parse(&aligned).unwrap());
    let caps = crate::capabilities_json();
    let c = caps["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == "jetstream_sink")
        .expect("jetstream_sink in capabilities")
        .clone();
    assert_eq!(c["delivery"], "checkpointed_at_least_once", "{c}");
    assert_eq!(c["recovery"], "aligned", "{c}");
    assert_eq!(c["replay"], "unsupported", "{c}");
    assert_eq!(c["enabled_by_build"], true, "{c}");
    assert!(c["duplicates"].as_str().unwrap().contains("possible"));
}

#[cfg(feature = "jetstream")]
#[test]
fn jetstream_sink_validation_bounds_schema_and_shared_reservation() {
    use sparrow_model::ErrorCode::*;
    let store = crate::Store::open_memory().unwrap();
    store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
    store.put_stream("events", EVENTS_SCHEMA).unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(
        [("tok".to_string(), "secret".to_string())]
            .into_iter()
            .collect(),
    );
    let allowed = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 4222);
    let denied = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        let spec = parse(value).map_err(|e| e.code)?;
        let plan = crate::bind_plan_with_store(&store, &spec, "p", 1).map_err(|e| e.code)?;
        let schema = crate::stream_to_schema(&store.get_stream(&spec.stream).unwrap())
            .map_err(|e| e.code)?;
        crate::validate::validate_io_with_plan(&spec, &schema, &plan, &secrets, policy, None)
            .map_err(|e| e.code)
    };
    let base = live_spec("nats://127.0.0.1:4222");
    check(&base, &allowed).unwrap();
    assert_eq!(check(&base, &denied), Err(PolicyDenied));
    let cases: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "wildcard subject",
            Box::new(|v| v["sink"]["jetstream"]["subject"] = json!("alerts.*")),
            InvalidArgument,
        ),
        (
            "dotted stream",
            Box::new(|v| v["sink"]["jetstream"]["stream"] = json!("A.B")),
            InvalidArgument,
        ),
        (
            "ack timeout too small",
            Box::new(|v| v["sink"]["jetstream"]["ack_timeout_ms"] = json!(1)),
            BoundExceeded,
        ),
        (
            "zero inflight acks",
            Box::new(|v| v["sink"]["jetstream"]["max_inflight_acks"] = json!(0)),
            BoundExceeded,
        ),
        (
            "unbounded retries",
            Box::new(|v| v["sink"]["jetstream"]["max_retries"] = json!(1000)),
            BoundExceeded,
        ),
        (
            "token over plain nats://",
            Box::new(|v| v["sink"]["jetstream"]["token_secret"] = json!("tok")),
            PolicyDenied,
        ),
        (
            "msg id column missing",
            Box::new(|v| v["sink"]["jetstream"]["msg_id_column"] = json!("nope")),
            InvalidSchema,
        ),
        (
            "msg id column float",
            Box::new(|v| v["sink"]["jetstream"]["msg_id_column"] = json!("temperature")),
            InvalidSchema,
        ),
        (
            "retained payloads exceed the reservation",
            Box::new(|v| {
                v["sink"]["jetstream"]["max_payload_bytes"] = json!(1024 * 1024);
                v["sink"]["jetstream"]["max_inflight_acks"] = json!(64);
            }),
            BoundExceeded,
        ),
    ];
    for (name, mutate, code) in cases {
        let mut v = base.clone();
        mutate(&mut v);
        assert_eq!(check(&v, &allowed), Err(code), "{name}");
    }
    let mut v = base.clone();
    v["sink"]["jetstream"]["msg_id_column"] = json!("device_id");
    check(&v, &allowed).unwrap();
    // Each client fits on its own; source plus sink must share 3/4 of the job.
    let mut v = base.clone();
    v["source"]["nats"]["max_payload_bytes"] = json!(1024 * 1024);
    v["source"]["nats"]["subscription_capacity"] = json!(1);
    v["sink"]["jetstream"]["max_payload_bytes"] = json!(256 * 1024);
    v["sink"]["jetstream"]["max_inflight_acks"] = json!(1);
    v["sink"]["jetstream"]["client_capacity"] = json!(1);
    check(&v, &allowed).unwrap();
    v["sink"]["jetstream"]["max_inflight_acks"] = json!(4);
    v["sink"]["jetstream"]["client_capacity"] = json!(4);
    assert_eq!(
        check(&v, &allowed),
        Err(BoundExceeded),
        "NATS source + JetStream sink buffers share one pipeline cap"
    );
    // Aligned File profile binds and passes the aligned plan gate.
    let aligned = aligned_spec(
        "nats://127.0.0.1:4222",
        &sparrow_connectors::ensure_default_data_root().join("unused.ndjson"),
        &sparrow_connectors::ensure_default_data_root().join("unused-checkpoints"),
    );
    let spec = parse(&aligned).unwrap();
    let plan = crate::bind_plan_with_store(&store, &spec, "p", 1).unwrap();
    crate::validate_aligned_plan(&spec, &plan).unwrap();
    let effective = crate::validate::effective_guarantees(&spec);
    assert_eq!(effective["sink_delivery"]["into_stream"], "at_least_once");
    assert_eq!(
        effective["sink_delivery"]["duplicates"],
        "deduplicated_within_stream_duplicate_window"
    );
    assert_eq!(effective["exactly_once"], false);
}

#[cfg(feature = "jetstream")]
mod broker {
    use super::*;
    use crate::{request_start, Store, Supervisor};
    use sparrow_testkit::nats::{jetstream, NatsSandbox};
    use std::sync::Arc;
    use std::time::Duration;

    struct Scratch(std::path::PathBuf);
    impl Scratch {
        fn new() -> Self {
            let root = sparrow_connectors::ensure_default_data_root().join(format!(
                "sparrow-js-sink-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn create_stream(broker: &NatsSandbox, name: &str, subject: &str) -> jetstream::Context {
        let context = broker.context().await;
        context
            .create_stream(jetstream::stream::Config {
                name: name.into(),
                subjects: vec![subject.into()],
                max_bytes: 16 * 1024 * 1024,
                storage: jetstream::stream::StorageType::File,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .unwrap();
        context
    }

    async fn count(context: &jetstream::Context, stream: &str) -> u64 {
        let mut s = context.get_stream(stream).await.unwrap();
        s.info().await.unwrap().state.messages
    }

    async fn rows(context: &jetstream::Context, stream: &str) -> Vec<Value> {
        let s = context.get_stream(stream).await.unwrap();
        let n = count(context, stream).await;
        let mut out = Vec::new();
        for seq in 1..=n {
            let msg = s.get_raw_message(seq).await.unwrap();
            out.push(serde_json::from_slice(&msg.payload).unwrap());
        }
        out
    }

    async fn wait_count(
        context: &jetstream::Context,
        stream: &str,
        n: u64,
        sup: &Arc<Supervisor>,
        store: &Store,
        name: &str,
    ) {
        let reached = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if count(context, stream).await >= n {
                    break;
                }
                sup.converge_once().await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            reached.is_ok(),
            "stream {stream} count {} < {n}; actual={:?}",
            count(context, stream).await,
            store.actual(name)
        );
    }

    fn append(file: &std::path::Path, from: i64, to: i64) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
            .unwrap();
        for id in from..=to {
            // Odd ids pass `WHERE v > 0`.
            let v = if id % 2 == 1 { id } else { -id };
            writeln!(f, "{}", json!({"id": id, "v": v})).unwrap();
        }
        f.sync_all().unwrap();
    }

    #[test]
    #[ignore = "requires SPARROW_NATS_SERVER (real nats-server binary)"]
    fn jetstream_sink_pipeline_nats_source_filter_exact_stream_count() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let fixture = NatsSandbox::start().await;
            let context = create_stream(&fixture, "ALERTS", "alerts.>").await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", fixture.port).unwrap();
            let spec = parse(&live_spec(&fixture.url())).unwrap();
            store.put_pipeline("js", &spec, None).unwrap();
            request_start(&store, "js", "test").unwrap();
            let oracle = sparrow_testkit::nats::async_nats::connect(fixture.url())
                .await
                .unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            let publish = |device: &'static str, t: f64| {
                let oracle = oracle.clone();
                async move {
                    let row = json!({"device_id": device, "temperature": t});
                    oracle
                        .publish("telemetry.x", row.to_string().into())
                        .await
                        .unwrap();
                }
            };
            // Core NATS has no replay: publish until the subscription is live.
            tokio::time::timeout(Duration::from_secs(10), async {
                while count(&context, "ALERTS").await == 0 {
                    publish("cold", -1.0).await;
                    publish("warmup", 1.0).await;
                    oracle.flush().await.unwrap();
                    sup.converge_once().await.unwrap();
                    assert_ne!(store.actual("js").unwrap().status, "failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("first filtered row in the stream");
            tokio::time::sleep(Duration::from_millis(300)).await;
            let c0 = count(&context, "ALERTS").await;
            // Lock-step: the NATS Core source is at-most-once and sheds on
            // slow-consumer bursts; this test counts the sink, not the source.
            for i in 0..20 {
                publish("cold", -(i as f64) - 1.0).await;
                publish("hot", i as f64 + 1.0).await;
                oracle.flush().await.unwrap();
                wait_count(&context, "ALERTS", c0 + i + 1, &sup, &store, "js").await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                count(&context, "ALERTS").await,
                c0 + 20,
                "no cold rows, no extras"
            );
            let snapshot = sup.flow_snapshot("js").unwrap().unwrap();
            assert_eq!(snapshot.sink_kind, "jetstream");
            sup.stop_all().await;
            let all = rows(&context, "ALERTS").await;
            assert!(
                all.iter().all(|r| r["device_id"] != "cold"),
                "WHERE drops cold rows"
            );
            assert_eq!(all.iter().filter(|r| r["device_id"] == "hot").count(), 20);
            let io = snapshot.diagnostics.snapshot();
            assert_eq!(io.jetstream_sink_acked, c0 + 20, "{io:?}");
            assert_eq!(io.jetstream_sink_failed, 0, "{io:?}");
            assert_eq!(io.jetstream_sink_fatal, 0, "{io:?}");
            assert_eq!(io.jetstream_sink_inflight, 0, "{io:?}");
        });
    }

    #[test]
    #[ignore = "requires SPARROW_NATS_SERVER (real nats-server binary)"]
    fn jetstream_sink_missing_stream_fails_the_job_and_never_creates_it() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let fixture = NatsSandbox::start().await;
            let context = fixture.context().await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", fixture.port).unwrap();
            let spec = parse(&live_spec(&fixture.url())).unwrap();
            store.put_pipeline("js", &spec, None).unwrap();
            request_start(&store, "js", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let error = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    sup.converge_once().await.unwrap();
                    if let Some(e) = store.actual("js").unwrap().last_error {
                        break e;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("missing stream must fail the job");
            assert!(error.contains("JetStream Sink failed closed"), "{error}");
            assert!(
                context.get_stream("ALERTS").await.is_err(),
                "never auto-created"
            );
            sup.stop_all().await;
        });
    }

    #[test]
    #[ignore = "requires SPARROW_NATS_SERVER (real nats-server binary)"]
    fn jetstream_sink_aligned_job_fails_closed_on_missing_stream() {
        let scratch = Scratch::new();
        let input = scratch.0.join("events.ndjson");
        append(&input, 1, 4);
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let fixture = NatsSandbox::start().await;
            let context = fixture.context().await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("events", EVENTS_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", fixture.port).unwrap();
            let value = aligned_spec(&fixture.url(), &input, &scratch.0.join("checkpoints"));
            store
                .put_pipeline("js", &parse(&value).unwrap(), None)
                .unwrap();
            request_start(&store, "js", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let error = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    sup.converge_once().await.unwrap();
                    if let Some(e) = store.actual("js").unwrap().last_error {
                        break e;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("aligned job must fail, not checkpoint past unconfirmed rows");
            assert!(error.contains("JetStream Sink failed closed"), "{error}");
            assert!(
                context.get_stream("OUT").await.is_err(),
                "never auto-created"
            );
            sup.stop_all().await;
        });
    }

    #[test]
    #[ignore = "requires SPARROW_NATS_SERVER (real nats-server binary)"]
    fn jetstream_sink_aligned_checkpoint_waits_for_pub_acks_and_restore_dedups() {
        let scratch = Scratch::new();
        let input = scratch.0.join("events.ndjson");
        append(&input, 1, 8);
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let fixture = NatsSandbox::start().await;
            let context = create_stream(&fixture, "OUT", "out.>").await;
            let store = Arc::new(Store::open(&scratch.0.join("catalog.db")).unwrap());
            store.put_stream("events", EVENTS_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", fixture.port).unwrap();
            let mut value = aligned_spec(&fixture.url(), &input, &scratch.0.join("checkpoints"));
            value["checkpoint"]["timeout_ms"] = json!(500);
            let spec = parse(&value).unwrap();
            store.put_pipeline("js", &spec, None).unwrap();
            request_start(&store, "js", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            wait_count(&context, "OUT", 4, &sup, &store, "js").await;
            let first = checkpoint(&sup).await;

            // Broker frozen: rows reach the sink but cannot be PubAcked, so
            // the barrier must not commit (outbox pending), and the job lives.
            fixture.pause();
            append(&input, 9, 12);
            tokio::time::sleep(Duration::from_millis(300)).await;
            let blocked = sup.checkpoint_named("js").await;
            assert!(blocked.is_err(), "checkpoint committed without PubAcks");
            tokio::time::sleep(Duration::from_millis(500)).await;
            let current = sup
                .checkpoint_inventory("js")
                .await
                .unwrap()
                .storage
                .current;
            assert_eq!(
                current,
                Some(first),
                "no commit while PubAcks are outstanding"
            );
            fixture.resume();
            wait_count(&context, "OUT", 6, &sup, &store, "js").await;
            let second = checkpoint(&sup).await;
            assert!(second > first);
            assert_eq!(store.actual("js").unwrap().status, "running");

            // Rows after the last checkpoint replay on restore; msg ids dedup.
            append(&input, 13, 16);
            wait_count(&context, "OUT", 8, &sup, &store, "js").await;
            sup.kill_named("js").await.unwrap();
            request_start(&store, "js", "test").unwrap();
            sup.converge_once().await.unwrap();
            let duplicates = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    sup.converge_once().await.unwrap();
                    if let Some(s) = sup.flow_snapshot("js").unwrap() {
                        let io = s.diagnostics.snapshot();
                        if io.jetstream_sink_duplicates >= 2 {
                            break io.jetstream_sink_duplicates;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("restore replays the uncheckpointed tail");
            assert_eq!(duplicates, 2);
            assert_eq!(
                sup.checkpoint_snapshot("js")
                    .unwrap()
                    .unwrap()
                    .1
                    .snapshot()
                    .restored_from,
                Some(second)
            );
            assert_eq!(
                count(&context, "OUT").await,
                8,
                "Nats-Msg-Id dedup on replay"
            );
            let ids: Vec<i64> = rows(&context, "OUT")
                .await
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect();
            assert_eq!(ids, vec![1, 3, 5, 7, 9, 11, 13, 15]);
            sup.stop_all().await;
            assert_eq!(kernel.admitted_jobs(), 0);
        });
    }

    async fn checkpoint(sup: &Arc<Supervisor>) -> u64 {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match sup.checkpoint_named("js").await {
                    Ok(id) => break id,
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await
        .expect("checkpoint commits once PubAcks arrive")
    }
}

//! Local DataBus through the real control path: spec matrix, validation, and
//! two pipelines chained inside one Supervisor (File -> bus -> SQL filter ->
//! HTTP), fan-out, overflow policies against a stalled consumer, and topic
//! cleanup on stop/restart/revision change.

#[cfg(feature = "demo-io")]
use crate::request_stop;
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

type Mutation = Box<dyn Fn(&mut Value)>;

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

/// Upstream: sealed File -> DataBus sink on `topic`.
fn upstream(path: &std::path::Path, topic: &str) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry",
        "source": {"kind": "file", "path": path, "file_contract": "sealed", "inbox_capacity": 8},
        "sink": {"kind": "databus", "outbox_capacity": 8, "databus": {"topic": topic}},
        "recovery": "restart_fresh"
    })
}

/// Downstream: DataBus source on `pattern` -> WHERE -> HTTP sink.
fn downstream(pattern: &str, sink_url: &str) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 0",
        "source": {"kind": "databus", "inbox_capacity": 8, "databus": {"topic": pattern}},
        "sink": {"kind": "http", "url": sink_url, "outbox_capacity": 8, "batch_rows": 8, "linger_ms": 1}
    })
}

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Input file with `n` rows; even ids are hot (> 0), odd ids cold.
fn input(n: usize) -> (Dir, PathBuf) {
    let dir = sparrow_connectors::ensure_default_data_root().join(format!(
        "databus-control-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("input.ndjson");
    let mut body = String::new();
    for i in 0..n {
        let t = if i % 2 == 0 { 1.0 + i as f64 } else { -1.0 };
        body.push_str(&json!({"device_id": format!("d{i}"), "temperature": t}).to_string());
        body.push('\n');
    }
    std::fs::write(&path, body).unwrap();
    (Dir(dir), path)
}

fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
    store
}

fn start(store: &Store, name: &str, value: &Value) {
    let etag = store
        .get_pipeline(name)
        .ok()
        .map(|row| format!("rev-{}", row.latest_revision));
    store
        .put_pipeline(name, &parse(value).unwrap(), etag.as_deref())
        .unwrap();
    request_start(store, name, "test").unwrap();
}

/// A sealed File producer may complete (and be reaped) quickly; keep its
/// diagnostics from the first converge that observes the running flow.
fn capture(
    sup: &Supervisor,
    name: &str,
    slot: &mut Option<Arc<sparrow_connectors::IoDiagnostics>>,
) {
    if slot.is_none() {
        *slot = sup.flow_snapshot(name).unwrap().map(|f| f.diagnostics);
    }
}

#[cfg(feature = "demo-io")]
fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
        .collect()
}

/// Converge until `done` holds; no pipeline may fail meanwhile.
async fn converge_until(
    sup: &Arc<Supervisor>,
    store: &Store,
    names: &[&str],
    what: &str,
    mut done: impl FnMut() -> bool,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            sup.converge_once().await.unwrap();
            for name in names {
                let actual = store.actual(name).unwrap();
                assert_ne!(actual.status, "failed", "{name}: {actual:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("deadline: {what}"));
}

#[cfg(feature = "demo-io")]
fn subscribers(sup: &Supervisor, pattern: &str) -> usize {
    sup.databus()
        .snapshot()
        .topics
        .iter()
        .find(|t| t.topic == pattern)
        .map_or(0, |t| t.subscribers)
}

/// HTTP endpoint that accepts connections but never answers, so a sink
/// pointed at it stalls and its pipeline stops draining the bus.
#[cfg(feature = "demo-io")]
async fn stalled_endpoint() -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    (port, task)
}

#[test]
fn databus_spec_matrix_rejects_mixed_fields_durable_claims_and_loops() {
    use sparrow_model::ErrorCode::*;
    let base = downstream("plant.>", "http://127.0.0.1:1/out");
    parse(&base).unwrap();
    let up = upstream(std::path::Path::new("/tmp/unused.ndjson"), "plant.raw");
    parse(&up).unwrap();
    let mutations: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "missing source block",
            Box::new(|v| {
                v["source"].as_object_mut().unwrap().remove("databus");
            }),
            InvalidArgument,
        ),
        (
            "source block on mqtt",
            Box::new(|v| v["source"]["kind"] = json!("mqtt")),
            InvalidArgument,
        ),
        (
            "unknown source field",
            Box::new(|v| v["source"]["databus"]["durable"] = json!(true)),
            InvalidArgument,
        ),
        (
            "mixed source host",
            Box::new(|v| v["source"]["host"] = json!("127.0.0.1")),
            InvalidArgument,
        ),
        (
            "mixed source path",
            Box::new(|v| v["source"]["path"] = json!("/tmp/x")),
            InvalidArgument,
        ),
        (
            "mixed source nats",
            Box::new(|v| {
                v["source"]["nats"] = json!({"servers":["nats://127.0.0.1:4222"],"subject":"x"})
            }),
            InvalidArgument,
        ),
        (
            "sink block on http",
            Box::new(|v| v["sink"]["databus"] = json!({"topic": "x"})),
            InvalidArgument,
        ),
        (
            "aligned",
            Box::new(|v| v["recovery"] = json!("aligned")),
            UnsupportedRestore,
        ),
        (
            "restore",
            Box::new(|v| v["restore"] = json!({"kind":"checkpoint","snapshot_id":"1"})),
            UnsupportedRestore,
        ),
        (
            "checkpoint_dir",
            Box::new(|v| v["checkpoint_dir"] = json!("/tmp/sparrow/x")),
            UnsupportedRestore,
        ),
        (
            "delivery",
            Box::new(|v| v["delivery"] = json!("checkpointed_at_least_once")),
            UnsupportedRestore,
        ),
        (
            "same-pipeline feedback loop",
            Box::new(|v| {
                v["sink"] =
                    json!({"kind":"databus","outbox_capacity":8,"databus":{"topic":"plant.hot"}})
            }),
            InvalidArgument,
        ),
    ];
    for (name, mutate, code) in mutations {
        let mut v = base.clone();
        mutate(&mut v);
        let err = parse(&v).expect_err(name);
        assert_eq!(err.code, code, "{name}: {}", err.message);
    }
    let sink_mutations: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "missing sink block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("databus");
            }),
            InvalidArgument,
        ),
        (
            "mixed sink url",
            Box::new(|v| v["sink"]["url"] = json!("http://127.0.0.1:1/")),
            InvalidArgument,
        ),
        (
            "mixed sink topic",
            Box::new(|v| v["sink"]["topic"] = json!("a/b")),
            InvalidArgument,
        ),
        (
            "unknown sink field",
            Box::new(|v| v["sink"]["databus"]["retain"] = json!(true)),
            InvalidArgument,
        ),
        (
            "aligned behind a File source",
            Box::new(|v| {
                v["recovery"] = json!("aligned");
                v["checkpoint_dir"] = json!("/tmp/sparrow/databus");
            }),
            UnsupportedRestore,
        ),
    ];
    for (name, mutate, code) in sink_mutations {
        let mut v = up.clone();
        mutate(&mut v);
        let err = parse(&v).expect_err(name);
        assert_eq!(err.code, code, "{name}: {}", err.message);
    }
    // Disjoint topics are not a loop; round trip keeps optional fields absent.
    let mut v = base.clone();
    v["sink"] = json!({"kind":"databus","outbox_capacity":8,"databus":{"topic":"alerts.hot"}});
    let spec = parse(&v).unwrap();
    let stored = serde_json::to_value(&spec).unwrap();
    assert!(stored["source"]["databus"].get("overflow").is_none());
    assert!(stored["sink"]["databus"].get("flush_timeout_ms").is_none());
    assert_eq!(parse(&stored).unwrap(), spec);
    assert_eq!(crate::replay_label_for_source("databus"), "unsupported");
    let caps = crate::capabilities_json();
    for kind in ["databus", "databus_sink"] {
        let c = caps["connectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["kind"] == kind)
            .unwrap_or_else(|| panic!("{kind} in capabilities"))
            .clone();
        assert_eq!(c["replay"], "unsupported", "{c}");
        assert_eq!(c["delivery"], "live_best_effort", "{c}");
        assert_eq!(c["enabled_by_build"], true, "{c}");
    }
}

#[test]
fn databus_validation_bounds_and_reservation_refusal() {
    use sparrow_model::ErrorCode::*;
    let schema = crate::stream_schema(
        "telemetry",
        &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
    )
    .unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(Default::default());
    let policy = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1);
    let check = |value: &Value| {
        crate::validate_io(&parse(value).unwrap(), &schema, &secrets, &policy, None)
            .map_err(|e| e.code)
    };
    let base = downstream("plant.>", "http://127.0.0.1:1/out");
    check(&base).unwrap();
    let (_dir, path) = input(1);
    let up = upstream(&path, "plant.raw");
    check(&up).unwrap();
    let cases: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "empty token",
            Box::new(|v| v["source"]["databus"]["topic"] = json!("plant..x")),
            InvalidArgument,
        ),
        (
            "> not last",
            Box::new(|v| v["source"]["databus"]["topic"] = json!("a.>.b")),
            InvalidArgument,
        ),
        (
            "bad overflow",
            Box::new(|v| v["source"]["databus"]["overflow"] = json!("spill")),
            InvalidArgument,
        ),
        (
            "zero capacity",
            Box::new(|v| v["source"]["databus"]["buffer_capacity"] = json!(0)),
            BoundExceeded,
        ),
        (
            "block timeout too long",
            Box::new(|v| {
                v["source"]["databus"]["overflow"] = json!("block");
                v["source"]["databus"]["block_timeout_ms"] = json!(60_000);
            }),
            BoundExceeded,
        ),
        (
            "buffer over the job reservation",
            Box::new(|v| v["source"]["databus"]["buffer_bytes"] = json!(4 * 1024 * 1024)),
            BoundExceeded,
        ),
        // The reservation total is computed before per-endpoint bounds:
        // huge values must saturate into a refusal, not overflow.
        (
            "huge buffer_bytes",
            Box::new(|v| v["source"]["databus"]["buffer_bytes"] = json!(usize::MAX)),
            BoundExceeded,
        ),
        (
            "huge buffer_capacity",
            Box::new(|v| v["source"]["databus"]["buffer_capacity"] = json!(usize::MAX)),
            BoundExceeded,
        ),
    ];
    for (name, mutate, code) in cases {
        let mut v = base.clone();
        mutate(&mut v);
        assert_eq!(check(&v), Err(code), "{name}");
    }
    let mut v = up.clone();
    v["sink"]["databus"]["topic"] = json!("plant.*");
    assert_eq!(check(&v), Err(InvalidArgument), "publish topic is literal");
    // Two graph inputs each fit half the reservation but not 3/4 together.
    let mut v = base.clone();
    v.as_object_mut().unwrap().remove("sql");
    v["source"]["databus"]["buffer_bytes"] = json!(1536 * 1024);
    v["graph"] = json!({
        "version": 1, "pipeline_id": 904, "revision_id": 1,
        "nodes": [
            {"id": 1, "kind": "memory_source", "table": "telemetry", "out": [3]},
            {"id": 2, "kind": "memory_source", "table": "telemetry", "out": [3]},
            {"id": 3, "kind": "union_all", "out": [4]},
            {"id": 4, "kind": "capture_sink", "name": "out"}
        ]
    });
    v["graph_io"] = json!({
        "sources": {"1": v["source"].clone(), "2": v["source"].clone()},
        "sinks": {"4": v["sink"].clone()}
    });
    let err = crate::validate_io(&parse(&v).unwrap(), &schema, &secrets, &policy, None)
        .expect_err("sum of DataBus buffers");
    assert_eq!(err.code, BoundExceeded);
    assert!(err.message.contains("DataBus"), "{err:?}");
    // One such input alone fits.
    let mut one = v.clone();
    one["graph_io"]["sources"]["2"]["databus"]["buffer_bytes"] = json!(64 * 1024);
    check(&one).unwrap();
}

#[test]
fn databus_nonlegacy_graph_feedback_is_rejected_by_parse_bind_and_public_validation() {
    use sparrow_model::ErrorCode;
    let (_dir, path) = input(1);
    let catalog = store();
    let file = json!({"kind":"file", "path":path, "file_contract":"sealed", "inbox_capacity":8});
    let source = json!({"kind":"databus", "inbox_capacity":8, "databus":{"topic":"plant.>"}});
    let log = json!({"kind":"log", "outbox_capacity":8});
    let sink = json!({"kind":"databus", "outbox_capacity":8, "databus":{"topic":"alerts.raw"}});
    let mut value = json!({
        "version":1, "stream":"telemetry", "source":file, "sink":log,
        "graph":{"version":1, "pipeline_id":906, "revision_id":1, "nodes":[
            {"id":1, "kind":"memory_source", "table":"telemetry", "out":[3]},
            {"id":2, "kind":"memory_source", "table":"telemetry", "out":[3]},
            {"id":3, "kind":"union_all", "out":[4]},
            {"id":4, "kind":"branch", "out":[5,6]},
            {"id":5, "kind":"capture_sink", "name":"log"},
            {"id":6, "kind":"capture_sink", "name":"bus"}
        ]},
        "graph_io":{"sources":{"1":file, "2":source}, "sinks":{"5":log, "6":sink}}
    });
    let valid = parse(&value).unwrap();
    assert_eq!(valid.source.kind, "file");
    assert_eq!(valid.sink.kind, "log");
    let plan = crate::bind_plan_with_store(&catalog, &valid, "graph-feedback", 1).unwrap();
    let schema = crate::stream_to_schema(&catalog.get_stream("telemetry").unwrap()).unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::empty();
    let policy = sparrow_connectors::TargetPolicy::deny_all();
    crate::validate_io(&valid, &schema, &secrets, &policy, None).unwrap();
    crate::validate::validate_io_with_plan(&valid, &schema, &plan, &secrets, &policy, None)
        .unwrap();
    // Only the non-lowest Sink changes. The exact same bound graph was valid
    // above, so these refusals cannot be explained by a malformed topology.
    value["graph_io"]["sinks"]["6"]["databus"]["topic"] = json!("plant.raw");
    let error = parse(&value).unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(error.message.contains("feedback loop"), "{error}");
    let typed: PipelineSpec = serde_json::from_value(value).unwrap();
    let error = typed.validate().unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(error.message.contains("feedback loop"), "{error}");
    assert_eq!(
        crate::bind_plan_with_store(&catalog, &typed, "graph-feedback", 1)
            .unwrap_err()
            .code,
        ErrorCode::InvalidArgument
    );
    for error in [
        crate::validate_io(&typed, &schema, &secrets, &policy, None).unwrap_err(),
        crate::validate::validate_io_with_plan(&typed, &schema, &plan, &secrets, &policy, None)
            .unwrap_err(),
    ] {
        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("feedback loop"), "{error}");
    }
    // A typed caller also cannot request durability for a DataBus endpoint.
    let mut durable = valid.clone();
    durable.recovery = "aligned".into();
    assert_eq!(
        crate::validate_io(&durable, &schema, &secrets, &policy, None)
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedRestore
    );
}

#[test]
#[cfg(feature = "demo-io")]
fn databus_chains_two_pipelines_with_exact_counts_and_fan_out() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        const N: usize = 200;
        let (_dir, path) = input(N);
        let a = sparrow_connectors::HttpCapture::start().await.unwrap();
        let b = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", a.port()).unwrap();
        store.put_allow("127.0.0.1", b.port()).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        // Both consumers attach before the producer exists (no replay).
        start(&store, "down-a", &downstream("plant.raw", &a.url()));
        start(&store, "down-b", &downstream("plant.>", &b.url()));
        let all = ["down-a", "down-b", "up"];
        converge_until(&sup, &store, &all[..2], "subscribed", || {
            subscribers(&sup, "plant.raw") == 1 && subscribers(&sup, "plant.>") == 1
        })
        .await;
        start(&store, "up", &upstream(&path, "plant.raw"));
        let mut up = None;
        converge_until(&sup, &store, &all, "fan-out output", || {
            capture(&sup, "up", &mut up);
            output(&a).len() >= N / 2 && output(&b).len() >= N / 2
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        for http in [&a, &b] {
            let rows = output(http);
            assert_eq!(rows.len(), N / 2, "exact count, no duplicates");
            let ids: Vec<_> = rows.iter().map(|r| r["device_id"].clone()).collect();
            let expected: Vec<_> = (0..N).step_by(2).map(|i| json!(format!("d{i}"))).collect();
            assert_eq!(ids, expected, "per-publisher order is preserved");
        }
        let io = up.expect("producer observed").snapshot();
        assert_eq!(io.databus_sink_published, N as u64, "{io:?}");
        assert_eq!(io.databus_sink_deliveries, 2 * N as u64, "{io:?}");
        assert_eq!(io.databus_sink_no_subscribers, 0);
        assert_eq!(io.databus_sink_fatal, 0);
        for name in ["down-a", "down-b"] {
            let flow = sup.flow_snapshot(name).unwrap().unwrap();
            assert_eq!(flow.source_kind, "databus");
            let io = flow.diagnostics.snapshot();
            assert_eq!(io.databus_source_rows, N as u64, "{name}: {io:?}");
            assert_eq!(
                io.databus_source_dropped_oldest
                    + io.databus_source_dropped_newest
                    + io.databus_source_dropped_bad
                    + io.databus_source_dropped_budget,
                0,
                "{name}: {io:?}"
            );
        }
        sup.stop_all().await;
        assert!(
            sup.databus().snapshot().topics.is_empty(),
            "no leaked topics"
        );
        a.stop().await;
        b.stop().await;
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn databus_overflow_policies_count_against_a_stalled_consumer() {
    for policy in ["drop_newest", "drop_oldest", "block"] {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            const N: usize = 200;
            let (_dir, path) = input(N);
            let (stalled, server) = stalled_endpoint().await;
            let healthy = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = store();
            store.put_allow("127.0.0.1", stalled).unwrap();
            store.put_allow("127.0.0.1", healthy.port()).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let mut slow = downstream("plant.raw", &format!("http://127.0.0.1:{stalled}/out"));
            slow["source"]["inbox_capacity"] = json!(1);
            slow["source"]["databus"]["buffer_capacity"] = json!(4);
            slow["source"]["databus"]["overflow"] = json!(policy);
            slow["source"]["databus"]["block_timeout_ms"] = json!(5);
            slow["sink"]["outbox_capacity"] = json!(1);
            slow["sink"]["batch_rows"] = json!(1);
            slow["sql"] = json!("SELECT device_id, temperature FROM telemetry");
            start(&store, "slow", &slow);
            start(&store, "fast", &downstream("plant.raw", &healthy.url()));
            converge_until(&sup, &store, &["slow", "fast"], "subscribed", || {
                subscribers(&sup, "plant.raw") == 2
            })
            .await;
            start(&store, "up", &upstream(&path, "plant.raw"));
            let mut up = None;
            converge_until(&sup, &store, &["fast", "up"], "healthy output", || {
                capture(&sup, "up", &mut up);
                output(&healthy).len() >= N / 2
            })
            .await;
            let up = up.expect("producer observed");
            converge_until(&sup, &store, &["fast", "up"], "all published", || {
                up.snapshot().databus_sink_published == N as u64
            })
            .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            // The stalled subscriber never stalls the healthy one.
            assert_eq!(output(&healthy).len(), N / 2, "{policy}");
            let slow = sup
                .flow_snapshot("slow")
                .unwrap()
                .unwrap()
                .diagnostics
                .snapshot();
            let fast = sup
                .flow_snapshot("fast")
                .unwrap()
                .unwrap()
                .diagnostics
                .snapshot();
            let io = up.snapshot();
            assert_eq!(fast.databus_source_received, N as u64, "{policy}: {fast:?}");
            // Every publish is either accepted by a subscriber buffer or
            // counted as dropped/timed out by exactly one policy counter.
            let rejected = slow.databus_source_dropped_newest + slow.databus_source_block_timeouts;
            assert_eq!(
                io.databus_sink_deliveries + rejected,
                2 * N as u64,
                "{policy}: {io:?} {slow:?}"
            );
            match policy {
                "drop_newest" => {
                    assert!(slow.databus_source_dropped_newest > 0, "{slow:?}");
                    assert_eq!(
                        slow.databus_source_dropped_oldest + slow.databus_source_block_timeouts,
                        0
                    );
                }
                "drop_oldest" => {
                    assert!(slow.databus_source_dropped_oldest > 0, "{slow:?}");
                    assert_eq!(rejected, 0, "{slow:?}");
                }
                _ => {
                    assert!(slow.databus_source_block_timeouts > 0, "{slow:?}");
                    assert!(io.databus_sink_blocked_publishes > 0, "{io:?}");
                    assert_eq!(
                        slow.databus_source_dropped_oldest + slow.databus_source_dropped_newest,
                        0
                    );
                }
            }
            assert!(slow.databus_source_buffer_items <= 4, "{slow:?}");
            sup.stop_all().await;
            assert!(sup.databus().snapshot().topics.is_empty(), "{policy}");
            healthy.stop().await;
            server.abort();
        });
    }
}

#[test]
#[cfg(feature = "demo-io")]
fn databus_stop_restart_and_revision_change_release_topics_and_reattach() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let usage = || sup.flow_snapshot("down").unwrap().map(|f| f.diagnostics);
        start(&store, "down", &downstream("plant.raw", &http.url()));
        converge_until(&sup, &store, &["down"], "subscribed", || {
            subscribers(&sup, "plant.raw") == 1
        })
        .await;
        assert_eq!(usage().unwrap().snapshot().databus_source_subscriptions, 1);
        // Stop releases the subscription (and its reservation) immediately.
        request_stop(&store, "down", "test").unwrap();
        converge_until(&sup, &store, &["down"], "unsubscribed", || {
            sup.databus().snapshot().topics.is_empty()
        })
        .await;
        // Restart re-attaches a fresh subscription to the same topic.
        request_start(&store, "down", "test").unwrap();
        converge_until(&sup, &store, &["down"], "re-attached", || {
            subscribers(&sup, "plant.raw") == 1
        })
        .await;
        // A producer started after re-attach is delivered exactly.
        let (_dir, path) = input(20);
        start(&store, "up", &upstream(&path, "plant.raw"));
        converge_until(&sup, &store, &["down", "up"], "re-attached output", || {
            output(&http).len() >= 10
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(output(&http).len(), 10);
        // A new revision on another topic drops the old registration.
        start(&store, "down", &downstream("plant.other", &http.url()));
        converge_until(&sup, &store, &["down"], "revision switch", || {
            subscribers(&sup, "plant.other") == 1 && subscribers(&sup, "plant.raw") == 0
        })
        .await;
        sup.stop_all().await;
        assert!(sup.databus().snapshot().topics.is_empty());
        http.stop().await;
    });
}

#[test]
fn databus_publisher_without_subscribers_completes_and_counts() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (_dir, path) = input(10);
        let store = store();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        start(&store, "up", &upstream(&path, "plant.raw"));
        let mut diag = None;
        converge_until(&sup, &store, &["up"], "completed", || {
            capture(&sup, "up", &mut diag);
            store.actual("up").unwrap().status == "completed"
        })
        .await;
        let io = diag.expect("flow observed").snapshot();
        assert_eq!(io.databus_sink_published, 10, "{io:?}");
        assert_eq!(io.databus_sink_no_subscribers, 10, "{io:?}");
        assert_eq!(io.databus_sink_deliveries, 0);
        assert!(sup.databus().snapshot().topics.is_empty());
        sup.stop_all().await;
    });
}

#[test]
fn databus_sink_registration_failure_fails_the_job_closed() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (_dir, path) = input(10);
        let store = store();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        // Exhaust the runtime publisher limit from outside the pipeline.
        let held: Vec<_> = (0..sparrow_connectors::databus::MAX_PUBLISHERS)
            .map(|_| sup.databus().publisher("other.x").unwrap())
            .collect();
        start(&store, "up", &upstream(&path, "plant.raw"));
        let mut diag = None;
        converge_until(&sup, &store, &[], "failed", || {
            capture(&sup, "up", &mut diag);
            store.actual("up").unwrap().status == "failed"
        })
        .await;
        let actual = store.actual("up").unwrap();
        let error = actual.last_error.unwrap_or_default();
        assert!(error.contains("DataBus Sink"), "{error}");
        if let Some(diag) = diag {
            assert_eq!(diag.snapshot().databus_sink_published, 0);
        }
        drop(held);
        sup.stop_all().await;
        assert!(sup.databus().snapshot().topics.is_empty());
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn databus_graph_io_sources_are_budgeted_and_deliver() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (_dir_a, path_a) = input(10);
        let (_dir_b, path_b) = input(6);
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let mut value = downstream("plant.a", &http.url());
        value.as_object_mut().unwrap().remove("sql");
        let mut second = value["source"].clone();
        second["databus"]["topic"] = json!("plant.b");
        value["graph"] = json!({
            "version": 1, "pipeline_id": 905, "revision_id": 1,
            "nodes": [
                {"id": 1, "kind": "memory_source", "table": "telemetry", "out": [3]},
                {"id": 2, "kind": "memory_source", "table": "telemetry", "out": [3]},
                {"id": 3, "kind": "union_all", "out": [4]},
                {"id": 4, "kind": "capture_sink", "name": "out"}
            ]
        });
        value["graph_io"] = json!({
            "sources": {"1": value["source"].clone(), "2": second},
            "sinks": {"4": value["sink"].clone()}
        });
        start(&store, "graph", &value);
        converge_until(&sup, &store, &["graph"], "subscribed", || {
            subscribers(&sup, "plant.a") == 1 && subscribers(&sup, "plant.b") == 1
        })
        .await;
        start(&store, "up-a", &upstream(&path_a, "plant.a"));
        start(&store, "up-b", &upstream(&path_b, "plant.b"));
        converge_until(&sup, &store, &["graph", "up-a", "up-b"], "union", || {
            output(&http).len() >= 16
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(output(&http).len(), 16, "no filter in this graph");
        sup.stop_all().await;
        assert!(sup.databus().snapshot().topics.is_empty());
        http.stop().await;
    });
}

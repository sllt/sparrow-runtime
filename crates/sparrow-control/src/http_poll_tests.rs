//! HTTP Poll Source through the real control path: catalog spec -> validate
//! -> Supervisor -> Kernel (SQL filter) -> HTTP sink, plus the spec matrix.

use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

/// Loopback API returning one hot and one cold reading per poll.
async fn api() -> (u16, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = polls.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let n = counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    head.push(byte[0]);
                }
                let body = format!(
                    r#"[{{"device_id":"hot","temperature":{}}},{{"device_id":"cold","temperature":-5.0}}]"#,
                    30 + n
                );
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            });
        }
    });
    (port, polls, task)
}

fn linear_spec(api_port: u16, sink_url: &str) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 0",
        "source": {
            "kind": "http_poll",
            "inbox_capacity": 8,
            "http_poll": {
                "url": format!("http://127.0.0.1:{api_port}/readings?site=a"),
                "interval_ms": 100,
                "timeout_ms": 1000,
                "headers": [{"name": "X-Tenant", "value": "t1"}]
            }
        },
        "sink": {"kind": "http", "url": sink_url, "outbox_capacity": 8, "batch_rows": 8, "linger_ms": 1}
    })
}

type Mutation = Box<dyn Fn(&mut Value)>;

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
        .collect()
}

async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    n: usize,
    sup: &Arc<Supervisor>,
    store: &Arc<Store>,
    name: &str,
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while output(http).len() < n {
            sup.converge_once().await.unwrap();
            let actual = store.actual(name).unwrap();
            assert_ne!(actual.status, "failed", "{actual:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("HTTP poll pipeline output deadline");
}

#[test]
fn http_poll_linear_pipeline_polls_filters_and_delivers() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (port, polls, server) = api().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        store.put_allow("127.0.0.1", port).unwrap();
        let spec = parse(&linear_spec(port, &http.url())).unwrap();
        store.put_pipeline("poll", &spec, None).unwrap();
        request_start(&store, "poll", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 3, &sup, &store, "poll").await;
        let rows = output(&http);
        assert!(
            rows.iter().all(|r| r["device_id"] == "hot"),
            "WHERE must drop cold rows: {rows:?}"
        );
        assert_eq!(rows[0]["temperature"], json!(30.0));
        assert!(polls.load(Ordering::SeqCst) >= 3);
        let snapshot = sup.flow_snapshot("poll").unwrap().unwrap();
        assert_eq!(snapshot.source_kind, "http_poll");
        let io = snapshot.diagnostics.snapshot();
        assert!(io.http_poll_rows >= 6 && io.http_poll_ok >= 3, "{io:?}");
        assert!(sup.io_snapshot().await.http_poll_inflight <= 1);
        sup.stop_all().await;
        http.stop().await;
        server.abort();
    });
}

#[test]
fn http_poll_graph_io_source_is_budgeted_and_delivers() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (port, _polls, server) = api().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        store.put_allow("127.0.0.1", port).unwrap();
        let mut value = linear_spec(port, &http.url());
        value.as_object_mut().unwrap().remove("sql");
        // Two budgeted HTTP Poll inputs merged by union_all force the DAG path.
        value["graph"] = json!({
            "version": 1, "pipeline_id": 902, "revision_id": 1,
            "nodes": [
                {"id": 1, "kind": "memory_source", "table": "telemetry", "out": [3]},
                {"id": 2, "kind": "memory_source", "table": "telemetry", "out": [3]},
                {"id": 3, "kind": "union_all", "out": [4]},
                {"id": 4, "kind": "capture_sink", "name": "out"}
            ]
        });
        value["graph_io"] = json!({
            "sources": {"1": value["source"].clone(), "2": value["source"].clone()},
            "sinks": {"4": value["sink"].clone()}
        });
        let spec = parse(&value).unwrap();
        store.put_pipeline("poll-graph", &spec, None).unwrap();
        request_start(&store, "poll-graph", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 4, &sup, &store, "poll-graph").await;
        let rows = output(&http);
        assert!(
            rows.iter().any(|r| r["device_id"] == "cold"),
            "no filter in this graph"
        );
        sup.stop_all().await;
        http.stop().await;
        server.abort();
    });
}

#[test]
fn http_poll_spec_matrix_rejects_mixed_fields_and_durable_claims() {
    let base = linear_spec(8080, "http://127.0.0.1:1/out");
    parse(&base).unwrap();
    let mutations: Vec<(&str, Mutation)> = vec![
        (
            "missing block",
            Box::new(|v| {
                v["source"].as_object_mut().unwrap().remove("http_poll");
            }),
        ),
        (
            "block on mqtt",
            Box::new(|v| v["source"]["kind"] = json!("mqtt")),
        ),
        (
            "unknown field",
            Box::new(|v| v["source"]["http_poll"]["method"] = json!("POST")),
        ),
        (
            "unknown auth type",
            Box::new(|v| {
                v["source"]["http_poll"]["auth"] = json!({"type":"digest","token_secret":"x"})
            }),
        ),
        (
            "inline auth value",
            Box::new(|v| v["source"]["http_poll"]["auth"] = json!({"type":"bearer","token":"x"})),
        ),
        (
            "mixed host",
            Box::new(|v| v["source"]["host"] = json!("127.0.0.1")),
        ),
        ("mixed tls", Box::new(|v| v["source"]["tls"] = json!(true))),
        (
            "mixed inbox_bytes",
            Box::new(|v| v["source"]["inbox_bytes"] = json!(1024)),
        ),
        ("aligned", Box::new(|v| v["recovery"] = json!("aligned"))),
        (
            "restore",
            Box::new(|v| v["restore"] = json!({"kind":"checkpoint","snapshot_id":"1"})),
        ),
        (
            "checkpoint_dir",
            Box::new(|v| v["checkpoint_dir"] = json!("/tmp/sparrow/x")),
        ),
        (
            "delivery",
            Box::new(|v| v["delivery"] = json!("checkpointed_at_least_once")),
        ),
    ];
    for (name, mutate) in mutations {
        let mut value = base.clone();
        mutate(&mut value);
        assert!(
            parse(&value).is_err(),
            "{name} must be rejected at spec parse"
        );
    }
    // Connector-level validation (secrets/policy) runs in validate_io.
    let schema = crate::stream_schema(
        "telemetry",
        &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
    )
    .unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(
        [("tok".to_string(), "value".to_string())]
            .into_iter()
            .collect(),
    );
    let sink_only = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1);
    let allowed = sink_only.clone().with_allow("127.0.0.1", 8080);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        crate::validate_io(&parse(value).unwrap(), &schema, &secrets, policy, None)
            .map_err(|e| e.code)
    };
    check(&base, &allowed).unwrap();
    use sparrow_model::ErrorCode::*;
    assert_eq!(check(&base, &sink_only), Err(PolicyDenied));
    // Default backoff ceiling follows long intervals instead of rejecting them.
    let mut slow = base.clone();
    slow["source"]["http_poll"]["interval_ms"] = json!(2 * 3600 * 1000);
    check(&slow, &allowed).unwrap();
    let mut v = base.clone();
    v["source"]["http_poll"]["auth"] = json!({"type":"bearer","token_secret":"tok"});
    assert_eq!(
        check(&v, &allowed),
        Err(PolicyDenied),
        "credentials over plain HTTP"
    );
    v["source"]["http_poll"]["url"] = json!("https://127.0.0.1:8080/readings");
    check(&v, &allowed).unwrap();
    v["source"]["http_poll"]["auth"] = json!({"type":"bearer","token_secret":"absent"});
    assert_eq!(check(&v, &allowed), Err(SecretMissing));
    let mut v = base.clone();
    v["source"]["http_poll"]["format"] = json!("csv");
    assert_eq!(check(&v, &allowed), Err(InvalidArgument));
    let mut v = base.clone();
    v["source"]["http_poll"]["interval_ms"] = json!(10);
    assert_eq!(check(&v, &allowed), Err(BoundExceeded));
    let mut v = base.clone();
    v["source"]["http_poll"]["max_response_bytes"] = json!(8 * 1024 * 1024);
    assert_eq!(check(&v, &allowed), Err(BoundExceeded));
    assert_eq!(crate::replay_label_for_source("http_poll"), "unsupported");
    // Stored spec round-trips without growing new defaults into the JSON.
    let stored = serde_json::to_value(parse(&base).unwrap()).unwrap();
    assert!(stored["source"]["http_poll"].get("auth").is_none());
    assert!(stored["source"]["http_poll"].get("conditional").is_none());
    let caps = crate::capabilities_json();
    let poll = caps["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == "http_poll")
        .unwrap();
    assert_eq!(poll["replay"], "unsupported");
    assert_eq!(poll["delivery"], "live_best_effort");
}

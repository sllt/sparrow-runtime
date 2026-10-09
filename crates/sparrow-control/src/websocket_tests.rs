//! WebSocket Source/Sink through the real control path: catalog spec -> validate
//! -> Supervisor -> Kernel (SQL filter) -> WebSocket sink, against in-process
//! tokio-tungstenite servers, plus the spec/validate matrix.

use crate::PipelineSpec;
use serde_json::{json, Value};

#[cfg_attr(not(feature = "websocket"), allow(dead_code))]
const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

fn linear_spec(source_port: u16, sink_port: u16) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 0",
        "source": {
            "kind": "websocket",
            "inbox_capacity": 8,
            "websocket": {"url": format!("ws://127.0.0.1:{source_port}/in")}
        },
        "sink": {
            "kind": "websocket",
            "outbox_capacity": 8,
            "websocket": {"url": format!("ws://127.0.0.1:{sink_port}/out")}
        }
    })
}

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

#[cfg(not(feature = "websocket"))]
#[test]
fn websocket_spec_without_the_feature_is_feature_unavailable() {
    let err = parse(&linear_spec(9001, 9002)).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::FeatureUnavailable);
}

#[cfg(feature = "websocket")]
type Mutation = Box<dyn Fn(&mut Value)>;

#[cfg(feature = "websocket")]
#[test]
fn websocket_spec_matrix_rejects_mixed_fields_and_durable_claims() {
    let base = linear_spec(9001, 9002);
    parse(&base).unwrap();
    let mutations: Vec<(&str, Mutation)> = vec![
        (
            "missing source block",
            Box::new(|v| {
                v["source"].as_object_mut().unwrap().remove("websocket");
            }),
        ),
        (
            "missing sink block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("websocket");
            }),
        ),
        (
            "source block on mqtt",
            Box::new(|v| v["source"]["kind"] = json!("mqtt")),
        ),
        (
            "sink block on log",
            Box::new(|v| v["sink"]["kind"] = json!("log")),
        ),
        (
            "unknown source field",
            Box::new(|v| v["source"]["websocket"]["listen"] = json!("0.0.0.0:1")),
        ),
        (
            "unknown sink field",
            Box::new(|v| v["sink"]["websocket"]["ack"] = json!(true)),
        ),
        (
            "inline token",
            Box::new(|v| v["source"]["websocket"]["token"] = json!("x")),
        ),
        (
            "unknown framing",
            Box::new(|v| v["source"]["websocket"]["framing"] = json!("lines")),
        ),
        (
            "unknown overflow",
            Box::new(|v| v["sink"]["websocket"]["overflow"] = json!("drop_oldest")),
        ),
        (
            "mixed source host",
            Box::new(|v| v["source"]["host"] = json!("127.0.0.1")),
        ),
        (
            "mixed source nats",
            Box::new(|v| {
                v["source"]["nats"] = json!({"servers":["nats://127.0.0.1:4222"],"subject":"a"})
            }),
        ),
        (
            "mixed source databus",
            Box::new(|v| v["source"]["databus"] = json!({"topic":"a"})),
        ),
        (
            "mixed sink url",
            Box::new(|v| v["sink"]["url"] = json!("http://127.0.0.1:1/out")),
        ),
        (
            "mixed sink topic",
            Box::new(|v| v["sink"]["topic"] = json!("a/b")),
        ),
        (
            "mixed sink action",
            Box::new(|v| v["sink"]["action"] = json!({"kind":"x"})),
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
    // A WebSocket sink behind a durable-capable source still refuses durable claims.
    let mut v = base.clone();
    v["source"] = json!({"kind":"file","path":"/tmp/sparrow/in.ndjson"});
    parse(&v).unwrap();
    v["recovery"] = json!("aligned");
    v["delivery"] = json!("checkpointed_at_least_once");
    v["checkpoint_dir"] = json!("/tmp/sparrow/ws-sink");
    let err = parse(&v).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(err.message.contains("WebSocket"), "{err:?}");

    let schema = crate::stream_schema(
        "telemetry",
        &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
    )
    .unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(
        [("tok".to_string(), "s3cr3t-value".to_string())]
            .into_iter()
            .collect(),
    );
    let allowed =
        sparrow_connectors::TargetPolicy::allow("127.0.0.1", 9001).with_allow("127.0.0.1", 9002);
    let denied = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        crate::validate_io(&parse(value).unwrap(), &schema, &secrets, policy, None)
    };
    let code = |value: &Value| check(value, &allowed).map_err(|e| e.code);
    use sparrow_model::ErrorCode::*;
    check(&base, &allowed).unwrap();
    assert_eq!(check(&base, &denied).unwrap_err().code, PolicyDenied);
    let mut v = base.clone();
    v["source"]["websocket"]["auth"] = json!({"type":"bearer","token_secret":"tok"});
    assert_eq!(code(&v), Err(PolicyDenied), "credentials over plain ws://");
    v["source"]["websocket"]["url"] = json!("wss://127.0.0.1:9001/in");
    code(&v).unwrap();
    v["source"]["websocket"]["auth"] = json!({"type":"bearer","token_secret":"absent"});
    assert_eq!(code(&v), Err(SecretMissing));
    let mut v = base.clone();
    v["sink"]["websocket"]["headers"] = json!([{"name":"X-Key","value_secret":"tok"}]);
    let err = check(&v, &allowed).unwrap_err();
    assert_eq!(err.code, PolicyDenied, "secret header over plain ws://");
    assert!(!err.message.contains("s3cr3t"), "{err:?}");
    let mut v = base.clone();
    v["source"]["websocket"]["url"] = json!("ws://user:pw@127.0.0.1:9001/in");
    assert!(code(&v).is_err(), "userinfo in URL");
    let mut v = base.clone();
    v["source"]["websocket"]["url"] = json!("http://127.0.0.1:9001/in");
    assert!(code(&v).is_err(), "scheme must be ws/wss");
    let mut v = base.clone();
    v["source"]["websocket"]["headers"] = json!([{"name":"Sec-WebSocket-Key","value":"x"}]);
    assert!(code(&v).is_err(), "reserved header");
    let mut v = base.clone();
    v["source"]["websocket"]["reconnect_attempts"] = json!(0);
    assert_eq!(code(&v), Err(BoundExceeded), "unbounded reconnect");
    let mut v = base.clone();
    v["source"]["websocket"]["idle_timeout_ms"] = json!(1000);
    v["source"]["websocket"]["ping_interval_ms"] = json!(1000);
    assert!(code(&v).is_err(), "idle timeout must exceed ping interval");
    let mut v = base.clone();
    v["sink"]["websocket"]["max_message_bytes"] = json!(1024 * 1024);
    assert_eq!(
        code(&v),
        Err(BoundExceeded),
        "sink queue x message must fit half the compact reservation"
    );
    // Each endpoint fits half the reservation, but the pair must fit 3/4.
    // Source: 128 KiB + 2 x 640 KiB = 1.375 MiB. Sink: 128 KiB + (4 + q) x
    // 256 KiB = 1.375 / 1.875 MiB for q = 1 / 3 (each <= 2 MiB, sum 2.75 /
    // 3.25 MiB against 3 MiB).
    let mut v = base.clone();
    v["source"]["websocket"]["max_message_bytes"] = json!(256 * 1024);
    v["source"]["websocket"]["prefetch_capacity"] = json!(1);
    v["sink"]["websocket"]["max_message_bytes"] = json!(256 * 1024);
    v["sink"]["websocket"]["queue_capacity"] = json!(1);
    code(&v).unwrap();
    v["sink"]["websocket"]["queue_capacity"] = json!(3);
    assert_eq!(code(&v), Err(BoundExceeded), "sum of WebSocket buffers");
    for capacity in [0, 65, usize::MAX] {
        let mut v = base.clone();
        v["source"]["websocket"]["prefetch_capacity"] = json!(capacity);
        assert_eq!(code(&v), Err(BoundExceeded));
    }
    let parsed = parse(&base).unwrap();
    let config =
        parsed
            .source
            .websocket
            .as_ref()
            .unwrap()
            .connector_config(schema.clone(), 8, false);
    assert_eq!(config.prefetch_capacity, 4);
    let mut upper = base.clone();
    upper["source"]["websocket"]["prefetch_capacity"] = json!(64);
    // The supported upper capacity still has to fit the real reservation;
    // lower the message ceiling, not the job budget, for this positive case.
    upper["source"]["websocket"]["max_message_bytes"] = json!(1024);
    code(&upper).unwrap();
    let upper = parse(&upper).unwrap();
    let config =
        upper
            .source
            .websocket
            .as_ref()
            .unwrap()
            .connector_config(schema.clone(), 8, false);
    assert_eq!(
        config.prefetch_capacity, 64,
        "explicit capacity must not be clamped"
    );

    // Frames x formats.
    let mut v = base.clone();
    v["source"]["format"] = json!("csv");
    code(&v).unwrap();
    v["source"]["websocket"]["framing"] = json!("ndjson");
    assert!(code(&v).is_err(), "CSV over ndjson framing");
    v["source"]["websocket"]["framing"] = json!("message");
    v["source"]["websocket"]["binary_frames"] = json!("decode");
    assert!(code(&v).is_err(), "CSV over binary frames");
    let mut v = base.clone();
    v["sink"]["format"] = json!("csv");
    code(&v).unwrap();
    v["sink"]["websocket"]["frame"] = json!("binary");
    assert!(code(&v).is_err(), "CSV sink with binary frames");

    assert_eq!(crate::replay_label_for_source("websocket"), "unsupported");
    let stored = serde_json::to_value(parse(&base).unwrap()).unwrap();
    assert!(stored["source"]["websocket"].get("auth").is_none());
    assert!(stored["sink"]["websocket"].get("overflow").is_none());
    assert_eq!(parse(&stored).unwrap(), parse(&base).unwrap());
    let caps = crate::capabilities_json();
    let find = |kind: &str| {
        caps["connectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["kind"] == kind)
            .unwrap_or_else(|| panic!("{kind} in capabilities"))
            .clone()
    };
    for kind in ["websocket", "websocket_sink"] {
        let c = find(kind);
        assert_eq!(c["replay"], "unsupported", "{c}");
        assert_eq!(c["delivery"], "live_best_effort", "{c}");
        assert_eq!(c["enabled_by_build"], true, "{c}");
    }
}

#[cfg(feature = "websocket")]
mod live {
    use super::*;
    use crate::{request_start, Store, Supervisor};
    use futures_util::{SinkExt, StreamExt};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    /// Feeds `rows` to every client that connects (one text frame per row).
    async fn feeder(
        rows: Vec<String>,
        paced: bool,
    ) -> (u16, mpsc::UnboundedReceiver<()>, mpsc::UnboundedSender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        let (permit, permits) = mpsc::unbounded_channel();
        let permits = Arc::new(tokio::sync::Mutex::new(permits));
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let rows = rows.clone();
                let tx = tx.clone();
                let permits = permits.clone();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                    let _ = tx.send(());
                    for row in rows {
                        if paced && permits.lock().await.recv().await.is_none() {
                            break;
                        }
                        ws.send(Message::text(row)).await.unwrap();
                    }
                    // Keep the session open (answering pings) until the client leaves.
                    while let Some(Ok(_)) = ws.next().await {}
                });
            }
        });
        (port, rx, permit)
    }

    /// Collects every text frame received from any client.
    async fn collector() -> (u16, mpsc::UnboundedReceiver<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                    while let Some(Ok(msg)) = ws.next().await {
                        if let Message::Text(text) = msg {
                            let _ = tx.send(serde_json::from_str(text.as_str()).unwrap());
                        }
                    }
                });
            }
        });
        (port, rx)
    }

    fn rows(prefix: &str, n: usize) -> Vec<String> {
        (0..n)
            .map(|i| {
                let t = if i % 2 == 0 { i as f64 + 1.0 } else { -1.0 };
                json!({"device_id": format!("{prefix}{i}"), "temperature": t}).to_string()
            })
            .collect()
    }

    async fn wait_for<F: Fn(&sparrow_connectors::IoSnapshot) -> bool>(
        sup: &Arc<Supervisor>,
        store: &Store,
        name: &str,
        done: F,
    ) -> sparrow_connectors::IoSnapshot {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                sup.converge_once().await.unwrap();
                let actual = store.actual(name).unwrap();
                assert_ne!(actual.status, "failed", "{actual:?}");
                if let Some(flow) = sup.flow_snapshot(name).unwrap() {
                    let io = flow.diagnostics.snapshot();
                    if done(&io) {
                        return io;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pipeline progress deadline")
    }

    #[test]
    fn websocket_pipeline_source_filter_sink_exact_counts() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            const N: usize = 200;
            let (src_port, mut connected, permits) = feeder(rows("d", N), true).await;
            let (sink_port, mut out) = collector().await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", src_port).unwrap();
            store.put_allow("127.0.0.1", sink_port).unwrap();
            let spec = parse(&linear_spec(src_port, sink_port)).unwrap();
            store.put_pipeline("ws", &spec, None).unwrap();
            request_start(&store, "ws", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            wait_for(&sup, &store, "ws", |io| io.websocket_source_connects == 1).await;
            // Loss is intentional on prefetch overflow. Pace this exact-count
            // test by actual pipeline progress instead of relying on burst timing.
            for n in 1..=N {
                permits.send(()).unwrap();
                wait_for(&sup, &store, "ws", |io| {
                    io.websocket_source_rows == n as u64
                        && io.websocket_sink_sent == n.div_ceil(2) as u64
                })
                .await;
            }
            let io = wait_for(&sup, &store, "ws", |io| {
                io.websocket_source_rows == N as u64 && io.websocket_sink_sent == (N / 2) as u64
            })
            .await;
            assert!(connected.try_recv().is_ok(), "source connected");
            let flow = sup.flow_snapshot("ws").unwrap().unwrap();
            assert_eq!(flow.source_kind, "websocket");
            assert_eq!(flow.sink_kind, "websocket");
            assert_eq!(io.websocket_source_received, N as u64, "{io:?}");
            assert_eq!(io.websocket_source_dropped_bad, 0);
            assert_eq!(io.websocket_source_dropped_overflow, 0);
            assert_eq!(io.websocket_source_connects, 1);
            assert_eq!(io.websocket_sink_connects, 1);
            let mut got = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), async {
                while got.len() < N / 2 {
                    got.push(out.recv().await.unwrap());
                }
            })
            .await
            .expect("sink server receives every hot row");
            sup.stop_all().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(out.try_recv().is_err(), "no extra rows");
            let expected: Vec<String> = (0..N).step_by(2).map(|i| format!("d{i}")).collect();
            let ids: Vec<String> = got
                .iter()
                .map(|r| r["device_id"].as_str().unwrap().to_string())
                .collect();
            assert_eq!(ids, expected, "WHERE keeps hot rows in order");
            let io = flow.diagnostics.snapshot();
            assert_eq!(io.websocket_sink_sent, (N / 2) as u64, "{io:?}");
            assert_eq!(io.websocket_sink_dropped_overflow, 0);
            assert_eq!(io.websocket_sink_discarded_on_close, 0);
            assert_eq!(io.websocket_sink_closes, 1, "close frame on stop: {io:?}");
        });
    }

    #[test]
    fn websocket_graph_io_sources_are_budgeted_and_deliver() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let (a_port, _a, _a_permits) = feeder(
                vec![json!({"device_id":"a","temperature":-3.0}).to_string()],
                false,
            )
            .await;
            let (b_port, _b, _b_permits) = feeder(
                vec![json!({"device_id":"b","temperature":-3.0}).to_string()],
                false,
            )
            .await;
            let (sink_port, mut out) = collector().await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            for port in [a_port, b_port, sink_port] {
                store.put_allow("127.0.0.1", port).unwrap();
            }
            let mut value = linear_spec(a_port, sink_port);
            value.as_object_mut().unwrap().remove("sql");
            let mut second = value["source"].clone();
            second["websocket"]["url"] = json!(format!("ws://127.0.0.1:{b_port}/in"));
            value["graph"] = json!({
                "version": 1, "pipeline_id": 904, "revision_id": 1,
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
            let spec = parse(&value).unwrap();
            store.put_pipeline("ws-graph", &spec, None).unwrap();
            request_start(&store, "ws-graph", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let mut seen = std::collections::BTreeSet::new();
            tokio::time::timeout(Duration::from_secs(10), async {
                while seen.len() < 2 {
                    sup.converge_once().await.unwrap();
                    let actual = store.actual("ws-graph").unwrap();
                    assert_ne!(actual.status, "failed", "{actual:?}");
                    while let Ok(Some(row)) =
                        tokio::time::timeout(Duration::from_millis(20), out.recv()).await
                    {
                        seen.insert(row["device_id"].as_str().unwrap().to_string());
                    }
                }
            })
            .await
            .expect("both graph inputs deliver (no filter in this graph)");
            sup.stop_all().await;
            assert_eq!(seen.into_iter().collect::<Vec<_>>(), ["a", "b"]);
        });
    }
}

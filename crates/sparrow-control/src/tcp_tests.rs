//! TCP Source/Sink through the real control path: catalog spec -> validate
//! -> Supervisor -> Kernel (SQL filter) -> TCP sink, against in-process tokio
//! TCP servers, plus the spec/validate matrix.

use crate::PipelineSpec;
use serde_json::{json, Value};

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
            "kind": "tcp",
            "inbox_capacity": 8,
            "tcp": {"host": "127.0.0.1", "port": source_port}
        },
        "sink": {
            "kind": "tcp",
            "outbox_capacity": 8,
            "tcp": {"host": "127.0.0.1", "port": sink_port}
        }
    })
}

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

type Mutation = Box<dyn Fn(&mut Value)>;

#[test]
fn tcp_typed_graph_nonlegacy_endpoint_refuses_durable_claims() {
    let tcp = linear_spec(9001, 9002);
    for source in [false, true] {
        let mut value = tcp.clone();
        value["source"] = json!({"kind":"file", "path":"/tmp/sparrow/input.jsonl"});
        value["sink"] = json!({"kind":"http", "url":"https://localhost/out"});
        value["graph_io"] = json!({
            "sources":{"1":value["source"].clone()},
            "sinks":{"2":value["sink"].clone()}
        });
        if source {
            value["graph_io"]["sources"]["3"] = tcp["source"].clone();
        } else {
            value["graph_io"]["sinks"]["3"] = tcp["sink"].clone();
        }
        for claim in [
            json!({"checkpoint_dir":"/tmp/sparrow/ck"}),
            json!({"recovery":"aligned"}),
            json!({"restore":{"kind":"checkpoint", "snapshot_id":"1"}}),
        ] {
            let mut changed = value.clone();
            changed
                .as_object_mut()
                .unwrap()
                .extend(claim.as_object().unwrap().clone());
            let typed: PipelineSpec = serde_json::from_value(changed).unwrap();
            assert_eq!(
                typed.check_delivery().unwrap_err().code,
                sparrow_model::ErrorCode::UnsupportedRestore
            );
        }
    }
}

#[test]
fn tcp_spec_matrix_rejects_mixed_fields_and_durable_claims() {
    let base = linear_spec(9001, 9002);
    parse(&base).unwrap();
    let mutations: Vec<(&str, Mutation)> = vec![
        (
            "missing source block",
            Box::new(|v| {
                v["source"].as_object_mut().unwrap().remove("tcp");
            }),
        ),
        (
            "missing sink block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("tcp");
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
            "tcp block on websocket",
            Box::new(|v| v["source"]["kind"] = json!("websocket")),
        ),
        (
            "unknown source field",
            Box::new(|v| v["source"]["tcp"]["listen"] = json!(true)),
        ),
        (
            "unknown sink field",
            Box::new(|v| v["sink"]["tcp"]["ack"] = json!(true)),
        ),
        (
            "unknown framing",
            Box::new(|v| v["source"]["tcp"]["framing"] = json!("varint")),
        ),
        (
            "unknown oversize",
            Box::new(|v| v["source"]["tcp"]["oversize"] = json!("truncate")),
        ),
        (
            "unknown overflow",
            Box::new(|v| v["sink"]["tcp"]["overflow"] = json!("drop_oldest")),
        ),
        (
            "length_bytes on lines",
            Box::new(|v| v["source"]["tcp"]["length_bytes"] = json!(4)),
        ),
        (
            "length_bytes 8",
            Box::new(|v| {
                v["sink"]["tcp"]["framing"] = json!("length_prefixed");
                v["sink"]["tcp"]["length_bytes"] = json!(8);
            }),
        ),
        (
            "port out of range",
            Box::new(|v| v["source"]["tcp"]["port"] = json!(70000)),
        ),
        (
            "mixed source host",
            Box::new(|v| v["source"]["host"] = json!("127.0.0.1")),
        ),
        (
            "mixed source tls",
            Box::new(|v| v["source"]["tls"] = json!(true)),
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
            "mixed sink port",
            Box::new(|v| v["sink"]["port"] = json!(1883)),
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
    // A TCP sink behind a durable-capable source still refuses durable claims.
    let mut v = base.clone();
    v["source"] = json!({"kind":"file","path":"/tmp/sparrow/in.ndjson"});
    parse(&v).unwrap();
    v["recovery"] = json!("aligned");
    v["delivery"] = json!("checkpointed_at_least_once");
    v["checkpoint_dir"] = json!("/tmp/sparrow/tcp-sink");
    let err = parse(&v).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(err.message.contains("TCP"), "{err:?}");

    let schema = crate::stream_schema(
        "telemetry",
        &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
    )
    .unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(Default::default());
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
    v["sink"]["tcp"]["host"] = json!("10.0.0.1");
    assert_eq!(
        code(&v),
        Err(PolicyDenied),
        "sink target outside the allowlist"
    );
    let mut v = base.clone();
    v["source"]["tcp"]["host"] = json!("bad host/x");
    assert!(code(&v).is_err(), "host syntax");
    let mut v = base.clone();
    v["source"]["tcp"]["tls_ca_pem"] = json!("-----BEGIN CERTIFICATE-----");
    assert!(code(&v).is_err(), "CA without tls");
    let mut v = base.clone();
    v["source"]["tcp"]["reconnect_attempts"] = json!(0);
    assert_eq!(code(&v), Err(BoundExceeded), "unbounded reconnect");
    let mut v = base.clone();
    v["source"]["tcp"]["max_frame_bytes"] = json!(1024 * 1024);
    assert!(code(&v).is_err(), "frame over the decode limit");
    let mut v = base.clone();
    v["source"]["tcp"]["idle_timeout_ms"] = json!(10);
    assert!(code(&v).is_err(), "idle timeout floor");
    let mut v = base.clone();
    v["source"]["tcp"]["inbox_bytes"] = json!(8 * 1024 * 1024);
    assert_eq!(code(&v), Err(BoundExceeded), "inbox over the queue budget");
    let mut v = base.clone();
    v["sink"]["tcp"]["queue_capacity"] = json!(64);
    assert_eq!(
        code(&v),
        Err(BoundExceeded),
        "sink queue x frame must fit half the compact reservation"
    );
    let mut v = base.clone();
    v["sink"]["tcp"]["framing"] = json!("length_prefixed");
    v["sink"]["tcp"]["length_bytes"] = json!(2);
    v["source"]["tcp"]["framing"] = json!("length_prefixed");
    v["source"]["tcp"]["oversize"] = json!("disconnect");
    v["source"]["tcp"]["keepalive_ms"] = json!(30000);
    code(&v).unwrap();
    // length_bytes 2: an omitted max_frame_bytes defaults to what the
    // prefix can express; an explicit larger value is refused, not clamped.
    let spec = parse(&v).unwrap();
    let sink = spec.sink.tcp.as_ref().unwrap().connector_config(8).client;
    assert_eq!(sink.max_frame_bytes, 65535);
    let source = spec.source.tcp.as_ref().unwrap().client_config();
    assert_eq!(
        source.max_frame_bytes,
        sparrow_connectors::tcp::MAX_FRAME_BYTES
    );
    v["sink"]["tcp"]["max_frame_bytes"] = json!(65536);
    assert_eq!(
        code(&v),
        Err(BoundExceeded),
        "u16 prefix cannot express 65536"
    );
    v["sink"]["tcp"]["max_frame_bytes"] = json!(65535);
    code(&v).unwrap();

    // Framing x formats.
    let mut v = base.clone();
    v["source"]["format"] = json!("csv");
    code(&v).unwrap();
    v["source"]["csv"] = json!({"multiline": true});
    assert!(code(&v).is_err(), "multiline CSV over lines");
    v["source"]["tcp"]["framing"] = json!("length_prefixed");
    code(&v).unwrap();
    let mut v = base.clone();
    v["sink"]["format"] = json!("csv");
    code(&v).unwrap();
    v["sink"]["tcp"]["framing"] = json!("length_prefixed");
    code(&v).unwrap();

    assert_eq!(crate::replay_label_for_source("tcp"), "unsupported");
    let stored = serde_json::to_value(parse(&base).unwrap()).unwrap();
    assert!(stored["source"]["tcp"].get("tls").is_none());
    assert!(stored["source"]["tcp"].get("framing").is_none());
    assert!(stored["sink"]["tcp"].get("overflow").is_none());
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
    for kind in ["tcp", "tcp_sink"] {
        let c = find(kind);
        assert_eq!(c["replay"], "unsupported", "{c}");
        assert_eq!(c["delivery"], "live_best_effort", "{c}");
        assert_eq!(c["enabled_by_build"], true, "{c}");
        assert_eq!(c["mode"], "client", "{c}");
    }
}

/// Each endpoint fits half the reservation, but all TCP endpoints of a graph
/// must fit 3/4 of it together.
#[test]
fn tcp_graph_reservation_total_is_bounded() {
    let schema = crate::stream_schema(
        "telemetry",
        &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
    )
    .unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(Default::default());
    let policy = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 9001)
        .with_allow("127.0.0.1", 9002)
        .with_allow("127.0.0.1", 9003);
    let graph = |queue: usize| {
        let mut value = linear_spec(9001, 9002);
        value.as_object_mut().unwrap().remove("sql");
        let mut second = value["sink"].clone();
        second["tcp"]["port"] = json!(9003);
        value["sink"]["tcp"]["queue_capacity"] = json!(queue);
        second["tcp"]["queue_capacity"] = json!(queue);
        value["graph"] = json!({
            "version": 1, "pipeline_id": 907, "revision_id": 1,
            "nodes": [
                {"id": 1, "kind": "memory_source", "table": "telemetry", "out": [2, 3]},
                {"id": 2, "kind": "capture_sink", "name": "a"},
                {"id": 3, "kind": "capture_sink", "name": "b"}
            ]
        });
        value["graph_io"] = json!({
            "sources": {"1": value["source"].clone()},
            "sinks": {"2": value["sink"].clone(), "3": second}
        });
        crate::validate_io(&parse(&value).unwrap(), &schema, &secrets, &policy, None)
    };
    graph(8).unwrap();
    let err = graph(24).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::BoundExceeded, "{err:?}");
    assert!(err.message.contains("TCP"), "{err:?}");
}

mod live {
    use super::*;
    use crate::{request_start, Store, Supervisor};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    #[derive(Clone, Copy)]
    enum Framing {
        Lines,
        U16,
        U32,
    }

    fn frame(framing: Framing, payload: &str) -> Vec<u8> {
        let mut out = Vec::new();
        match framing {
            Framing::Lines => {
                out.extend_from_slice(payload.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Framing::U16 => {
                out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
                out.extend_from_slice(payload.as_bytes());
            }
            Framing::U32 => {
                out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                out.extend_from_slice(payload.as_bytes());
            }
        }
        out
    }

    /// Writes `rows` to every client that connects, coalesced into a few
    /// writes that split frames at odd offsets, then waits for the client
    /// to leave.
    async fn feeder(framing: Framing, rows: Vec<String>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let bytes: Vec<u8> = rows.iter().flat_map(|r| frame(framing, r)).collect();
                tokio::spawn(async move {
                    for chunk in bytes.chunks(997) {
                        tcp.write_all(chunk).await.unwrap();
                    }
                    let mut sink = [0u8; 64];
                    while matches!(tcp.read(&mut sink).await, Ok(n) if n > 0) {}
                });
            }
        });
        port
    }

    /// Collects every frame received from any client.
    async fn collector(framing: Framing) -> (u16, mpsc::UnboundedReceiver<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut tcp = tokio::io::BufReader::new(tcp);
                    loop {
                        let payload = match framing {
                            Framing::Lines => {
                                let mut line = String::new();
                                use tokio::io::AsyncBufReadExt;
                                if tcp.read_line(&mut line).await.unwrap_or(0) == 0 {
                                    return;
                                }
                                assert!(line.ends_with('\n'), "{line:?}");
                                line.into_bytes()
                            }
                            Framing::U16 | Framing::U32 => {
                                let len = if let Framing::U16 = framing {
                                    match tcp.read_u16().await {
                                        Ok(n) => n as usize,
                                        Err(_) => return,
                                    }
                                } else {
                                    match tcp.read_u32().await {
                                        Ok(n) => n as usize,
                                        Err(_) => return,
                                    }
                                };
                                let mut buf = vec![0u8; len];
                                tcp.read_exact(&mut buf).await.unwrap();
                                buf
                            }
                        };
                        let _ = tx.send(serde_json::from_slice(&payload).unwrap());
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

    fn exact_counts(name: &str, source: Framing, sink: Framing, configure: impl Fn(&mut Value)) {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            const N: usize = 200;
            let src_port = feeder(source, rows("d", N)).await;
            let (sink_port, mut out) = collector(sink).await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", src_port).unwrap();
            store.put_allow("127.0.0.1", sink_port).unwrap();
            let mut value = linear_spec(src_port, sink_port);
            configure(&mut value);
            let spec = parse(&value).unwrap();
            store.put_pipeline(name, &spec, None).unwrap();
            request_start(&store, name, "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let io = wait_for(&sup, &store, name, |io| {
                io.tcp_source_rows == N as u64 && io.tcp_sink_sent == (N / 2) as u64
            })
            .await;
            let flow = sup.flow_snapshot(name).unwrap().unwrap();
            assert_eq!(flow.source_kind, "tcp");
            assert_eq!(flow.sink_kind, "tcp");
            assert_eq!(io.tcp_source_received, N as u64, "{io:?}");
            assert_eq!(io.tcp_source_dropped_bad, 0);
            assert_eq!(io.tcp_source_dropped_oversize, 0);
            assert_eq!(io.tcp_source_connects, 1);
            assert_eq!(io.tcp_sink_connects, 1);
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
            assert_eq!(io.tcp_sink_sent, (N / 2) as u64, "{io:?}");
            assert_eq!(io.tcp_sink_dropped_overflow, 0);
            assert_eq!(io.tcp_sink_discarded_on_close, 0);
            assert_eq!(io.tcp_sink_closes, 1, "shutdown on stop: {io:?}");
            assert_eq!(io.tcp_sink_fatal, 0);
        });
    }

    #[test]
    fn tcp_lines_pipeline_source_filter_sink_exact_counts() {
        exact_counts("tcp-lines", Framing::Lines, Framing::Lines, |_| {});
    }

    #[test]
    fn tcp_length_prefixed_pipeline_exact_counts() {
        exact_counts("tcp-prefixed", Framing::U32, Framing::U16, |v| {
            v["source"]["tcp"]["framing"] = json!("length_prefixed");
            v["sink"]["tcp"]["framing"] = json!("length_prefixed");
            v["sink"]["tcp"]["length_bytes"] = json!(2);
        });
    }

    #[test]
    fn tcp_graph_io_sources_are_budgeted_and_deliver() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let a_port = feeder(
                Framing::Lines,
                vec![json!({"device_id":"a","temperature":-3.0}).to_string()],
            )
            .await;
            let b_port = feeder(
                Framing::Lines,
                vec![json!({"device_id":"b","temperature":-3.0}).to_string()],
            )
            .await;
            let (sink_port, mut out) = collector(Framing::Lines).await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            for port in [a_port, b_port, sink_port] {
                store.put_allow("127.0.0.1", port).unwrap();
            }
            let mut value = linear_spec(a_port, sink_port);
            value.as_object_mut().unwrap().remove("sql");
            let mut second = value["source"].clone();
            second["tcp"]["port"] = json!(b_port);
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
            let spec = parse(&value).unwrap();
            store.put_pipeline("tcp-graph", &spec, None).unwrap();
            request_start(&store, "tcp-graph", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let mut seen = std::collections::BTreeSet::new();
            tokio::time::timeout(Duration::from_secs(10), async {
                while seen.len() < 2 {
                    sup.converge_once().await.unwrap();
                    let actual = store.actual("tcp-graph").unwrap();
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

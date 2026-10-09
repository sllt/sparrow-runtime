//! NATS Core Source/Sink through the real control path: catalog spec -> validate
//! -> Supervisor -> Kernel (SQL filter) -> NATS sink, plus the spec matrix.
//! Broker tests need a real `nats-server` binary in `SPARROW_NATS_SERVER`.

use crate::PipelineSpec;
use serde_json::{json, Value};

const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

fn linear_spec(url: &str) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 0",
        "source": {
            "kind": "nats",
            "inbox_capacity": 8,
            "nats": {"servers": [url], "subject": "telemetry.>", "queue_group": "sparrow"}
        },
        "sink": {
            "kind": "nats",
            "outbox_capacity": 8,
            "nats": {"servers": [url], "subject": "alerts.hot"}
        }
    })
}

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

#[cfg(not(feature = "nats"))]
#[test]
fn nats_spec_without_the_feature_is_feature_unavailable() {
    let err = parse(&linear_spec("nats://127.0.0.1:4222")).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::FeatureUnavailable);
}

#[cfg(feature = "nats")]
type Mutation = Box<dyn Fn(&mut Value)>;

#[cfg(feature = "nats")]
#[test]
fn nats_spec_matrix_rejects_mixed_fields_and_durable_claims() {
    let base = linear_spec("nats://127.0.0.1:4222");
    parse(&base).unwrap();
    let mutations: Vec<(&str, Mutation)> = vec![
        (
            "missing source block",
            Box::new(|v| {
                v["source"].as_object_mut().unwrap().remove("nats");
            }),
        ),
        (
            "missing sink block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("nats");
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
            Box::new(|v| v["source"]["nats"]["durable"] = json!("x")),
        ),
        (
            "unknown sink field",
            Box::new(|v| v["sink"]["nats"]["stream"] = json!("x")),
        ),
        (
            "inline token",
            Box::new(|v| v["source"]["nats"]["token"] = json!("x")),
        ),
        (
            "mixed source host",
            Box::new(|v| v["source"]["host"] = json!("127.0.0.1")),
        ),
        (
            "mixed source topic",
            Box::new(|v| v["source"]["topic"] = json!("a/b")),
        ),
        (
            "mixed source jetstream",
            Box::new(|v| {
                v["source"]["jetstream"] = json!({"servers":["nats://127.0.0.1:4222"],"namespace":"n",
                "stream":"S","consumer":"c","ownership_bucket":"O"})
            }),
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
    // A NATS sink behind a non-NATS source is still refused durable claims.
    let mut v = base.clone();
    v["source"] = json!({"kind":"file","path":"/tmp/sparrow/in.ndjson"});
    parse(&v).unwrap();
    v["recovery"] = json!("aligned");
    v["delivery"] = json!("checkpointed_at_least_once");
    v["checkpoint_dir"] = json!("/tmp/sparrow/nats-sink");
    let err = parse(&v).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(err.message.contains("NATS Core"), "{err:?}");

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
    let allowed = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 4222);
    let denied = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        crate::validate_io(&parse(value).unwrap(), &schema, &secrets, policy, None)
            .map_err(|e| e.code)
    };
    use sparrow_model::ErrorCode::*;
    check(&base, &allowed).unwrap();
    assert_eq!(check(&base, &denied), Err(PolicyDenied));
    let mut v = base.clone();
    v["source"]["nats"]["token_secret"] = json!("tok");
    assert_eq!(
        check(&v, &allowed),
        Err(PolicyDenied),
        "token over plain nats://"
    );
    v["source"]["nats"]["servers"] = json!(["tls://127.0.0.1:4222"]);
    check(&v, &allowed).unwrap();
    v["source"]["nats"]["token_secret"] = json!("absent");
    assert_eq!(check(&v, &allowed), Err(SecretMissing));
    let mut v = base.clone();
    v["source"]["nats"]["servers"] = json!(["nats://user:pw@127.0.0.1:4222"]);
    assert!(check(&v, &allowed).is_err(), "userinfo in URL");
    let mut v = base.clone();
    v["source"]["nats"]["subject"] = json!("a.>.b");
    assert!(check(&v, &allowed).is_err(), "> must be last");
    let mut v = base.clone();
    v["sink"]["nats"]["subject"] = json!("alerts.*");
    assert!(check(&v, &allowed).is_err(), "sink subject is literal");
    let mut v = base.clone();
    v["source"]["nats"]["queue_group"] = json!("bad group");
    assert!(check(&v, &allowed).is_err());
    let mut v = base.clone();
    v["source"]["nats"]["reconnect_attempts"] = json!(0);
    assert_eq!(
        check(&v, &allowed),
        Err(BoundExceeded),
        "unbounded reconnect is refused"
    );
    let mut v = base.clone();
    v["source"]["nats"]["max_payload_bytes"] = json!(1024 * 1024);
    v["source"]["nats"]["subscription_capacity"] = json!(16);
    assert_eq!(
        check(&v, &allowed),
        Err(BoundExceeded),
        "SDK buffers must fit the compact reservation"
    );

    // Each client fits half the reservation, but the pair must fit 3/4.
    let mut v = base.clone();
    v["source"]["nats"]["max_payload_bytes"] = json!(1024 * 1024);
    v["source"]["nats"]["subscription_capacity"] = json!(1);
    v["sink"]["nats"]["max_payload_bytes"] = json!(1024 * 1024);
    v["sink"]["nats"]["client_capacity"] = json!(1);
    check(&v, &allowed).unwrap();
    v["source"]["nats"]["max_payload_bytes"] = json!(65536);
    v["source"]["nats"]["subscription_capacity"] = json!(24);
    assert_eq!(
        check(&v, &allowed),
        Err(BoundExceeded),
        "sum of NATS SDK buffers over the pipeline"
    );
    assert_eq!(crate::replay_label_for_source("nats"), "unsupported");
    let stored = serde_json::to_value(parse(&base).unwrap()).unwrap();
    assert!(stored["source"]["nats"].get("token_secret").is_none());
    assert!(stored["sink"]["nats"].get("publish_timeout_ms").is_none());
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
    for kind in ["nats", "nats_sink"] {
        let c = find(kind);
        assert_eq!(c["replay"], "unsupported", "{c}");
        assert_eq!(c["delivery"], "live_best_effort", "{c}");
        assert_eq!(c["enabled_by_build"], true, "{c}");
    }
}

#[cfg(feature = "nats")]
mod broker {
    use super::*;
    use crate::{request_start, Store, Supervisor};
    use futures_util::StreamExt;
    use sparrow_testkit::nats::{async_nats, NatsSandbox};
    use std::sync::Arc;
    use std::time::Duration;

    async fn next_json(sub: &mut async_nats::Subscriber) -> Value {
        let msg = tokio::time::timeout(Duration::from_secs(8), sub.next())
            .await
            .expect("NATS output deadline")
            .expect("subscription open");
        serde_json::from_slice(&msg.payload).unwrap()
    }

    #[test]
    #[ignore = "requires SPARROW_NATS_SERVER (real nats-server binary)"]
    fn nats_core_pipeline_source_filter_sink_end_to_end() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let fixture = NatsSandbox::start().await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", fixture.port).unwrap();
            let spec = parse(&linear_spec(&fixture.url())).unwrap();
            store.put_pipeline("nats", &spec, None).unwrap();
            request_start(&store, "nats", "test").unwrap();
            let oracle = async_nats::connect(fixture.url()).await.unwrap();
            let mut out = oracle.subscribe("alerts.>").await.unwrap();
            oracle.flush().await.unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            // Core NATS has no replay: publish until the subscription is live.
            let mut seq = 0u32;
            let first = tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    seq += 1;
                    let cold = json!({"device_id":"cold","temperature":-1.0});
                    let hot = json!({"device_id":"hot","temperature":seq as f64});
                    oracle
                        .publish("telemetry.a.cold", cold.to_string().into())
                        .await
                        .unwrap();
                    oracle
                        .publish("telemetry.b.hot", hot.to_string().into())
                        .await
                        .unwrap();
                    oracle.flush().await.unwrap();
                    sup.converge_once().await.unwrap();
                    assert_ne!(store.actual("nats").unwrap().status, "failed");
                    if let Ok(Some(msg)) =
                        tokio::time::timeout(Duration::from_millis(100), out.next()).await
                    {
                        break (msg.subject.to_string(), msg.payload);
                    }
                }
            })
            .await
            .expect("first filtered row");
            assert_eq!(first.0, "alerts.hot");
            let row: Value = serde_json::from_slice(&first.1).unwrap();
            assert_eq!(row["device_id"], "hot", "WHERE must drop cold rows");
            // Steady state: a known hot row arrives, cold rows never do.
            oracle
                .publish(
                    "telemetry.z",
                    json!({"device_id":"cold","temperature":-9.0})
                        .to_string()
                        .into(),
                )
                .await
                .unwrap();
            oracle
                .publish(
                    "telemetry.z",
                    json!({"device_id":"marker","temperature":4242.0})
                        .to_string()
                        .into(),
                )
                .await
                .unwrap();
            oracle.flush().await.unwrap();
            loop {
                let row = next_json(&mut out).await;
                assert_ne!(row["device_id"], "cold");
                if row["device_id"] == "marker" {
                    assert_eq!(row["temperature"], json!(4242.0));
                    break;
                }
            }
            let snapshot = sup.flow_snapshot("nats").unwrap().unwrap();
            assert_eq!(snapshot.source_kind, "nats");
            assert_eq!(snapshot.sink_kind, "nats");
            let io = snapshot.diagnostics.snapshot();
            assert!(
                io.nats_source_rows >= 3 && io.nats_sink_published >= 2,
                "{io:?}"
            );
            assert_eq!(io.nats_source_dropped_bad, 0);
            sup.stop_all().await;
            let io = snapshot.diagnostics.snapshot();
            assert!(io.nats_sink_flushes >= 1, "sink flushes on stop: {io:?}");
            assert_eq!(io.nats_sink_failed, 0, "{io:?}");
        });
    }

    #[test]
    #[ignore = "requires SPARROW_NATS_SERVER (real nats-server binary)"]
    fn nats_core_graph_io_sources_are_budgeted_and_deliver() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let fixture = NatsSandbox::start().await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", fixture.port).unwrap();
            let mut value = linear_spec(&fixture.url());
            value.as_object_mut().unwrap().remove("sql");
            // Each client reserves its SDK buffers; three default clients (~1.4MiB each)
            // would exceed the compact 4MiB job reservation, so shrink them here.
            value["source"]["nats"]["subscription_capacity"] = json!(4);
            value["sink"]["nats"]["client_capacity"] = json!(4);
            let mut second = value["source"].clone();
            second["nats"]["subject"] = json!("other.*");
            second["nats"]
                .as_object_mut()
                .unwrap()
                .remove("queue_group");
            value["graph"] = json!({
                "version": 1, "pipeline_id": 903, "revision_id": 1,
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
            store.put_pipeline("nats-graph", &spec, None).unwrap();
            request_start(&store, "nats-graph", "test").unwrap();
            let oracle = async_nats::connect(fixture.url()).await.unwrap();
            let mut out = oracle.subscribe("alerts.hot").await.unwrap();
            oracle.flush().await.unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            let mut seen = std::collections::BTreeSet::new();
            tokio::time::timeout(Duration::from_secs(8), async {
                while seen.len() < 2 {
                    for (subject, id) in [("telemetry.x", "a"), ("other.y", "b")] {
                        let row = json!({"device_id": id, "temperature": -3.0});
                        oracle
                            .publish(subject, row.to_string().into())
                            .await
                            .unwrap();
                    }
                    oracle.flush().await.unwrap();
                    sup.converge_once().await.unwrap();
                    let actual = store.actual("nats-graph").unwrap();
                    assert_ne!(actual.status, "failed", "{actual:?}");
                    while let Ok(Some(msg)) =
                        tokio::time::timeout(Duration::from_millis(50), out.next()).await
                    {
                        let row: Value = serde_json::from_slice(&msg.payload).unwrap();
                        seen.insert(row["device_id"].as_str().unwrap().to_string());
                    }
                }
            })
            .await
            .expect("both graph inputs deliver (no filter in this graph)");
            sup.stop_all().await;
        });
    }
}

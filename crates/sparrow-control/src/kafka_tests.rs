//! Kafka Source/Sink through the control path: spec / validate matrix, and
//! (opt-in, real broker) catalog spec -> Supervisor -> Kernel (SQL filter)
//! -> Kafka sink, stopped and restarted from the group's committed offsets.

use crate::PipelineSpec;
use serde_json::{json, Value};

#[cfg_attr(not(feature = "kafka"), allow(dead_code))]
const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

fn linear_spec(port: u16) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 0",
        "source": {
            "kind": "kafka",
            "inbox_capacity": 8,
            "kafka": {
                "brokers": [format!("127.0.0.1:{port}")],
                "topic": "in",
                "group_id": "sparrow-telemetry",
                "auto_offset_reset": "earliest"
            }
        },
        "sink": {
            "kind": "kafka",
            "outbox_capacity": 8,
            "kafka": {"brokers": [format!("127.0.0.1:{port}")], "topic": "out", "key_column": "device_id"}
        }
    })
}

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

#[cfg(not(feature = "kafka"))]
#[test]
fn kafka_spec_without_the_feature_is_feature_unavailable() {
    let err = parse(&linear_spec(9092)).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::FeatureUnavailable);
}

#[cfg(feature = "kafka")]
type Mutation = Box<dyn Fn(&mut Value)>;

#[cfg(feature = "kafka")]
#[test]
fn kafka_spec_matrix_rejects_mixed_fields_and_durable_claims() {
    let base = linear_spec(9092);
    parse(&base).unwrap();
    let mutations: Vec<(&str, Mutation)> = vec![
        (
            "missing source block",
            Box::new(|v| {
                v["source"].as_object_mut().unwrap().remove("kafka");
            }),
        ),
        (
            "missing sink block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("kafka");
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
            "implicit offset reset",
            Box::new(|v| {
                v["source"]["kafka"]
                    .as_object_mut()
                    .unwrap()
                    .remove("auto_offset_reset");
            }),
        ),
        (
            "unknown offset reset",
            Box::new(|v| v["source"]["kafka"]["auto_offset_reset"] = json!("smallest")),
        ),
        (
            "unknown source field",
            Box::new(|v| v["source"]["kafka"]["enable_auto_commit"] = json!(true)),
        ),
        (
            "unknown sink field",
            Box::new(|v| v["sink"]["kafka"]["transactional_id"] = json!("x")),
        ),
        (
            "unknown acks",
            Box::new(|v| v["sink"]["kafka"]["acks"] = json!("none")),
        ),
        (
            "mixed source host",
            Box::new(|v| v["source"]["host"] = json!("127.0.0.1")),
        ),
        (
            "mixed source websocket",
            Box::new(|v| v["source"]["websocket"] = json!({"url":"ws://127.0.0.1:1/"})),
        ),
        (
            "mixed sink url",
            Box::new(|v| v["sink"]["url"] = json!("http://127.0.0.1:1/out")),
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
    // A Kafka sink behind a durable-capable source still refuses durable claims.
    let mut v = base.clone();
    v["source"] = json!({"kind":"file","path":"/tmp/sparrow/in.ndjson"});
    parse(&v).unwrap();
    v["recovery"] = json!("aligned");
    v["delivery"] = json!("checkpointed_at_least_once");
    v["checkpoint_dir"] = json!("/tmp/sparrow/kafka-sink");
    let err = parse(&v).unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(err.message.contains("Kafka"), "{err:?}");

    let schema = crate::stream_schema(
        "telemetry",
        &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
    )
    .unwrap();
    let secrets = sparrow_connectors::MapSecretResolver::new(Default::default());
    let allowed = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 9092);
    let denied = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        crate::validate_io(&parse(value).unwrap(), &schema, &secrets, policy, None)
    };
    let code = |value: &Value| check(value, &allowed).map_err(|e| e.code);
    use sparrow_model::ErrorCode::*;
    check(&base, &allowed).unwrap();
    assert_eq!(check(&base, &denied).unwrap_err().code, PolicyDenied);
    let mut v = base.clone();
    v["source"]["kafka"]["brokers"] = json!(["kafka://127.0.0.1:9092"]);
    assert_eq!(code(&v), Err(InvalidArgument), "host:port only");
    let mut v = base.clone();
    v["source"]["kafka"]["group_id"] = json!("a b");
    assert_eq!(code(&v), Err(InvalidArgument));
    let mut v = base.clone();
    v["sink"]["kafka"]["acks"] = json!("leader");
    assert_eq!(
        code(&v),
        Err(InvalidArgument),
        "idempotence requires acks=all"
    );
    v["sink"]["kafka"]["idempotence"] = json!(false);
    code(&v).unwrap();
    for (side, field, value) in [
        ("source", "prefetch_bytes", json!(usize::MAX)),
        ("source", "fetch_max_bytes", json!(1024)),
        ("source", "max_message_bytes", json!(0)),
        ("source", "session_timeout_ms", json!(1000)),
        ("source", "commit_interval_ms", json!(u64::MAX)),
        ("sink", "queue_bytes", json!(usize::MAX)),
        ("sink", "max_in_flight", json!(0)),
        ("sink", "delivery_timeout_ms", json!(u64::MAX)),
    ] {
        let mut v = base.clone();
        v[side]["kafka"][field] = value;
        assert_eq!(code(&v), Err(BoundExceeded), "{side}.{field}");
    }
    // Each endpoint fits half the reservation (2 MiB); together they must fit
    // 3/4 (3 MiB). Source: 768 KiB prefetch + 2 x 384 KiB fetch + 256 KiB =
    // 1792 KiB. Sink: queue + 2 x 64 KiB + 256 KiB = 896 KiB with a 512 KiB
    // queue (sum 2688 KiB) but 1408 KiB with the default 1 MiB (3200 KiB).
    let mut v = base.clone();
    v["source"]["kafka"]["prefetch_bytes"] = json!(768 * 1024);
    v["source"]["kafka"]["fetch_max_bytes"] = json!(384 * 1024);
    v["sink"]["kafka"]["queue_bytes"] = json!(512 * 1024);
    code(&v).unwrap();
    v["sink"]["kafka"]["queue_bytes"] = json!(1024 * 1024);
    assert_eq!(code(&v), Err(BoundExceeded), "sum of librdkafka buffers");
    let mut v = base.clone();
    v["source"]["kafka"]["prefetch_bytes"] = json!(1024 * 1024);
    v["source"]["kafka"]["fetch_max_bytes"] = json!(512 * 1024);
    assert_eq!(code(&v), Err(BoundExceeded), "one endpoint over half");

    // Formats: JSON / CSV / protobuf on both sides.
    let mut v = base.clone();
    v["source"]["format"] = json!("csv");
    v["sink"]["format"] = json!("csv");
    code(&v).unwrap();
    assert!(crate::spec::PROTOBUF_SOURCE_KINDS.contains(&"kafka"));
    assert!(crate::spec::PROTOBUF_SINK_KINDS.contains(&"kafka"));

    // Explicit values reach the connector unclamped.
    let mut v = base.clone();
    v["source"]["kafka"]["auto_offset_reset"] = json!("error");
    v["source"]["kafka"]["max_message_bytes"] = json!(1024 * 1024);
    v["source"]["kafka"]["fetch_max_bytes"] = json!(1024 * 1024 + 1024);
    v["source"]["kafka"]["prefetch_bytes"] = json!(1024 * 1024 + 1024);
    let parsed = parse(&v).unwrap();
    let config = parsed
        .source
        .kafka
        .as_ref()
        .unwrap()
        .connector_config(schema.clone(), 8, true);
    assert_eq!(
        config.auto_offset_reset,
        sparrow_connectors::kafka::OffsetReset::Error
    );
    assert_eq!(config.max_message_bytes, 1024 * 1024);
    assert!(config.fail_on_decode);

    assert_eq!(crate::replay_label_for_source("kafka"), "unsupported");
    let stored = serde_json::to_value(parse(&base).unwrap()).unwrap();
    assert!(stored["sink"]["kafka"].get("acks").is_none());
    assert_eq!(parse(&stored).unwrap(), parse(&base).unwrap());
    let caps = crate::capabilities_json();
    for kind in ["kafka", "kafka_sink"] {
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

// The connector crate's broker sandbox, shared by path (it names the client
// crate as `super::rdkafka`).
#[cfg(feature = "kafka")]
use sparrow_connectors::kafka::rdkafka;
#[cfg(feature = "kafka")]
#[path = "../../sparrow-connectors/src/kafka/broker.rs"]
mod kafka_broker;

#[cfg(feature = "kafka")]
mod live {
    use super::*;
    use crate::{request_start, Store, Supervisor};
    use std::sync::Arc;
    use std::time::Duration;

    use super::kafka_broker::KafkaSandbox;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::message::Message;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{Offset, TopicPartitionList};
    use sparrow_connectors::kafka::rdkafka;

    async fn produce(broker: &KafkaSandbox, rows: impl Iterator<Item = (String, f64)>) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker.bootstrap())
            .create()
            .unwrap();
        for (id, t) in rows {
            let body = json!({"device_id": id, "temperature": t}).to_string();
            producer
                .send(
                    FutureRecord::<(), str>::to("in").payload(&body),
                    Duration::from_secs(10),
                )
                .await
                .unwrap();
        }
    }

    async fn read_out(broker: &KafkaSandbox, n: usize) -> Vec<(String, String)> {
        let bootstrap = broker.bootstrap();
        tokio::task::spawn_blocking(move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", bootstrap)
                .set("group.id", "reader")
                .set("enable.auto.commit", "false")
                .create()
                .unwrap();
            let mut list = TopicPartitionList::new();
            list.add_partition_offset("out", 0, Offset::Beginning)
                .unwrap();
            consumer.assign(&list).unwrap();
            let mut out = Vec::new();
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            let mut quiet = None;
            while std::time::Instant::now() < deadline {
                match consumer.poll(Duration::from_millis(100)) {
                    Some(Ok(m)) => {
                        let v: Value = serde_json::from_slice(m.payload().unwrap()).unwrap();
                        let key = String::from_utf8(m.key().unwrap().to_vec()).unwrap();
                        out.push((key, v["device_id"].as_str().unwrap().to_string()));
                    }
                    _ if out.len() >= n => {
                        let q = *quiet.get_or_insert_with(std::time::Instant::now);
                        if q.elapsed() > Duration::from_millis(500) {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            out
        })
        .await
        .unwrap()
    }

    async fn wait_for<F: Fn(&sparrow_connectors::IoSnapshot) -> bool>(
        sup: &Arc<Supervisor>,
        store: &Store,
        name: &str,
        done: F,
    ) -> sparrow_connectors::IoSnapshot {
        tokio::time::timeout(Duration::from_secs(60), async {
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
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("pipeline progress deadline")
    }

    fn rows(from: usize, to: usize) -> impl Iterator<Item = (String, f64)> {
        (from..to).map(|i| {
            (
                format!("d{i}"),
                if i % 2 == 0 { i as f64 + 1.0 } else { -1.0 },
            )
        })
    }

    #[test]
    #[ignore = "requires SPARROW_KAFKA_HOME/SPARROW_JAVA_HOME; isolated broker child only"]
    fn kafka_pipeline_filters_delivers_and_resumes_from_committed_offsets() {
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let broker = KafkaSandbox::start();
            broker.create_topic("in", 1).await;
            broker.create_topic("out", 1).await;
            produce(&broker, rows(0, 40)).await;
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
            store.put_allow("127.0.0.1", broker.port).unwrap();
            let spec = parse(&linear_spec(broker.port)).unwrap();
            store.put_pipeline("k", &spec, None).unwrap();
            request_start(&store, "k", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let io = wait_for(&sup, &store, "k", |io| {
                io.kafka.source_rows == 40 && io.kafka.sink_acked == 20
            })
            .await;
            let flow = sup.flow_snapshot("k").unwrap().unwrap();
            assert_eq!(flow.source_kind, "kafka");
            assert_eq!(flow.sink_kind, "kafka");
            assert_eq!(io.kafka.source_poison_skipped, 0, "{io:?}");
            assert_eq!(io.kafka.sink_failed, 0);
            sup.stop_all().await;

            // Restart: the group resumes after the 40 admitted rows.
            produce(&broker, rows(40, 60)).await;
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            let io = wait_for(&sup, &store, "k", |io| {
                io.kafka.source_rows == 20 && io.kafka.sink_acked == 10
            })
            .await;
            assert_eq!(io.kafka.source_received, 20, "no redelivery: {io:?}");
            sup.stop_all().await;
            let out = read_out(&broker, 30).await;
            let expected: Vec<String> = (0..60).step_by(2).map(|i| format!("d{i}")).collect();
            assert_eq!(
                out.iter().map(|o| o.1.clone()).collect::<Vec<_>>(),
                expected
            );
            assert!(
                out.iter().all(|(k, id)| k == id),
                "key_column is the record key"
            );
        });
    }
}

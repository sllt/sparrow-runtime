//! Core-A reliability coverage for JetStream + Count/IoT + HTTP.
//!
//! The broker test is intentionally ignored by default.  It uses the same
//! isolated, pinned NATS fixture as K2 and must never connect to an operator's
//! existing broker.

use crate::{request_start, validate_aligned_plan, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use sparrow_testkit::nats::NatsSandbox;
use std::{sync::Arc, time::Duration};

fn graph_spec(server: &str, consumer: &str, sink: &str, checkpoint_dir: &std::path::Path) -> Value {
    json!({
        "version": 1,
        "stream": "sensors",
        "source": {
            "kind": "jetstream",
            "jetstream": {
                "servers": [server],
                "namespace": "test_account",
                "stream": "INPUT",
                "consumer": consumer,
                "ownership_bucket": "OWNERS",
                "max_pending": 16,
                "pending_bytes": 262144,
                "pull_messages": 4,
                "pull_bytes": 73728
            }
        },
        "sink": {
            "kind": "http",
            "url": sink,
            "batch_rows": 32,
            "linger_ms": 10,
            "outbox_capacity": 16
        },
        "delivery": "checkpointed_at_least_once",
        "recovery": "aligned",
        "checkpoint_dir": checkpoint_dir,
        "checkpoint": {
            "interval_ms": 10000,
            "timeout_ms": 2000,
            "resume_latest": true
        },
        "graph": {
            "version": 1,
            "pipeline_id": 501,
            "revision_id": 1,
            "nodes": [
                {"id": 1, "kind": "memory_source", "table": "sensors", "out": [2]},
                {
                    "id": 2,
                    "kind": "window_agg",
                    "keys": ["device_id"],
                    "window": {"kind": "count", "size": 2},
                    "aggs": [{"fn": "sum", "expr": {"k": "col", "name": "v"}, "alias": "s"}],
                    "out": [3]
                },
                {
                    "id": 3,
                    "kind": "change_detect",
                    "iot": {
                        "keys": ["device_id"],
                        "fields": ["s"],
                        "emit_first": true,
                        "ttl_micros": 0,
                        "max_keys": 64,
                        "invalid": "error"
                    },
                    "out": [4]
                },
                {"id": 4, "kind": "capture_sink", "name": "out"}
            ]
        }
    })
}

fn parse_spec(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn telemetry_catalog() -> sparrow_plan::Catalog {
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "sensors",
        crate::stream_schema(
            "sensors",
            &serde_json::from_str(
                r#"{"fields":[
                    {"name":"device_id","type":"utf8","nullable":false},
                    {"name":"v","type":"int64","nullable":false}
                ]}"#,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    catalog
}

#[test]
fn core_a_jetstream_iot_admission_reports_v7_and_rejects_positive_ttl() {
    let checkpoint = sparrow_connectors::ensure_default_data_root().join("core-a-admission");
    let mut value = graph_spec(
        "nats://127.0.0.1:4222",
        "core-a-admission",
        "http://127.0.0.1:1/telemetry",
        &checkpoint,
    );
    let catalog = telemetry_catalog();
    let spec = parse_spec(value.clone());
    let plan = crate::bind_plan(&spec, &catalog, "core-a-admission", 1).unwrap();
    validate_aligned_plan(&spec, &plan).unwrap();
    let effective = crate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(effective["aligned_eligible"], true);
    assert_eq!(
        effective["aligned_eligibility_reason"],
        "jetstream_reliable_iot_v7_ttl0_count_or_iot_state"
    );
    assert_eq!(effective["checkpoint_participants"]["snapshot_version"], 7);
    assert_eq!(effective["checkpoint_participants"]["profile"], "reliable_iot_v7");
    assert_eq!(effective["iot"]["snapshot_version"], 7);
    assert_eq!(effective["iot"]["ttl_micros"], json!([0]));

    let mut deadband = graph_spec(
        "nats://127.0.0.1:4222",
        "core-a-admission-deadband",
        "http://127.0.0.1:1/telemetry",
        &checkpoint,
    );
    deadband["graph"]["nodes"][2]["kind"] = json!("deadband");
    deadband["graph"]["nodes"][2]["iot"]["deadband"] =
        json!({"mode":"absolute","baseline":"last_output","threshold":1.0});
    let deadband = parse_spec(deadband);
    let plan = crate::bind_plan(&deadband, &catalog, "core-a-admission-deadband", 1).unwrap();
    validate_aligned_plan(&deadband, &plan).unwrap();
    assert_eq!(
        crate::effective_guarantees_with_plan(&deadband, &plan)["checkpoint_participants"]["snapshot_version"],
        7
    );

    let mut dag = graph_spec(
        "nats://127.0.0.1:4222",
        "core-a-dag-boundary",
        "http://127.0.0.1:1/telemetry",
        &checkpoint,
    );
    let source = dag["source"].clone();
    let sink = dag["sink"].clone();
    dag["graph_io"] = json!({"sources":{"1":source},"sinks":{"4":sink}});
    let error = PipelineSpec::from_json(&serde_json::to_vec(&dag).unwrap()).unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::FeatureUnavailable);
    assert!(error.message.contains("linear reliability profile"));

    value["graph"]["nodes"][2]["iot"]["ttl_micros"] = json!(1);
    let positive_ttl = parse_spec(value);
    let plan = crate::bind_plan(&positive_ttl, &catalog, "core-a-admission-ttl", 1).unwrap();
    let error = validate_aligned_plan(&positive_ttl, &plan).unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(error.message.contains("ttl_micros=0"));

    let mut template = PipelineSpec::from_json(include_bytes!(
        "../../../deploy/pipeline-jetstream-iot.json"
    )).unwrap();
    template.checkpoint_dir = Some(checkpoint.to_string_lossy().into_owned());
    let plan = crate::bind_plan(&template, &catalog, "core-a-template", 1).unwrap();
    validate_aligned_plan(&template, &plan).unwrap();
    assert_eq!(crate::effective_guarantees_with_plan(&template, &plan)
        ["checkpoint_participants"]["snapshot_version"], 7);
}

fn catalog(fixture: &NatsSandbox, http: &sparrow_connectors::HttpCapture) -> Arc<Store> {
    let store = Arc::new(Store::open(&fixture.root.join("catalog.db")).unwrap());
    store
        .put_stream(
            "sensors",
            r#"{"fields":[
                {"name":"device_id","type":"utf8","nullable":false},
                {"name":"v","type":"int64","nullable":false}
            ]}"#,
        )
        .unwrap();
    store.put_allow("127.0.0.1", fixture.port).unwrap();
    store.put_allow("127.0.0.1", http.port()).unwrap();
    store
}

fn outputs(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
        .collect()
}

async fn wait_outputs(
    http: &sparrow_connectors::HttpCapture,
    expected: usize,
    supervisor: &Arc<Supervisor>,
    store: &Store,
    name: &str,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while outputs(http).len() < expected && std::time::Instant::now() < deadline {
        supervisor.converge_once().await.unwrap();
        assert_ne!(store.actual(name).unwrap().status, "failed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(outputs(http).len() >= expected, "outputs={:?}", outputs(http));
}

async fn wait_bootstrap(supervisor: &Arc<Supervisor>, store: &Store, name: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    loop {
        if supervisor
            .checkpoint_inventory(name)
            .await
            .ok()
            .is_some_and(|inventory| inventory.storage.current.is_some())
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bootstrap failed: {:?}",
            store.actual(name)
        );
        supervisor.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn checkpoint_when_idle(supervisor: &Arc<Supervisor>, name: &str) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        match supervisor.checkpoint_named(name).await {
            Ok(id) => return id,
            Err(error)
                if error.code == sparrow_model::ErrorCode::ResourceExhausted
                    && error.message == "checkpoint already queued or active"
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("checkpoint failed: {error}"),
        }
    }
}

async fn wait_published(supervisor: &Arc<Supervisor>, name: &str, cut: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let published = supervisor
            .checkpoint_snapshot(name)
            .unwrap()
            .and_then(|(_, control)| control.snapshot().reliable_source)
            .map(|source| source.published_cut);
        if published.is_some_and(|position| position >= cut) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "published source cut did not reach {cut}: {published:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn publish_values(context: &sparrow_testkit::nats::jetstream::Context, values: &[i64]) {
    for value in values {
        context
            .publish(
                "input.rows",
                format!("{{\"device_id\":\"d1\",\"v\":{value}}}").into(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
    }
}

async fn reader_consumer(
    context: &sparrow_testkit::nats::jetstream::Context,
    consumer: &str,
) -> sparrow_testkit::nats::jetstream::consumer::PullConsumer {
    let kv = context.get_key_value("OWNERS").await.unwrap();
    let entry = kv.entry(format!("INPUT.{consumer}")).await.unwrap().unwrap();
    let nonce: String = entry.value[36..]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    context
        .get_stream("INPUT")
        .await
        .unwrap()
        .get_consumer(&format!("{consumer}_{nonce}"))
        .await
        .unwrap()
}

async fn wait_acked(
    context: &sparrow_testkit::nats::jetstream::Context,
    consumer: &str,
    cut: u64,
) {
    let mut reader = reader_consumer(context, consumer).await;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let info = reader.info().await.unwrap();
            if info.ack_floor.stream_sequence == cut && info.num_ack_pending == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("broker must confirm the complete committed prefix");
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn core_a_jetstream_count_change_v7_checkpoint_restore_and_http_ack_cut() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let checkpoint = fixture.root.join("checkpoint-core-a");
        let spec = parse_spec(graph_spec(
            &fixture.url(),
            "core-a",
            &http.url(),
            &checkpoint,
        ));
        store.put_pipeline("core-a", &spec, None).unwrap();
        request_start(&store, "core-a", "core-a").unwrap();
        let mut supervisor = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        Arc::get_mut(&mut supervisor).unwrap().stable_run = Duration::ZERO;
        supervisor.converge_once().await.unwrap();
        wait_bootstrap(&supervisor, &store, "core-a").await;

        // The third input is deliberately left in the Count state when the
        // first checkpoint is taken. Restore must retain it and the IoT
        // baseline produced by the first two rows.
        publish_values(&context, &[1, 2, 3]).await;
        wait_outputs(&http, 1, &supervisor, &store, "core-a").await;
        wait_published(&supervisor, "core-a", 3).await;
        let first_checkpoint = checkpoint_when_idle(&supervisor, "core-a").await;
        wait_acked(&context, "core-a", 3).await;

        let snapshot = sparrow_runtime::CheckpointStore::open_readonly(&checkpoint)
            .unwrap()
            .recover_pipeline_required()
            .unwrap();
        let bytes = std::fs::read(
            checkpoint.join(format!("chk-{first_checkpoint:08}/0000.bin")),
        )
        .unwrap();
        assert_eq!(&bytes[4..6], &7u16.to_le_bytes());
        assert_eq!(snapshot.source.identity.kind, "jetstream-v1");
        assert_eq!(snapshot.source.offset_bytes, 3);
        assert_eq!(snapshot.windows.len(), 1);
        assert_eq!(snapshot.windows[0].entries.len(), 1);
        assert_eq!(snapshot.iot.len(), 1);
        assert_eq!(snapshot.next_output.unwrap().first(), 2);

        supervisor.kill_named("core-a").await.unwrap();
        request_start(&store, "core-a", "core-a").unwrap();
        supervisor.converge_once().await.unwrap();

        // v4 Count state restoration emits 7 from the retained third row;
        // v7 IoT restoration then suppresses a repeated 7 baseline.
        publish_values(&context, &[4]).await;
        wait_outputs(&http, 2, &supervisor, &store, "core-a").await;
        publish_values(&context, &[3, 4]).await;
        // The repeated value is intentionally suppressed by IoT, so output
        // count cannot prove that source records reached the runtime.
        wait_published(&supervisor, "core-a", 6).await;
        let second_checkpoint = checkpoint_when_idle(&supervisor, "core-a").await;
        wait_acked(&context, "core-a", 6).await;

        let all = outputs(&http);
        assert_eq!(all.len(), 2);
        assert_eq!(
            all.iter()
                .map(|row| row["data"]["s"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![3, 7]
        );
        let ids = all
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(ids[0].ends_with("0000000000000001"));
        assert!(ids[1].ends_with("0000000000000002"));

        let second = sparrow_runtime::CheckpointStore::open_readonly(&checkpoint)
            .unwrap()
            .recover_pipeline_required()
            .unwrap();
        assert_eq!(second.source.offset_bytes, 6);
        assert_eq!(second.iot.len(), 1);
        assert_eq!(second.next_output.unwrap().first(), 3);
        assert_eq!(
            &std::fs::read(checkpoint.join(format!("chk-{second_checkpoint:08}/0000.bin")))
                .unwrap()[4..6],
            &7u16.to_le_bytes()
        );

        supervisor.kill_named("core-a").await.unwrap();
        http.stop().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

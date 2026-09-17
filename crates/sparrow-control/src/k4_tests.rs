//! K4 control-plane coverage: real File -> IoT state -> HTTP, plus the
//! admission matrix and the two shipped templates.  The runtime owns the
//! operator semantics; these tests intentionally exercise catalog binding,
//! aligned v6 selection, restore, and the user-visible rejection boundary.

use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const TELEMETRY_SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true}
]}"#;

struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let root = sparrow_connectors::ensure_default_data_root().join(format!(
            "sparrow-k4-{label}-{}-{}",
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

fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
        .collect()
}

fn temperature_values(http: &sparrow_connectors::HttpCapture) -> Vec<f64> {
    output(http)
        .into_iter()
        .map(|row| row["temperature"].as_f64().unwrap())
        .collect()
}

async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    expected: usize,
    sup: &Arc<Supervisor>,
    store: &Arc<Store>,
    name: &str,
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while output(http).len() < expected {
            sup.converge_once().await.unwrap();
            let actual = store.actual(name).unwrap();
            assert_ne!(actual.status, "failed", "{actual:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("K4 HTTP output deadline");
}

fn graph_spec(kind: &str, iot: Value) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "source": {
            "kind": "file",
            "path": "/tmp/k4-placeholder.ndjson",
            "file_contract": "append_only",
            "inbox_capacity": 8
        },
        "sink": {
            "kind": "http",
            "url": "http://127.0.0.1:1/telemetry",
            "outbox_capacity": 8,
            "batch_rows": 8,
            "linger_ms": 1
        },
        "recovery": "restart_fresh",
        "graph": {
            "version": 1,
            "pipeline_id": 401,
            "revision_id": 1,
            "nodes": [
                {"id": 1, "kind": "memory_source", "table": "telemetry", "out": [2]},
                {"id": 2, "kind": kind, "iot": iot, "out": [3]},
                {"id": 3, "kind": "capture_sink", "name": "out"}
            ]
        }
    })
}

fn iot_config(emit_first: bool, ttl_micros: i64, invalid: &str, deadband: Option<Value>) -> Value {
    let mut value = json!({
        "keys": ["device_id"],
        "fields": ["temperature"],
        "emit_first": emit_first,
        "ttl_micros": ttl_micros,
        "max_keys": 64,
        "invalid": invalid
    });
    if let Some(deadband) = deadband {
        value["deadband"] = deadband;
    }
    value
}

fn with_source_sink(mut value: Value, path: &std::path::Path, url: &str) -> Value {
    value["source"]["path"] = json!(path);
    value["sink"]["url"] = json!(url);
    value
}

fn parse_spec(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn install_telemetry(store: &Arc<Store>) {
    store.put_stream("telemetry", TELEMETRY_SCHEMA).unwrap();
}

fn graph_change_spec(
    first: &std::path::Path,
    second: &std::path::Path,
    url: &str,
    checkpoint: &std::path::Path,
) -> PipelineSpec {
    let source = json!({
        "kind":"file","path":first,"file_contract":"append_only","inbox_capacity":8
    });
    let other = json!({
        "kind":"file","path":second,"file_contract":"append_only","inbox_capacity":8
    });
    let sink = json!({
        "kind":"http","url":url,"outbox_capacity":8,"batch_rows":8,"linger_ms":1
    });
    parse_spec(json!({
        "version":1,"stream":"telemetry","source":source,"sink":sink,
        "recovery":"aligned","checkpoint_dir":checkpoint,
        "checkpoint":{"timeout_ms":2000,"resume_latest":true},
        "graph_io":{"sources":{"1":source,"2":other},"sinks":{"5":sink}},
        "graph":{"version":1,"pipeline_id":403,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"telemetry","out":[3]},
            {"id":2,"kind":"memory_source","table":"telemetry","out":[3]},
            {"id":3,"kind":"union_all","out":[4]},
            {"id":4,"kind":"change_detect","iot":iot_config(true,0,"ignore",None),"out":[5]},
            {"id":5,"kind":"capture_sink","name":"changed"}
        ]}
    }))
}

#[test]
fn k4_multi_file_graph_change_detect_uses_v6_checkpoint_restore() {
    let scratch = Scratch::new("graph-change-restore");
    let first = scratch.0.join("first.ndjson");
    let second = scratch.0.join("second.ndjson");
    std::fs::write(&first, b"{\"device_id\":\"edge-a\",\"temperature\":20.0}\n").unwrap();
    std::fs::write(
        &second,
        b"{\"device_id\":\"edge-b\",\"temperature\":30.0}\n",
    )
    .unwrap();
    let checkpoint = scratch.0.join("checkpoint");
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&scratch.0.join("catalog.db")).unwrap());
        install_telemetry(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = graph_change_spec(&first, &second, &http.url(), &checkpoint);
        store.put_pipeline("graph-change", &spec, None).unwrap();
        request_start(&store, "graph-change", "k4").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 2, &sup, &store, "graph-change").await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let id = sup.checkpoint_named("graph-change").await.unwrap();
        append(&first, b"{\"device_id\":\"edge-a\",\"temperature\":20.0}\n");
        append(
            &second,
            b"{\"device_id\":\"edge-b\",\"temperature\":35.0}\n",
        );
        wait_rows(&http, 3, &sup, &store, "graph-change").await;
        sup.kill_named("graph-change").await.unwrap();
        request_start(&store, "graph-change", "k4").unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 4, &sup, &store, "graph-change").await;
        // Union preserves each input's order, not a cross-source arrival
        // order. Verify both the device identity and its replay sequence.
        let rows = output(&http);
        assert_eq!(rows.len(), 4);
        for (device, expected) in [("edge-a", vec![20.0]), ("edge-b", vec![30.0, 35.0, 35.0])] {
            let actual: Vec<_> = rows.iter().filter(|row| row["device_id"] == device)
                .map(|row| row["temperature"].as_f64().unwrap()).collect();
            assert_eq!(actual, expected);
        }
        assert_eq!(
            sup.checkpoint_snapshot("graph-change")
                .unwrap()
                .unwrap()
                .1
                .snapshot()
                .restored_from,
            Some(id)
        );
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn k4_file_change_detect_checkpoint_restore_and_semantic_change_refusal() {
    let scratch = Scratch::new("change-restore");
    let file = scratch.0.join("telemetry.ndjson");
    std::fs::write(
        &file,
        b"{\"device_id\":\"edge-a\",\"temperature\":20.0}\n{\"device_id\":\"edge-a\",\"temperature\":20.0}\n{\"device_id\":\"edge-b\",\"temperature\":5.0}\n{\"device_id\":\"edge-a\",\"temperature\":22.0}\n",
    )
    .unwrap();
    let checkpoint = scratch.0.join("checkpoint");
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&scratch.0.join("catalog.db")).unwrap());
        install_telemetry(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut value = graph_spec(
            "change_detect",
            iot_config(true, 0, "ignore", None),
        );
        value["recovery"] = json!("aligned");
        value["checkpoint_dir"] = json!(checkpoint);
        value["checkpoint"] = json!({"timeout_ms":2000,"resume_latest":true});
        let value = with_source_sink(value, &file, &http.url());
        let spec = parse_spec(value.clone());
        store.put_pipeline("change", &spec, None).unwrap();
        request_start(&store, "change", "k4").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 3, &sup, &store, "change").await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let checkpoint_id = sup.checkpoint_named("change").await.unwrap();

        append(&file, b"{\"device_id\":\"edge-a\",\"temperature\":22.0}\n{\"device_id\":\"edge-a\",\"temperature\":25.0}\n");
        wait_rows(&http, 4, &sup, &store, "change").await;
        sup.kill_named("change").await.unwrap();
        request_start(&store, "change", "k4").unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 5, &sup, &store, "change").await;

        append(&file, b"{\"device_id\":\"edge-a\",\"temperature\":25.0}\n{\"device_id\":\"edge-a\",\"temperature\":27.0}\n");
        wait_rows(&http, 6, &sup, &store, "change").await;
        assert_eq!(
            temperature_values(&http),
            vec![20.0, 5.0, 22.0, 25.0, 25.0, 27.0]
        );
        assert_eq!(
            sup.checkpoint_snapshot("change")
                .unwrap()
                .unwrap()
                .1
                .snapshot()
                .restored_from,
            Some(checkpoint_id)
        );

        sup.kill_named("change").await.unwrap();
        let mut changed = value;
        changed["graph"]["nodes"][1]["iot"]["emit_first"] = json!(false);
        let changed = parse_spec(changed);
        let etag = store.get_pipeline("change").unwrap().etag;
        store
            .put_pipeline("change", &changed, Some(&etag))
            .unwrap();
        request_start(&store, "change", "k4").unwrap();
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("change").unwrap().status, "failed");
        assert_eq!(output(&http).len(), 6);
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn k4_file_deadband_fresh_filters_and_resets_state_on_restart() {
    let scratch = Scratch::new("deadband-fresh");
    let file = scratch.0.join("telemetry.ndjson");
    std::fs::write(
        &file,
        b"{\"device_id\":\"edge-a\",\"temperature\":20.0}\n{\"device_id\":\"edge-a\",\"temperature\":20.4}\n{\"device_id\":\"edge-a\",\"temperature\":20.9}\n{\"device_id\":\"edge-a\",\"temperature\":21.6}\n{\"device_id\":\"edge-a\",\"temperature\":21.7}\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open_memory().unwrap());
        install_telemetry(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse_spec(with_source_sink(
            graph_spec(
                "deadband",
                iot_config(
                    true,
                    5_000_000,
                    "error",
                    Some(json!({"mode":"absolute","baseline":"last_output","threshold":1.0})),
                ),
            ),
            &file,
            &http.url(),
        ));
        store.put_pipeline("deadband", &spec, None).unwrap();
        request_start(&store, "deadband", "k4").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 2, &sup, &store, "deadband").await;
        assert_eq!(temperature_values(&http), vec![20.0, 21.6]);
        sup.kill_named("deadband").await.unwrap();
        request_start(&store, "deadband", "k4").unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 4, &sup, &store, "deadband").await;
        assert_eq!(temperature_values(&http), vec![20.0, 21.6, 20.0, 21.6]);
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn k4_invalid_ttl_deadband_shape_and_jetstream_are_rejected() {
    let catalog = {
        let mut catalog = sparrow_plan::Catalog::new();
        catalog.insert(
        "telemetry",
        crate::stream_schema(
            "telemetry",
            &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
        )
            .unwrap(),
        );
        catalog
    };
    let invalid_ttl = parse_spec(graph_spec(
        "change_detect",
        iot_config(true, -1, "ignore", None),
    ));
    assert_eq!(
        crate::bind_plan(&invalid_ttl, &catalog, "ttl", 1)
            .unwrap_err()
            .code,
        sparrow_model::ErrorCode::InvalidArgument
    );

    let mut invalid_deadband = graph_spec(
        "deadband",
        iot_config(
            true,
            0,
            "error",
            Some(json!({"mode":"absolute","baseline":"last_output","threshold":1.0})),
        ),
    );
    invalid_deadband["graph"]["nodes"][1]["iot"]["fields"] = json!(["temperature", "temperature"]);
    let invalid_deadband = parse_spec(invalid_deadband);
    assert_eq!(
        crate::bind_plan(&invalid_deadband, &catalog, "deadband", 1)
            .unwrap_err()
            .code,
        sparrow_model::ErrorCode::InvalidArgument
    );

    let mut aligned_ttl = graph_spec("change_detect", iot_config(true, 1_000, "ignore", None));
    aligned_ttl["recovery"] = json!("aligned");
    let aligned_ttl = parse_spec(aligned_ttl);
    let plan = crate::bind_plan(&aligned_ttl, &catalog, "ttl-aligned", 1).unwrap();
    assert_eq!(
        crate::validate_aligned_plan(&aligned_ttl, &plan)
            .unwrap_err()
            .code,
        sparrow_model::ErrorCode::UnsupportedRestore
    );

    let mut jetstream = graph_spec("change_detect", iot_config(true, 0, "ignore", None));
    jetstream["source"] = json!({
        "kind":"jetstream",
        "jetstream": {
            "servers":["nats://127.0.0.1:4222"],
            "namespace":"default",
            "stream":"events",
            "consumer":"consumer",
            "ownership_bucket":"bucket"
        }
    });
    let error = PipelineSpec::from_json(&serde_json::to_vec(&jetstream).unwrap()).unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::FeatureUnavailable);
    assert!(error.message.contains("IoT"));
}

#[test]
fn k4_linear_aligned_iot_requires_explicit_checkpoint_dir_and_http_sink() {
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "telemetry",
        crate::stream_schema(
            "telemetry",
            &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
        )
        .unwrap(),
    );

    let mut missing_dir = graph_spec("change_detect", iot_config(true, 0, "ignore", None));
    missing_dir["recovery"] = json!("aligned");
    let missing_dir = parse_spec(missing_dir);
    let missing_plan = crate::bind_plan(&missing_dir, &catalog, "k4-missing-dir", 1).unwrap();
    assert_eq!(
        crate::validate_aligned_plan(&missing_dir, &missing_plan)
            .unwrap_err()
            .code,
        sparrow_model::ErrorCode::UnsupportedRestore
    );

    let mut non_http = graph_spec("change_detect", iot_config(true, 0, "ignore", None));
    non_http["recovery"] = json!("aligned");
    non_http["checkpoint_dir"] = json!("/tmp/k4-linear-checkpoint");
    non_http["sink"] = json!({"kind":"log"});
    let non_http = parse_spec(non_http);
    let plan = crate::bind_plan(&non_http, &catalog, "k4-non-http", 1).unwrap();
    assert_eq!(
        crate::validate_aligned_plan(&non_http, &plan)
            .unwrap_err()
            .code,
        sparrow_model::ErrorCode::UnsupportedRestore
    );

    let effective = crate::effective_guarantees_with_plan(&missing_dir, &missing_plan);
    assert_eq!(effective["aligned_eligible"], false);
}

#[test]
fn k4_effective_iot_metadata_covers_live_sources_without_generation() {
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "telemetry",
        crate::stream_schema(
            "telemetry",
            &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
        )
        .unwrap(),
    );
    for source in [
        json!({"kind":"mqtt","host":"127.0.0.1","port":1883}),
        json!({"kind":"http_push","bind":"127.0.0.1:0"}),
    ] {
        let mut value = graph_spec(
            "deadband",
            iot_config(
                true,
                5_000_000,
                "error",
                Some(json!({"mode":"absolute","baseline":"last_output","threshold":1.0})),
            ),
        );
        value["source"] = source;
        let spec = parse_spec(value);
        let plan = crate::bind_plan(&spec, &catalog, "k4-live-iot", 1).unwrap();
        let effective = crate::effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(effective["iot"]["recovery"], "restart_fresh_empty_state_no_persisted_generation");
        assert_eq!(effective["iot"]["continuity"], "not_preserved_on_restart_or_reset");
        assert_eq!(effective["iot"]["ttl_micros"], json!([5_000_000]));
    }
}

#[test]
fn k4_replay_label_requires_all_graph_sources_replayable() {
    let scratch = Scratch::new("replay-label");
    let first = scratch.0.join("first.ndjson");
    let second = scratch.0.join("second.ndjson");
    let checkpoint = scratch.0.join("checkpoint");
    let spec = graph_change_spec(&first, &second, "http://127.0.0.1:1/out", &checkpoint);
    let mut value = serde_json::to_value(spec).unwrap();
    value["recovery"] = json!("restart_fresh");
    value["checkpoint"] = Value::Null;
    value["graph_io"]["sources"]["2"] = json!({
        "kind":"mqtt","host":"127.0.0.1","port":1883
    });
    let mixed = parse_spec(value);
    assert_eq!(
        crate::validate::replay_label_for_spec(&mixed),
        "unsupported"
    );
}

#[test]
fn k4_graph_io_schema_resolution_uses_physical_source_chain() {
    let source = json!({"kind":"file","path":"/tmp/k4-source-a.ndjson"});
    let second = json!({"kind":"file","path":"/tmp/k4-source-b.ndjson"});
    let sink = json!({"kind":"log"});
    let spec = parse_spec(json!({
        "version":1,
        "stream":"s1",
        "source":source,
        "sink":sink,
        "graph_io":{"sources":{"1":source,"2":second},"sinks":{"3":sink,"4":sink}},
        "graph":{"version":1,"pipeline_id":405,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"s1","out":[5]},
            {"id":2,"kind":"memory_source","table":"s2","out":[6]},
            {"id":5,"kind":"change_detect","iot":{"keys":["device_id"],"fields":["temperature"],"emit_first":true,"ttl_micros":0,"max_keys":8,"invalid":"error"},"out":[3]},
            {"id":6,"kind":"change_detect","iot":{"keys":["device_id"],"fields":["state"],"emit_first":true,"ttl_micros":0,"max_keys":8,"invalid":"error"},"out":[4]},
            {"id":3,"kind":"capture_sink","name":"a"},
            {"id":4,"kind":"capture_sink","name":"b"}
        ]}
    }));
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "s1",
        crate::stream_schema(
            "s1",
            &serde_json::from_str(r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"temperature","type":"float64","nullable":false}]}"#).unwrap(),
        ).unwrap(),
    );
    catalog.insert(
        "s2",
        crate::stream_schema(
            "s2",
            &serde_json::from_str(r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"state","type":"bool","nullable":false}]}"#).unwrap(),
        ).unwrap(),
    );
    let plan = crate::bind_plan(&spec, &catalog, "k4-schema", 1).unwrap();
    let second_schema = crate::validate::graph_endpoint_schema(&plan, 2, true).unwrap();
    assert_eq!(second_schema.field_by_name("state").unwrap().data_type, sparrow_model::DataType::Bool);
    assert!(second_schema.field_by_name("temperature").is_none());
}

#[test]
fn k4_legacy_linear_templates_remain_aligned_compatible() {
    let scratch = Scratch::new("legacy-templates");
    let input = scratch.0.join("input.ndjson");
    let checkpoint = scratch.0.join("checkpoint");
    std::fs::write(&input, b"{\"device_id\":\"d\",\"v\":1}\n").unwrap();
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "sensors",
        crate::stream_schema(
            "sensors",
            &serde_json::from_str(
                r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}"#,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    for (name, text) in [
        (
            "zero",
            include_str!("../../../deploy/pipeline-k1-zero.json"),
        ),
        (
            "two-count",
            include_str!("../../../deploy/pipeline-k1-two-count.json"),
        ),
    ] {
        let mut spec = PipelineSpec::from_json(text.as_bytes()).unwrap();
        spec.source.path = Some(input.to_string_lossy().into_owned());
        spec.checkpoint_dir = Some(checkpoint.join(name).to_string_lossy().into_owned());
        let plan = crate::bind_plan(&spec, &catalog, name, 1).unwrap();
        assert!(!plan.has_iot());
        crate::validate_aligned_plan(&spec, &plan).unwrap();
    }
}

#[test]
fn k4_templates_match_the_declared_golden_contract() {
    let change: PipelineSpec =
        PipelineSpec::from_json(include_bytes!("../../../deploy/pipeline-k4-change.json")).unwrap();
    let deadband: PipelineSpec =
        PipelineSpec::from_json(include_bytes!("../../../deploy/pipeline-k4-deadband.json"))
            .unwrap();
    assert_eq!(change.recovery, "aligned");
    assert_eq!(deadband.recovery, "restart_fresh");
    assert_eq!(
        change.graph.as_ref().unwrap().nodes[1].kind,
        "change_detect"
    );
    assert_eq!(deadband.graph.as_ref().unwrap().nodes[1].kind, "deadband");
    let change_golden: Vec<Value> =
        serde_json::from_str(include_str!("../../../deploy/k4-change.expected.json")).unwrap();
    let deadband_golden: Vec<Value> =
        serde_json::from_str(include_str!("../../../deploy/k4-deadband.expected.json")).unwrap();
    assert_eq!(change_golden.len(), 3);
    assert_eq!(deadband_golden.len(), 2);

    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "telemetry",
        crate::stream_schema(
            "telemetry",
            &serde_json::from_str(TELEMETRY_SCHEMA).unwrap(),
        )
        .unwrap(),
    );
    let fresh_plan = crate::bind_plan(&deadband, &catalog, "k4-deadband", 1).unwrap();
    let fresh = crate::effective_guarantees_with_plan(&deadband, &fresh_plan);
    assert_eq!(fresh["checkpoint_participants"]["snapshot_version"], 6);
    assert_eq!(fresh["aligned_eligible"], false);
    assert_eq!(
        fresh["iot"]["continuity"],
        "not_preserved_on_restart_or_reset"
    );
    let aligned_plan = crate::bind_plan(&change, &catalog, "k4-change", 1).unwrap();
    let aligned = crate::effective_guarantees_with_plan(&change, &aligned_plan);
    assert_eq!(aligned["checkpoint_participants"]["snapshot_version"], 6);
    assert_eq!(
        aligned["iot"]["continuity"],
        "preserved_from_compatible_v6_checkpoint"
    );

    // Execute the shipped fixtures, not just a hand-written equivalent plan.
    let scratch = Scratch::new("shipped-templates");
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        for (name, mut spec, input, expected) in [
            ("change", change, include_str!("../../../deploy/k4-change.ndjson"), change_golden),
            ("deadband", deadband, include_str!("../../../deploy/k4-deadband.ndjson"), deadband_golden),
        ] {
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            let file = scratch.0.join(format!("{name}.ndjson"));
            std::fs::write(&file, input).unwrap();
            spec.source.path = Some(file.to_string_lossy().into_owned());
            spec.sink.url = Some(http.url());
            if spec.recovery == "aligned" {
                spec.checkpoint_dir = Some(scratch.0.join("checkpoint").to_string_lossy().into_owned());
            }
            let store = Arc::new(Store::open(&scratch.0.join(format!("{name}.db"))).unwrap());
            install_telemetry(&store);
            store.put_allow("127.0.0.1", http.port()).unwrap();
            store.put_pipeline(name, &spec, None).unwrap();
            request_start(&store, name, "k4").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            wait_rows(&http, expected.len(), &sup, &store, name).await;
            assert_eq!(output(&http), expected);
            if spec.recovery == "aligned" {
                assert!(sup.checkpoint_named(name).await.unwrap() > 0,
                    "the shipped append-only template must remain checkpointable at EOF");
            }
            sup.stop_all().await;
            http.stop().await;
        }
    });
}

fn append(path: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

//! B2-A control-plane coverage: immutable static Lookup dependencies on the
//! File -> required HTTP aligned profile.  The older fresh path and all
//! stateful/temporal/other-source rejection boundaries remain explicit.

use crate::{request_start, PipelineSpec, ReferenceTableSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let root = sparrow_connectors::ensure_default_data_root().join(format!(
            "sparrow-b2-{label}-{}-{}",
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

fn install_stream(store: &Store) {
    store
        .put_stream(
            "sensors",
            &json!({
                "fields":[
                    {"name":"device_id","type":"utf8","nullable":false},
                    {"name":"v","type":"int64","nullable":false}
                ]
            })
            .to_string(),
        )
        .unwrap();
}

fn table_spec(threshold_a: i64) -> ReferenceTableSpec {
    serde_json::from_value(json!({
        "fields":[
            {"name":"device_id","type":"utf8","nullable":false},
            {"name":"threshold","type":"int64","nullable":false}
        ],
        "keys":["device_id"],
        "rows":[["a",threshold_a],["b",20]]
    }))
    .unwrap()
}

fn base_value(
    path: &std::path::Path,
    checkpoint: Option<&std::path::Path>,
    url: &str,
    binding: (&str, u64, &str),
) -> Value {
    let mut references = serde_json::Map::new();
    references.insert(
        binding.0.to_string(),
        json!({"revision":binding.1,"sha256":binding.2}),
    );
    let mut value = json!({
        "version":1,
        "stream":"sensors",
        "reference_tables": references,
        "source":{"kind":"file","path":path,"file_contract":"append_only","inbox_capacity":8},
        "sink":{"kind":"http","url":url,"outbox_capacity":8,"batch_rows":8,"linger_ms":1},
        "delivery":"live_best_effort",
        "recovery": if checkpoint.is_some() {"aligned"} else {"restart_fresh"},
        "graph":{"version":1,"pipeline_id":802,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
            {"id":2,"kind":"lookup","table":binding.0,
                "on":[{"stream":"device_id","table":"device_id"}],
                "keep":["threshold"],"out":[3]},
            {"id":3,"kind":"capture_sink","name":"output"}
        ]}
    });
    if let Some(checkpoint) = checkpoint {
        value["checkpoint_dir"] = json!(checkpoint);
        value["checkpoint"] = json!({"resume_latest":true});
    }
    value
}

fn parse(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
        .collect()
}

async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    expected: usize,
    supervisor: &Arc<Supervisor>,
    store: &Arc<Store>,
    name: &str,
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while output(http).len() < expected {
            supervisor.converge_once().await.unwrap();
            let actual = store.actual(name).unwrap();
            assert_ne!(actual.status, "failed", "{actual:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("B2-A HTTP output deadline");
}

#[test]
fn core_b2_reference_aligned_plan_uses_cpl3_and_profile8_shape() {
    let scratch = Scratch::new("shape");
    let store = Store::open_memory().unwrap();
    install_stream(&store);
    let table = store
        .publish_reference_table("limits", 0, &table_spec(10))
        .unwrap();
    let value = base_value(
        &scratch.0.join("input.ndjson"),
        Some(&scratch.0.join("checkpoint")),
        "http://127.0.0.1:9/unused",
        ("limits", table.revision, &table.sha256),
    );
    let spec = parse(value);
    let plan = crate::validate::bind_plan_with_store(&store, &spec, "shape", 1).unwrap();
    crate::validate::validate_aligned_plan(&spec, &plan).unwrap();
    let shape = crate::validate::reference_dependency_shape(&spec).unwrap();
    assert_eq!(shape.len(), 1);
    assert_eq!(shape[0].runtime_crc32, 0);
    let layout = crate::validate::checkpoint_plan_with_references(&spec, &plan, &shape).unwrap();
    assert!(layout.has_references());
    assert!(layout.states.is_empty());
    assert_eq!(&layout.encode().unwrap()[..4], b"CPL3");

    // Also exercise the shipped production-shaped template: its zero digest
    // is replaced only after the catalog publication returns the exact
    // immutable identity.
    let mut shipped: Value = serde_json::from_slice(include_bytes!(
        "../../../deploy/pipeline-reference-lookup-aligned.json"
    ))
    .unwrap();
    shipped["source"]["path"] = json!(scratch.0.join("shipped.ndjson"));
    shipped["checkpoint_dir"] = json!(scratch.0.join("shipped-checkpoint"));
    shipped["sink"]["url"] = json!("http://127.0.0.1:9/shipped");
    shipped["reference_tables"]["limits"] =
        json!({"revision":table.revision,"sha256":table.sha256});
    let shipped = parse(shipped);
    let shipped_plan = crate::validate::bind_plan_with_store(&store, &shipped, "shipped", 1)
        .unwrap();
    crate::validate::validate_aligned_plan(&shipped, &shipped_plan).unwrap();
    let shipped_effective = crate::effective_guarantees_with_plan(&shipped, &shipped_plan);
    assert_eq!(
        shipped_effective["checkpoint_participants"]["snapshot_version"],
        sparrow_runtime::pipeline_checkpoint::REFERENCE_SNAPSHOT_VERSION
    );
    assert_eq!(
        shipped_effective["checkpoint_participants"]["manifest_version"],
        "CPL3"
    );
}

#[test]
fn core_b2_reference_fresh_path_remains_b1_compatible_without_checkpoint_dir() {
    let scratch = Scratch::new("fresh");
    let store = Store::open_memory().unwrap();
    install_stream(&store);
    let table = store
        .publish_reference_table("limits", 0, &table_spec(10))
        .unwrap();
    let spec = parse(base_value(
        &scratch.0.join("input.ndjson"),
        None,
        "http://127.0.0.1:9/unused",
        ("limits", table.revision, &table.sha256),
    ));
    let plan = crate::validate::bind_plan_with_store(&store, &spec, "fresh", 1).unwrap();
    crate::validate::validate_aligned_plan(&spec, &plan).unwrap();
    let effective = crate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(effective["recovery"], "restart_fresh");
    assert_eq!(effective["aligned_eligible"], false);
}

#[test]
fn core_b2_file_lookup_manual_checkpoint_restore_uses_verified_revision() {
    let scratch = Scratch::new("file-restore");
    let input = scratch.0.join("input.ndjson");
    let checkpoint = scratch.0.join("checkpoint");
    std::fs::write(
        &input,
        b"{\"device_id\":\"a\",\"v\":7}\n{\"device_id\":\"unknown\",\"v\":8}\n{\"device_id\":\"b\",\"v\":9}\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&scratch.0.join("catalog.db")).unwrap());
        install_stream(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let published = store
            .publish_reference_table("limits", 0, &table_spec(10))
            .unwrap();
        let spec = parse(base_value(
            &input,
            Some(&checkpoint),
            &http.url(),
            ("limits", published.revision, &published.sha256),
        ));
        store.put_pipeline("lookup", &spec, None).unwrap();
        request_start(&store, "lookup", "b2").unwrap();
        let supervisor = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        supervisor.converge_once().await.unwrap();
        wait_rows(&http, 3, &supervisor, &store, "lookup").await;
        let checkpoint_id = supervisor.checkpoint_named("lookup").await.unwrap();
        let current_before = std::fs::read(checkpoint.join("CURRENT")).unwrap();
        let checkpoint_inventory = sparrow_runtime::CheckpointStore::open_readonly(&checkpoint)
            .unwrap()
            .inventory()
            .unwrap();
        let current_generation = checkpoint_inventory
            .generations
            .iter()
            .find(|generation| generation.current)
            .expect("CURRENT generation");
        assert_eq!(
            current_generation
                .metadata
                .as_ref()
                .expect("snapshot metadata")
                .version,
            sparrow_runtime::pipeline_checkpoint::REFERENCE_SNAPSHOT_VERSION
        );

        // Publishing a new head does not mutate the already attached v1
        // snapshot.  The running lookup must continue returning threshold 10.
        let newer = store
            .publish_reference_table("limits", published.revision, &table_spec(99))
            .unwrap();
        assert_ne!(newer.sha256, published.sha256);
        let mut appended = std::fs::OpenOptions::new()
            .append(true)
            .open(&input)
            .unwrap();
        appended
            .write_all(b"{\"device_id\":\"a\",\"v\":11}\n")
            .unwrap();
        appended.sync_all().unwrap();
        wait_rows(&http, 4, &supervisor, &store, "lookup").await;
        assert_eq!(output(&http)[3]["threshold"], 10);
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current_before);

        supervisor.kill_named("lookup").await.unwrap();
        request_start(&store, "lookup", "b2").unwrap();
        supervisor.converge_once().await.unwrap();
        wait_rows(&http, 5, &supervisor, &store, "lookup").await;
        assert_eq!(output(&http)[4]["threshold"], 10);
        assert_eq!(
            supervisor
                .checkpoint_snapshot("lookup")
                .unwrap()
                .unwrap()
                .1
                .snapshot()
                .restored_from,
            Some(checkpoint_id)
        );
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current_before);
        supervisor.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn core_b2_reference_binding_change_refuses_restore_and_preserves_current() {
    let scratch = Scratch::new("binding-change");
    let input = scratch.0.join("input.ndjson");
    let checkpoint = scratch.0.join("checkpoint");
    std::fs::write(&input, b"{\"device_id\":\"a\",\"v\":7}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&scratch.0.join("catalog.db")).unwrap());
        install_stream(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let old = store
            .publish_reference_table("limits", 0, &table_spec(10))
            .unwrap();
        let spec = parse(base_value(
            &input,
            Some(&checkpoint),
            &http.url(),
            ("limits", old.revision, &old.sha256),
        ));
        store.put_pipeline("lookup-change", &spec, None).unwrap();
        request_start(&store, "lookup-change", "b2").unwrap();
        let supervisor = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        supervisor.converge_once().await.unwrap();
        wait_rows(&http, 1, &supervisor, &store, "lookup-change").await;
        supervisor.checkpoint_named("lookup-change").await.unwrap();
        let current_before = std::fs::read(checkpoint.join("CURRENT")).unwrap();

        let newer = store
            .publish_reference_table("limits", old.revision, &table_spec(99))
            .unwrap();
        let mut changed = serde_json::to_value(&spec).unwrap();
        changed["reference_tables"]["limits"] =
            json!({"revision":newer.revision,"sha256":newer.sha256});
        let changed = parse(changed);
        let etag = store.get_pipeline("lookup-change").unwrap().etag;
        store
            .put_pipeline("lookup-change", &changed, Some(&etag))
            .unwrap();
        request_start(&store, "lookup-change", "b2").unwrap();
        supervisor.converge_once().await.unwrap();
        assert_eq!(store.actual("lookup-change").unwrap().status, "failed");
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current_before);
        supervisor.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn core_b2_corrupt_reference_catalog_refuses_start_without_touching_current() {
    let scratch = Scratch::new("catalog-corrupt");
    let input = scratch.0.join("input.ndjson");
    let catalog_path = scratch.0.join("catalog.db");
    let checkpoint = scratch.0.join("checkpoint");
    std::fs::write(&input, b"{\"device_id\":\"a\",\"v\":7}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&catalog_path).unwrap());
        install_stream(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let published = store
            .publish_reference_table("limits", 0, &table_spec(10))
            .unwrap();
        let spec = parse(base_value(
            &input,
            Some(&checkpoint),
            &http.url(),
            ("limits", published.revision, &published.sha256),
        ));
        store.put_pipeline("lookup-corrupt", &spec, None).unwrap();
        request_start(&store, "lookup-corrupt", "b2").unwrap();
        let supervisor = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        supervisor.converge_once().await.unwrap();
        wait_rows(&http, 1, &supervisor, &store, "lookup-corrupt").await;
        supervisor.checkpoint_named("lookup-corrupt").await.unwrap();
        let current_before = std::fs::read(checkpoint.join("CURRENT")).unwrap();
        supervisor.stop_all().await;
        drop(supervisor);
        drop(store);

        // Damage the persisted payload itself, not the request binding.  The
        // next exact-resolution/start must fail before opening the checkpoint
        // source profile or changing CURRENT.
        let connection = rusqlite::Connection::open(&catalog_path).unwrap();
        connection
            .execute(
                "UPDATE reference_table_revisions
                 SET table_json=?1
                 WHERE name='limits' AND revision=1",
                [r#"{"fields":[],"keys":[],"rows":[]}"#],
            )
            .unwrap();
        drop(connection);

        let corrupted = Arc::new(Store::open(&catalog_path).unwrap());
        request_start(&corrupted, "lookup-corrupt", "b2").unwrap();
        let restarted = Supervisor::new(corrupted.clone(), kernel.clone(), false, None).unwrap();
        restarted.converge_once().await.unwrap();
        let actual = corrupted.actual("lookup-corrupt").unwrap();
        assert_eq!(actual.status, "failed");
        assert!(actual
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("reference table fields")));
        assert_eq!(output(&http).len(), 1, "corrupt catalog must not activate File/HTTP I/O");
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current_before);
        restarted.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn core_b2_reference_history_requires_explicit_resume_or_new_directory() {
    let scratch = Scratch::new("history-guard");
    let input = scratch.0.join("input.ndjson");
    let catalog_path = scratch.0.join("catalog.db");
    let checkpoint = scratch.0.join("checkpoint");
    std::fs::write(&input, b"{\"device_id\":\"a\",\"v\":7}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&catalog_path).unwrap());
        install_stream(&store);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let published = store
            .publish_reference_table("limits", 0, &table_spec(10))
            .unwrap();

        // An empty v8 directory may be started explicitly without automatic
        // resume.  Manual checkpointing remains available in this mode.
        let mut fresh_value = base_value(
            &input,
            Some(&checkpoint),
            &http.url(),
            ("limits", published.revision, &published.sha256),
        );
        fresh_value["checkpoint"] = json!({"resume_latest": false});
        let fresh = parse(fresh_value);
        store.put_pipeline("lookup-history", &fresh, None).unwrap();
        request_start(&store, "lookup-history", "b2").unwrap();
        let supervisor = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        supervisor.converge_once().await.unwrap();
        wait_rows(&http, 1, &supervisor, &store, "lookup-history").await;
        let checkpoint_id = supervisor.checkpoint_named("lookup-history").await.unwrap();
        assert_ne!(checkpoint_id, 0);
        let current_before = std::fs::read(checkpoint.join("CURRENT")).unwrap();
        let output_before = output(&http);

        // Reusing the same v8 directory without an explicit resume must not
        // silently turn into a fresh replay, even when resume_latest is false.
        supervisor.kill_named("lookup-history").await.unwrap();
        request_start(&store, "lookup-history", "b2").unwrap();
        supervisor.converge_once().await.unwrap();
        let rejected = store.actual("lookup-history").unwrap();
        assert_eq!(rejected.status, "failed", "{rejected:?}");
        assert!(rejected
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("reference checkpoint history")));
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current_before);
        assert_eq!(output(&http), output_before);

        // A new stored revision that explicitly enables resume_latest may
        // recover the old cut.  Since the only row was already committed, the
        // source must seek past it and emit no duplicate HTTP row.
        let mut resume_value = serde_json::to_value(&fresh).unwrap();
        resume_value["checkpoint"] = json!({"resume_latest": true});
        let resumed = parse(resume_value);
        let etag = store.get_pipeline("lookup-history").unwrap().etag;
        store
            .put_pipeline("lookup-history", &resumed, Some(&etag))
            .unwrap();
        request_start(&store, "lookup-history", "b2").unwrap();
        supervisor.converge_once().await.unwrap();
        let running = store.actual("lookup-history").unwrap();
        assert_eq!(running.status, "running", "{running:?}");
        assert_eq!(output(&http), output_before);
        assert_eq!(
            supervisor
                .checkpoint_snapshot("lookup-history")
                .unwrap()
                .unwrap()
                .1
                .snapshot()
                .restored_from,
            Some(checkpoint_id)
        );
        supervisor.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn core_b2_reference_aligned_rejects_et_pt_temporal_lossy_and_unreliable_sources() {
    let scratch = Scratch::new("reject");
    let store = Store::open_memory().unwrap();
    install_stream(&store);
    let table = store
        .publish_reference_table("limits", 0, &table_spec(10))
        .unwrap();
    let base = base_value(
        &scratch.0.join("input.ndjson"),
        Some(&scratch.0.join("checkpoint")),
        "http://127.0.0.1:9/unused",
        ("limits", table.revision, &table.sha256),
    );

    for (name, kind) in [("pt", "processing_time"), ("et", "event_time")] {
        let mut value = base.clone();
        value["graph"]["nodes"] = json!([
            {"id":1,"kind":"memory_source","table":"sensors","event_time_field":if kind == "event_time" {json!("v")} else {Value::Null},"out":[2]},
            {"id":2,"kind":"lookup","table":"limits","on":[{"stream":"device_id","table":"device_id"}],"keep":["threshold"],"out":[3]},
            {"id":3,"kind":"window_agg","keys":["device_id"],"event_time_field":if kind == "event_time" {json!("v")} else {Value::Null},"window":{"kind":kind,"size_micros":1000,"size":2,"event_time_field":if kind == "event_time" {json!("v")} else {Value::Null}},"aggs":[{"fn":"sum","expr":{"k":"col","name":"v"},"alias":"total"}],"out":[4]},
            {"id":4,"kind":"capture_sink","name":"output"}
        ]);
        if kind == "event_time" {
            // Explicit source time makes this a DAG plan even with one
            // source. Bind a valid connector topology before testing the
            // reference checkpoint's unsupported time-state boundary.
            value["graph_io"] = json!({
                "sources":{"1":value["source"].clone()},
                "sinks":{"4":value["sink"].clone()}
            });
        }
        let spec = parse(value);
        let plan = crate::validate::bind_plan_with_store(&store, &spec, name, 1).unwrap();
        let error = crate::validate::validate_aligned_plan(&spec, &plan).unwrap_err();
        assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore);
    }

    let mut temporal = base.clone();
    temporal["graph"]["nodes"][1]["temporal"] = json!(true);
    temporal["graph"]["nodes"][1]["as_of_field"] = json!("v");
    assert!(crate::validate::bind_plan_with_store(
        &store,
        &parse(temporal),
        "temporal",
        1
    )
    .is_err());

    let mut lossy = base.clone();
    let source = lossy["source"].clone();
    let sink = lossy["sink"].clone();
    lossy["graph_io"] = json!({
        "sources":{"1":source},
        "sinks":{"4":sink.clone(),"5":sink}
    });
    lossy["graph"]["nodes"] = json!([
        {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
        {"id":2,"kind":"lookup","table":"limits","on":[{"stream":"device_id","table":"device_id"}],"keep":["threshold"],"out":[3]},
        {"id":3,"kind":"branch","best_effort":[5],"out":[4,5]},
        {"id":4,"kind":"capture_sink","name":"required"},
        {"id":5,"kind":"best_effort_sink","name":"lossy"}
    ]);
    let lossy_spec = parse(lossy);
    let lossy_plan = crate::validate::bind_plan_with_store(&store, &lossy_spec, "lossy", 1).unwrap();
    assert!(crate::validate::validate_aligned_plan(&lossy_spec, &lossy_plan).is_err());

    let mut unreliable = base;
    unreliable["source"] = json!({"kind":"mqtt","host":"127.0.0.1","port":1883});
    assert!(PipelineSpec::from_json(&serde_json::to_vec(&unreliable).unwrap()).is_err());
}

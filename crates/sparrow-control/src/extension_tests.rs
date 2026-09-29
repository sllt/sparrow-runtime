#![cfg(all(target_os = "linux", target_env = "gnu"))]
use crate::*;
use serde_json::{json, Value};
use sparrow_expr::plugins as plugin_runtime;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
#[path = "../../../tests/support/extension_plugin.rs"]
mod fixture;
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_control_stop_under_backpressure_reclaims_sessions_and_pins() {
    let _serial = SERIAL.lock().unwrap();
    let (d, store, manager, mut bindings) = setup();
    let (m, b) = fixture::fault("sink");
    bindings[1] = serde_json::to_value(fixture::install(&manager, m, &b)).unwrap();
    let mut spec = spec(&bindings, &d.0, false);
    spec.source.plugin.as_mut().unwrap().config = json!({"start":1,"count":1000000});
    spec.sink.plugin.as_mut().unwrap().config = json!({"mode":"hang"});
    store.put_pipeline("stop", &spec, None).unwrap();
    let kernel = Arc::new(host_kernel().unwrap());
    kernel.block_on(async {
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "stop", "test").unwrap();
        sup.converge_once().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while plugin_runtime::extension::active_sessions() < 2 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(manager
            .disable(bindings[0]["manifest_sha256"].as_str().unwrap())
            .is_err());
        tokio::time::sleep(Duration::from_millis(20)).await;
        let at = Instant::now();
        request_stop(&store, "stop", "test").unwrap();
        sup.converge_once().await.unwrap();
        assert!(at.elapsed() < Duration::from_secs(3));
        assert_eq!(store.actual("stop").unwrap().status, "stopped");
        assert_eq!(plugin_runtime::extension::active_sessions(), 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.live_tasks(), 0);
        assert!(manager.list().unwrap().iter().all(|i| i.pins == 0));
        sup.stop_all().await;
    });
}

#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_control_reserves_memory_before_sink_open_effects() {
    let _serial = SERIAL.lock().unwrap();
    let (d, _store, manager, bindings) = setup();
    let binding = serde_json::from_value(bindings[1].clone()).unwrap();
    let extension = manager
        .resolve_extension(&binding, plugin_runtime::extension::Role::Sink)
        .unwrap();
    let mut budget = sparrow_model::ResourceBudget::compact();
    budget.reservation_bytes = 4096;
    let owner = sparrow_model::MemoryOwner::new(budget);
    let diag = sparrow_connectors::IoDiagnostics::new();
    let output = d.0.join("must-not-open.ndjson");
    let kernel = host_kernel().unwrap();
    kernel.block_on(async {
        let (tx, rx) = sparrow_io::observed::channel(1);
        drop(tx);
        crate::plugin_io::sink(
            kernel.handle(),
            extension,
            json!({"path":output}),
            rx,
            tokio_util::sync::CancellationToken::new(),
            owner.clone(),
            diag.clone(),
            None,
        )
        .await
        .unwrap();
    });
    assert!(!output.exists());
    assert_eq!(
        diag.plugin_failed
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(owner.usage().reservation_bytes, 0);
    assert_eq!(plugin_runtime::extension::active_sessions(), 0);
}
fn setup() -> (fixture::Dir, Arc<Store>, Arc<plugins::Manager>, Vec<Value>) {
    let d = fixture::dir();
    let manager = fixture::manager(&d.0.join("packages"));
    let mut bindings = Vec::new();
    for role in ["source", "sink", "transform"] {
        let (m, b) = fixture::sample(role);
        bindings.push(serde_json::to_value(fixture::install(&manager, m, &b)).unwrap());
    }
    let store = Arc::new(Store::open_memory().unwrap());
    store.configure_plugins(manager.clone()).unwrap();
    store
        .put_stream(
            "s",
            r#"{"fields":[{"name":"value","type":"int64","nullable":false}]}"#,
        )
        .unwrap();
    (d, store, manager, bindings)
}
fn spec(bindings: &[Value], directory: &std::path::Path, graph: bool) -> PipelineSpec {
    let mut source = bindings[0].clone();
    source["config"] = json!({"start":1,"count":35});
    let source = json!({"kind":"plugin","inbox_capacity":1,"plugin":source});
    let mut sink = bindings[1].clone();
    sink["config"] = json!({"path":directory.join("output.ndjson")});
    let sink = json!({"kind":"plugin","outbox_capacity":1,"plugin":sink});
    let mut body = json!({"stream":"s","source":source,"sink":sink});
    if graph {
        let mut transformed = sink.clone();
        transformed["plugin"]["config"]["path"] = json!(directory.join("transformed.ndjson"));
        let mut transform = bindings[2].clone();
        transform["config"] = json!({"factor":2,"copies":2});
        body["graph"] = json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"s","out":[2]},
            {"id":2,"kind":"branch","out":[3,4]},
            {"id":3,"kind":"plugin_transform","plugin":transform,"out":[5]},
            {"id":4,"kind":"capture_sink","name":"original"},
            {"id":5,"kind":"capture_sink","name":"transformed"}]});
        body["graph_io"] = json!({"sources":{"1":source},"sinks":{"4":sink,"5":transformed}});
    } else {
        body["sql"] = json!("SELECT value FROM s");
    }
    PipelineSpec::from_json(&serde_json::to_vec(&body).unwrap()).unwrap()
}
fn rows(path: std::path::PathBuf) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}
#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_control_linear_dag_backpressure_refs_and_reaping() {
    let _serial = SERIAL.lock().unwrap();
    for graph in [false, true] {
        let (d, store, manager, bindings) = setup();
        let spec = spec(&bindings, &d.0, graph);
        let plan = bind_plan_with_store(&store, &spec, "extension", 1).unwrap();
        assert_eq!(plan.edges.is_some(), graph);
        assert!(
            !effective_guarantees_with_plan(&spec, &plan)["aligned_eligible"]
                .as_bool()
                .unwrap()
        );
        drop(plan);
        let published = store.put_pipeline("extension", &spec, None).unwrap();
        let kernel = Arc::new(host_kernel().unwrap());
        kernel.block_on(async {
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "extension", "test").unwrap();
            sup.converge_once().await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                sup.converge_once().await.unwrap();
                let actual = store.actual("extension").unwrap();
                assert_ne!(actual.status, "failed", "{:?}", actual.last_error);
                if actual.status == "completed" {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "extension did not complete: {:?}",
                    actual
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                rows(d.0.join("output.ndjson")),
                (1..36).map(|v| json!({"value":v})).collect::<Vec<_>>()
            );
            if graph {
                assert_eq!(
                    rows(d.0.join("transformed.ndjson")),
                    (1..36)
                        .flat_map(|v| vec![json!({"value":v*2}); 2])
                        .collect::<Vec<_>>()
                );
            }
            assert_eq!(plugin_runtime::extension::active_sessions(), 0);
            assert_eq!(kernel.admitted_jobs(), 0);
            assert_eq!(kernel.live_tasks(), 0);
            // Restart must hold, not replay a non-durable native connector.
            store.reset_actual_after_process_restart().unwrap();
            sup.converge_once().await.unwrap();
            assert!(store.actual("extension").unwrap().restart_blocked);
            assert_eq!(plugin_runtime::extension::active_sessions(), 0);
            request_stop(&store, "extension", "test").unwrap();
            sup.converge_once().await.unwrap();
            sup.stop_all().await;
        });
        for binding in bindings.iter().take(if graph { 3 } else { 2 }) {
            let hash = binding["manifest_sha256"].as_str().unwrap();
            assert_eq!(store.plugin_references(hash).unwrap()["count"], 1);
            manager.disable(hash).unwrap();
            assert!(store.uninstall_plugin(hash).is_err());
        }
        store.retire_pipeline("extension", &published.etag).unwrap();
        for binding in bindings.iter().take(if graph { 3 } else { 2 }) {
            store
                .uninstall_plugin(binding["manifest_sha256"].as_str().unwrap())
                .unwrap();
        }
    }
}
#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_control_validation_and_opaque_config_references() {
    let _serial = SERIAL.lock().unwrap();
    let (d, store, manager, bindings) = setup();
    let spec = spec(&bindings, &d.0, true);
    let raw = serde_json::to_value(&spec).unwrap();
    for change in [
        json!({"recovery":"aligned"}),
        json!({"checkpoint_dir":"/never"}),
        json!({"restore":{"kind":"none"}}),
    ] {
        let mut body = raw.clone();
        body.as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        assert!(PipelineSpec::from_json(&serde_json::to_vec(&body).unwrap()).is_err());
    }
    let mut wrong = spec.clone();
    wrong.graph.as_mut().unwrap().nodes[2]
        .plugin
        .as_mut()
        .unwrap()
        .manifest_sha256 = bindings[0]["manifest_sha256"].as_str().unwrap().into();
    assert!(bind_plan_with_store(&store, &wrong, "wrong", 1).is_err());
    let mut wrong = spec.clone();
    wrong.graph.as_mut().unwrap().nodes[2]
        .plugin
        .as_mut()
        .unwrap()
        .config = json!({"large":"x".repeat(4096)});
    assert!(bind_plan_with_store(&store, &wrong, "wrong", 1).is_err());
    let mut opaque = spec.clone();
    opaque.graph.as_mut().unwrap().nodes[2]
        .plugin
        .as_mut()
        .unwrap()
        .config = json!({"k":"call","name":"plugin_call","args":[]});
    // Durable reference extraction only traverses real expressions, not config.
    store.put_pipeline("opaque", &opaque, None).unwrap();
    assert_eq!(
        store
            .plugin_references(bindings[2]["manifest_sha256"].as_str().unwrap())
            .unwrap()["count"],
        1
    );
    let plan = bind_plan_with_store(&store, &spec, "query", 1).unwrap();
    assert!(sparrow_runtime::finite::execute(
        plan,
        Default::default(),
        Default::default(),
        tokio_util::sync::CancellationToken::new()
    )
    .is_err());
    let mut nullable = serde_json::to_value(&spec).unwrap();
    nullable["sink"]["action"] = json!({});
    assert!(PipelineSpec::from_json(&serde_json::to_vec(&nullable).unwrap()).is_err());
    assert!(manager.list().unwrap().iter().all(|i| i.pins == 0));
}
#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_control_fault_is_failed_not_completed_or_auto_replayed() {
    let _serial = SERIAL.lock().unwrap();
    let (d, store, manager, mut bindings) = setup();
    let (m, b) = fixture::fault("sink");
    bindings[1] = serde_json::to_value(fixture::install(&manager, m, &b)).unwrap();
    let mut spec = spec(&bindings, &d.0, false);
    spec.sink.plugin.as_mut().unwrap().config = json!({"mode":"flush_fail"});
    store.put_pipeline("fault", &spec, None).unwrap();
    let kernel = Arc::new(host_kernel().unwrap());
    kernel.block_on(async {
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "fault", "test").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            sup.converge_once().await.unwrap();
            if store.actual("fault").unwrap().status == "failed" {
                break;
            }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let attempt = store.actual("fault").unwrap().attempt_id;
        for _ in 0..5 {
            sup.converge_once().await.unwrap();
        }
        assert_eq!(store.actual("fault").unwrap().attempt_id, attempt);
        assert!(store.actual("fault").unwrap().restart_blocked);
        sup.stop_all().await;
        assert_eq!(plugin_runtime::extension::active_sessions(), 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.live_tasks(), 0);
    });
}

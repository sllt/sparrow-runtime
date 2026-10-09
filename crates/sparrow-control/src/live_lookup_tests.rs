//! Control-to-Kernel hot-follow integration, not just in-memory table unit tests.
use crate::*;
use serde_json::{json, Value};
use std::{io::Write, sync::Arc, time::Duration};
struct Dir(std::path::PathBuf);
impl Dir {
    fn new() -> Self {
        let d = sparrow_connectors::default_data_root().join(format!(
            "lookup-live-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn table(value: i64) -> ReferenceTableSpec {
    serde_json::from_value(json!({"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"threshold","type":"int64","nullable":false}],"keys":["device_id"],"rows":[["a",value]]})).unwrap()
}
fn store() -> Arc<Store> {
    let s = Arc::new(Store::open_memory().unwrap());
    s.put_stream(
        "sensors",
        r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false}]}"#,
    )
    .unwrap();
    s
}
fn spec(
    path: &std::path::Path,
    url: &str,
    revision: &ReferenceTableRow,
    follow: bool,
) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&json!({"stream":"sensors","reference_tables":{"limits":{"revision":revision.revision,"sha256":revision.sha256,"follow_latest":follow}},"source":{"kind":"file","path":path,"file_contract":"append_only"},"sink":{"kind":"http","url":url},"graph":{"version":1,"pipeline_id":1,"revision_id":1,"nodes":[{"id":1,"kind":"memory_source","table":"sensors","out":[2]},{"id":2,"kind":"lookup","table":"limits","on":[{"stream":"device_id","table":"device_id"}],"keep":["threshold"],"out":[3]},{"id":3,"kind":"capture_sink"}]}})).unwrap()).unwrap()
}
fn append(path: &std::path::Path) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(b"{\"device_id\":\"a\"}\n").unwrap();
    f.sync_data().unwrap();
}
#[cfg(feature = "demo-io")]
fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|b| serde_json::from_slice::<Vec<Value>>(b).unwrap())
        .collect()
}
#[cfg(feature = "demo-io")]
async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    n: usize,
    sup: &Arc<Supervisor>,
    store: &Store,
    name: &str,
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while output(http).len() < n {
            sup.converge_once().await.unwrap();
            assert_ne!(
                store.actual(name).unwrap().status,
                "failed",
                "{:?}",
                store.actual(name).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[test]
#[cfg(feature = "demo-io")]
fn tab_live_follow_updates_rolls_back_and_static_pin_does_not_change() {
    for follow in [false, true] {
        let dir = Dir::new();
        let path = dir.0.join("source.ndjson");
        std::fs::write(&path, b"").unwrap();
        let store = store();
        let first = store
            .publish_reference_table("limits", 0, &table(10))
            .unwrap();
        let kernel = Arc::new(host_kernel().unwrap());
        kernel.block_on(async {
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            store.put_allow("127.0.0.1", http.port()).unwrap();
            let config = spec(&path, &http.url(), &first, follow);
            store.put_pipeline("hot", &config, None).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "hot", "test").unwrap();
            sup.converge_once().await.unwrap();
            append(&path);
            wait_rows(&http, 1, &sup, &store, "hot").await;
            assert_eq!(output(&http)[0]["threshold"], 10);
            let mutation = serde_json::from_value(
                json!({"expected_revision":1,"operations":[{"op":"upsert","row":["a",20]}]}),
            )
            .unwrap();
            store.mutate_reference_table("limits", &mutation).unwrap();
            sup.converge_once().await.unwrap();
            append(&path);
            wait_rows(&http, 2, &sup, &store, "hot").await;
            assert_eq!(output(&http)[1]["threshold"], if follow { 20 } else { 10 });
            if follow {
                assert_eq!(
                    sup.lookup_snapshot("hot").unwrap()["live_tables"]["limits"]
                        ["observed_revision"],
                    2
                );
            }
            store
                .rollback_reference_table(
                    "limits",
                    &RollbackSpec {
                        expected_revision: 2,
                        target_revision: 1,
                    },
                )
                .unwrap();
            sup.converge_once().await.unwrap();
            append(&path);
            wait_rows(&http, 3, &sup, &store, "hot").await;
            assert_eq!(output(&http)[2]["threshold"], 10);
            assert_eq!(
                store.get_pipeline("hot").unwrap().spec.reference_tables["limits"].revision,
                1
            );
            sup.stop_all().await;
            http.stop().await;
            assert_eq!(kernel.admitted_jobs(), 0);
            assert_eq!(kernel.live_tasks(), 0);
        });
    }
}
#[test]
#[cfg(feature = "demo-io")]
fn tab_live_start_resolves_latest_then_incompatible_publication_fails_without_stale_fallback() {
    let dir = Dir::new();
    let path = dir.0.join("source.ndjson");
    std::fs::write(&path, b"").unwrap();
    let store = store();
    let first = store
        .publish_reference_table("limits", 0, &table(10))
        .unwrap();
    store
        .publish_reference_table("limits", 1, &table(30))
        .unwrap();
    let kernel = Arc::new(host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        store
            .put_pipeline("hot", &spec(&path, &http.url(), &first, true), None)
            .unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "hot", "test").unwrap();
        sup.converge_once().await.unwrap();
        append(&path);
        wait_rows(&http, 1, &sup, &store, "hot").await;
        assert_eq!(output(&http)[0]["threshold"], 30);
        let mut incompatible = table(40);
        incompatible.fields[1].name = "changed".into();
        store
            .publish_reference_table("limits", 2, &incompatible)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                sup.converge_once().await.unwrap();
                if store.actual("hot").unwrap().status == "failed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let attempt = store.actual("hot").unwrap().attempt_id;
        append(&path);
        for _ in 0..3 {
            sup.converge_once().await.unwrap();
        }
        assert_eq!(store.actual("hot").unwrap().attempt_id, attempt);
        assert_eq!(output(&http).len(), 1);
        assert!(store.actual("hot").unwrap().restart_blocked);
        sup.stop_all().await;
        http.stop().await;
        assert_eq!(kernel.admitted_jobs(), 0);
    });
}
#[test]
fn tab_live_and_remote_binding_recovery_and_schema_gates_preserve_old_json() {
    let dir = Dir::new();
    let path = dir.0.join("source.ndjson");
    std::fs::write(&path, b"").unwrap();
    let store = store();
    let first = store
        .publish_reference_table("limits", 0, &table(10))
        .unwrap();
    let old = spec(&path, "http://127.0.0.1:9/", &first, false);
    assert!(serde_json::to_value(&old)
        .unwrap()
        .get("external_lookups")
        .is_none());
    assert!(
        serde_json::to_value(&old).unwrap()["reference_tables"]["limits"]
            .get("follow_latest")
            .is_none()
    );
    let live = spec(&path, "http://127.0.0.1:9/", &first, true);
    assert!(live.requires_explicit_restart());
    let live_plan = bind_plan_with_store(&store, &live, "live", 1).unwrap();
    let mut forged = live.clone();
    forged.recovery = "aligned".into();
    forged.checkpoint_dir = Some(dir.0.join("checkpoint").to_string_lossy().into_owned());
    assert!(
        validate_aligned_plan(&forged, &live_plan).is_err(),
        "public aligned validator must reject a serde-created live spec"
    );
    for patch in [
        json!({"recovery":"aligned"}),
        json!({"checkpoint_dir":"/no"}),
        json!({"restore":{"kind":"none"}}),
    ] {
        let mut value = serde_json::to_value(&live).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        assert!(PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut value = serde_json::to_value(&old).unwrap();
    value["reference_tables"] = json!({});
    value["external_lookups"] = json!({"limits":{"url":"http://127.0.0.1:9/lookup","fields":table(10).fields,"keys":["device_id"],"options":{"max_inflight":2,"cache_ttl_ms":0}}});
    let remote = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
    let plan = bind_plan_with_store(&store, &remote, "remote", 1).unwrap();
    assert_eq!(
        effective_guarantees_with_plan(&remote, &plan)["aligned_eligible"],
        false
    );
    assert!(validate::validate_io_with_plan(
        &remote,
        plan.source_schema().unwrap(),
        &plan,
        &validate::StoreSecrets::new(store.clone()),
        &sparrow_connectors::TargetPolicy::deny_all(),
        None
    )
    .is_err());
    assert!(
        validate_io(
            &remote,
            plan.source_schema().unwrap(),
            &validate::StoreSecrets::new(store.clone()),
            &sparrow_connectors::TargetPolicy::deny_all(),
            None
        )
        .is_err(),
        "legacy IO validator must enforce remote target policy too"
    );
    value["external_lookups"]["limits"]["keys"] = json!(["threshold"]);
    let wrong = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(bind_plan_with_store(&store, &wrong, "wrong", 1).is_err());
}

#[test]
#[cfg(feature = "demo-io")]
fn tab_live_graph_refresh_failure_is_visible_before_reap_without_root_counter_duplication() {
    let dir = Dir::new();
    let path_a = dir.0.join("source-a.ndjson");
    let path_b = dir.0.join("source-b.ndjson");
    std::fs::write(&path_a, b"").unwrap();
    std::fs::write(&path_b, b"").unwrap();
    let store = store();
    let first = store
        .publish_reference_table("limits", 0, &table(10))
        .unwrap();
    let kernel = Arc::new(host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut value = serde_json::to_value(spec(&path_a, &http.url(), &first, true)).unwrap();
        let source_a = value["source"].clone();
        let mut source_b = source_a.clone();
        source_b["path"] = json!(path_b);
        value["graph_io"] = json!({
            "sources":{"1":source_a,"4":source_b},
            "sinks":{"3":value["sink"].clone()},
        });
        value["graph"]["nodes"] = json!([
            {"id":1,"kind":"memory_source","table":"sensors","out":[5]},
            {"id":4,"kind":"memory_source","table":"sensors","out":[5]},
            {"id":5,"kind":"union_all","out":[2]},
            {"id":2,"kind":"lookup","table":"limits","on":[{"stream":"device_id","table":"device_id"}],"keep":["threshold"],"out":[3]},
            {"id":3,"kind":"capture_sink"},
        ]);
        let config = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        let plan = bind_plan_with_store(&store, &config, "graph_hot", 1).unwrap();
        assert!(plan.edges.is_some(), "the regression must exercise the DAG startup path");
        store.put_pipeline("graph_hot", &config, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "graph_hot", "test").unwrap();
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("graph_hot").unwrap().status, "running");
        assert!(sup.flow_snapshot("graph_hot").unwrap().unwrap().graph_ports.is_some());
        assert_eq!(sup.io_snapshot().await.lookup_update_failed, 0);

        let mut incompatible = table(20);
        incompatible.fields[1].name = "changed".into();
        store.publish_reference_table("limits", 1, &incompatible).unwrap();
        // This convergence reaps only at entry, before triggering refresh.
        // Append-only empty inputs keep the Job live until refresh cancels it;
        // the just-failed attempt stays in the registry until the NEXT reap,
        // independent of how quickly its tasks observe cancellation.
        sup.converge_once().await.unwrap();
        let flow = sup.flow_snapshot("graph_hot").unwrap().expect("not yet reaped");
        let ports = flow.graph_ports.as_ref().expect("graph port diagnostics");
        assert_eq!(flow.diagnostics.lookup_update_failed.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(sup.lookup_snapshot("graph_hot").unwrap()["live_tables"]["limits"]["failed"], true);

        // Distinct test values make accidental full-root merging visible,
        // not just the omitted operator-level refresh-failure counter.
        flow.diagnostics.http_posted.store(7, std::sync::atomic::Ordering::Relaxed);
        ports.sinks[&3].http_posted.store(11, std::sync::atomic::Ordering::Relaxed);
        let io = sup.io_snapshot().await;
        assert_eq!(io.lookup_update_failed, 1);
        assert_eq!(io.http_posted, 11, "root connector counters must not be merged again");
        assert!(http.bodies().is_empty());
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                sup.converge_once().await.unwrap();
                if store.actual("graph_hot").unwrap().status == "failed" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(store.actual("graph_hot").unwrap().restart_blocked);
        assert!(!sup.lookup_snapshot("graph_hot").unwrap()["available"].as_bool().unwrap());
        assert_eq!(sup.io_snapshot().await.lookup_update_failed, 0, "running-attempt counters disappear after reap");
        sup.stop_all().await;
        http.stop().await;
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.live_tasks(), 0);
    });
}

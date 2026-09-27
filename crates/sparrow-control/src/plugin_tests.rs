#![cfg(all(target_os = "linux", target_env = "gnu"))]
use crate::*;
use sparrow_model::{Row, Scalar};
use std::sync::Arc;
#[path = "../../../tests/support/native_plugin.rs"]
mod fixture;
fn setup() -> (fixture::Dir, Arc<Store>, Arc<plugins::Manager>, String) {
    let d = fixture::dir();
    let manager = plugins::Manager::open(&d.0.join("plugins"), true).unwrap();
    let digest = manager
        .install(
            serde_json::from_value(fixture::manifest()).unwrap(),
            &fixture::artifact().0,
        )
        .unwrap()
        .manifest_sha256;
    manager.enable(&digest, &digest).unwrap();
    let store = Arc::new(Store::open_memory().unwrap());
    store.configure_plugins(manager.clone()).unwrap();
    store
        .put_stream(
            "s",
            r#"{"fields":[{"name":"value","type":"int64","nullable":true}]}"#,
        )
        .unwrap();
    (d, store, manager, digest)
}
#[test]
fn plugins_sql_graph_kernel_pin_and_scope_isolation() {
    let (_dir, store, manager, digest) = setup();
    let catalog = binder_catalog(&store).unwrap();
    let logical =
        sparrow_sql::bind_sql(&fixture::sql(&digest), &catalog, 1.into(), 1.into()).unwrap();
    let plan = sparrow_plan::physicalize(&logical, &Default::default());
    assert!(plan.has_plugins());
    drop(logical);
    let kernel = sparrow_runtime::Kernel::new(Default::default()).unwrap();
    let capture = sparrow_runtime::SharedCapture::new();
    kernel
        .run(sparrow_runtime::JobRequest::new(
            plan.clone(),
            vec![
                Row {
                    values: vec![Scalar::Int64(21)],
                },
                Row {
                    values: vec![Scalar::Null],
                },
            ],
            capture.clone(),
        ))
        .unwrap();
    assert_eq!(
        capture.rows(),
        vec![vec![Scalar::Int64(42)], vec![Scalar::Null]]
    );
    assert!(manager.disable(&digest).is_err());
    assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
    assert!(sparrow_runtime::finite::execute(
        plan.clone(),
        std::collections::HashMap::from([(1.into(), vec![])]),
        Default::default(),
        tokio_util::sync::CancellationToken::new()
    )
    .is_err());
    let raw = serde_json::json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"project","exprs":[{"alias":"doubled","expr":{"k":"call","name":"plugin_call","args":[{"k":"lit","value":{"t":"utf8","v":"native_math"}},{"k":"lit","value":{"t":"utf8","v":"v1"}},{"k":"lit","value":{"t":"utf8","v":digest}},{"k":"lit","value":{"t":"utf8","v":"double"}},{"k":"col","name":"value"}]}}],"out":[3]},{"id":3,"kind":"capture_sink"}]});
    let graph = sparrow_plan::GraphSpec::from_json(&raw.to_string()).unwrap();
    let logical_graph = sparrow_plan::bind_graph(&graph, &catalog).unwrap();
    let graph = sparrow_plan::physicalize(&logical_graph, &Default::default());
    drop(logical_graph);
    let out = sparrow_runtime::SharedCapture::new();
    kernel
        .run(sparrow_runtime::JobRequest::new(
            graph,
            vec![Row {
                values: vec![Scalar::Int64(-4)],
            }],
            out.clone(),
        ))
        .unwrap();
    assert_eq!(out.rows(), vec![vec![Scalar::Int64(-8)]]);
    let mut other = sparrow_plan::Catalog::new();
    other.insert("s", plan.source_schema().unwrap().clone());
    assert!(sparrow_sql::bind_sql(&fixture::sql(&digest), &other, 1.into(), 1.into()).is_err());
    drop(plan);
    manager.disable(&digest).unwrap();
    assert_eq!(manager.list().unwrap()[0].pins, 0);
    assert_eq!(kernel.live_tasks(), 0);
}
#[test]
fn plugins_binding_recovery_and_malformed_calls_reject_before_io() {
    let (_dir, store, manager, digest) = setup();
    let sql = fixture::sql(&digest);
    let catalog = binder_catalog(&store).unwrap();
    for bad in [
        sql.replace("'v1'", "'v2'"),
        sql.replace(&digest, &"0".repeat(64)),
        sql.replace("'double'", "'missing'"),
        sql.replace(",value)", ",'text')"),
        sql.replace("('native_math'", "(value"),
        sql.replace("AS doubled", "OVER () AS doubled"),
    ] {
        assert!(
            sparrow_sql::bind_sql(&bad, &catalog, 1.into(), 1.into()).is_err(),
            "{bad}"
        );
    }
    let spec = serde_json::json!({"version":1,"stream":"s","sql":sql,"source":{"kind":"file","path":"/var/lib/sparrow/data/absent.ndjson","file_contract":"sealed"},"sink":{"kind":"http","url":"http://127.0.0.1:9/"},"recovery":"restart_fresh"});
    let mut spec = PipelineSpec::from_json(&serde_json::to_vec(&spec).unwrap()).unwrap();
    let plan = bind_plan_with_store(&store, &spec, "native", 1).unwrap();
    validate_aligned_plan(&spec, &plan).unwrap();
    spec.recovery = "aligned".into();
    assert!(validate_aligned_plan(&spec, &plan).is_err());
    assert_eq!(
        effective_guarantees_with_plan(&spec, &plan)["aligned_eligible"],
        false
    );
    drop(plan);
    assert_eq!(manager.list().unwrap()[0].pins, 0);
}
#[test]
fn plugins_running_job_cancellation_holds_pin_and_refunds() {
    let (_dir, store, manager, digest) = setup();
    let plan = sparrow_plan::physicalize(
        &sparrow_sql::bind_sql(
            &fixture::sql(&digest),
            &binder_catalog(&store).unwrap(),
            1.into(),
            1.into(),
        )
        .unwrap(),
        &Default::default(),
    );
    let kernel = sparrow_runtime::Kernel::new(Default::default()).unwrap();
    let capture = sparrow_runtime::SharedCapture::new();
    capture.stall.stall();
    let (tx, rx) = sparrow_io::observed::channel(1);
    let mut request = sparrow_runtime::JobRequest::new(plan, vec![], capture);
    request.live_in = Some(rx);
    let job = kernel.submit(request).unwrap();
    let owner = job.memory_owner();
    kernel.block_on(async {
        tx.send(Row {
            values: vec![Scalar::Int64(1)],
        })
        .await
        .unwrap();
        assert!(manager.disable(&digest).is_err());
        tokio::time::timeout(std::time::Duration::from_secs(3), job.stop())
            .await
            .unwrap()
            .ok();
    });
    drop(tx);
    manager.disable(&digest).unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(kernel.live_tasks(), 0);
}
#[cfg(feature = "demo-io")]
#[test]
fn plugins_two_real_file_values_reach_required_http() {
    let (_dir, store, manager, digest) = setup();
    let root = sparrow_connectors::ensure_default_data_root().join(format!(
        "native-{}.ndjson",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    std::fs::write(&root, b"{\"value\":21}\n{\"value\":-4}\n").unwrap();
    let kernel = Arc::new(host_kernel().unwrap());
    kernel.block_on(async{let http=sparrow_connectors::HttpCapture::start().await.unwrap();store.put_allow("127.0.0.1",http.port()).unwrap();
        let spec=serde_json::json!({"version":1,"stream":"s","sql":fixture::sql(&digest),"source":{"kind":"file","path":root,"file_contract":"sealed"},"sink":{"kind":"http","url":http.url()},"recovery":"restart_fresh","fail_on_decode":true});
        let spec=PipelineSpec::from_json(&serde_json::to_vec(&spec).unwrap()).unwrap();store.put_pipeline("native",&spec,None).unwrap();let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();request_start(&store,"native","test").unwrap();
        let state=tokio::time::timeout(std::time::Duration::from_secs(8),async{loop{sup.converge_once().await.unwrap();let s=store.actual("native").unwrap();if matches!(s.status.as_str(),"failed"|"completed"){break s;}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}}).await.unwrap();assert_eq!(state.status.as_str(),"completed","{state:?}");
        let rows:Vec<serde_json::Value>=http.bodies().iter().flat_map(|b|serde_json::from_slice::<Vec<serde_json::Value>>(b).unwrap()).collect();assert_eq!(rows,vec![serde_json::json!({"doubled":42}),serde_json::json!({"doubled":-8})]);sup.stop_all().await;http.stop().await;
    });
    assert_eq!(manager.list().unwrap()[0].pins, 0);
}

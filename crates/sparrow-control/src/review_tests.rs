//! Supervisor / catalog regressions for the static review (R10/R24).

use std::sync::Arc;

use sparrow_model::ErrorCode;

use crate::spec::{PipelineSpec, SinkSpec, SourceSpec};
use crate::store::Store;
use crate::supervisor::{
    compact_kernel, request_start, request_start_at, request_stop, retry_backoff, Supervisor,
    MAX_PIPELINE_ATTEMPTS,
};

const STREAM: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"v","type":"int64","nullable":false}
]}"#;

const STREAM_ET: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"v","type":"int64","nullable":false},
  {"name":"ts","type":"int64","nullable":false}
]}"#;

const ET_TUMBLE_SQL: &str =
    "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, TUMBLE(ts, INTERVAL '10' SECOND)";

#[cfg(feature = "demo-io")]
fn http_window_rows(http: &sparrow_connectors::HttpCapture) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for body in http.body_strings() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            match v {
                serde_json::Value::Array(items) => out.extend(items),
                other => out.push(other),
            }
        }
    }
    out
}

#[cfg(feature = "demo-io")]
fn window_counts(rows: &[serde_json::Value]) -> Vec<i64> {
    rows.iter()
        .filter_map(|r| r.get("n").and_then(|v| v.as_i64()))
        .collect()
}

fn tmp(name: &str) -> std::path::PathBuf {
    sparrow_connectors::ensure_default_data_root().join(format!(
        "sparrow-ctl-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn file_spec(path: &str, sql: &str, recovery: &str, chk: Option<&str>) -> PipelineSpec {
    PipelineSpec {
        version: 1,
        stream: "sensors".into(),
        sql: Some(sql.into()),
        graph: None,
        source: SourceSpec {
            kind: "file".into(),
            host: None,
            port: None,
            topic: "sensors/json".into(),
            client_id: None,
            qos: 0,
            clean_session: true,
            username_secret: None,
            password_secret: None,
            skip_verify: false,
            inbox_capacity: 8,
            use_demo_io: false,
            bind: None,
            path: Some(path.into()),
            tls: false,
            file_contract: None,
        },
        sink: SinkSpec {
            kind: "log".into(),
            url: None,
            skip_verify: false,
            outbox_capacity: 8,
            use_demo_io: false,
            header_secret: None,
            host: None,
            port: None,
            topic: None,
            client_id: None,
            qos: 0,
            clean_session: true,
            tls: false,
        },
        delivery: "live_best_effort".into(),
        recovery: recovery.into(),
        restore: None,
        checkpoint_dir: chk.map(|s| s.to_string()),
        fail_on_decode: false,
    }
}

#[test]
fn r24_start_failure_is_actual_failed() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("missing.ndjson");
        // Aligned without a window operator — start path must fail closed.
        let spec = file_spec(
            &path.to_string_lossy(),
            "SELECT device_id FROM sensors",
            "aligned",
            None,
        );
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        store.put_pipeline("broken", &spec, None).unwrap();
        request_start(&store, "broken", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("broken").unwrap();
        assert_eq!(actual.status, "failed", "{actual:?}");
        assert!(actual.last_error.is_some());
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn r10_file_aligned_start_checkpoint_via_supervisor() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("events.ndjson");
        let chk = tmp("chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        let spec = file_spec(
            &path.to_string_lossy(),
            "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        store.put_pipeline("filehot", &spec, None).unwrap();
        request_start(&store, "filehot", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("filehot").unwrap();
        assert_eq!(actual.status, "running", "file+aligned must start: {:?}", actual.last_error);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let id = sup.checkpoint_named("filehot").await.expect("checkpoint");
        assert!(id >= 1);
        sup.kill_named("filehot").await.unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn r23_start_named_uses_desired_revision_not_latest() {
    let store = Store::open_memory().unwrap();
    store.put_stream("sensors", STREAM).unwrap();
    let path = tmp("rev.ndjson");
    std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
    let mut a = file_spec(
        &path.to_string_lossy(),
        "SELECT device_id FROM sensors",
        "restart_fresh",
        None,
    );
    store.put_pipeline("rev", &a, None).unwrap();
    a.sql = Some("SELECT v FROM sensors".into());
    store.put_pipeline("rev", &a, Some("rev-1")).unwrap();
    request_start_at(&store, "rev", "test", Some(1)).unwrap();
    let desired = store.desired("rev").unwrap();
    assert_eq!(desired.revision, Some(1));
    let loaded = store
        .get_pipeline_revision("rev", desired.revision.unwrap())
        .unwrap();
    assert_eq!(
        loaded.spec.sql.as_deref(),
        Some("SELECT device_id FROM sensors")
    );
    let latest = store.get_pipeline("rev").unwrap();
    assert_eq!(latest.latest_revision, 2);
    assert_ne!(loaded.spec.sql, latest.spec.sql);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn mqtt_aligned_still_rejected() {
    let spec = PipelineSpec {
        version: 1,
        stream: "sensors".into(),
        sql: Some("SELECT device_id FROM sensors".into()),
        graph: None,
        source: SourceSpec {
            kind: "mqtt".into(),
            host: Some("127.0.0.1".into()),
            port: Some(1883),
            topic: "t".into(),
            client_id: Some("x".into()),
            qos: 0,
            clean_session: true,
            username_secret: None,
            password_secret: None,
            skip_verify: false,
            inbox_capacity: 8,
            use_demo_io: false,
            bind: None,
            path: None,
            tls: false,
            file_contract: None,
        },
        sink: SinkSpec {
            kind: "log".into(),
            url: None,
            skip_verify: false,
            outbox_capacity: 8,
            use_demo_io: false,
            header_secret: None,
            host: None,
            port: None,
            topic: None,
            client_id: None,
            qos: 0,
            clean_session: true,
            tls: false,
        },
        delivery: "live_best_effort".into(),
        recovery: "aligned".into(),
        restore: None,
        checkpoint_dir: None,
        fail_on_decode: false,
    };
    assert_eq!(
        spec.check_delivery().unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
}

#[test]
fn n1_healthy_start_stop_cycles_not_held() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("n1-cycles.ndjson");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let spec = file_spec(
            &path.to_string_lossy(),
            "SELECT device_id FROM sensors",
            "restart_fresh",
            None,
        );
        store.put_pipeline("n1cycles", &spec, None).unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        for i in 0..10 {
            request_start(&store, "n1cycles", "test").unwrap();
            let _ = sup.converge_once().await;
            let actual = store.actual("n1cycles").unwrap();
            assert_eq!(
                actual.status, "running",
                "cycle {i} must start: attempt_id={} cf={} err={:?}",
                actual.attempt_id, actual.consecutive_failures, actual.last_error
            );
            assert_eq!(actual.consecutive_failures, 0);
            request_stop(&store, "n1cycles", "test").unwrap();
            let _ = sup.converge_once().await;
            let actual = store.actual("n1cycles").unwrap();
            assert_eq!(actual.status, "stopped", "cycle {i} stop: {actual:?}");
        }
        request_start(&store, "n1cycles", "test").unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n1cycles").unwrap();
        assert_eq!(
            actual.status, "running",
            "after ≥10 healthy start/stop cycles must still start; not stuck stopped without error: {actual:?}"
        );
        assert!(
            actual.attempt_id >= 16,
            "lifetime attempt_id should have passed the old cap: {}",
            actual.attempt_id
        );
        assert_eq!(actual.consecutive_failures, 0);
        assert!(
            actual.last_error.is_none()
                || !actual
                    .last_error
                    .as_deref()
                    .unwrap()
                    .starts_with("held:"),
            "{:?}",
            actual.last_error
        );
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn n1_consecutive_failure_cap_holds_with_error() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("n1-cap.ndjson");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let spec = file_spec(
            &path.to_string_lossy(),
            "SELECT device_id FROM sensors",
            "restart_fresh",
            None,
        );
        store.put_pipeline("n1cap", &spec, None).unwrap();
        for _ in 0..MAX_PIPELINE_ATTEMPTS {
            let attempt = store
                .actual("n1cap")
                .map(|a| a.attempt_id.saturating_add(1))
                .unwrap_or(1);
            store
                .set_actual("n1cap", "failed", Some(1), attempt, Some("boom"))
                .unwrap();
        }
        let before = store.actual("n1cap").unwrap();
        assert!(
            before.consecutive_failures >= MAX_PIPELINE_ATTEMPTS,
            "{before:?}"
        );
        store.set_desired("n1cap", "running", Some(1)).unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let held = store.actual("n1cap").unwrap();
        assert_ne!(
            held.status, "running",
            "cap must skip converge start: {held:?}"
        );
        let err = held.last_error.as_deref().unwrap_or("");
        assert!(
            err.contains("held:") && err.contains("consecutive_failures"),
            "held pipeline must record last_error, got {held:?}"
        );
        request_start(&store, "n1cap", "test").unwrap();
        assert_eq!(store.actual("n1cap").unwrap().consecutive_failures, 0);
        let _ = sup.converge_once().await;
        let after = store.actual("n1cap").unwrap();
        assert_eq!(
            after.status, "running",
            "request_start must clear the hold: {after:?}"
        );
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn n8_failed_pipeline_backoff_does_not_block_healthy_converge() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();

        // Failed sibling first so list_desired yields it before the healthy job.
        // Aligned without a window operator fails start (same as r24).
        let fail_path = tmp("n8-fail.ndjson");
        std::fs::write(&fail_path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let fail_spec = file_spec(
            &fail_path.to_string_lossy(),
            "SELECT device_id FROM sensors",
            "aligned",
            None,
        );
        store.put_pipeline("n8fail", &fail_spec, None).unwrap();
        let ok_path = tmp("n8-ok.ndjson");
        std::fs::write(&ok_path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let ok_spec = file_spec(
            &ok_path.to_string_lossy(),
            "SELECT device_id FROM sensors",
            "restart_fresh",
            None,
        );
        store.put_pipeline("n8ok", &ok_spec, None).unwrap();

        // reset_actual_after_process_restart rewrites status to stopped.
        // Seed the failed sibling *after* Supervisor::new so backoff applies.
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        for _ in 0..6 {
            let attempt = store
                .actual("n8fail")
                .map(|a| a.attempt_id.saturating_add(1))
                .unwrap_or(1);
            store
                .set_actual("n8fail", "failed", Some(1), attempt, Some("boom"))
                .unwrap();
        }
        let failed = store.actual("n8fail").unwrap();
        assert_eq!(failed.consecutive_failures, 6, "{failed:?}");
        assert!(
            retry_backoff(6) >= std::time::Duration::from_millis(2_000),
            "fixture must sit in the old 2.56s sleep window, got {:?}",
            retry_backoff(6)
        );
        store.set_desired("n8fail", "running", Some(1)).unwrap();
        request_start(&store, "n8ok", "test").unwrap();

        let t0 = std::time::Instant::now();
        let _ = sup.converge_once().await;
        let started = store.actual("n8ok").unwrap();
        assert_eq!(
            started.status, "running",
            "healthy start must not wait on the failed sibling backoff: {started:?}"
        );
        let start_elapsed = t0.elapsed();
        assert!(
            start_elapsed < std::time::Duration::from_millis(800),
            "healthy start took {start_elapsed:?}; old shared sleep is {:?}",
            retry_backoff(6)
        );

        request_stop(&store, "n8ok", "test").unwrap();
        let _ = sup.converge_once().await;
        let stopped = store.actual("n8ok").unwrap();
        assert_eq!(stopped.status, "stopped", "healthy stop: {stopped:?}");
        let total = t0.elapsed();
        assert!(
            total < std::time::Duration::from_secs(2),
            "healthy start+stop during the would-be sleep window took {total:?}"
        );

        let still_failed = store.actual("n8fail").unwrap();
        assert_ne!(
            still_failed.status, "running",
            "failed pipeline must stay deferred, not start in the same loop: {still_failed:?}"
        );

        let _ = std::fs::remove_file(&fail_path);
        let _ = std::fs::remove_file(&ok_path);
    });
}

#[test]
fn n2_empty_sql_is_invalid_argument() {
    let mut spec = file_spec(
        "/tmp/n2.ndjson",
        "SELECT device_id FROM sensors",
        "restart_fresh",
        None,
    );
    spec.sql = Some(String::new());
    let err = spec.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    spec.sql = Some("   \n".into());
    assert_eq!(
        spec.validate().unwrap_err().code,
        ErrorCode::InvalidArgument
    );
}

#[test]
fn p0_1_aligned_rejects_or_honors_where_projection() {
    use sparrow_model::{
        DataType, Field, FieldId, PipelineId, RevisionId, Row, Scalar, Schema, SchemaId,
    };
    use sparrow_plan::{physicalize, Catalog, PlanOptions};
    use sparrow_runtime::{JobRequest, SharedCapture};
    use sparrow_sql::bind_sql;

    let mut cat = Catalog::new();
    cat.insert(
        "sensors",
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap(),
    );
    let sql = "SELECT COUNT(*) AS n, device_id FROM sensors WHERE v > 1 GROUP BY device_id, COUNT_WINDOW(2)";
    let bound = bind_sql(sql, &cat, PipelineId::new(1), RevisionId::new(1)).unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    assert!(
        plan.stages
            .iter()
            .any(|s| matches!(s, sparrow_plan::PhysicalStage::Transform { .. })),
        "WHERE must remain on the aligned Kernel plan"
    );
    let root = sparrow_connectors::ensure_default_data_root();
    let spec = file_spec(
        &root.join("p01.ndjson").to_string_lossy(),
        sql,
        "aligned",
        Some(&root.join("p01-chk").to_string_lossy()),
    );
    crate::validate::validate_aligned_plan(&spec, &plan)
        .expect("aligned+WHERE must be honored (not stripped/rejected)");
    let kernel = compact_kernel().unwrap();
    let rows = [1, 2, 3, 4]
        .into_iter()
        .map(|v| Row {
            values: vec![Scalar::utf8("d1"), Scalar::Int64(v)],
        })
        .collect();
    let cap = SharedCapture::new();
    kernel
        .run(JobRequest::new(plan, rows, cap.clone()))
        .unwrap();
    assert_eq!(
        cap.row_count(),
        1,
        "honored WHERE v>1 + COUNT_WINDOW(2) emits one window; stripped filter would emit two: {:?}",
        cap.rows()
    );
}

#[test]
fn p0_2_pt_aligned_rejected() {
    use sparrow_model::{DataType, Field, FieldId, PipelineId, RevisionId, Schema, SchemaId};
    use sparrow_plan::{physicalize, Catalog, PlanOptions};
    use sparrow_sql::bind_sql;

    let mut cat = Catalog::new();
    cat.insert(
        "sensors",
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap(),
    );
    let sql = "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, TUMBLE(PROCESSING_TIME, INTERVAL '1' SECOND)";
    let bound = bind_sql(sql, &cat, PipelineId::new(1), RevisionId::new(1)).unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    assert!(plan.has_processing_time_window());
    let root = sparrow_connectors::ensure_default_data_root();
    let spec = file_spec(
        &root.join("p02.ndjson").to_string_lossy(),
        sql,
        "aligned",
        Some(&root.join("p02-chk").to_string_lossy()),
    );
    let err = crate::validate::validate_aligned_plan(&spec, &plan).unwrap_err();
    assert_eq!(err.code, ErrorCode::UnsupportedRestore);
    assert!(
        err.message.to_ascii_lowercase().contains("processing-time") || err.message.contains("PT"),
        "{}",
        err.message
    );
}

#[cfg(feature = "demo-io")]
#[test]
fn p0_4_barrier_waits_for_real_sink_flush() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("p04.ndjson");
        let chk = tmp("p04-chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        http.set_delay_ms(400);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(1)",
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        store.put_pipeline("p04", &spec, None).unwrap();
        request_start(&store, "p04", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("p04").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let current = chk.join("CURRENT");
        assert!(
            !current.exists(),
            "CURRENT must not exist before checkpoint"
        );
        let appeared_early = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&appeared_early);
        let watch = current.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            if watch.exists() {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        let t0 = std::time::Instant::now();
        let id = sup.checkpoint_named("p04").await.expect("checkpoint");
        let elapsed = t0.elapsed();
        assert!(id >= 1);
        assert!(
            elapsed >= std::time::Duration::from_millis(250),
            "checkpoint returned in {elapsed:?} — did not wait for HTTP sink flush"
        );
        assert!(
            !appeared_early.load(std::sync::atomic::Ordering::SeqCst),
            "CURRENT must not be published before the sink flush ack"
        );
        assert!(current.exists());
        sup.kill_named("p04").await.unwrap();
        http.stop().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn n4_stale_barrier_ack_not_used_for_next_checkpoint() {
    use sparrow_runtime::CheckpointStore;
    use std::io::Write;

    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("n4.ndjson");
        let chk = tmp("n4-chk");
        std::fs::create_dir_all(&chk).unwrap();
        // 2 rows → COUNT_WINDOW(2) emits once; freeze leftover = 0.
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        http.set_delay_ms(6_500);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        store.put_pipeline("n4", &spec, None).unwrap();
        request_start(&store, "n4", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n4").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        let current = chk.join("CURRENT");
        assert!(!current.exists(), "CURRENT must not exist before checkpoint");

        let first = sup.checkpoint_named("n4").await;
        assert!(
            first.is_err(),
            "slow sink must time out barrier #1 without a dishonest commit: {first:?}"
        );
        assert!(
            !current.exists(),
            "timed-out barrier must not publish CURRENT"
        );

        // Rows between barriers. A stitch of barrier #1's empty freeze + this
        // pos would drop v=3,4,5 from both state and replay.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(
                b"{\"device_id\":\"d1\",\"v\":3}\n{\"device_id\":\"d1\",\"v\":4}\n{\"device_id\":\"d1\",\"v\":5}\n",
            )
            .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        http.set_delay_ms(0);
        let wait0 = std::time::Instant::now();
        while http.bodies().is_empty() && wait0.elapsed() < std::time::Duration::from_secs(8) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // Late freeze/flush for id=1 must now sit in ack_rx (or have been drained).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let id = sup
            .checkpoint_named("n4")
            .await
            .expect("second checkpoint after sink recovers");
        assert!(
            id >= 2,
            "monotonic checkpoint id: timed-out barrier #1 must not be reused, got {id}"
        );
        assert!(current.exists(), "second checkpoint must publish CURRENT");

        let snap = CheckpointStore::open(&chk)
            .unwrap()
            .recover_committed()
            .unwrap()
            .expect("committed snapshot");
        assert_eq!(snap.checkpoint_id, id);
        assert_eq!(
            snap.ingested_rows, 5,
            "cut must be the freeze covering all ingested rows, not a stale ack + new pos: {snap:?}"
        );
        assert_eq!(snap.source.record_index, 5, "{:?}", snap.source);
        let leftover: u64 = snap.window.entries.iter().map(|e| e.count).sum();
        assert_eq!(
            leftover, 1,
            "COUNT_WINDOW(2) at 5 rows leftover=1; empty freeze from barrier #1 \
             stitched with current pos is leftover=0 (rows 3–5 lost): {snap:?}"
        );

        sup.kill_named("n4").await.unwrap();
        http.stop().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn n7_aligned_checkpoint_refuses_after_sink_4xx() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("n7.ndjson");
        let chk = tmp("n7-chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        http.set_status(400);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(1)",
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        store.put_pipeline("n7", &spec, None).unwrap();
        request_start(&store, "n7", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n7").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);
        let wait0 = std::time::Instant::now();
        loop {
            let io = sup.io_snapshot().await;
            if io.http_dropped >= 1 || io.http_failed >= 1 {
                break;
            }
            if wait0.elapsed() > std::time::Duration::from_secs(3) {
                panic!("HTTP 4xx drop never appeared in diag: {io}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let current = chk.join("CURRENT");
        assert!(!current.exists());
        let err = sup
            .checkpoint_named("n7")
            .await
            .expect_err("aligned must refuse commit after HTTP 4xx drop");
        assert!(
            !current.exists(),
            "CURRENT must not advance after a dropped/4xx flush"
        );
        let io = sup.io_snapshot().await;
        assert!(
            io.http_dropped >= 1 || io.http_failed >= 1,
            "drops must be visible in diag: {io}"
        );
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("refus") || msg.contains("drop") || msg.contains("align"),
            "{err}"
        );

        sup.kill_named("n7").await.unwrap();
        http.stop().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn n5_append_only_et_window_accepts_rows_after_eof_poll() {
    use std::io::Write;

    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM_ET).unwrap();
        let path = tmp("n5-ao.ndjson");
        let chk = tmp("n5-ao-chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            concat!(
                r#"{"device_id":"d1","v":1,"ts":1000000}"#,
                "\n",
                r#"{"device_id":"d1","v":2,"ts":4000000}"#,
                "\n",
            ),
        )
        .unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            ET_TUMBLE_SQL,
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        spec.source.file_contract = Some("append_only".into());
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        store.put_pipeline("n5ao", &spec, None).unwrap();
        request_start(&store, "n5ao", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n5ao").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);

        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_secs(2) {
            if kernel.metrics.snapshot().ingested_rows >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            kernel.metrics.snapshot().ingested_rows >= 2,
            "seed rows never ingested"
        );
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            http_window_rows(&http).is_empty(),
            "AppendOnly must not inject a terminal watermark at first EOF (that would close [0,10s) and poison later rows): {:?}",
            http_window_rows(&http)
        );

        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, r#"{{"device_id":"d1","v":3,"ts":12000000}}"#).unwrap();
            writeln!(f, r#"{{"device_id":"d1","v":4,"ts":22000000}}"#).unwrap();
            f.flush().unwrap();
        }

        let t1 = std::time::Instant::now();
        let mut saw_second = false;
        while t1.elapsed() < std::time::Duration::from_secs(3) {
            let ns = window_counts(&http_window_rows(&http));
            // 12s closes [0,10s) n=2; 22s closes [10s,20s) n=1.
            // A poisoned MAX watermark would emit only n=2 at first EOF
            // and drop the later window.
            if ns.iter().any(|&n| n == 1) {
                saw_second = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        assert!(
            saw_second,
            "rows appended after EOF poll must still produce ET finals (not all late): bodies={:?} ingested={}",
            http_window_rows(&http),
            kernel.metrics.snapshot().ingested_rows
        );
        let after = store.actual("n5ao").unwrap();
        assert_eq!(
            after.status, "running",
            "AppendOnly must keep polling, not complete on EOF: {:?}",
            after.last_error
        );

        sup.kill_named("n5ao").await.unwrap();
        http.stop().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn n5_sealed_eof_emits_final_et_windows() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM_ET).unwrap();
        let path = tmp("n5-seal.ndjson");
        let chk = tmp("n5-seal-chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            concat!(
                r#"{"device_id":"d1","v":1,"ts":1000000}"#,
                "\n",
                r#"{"device_id":"d1","v":2,"ts":4000000}"#,
                "\n",
            ),
        )
        .unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            ET_TUMBLE_SQL,
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        spec.source.file_contract = Some("sealed".into());
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        store.put_pipeline("n5s", &spec, None).unwrap();
        request_start(&store, "n5s", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n5s").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);

        let t0 = std::time::Instant::now();
        let mut saw_final = false;
        while t0.elapsed() < std::time::Duration::from_secs(3) {
            if window_counts(&http_window_rows(&http))
                .iter()
                .any(|&n| n == 2)
            {
                saw_final = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        assert!(
            saw_final,
            "Sealed EOF must emit the last ET window: {:?}",
            http_window_rows(&http)
        );

        let t1 = std::time::Instant::now();
        let mut completed = false;
        while t1.elapsed() < std::time::Duration::from_secs(3) {
            let _ = sup.converge_once().await;
            if store.actual("n5s").unwrap().status == "completed" {
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        assert!(
            completed,
            "Sealed file job must complete honestly after terminal watermark: {:?}",
            store.actual("n5s")
        );

        http.stop().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn n5_restart_fresh_sealed_emits_final_et_windows() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM_ET).unwrap();
        let path = tmp("n5-rf-seal.ndjson");
        std::fs::write(
            &path,
            concat!(
                r#"{"device_id":"d1","v":1,"ts":1000000}"#,
                "\n",
                r#"{"device_id":"d1","v":2,"ts":4000000}"#,
                "\n",
            ),
        )
        .unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            ET_TUMBLE_SQL,
            "restart_fresh",
            None,
        );
        spec.source.file_contract = Some("sealed".into());
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        store.put_pipeline("n5rfs", &spec, None).unwrap();
        request_start(&store, "n5rfs", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n5rfs").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);

        let t0 = std::time::Instant::now();
        let mut saw_final = false;
        while t0.elapsed() < std::time::Duration::from_secs(3) {
            if window_counts(&http_window_rows(&http))
                .iter()
                .any(|&n| n == 2)
            {
                saw_final = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        assert!(
            saw_final,
            "restart_fresh + Sealed must emit the last ET window: {:?}",
            http_window_rows(&http)
        );

        let t1 = std::time::Instant::now();
        let mut completed = false;
        while t1.elapsed() < std::time::Duration::from_secs(3) {
            let _ = sup.converge_once().await;
            if store.actual("n5rfs").unwrap().status == "completed" {
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        assert!(
            completed,
            "restart_fresh + Sealed must complete after EOF: {:?}",
            store.actual("n5rfs")
        );

        http.stop().await;
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn n5_restart_fresh_decode_errors_counted() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM_ET).unwrap();
        let path = tmp("n5-rf-dec.ndjson");
        std::fs::write(
            &path,
            concat!(
                r#"{"device_id":"d1","v":1,"ts":1000000}"#,
                "\n",
                "this is not json\n",
                r#"{"device_id":"d1","v":2,"ts":4000000}"#,
                "\n",
            ),
        )
        .unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            ET_TUMBLE_SQL,
            "restart_fresh",
            None,
        );
        spec.source.file_contract = Some("append_only".into());
        store.put_pipeline("n5rfd", &spec, None).unwrap();
        request_start(&store, "n5rfd", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n5rfd").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);

        let t0 = std::time::Instant::now();
        let mut errs = 0u64;
        while t0.elapsed() < std::time::Duration::from_secs(2) {
            errs = sup.io_snapshot().await.decode_errors;
            if errs >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            errs >= 1,
            "restart_fresh file decode errors must increment IoDiagnostics.decode_errors (got {errs})"
        );

        sup.kill_named("n5rfd").await.unwrap();
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn p1_17_fail_on_decode_fails_the_job() {
    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM_ET).unwrap();
        let path = tmp("p117-fail-dec.ndjson");
        std::fs::write(
            &path,
            concat!(
                r#"{"device_id":"d1","v":1,"ts":1000000}"#,
                "\n",
                "this is not json\n",
            ),
        )
        .unwrap();
        let mut spec = file_spec(
            &path.to_string_lossy(),
            ET_TUMBLE_SQL,
            "restart_fresh",
            None,
        );
        spec.source.file_contract = Some("append_only".into());
        spec.fail_on_decode = true;
        store.put_pipeline("p117", &spec, None).unwrap();
        request_start(&store, "p117", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let t0 = std::time::Instant::now();
        let mut failed = false;
        while t0.elapsed() < std::time::Duration::from_secs(3) {
            let _ = sup.converge_once().await;
            if let Ok(a) = store.actual("p117") {
                if a.status == "failed" {
                    failed = true;
                    let err = a.last_error.unwrap_or_default();
                    assert!(
                        err.contains("decode") || err.contains("fail_on_decode") || err.contains("codec"),
                        "failed for decode, got: {err}"
                    );
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        assert!(failed, "fail_on_decode must fail the job, not only count: {:?}", store.actual("p117"));
        let _ = sup.kill_named("p117").await;
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn n15_aligned_checkpoint_records_real_duration_and_bytes() {
    use sparrow_runtime::CheckpointStore;

    let kernel = Arc::new(compact_kernel().unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let path = tmp("n15.ndjson");
        let chk = tmp("n15-chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        let spec = file_spec(
            &path.to_string_lossy(),
            "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
            "aligned",
            Some(&chk.to_string_lossy()),
        );
        store.put_pipeline("n15", &spec, None).unwrap();
        request_start(&store, "n15", "test").unwrap();
        let sup = Supervisor::new(Arc::clone(&store), Arc::clone(&kernel), false, None).unwrap();
        let _ = sup.converge_once().await;
        let actual = store.actual("n15").unwrap();
        assert_eq!(actual.status, "running", "{:?}", actual.last_error);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let id = sup.checkpoint_named("n15").await.expect("checkpoint");
        assert!(id >= 1);
        let metrics = kernel.metrics.snapshot();
        assert!(
            metrics.checkpoint_commits >= 1,
            "aligned commit must increment checkpoint_commits: {metrics:?}"
        );
        assert_ne!(
            metrics.checkpoint_bytes, 1,
            "must not record the old hardcoded 1-byte payload: {metrics:?}"
        );
        let recovered = CheckpointStore::open(&chk)
            .unwrap()
            .recover_committed()
            .unwrap()
            .expect("CURRENT");
        let encoded = recovered
            .encode_with_max_state_keys(kernel.budget().max_state_keys)
            .unwrap();
        assert_eq!(
            metrics.checkpoint_bytes,
            encoded.len() as u64,
            "checkpoint_bytes must be the spawn_blocking payload length"
        );
        sup.kill_named("n15").await.unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn n16_layout_from_physical_uses_filter_before_window() {
    use sparrow_expr::Expr;
    use sparrow_model::{
        DataType, Field, FieldId, OperatorId, PipelineId, RevisionId, Schema, SchemaId,
    };
    use sparrow_plan::{expr_fingerprint, physicalize, Catalog, PhysicalStage, PlanOptions, TransformStep};
    use sparrow_sql::bind_sql;

    let mut cat = Catalog::new();
    cat.insert(
        "sensors",
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap(),
    );
    let sql =
        "SELECT COUNT(*) AS n, device_id FROM sensors WHERE v > 1 GROUP BY device_id, COUNT_WINDOW(2)";
    let bound = bind_sql(sql, &cat, PipelineId::new(1), RevisionId::new(1)).unwrap();
    let mut plan = physicalize(&bound, &PlanOptions { fuse: true });
    let before = crate::supervisor::layout_from_physical(&plan).unwrap();
    assert_ne!(before.where_fingerprint, 0, "WHERE before window must be fingerprinted");

    let after = Expr::Column {
        name: "must_not_win".into(),
    };
    plan.stages.push(PhysicalStage::Transform {
        steps: vec![TransformStep::Filter {
            operator: OperatorId::new(99),
            predicate: after.clone(),
            input: plan
                .stages
                .iter()
                .find_map(|s| match s {
                    PhysicalStage::WindowAgg { output, .. } => Some(output.clone()),
                    _ => None,
                })
                .expect("window output"),
        }],
    });
    let layout = crate::supervisor::layout_from_physical(&plan).unwrap();
    assert_eq!(
        layout.where_fingerprint, before.where_fingerprint,
        "trailing Filter after the window must not change the layout fingerprint"
    );
    assert_ne!(layout.where_fingerprint, expr_fingerprint(&after));
}

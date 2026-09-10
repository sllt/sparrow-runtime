//! Supervisor / catalog regressions for the static review (R10/R24).

use std::sync::Arc;

use sparrow_model::ErrorCode;

use crate::spec::{PipelineSpec, SinkSpec, SourceSpec};
use crate::store::Store;
use crate::supervisor::{compact_kernel, request_start, request_start_at, Supervisor};

const STREAM: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"v","type":"int64","nullable":false}
]}"#;

fn tmp(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
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
    assert_eq!(loaded.spec.sql.as_deref(), Some("SELECT device_id FROM sensors"));
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
    };
    assert_eq!(spec.check_delivery().unwrap_err().code, ErrorCode::UnsupportedRestore);
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
    let spec = file_spec("/tmp/p01.ndjson", sql, "aligned", Some("/tmp/p01-chk"));
    crate::validate::validate_aligned_plan(&spec, &plan)
        .expect("aligned+WHERE must be honored (not stripped/rejected)");
    let kernel = compact_kernel().unwrap();
    let rows = [1, 2, 3, 4].into_iter().map(|v| Row {
        values: vec![Scalar::utf8("d1"), Scalar::Int64(v)],
    }).collect();
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
    use sparrow_model::{
        DataType, Field, FieldId, PipelineId, RevisionId, Schema, SchemaId,
    };
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
    let spec = file_spec("/tmp/p02.ndjson", sql, "aligned", Some("/tmp/p02-chk"));
    let err = crate::validate::validate_aligned_plan(&spec, &plan).unwrap_err();
    assert_eq!(err.code, ErrorCode::UnsupportedRestore);
    assert!(
        err.message.to_ascii_lowercase().contains("processing-time")
            || err.message.contains("PT"),
        "{}",
        err.message
    );
}

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
        assert!(!current.exists(), "CURRENT must not exist before checkpoint");
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

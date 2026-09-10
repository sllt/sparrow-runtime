//! R4/R5 production-path regressions: admission, checkpoint isolation, safe-mode.
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use sparrow_model::{ErrorCode, ResourceBudget};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture};

use crate::{host_kernel_with_max_jobs, request_start, request_stop, PipelineSpec, Store, Supervisor};

const STREAM: &str = r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}"#;
const SQL: &str = "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)";

struct Scratch(std::path::PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = sparrow_connectors::ensure_default_data_root().join(format!("r4-{}-{}",
            std::process::id(), NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, content: impl AsRef<[u8]>) -> std::path::PathBuf {
        let path = self.0.join("input.ndjson");
        std::fs::write(&path, content).unwrap();
        path
    }
}
impl Drop for Scratch {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn spec(path: &std::path::Path, recovery: &str) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&json!({
        "stream":"sensors", "sql": SQL,
        "source":{"kind":"file", "path":path, "file_contract":"append_only"},
        "sink":{"kind":"log"}, "recovery":recovery,
    })).unwrap()).unwrap()
}

#[test]
fn r5_safe_mode_survives_history_eviction_and_catalog_reopen() {
    let scratch = Scratch::new();
    let path = scratch.file(b"bad-json\n");
    let catalog = scratch.0.join("catalog.db");
    let kernel = Arc::new(host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open(&catalog).unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let mut pipeline = spec(&path, "restart_fresh");
        pipeline.fail_on_decode = true;
        store.put_pipeline("a", &pipeline, None).unwrap();
        store.put_pipeline("noise", &pipeline, None).unwrap();
        request_start(&store, "a", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), true, None).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            sup.converge_once().await.unwrap();
            if store.actual("a").unwrap().status == "failed" { break; }
            assert!(Instant::now() < deadline, "source must fail");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let failed = store.actual("a").unwrap();
        assert!(failed.restart_blocked);
        assert_eq!(failed.consecutive_failures, 1);
        assert!(failed.last_error.unwrap().contains("held: safe-mode"));
        let starts = kernel.metrics.snapshot().jobs_started;
        for _ in 0..100 { store.insert_attempt("noise", 1, "waiting", Some("capacity")).unwrap(); }
        assert!(store.last_attempt("a").unwrap().is_none());
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("a").unwrap().status, "failed");
        assert_eq!(kernel.metrics.snapshot().jobs_started, starts);
        drop(sup);
        drop(store);

        let store = Arc::new(Store::open(&catalog).unwrap());
        let sup = Supervisor::new(store.clone(), kernel.clone(), true, None).unwrap();
        sup.converge_once().await.unwrap();
        let held = store.actual("a").unwrap();
        assert_eq!(held.status, "stopped");
        assert!(held.restart_blocked);
        assert!(held.last_error.unwrap().contains("fail_on_decode"));
        assert_eq!(kernel.metrics.snapshot().jobs_started, starts);

        std::fs::write(&path, b"").unwrap();
        request_start(&store, "a", "test").unwrap();
        assert!(!store.actual("a").unwrap().restart_blocked);
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("a").unwrap().status, "running");
        assert_eq!(store.actual("a").unwrap().consecutive_failures, 0);
        assert_eq!(kernel.metrics.snapshot().jobs_started, starts + 1);
        sup.stop_all().await;
    });
}

#[test]
fn r4_host_capacity_waiting_and_configurable_seventeenth_job() {
    for capacity in [16, 32] {
        let scratch = Scratch::new();
        let path = scratch.file(b"");
        let kernel = Arc::new(host_kernel_with_max_jobs(capacity).unwrap());
        kernel.block_on(async {
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("sensors", STREAM).unwrap();
            for i in 0..17 {
                let name = format!("p{i:02}");
                store.put_pipeline(&name, &spec(&path, "restart_fresh"), None).unwrap();
                request_start(&store, &name, "test").unwrap();
            }
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            let actual = store.actual("p16").unwrap();
            assert_eq!(actual.consecutive_failures, 0);
            assert_eq!(kernel.admitted_jobs(), capacity.min(17));
            if capacity == 16 {
                assert_eq!(actual.status, "waiting");
                assert!(actual.last_error.unwrap().contains("capacity: 16/16 jobs admitted"));
                assert_eq!(store.last_attempt("p16").unwrap().unwrap().outcome, "waiting");
                sup.converge_once().await.unwrap();
                assert_eq!(store.actual("p16").unwrap().attempt_id, actual.attempt_id);
                // Stop while waiting must not remain visibly waiting forever.
                request_stop(&store, "p16", "test").unwrap();
                sup.converge_once().await.unwrap();
                assert_eq!(store.actual("p16").unwrap().status, "stopped");
                request_stop(&store, "p00", "test").unwrap();
                request_start(&store, "p16", "test").unwrap();
                sup.converge_once().await.unwrap();
            }
            assert_eq!(store.actual("p16").unwrap().status, "running");
            sup.stop_all().await;
            assert_eq!(kernel.admitted_jobs(), 0);
            assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        });
    }
    for invalid in ["", "0", "257", "-1", "abc", "999999999999999999999999"] {
        assert!(crate::parse_max_jobs(invalid).is_err());
    }
    for valid in ["1", "16", "32", "256"] { assert!(crate::parse_max_jobs(valid).is_ok()); }
    assert!(host_kernel_with_max_jobs(0).is_err());
}

#[test]
fn r4_checkpoint_bound_failure_keeps_job_running_and_retry_can_commit() {
    use std::io::Write;
    let scratch = Scratch::new();
    let row = format!("{{\"device_id\":\"{}\",\"v\":1}}\n", "x".repeat(128));
    let path = scratch.file(&row);
    let job = ResourceBudget { reservation_bytes: 512, retention_bytes: 8192, ..ResourceBudget::compact() };
    let kernel = Arc::new(Kernel::new_with_job_budget(KernelOptions {
        budget: ResourceBudget::compact(), mailbox: MailboxConfig { max_items: 8, max_bytes: 1024 },
        worker_threads: 2, rows_per_batch: 1,
    }, job).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        store.put_pipeline("small", &spec(&path, "aligned"), None).unwrap();
        request_start(&store, "small", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while kernel.metrics.snapshot().state_bytes == 0 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for _ in 0..2 {
            assert_eq!(sup.checkpoint_named("small").await.unwrap_err().code, ErrorCode::BoundExceeded);
            sup.converge_once().await.unwrap();
            let actual = store.actual("small").unwrap();
            assert_eq!(actual.status, "running");
            assert_eq!(actual.consecutive_failures, 0);
            assert_eq!(kernel.metrics.snapshot().jobs_failed, 0);
            assert_eq!(kernel.process_owner().usage().reservation_bytes, 0);
        }
        // Complete the count window so its next (empty) freeze fits the quota.
        let mut append = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        append.write_all(row.as_bytes()).unwrap();
        append.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while kernel.metrics.snapshot().emitted_rows == 0 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(sup.checkpoint_named("small").await.unwrap(), 3);
        let metrics = kernel.metrics.snapshot();
        assert_eq!(metrics.checkpoint_aborts, 2);
        assert_eq!(metrics.checkpoint_commits, 1);
        assert_eq!(metrics.jobs_failed, 0);
        sup.stop_all().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

#[test]
fn r4_oversize_decode_policy_on_both_supervisor_paths() {
    for recovery in ["aligned", "restart_fresh"] {
        for fail in [false, true] {
            let scratch = Scratch::new();
            let path = scratch.file(format!("{}\n{{\"device_id\":\"ok\",\"v\":1}}\n{{\"device_id\":\"ok\",\"v\":2}}\n", "x".repeat(70 * 1024)));
            let kernel = Arc::new(host_kernel_with_max_jobs(1).unwrap());
            kernel.block_on(async {
                let store = Arc::new(Store::open_memory().unwrap());
                store.put_stream("sensors", STREAM).unwrap();
                let mut pipeline = spec(&path, recovery);
                pipeline.fail_on_decode = fail;
                store.put_pipeline("input", &pipeline, None).unwrap();
                request_start(&store, "input", "test").unwrap();
                let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
                sup.converge_once().await.unwrap();
                let deadline = Instant::now() + Duration::from_secs(3);
                if fail {
                    loop {
                        sup.converge_once().await.unwrap();
                        if store.actual("input").unwrap().status == "failed" { break; }
                        assert!(Instant::now() < deadline);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    assert_eq!(store.actual("input").unwrap().consecutive_failures, 1);
                    assert!(store.actual("input").unwrap().last_error.unwrap().contains("fail_on_decode"));
                } else {
                    while kernel.metrics.snapshot().emitted_rows == 0 {
                        assert!(Instant::now() < deadline);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    assert_eq!(sup.io_snapshot().await.decode_errors, 1);
                    assert_eq!(store.actual("input").unwrap().status, "running");
                    assert_eq!(kernel.metrics.snapshot().ingested_rows, 2);
                    if recovery == "aligned" { sup.checkpoint_named("input").await.unwrap(); }
                }
                sup.stop_all().await;
            });
        }
    }
}

#[test]
fn r4_unfused_plan_cannot_exceed_job_queue_quota() {
    use sparrow_plan::{bind_graph, physicalize, GraphSpec, PlanOptions};
    let store = Store::open_memory().unwrap();
    store.put_stream("sensors", STREAM).unwrap();
    let mut nodes = vec![json!({"id":1,"kind":"memory_source","table":"sensors","out":[2]})];
    for id in 2..=9 { nodes.push(json!({"id":id,"kind":"filter","predicate":{"k":"lit","value":{"t":"bool","v":true}},"out":[id+1]})); }
    nodes.push(json!({"id":10,"kind":"capture_sink","name":"out"}));
    let graph = GraphSpec::from_json(&json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":nodes}).to_string()).unwrap();
    let bound = bind_graph(&graph, &crate::binder_catalog(&store).unwrap()).unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: false });
    assert_eq!(plan.mailbox_count(), 9);
    let kernel = host_kernel_with_max_jobs(16).unwrap();
    let err = kernel.submit(JobRequest::new(plan, Vec::new(), SharedCapture::disabled())).err().unwrap();
    assert_eq!(err.code, ErrorCode::ResourceExhausted);
    assert!(err.message.contains("job queue quota"));
    assert!(!err.retryable);
    assert_eq!(kernel.admitted_jobs(), 0);
    assert_eq!(kernel.queue_reserved(), 0);
    let fused = physicalize(&bound, &PlanOptions { fuse: true });
    kernel.run(JobRequest::new(fused, Vec::new(), SharedCapture::disabled())).unwrap();
}

//! HTTP API regressions: file/aligned (R10), desired revision (R23), job failed (R24).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sparrow_control::{compact_kernel, Store};
use sparrow_server::{boot, router, wait_status, AppState};
use tower::ServiceExt;

const TOKEN: &str = "review-api-token";
const STREAM: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"v","type":"int64","nullable":false}
]}"#;

#[test]
fn r9_status_schema_cache_invalidation_and_shared_histogram_contract() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let supervisor =
            sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState {
            store: store.clone(),
            supervisor,
            token: Arc::new(TOKEN.into()),
            safe_mode: false,
        };
        let path = tmp("r9-cache.ndjson");
        std::fs::write(&path, b"").unwrap();
        let mut spec = restart_file_spec(&path);
        spec.sql = Some(
            "SELECT device_id, SUM(v) AS s FROM sensors GROUP BY device_id, COUNT_WINDOW(2)".into(),
        );
        store.put_pipeline("cached", &spec, None).unwrap();
        let (_, first) = call(&state, auth_get("/v1/pipelines/cached/status")).await;
        assert_eq!(first["effective"]["aligned_eligible"], true);
        store
            .put_stream(
                "sensors",
                r#"{"fields":[{"name":"wrong","type":"int64","nullable":false}]}"#,
            )
            .unwrap();
        let (code, invalid) = call(&state, auth_get("/v1/pipelines/cached/status")).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(invalid["revision"], 1);
        assert!(invalid["effective"]["aligned_eligible"].is_null());
        store.put_stream("sensors", STREAM).unwrap();
        sparrow_control::request_start(&store, "cached", "test").unwrap();
        state.supervisor.converge_once().await.unwrap();
        let (_, live) = call(&state, auth_get("/v1/pipelines/cached/status")).await;
        assert_eq!(live["effective"]["aligned_eligible"], true);
        assert_eq!(
            live["histogram_contract"]["bucket_upper_us"]
                .as_array()
                .unwrap()
                .len(),
            32
        );
        assert_eq!(live["histogram_contract"]["bucket_upper_us"][0], 1);
        assert!(live["histogram_contract"]["bucket_upper_us"][31].is_null());
        assert!(live["observation"]["latency"]["window"]
            .get("bucket_upper_us")
            .is_none());
        assert_eq!(
            live["observation"]["latency"]["window"]["buckets"]
                .as_array()
                .unwrap()
                .len(),
            32
        );
        assert_eq!(
            live["observation"]["runtime_progress"]["snapshot_consistency"],
            "independently_sampled_monotonic_counters"
        );
        for edge in live["mailboxes"]["edges"].as_array().unwrap() {
            assert_eq!(edge["accounting_valid"], true);
            assert_eq!(edge["accounting_errors_total"], 0);
        }
        let (_, metrics) = call(&state, auth_get("/v1/metrics")).await;
        assert_eq!(metrics["histogram_contract"], live["histogram_contract"]);
        assert_eq!(
            serde_json::to_string(&metrics)
                .unwrap()
                .matches("bucket_upper_us")
                .count(),
            1
        );
        state.supervisor.stop_all().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn obs_status_and_metrics_report_runtime_scope_not_unknown_connector_queues() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let supervisor =
            sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState {
            store: store.clone(),
            supervisor,
            token: Arc::new(TOKEN.into()),
            safe_mode: false,
        };
        let path = tmp("obs-status.ndjson");
        std::fs::write(&path, b"").unwrap();
        store
            .put_pipeline("observe", &restart_file_spec(&path), None)
            .unwrap();
        let (_, before) = call(&state, auth_get("/v1/pipelines/observe/status")).await;
        assert_eq!(before["mailboxes"]["available"], false);
        assert_eq!(before["mailboxes"]["reason"], "no_active_attempt");
        sparrow_control::request_start(&store, "observe", "test").unwrap();
        state.supervisor.converge_once().await.unwrap();
        let live = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (_, status) = call(&state, auth_get("/v1/pipelines/observe/status")).await;
                if status["mailboxes"]["available"] == true {
                    break status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let view = &live["mailboxes"];
        let observation = &live["observation"];
        assert_eq!(observation["available"], true);
        assert_eq!(observation["running_revision"], 1);
        assert_eq!(
            observation["runtime_attempt_id"],
            view["runtime_attempt_id"]
        );
        assert_eq!(observation["source_inbox"]["available"], true);
        assert_eq!(observation["sink_outbox"]["available"], true);
        assert!(
            observation["source_pending_accounted_bytes"].is_null(),
            "File poll scratch is not a measured MQTT pending row"
        );
        assert_eq!(observation["source"]["silence_is_failure"], false);
        assert!(observation["latency"]["transform"]["p99_upper_us"].is_null());
        assert_eq!(
            observation["latency_contract"]["business_ack_available"],
            false
        );
        assert_eq!(view["scope"], "runtime_mailboxes");
        assert_eq!(view["clock"], "process_monotonic");
        assert_eq!(view["age_origin"], "mailbox_enqueue");
        assert_eq!(view["running_revision"], 1);
        assert_eq!(view["coverage"]["source_inbox"], false);
        assert_eq!(view["coverage"]["sink_outbox"], false);
        assert_eq!(view["coverage"]["end_to_end_age"], false);
        for edge in view["edges"].as_array().unwrap() {
            assert_eq!(edge["queued"]["items"], 0);
            assert!(edge["oldest_queued_age_us"].is_null());
            assert!(edge["oldest_queued_data_age_us"].is_null());
            assert!(edge["metadata_bytes"].as_u64().unwrap() > 0);
        }
        // Latest stored revision is not necessarily the running revision.
        store
            .put_pipeline("observe", &restart_file_spec(&path), Some("rev-1"))
            .unwrap();
        let (_, newer) = call(&state, auth_get("/v1/pipelines/observe/status")).await;
        assert_eq!(newer["revision"], 2);
        assert_eq!(newer["mailboxes"]["running_revision"], 1);
        let (_, metrics) = call(&state, auth_get("/v1/metrics")).await;
        assert_eq!(
            metrics["queue_metrics_available"], false,
            "legacy whole-path gauges are still unavailable"
        );
        assert_eq!(metrics["mailboxes"]["scope"], "runtime_mailboxes");
        assert_eq!(metrics["mailboxes"]["jobs"].as_array().unwrap().len(), 1);
        let first_attempt = view["runtime_attempt_id"].clone();
        state.supervisor.stop_all().await;
        let (_, stopped) = call(&state, auth_get("/v1/pipelines/observe/status")).await;
        assert_eq!(stopped["mailboxes"]["reason"], "no_active_attempt");
        assert_eq!(stopped["observation"]["reason"], "no_active_attempt");
        let (_, metrics) = call(&state, auth_get("/v1/metrics")).await;
        assert!(metrics["mailboxes"]["jobs"].as_array().unwrap().is_empty());
        assert!(metrics["observations"]["jobs"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        sparrow_control::request_start(&store, "observe", "test").unwrap();
        state.supervisor.converge_once().await.unwrap();
        let restarted = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (_, status) = call(&state, auth_get("/v1/pipelines/observe/status")).await;
                if status["mailboxes"]["available"] == true {
                    break status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_ne!(restarted["mailboxes"]["runtime_attempt_id"], first_attempt);
        assert_eq!(restarted["mailboxes"]["running_revision"], 2);
        state.supervisor.stop_all().await;
        std::fs::remove_file(path).unwrap();
    });
}

#[cfg(feature = "demo-io")]
#[test]
fn obs_api_separates_slow_and_failed_sink_from_idle_healthy_sibling() {
    use std::io::Write;
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(2).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let supervisor =
            sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState {
            store: store.clone(),
            supervisor,
            token: Arc::new(TOKEN.into()),
            safe_mode: false,
        };
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        http.set_delay_ms(50);
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let slow = tmp("obs-slow.ndjson");
        let fast = tmp("obs-fast.ndjson");
        std::fs::write(&slow, b"").unwrap();
        std::fs::write(&fast, b"").unwrap();
        let mut spec = restart_file_spec(&slow);
        spec.source.inbox_capacity = 2;
        spec.sink.kind = "http".into();
        spec.sink.url = Some(http.url());
        spec.sink.outbox_capacity = 2;
        store.put_pipeline("slow", &spec, None).unwrap();
        store
            .put_pipeline("fast", &restart_file_spec(&fast), None)
            .unwrap();
        for name in ["slow", "fast"] {
            sparrow_control::request_start(&store, name, "test").unwrap();
        }
        state.supervisor.converge_once().await.unwrap();
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&slow)
                .unwrap();
            for _ in 0..512 {
                writeln!(f, "{{\"device_id\":\"d\",\"v\":1}}").unwrap();
            }
        }
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&fast)
                .unwrap();
            writeln!(f, "{{\"device_id\":\"fast\",\"v\":1}}").unwrap();
        }
        let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (_, a) = call(&state, auth_get("/v1/pipelines/slow/status")).await;
                let (_, b) = call(&state, auth_get("/v1/pipelines/fast/status")).await;
                if a["observation"]["sink_outbox"]["queued_items"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0
                    && a["observation"]["delivery"]["encoded_credit_bytes"]
                        .as_u64()
                        .unwrap_or(0)
                        > 0
                    && a["observation"]["delivery"]["active_http_requests"]
                        .as_u64()
                        .unwrap_or(0)
                        > 0
                    && b["observation"]["delivery"]["completed_rows_total"] == 1
                {
                    break (a, b);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_ne!(
            a["observation"]["runtime_attempt_id"],
            b["observation"]["runtime_attempt_id"]
        );
        assert_eq!(b["observation"]["sink_outbox"]["queued_items"], 0);
        assert_eq!(b["observation"]["source"]["state"], "waiting_for_append");
        assert!(
            a["observation"]["delivery"]["encoded_credit_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(a["observation"]["sink_outbox"]["oldest_queued_age_us"].is_u64());
        http.set_status(400);
        http.set_delay_ms(0);
        let failed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (_, value) = call(&state, auth_get("/v1/pipelines/slow/status")).await;
                // Endpoint and delivery components are deliberately not an atomic
                // cross-pipeline snapshot: a response may fail between the reads.
                if value["observation"]["delivery"]["failed_or_cancelled_batches_total"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0
                    && value["observation"]["sink"]["state"] == "failed"
                    && value["observation"]["sink"]["reason"] == "http_4xx"
                {
                    break value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(failed["observation"]["sink"]["state"], "failed");
        assert_eq!(failed["observation"]["sink"]["reason"], "http_4xx");
        state.supervisor.stop_all().await;
        http.stop().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        std::fs::remove_file(slow).unwrap();
        std::fs::remove_file(fast).unwrap();
    });
}

#[test]
fn base01_api_and_kernel_agree_on_aligned_plan_eligibility() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let supervisor = sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState { store: store.clone(), supervisor, token: Arc::new(TOKEN.into()), safe_mode: false };
        let path = tmp("base01.ndjson");
        let checkpoint = tmp("base01-checkpoint");
        std::fs::write(&path, b"").unwrap();
        let mut base = restart_file_spec(&path);
        base.recovery = "aligned".into();
        base.checkpoint_dir = Some(checkpoint.to_string_lossy().into());
        let mut single = base.clone();
        single.sql = Some("SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)".into());
        let mut pt = base.clone();
        pt.sql = Some("SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, TUMBLE(PROCESSING_TIME, INTERVAL '1' SECOND)".into());
        let mut multi = base.clone();
        multi.sql = None;
        multi.graph = Some(serde_json::from_value(json!({
            "version":1, "pipeline_id":1, "revision_id":1, "nodes":[
                {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
                {"id":2,"kind":"window_agg","keys":["device_id"],"window":{"kind":"count","size":2},"aggs":[{"fn":"count","alias":"n"}],"out":[3]},
                {"id":3,"kind":"window_agg","keys":["device_id"],"window":{"kind":"count","size":2},"aggs":[{"fn":"count","alias":"m"}],"out":[4]},
                {"id":4,"kind":"capture_sink","name":"out"}
            ]
        })).unwrap());
        for (name, spec, eligible) in [("none", base, false), ("pt", pt, false), ("multi", multi, false), ("single", single, true)] {
            let plan = sparrow_control::bind_plan(&spec, &sparrow_control::binder_catalog(&store).unwrap(), name, 1).unwrap();
            assert_eq!(sparrow_control::validate::validate_aligned_plan(&spec, &plan).is_ok(), eligible);
            let serialized = serde_json::to_string(&spec).unwrap();
            for endpoint in ["/v1/validate", "/v1/explain"] {
                let (status, reply) = call(&state, auth_post(endpoint, &serialized)).await;
                assert_eq!(status.is_success(), eligible, "{name} {endpoint}: {reply}");
                if eligible { assert_eq!(reply["effective"]["aligned_eligible"], true); }
            }
            store.put_pipeline(name, &spec, None).unwrap();
            let (_, reply) = call(&state, auth_get(&format!("/v1/pipelines/{name}/status"))).await;
            assert_eq!(reply["effective"]["aligned_eligible"], eligible);
            assert_eq!(reply["effective"]["scope"], "stored_latest_revision");
            if !eligible {
                let request = sparrow_runtime::JobRequest::new(plan, vec![], sparrow_runtime::SharedCapture::new())
                    .with_aligned(sparrow_runtime::barrier::AlignedJob {
                        restore: None, acks: Default::default(), outbox: Arc::new(sparrow_model::InflightCounter::new()),
                    });
                assert!(kernel.submit(request).is_err(), "embedded Kernel bypass: {name}");
                assert_eq!(kernel.admitted_jobs(), 0);
                assert_eq!(kernel.queue_reserved(), 0);
                assert_eq!(kernel.metrics.snapshot().jobs_started, 0);
                // Persisted invalid specs cannot bypass the same check via start.
                sparrow_control::request_start(&store, name, "test").unwrap();
                let _ = state.supervisor.converge_once().await;
                assert_ne!(store.actual(name).unwrap().status, "running");
                assert_eq!(kernel.admitted_jobs(), 0);
            }
        }
        state.supervisor.stop_all().await;
        assert!(!checkpoint.join("CURRENT").exists());
        std::fs::remove_file(path).unwrap();
        let _ = std::fs::remove_dir_all(checkpoint);
    });
}

#[test]
fn r6_repeated_start_running_is_idempotent_but_new_revision_still_starts() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(2).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        let supervisor =
            sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState {
            store: store.clone(),
            supervisor,
            token: Arc::new(TOKEN.into()),
            safe_mode: false,
        };
        let path = tmp("r6-repeat-start.ndjson");
        std::fs::write(&path, b"").unwrap();
        store.put_stream("sensors", STREAM).unwrap();
        let spec = restart_file_spec(&path);
        store.put_pipeline("idem", &spec, None).unwrap();
        let (status, _) = call(&state, auth_post("/v1/pipelines/idem/start", "{}")).await;
        assert_eq!(status, StatusCode::OK);
        state.supervisor.converge_once().await.unwrap();
        let (_, before) = call(&state, auth_get("/v1/pipelines/idem/status")).await;
        assert_eq!(before["actual"]["status"], "running");
        assert_eq!(before["actual"]["revision"], 1);
        let history_id = store.last_attempt("idem").unwrap().unwrap().id;
        for _ in 0..3 {
            let (status, reply) = call(&state, auth_post("/v1/pipelines/idem/start", "{}")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                reply["actual"], before["actual"],
                "even the immediate reply must stay running"
            );
            state.supervisor.converge_once().await.unwrap();
            let (_, after) = call(&state, auth_get("/v1/pipelines/idem/status")).await;
            assert_eq!(after["actual"], before["actual"]);
            assert_eq!(kernel.metrics.snapshot().jobs_started, 1);
            assert_eq!(kernel.metrics.snapshot().jobs_stopped, 0);
            assert_eq!(store.last_attempt("idem").unwrap().unwrap().id, history_id);
        }
        store.put_pipeline("idem", &spec, Some("rev-1")).unwrap();
        let (status, reply) = call(
            &state,
            auth_post("/v1/pipelines/idem/start", r#"{"revision":2}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reply["actual"]["status"], "stopped");
        assert!(reply["actual"]["revision"].is_null());
        assert_eq!(reply["desired"]["revision"], 2);
        state.supervisor.converge_once().await.unwrap();
        let (_, after) = call(&state, auth_get("/v1/pipelines/idem/status")).await;
        assert_eq!(after["actual"]["status"], "running");
        assert_eq!(after["actual"]["revision"], 2);
        assert_eq!(kernel.metrics.snapshot().jobs_started, 2);
        assert_eq!(kernel.admitted_jobs(), 1);
        state.supervisor.stop_all().await;
        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn r6_status_exposes_failure_latch_and_retained_counter() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let path = tmp("r6-status-fields.ndjson");
        std::fs::write(&path, b"").unwrap();
        for safe_mode in [false, true] {
            let store = Arc::new(Store::open_memory().unwrap());
            let supervisor =
                sparrow_control::Supervisor::new(store.clone(), kernel.clone(), safe_mode, None)
                    .unwrap();
            let state = AppState {
                store: store.clone(),
                supervisor,
                token: Arc::new(TOKEN.into()),
                safe_mode,
            };
            store.put_stream("sensors", STREAM).unwrap();
            store
                .put_pipeline("status", &restart_file_spec(&path), None)
                .unwrap();
            store
                .set_actual("status", "failed", Some(1), 1, Some("test fault"))
                .unwrap();
            let (status, failed) = call(&state, auth_get("/v1/pipelines/status/status")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(failed["safe_mode"], safe_mode);
            assert_eq!(failed["actual"]["consecutive_failures"], 1);
            assert_eq!(failed["actual"]["restart_blocked"], true);
            assert_eq!(
                failed["actual"]["last_error"], "test fault",
                "fields are visible before converge writes a held prefix"
            );
            store
                .set_actual("status", "running", Some(1), 2, None)
                .unwrap();
            let (status, running) = call(&state, auth_get("/v1/pipelines/status/status")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(running["actual"]["status"], "running");
            assert_eq!(running["actual"]["consecutive_failures"], 1);
            assert_eq!(running["actual"]["restart_blocked"], false);
        }
        std::fs::remove_file(path).unwrap();
    });
}

fn restart_file_spec(path: &std::path::Path) -> sparrow_control::PipelineSpec {
    sparrow_control::PipelineSpec::from_json(
        &serde_json::to_vec(&json!({
            "stream":"sensors", "sql":"SELECT device_id FROM sensors",
            "source":{"kind":"file","path":path,"file_contract":"append_only"},
            "sink":{"kind":"log"}, "recovery":"restart_fresh"
        }))
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn r4_capacity_waiting_is_visible_in_http_status() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        let supervisor =
            sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState {
            store: store.clone(),
            supervisor,
            token: Arc::new(TOKEN.into()),
            safe_mode: false,
        };
        let path = tmp("r4-capacity.ndjson");
        std::fs::write(&path, b"").unwrap();
        store.put_stream("sensors", STREAM).unwrap();
        for name in ["a", "b"] {
            let spec = sparrow_control::PipelineSpec::from_json(
                &serde_json::to_vec(&json!({
                    "stream":"sensors", "sql":"SELECT device_id FROM sensors",
                    "source":{"kind":"file","path":path,"file_contract":"append_only"},
                    "sink":{"kind":"log"}
                }))
                .unwrap(),
            )
            .unwrap();
            store.put_pipeline(name, &spec, None).unwrap();
            sparrow_control::request_start(&store, name, "test").unwrap();
        }
        state.supervisor.converge_once().await.unwrap();
        let (status, body) = call(&state, auth_get("/v1/pipelines/b/status")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["desired"]["status"], "running");
        assert_eq!(body["actual"]["status"], "waiting");
        assert!(body["actual"]["last_error"]
            .as_str()
            .unwrap()
            .contains("capacity: 1/1 jobs admitted"));
        state.supervisor.stop_all().await;
        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn r3_graph_endpoints_use_registered_stream_catalog() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (status, _) = call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        assert_eq!(status, StatusCode::CREATED);
        let graph = json!({"version":1, "pipeline_id":1, "revision_id":1, "nodes":[
            {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
            {"id":2,"kind":"capture_sink","name":"out"}
        ]})
        .to_string();
        for endpoint in ["/v1/graphs/validate", "/v1/graphs/explain"] {
            let (status, body) = call(&state, auth_post(endpoint, &graph)).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["accepted"], true);
        }
    });
}

fn tmp(name: &str) -> std::path::PathBuf {
    sparrow_connectors::ensure_default_data_root().join(format!(
        "sparrow-api-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

async fn call(state: &AppState, req: Request<Body>) -> (StatusCode, Value) {
    let res = router(state.clone()).oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(json!({"raw": String::from_utf8_lossy(&bytes)}))
    };
    (status, json)
}

fn auth_put(uri: &str, body: impl Into<String>) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body.into()))
        .unwrap()
}

fn auth_post(uri: &str, body: impl AsRef<str>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body.as_ref().to_string()))
        .unwrap()
}

async fn setup() -> AppState {
    let store = Arc::new(Store::open_memory().unwrap());
    let kernel = Arc::new(compact_kernel().unwrap());
    let (state, _) = boot(store, kernel, TOKEN.into(), false, false)
        .await
        .unwrap();
    state
}

fn file_window_spec(path: &str, chk: &str) -> Value {
    json!({
        "version": 1,
        "stream": "sensors",
        "sql": "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
        "source": { "kind": "file", "path": path },
        "sink": { "kind": "log" },
        "delivery": "live_best_effort",
        "recovery": "aligned",
        "checkpoint_dir": chk
    })
}

#[test]
fn r10_file_aligned_http_create_start_checkpoint_kill_restore() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("events.ndjson");
        let chk = tmp("chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        let (st, _) = call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        assert_eq!(st, StatusCode::CREATED);
        let spec = file_window_spec(&path.to_string_lossy(), &chk.to_string_lossy());
        let (st, body) = call(&state, auth_put("/v1/pipelines/filehot", spec.to_string())).await;
        assert_eq!(st, StatusCode::CREATED, "{body}");
        let (st, body) = call(&state, auth_post("/v1/pipelines/filehot/start", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "filehot", "running", Duration::from_secs(5))
            .await
            .expect("file+aligned must run via HTTP API");
        tokio::time::sleep(Duration::from_millis(80)).await;
        let (st, body) = call(&state, auth_post("/v1/pipelines/filehot/checkpoint", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert!(body["checkpoint_id"].as_u64().unwrap() >= 1);
        assert_eq!(body["exactly_once"], false);
        let (st, _) = call(&state, auth_post("/v1/pipelines/filehot/kill", "{}")).await;
        assert_eq!(st, StatusCode::OK);
        let (st, body) = call(&state, auth_post("/v1/pipelines/filehot/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "filehot", "running", Duration::from_secs(5))
            .await
            .expect("restore must start from the committed checkpoint");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn r23_start_desired_revision_not_latest() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("rev.ndjson");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec1 = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT device_id FROM sensors",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        });
        let (st, created) = call(&state, auth_put("/v1/pipelines/rev", spec1.to_string())).await;
        assert_eq!(st, StatusCode::CREATED, "{created}");
        let spec2 = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT v FROM sensors",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        });
        let (st, _) = call(
            &state,
            Request::builder()
                .method("PUT")
                .uri("/v1/pipelines/rev")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .header("if-match", "rev-1")
                .body(Body::from(spec2.to_string()))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let (st, body) = call(
            &state,
            auth_post("/v1/pipelines/rev/start", r#"{"revision":1}"#),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "rev", "running", Duration::from_secs(5))
            .await
            .unwrap();
        let actual = state.store.actual("rev").unwrap();
        assert_eq!(
            actual.revision,
            Some(1),
            "must run desired revision 1, not latest 2"
        );
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn r24_aligned_without_window_is_failed_not_stuck_running() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("nowin.ndjson");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT device_id FROM sensors",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "aligned"
        });
        let (st, body) = call(&state, auth_put("/v1/pipelines/nowin", spec.to_string())).await;
        assert!(
            st.is_client_error(),
            "aligned without a window must fail closed at put/validate, got {st} {body}"
        );
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("window"),
            "{body}"
        );
        let _ = std::fs::remove_file(&path);
    });
}

fn auth_get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

#[test]
fn r12_window_size_change_rejects_restore_via_api() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("r12.ndjson");
        let chk = tmp("r12chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "aligned",
            "checkpoint_dir": chk.to_string_lossy()
        });
        call(&state, auth_put("/v1/pipelines/r12", spec.to_string())).await;
        call(&state, auth_post("/v1/pipelines/r12/start", "{}")).await;
        wait_status(&state, "r12", "running", Duration::from_secs(5)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        call(&state, auth_post("/v1/pipelines/r12/checkpoint", "{}")).await;
        call(&state, auth_post("/v1/pipelines/r12/kill", "{}")).await;
        let spec2 = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(8)",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "aligned",
            "checkpoint_dir": chk.to_string_lossy()
        });
        call(
            &state,
            Request::builder()
                .method("PUT")
                .uri("/v1/pipelines/r12")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .header("if-match", "rev-1")
                .body(Body::from(spec2.to_string()))
                .unwrap(),
        )
        .await;
        let (st, body) = call(&state, auth_post("/v1/pipelines/r12/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "r12", "failed", Duration::from_secs(6))
            .await
            .expect("window size change must reject snapshot reuse");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn r15_oversize_manifest_restore_fails_via_api() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("r15.ndjson");
        let chk = tmp("r15chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec = file_window_spec(&path.to_string_lossy(), &chk.to_string_lossy());
        call(&state, auth_put("/v1/pipelines/r15", spec.to_string())).await;
        call(&state, auth_post("/v1/pipelines/r15/start", "{}")).await;
        wait_status(&state, "r15", "running", Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        call(&state, auth_post("/v1/pipelines/r15/checkpoint", "{}")).await;
        call(&state, auth_post("/v1/pipelines/r15/kill", "{}")).await;
        let current = std::fs::read_to_string(chk.join("CURRENT")).unwrap();
        let gen = current.trim();
        let manifest = chk.join(gen).join("MANIFEST");
        assert!(manifest.exists(), "expected {manifest:?}");
        std::fs::write(&manifest, vec![b'X'; 300 * 1024]).unwrap();
        let (st, body) = call(&state, auth_post("/v1/pipelines/r15/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "r15", "failed", Duration::from_secs(6))
            .await
            .expect("oversize MANIFEST must reject restore");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn r28_same_prefix_different_suffix_rejects_restore() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("r28.ndjson");
        let chk = tmp("r28chk");
        std::fs::create_dir_all(&chk).unwrap();
        let mut body = String::new();
        for i in 0..400 {
            body.push_str(&format!("{{\"device_id\":\"d1\",\"v\":{i}}}\n"));
        }
        std::fs::write(&path, body.as_bytes()).unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec = file_window_spec(&path.to_string_lossy(), &chk.to_string_lossy());
        call(&state, auth_put("/v1/pipelines/r28", spec.to_string())).await;
        call(&state, auth_post("/v1/pipelines/r28/start", "{}")).await;
        wait_status(&state, "r28", "running", Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        call(&state, auth_post("/v1/pipelines/r28/checkpoint", "{}")).await;
        call(&state, auth_post("/v1/pipelines/r28/kill", "{}")).await;
        let mut data = std::fs::read(&path).unwrap();
        *data.last_mut().unwrap() = b'Z';
        std::fs::write(&path, data).unwrap();
        let (st, body) = call(&state, auth_post("/v1/pipelines/r28/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "r28", "failed", Duration::from_secs(6))
            .await
            .expect("same-prefix different-suffix file must reject restore");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[cfg(feature = "demo-io")]
const LIVE_STREAM: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true},
  {"name":"humidity","type":"float64","nullable":true},
  {"name":"ts","type":"timestamp_micros_utc","nullable":false},
  {"name":"payload","type":"dynamic","nullable":true}
]}"#;

#[test]
fn r11_checkpoint_via_api_flushes_then_commits() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("r11.ndjson");
        let chk = tmp("r11chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec = file_window_spec(&path.to_string_lossy(), &chk.to_string_lossy());
        call(&state, auth_put("/v1/pipelines/r11", spec.to_string())).await;
        call(&state, auth_post("/v1/pipelines/r11/start", "{}")).await;
        wait_status(&state, "r11", "running", Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let (st, body) = call(&state, auth_post("/v1/pipelines/r11/checkpoint", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert_eq!(body["exactly_once"], false);
        assert!(body["checkpoint_id"].as_u64().unwrap() >= 1);
        assert!(
            chk.join("CURRENT").exists(),
            "flush-then-commit must publish CURRENT"
        );
        let (_, metrics) = call(&state, auth_get("/v1/metrics")).await;
        assert!(
            metrics["checkpoint_commits"].as_u64().unwrap_or(0) >= 1,
            "API checkpoint must increment commits: {metrics}"
        );
        call(&state, auth_post("/v1/pipelines/r11/kill", "{}")).await;
        let (st, body) = call(&state, auth_post("/v1/pipelines/r11/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "r11", "running", Duration::from_secs(5))
            .await
            .expect("restore after flushed checkpoint must run");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn r12_where_change_rejects_restore_via_api() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("r12w.ndjson");
        let chk = tmp("r12wchk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        let spec = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "aligned",
            "checkpoint_dir": chk.to_string_lossy()
        });
        call(&state, auth_put("/v1/pipelines/r12w", spec.to_string())).await;
        call(&state, auth_post("/v1/pipelines/r12w/start", "{}")).await;
        wait_status(&state, "r12w", "running", Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        call(&state, auth_post("/v1/pipelines/r12w/checkpoint", "{}")).await;
        call(&state, auth_post("/v1/pipelines/r12w/kill", "{}")).await;
        let spec2 = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT COUNT(*) AS n, device_id FROM sensors WHERE v > 0 GROUP BY device_id, COUNT_WINDOW(2)",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "aligned",
            "checkpoint_dir": chk.to_string_lossy()
        });
        call(
            &state,
            Request::builder()
                .method("PUT")
                .uri("/v1/pipelines/r12w")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .header("if-match", "rev-1")
                .body(Body::from(spec2.to_string()))
                .unwrap(),
        )
        .await;
        let (st, body) = call(&state, auth_post("/v1/pipelines/r12w/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "r12w", "failed", Duration::from_secs(6))
            .await
            .expect("WHERE change must refuse snapshot reuse");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
#[cfg(feature = "demo-io")]
fn r25_mqtt_http_ingest_moves_metrics() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        let job_kernel = Arc::new(compact_kernel().unwrap());
        let (state, _) = boot(store, job_kernel, TOKEN.into(), false, true)
            .await
            .unwrap();
        let (st, _) = call(&state, auth_put("/v1/streams/sensors", LIVE_STREAM)).await;
        assert_eq!(st, StatusCode::CREATED);
        let spec = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT device_id, temperature, ts FROM sensors WHERE temperature > 25",
            "source": { "kind": "mqtt", "use_demo_io": true, "topic": "sensors/json", "client_id": "r25" },
            "sink": { "kind": "http", "use_demo_io": true },
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        });
        call(&state, auth_put("/v1/pipelines/r25", spec.to_string())).await;
        let (st, body) = call(&state, auth_post("/v1/pipelines/r25/start", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "r25", "running", Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let (st, pubd) = call(&state, auth_post("/v1/demo/publish-fixture", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{pubd}");
        let start = std::time::Instant::now();
        let body = loop {
            let (_, m) = call(&state, auth_get("/v1/metrics")).await;
            let ingested = m["ingested_rows"].as_u64().unwrap_or(0);
            let emitted = m["emitted_rows"].as_u64().unwrap_or(0);
            if ingested > 0 || emitted > 0 {
                break m;
            }
            if start.elapsed() > Duration::from_secs(6) {
                panic!("MQTT/HTTP ingest must move counters: {m}");
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        };
        assert!(
            body["ingested_rows"].as_u64().unwrap_or(0) > 0
                || body["emitted_rows"].as_u64().unwrap_or(0) > 0,
            "{body}"
        );
        let _ = call(&state, auth_post("/v1/pipelines/r25/stop", "{}")).await;
        assert_eq!(body["queue_metrics_available"], false);
        assert_eq!(body["io_scope"], "running_attempts");
        assert!(body["io"]["mqtt_reconnects"].is_u64());
        assert!(body["io"]["mqtt_backpressure_waits"].is_u64());
        assert!(body["io"]["mqtt_backpressure_recovered"].is_u64());
        for name in ["mqtt_inbox_items", "mqtt_inbox_bytes", "mqtt_inbox_peak_bytes", "mqtt_pending_bytes",
            "mqtt_dropped_budget", "mqtt_dropped_oversize", "mqtt_quickack_calls", "mqtt_quickack_errors", "http_acked_batches"] {
            assert!(body["io"][name].is_u64(), "missing {name}");
        }
        assert!(body["io"]["http_retries"].is_u64());
    });
}

#[cfg(feature = "demo-io")]
#[test]
fn p0_3_restore_after_prior_start_restores_checkpoint() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let path = tmp("p03.ndjson");
        let chk = tmp("p03-chk");
        std::fs::create_dir_all(&chk).unwrap();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n{\"device_id\":\"d1\",\"v\":3}\n",
        )
        .unwrap();
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        call(
            &state,
            auth_put(
                "/v1/allowlist",
                json!({"host":"127.0.0.1","port": http.port()}).to_string(),
            ),
        )
        .await;
        let (st, _) = call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        assert_eq!(st, StatusCode::CREATED);
        let spec = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(3)",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "http", "url": http.url() },
            "delivery": "live_best_effort",
            "recovery": "aligned",
            "checkpoint_dir": chk.to_string_lossy(),
        });
        let (st, body) = call(&state, auth_put("/v1/pipelines/p03", spec.to_string())).await;
        assert_eq!(st, StatusCode::CREATED, "{body}");
        let (st, body) = call(&state, auth_post("/v1/pipelines/p03/start", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "p03", "running", Duration::from_secs(5))
            .await
            .expect("first start");
        let start = std::time::Instant::now();
        while http.bodies().is_empty() && start.elapsed() < Duration::from_secs(4) {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        assert_eq!(http.bodies().len(), 1, "first run must emit the completed window");
        let (st, body) = call(&state, auth_post("/v1/pipelines/p03/checkpoint", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        let (st, _) = call(&state, auth_post("/v1/pipelines/p03/kill", "{}")).await;
        assert_eq!(st, StatusCode::OK);
        let mut more = std::fs::read(&path).unwrap();
        more.extend_from_slice(b"{\"device_id\":\"d1\",\"v\":4}\n");
        std::fs::write(&path, more).unwrap();
        let (st, body) = call(&state, auth_post("/v1/pipelines/p03/restore", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        wait_status(&state, "p03", "running", Duration::from_secs(5))
            .await
            .expect("restore must start the revision that contains restore=checkpoint");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            http.bodies().len(),
            1,
            "restore must resume empty window + v=4 (no complete window); fresh open would re-emit n=3: {:?}",
            http.body_strings()
        );
        let row = state.store.get_pipeline("p03").unwrap();
        assert_eq!(
            row.spec.restore.as_ref().map(|r| r.kind.as_str()),
            Some("checkpoint"),
            "latest revision must contain restore=checkpoint"
        );
        http.stop().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&chk);
    });
}

#[test]
fn p1_29_unauthenticated_does_not_write_audit() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let before = state.store.list_audit(200).unwrap().len();
        let req = Request::builder()
            .method("GET")
            .uri("/v1/audit")
            .body(Body::empty())
            .unwrap();
        let (st, _) = call(&state, req).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let after = state.store.list_audit(200).unwrap().len();
        assert_eq!(
            before, after,
            "auth failure must not touch the audit ledger"
        );
    });
}

#[test]
fn n2_put_empty_sql_is_4xx() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (st, _) = call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        assert_eq!(st, StatusCode::CREATED);
        let path = tmp("n2-empty.ndjson");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        for sql in ["", "   "] {
            let spec = json!({
                "version": 1,
                "stream": "sensors",
                "sql": sql,
                "source": { "kind": "file", "path": path.to_string_lossy() },
                "sink": { "kind": "log" },
                "delivery": "live_best_effort",
                "recovery": "restart_fresh"
            });
            let (st, body) =
                call(&state, auth_put("/v1/pipelines/n2empty", spec.to_string())).await;
            assert!(
                st.is_client_error(),
                "PUT sql={sql:?} must be 4xx, not disconnect: {st} {body}"
            );
            assert_ne!(st, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        }
        let (st, _) = call(
            &state,
            Request::builder()
                .method("GET")
                .uri("/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(
            st,
            StatusCode::OK,
            "connection must stay up after empty SQL PUT"
        );
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn p3_56_start_rejects_unparseable_body() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (st, _) = call(&state, auth_put("/v1/streams/sensors", STREAM)).await;
        assert_eq!(st, StatusCode::CREATED);
        let path = tmp("p356.ndjson");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let spec = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT device_id, v FROM sensors",
            "source": { "kind": "file", "path": path.to_string_lossy() },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        });
        let (st, body) = call(&state, auth_put("/v1/pipelines/p356", spec.to_string())).await;
        assert_eq!(st, StatusCode::CREATED, "{body}");
        let (st, body) = call(&state, auth_post("/v1/pipelines/p356/start", "{not-json")).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        let _ = std::fs::remove_file(&path);
    });
}

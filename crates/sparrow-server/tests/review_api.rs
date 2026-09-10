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

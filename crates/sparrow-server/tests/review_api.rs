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
    std::env::temp_dir().join(format!(
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
    let (state, _) = boot(store, kernel, TOKEN.into(), false, false).await.unwrap();
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
        assert_eq!(actual.revision, Some(1), "must run desired revision 1, not latest 2");
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
        call(&state, auth_put("/v1/pipelines/nowin", spec.to_string())).await;
        let (st, _) = call(&state, auth_post("/v1/pipelines/nowin/start", "{}")).await;
        assert_eq!(st, StatusCode::OK);
        wait_status(&state, "nowin", "failed", Duration::from_secs(5))
            .await
            .expect("missing window on aligned start must surface Failed");
        let _ = std::fs::remove_file(&path);
    });
}

//! API-driven MQTT → filter → HTTP using in-process demo I/O.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sparrow_control::{compact_kernel, Store};
use sparrow_server::{boot, router, wait_status, AppState};
use tower::ServiceExt;

const TOKEN: &str = "m3-test-token";
const STREAM: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true},
  {"name":"humidity","type":"float64","nullable":true},
  {"name":"ts","type":"timestamp_micros_utc","nullable":false},
  {"name":"payload","type":"dynamic","nullable":true}
]}"#;

fn spec() -> Value {
    json!({
        "version": 1,
        "stream": "sensors",
        "sql": "SELECT device_id, temperature, ts FROM sensors WHERE temperature > 25",
        "source": { "kind": "mqtt", "use_demo_io": true, "topic": "sensors/json", "client_id": "m3" },
        "sink": { "kind": "http", "use_demo_io": true },
        "delivery": "live_best_effort",
        "recovery": "restart_fresh"
    })
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

fn auth_get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

async fn setup() -> AppState {
    let store = Arc::new(Store::open_memory().unwrap());
    let kernel = Arc::new(compact_kernel().unwrap());
    let (state, _) = boot(store, kernel, TOKEN.into(), false, true).await.unwrap();
    state
}

#[test]
fn health_is_public_mutate_is_not() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (st, body) = call(
            &state,
            Request::builder()
                .uri("/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["delivery"], "live_best_effort");
        assert_eq!(body["recovery"], "restart_fresh");

        let (st, body) = call(&state, auth_put("/v1/streams/sensors", STREAM.to_string())).await;
        // reuse auth_put but strip auth
        let _ = (st, body);
        let (st, body) = call(
            &state,
            Request::builder()
                .method("PUT")
                .uri("/v1/streams/sensors")
                .header("content-type", "application/json")
                .body(Body::from(STREAM))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "policy_denied");
    });
}

#[test]
fn restore_and_at_least_once_rejected() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        call(&state, auth_put("/v1/streams/sensors", STREAM.to_string())).await;
        let mut bad = spec();
        bad["delivery"] = json!("at_least_once");
        let (st, body) = call(&state, auth_post("/v1/validate", bad.to_string())).await;
        assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "unsupported_delivery");

        let mut bad = spec();
        bad["restore"] = json!({"kind":"checkpoint","snapshot_id":"snap-1"});
        let (st, body) = call(&state, auth_post("/v1/validate", bad.to_string())).await;
        assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "unsupported_restore");
        assert_eq!(body["recovery"], "restart_fresh");

        let mut bad = spec();
        bad["recovery"] = json!("aligned");
        bad["restore"] = json!({"kind":"checkpoint","snapshot_id":"snap-1"});
        let (st, body) = call(&state, auth_post("/v1/validate", bad.to_string())).await;
        assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(st.as_u16(), 422);
        assert_eq!(body["error"]["code"], "unsupported_restore");
    });
}

#[test]
fn v1_file_aligned_validate_and_metrics() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        call(&state, auth_put("/v1/streams/sensors", STREAM.to_string())).await;
        let ok = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT device_id FROM sensors",
            "source": { "kind": "file", "path": "/tmp/sparrow-v1-events.ndjson" },
            "sink": { "kind": "log" },
            "delivery": "live_best_effort",
            "recovery": "aligned",
            "restore": { "kind": "checkpoint", "snapshot_id": "1" }
        });
        let (st, body) = call(&state, auth_post("/v1/validate", ok.to_string())).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert_eq!(body["accepted"], true);
        assert_eq!(body["recovery_aligned"], "aligned");

        let (st, metrics) = call(&state, auth_get("/v1/metrics")).await;
        assert_eq!(st, StatusCode::OK, "{metrics}");
        assert!(metrics.get("jobs_started").is_some());
        assert!(metrics["log"].as_str().unwrap().contains("sparrow_metrics"));
        assert_eq!(metrics["label_budget"], "job_and_connector_only; no per-event labels");

        let (st, _) = call(
            &state,
            Request::builder()
                .uri("/v1/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);

        let huge = "x".repeat(70 * 1024);
        let (st, body) = call(
            &state,
            Request::builder()
                .method("POST")
                .uri("/v1/validate")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(huge))
                .unwrap(),
        )
        .await;
        assert!(st.as_u16() == 413 || st.as_u16() == 400, "{st} {body}");

        let denied = json!({
            "version": 1,
            "stream": "sensors",
            "sql": "SELECT device_id FROM sensors",
            "source": { "kind": "mqtt", "host": "127.0.0.1", "port": 1883, "topic": "t" },
            "sink": { "kind": "http", "url": "http://evil.example:9/" },
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        });
        let (st, body) = call(&state, auth_post("/v1/validate", denied.to_string())).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "policy_denied");
    });
}

#[test]
fn graph_explain_v03_et_window() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let graph = sparrow_plan::et_tumble_template();
        let (st, body) = call(&state, auth_post("/v1/graphs/validate", graph)).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert_eq!(body["accepted"], true);
        let (st, body) = call(&state, auth_post("/v1/graphs/explain", graph)).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert!(body["time"].as_str().unwrap().contains("event-time"));
        assert!(body["state"].as_str().unwrap().contains("window_agg"));
        assert!(body["guarantee"].as_str().unwrap().contains("live_best_effort"));
        assert!(body["physical"].as_array().unwrap().iter().any(|s| {
            s.as_str().unwrap().contains("window")
        }));
    });
}

#[test]
fn api_mqtt_http_loop_and_fresh_restart() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (st, _) = call(&state, auth_put("/v1/streams/sensors", STREAM.to_string())).await;
        assert_eq!(st, StatusCode::CREATED);

        let (st, expl) = call(&state, auth_post("/v1/explain", spec().to_string())).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(expl["delivery"], "live_best_effort");
        assert!(expl["fused"].as_bool().unwrap());

        let (st, created) = call(&state, auth_put("/v1/pipelines/hot", spec().to_string())).await;
        assert_eq!(st, StatusCode::CREATED, "{created}");
        assert_eq!(created["etag"], "rev-1");

        let (st, _) = call(&state, auth_post("/v1/pipelines/hot/start", "{}")).await;
        assert_eq!(st, StatusCode::OK);
        wait_status(&state, "hot", "running", Duration::from_secs(5))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(80)).await;
        let (st, pubd) = call(&state, auth_post("/v1/demo/publish-fixture", "{}")).await;
        assert_eq!(st, StatusCode::OK, "{pubd}");

        let start = std::time::Instant::now();
        let bodies = loop {
            let (_, cap) = call(&state, auth_get("/v1/demo/capture")).await;
            let bodies = cap["bodies"].as_array().cloned().unwrap_or_default();
            if bodies.len() >= 3 {
                break bodies;
            }
            if start.elapsed() > Duration::from_secs(6) {
                panic!("expected 3 HTTP bodies, got {bodies:?}");
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        };
        let text: Vec<String> = bodies.iter().map(|v| v.as_str().unwrap_or("").into()).collect();
        assert!(text.iter().any(|b| b.contains("edge-a") && b.contains("26.2")), "{text:?}");
        assert!(text.iter().any(|b| b.contains("edge-b")));
        assert!(text.iter().any(|b| b.contains("edge-c")));

        let (_, st_json) = call(&state, auth_get("/v1/pipelines/hot/status")).await;
        assert_eq!(st_json["desired"]["status"], "running");
        assert_eq!(st_json["actual"]["status"], "running");
        let attempt1 = st_json["actual"]["attempt_id"].as_u64().unwrap();
        assert!(attempt1 >= 1);

        let (st, _) = call(&state, auth_post("/v1/pipelines/hot/stop", "{}")).await;
        assert_eq!(st, StatusCode::OK);
        wait_status(&state, "hot", "stopped", Duration::from_secs(5))
            .await
            .unwrap();

        // Simulate process restart: new supervisor, same catalog. Jobs are not restored.
        state.store.reset_actual_after_process_restart().unwrap();
        let names = state.store.list_pipeline_names().unwrap();
        assert_eq!(names, vec!["hot".to_string()]);
        let desired = state.store.desired("hot").unwrap();
        assert_eq!(desired.status, "stopped");
        let actual = state.store.actual("hot").unwrap();
        assert_eq!(actual.status, "stopped");
        assert_eq!(st_json["recovery"], "restart_fresh");
        assert_eq!(st_json["effective"]["recovery"], "restart_fresh");
        assert_eq!(st_json["effective"]["exactly_once"], false);
        assert!(st_json["effective"]["recovery_risk"].as_str().unwrap().contains("no_durable_restore"));
        assert_eq!(st_json["honesty"].as_str().unwrap().contains("fresh"), true);

        // Start again → new attempt, not compute recovery.
        call(&state, auth_post("/v1/pipelines/hot/start", "{}")).await;
        wait_status(&state, "hot", "running", Duration::from_secs(5))
            .await
            .unwrap();
        let (_, st2) = call(&state, auth_get("/v1/pipelines/hot/status")).await;
        let attempt2 = st2["actual"]["attempt_id"].as_u64().unwrap();
        assert!(attempt2 > attempt1, "fresh attempt expected, {attempt1} -> {attempt2}");
    });
}

#[test]
fn if_match_required_on_update() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        call(&state, auth_put("/v1/streams/sensors", STREAM.to_string())).await;
        call(&state, auth_put("/v1/pipelines/hot", spec().to_string())).await;
        let (st, body) = call(&state, auth_put("/v1/pipelines/hot", spec().to_string())).await;
        assert_eq!(st, StatusCode::PRECONDITION_REQUIRED, "{body}");
        let req = Request::builder()
            .method("PUT")
            .uri("/v1/pipelines/hot")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .header("if-match", "rev-1")
            .body(Body::from(spec().to_string()))
            .unwrap();
        let (st, body) = call(&state, req).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert_eq!(body["etag"], "rev-2");
    });
}

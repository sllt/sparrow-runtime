#![cfg(all(target_os = "linux", target_env = "gnu"))]
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;
#[path = "../../../tests/support/native_plugin.rs"]
mod fixture;
async fn request(
    app: &axum::Router,
    path: &str,
    body: Option<String>,
    auth: bool,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .uri(path)
        .method(if body.is_some() { "POST" } else { "GET" })
        .header("content-type", "application/json");
    if auth {
        req = req.header("authorization", "Bearer plugin-test-token");
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.unwrap_or_default())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}
#[tokio::test]
async fn plugins_api_upload_auth_approval_pin_and_body_limits() {
    let d = fixture::dir();
    let manager = sparrow_control::plugins::Manager::open(&d.0.join("packages"), true).unwrap();
    let store = Arc::new(sparrow_control::Store::open_memory().unwrap());
    store.configure_plugins(manager.clone()).unwrap();
    store
        .put_stream(
            "s",
            r#"{"fields":[{"name":"value","type":"int64","nullable":false}]}"#,
        )
        .unwrap();
    let kernel = Arc::new(sparrow_control::host_kernel().unwrap());
    let supervisor = sparrow_control::Supervisor::new(store.clone(), kernel, false, None).unwrap();
    let state = sparrow_server::AppState {
        store,
        supervisor,
        token: Arc::new("plugin-test-token".into()),
        safe_mode: false,
    };
    let audit = state.store.clone();
    let app = sparrow_server::router(state.clone());
    assert_eq!(
        request(&app, "/v1/plugins", None, false).await.0,
        StatusCode::UNAUTHORIZED
    );
    // A valid ELF with trailing bytes exercises the dedicated upload limit,
    // not a global enlargement of all ordinary management requests.
    let mut bytes = fixture::artifact().0.clone();
    bytes.resize(100_000, 0);
    let mut manifest = fixture::manifest();
    manifest["artifact_sha256"] = json!(sparrow_control::plugins::sha256(&bytes));
    // Base64 is produced by the CLI-compatible wire encoder in this test only.
    let input = d.0.join("artifact.so");
    std::fs::write(&input, bytes).unwrap();
    let encoded = std::process::Command::new("base64")
        .args(["-w0"])
        .arg(input)
        .output()
        .unwrap();
    assert!(encoded.status.success());
    let body =
        json!({"manifest":manifest,"artifact_base64":String::from_utf8(encoded.stdout).unwrap()})
            .to_string();
    assert_eq!(
        request(&app, "/v1/plugins/install", Some(body.clone()), false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&app, "/v1/validate", Some(body.clone()), true)
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let (status, value) = request(&app, "/v1/plugins/install", Some(body), true).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let digest = value["manifest_sha256"].as_str().unwrap();
    assert_eq!(value["enabled"], false);
    let url = format!("/v1/plugins/{digest}/enable");
    assert_ne!(
        request(
            &app,
            &url,
            Some(json!({"approve_manifest_sha256":"bad"}).to_string()),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    let mut safe = state;
    safe.safe_mode = true;
    assert_eq!(
        request(
            &sparrow_server::router(safe),
            &url,
            Some(json!({"approve_manifest_sha256":digest}).to_string()),
            true
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &app,
            &url,
            Some(json!({"approve_manifest_sha256":digest}).to_string()),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(audit
        .list_audit(100)
        .unwrap()
        .iter()
        .any(|a| a.action == "plugin_enable_attempt" && a.target.as_deref() == Some(digest)));
    let query = json!({"sql":fixture::sql(digest),"inputs":[{"stream":"s","rows":[{"value":1}]}]});
    assert_ne!(
        request(&app, "/v1/query", Some(query.to_string()), true)
            .await
            .0,
        StatusCode::OK,
        "non-preemptible native must not enter finite query"
    );
    let pin = manager
        .resolve("native_math", "v1", digest, "double")
        .unwrap();
    assert_ne!(
        request(
            &app,
            &format!("/v1/plugins/{digest}/disable"),
            Some("{}".into()),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    drop(pin);
    assert_eq!(
        request(
            &app,
            &format!("/v1/plugins/{digest}/disable"),
            Some("{}".into()),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_ne!(
        request(
            &app,
            &format!("/v1/plugins/{digest}/uninstall"),
            Some("{}".into()),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        request(
            &app,
            "/v1/plugins/install",
            Some("x".repeat(6 * 1024 * 1024 + 1)),
            true
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

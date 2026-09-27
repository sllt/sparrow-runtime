use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;
#[tokio::test]
async fn analysis_query_api_auth_isolation_and_limits() {
    let store = Arc::new(sparrow_control::Store::open_memory().unwrap());
    store
        .put_stream(
            "s",
            r#"{"fields":[{"name":"value","type":"int64","nullable":false}]}"#,
        )
        .unwrap();
    let kernel = Arc::new(sparrow_control::host_kernel().unwrap());
    let supervisor =
        sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
    let state = sparrow_server::AppState {
        store,
        supervisor,
        token: Arc::new("analysis-test-token".into()),
        safe_mode: false,
    };
    let request = json!({"sql":"SELECT sha256(to_string(value)) AS digest FROM s","inputs":[{"stream":"s","rows":[{"value":123}]}]});
    let response = sparrow_server::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = sparrow_server::router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("authorization", "Bearer analysis-test-token")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["execution"], "independent_bounded_kernel");
    assert_eq!(
        value["rows"][0]["digest"],
        "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"
    );
    assert_eq!(
        kernel.metrics.snapshot().jobs_started,
        0,
        "query must not consume a live-rule Kernel slot"
    );
}

//! TAB-02 public atomic mutation, rollback and finite-history API contract.
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sparrow_control::{compact_kernel, Store, Supervisor};
use sparrow_server::{router, AppState};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "tab02-api-test-token";
fn setup() -> AppState {
    let store = Arc::new(Store::open_memory().unwrap());
    let supervisor = Supervisor::new(
        store.clone(),
        Arc::new(compact_kernel().unwrap()),
        false,
        None,
    )
    .unwrap();
    AppState {
        store,
        supervisor,
        token: Arc::new(TOKEN.into()),
        safe_mode: false,
    }
}
async fn call(
    state: &AppState,
    method: Method,
    uri: &str,
    body: Value,
    auth: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if auth {
        request = request.header("authorization", format!("Bearer {TOKEN}"));
    }
    let response = router(state.clone())
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}
fn publication() -> Value {
    json!({"expected_revision":0,"table":{
        "fields":[{"name":"id","type":"utf8","nullable":false},{"name":"value","type":"int64","nullable":true}],
        "keys":["id"],"rows":[["a",10],["b",11]]
    }})
}
fn has_rows(value: &Value) -> bool {
    match value {
        Value::Object(values) => values.contains_key("rows") || values.values().any(has_rows),
        Value::Array(values) => values.iter().any(has_rows),
        _ => false,
    }
}

#[test]
fn tab02_api_mutations_rollback_history_cas_and_audit() {
    compact_kernel().unwrap().block_on(async {
        let state = setup();
        for (method, uri) in [
            (Method::POST, "/v1/tables/sites/mutate"),
            (Method::POST, "/v1/tables/sites/rollback"),
            (Method::GET, "/v1/tables/sites/revisions"),
        ] {
            assert_eq!(
                call(&state, method, uri, json!({}), false).await.0,
                StatusCode::UNAUTHORIZED
            );
        }
        let (status, first) =
            call(&state, Method::PUT, "/v1/tables/sites", publication(), true).await;
        assert_eq!(status, StatusCode::CREATED);
        let batch = json!({"expected_revision":1,"operations":[
            {"op":"upsert","row":["a",20]},
            {"op":"delete","key":["b"]},
            {"op":"upsert","row":["c",null]}
        ]});
        let (status, updated) = call(
            &state,
            Method::POST,
            "/v1/tables/sites/mutate",
            batch.clone(),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["revision"], 2);
        assert_eq!(updated["row_count"], 2);
        assert!(!has_rows(&updated));
        assert_ne!(updated["sha256"], first["sha256"]);
        let (status, replay) =
            call(&state, Method::POST, "/v1/tables/sites/mutate", batch, true).await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{replay}");

        let rollback = json!({"expected_revision":2,"target_revision":1});
        let (status, restored) = call(
            &state,
            Method::POST,
            "/v1/tables/sites/rollback",
            rollback.clone(),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{restored}");
        assert_eq!(restored["revision"], 3);
        assert!(!has_rows(&restored));
        assert_ne!(restored["sha256"], first["sha256"]);
        assert_eq!(
            call(
                &state,
                Method::POST,
                "/v1/tables/sites/rollback",
                rollback,
                true
            )
            .await
            .0,
            StatusCode::PRECONDITION_FAILED
        );

        let (_, latest) = call(&state, Method::GET, "/v1/tables/sites", Value::Null, true).await;
        assert_eq!(latest["table"]["rows"], publication()["table"]["rows"]);
        let (_, old) = call(
            &state,
            Method::GET,
            "/v1/tables/sites/revisions/2",
            Value::Null,
            true,
        )
        .await;
        assert_eq!(old["table"]["rows"], json!([["a", 20], ["c", null]]));
        let (status, history) = call(
            &state,
            Method::GET,
            "/v1/tables/sites/revisions",
            Value::Null,
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(history["revisions"].as_array().unwrap().len(), 3);
        assert!(!has_rows(&history));
        let (_, audit) = call(&state, Method::GET, "/v1/audit", Value::Null, true).await;
        for action in ["mutate_reference_table", "rollback_reference_table"] {
            assert!(audit["audit"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["action"] == action && item["outcome"] == "ok"));
            assert!(audit["audit"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["action"] == action && item["outcome"] == "failed"));
        }
        state.supervisor.shutdown().await;
    });
}

#[test]
fn tab02_api_failures_are_bounded_strict_atomic_and_non_data_bearing() {
    compact_kernel().unwrap().block_on(async {
        let state = setup();
        assert_eq!(call(&state, Method::PUT, "/v1/tables/sites", publication(), true).await.0, StatusCode::CREATED);
        let marker = "business-data-marker-never-reflect";
        for invalid in [
            json!({"expected_revision":1,"operations":[{"op":"upsert","row":["a",20]},{"op":"delete","key":[marker]}]}),
            json!({"expected_revision":1,"operations":[{"op":"upsert","row":[marker,marker]}]}),
            json!({"expected_revision":1,"operations":[{"op":"upsert","row":["a",20],"unknown":marker}]}),
            json!({"expected_revision":1,"operations":[],"unknown":marker}),
            json!({"operations":[]}),
        ] {
            let (status, error) = call(&state, Method::POST, "/v1/tables/sites/mutate", invalid, true).await;
            assert!(!status.is_success());
            assert!(!error.to_string().contains(marker));
            assert_eq!(state.store.get_reference_table("sites").unwrap().revision, 1);
        }
        let valid = json!({"expected_revision":1,"operations":[{"op":"upsert","row":["a",20]}]});
        state.store.debug_fail_next_commit();
        assert!(!call(&state, Method::POST, "/v1/tables/sites/mutate", valid.clone(), true).await.0.is_success());
        assert_eq!(state.store.get_reference_table("sites").unwrap().revision, 1);
        assert_eq!(call(&state, Method::POST, "/v1/tables/sites/mutate", valid, true).await.0, StatusCode::OK);
        for invalid in [
            json!({"expected_revision":2,"target_revision":1,"unknown":marker}),
            json!({"expected_revision":2,"target_revision":3}),
            json!({"expected_revision":2,"target_revision":0}),
        ] {
            let (status, error) = call(&state, Method::POST, "/v1/tables/sites/rollback", invalid, true).await;
            assert!(!status.is_success());
            assert!(!error.to_string().contains(marker));
            assert_eq!(state.store.get_reference_table("sites").unwrap().revision, 2);
        }
        assert!(!call(&state, Method::GET, "/v1/tables/missing/revisions", Value::Null, true).await.0.is_success());
        for endpoint in ["mutate", "rollback"] {
            let body = Value::String("x".repeat(70 * 1024));
            assert_eq!(call(&state, Method::POST, &format!("/v1/tables/sites/{endpoint}"), body, true).await.0, StatusCode::PAYLOAD_TOO_LARGE);
        }
        let (_, audit) = call(&state, Method::GET, "/v1/audit", Value::Null, true).await;
        assert!(!audit.to_string().contains(marker));
        state.supervisor.shutdown().await;
    });
}

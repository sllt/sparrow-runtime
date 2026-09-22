//! B1 immutable reference-table API contract.
//!
//! These tests exercise the HTTP boundary rather than reaching into SQLite:
//! authentication, bounded/strict input, publication CAS, immutable revision
//! reads, metadata-only listing, pipeline pinning, conservative GC, and the
//! status distinction between stored-latest and running-actual configuration.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sparrow_control::{compact_kernel, PipelineSpec, Store, Supervisor};
use sparrow_server::{router, AppState};
use tower::ServiceExt;

const TOKEN: &str = "core-b-api-test-token";

async fn setup() -> AppState {
    let store = Arc::new(Store::open_memory().unwrap());
    let kernel = Arc::new(compact_kernel().unwrap());
    let supervisor = Supervisor::new(store.clone(), kernel, false, None).unwrap();
    AppState {
        store,
        supervisor,
        token: Arc::new(TOKEN.into()),
        safe_mode: false,
    }
}

async fn call(state: &AppState, request: Request<Body>) -> (StatusCode, Value) {
    let response = router(state.clone()).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&bytes)}))
    };
    (status, body)
}

fn request(method: Method, uri: &str, body: Value, authenticated: bool) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if authenticated {
        builder = builder.header("authorization", format!("Bearer {TOKEN}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

fn get(uri: &str) -> Request<Body> {
    request(Method::GET, uri, Value::Null, true)
}

fn table_request(expected_revision: u64, value: i64) -> Value {
    json!({
        "expected_revision": expected_revision,
        "table": {
            "fields": [
                {"name": "id", "type": "utf8", "nullable": false},
                {"name": "value", "type": "int64", "nullable": true}
            ],
            "keys": ["id"],
            "rows": [["a", value], ["b", value + 1]]
        }
    })
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key(key) || object.values().any(|child| contains_key(child, key))
        }
        Value::Array(values) => values.iter().any(|child| contains_key(child, key)),
        _ => false,
    }
}

#[test]
fn core_b_table_api_auth_strict_input_limit_and_non_data_errors() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;

        let (status, body) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(0, 10), false),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "policy_denied");

        let (status, _) = call(
            &state,
            request(Method::GET, "/v1/tables", Value::Null, false),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let oversized = Value::String("x".repeat(70 * 1024));
        let (status, _) = call(
            &state,
            Request::builder()
                .method(Method::PUT)
                .uri("/v1/tables/sites")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(oversized.as_str().unwrap().to_owned()))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

        let marker = "row-data-must-not-escape";
        let invalid = json!({
            "expected_revision": 0,
            "table": {
                "fields": [
                    {"name": "id", "type": "utf8", "nullable": false},
                    {"name": "value", "type": "int64", "nullable": false}
                ],
                "keys": ["id"],
                "rows": [[marker, "wrong-type"]]
            }
        });
        let (status, body) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", invalid, true),
        )
        .await;
        assert!(
            !status.is_success(),
            "invalid table must be rejected: {body}"
        );
        assert!(!body.to_string().contains(marker));

        let (status, body) = call(&state, get("/v1/tables")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["tables"].as_array().unwrap().len(), 0);
        assert!(!contains_key(&body, "rows"));

        let (status, body) = call(&state, get("/v1/tables/missing/dependencies")).await;
        assert!(
            !status.is_success(),
            "unknown table dependency preview accepted"
        );
        assert!(!contains_key(&body, "rows"));

        state.supervisor.shutdown().await;
    });
}

#[test]
fn core_b_publish_cas_and_exact_revision_reads_are_immutable() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;

        let (status, first) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(0, 10), true),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{first}");
        assert_eq!(first["revision"], 1);
        assert_eq!(first["sha256"].as_str().unwrap().len(), 64);
        assert!(!contains_key(&first, "rows"));
        let sha1 = first["sha256"].as_str().unwrap().to_owned();

        let (status, list) = call(&state, get("/v1/tables")).await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["tables"][0]["name"], "sites");
        assert_eq!(list["tables"][0]["revision"], 1);
        assert_eq!(list["tables"][0]["sha256"], sha1);
        assert_eq!(list["tables"][0]["row_count"], 2);
        assert!(!contains_key(&list, "rows"));

        let (status, latest) = call(&state, get("/v1/tables/sites")).await;
        assert_eq!(status, StatusCode::OK, "{latest}");
        assert_eq!(latest["revision"], 1);
        assert_eq!(latest["table"]["rows"][0][0], "a");

        let (status, exact) = call(&state, get("/v1/tables/sites/revisions/1")).await;
        assert_eq!(status, StatusCode::OK, "{exact}");
        assert_eq!(exact["revision"], 1);
        assert_eq!(exact["sha256"], sha1);
        assert_eq!(exact["table"]["rows"][0][1], 10);

        let (status, stale) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(0, 99), true),
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{stale}");
        assert!(!stale.to_string().contains("99"));

        let (status, _) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(1, 20), true),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, old) = call(&state, get("/v1/tables/sites/revisions/1")).await;
        assert_eq!(status, StatusCode::OK, "{old}");
        assert_eq!(old["sha256"], sha1);
        assert_eq!(old["table"]["rows"][0][1], 10);
        let (status, latest) = call(&state, get("/v1/tables/sites")).await;
        assert_eq!(status, StatusCode::OK, "{latest}");
        assert_eq!(latest["revision"], 2);
        assert_ne!(latest["sha256"], sha1);
        assert_eq!(latest["table"]["rows"][0][1], 20);

        let (status, audit) = call(&state, get("/v1/audit")).await;
        assert_eq!(status, StatusCode::OK, "{audit}");
        let actions = audit["audit"].as_array().unwrap();
        assert!(actions
            .iter()
            .any(|row| row["action"] == "put_reference_table"));
        assert!(actions.iter().any(|row| row["outcome"] == "failed"));
        assert!(actions
            .iter()
            .all(|row| !row.to_string().contains("row-data-must-not-escape")));

        state.supervisor.shutdown().await;
    });
}

#[test]
fn core_b_gc_keeps_pipeline_revision_pins_and_deletes_only_unpinned_history() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (status, first) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(0, 10), true),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{first}");
        let sha1 = first["sha256"].as_str().unwrap().to_owned();
        for (expected, value) in [(1, 20), (2, 30)] {
            let (status, body) = call(
                &state,
                request(
                    Method::PUT,
                    "/v1/tables/sites",
                    table_request(expected, value),
                    true,
                ),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }

        // Store pinning is attached to every persisted pipeline revision.  A
        // direct Store insert here avoids conflating this GC test with the
        // separate SQL Lookup binder acceptance test.
        let pipeline: PipelineSpec = serde_json::from_value(json!({
            "version": 1,
            "stream": "events",
            "reference_tables": {
                "sites": {"revision": 1, "sha256": sha1}
            },
            "sql": "SELECT id FROM events",
            "source": {"kind": "file", "path": "fixture.ndjson"},
            "sink": {"kind": "log"},
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        }))
        .unwrap();
        state.store.put_pipeline("pinned", &pipeline, None).unwrap();

        let (status, dependencies) = call(&state, get("/v1/tables/sites/dependencies")).await;
        assert_eq!(status, StatusCode::OK, "{dependencies}");
        assert_eq!(dependencies["name"], "sites");
        assert_eq!(dependencies["latest_revision"], 3);
        assert_eq!(dependencies["pins"]["total_count"], 1);
        assert_eq!(dependencies["pins"]["returned_count"], 1);
        assert_eq!(dependencies["pins"]["truncated"], false);
        assert_eq!(dependencies["pins"]["items"][0]["pipeline"], "pinned");
        assert_eq!(dependencies["pins"]["items"][0]["pipeline_revision"], 1);
        assert_eq!(dependencies["pins"]["items"][0]["table_revision"], 1);
        assert_eq!(dependencies["pins"]["items"][0]["sha256"], sha1);
        assert!(!contains_key(&dependencies, "rows"));

        let (status, gc) = call(
            &state,
            request(Method::POST, "/v1/tables/sites/gc", Value::Null, true),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{gc}");
        assert_eq!(gc["gc"]["deleted"], 1);
        assert_eq!(gc["gc"]["latest_revision"], 3);
        assert_eq!(gc["gc"]["pinned_revisions"], json!([1]));

        let (status, _) = call(&state, get("/v1/tables/sites/revisions/2")).await;
        assert!(
            !status.is_success(),
            "unpinned historical revision must be gone"
        );
        let (status, retained) = call(&state, get("/v1/tables/sites/revisions/1")).await;
        assert_eq!(status, StatusCode::OK, "{retained}");
        let (status, latest) = call(&state, get("/v1/tables/sites")).await;
        assert_eq!(status, StatusCode::OK, "{latest}");
        assert_eq!(latest["revision"], 3);

        let (status, audit) = call(&state, get("/v1/audit")).await;
        assert_eq!(status, StatusCode::OK, "{audit}");
        assert!(audit["audit"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| { row["action"] == "gc_reference_table" && row["outcome"] == "ok" }));

        state.supervisor.shutdown().await;
    });
}

#[test]
fn core_b_status_separates_stored_latest_and_running_actual_table_bindings() {
    let kernel = compact_kernel().unwrap();
    kernel.block_on(async {
        let state = setup().await;
        let (status, first) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(0, 10), true),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{first}");
        let sha = first["sha256"].as_str().unwrap().to_owned();
        let pipeline: PipelineSpec = serde_json::from_value(json!({
            "version": 1,
            "stream": "events",
            "reference_tables": {
                "sites": {"revision": 1, "sha256": sha}
            },
            "sql": "SELECT id FROM events",
            "source": {"kind": "file", "path": "fixture.ndjson"},
            "sink": {"kind": "log"},
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        }))
        .unwrap();
        state.store.put_pipeline("bound", &pipeline, None).unwrap();
        // Deterministic catalog fixture only: no Supervisor run loop is
        // spawned here.  The production process driver covers real running
        // and restart convergence; this test isolates status projection.
        state
            .store
            .set_actual("bound", "running", Some(1), 1, None)
            .unwrap();

        let (status, second) = call(
            &state,
            request(Method::PUT, "/v1/tables/sites", table_request(1, 20), true),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second}");
        let sha2 = second["sha256"].as_str().unwrap().to_owned();
        let pipeline2: PipelineSpec = serde_json::from_value(json!({
            "version": 1,
            "stream": "events",
            "reference_tables": {
                "sites": {"revision": 2, "sha256": sha2}
            },
            "sql": "SELECT id FROM events",
            "source": {"kind": "file", "path": "fixture.ndjson"},
            "sink": {"kind": "log"},
            "delivery": "live_best_effort",
            "recovery": "restart_fresh"
        }))
        .unwrap();
        state
            .store
            .put_pipeline("bound", &pipeline2, Some("rev-1"))
            .unwrap();

        let (status, body) = call(&state, get("/v1/pipelines/bound/status")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["reference_tables"]["stored_latest"]["revision"], 2);
        assert_eq!(
            body["reference_tables"]["stored_latest"]["bindings"]["sites"]["revision"],
            2
        );
        assert_eq!(
            body["reference_tables"]["running_actual"]["available"],
            true
        );
        assert_eq!(body["reference_tables"]["running_actual"]["revision"], 1);
        assert_eq!(
            body["reference_tables"]["running_actual"]["bindings"]["sites"]["revision"],
            1
        );

        // A stale revision on a stopped/failed actual row must not be
        // presented as the live binding.
        state
            .store
            .set_actual("bound", "stopped", Some(1), 2, None)
            .unwrap();
        let (status, stopped) = call(&state, get("/v1/pipelines/bound/status")).await;
        assert_eq!(status, StatusCode::OK, "{stopped}");
        assert_eq!(stopped["reference_tables"]["stored_latest"]["revision"], 2);
        assert_eq!(
            stopped["reference_tables"]["running_actual"]["available"],
            false
        );
        assert_eq!(
            stopped["reference_tables"]["running_actual"]["reason"],
            "actual_not_running"
        );

        state.supervisor.shutdown().await;
    });
}

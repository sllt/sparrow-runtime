//! `/v1` management API. Bearer token required for mutating calls.
//! Default bind is loopback. Body size is capped.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sparrow_control::{
    bind_plan, binder_catalog, capabilities_json, effective_guarantees, explain_plan, honesty_json,
    request_start, request_start_at, request_stop, stream_schema, validate_aligned_plan,
    validate_io, DemoHarness,
    PipelineSpec,
    RestoreSpec, Store, StreamSpec, Supervisor, HONESTY,
};
use sparrow_runtime::Kernel;
use sparrow_model::{ErrorCode, SparrowError};
use tower_http::limit::RequestBodyLimitLayer;

pub const DEFAULT_BIND: &str = "127.0.0.1:43180";
pub const MAX_BODY: usize = 64 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub supervisor: Arc<Supervisor>,
    pub token: Arc<String>,
    pub safe_mode: bool,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/validate", post(validate))
        .route("/v1/explain", post(explain))
        .route("/v1/graphs/validate", post(graph_validate))
        .route("/v1/graphs/explain", post(graph_explain))
        .route("/v1/test", post(test_plan))
        .route("/v1/streams", get(list_streams))
        .route("/v1/streams/{name}", put(put_stream).get(get_stream))
        .route("/v1/pipelines", get(list_pipelines))
        .route("/v1/pipelines/{name}", put(put_pipeline).get(get_pipeline))
        .route("/v1/pipelines/{name}/status", get(pipeline_status))
        .route("/v1/pipelines/{name}/start", post(start_pipeline))
        .route("/v1/pipelines/{name}/stop", post(stop_pipeline))
        .route("/v1/pipelines/{name}/checkpoint", post(checkpoint_pipeline))
        .route("/v1/pipelines/{name}/restore", post(restore_pipeline))
        .route("/v1/pipelines/{name}/kill", post(kill_pipeline))
        .route("/v1/allowlist", put(put_allow))
        .route("/v1/secrets/{name}", put(put_secret))
        .route("/v1/audit", get(list_audit))
        .route("/v1/metrics", get(metrics))
        .route("/v1/demo/io", get(demo_io))
        .route("/v1/demo/publish-fixture", post(demo_publish))
        .route("/v1/demo/capture", get(demo_capture))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(RequestBodyLimitLayer::new(MAX_BODY))
        .with_state(state)
}

pub struct ApiError {
    status: StatusCode,
    err: SparrowError,
}

impl From<SparrowError> for ApiError {
    fn from(err: SparrowError) -> Self {
        let status = match err.code {
            ErrorCode::InvalidArgument | ErrorCode::InvalidSchema | ErrorCode::TypeMismatch => {
                StatusCode::BAD_REQUEST
            }
            ErrorCode::PolicyDenied | ErrorCode::SecretMissing => StatusCode::FORBIDDEN,
            ErrorCode::UnsupportedDelivery | ErrorCode::UnsupportedRestore | ErrorCode::FeatureUnavailable => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            ErrorCode::MaxRecordSize | ErrorCode::BoundExceeded => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self { status, err }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = honesty_json();
        if let Value::Object(map) = &mut body {
            map.insert(
                "error".into(),
                json!({
                    "code": self.err.code.as_str(),
                    "message": self.err.message,
                    "retryable": self.err.retryable,
                }),
            );
        }
        (self.status, Json(body)).into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

fn require_auth(state: &AppState, headers: &HeaderMap) -> ApiResult<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = raw.strip_prefix("Bearer ").unwrap_or("");
    if !tokens_equal(token, &state.token) {
        // P1-29: do not touch SQLite on auth failure (sync audit is a DoS / wipe vector).
        return Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            err: SparrowError::new(ErrorCode::PolicyDenied, "unauthorized: bearer token required"),
        });
    }
    Ok("token".into())
}

fn tokens_equal(a: &str, b: &str) -> bool {
    if a.len() != b.len() || a.is_empty() {
        return false;
    }
    a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn if_match(headers: &HeaderMap) -> Option<String> {
    headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string())
}

async fn root() -> Json<Value> {
    Json(json!({
        "name": "sparrow-server",
        "version": env!("CARGO_PKG_VERSION"),
        "bind_default": DEFAULT_BIND,
        "delivery": "live_best_effort",
        "recovery": "restart_fresh",
        "recovery_aligned": "aligned",
        "honesty": HONESTY,
        "endpoints": [
            "GET /v1/health",
            "GET /v1/metrics",
            "POST /v1/validate",
            "POST /v1/explain",
            "POST /v1/graphs/validate",
            "POST /v1/graphs/explain",
            "PUT /v1/streams/{name}",
            "PUT /v1/pipelines/{name}",
            "POST /v1/pipelines/{name}/start",
            "POST /v1/pipelines/{name}/stop",
            "GET /v1/pipelines/{name}/status"
        ]
    }))
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "safe_mode": state.safe_mode,
        "catalog_schema_version": sparrow_control::CATALOG_SCHEMA_VERSION,
        "format_version": sparrow_control::FORMAT_VERSION,
        "delivery": "live_best_effort",
        "recovery": "restart_fresh",
        "recovery_aligned": "aligned",
        "replay": "unsupported",
        "honesty": HONESTY,
    }))
}

async fn capabilities(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(capabilities_json()))
}

async fn validate(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
    let report = run_validate(&state, &spec)?;
    let _ = state.store.audit(&actor, "validate", spec.sql.as_deref(), None, "ok");
    Ok(Json(report))
}

async fn explain(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
    Ok(Json(run_explain(&state, &spec)?))
}

async fn test_plan(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
    let mut out = run_validate(&state, &spec)?;
    if let Value::Object(map) = &mut out {
        map.insert("explain".into(), run_explain(&state, &spec)?);
        map.insert("started".into(), json!(false));
        map.insert("note".into(), json!("test does not start I/O"));
    }
    Ok(Json(out))
}

fn run_validate(state: &AppState, spec: &PipelineSpec) -> ApiResult<Value> {
    spec.check_delivery().map_err(ApiError::from)?;
    let stream = state.store.get_stream(&spec.stream).map_err(ApiError::from)?;
    let schema = stream_schema(
        &stream.name,
        &serde_json::from_str::<StreamSpec>(&stream.schema_json).map_err(|e| {
            ApiError::from(SparrowError::new(ErrorCode::InvalidSchema, e.to_string()))
        })?,
    )
    .map_err(ApiError::from)?;
    let catalog = binder_catalog(&state.store).map_err(ApiError::from)?;
    let plan = bind_plan(spec, &catalog, spec.stream.as_str(), 0).map_err(ApiError::from)?;
    validate_aligned_plan(spec, &plan).map_err(ApiError::from)?;
    let demo = state.supervisor.demo_endpoints();
    let policy = sparrow_control::validate::store_policy(&state.store, demo.as_ref())
        .map_err(ApiError::from)?;
    let secrets = sparrow_control::validate::StoreSecrets::new(Arc::clone(&state.store));
    validate_io(spec, &schema, &secrets, &policy, demo.as_ref()).map_err(ApiError::from)?;
    let mut body = honesty_json();
    if let Value::Object(map) = &mut body {
        map.insert("accepted".into(), json!(true));
        map.insert("stream".into(), json!(spec.stream));
    }
    Ok(body)
}

fn run_explain(state: &AppState, spec: &PipelineSpec) -> ApiResult<Value> {
    let catalog = binder_catalog(&state.store).map_err(ApiError::from)?;
    let plan = bind_plan(spec, &catalog, spec.stream.as_str(), 0).map_err(ApiError::from)?;
    let e = explain_plan(&plan);
    Ok(json!({
        "accepted": e.accepted,
        "stages": e.stages,
        "physical": e.physical,
        "fusion": e.fusion,
        "time": e.time,
        "state": e.state,
        "guarantee": e.guarantee,
        "fused": e.fused,
        "mailbox_count": e.mailbox_count,
        "delivery": e.delivery,
        "recovery": e.recovery,
        "replay": e.replay,
        "honesty": e.honesty,
        "experimental": e.experimental,
    }))
}

async fn graph_validate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    let text = std::str::from_utf8(&body).map_err(|_| {
        ApiError::from(SparrowError::new(
            ErrorCode::InvalidArgument,
            "GraphSpec must be UTF-8 JSON",
        ))
    })?;
    let spec = sparrow_plan::GraphSpec::from_json(text).map_err(ApiError::from)?;
    let catalog = sparrow_plan::Catalog::new();
    let bound = sparrow_plan::validate_graph(&spec, &catalog).map_err(ApiError::from)?;
    Ok(Json(json!({
        "accepted": true,
        "nodes": bound.nodes.len(),
        "honesty": HONESTY,
    })))
}

async fn graph_explain(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    let text = std::str::from_utf8(&body).map_err(|_| {
        ApiError::from(SparrowError::new(
            ErrorCode::InvalidArgument,
            "GraphSpec must be UTF-8 JSON",
        ))
    })?;
    let spec = sparrow_plan::GraphSpec::from_json(text).map_err(ApiError::from)?;
    let report = sparrow_plan::explain_graph(&spec, &sparrow_plan::Catalog::new())
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "accepted": report.accepted,
        "stages": report.stages,
        "physical": report.physical,
        "fusion": report.fusion,
        "time": report.time,
        "state": report.state,
        "guarantee": report.guarantee,
        "fused": report.fused,
        "mailbox_count": report.mailbox_count,
        "delivery": report.delivery,
        "recovery": report.recovery,
        "honesty": report.honesty,
        "experimental": report.experimental,
    })))
}

async fn put_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let actor = require_auth(&state, &headers)?;
    let spec = StreamSpec::from_json(&body).map_err(ApiError::from)?;
    let _ = stream_schema(&name, &spec).map_err(ApiError::from)?;
    let json = serde_json::to_string(&spec).map_err(|e| {
        ApiError::from(SparrowError::new(ErrorCode::InvalidArgument, e.to_string()))
    })?;
    state.store.put_stream(&name, &json).map_err(ApiError::from)?;
    state
        .store
        .audit(&actor, "put_stream", Some(&name), None, "ok")
        .map_err(ApiError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"name": name, "fields": spec.fields})),
    ))
}

async fn get_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let row = state.store.get_stream(&name).map_err(ApiError::from)?;
    let spec: Value = serde_json::from_str(&row.schema_json).unwrap_or(Value::Null);
    Ok(Json(json!({"name": row.name, "schema": spec})))
}

async fn list_streams(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let names: Vec<String> = state
        .store
        .list_streams()
        .map_err(ApiError::from)?
        .into_iter()
        .map(|s| s.name)
        .collect();
    Ok(Json(json!({"streams": names})))
}

async fn put_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let actor = require_auth(&state, &headers)?;
    let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
    run_validate(&state, &spec)?;
    let etag = if_match(&headers);
    let created = state.store.get_pipeline(&name).is_err();
    let row = state
        .store
        .put_pipeline(&name, &spec, etag.as_deref())
        .map_err(|e| {
            if e.message.contains("If-Match") {
                ApiError {
                    status: if e.message.contains("required") {
                        StatusCode::PRECONDITION_REQUIRED
                    } else {
                        StatusCode::PRECONDITION_FAILED
                    },
                    err: e,
                }
            } else {
                ApiError::from(e)
            }
        })?;
    state
        .store
        .audit(&actor, "put_pipeline", Some(&name), Some(&row.etag), "ok")
        .map_err(ApiError::from)?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(json!({
            "name": row.name,
            "revision": row.latest_revision,
            "etag": row.etag,
            "delivery": "live_best_effort",
            "recovery": "restart_fresh",
        })),
    ))
}

async fn get_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(status_body(&state, &name)?))
}

async fn pipeline_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(status_body(&state, &name)?))
}

fn status_body(state: &AppState, name: &str) -> ApiResult<Value> {
    let row = state.store.get_pipeline(name).map_err(ApiError::from)?;
    let desired = state.store.desired(name).ok();
    let actual = state.store.actual(name).ok();
    Ok(json!({
        "name": row.name,
        "revision": row.latest_revision,
        "etag": row.etag,
        "spec": row.spec,
        "desired": desired.map(|d| json!({"revision": d.revision, "status": d.status})),
        "actual": actual.map(|a| json!({
            "revision": a.revision,
            "status": a.status,
            "attempt_id": a.attempt_id,
            "last_error": a.last_error,
        })),
        "delivery": "live_best_effort",
        "recovery": row.spec.recovery.clone(),
        "replay": if matches!(row.spec.source.kind.as_str(), "file" | "file_replay" | "replay") {
            "replayable"
        } else {
            "unsupported"
        },
        "honesty": HONESTY,
        "effective": effective_guarantees(&row.spec),
    }))
}

async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let snap = state.supervisor.kernel().metrics.snapshot();
    let io = state.supervisor.io_snapshot().await;
    Ok(Json(json!({
        "jobs_started": snap.jobs_started,
        "jobs_stopped": snap.jobs_stopped,
        "jobs_failed": snap.jobs_failed,
        "ingested_rows": snap.ingested_rows,
        "emitted_rows": snap.emitted_rows,
        "queue_items": snap.queue_items,
        "queue_bytes": snap.queue_bytes,
        "state_keys": snap.state_keys,
        "state_bytes": snap.state_bytes,
        "live_samples": snap.live_samples,
        "watermark_lag_micros": snap.watermark_lag_micros,
        "checkpoint_duration_micros": snap.checkpoint_duration_micros,
        "checkpoint_bytes": snap.checkpoint_bytes,
        "checkpoint_commits": snap.checkpoint_commits,
        "checkpoint_aborts": snap.checkpoint_aborts,
        "io": {
            "mqtt_received": io.mqtt_received,
            "mqtt_decoded": io.mqtt_decoded,
            "mqtt_dropped_bad": io.mqtt_dropped_bad,
            "mqtt_dropped_full": io.mqtt_dropped_full,
            "http_posted": io.http_posted,
            "http_failed": io.http_failed,
            "http_dropped": io.http_dropped,
            "http_inflight": io.http_inflight,
            "log_written": io.log_written,
            "decode_errors": io.decode_errors,
        },
        "log": snap.log_line(),
        "label_budget": "job_and_connector_only; no per-event labels",
        "honesty": HONESTY,
    })))
}

async fn list_pipelines(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let names = state.store.list_pipeline_names().map_err(ApiError::from)?;
    Ok(Json(json!({"pipelines": names})))
}

#[derive(Deserialize, Default)]
struct StartBody {
    revision: Option<u64>,
}

async fn start_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    if let Some(tag) = if_match(&headers) {
        let row = state.store.get_pipeline(&name).map_err(ApiError::from)?;
        if tag != row.etag {
            return Err(ApiError {
                status: StatusCode::PRECONDITION_FAILED,
                err: SparrowError::new(ErrorCode::InvalidArgument, "If-Match does not match etag"),
            });
        }
    }
    let revision = if body.is_empty() {
        None
    } else {
        serde_json::from_slice::<StartBody>(&body)
            .ok()
            .and_then(|b| b.revision)
    };
    request_start_at(&state.store, &name, &actor, revision).map_err(ApiError::from)?;
    state.supervisor.wake();
    let mut body = status_body(&state, &name)?;
    if let Value::Object(map) = &mut body {
        map.insert(
            "note".into(),
            json!("desired state committed; supervisor converges asynchronously. This is not exactly-once."),
        );
    }
    Ok(Json(body))
}

async fn checkpoint_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let id = state
        .supervisor
        .checkpoint_named(&name)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "checkpoint_id": id,
        "exactly_once": false,
        "honesty": HONESTY,
    })))
}

async fn restore_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let _ = state.supervisor.kill_named(&name).await;
    let store = state.store.clone();
    let store_c = store.clone();
    let name_c = name.clone();
    store
        .run_blocking(move || {
            let row = store_c.get_pipeline(&name_c)?;
            let mut spec = row.spec;
            spec.restore = Some(RestoreSpec {
                kind: "checkpoint".into(),
                snapshot_id: Some("aligned".into()),
                client_id: None,
            });
            store_c.put_pipeline(&name_c, &spec, Some(&row.etag))?;
            Ok(())
        })
        .await
        .map_err(ApiError::from)?;
    // Start the latest revision — the one that contains restore=checkpoint (P0-3).
    request_start(&state.store, &name, &actor).map_err(ApiError::from)?;
    state.supervisor.wake();
    let mut body = status_body(&state, &name)?;
    if let Value::Object(map) = &mut body {
        map.insert(
            "note".into(),
            json!("restore requested; supervisor starts the revision that contains restore=checkpoint. Not exactly-once."),
        );
    }
    Ok(Json(body))
}

async fn kill_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    request_stop(&state.store, &name, &actor).map_err(ApiError::from)?;
    state.supervisor.kill_named(&name).await.map_err(ApiError::from)?;
    state.supervisor.wake();
    Ok(Json(status_body(&state, &name)?))
}

async fn stop_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    request_stop(&state.store, &name, &actor).map_err(ApiError::from)?;
    state.supervisor.wake();
    Ok(Json(status_body(&state, &name)?))
}

#[derive(Deserialize)]
struct AllowBody {
    host: String,
    port: u16,
}

async fn put_allow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<AllowBody>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    state.store.put_allow(&body.host, body.port).map_err(ApiError::from)?;
    state
        .store
        .audit(&actor, "allowlist", Some(&format!("{}:{}", body.host, body.port)), None, "ok")
        .map_err(ApiError::from)?;
    Ok(Json(json!({"host": body.host, "port": body.port})))
}

#[derive(Deserialize)]
struct SecretBody {
    value: String,
}

async fn put_secret(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<SecretBody>,
) -> ApiResult<StatusCode> {
    let actor = require_auth(&state, &headers)?;
    if body.value.len() > 4096 {
        return Err(ApiError::from(SparrowError::new(
            ErrorCode::MaxRecordSize,
            "secret value exceeds 4KiB",
        )));
    }
    state.store.put_secret(&name, &body.value).map_err(ApiError::from)?;
    state
        .store
        .audit(&actor, "put_secret", Some(&name), Some("value redacted"), "ok")
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let rows = state.store.list_audit(50).map_err(ApiError::from)?;
    Ok(Json(json!({
        "audit": rows.iter().map(|r| json!({
            "id": r.id,
            "at_ms": r.at_ms,
            "actor": r.actor,
            "action": r.action,
            "target": r.target,
            "detail": r.detail,
            "outcome": r.outcome,
        })).collect::<Vec<_>>(),
        "cap": 200,
    })))
}

async fn demo_io(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let Some(d) = state.supervisor.demo_endpoints() else {
        return Err(ApiError::from(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "demo I/O is not enabled (pass --demo-io)",
        )));
    };
    Ok(Json(json!({
        "mqtt_host": d.mqtt_host,
        "mqtt_port": d.mqtt_port,
        "http_url": d.http_url,
        "http_port": d.http_port,
    })))
}

async fn demo_publish(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let Some(demo) = state.supervisor.demo() else {
        return Err(ApiError::from(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "demo I/O is not enabled",
        )));
    };
    demo.publish_fixture().await.map_err(ApiError::from)?;
    Ok(Json(json!({"published": 6, "topic": "sensors/json"})))
}

async fn demo_capture(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let Some(demo) = state.supervisor.demo() else {
        return Err(ApiError::from(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "demo I/O is not enabled",
        )));
    };
    Ok(Json(json!({"bodies": demo.http.body_strings()})))
}

pub async fn boot(
    store: Arc<Store>,
    kernel: Arc<Kernel>,
    token: String,
    safe_mode: bool,
    demo_io: bool,
) -> Result<(AppState, Option<Arc<DemoHarness>>), SparrowError> {
    let demo = if demo_io {
        let h = Arc::new(DemoHarness::start().await?);
        store.put_allow(&h.broker.host(), h.broker.port())?;
        store.put_allow("127.0.0.1", h.http.port())?;
        Some(h)
    } else {
        None
    };
    let supervisor = Supervisor::new(Arc::clone(&store), kernel, safe_mode, demo.clone())?;
    let state = AppState {
        store,
        supervisor: Arc::clone(&supervisor),
        token: Arc::new(token),
        safe_mode,
    };
    tokio::spawn(supervisor.run_loop());
    Ok((state, demo))
}

pub async fn serve(state: AppState, addr: SocketAddr) -> Result<(), SparrowError> {
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        SparrowError::new(ErrorCode::Internal, format!("bind {addr}: {e}"))
    })?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("server: {e}")))
}

pub async fn wait_status(
    state: &AppState,
    name: &str,
    actual: &str,
    timeout: Duration,
) -> Result<(), SparrowError> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(a) = state.store.actual(name) {
            if a.status == actual {
                return Ok(());
            }
            if a.status == "failed" && actual != "failed" {
                return Err(SparrowError::new(
                    ErrorCode::JobFailed,
                    a.last_error.unwrap_or_else(|| "pipeline failed".into()),
                ));
            }
        }
        if start.elapsed() > timeout {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                format!("timeout waiting for actual={actual}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

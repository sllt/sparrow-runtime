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
#[cfg(feature = "demo-io")]
use sparrow_control::DemoHarness;
use sparrow_control::{
    binder_catalog, capabilities_json, explain_plan_with, honesty_json, request_start_at,
    request_stop, stream_schema, validate_aligned_plan, DemoIo, PipelineSpec, RestoreSpec, Store,
    StreamSpec, Supervisor, HONESTY,
};
use sparrow_model::{ErrorCode, SparrowError};
use sparrow_runtime::Kernel;
use tower_http::limit::RequestBodyLimitLayer;
mod operations;
mod outbox;
mod input_recovery;
mod plugins;
mod reference_tables;
pub mod auth;
pub mod ui;
mod authoring;
pub use auth::{Auth, Principal, Role};
pub use ui::UiAssets;

pub const DEFAULT_BIND: &str = "127.0.0.1:43180";
pub const MAX_BODY: usize = 64 * 1024;
static API_WORK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(64);
static API_REQUESTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(128);

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub supervisor: Arc<Supervisor>,
    /// Authentication/authorization config. Named `token` for source
    /// compatibility: `Arc::new("tok".into())` builds legacy single-token admin.
    pub token: Arc<Auth>,
    pub safe_mode: bool,
}

pub fn router(state: AppState) -> Router {
    build_router(state, None)
}

fn build_router(state: AppState, ui_assets: Option<Arc<UiAssets>>) -> Router {
    let ui_public = ui_assets.is_some();
    let base: Router<AppState> = match ui_assets {
        Some(assets) => ui::router(assets),
        None => Router::new(),
    };
    base
        .route("/", get(root))
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/validate", post(validate))
        .route("/v1/explain", post(explain))
        .route("/v1/graphs/validate", post(graph_validate))
        .route("/v1/graphs/explain", post(graph_explain))
        .route("/v1/test", post(test_plan))
        .route("/v1/query", post(query))
        .route("/v1/streams", get(list_streams))
        .route("/v1/streams/{name}", put(put_stream).get(get_stream))
        .route("/v1/tables", get(reference_tables::list_tables))
        .route(
            "/v1/tables/{name}",
            put(reference_tables::put_table).get(reference_tables::get_latest_table),
        )
        .route(
            "/v1/tables/{name}/revisions",
            get(reference_tables::list_table_revisions),
        )
        .route(
            "/v1/tables/{name}/revisions/{revision}",
            get(reference_tables::get_table_revision),
        )
        .route(
            "/v1/tables/{name}/dependencies",
            get(reference_tables::table_dependencies),
        )
        .route("/v1/tables/{name}/gc", post(reference_tables::gc_table))
        .route("/v1/tables/{name}/mutate", post(reference_tables::mutate_table))
        .route("/v1/tables/{name}/rollback", post(reference_tables::rollback_table))
        .route("/v1/pipelines", get(list_pipelines))
        .route("/v1/pipelines/{name}", put(put_pipeline).get(get_pipeline))
        .route("/v1/pipelines/{name}/status", get(pipeline_status))
        .route("/v1/pipelines/{name}/start", post(start_pipeline))
        .route("/v1/pipelines/{name}/stop", post(stop_pipeline))
        .route("/v1/pipelines/{name}/checkpoint", post(checkpoint_pipeline))
        .route("/v1/pipelines/{name}/outbox", get(outbox::status))
        .route("/v1/pipelines/{name}/outbox/entries", get(outbox::entries))
        .route("/v1/pipelines/{name}/outbox/entries/{id}", get(outbox::entry))
        .route("/v1/pipelines/{name}/outbox/command", post(outbox::command))
        .route("/v1/pipelines/{name}/input-dlq", get(input_recovery::status))
        .route("/v1/pipelines/{name}/input-dlq/entries", get(input_recovery::entries))
        .route("/v1/pipelines/{name}/input-dlq/entries/{position}", get(input_recovery::entry))
        .route("/v1/pipelines/{name}/input-dlq/purge", post(input_recovery::purge))
        .route("/v1/pipelines/{name}/recovery/preview", post(input_recovery::preview))
        .route("/v1/pipelines/{name}/recovery/execute", post(input_recovery::execute))
        .route("/v1/pipelines/{name}/recovery/operations", get(input_recovery::operations))
        .route("/v1/pipelines/{name}/recovery/operations/{id}", get(input_recovery::operation))
        .route("/v1/pipelines/{name}/recovery/operations/{id}/finish", post(input_recovery::finish))
        .route("/v1/pipelines/{name}/recovery/operations/{id}/abort", post(input_recovery::abort))
        .route(
            "/v1/pipelines/{name}/checkpoints",
            get(operations::checkpoints),
        )
        .route("/v1/pipelines/{name}/diagnose", get(operations::diagnose))
        .route("/v1/pipelines/{name}/restore", post(restore_pipeline))
        .route("/v1/pipelines/{name}/kill", post(kill_pipeline))
        .route("/v1/allowlist", put(put_allow))
        .route("/v1/secrets/{name}", put(put_secret))
        .route("/v1/audit", get(list_audit))
        .route("/v1/auth/me", get(auth_me))
        .route("/v1/metrics", get(metrics))
        .route("/v1/demo/io", get(demo_io))
        .route("/v1/demo/publish-fixture", post(demo_publish))
        .route("/v1/demo/capture", get(demo_capture))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(RequestBodyLimitLayer::new(MAX_BODY))
        .merge(plugins::router())
        .merge(authoring::router())
        .layer(axum::middleware::from_fn_with_state(
            (state.clone(), ui_public),
            management_guard,
        ))
        .with_state(state)
}

async fn management_guard(
    State((state, ui_public)): State<(AppState, bool)>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path();
    if ui_public && (path == "/ui" || path.starts_with("/ui/")) {
        // Static workbench assets are anonymous; they carry their own
        // security headers. No management data is served under /ui/.
        return next.run(request).await;
    }
    let mut request = request;
    if !matches!(request.uri().path(), "/" | "/v1/health") {
        let principal = match authenticate(&state, request.headers()) {
            Ok(p) => p,
            Err(error) => return no_store(error.into_response()),
        };
        let (action, needed) = auth::required(request.method(), request.uri().path());
        if principal.role < needed {
            // Authorization failures, like authentication failures, do not touch SQLite.
            return no_store(
                ApiError {
                    status: StatusCode::FORBIDDEN,
                    err: SparrowError::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "forbidden: role {} may not perform {action} (requires {})",
                            principal.role.as_str(),
                            needed.as_str()
                        ),
                    ),
                }
                .into_response(),
            );
        }
        request.extensions_mut().insert(principal);
    }
    let read_only = matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    );
    if state.supervisor.is_draining() && !read_only {
        return ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            err: SparrowError::new(
                ErrorCode::Cancelled,
                "server is draining; mutation not accepted",
            )
            .retryable(true),
        }
        .into_response();
    }
    let Ok(_permit) = API_REQUESTS.try_acquire() else {
        return ApiError::from(
            SparrowError::new(
                ErrorCode::ResourceExhausted,
                "management request capacity exhausted",
            )
            .retryable(true),
        )
        .into_response();
    };
    match tokio::time::timeout(Duration::from_secs(125), next.run(request)).await {
        Ok(response) => no_store(response),
        Err(_) => ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            err: SparrowError::new(
                ErrorCode::ResourceExhausted,
                "management deadline exceeded; operation outcome may require status inspection",
            ),
        }
        .into_response(),
    }
}

#[derive(Debug)]
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
            ErrorCode::UnsupportedDelivery
            | ErrorCode::UnsupportedRestore
            | ErrorCode::FeatureUnavailable => StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::MaxRecordSize | ErrorCode::BoundExceeded => StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::Cancelled => StatusCode::CONFLICT,
            ErrorCode::Conflict => StatusCode::PRECONDITION_FAILED,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self { status, err }
    }
}

fn public_error_context(error: &SparrowError) -> Vec<Value> {
    const ALLOWED: &[&str] = &[
        "operator",
        "field",
        "pipeline",
        "job_attempt",
        "checkpoint_outcome",
        "admission",
        "phase",
        "scope",
        "snapshot_id",
        "revision",
        "expected_revision",
        "current_etag",
        "etag",
        "current_revision",
        "latest_revision",
        "table",
        "pins",
        "max_keys",
        "max_bytes",
        "max_rows",
        "max_timers",
    ];
    error
        .context
        .iter()
        .filter(|(key, value)| {
            ALLOWED.contains(&key.as_str())
                || match key.as_str() {
                    "plugin_package" | "plugin_version" | "plugin_function" => {
                        value.len() <= 32
                            && !value.is_empty()
                            && value
                                .bytes()
                                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
                    }
                    "script_phase" => matches!(
                        value.as_str(),
                        "compile"
                            | "initialize"
                            | "export"
                            | "arguments"
                            | "call"
                            | "result"
                            | "cache"
                            | "runtime"
                    ),
                    "script_frames" => {
                        value.len() <= 128
                            && !value.is_empty()
                            && value
                                .bytes()
                                .all(|b| b.is_ascii_digit() || b == b':' || b == b',')
                    }
                    _ => false,
                }
        })
        .take(8)
        .map(|(key, value)| {
            json!({
                "key": key,
                "value": value.chars().take(128).collect::<String>(),
            })
        })
        .collect()
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = honesty_json();
        if let Value::Object(map) = &mut body {
            let context = public_error_context(&self.err);
            map.insert(
                "error".into(),
                json!({
                    "code": self.err.code.as_str(),
                    "message": self.err.message,
                    "retryable": self.err.retryable,
                    "context": context,
                }),
            );
        }
        (self.status, Json(body)).into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

/// SQLite and synchronous plan/config work must not block Axum workers.
async fn blocking_api<T, F>(f: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> ApiResult<T> + Send + 'static,
{
    let permit = API_WORK.try_acquire().map_err(|_| {
        ApiError::from(
            SparrowError::new(
                ErrorCode::ResourceExhausted,
                "management worker capacity exhausted",
            )
            .retryable(true),
        )
    })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .map_err(|e| {
        ApiError::from(SparrowError::new(
            ErrorCode::Internal,
            format!("API worker: {e}"),
        ))
    })?
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

fn authenticate(state: &AppState, headers: &HeaderMap) -> ApiResult<Principal> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = raw.strip_prefix("Bearer ").unwrap_or("");
    // P1-29: do not touch SQLite on auth failure (sync audit is a DoS / wipe vector).
    state.token.authenticate(token).ok_or_else(|| ApiError {
        status: StatusCode::UNAUTHORIZED,
        err: SparrowError::new(ErrorCode::PolicyDenied, "unauthorized: bearer token required"),
    })
}

/// Returns the authenticated actor. Role checks happen centrally in
/// `management_guard`; handlers use this for the audit identity.
fn require_auth(state: &AppState, headers: &HeaderMap) -> ApiResult<String> {
    authenticate(state, headers).map(|p| p.actor)
}

async fn auth_me(
    axum::Extension(p): axum::Extension<Principal>,
    State(state): State<AppState>,
) -> Json<Value> {
    Json(json!({
        "actor": p.actor,
        "role": p.role.as_str(),
        "allowed_actions": auth::allowed_actions(p.role),
        "auth_mode": state.token.mode(),
        "safe_mode": state.safe_mode,
        "draining": state.supervisor.is_draining(),
        "status_projection": if p.role == Role::Viewer { "viewer_safe" } else { "full" },
    }))
}

/// Server-side viewer projection: the same allowlist as the diagnostic
/// bundle. No spec, SQL, destinations, SecretRefs or free-form error text.
fn viewer_status(body: &Value) -> Value {
    json!({
        "name": body["name"],
        "revision": body["revision"],
        "projection": "viewer_safe",
        "safe_mode": body["safe_mode"],
        "desired": body["desired"],
        "actual": if body["actual"].is_null() { Value::Null } else { json!({
            "revision": body["actual"]["revision"],
            "status": body["actual"]["status"],
            "attempt_id": body["actual"]["attempt_id"],
            "consecutive_failures": body["actual"]["consecutive_failures"],
            "restart_blocked": body["actual"]["restart_blocked"],
            "has_error": !body["actual"]["last_error"].is_null(),
        })},
        "delivery": body["delivery"],
        "replay": body["replay"],
        "effective": {"aligned_eligible": body["effective"]["aligned_eligible"], "recovery": body["effective"]["recovery"]},
        "observation": body["observation"],
        "mailboxes": body["mailboxes"],
        "checkpoint": body["checkpoint"],
        "histogram_contract": body["histogram_contract"],
        "redaction": "allowlisted_fields_no_raw_spec_sql_destinations_secrets_or_free_form_errors",
    })
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
            "POST /v1/query",
            "POST /v1/explain",
            "POST /v1/graphs/validate",
            "POST /v1/graphs/explain",
            "PUT /v1/streams/{name}",
            "GET /v1/tables",
            "PUT /v1/tables/{name}",
            "GET /v1/tables/{name}",
            "GET /v1/tables/{name}/revisions/{revision}",
            "GET /v1/tables/{name}/dependencies",
            "POST /v1/tables/{name}/gc",
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

async fn validate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    blocking_api(move || {
        let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
        let report = run_validate(&state, &spec)?;
        let _ = state
            .store
            .audit(&actor, "validate", None, Some("sql_not_recorded"), "ok");
        Ok(Json(report))
    })
    .await
}

async fn explain(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    blocking_api(move || {
        let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
        Ok(Json(run_explain(&state, &spec)?))
    })
    .await
}

async fn test_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    blocking_api(move || {
        let spec = PipelineSpec::from_json(&body).map_err(ApiError::from)?;
        let mut out = run_validate(&state, &spec)?;
        if let Value::Object(map) = &mut out {
            map.insert("explain".into(), run_explain(&state, &spec)?);
            map.insert("started".into(), json!(false));
            map.insert("note".into(), json!("test does not start I/O"));
        }
        Ok(Json(out))
    })
    .await
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    require_auth(&state, &headers)?;
    let output = sparrow_control::query::execute(state.store.clone(), body.to_vec())
        .await
        .map_err(ApiError::from)?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        Bytes::from_owner(output),
    )
        .into_response())
}

fn run_validate(state: &AppState, spec: &PipelineSpec) -> ApiResult<Value> {
    spec.check_delivery().map_err(ApiError::from)?;
    let stream = state
        .store
        .get_stream(&spec.stream)
        .map_err(ApiError::from)?;
    let schema = stream_schema(
        &stream.name,
        &serde_json::from_str::<StreamSpec>(&stream.schema_json).map_err(|e| {
            ApiError::from(SparrowError::new(ErrorCode::InvalidSchema, e.to_string()))
        })?,
    )
    .map_err(ApiError::from)?;
    let plan = sparrow_control::validate::bind_plan_with_store(
        &state.store,
        spec,
        spec.stream.as_str(),
        0,
    )
    .map_err(ApiError::from)?;
    validate_aligned_plan(spec, &plan).map_err(ApiError::from)?;
    let demo = state.supervisor.demo_endpoints();
    let policy = sparrow_control::validate::store_policy(&state.store, demo.as_ref())
        .map_err(ApiError::from)?;
    let secrets = sparrow_control::validate::StoreSecrets::new(Arc::clone(&state.store));
    sparrow_control::validate::validate_io_with_plan(
        spec,
        &schema,
        &plan,
        &secrets,
        &policy,
        demo.as_ref(),
    )
    .map_err(ApiError::from)?;
    let mut body = honesty_json();
    if let Value::Object(map) = &mut body {
        map.insert("accepted".into(), json!(true));
        map.insert("stream".into(), json!(spec.stream));
        map.insert(
            "effective".into(),
            sparrow_control::effective_guarantees_with_plan(spec, &plan),
        );
    }
    Ok(body)
}

fn run_explain(state: &AppState, spec: &PipelineSpec) -> ApiResult<Value> {
    let plan = sparrow_control::validate::bind_plan_with_store(
        &state.store,
        spec,
        spec.stream.as_str(),
        0,
    )
    .map_err(ApiError::from)?;
    validate_aligned_plan(spec, &plan).map_err(ApiError::from)?;
    let recovery = spec
        .check_delivery()
        .map(|(_, r)| r)
        .unwrap_or(sparrow_model::RecoveryPolicy::RestartFresh);
    let replay = sparrow_control::validate::replay_label_for_spec(spec);
    let mut e = explain_plan_with(&plan, recovery, replay);
    if spec.source.kind == "jetstream" {
        e.delivery = "checkpointed_at_least_once";
        e.guarantee="JetStream replay + HTTP 2xx acceptance + durable checkpoint before ACK; not business exactly-once".into();
        e.experimental = true;
    }
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
        "effective": sparrow_control::effective_guarantees_with_plan(spec, &plan),
        "checkpoint_policy": spec.checkpoint,
        "checkpoint_warnings": spec.checkpoint_warnings(),
    }))
}

async fn graph_validate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    blocking_api(move || {
        let text = std::str::from_utf8(&body).map_err(|_| {
            ApiError::from(SparrowError::new(
                ErrorCode::InvalidArgument,
                "GraphSpec must be UTF-8 JSON",
            ))
        })?;
        let spec = sparrow_plan::GraphSpec::from_json(text).map_err(ApiError::from)?;
        let catalog = binder_catalog(&state.store).map_err(ApiError::from)?;
        let bound = sparrow_plan::validate_graph(&spec, &catalog).map_err(ApiError::from)?;
        Ok(Json(json!({
            "accepted": true,
            "nodes": bound.nodes.len(),
            "honesty": HONESTY,
        })))
    })
    .await
}

async fn graph_explain(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let _ = require_auth(&state, &headers)?;
    blocking_api(move || {
        let text = std::str::from_utf8(&body).map_err(|_| {
            ApiError::from(SparrowError::new(
                ErrorCode::InvalidArgument,
                "GraphSpec must be UTF-8 JSON",
            ))
        })?;
        let spec = sparrow_plan::GraphSpec::from_json(text).map_err(ApiError::from)?;
        let catalog = binder_catalog(&state.store).map_err(ApiError::from)?;
        let report = sparrow_plan::explain_graph(&spec, &catalog).map_err(ApiError::from)?;
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
    })
    .await
}

async fn put_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let actor = require_auth(&state, &headers)?;
    blocking_api(move || {
        let spec = StreamSpec::from_json(&body).map_err(ApiError::from)?;
        let _ = stream_schema(&name, &spec).map_err(ApiError::from)?;
        let json = serde_json::to_string(&spec).map_err(|e| {
            ApiError::from(SparrowError::new(ErrorCode::InvalidArgument, e.to_string()))
        })?;
        // K5.2: with If-Match the check and write share one transaction
        // ("absent" = create only). Without it the legacy unconditional
        // upsert is kept for CLI/API compatibility; the workbench always sends it.
        let etag = match if_match(&headers) {
            Some(want) => state.store.put_stream_if(&name, &json, &want).map_err(ApiError::from)?,
            None => {
                state.store.put_stream(&name, &json).map_err(ApiError::from)?;
                Store::stream_etag(&json)
            }
        };
        state
            .store
            .audit(&actor, "put_stream", Some(&name), None, "ok")
            .map_err(ApiError::from)?;
        Ok((
            StatusCode::CREATED,
            Json(json!({"name": name, "fields": spec.fields, "etag": etag})),
        ))
    })
    .await
}

async fn get_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let row = state.store.get_stream(&name).map_err(ApiError::from)?;
        let spec: Value = serde_json::from_str(&row.schema_json).unwrap_or(Value::Null);
        Ok(Json(json!({"name": row.name, "schema": spec, "etag": Store::stream_etag(&row.schema_json)})))
    })
    .await
}

async fn list_streams(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let names: Vec<String> = state
            .store
            .list_streams()
            .map_err(ApiError::from)?
            .into_iter()
            .map(|s| s.name)
            .collect();
        Ok(Json(json!({"streams": names})))
    })
    .await
}

async fn put_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let actor = require_auth(&state, &headers)?;
    blocking_api(move || {
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
    })
    .await
}

async fn get_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || Ok(Json(status_body(&state, &name)?))).await
}

async fn pipeline_status(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let body = status_body(&state, &name)?;
        Ok(Json(if p.role == Role::Viewer { viewer_status(&body) } else { body }))
    })
    .await
}

fn reference_tables_status(
    state: &AppState,
    name: &str,
    latest: &sparrow_control::PipelineRow,
    actual: Option<&sparrow_control::ActualState>,
) -> Value {
    let latest_bindings =
        serde_json::to_value(&latest.spec.reference_tables).unwrap_or_else(|_| json!({}));
    let running = match actual {
        Some(value) if value.status == "running" => match value.revision {
            None => json!({
                "available": false,
                "reason": "running_without_revision",
            }),
            // Most status polls describe the same immutable revision we have
            // already loaded above. Avoid another SQLite read/spec decode on
            // the default non-Lookup path and on ordinary reference Jobs.
            Some(revision) if revision == latest.latest_revision => json!({
                "available": true,
                "revision": revision,
                "bindings": latest_bindings.clone(),
            }),
            Some(revision) => match state.store.get_pipeline_revision(name, revision) {
                Ok(row) => json!({
                    "available": true,
                    "revision": revision,
                    "bindings": serde_json::to_value(&row.spec.reference_tables)
                        .unwrap_or_else(|_| json!({})),
                }),
                Err(_) => json!({
                    "available": false,
                    "revision": revision,
                    "reason": "running_revision_not_readable",
                }),
            },
        },
        Some(value) => json!({
            "available": false,
            "revision": value.revision,
            "status": value.status,
            "reason": "actual_not_running",
        }),
        None => json!({"available": false, "reason": "no_actual_state"}),
    };
    json!({
        "stored_latest": {
            "available": true,
            "revision": latest.latest_revision,
            "bindings": latest_bindings,
        },
        "running_actual": running,
    })
}

fn status_body(state: &AppState, name: &str) -> ApiResult<Value> {
    let (row, mut effective) = state
        .store
        .effective_pipeline_status(name)
        .map_err(ApiError::from)?;
    let desired = state.store.desired(name).ok();
    let actual = state.store.actual(name).ok();
    let reference_tables = reference_tables_status(state, name, &row, actual.as_ref());
    // Describe the stored latest revision, not a possibly older running attempt.
    // Failed binding must not make status unreadable or claim eligibility.
    effective["scope"] = json!("stored_latest_revision");
    effective["revision"] = json!(row.latest_revision);
    let mailboxes = match state.supervisor.mailbox_snapshot(name) {
        Ok(Some(snapshot)) => mailbox_snapshot_json(&snapshot),
        Ok(None) => {
            json!({"available": false, "scope": "runtime_mailboxes", "reason": "no_active_attempt"})
        }
        Err(_) => {
            json!({"available": false, "scope": "runtime_mailboxes", "reason": "registry_busy"})
        }
    };
    let observation = match state.supervisor.flow_snapshot(name) {
        Ok(Some(s)) => flow_snapshot_json(&s),
        Ok(None) => json!({"available":false,"reason":"no_active_attempt"}),
        Err(_) => json!({"available":false,"reason":"registry_busy"}),
    };
    Ok(json!({
        "name": row.name,
        "revision": row.latest_revision,
        "etag": row.etag,
        "spec": row.spec,
        "reference_tables": reference_tables,
        "lookup_runtime": state.supervisor.lookup_snapshot(name).unwrap_or_else(|_|json!({"available":false,"reason":"registry_busy"})),
        "safe_mode": state.safe_mode,
        "desired": desired.map(|d| json!({"revision": d.revision, "status": d.status})),
        "actual": actual.map(|a| json!({
            "revision": a.revision,
            "status": a.status,
            "attempt_id": a.attempt_id,
            "consecutive_failures": a.consecutive_failures,
            "restart_blocked": a.restart_blocked,
            "last_error": a.last_error,
        })),
        "delivery": effective["delivery"],
        "recovery": row.spec.recovery.clone(),
        "replay": sparrow_control::validate::replay_label_for_spec(&row.spec),
        "honesty": HONESTY,
        "effective": effective,
        "mailboxes": mailboxes,
        "observation": observation,
        "histogram_contract": histogram_contract_json(),
        "checkpoint": operations::checkpoint_status(state, name),
    }))
}

fn queue_depth_json(depth: &sparrow_runtime::mailbox_observe::QueueDepth) -> Value {
    json!({"items": depth.items, "data_batches": depth.data_batches, "rows": depth.rows,
        "controls": depth.controls, "accounted_bytes": depth.accounted_bytes})
}

fn histogram_json(h: &sparrow_model::observation::HistogramSnapshot) -> Value {
    json!({"samples":h.count,"sum_us":h.sum_us,"max_us":if h.count>0{Some(h.max_us)}else{None},
        "buckets":h.buckets,
        "p50_upper_us":h.percentile_upper_us(50),"p95_upper_us":h.percentile_upper_us(95),"p99_upper_us":h.percentile_upper_us(99)})
}
fn histogram_contract_json() -> Value {
    json!({"bucket_upper_us":(0..32).map(|i|if i<31{Some(1u64<<i)}else{None}).collect::<Vec<_>>(),
        "quantile_min_samples":100,"scope":"attempt_cumulative","quantile_kind":"bucket_upper_bound","overflow_is_unbounded":true})
}
fn endpoint_json(e: &sparrow_model::observation::EndpointSnapshot) -> Value {
    json!({"state":e.state.as_str(),"reason":e.reason,"changed_age_us":e.changed_age_us,
        "last_progress_age_us":e.progress_age_us,"progress_units_total":e.progress_units,"failures_total":e.failures,
        "last_error_code":e.last_error_code.map(|c|c.as_str()),"silence_is_failure":false})
}
fn boundary_queue_json(q: Option<&std::sync::Arc<sparrow_io::observed::QueueObserver>>) -> Value {
    let Some(q) = q else {
        return json!({"available":false,"reason":"unobserved_channel"});
    };
    let Some(s) = q.snapshot() else {
        return json!({"available":false,"reason":"initializing"});
    };
    json!({"available":true,"capacity_items":s.capacity,"queued_items":s.items,"queued_rows":s.rows,
        "queued_logical_bytes":s.bytes,"peak_items":s.peak_items,"peak_logical_bytes":s.peak_bytes,
        "metadata_bytes":q.metadata_bytes(),"oldest_queued_age_us":s.oldest_age_us,"receiver_open":s.receiver_open,
        "enqueued_total":s.enqueued,"received_total":s.received,"discarded_on_close_total":s.discarded,
        "waiting_senders":s.waiting,"capacity_waits_total":s.waits,"aborted_waits_total":s.aborted_waits,
        "residence_sample_every":sparrow_io::observed::RESIDENCE_SAMPLE_EVERY,
        "residence":histogram_json(&s.residence),"capacity_wait":histogram_json(&s.capacity_wait),
        "bytes_are_rss":false,"age_origin":"channel_publish","controls_included":true})
}
fn flow_snapshot_json(s: &sparrow_control::supervisor::PipelineFlowSnapshot) -> Value {
    let diag = &s.diagnostics;
    let Some((source, sink)) = diag.observation.endpoints() else {
        return json!({"available":false,"reason":"initializing"});
    };
    let source_queue = boundary_queue_json(diag.source_queue.get());
    let sink_queue = boundary_queue_json(diag.sink_queue.get());
    let io = s
        .graph_ports
        .as_ref()
        .map_or_else(|| diag.snapshot(), |ports| ports.snapshot());
    let delivery = diag.observation.delivery().unwrap_or_default();
    let mut reasons = Vec::new();
    if s.cancel_requested {
        reasons.push("cancellation_requested");
    }
    if s.finished {
        reasons.push("execution_ended");
    }
    if matches!(
        source.state,
        sparrow_model::observation::HealthState::Connecting
            | sparrow_model::observation::HealthState::Reconnecting
            | sparrow_model::observation::HealthState::Failed
    ) {
        reasons.push("source_connection_or_failure");
    }
    if matches!(
        sink.state,
        sparrow_model::observation::HealthState::Reconnecting
            | sparrow_model::observation::HealthState::Failed
    ) {
        reasons.push("sink_retry_or_failure");
    }
    if source_queue["queued_items"].as_u64().unwrap_or(0) > 0 {
        reasons.push("source_queue_nonempty");
    }
    if sink_queue["queued_items"].as_u64().unwrap_or(0) > 0 {
        reasons.push("sink_queue_nonempty");
    }
    if io.mqtt_pending_bytes > 0 {
        reasons.push("source_admission_pending");
    }
    if io.http_inflight > 0 {
        reasons.push("http_delivery_inflight");
    }
    if delivery.failed_groups > 0 {
        reasons.push("delivery_failures_recorded_this_attempt");
    }
    // No arbitrary inactivity threshold: a quiet source may be perfectly fine.
    if reasons.is_empty() {
        reasons.push("no_current_pressure_evidence");
    }
    let latency: serde_json::Map<String, Value> = diag
        .observation
        .histograms()
        .into_iter()
        .map(|(n, h)| (n.into(), histogram_json(&h)))
        .collect();
    let mut value = json!({"available":true,"running_revision":s.running_revision,"runtime_attempt_id":s.runtime_attempt_id,
        "scope":"active_runtime_attempt","retention":"active_handles_only","clock":"process_monotonic",
        "snapshot_consistency":"per_component_not_cross_pipeline_atomic",
        "source_kind":s.source_kind,"sink_kind":s.sink_kind,"source":endpoint_json(&source),"sink":endpoint_json(&sink),
        "execution_finished":s.finished,"cancel_requested":s.cancel_requested,"diagnosis_reasons":reasons,
        "source_inbox":source_queue,"sink_outbox":sink_queue,
        "source_pending_accounted_bytes":if s.source_kind=="mqtt"{Some(io.mqtt_pending_bytes)}else{None},
        "runtime_progress":{"ingested_rows":delivery.runtime_ingested_rows,"emitted_rows":delivery.runtime_emitted_rows,
            "transform_filtered_rows":delivery.transform_filtered_rows,"scope":"this_attempt_only_not_external_ack",
            "snapshot_consistency":"independently_sampled_monotonic_counters"},
        "legacy_io_counters":{"scope":"this_attempt_both_source_and_sink_roles_not_source_only",
            "mqtt_received":io.mqtt_received,"mqtt_decoded":io.mqtt_decoded,"mqtt_bad":io.mqtt_dropped_bad,
            "mqtt_full":io.mqtt_dropped_full,"mqtt_budget":io.mqtt_dropped_budget,"mqtt_oversize":io.mqtt_dropped_oversize,
            "http_posted":io.http_posted,"http_failed":io.http_failed,"http_encode_errors":io.http_encode_errors,
            "http_budget_drops":io.http_budget_drops,"http_dropped":io.http_dropped,"http_retries":io.http_retries,
            "decode_errors":io.decode_errors,"budget_and_full_may_overlap":true},
        "http_poll":if s.source_kind=="http_poll"{Some(json!({"requests":io.http_poll_requests,"ok":io.http_poll_ok,
            "not_modified":io.http_poll_not_modified,"failed":io.http_poll_failed,"timeouts":io.http_poll_timeouts,
            "status_errors":io.http_poll_status_errors,"oversize":io.http_poll_oversize,"bad_responses":io.http_poll_bad_responses,
            "rows":io.http_poll_rows,"dropped_bad":io.http_poll_dropped_bad,"dropped_oversize":io.http_poll_dropped_oversize,
            "dropped_budget":io.http_poll_dropped_budget,"skipped_ticks":io.http_poll_skipped_ticks,
            "backpressure_waits":io.http_poll_backpressure_waits,"inflight":io.http_poll_inflight,
            "inbox_items":io.http_poll_inbox_items,"inbox_bytes":io.http_poll_inbox_bytes,
            "scope":"this_attempt; rows=admitted_to_volatile_inbox_not_delivery_receipt"}))}else{None},
        "http_active_delivery_groups":io.http_inflight,
        "delivery":{"active_input_batches":delivery.active_groups,"active_rows":delivery.active_rows,
            "encoded_credit_bytes":delivery.encoded_credit_bytes,"peak_encoded_credit_bytes":delivery.peak_encoded_credit_bytes,
            "active_http_requests":delivery.active_http_requests,"http_attempts_started_total":delivery.http_attempts_started,
            "http_attempts_finished_total":delivery.http_attempts_finished,"http_attempts_cancelled_total":delivery.http_attempts_cancelled,
            "active_input_accounted_bytes":delivery.active_bytes,"peak_input_batches":delivery.peak_groups,"peak_input_accounted_bytes":delivery.peak_bytes,
            "dequeued_batches_total":delivery.accepted_groups,"dequeued_rows_total":delivery.accepted_rows,
            "completed_batches_total":delivery.completed_groups,"completed_rows_total":delivery.completed_rows,
            "failed_or_cancelled_batches_total":delivery.failed_groups,"failed_or_cancelled_rows_total":delivery.failed_rows,
            "unknown_origin_batches_total":delivery.unknown_origin_groups,"http_incomplete_response_bodies_total":delivery.body_incomplete},
        "latency":latency,
        "latency_contract":{"http_completion":"2xx_headers_then_bounded_body_drain_attempt_not_business_ack",
            "mqtt_completion":"qos0_socket_write_not_broker_ack","log_completion":"local_write_attempt_not_durable",
            "samples":"operation_or_coalesced_delivery_group_not_per_row; failures_included_except_http_body_completed_only",
            "local_origin":if s.source_kind=="file"{"decoded_file_batch_ready"}else{"complete_local_request_or_publish_before_decode"},
            "origin_propagation":"conservative_input_batch_bounds; window_aggregate_outputs_unknown",
            "device_event_age_available":false,"broker_wait_available":false,"business_ack_available":false,
            "restored_monotonic_age_available":false,"payload_semantics_changed":false}});
    if diag.durable_outbox_enabled.load(std::sync::atomic::Ordering::Relaxed) {
        value["delivery"]["outbox_persisted_batches"] = json!(io.outbox_persisted_batches);
        value["delivery"]["completion_basis"] = json!("local_outbox_FULL_commit_not_remote_HTTP_2xx");
        value["latency_contract"]["http_completion"] = json!("independent_sender; inspect_pipeline_outbox_endpoint_for_remote_delivery");
    }
    value["legacy_io_counters"]["input_quarantined"]=json!(io.input_quarantined);
    if s.source_kind == "nats" {
        value["nats_source"] = json!({"received":io.nats_source_received,"rows":io.nats_source_rows,
            "dropped_bad":io.nats_source_dropped_bad,"dropped_oversize":io.nats_source_dropped_oversize,
            "dropped_budget":io.nats_source_dropped_budget,"backpressure_waits":io.nats_source_backpressure_waits,
            "slow_consumer_events":io.nats_source_slow_consumer,"disconnects":io.nats_source_disconnects,
            "reconnects":io.nats_source_reconnects,"client_errors":io.nats_source_client_errors,
            "inbox_items":io.nats_source_inbox_items,"inbox_bytes":io.nats_source_inbox_bytes,
            "semantics":"live_best_effort_at_most_once_no_replay_no_ack",
            "scope":"this_attempt; slow_consumer_events=exact_local_prefetch_drops_not_total_network_loss"});
    }
    if s.sink_kind == "nats" {
        value["nats_sink"] = json!({"published":io.nats_sink_published,"failed":io.nats_sink_failed,
            "dropped_bad":io.nats_sink_dropped_bad,"dropped_oversize":io.nats_sink_dropped_oversize,
            "discarded_on_close":io.nats_sink_discarded_on_close,"flushes":io.nats_sink_flushes,
            "flush_failed":io.nats_sink_flush_failed,"disconnects":io.nats_sink_disconnects,
            "reconnects":io.nats_sink_reconnects,"client_errors":io.nats_sink_client_errors,
            "sessions":io.nats_sink_sessions,
            "semantics":"live_best_effort_at_most_once_no_broker_ack",
            "scope":"this_attempt; published=handed_to_client_not_broker_ack"});
    }
    if s.source_kind == "databus" {
        value["databus_source"] = json!({"received":io.databus_source_received,
            "rows":io.databus_source_rows,"dropped_bad":io.databus_source_dropped_bad,
            "dropped_oversize":io.databus_source_dropped_oversize,
            "dropped_budget":io.databus_source_dropped_budget,
            "dropped_oldest":io.databus_source_dropped_oldest,
            "dropped_newest":io.databus_source_dropped_newest,
            "block_timeouts":io.databus_source_block_timeouts,
            "backpressure_waits":io.databus_source_backpressure_waits,
            "discarded_on_close":io.databus_source_discarded_on_close,
            "buffer_items":io.databus_source_buffer_items,"buffer_bytes":io.databus_source_buffer_bytes,
            "subscriptions":io.databus_source_subscriptions,
            "inbox_items":io.databus_source_inbox_items,"inbox_bytes":io.databus_source_inbox_bytes,
            "semantics":"live_best_effort_at_most_once_in_process_no_replay",
            "scope":"this_attempt; only_messages_published_after_attach"});
    }
    if s.sink_kind == "databus" {
        value["databus_sink"] = json!({"published":io.databus_sink_published,
            "deliveries":io.databus_sink_deliveries,"no_subscribers":io.databus_sink_no_subscribers,
            "dropped_bad":io.databus_sink_dropped_bad,"dropped_oversize":io.databus_sink_dropped_oversize,
            "dropped_budget":io.databus_sink_dropped_budget,
            "blocked_publishes":io.databus_sink_blocked_publishes,
            "discarded_on_close":io.databus_sink_discarded_on_close,
            "batches":io.databus_sink_batches,"fatal":io.databus_sink_fatal,
            "semantics":"live_best_effort_at_most_once; done=offered_to_every_matching_subscriber",
            "scope":"this_attempt; deliveries=subscriber_buffer_accepts"});
    }
    if s.sink_kind == "influxdb" {
        value["influxdb_sink"] = json!({"rows_written":io.influxdb_sink_rows_written,"requests_ok":io.influxdb_sink_requests_ok,"bytes_sent":io.influxdb_sink_bytes_sent,"gzip_fallbacks":io.influxdb_sink_gzip_fallbacks,"retries":io.influxdb_sink_retries,"retry_after_waits":io.influxdb_sink_retry_after_waits,"retry_after_capped":io.influxdb_sink_retry_after_capped,"dropped_bad":io.influxdb_sink_dropped_bad,"dropped_oversize":io.influxdb_sink_dropped_oversize,"dropped_budget":io.influxdb_sink_dropped_budget,"rejected_requests":io.influxdb_sink_rejected_requests,"rejected_rows":io.influxdb_sink_rejected_rows,"partial_writes":io.influxdb_sink_partial_writes,"discarded_on_close":io.influxdb_sink_discarded_on_close,"fatal":io.influxdb_sink_fatal,
            "semantics":"live_best_effort; batch_acked_after_http_204; 422_partial_write_fails_batch",
            "scope":"this_attempt"});
    }
    if s.source_kind == "websocket" {
        value["websocket_source"] = json!({"received":io.websocket_source_received,"rows":io.websocket_source_rows,"dropped_bad":io.websocket_source_dropped_bad,
            "dropped_oversize":io.websocket_source_dropped_oversize,"dropped_binary":io.websocket_source_dropped_binary,"dropped_budget":io.websocket_source_dropped_budget,"dropped_overflow":io.websocket_source_dropped_overflow,
            "backpressure_waits":io.websocket_source_backpressure_waits,"connects":io.websocket_source_connects,"reconnects":io.websocket_source_reconnects,
            "disconnects":io.websocket_source_disconnects,"connect_failures":io.websocket_source_connect_failures,"heartbeat_timeouts":io.websocket_source_heartbeat_timeouts,
            "pings_sent":io.websocket_source_pings_sent,"inbox_items":io.websocket_source_inbox_items,"inbox_bytes":io.websocket_source_inbox_bytes,
            "semantics":"live_best_effort_at_most_once_no_replay_no_ack",
            "scope":"this_attempt; dropped_overflow=complete_data_messages_dropped_at_prefetch; dropped_oversize=messages_rejected_by_size_limit_plus_records_over_decode_limit"});
    }
    if s.sink_kind == "websocket" {
        value["websocket_sink"] = json!({"sent":io.websocket_sink_sent,"dropped_bad":io.websocket_sink_dropped_bad,"dropped_oversize":io.websocket_sink_dropped_oversize,
            "dropped_overflow":io.websocket_sink_dropped_overflow,"dropped_budget":io.websocket_sink_dropped_budget,"backpressure_waits":io.websocket_sink_backpressure_waits,"send_failed":io.websocket_sink_send_failed,
            "send_timeouts":io.websocket_sink_send_timeouts,"discarded_on_close":io.websocket_sink_discarded_on_close,"connects":io.websocket_sink_connects,
            "reconnects":io.websocket_sink_reconnects,"disconnects":io.websocket_sink_disconnects,"connect_failures":io.websocket_sink_connect_failures,
            "heartbeat_timeouts":io.websocket_sink_heartbeat_timeouts,"pings_sent":io.websocket_sink_pings_sent,"ignored_frames":io.websocket_sink_ignored_frames,
            "closes":io.websocket_sink_closes,"close_failed":io.websocket_sink_close_failed,"fatal":io.websocket_sink_fatal,
            "queue_items":io.websocket_sink_queue_items,
            "semantics":"live_best_effort_at_most_once_no_application_ack",
            "scope":"this_attempt; sent=written_to_socket_not_peer_receipt"});
    }
    if s.sink_kind == "redis" {
        value["redis_sink"] = json!({"commands_ok":io.redis_sink_commands_ok,"pipelines":io.redis_sink_pipelines,"bytes_sent":io.redis_sink_bytes_sent,"command_errors":io.redis_sink_command_errors,"publish_no_receivers":io.redis_sink_publish_no_receivers,"dropped_bad":io.redis_sink_dropped_bad,"dropped_oversize":io.redis_sink_dropped_oversize,"dropped_budget":io.redis_sink_dropped_budget,"retries":io.redis_sink_retries,"connects":io.redis_sink_connects,"connect_failures":io.redis_sink_connect_failures,"unknown_outcome":io.redis_sink_unknown_outcome,"discarded_on_close":io.redis_sink_discarded_on_close,"fatal":io.redis_sink_fatal,
            "semantics":"live_best_effort; batch_acked_after_every_command_reply; unknown_outcome=non_idempotent_command_sent_without_reply_not_resent",
            "scope":"this_attempt"});
    }
    if s.source_kind == "postgres" {
        value["postgres_source"] = json!({"rows":io.postgres_source_rows,"queries":io.postgres_source_queries,"dropped_bad":io.postgres_source_dropped_bad,"dropped_oversize":io.postgres_source_dropped_oversize,"backpressure_waits":io.postgres_source_backpressure_waits,"budget_waits":io.postgres_source_budget_waits,"connects":io.postgres_source_connects,"connect_failures":io.postgres_source_connect_failures,"query_failures":io.postgres_source_query_failures,"timeouts":io.postgres_source_timeouts,
            "tracking_value":(io.postgres_source_tracking_set > 0).then_some(io.postgres_source_tracking_value),
            "inbox_items":io.postgres_source_inbox_items,"inbox_bytes":io.postgres_source_inbox_bytes,
            "semantics":"live_best_effort; tracking_value_advanced_after_whole_page_admitted; not_a_replay_point; late_rows_with_smaller_values_skipped",
            "scope":"this_attempt"});
    }
    if s.sink_kind == "postgres" {
        value["postgres_sink"] = json!({"rows":io.postgres_sink_rows,"transactions":io.postgres_sink_transactions,"statements":io.postgres_sink_statements,"batch_errors":io.postgres_sink_batch_errors,"dropped_bad":io.postgres_sink_dropped_bad,"dropped_oversize":io.postgres_sink_dropped_oversize,"budget_waits":io.postgres_sink_budget_waits,"retries":io.postgres_sink_retries,"connects":io.postgres_sink_connects,"connect_failures":io.postgres_sink_connect_failures,"timeouts":io.postgres_sink_timeouts,"unknown_outcome":io.postgres_sink_unknown_outcome,"discarded_on_close":io.postgres_sink_discarded_on_close,"fatal":io.postgres_sink_fatal,
            "semantics":"live_best_effort; batch_acked_after_commit; unknown_outcome=insert_commit_sent_without_reply_not_retried",
            "scope":"this_attempt"});
    }
    if s.source_kind == "tcp" {
        value["tcp_source"] = json!({"received":io.tcp_source_received,"rows":io.tcp_source_rows,"bytes_read":io.tcp_source_bytes_read,
            "dropped_bad":io.tcp_source_dropped_bad,"dropped_oversize":io.tcp_source_dropped_oversize,"dropped_partial":io.tcp_source_dropped_partial,
            "dropped_budget":io.tcp_source_dropped_budget,"backpressure_waits":io.tcp_source_backpressure_waits,"connects":io.tcp_source_connects,
            "reconnects":io.tcp_source_reconnects,"disconnects":io.tcp_source_disconnects,"connect_failures":io.tcp_source_connect_failures,
            "idle_timeouts":io.tcp_source_idle_timeouts,"inbox_items":io.tcp_source_inbox_items,"inbox_bytes":io.tcp_source_inbox_bytes,
            "semantics":"live_best_effort_at_most_once_no_replay_no_ack",
            "scope":"this_attempt; dropped_partial=unterminated_record_at_disconnect"});
    }
    if s.sink_kind == "tcp" {
        value["tcp_sink"] = json!({"sent":io.tcp_sink_sent,"bytes_written":io.tcp_sink_bytes_written,"dropped_bad":io.tcp_sink_dropped_bad,
            "dropped_oversize":io.tcp_sink_dropped_oversize,"dropped_budget":io.tcp_sink_dropped_budget,"dropped_overflow":io.tcp_sink_dropped_overflow,"backpressure_waits":io.tcp_sink_backpressure_waits,
            "send_failed":io.tcp_sink_send_failed,"send_timeouts":io.tcp_sink_send_timeouts,"discarded_on_close":io.tcp_sink_discarded_on_close,
            "connects":io.tcp_sink_connects,"reconnects":io.tcp_sink_reconnects,"disconnects":io.tcp_sink_disconnects,
            "connect_failures":io.tcp_sink_connect_failures,"ignored_bytes":io.tcp_sink_ignored_bytes,"closes":io.tcp_sink_closes,
            "close_failed":io.tcp_sink_close_failed,"fatal":io.tcp_sink_fatal,"queue_items":io.tcp_sink_queue_items,
            "semantics":"live_best_effort_at_most_once_no_application_ack",
            "scope":"this_attempt; sent=frame_written_to_socket_not_peer_receipt"});
    }
    if s.sink_kind == "jetstream" {
        value["jetstream_sink"] = json!({"acked":io.jetstream_sink_acked,
            "duplicates":io.jetstream_sink_duplicates,"retries":io.jetstream_sink_retries,
            "ack_timeouts":io.jetstream_sink_ack_timeouts,"failed":io.jetstream_sink_failed,
            "batches":io.jetstream_sink_batches,"discarded_on_close":io.jetstream_sink_discarded_on_close,
            "dropped_bad":io.jetstream_sink_dropped_bad,"dropped_oversize":io.jetstream_sink_dropped_oversize,
            "msg_id_missing":io.jetstream_sink_msg_id_missing,"disconnects":io.jetstream_sink_disconnects,
            "reconnects":io.jetstream_sink_reconnects,"client_errors":io.jetstream_sink_client_errors,
            "sessions":io.jetstream_sink_sessions,"fatal":io.jetstream_sink_fatal,
            "inflight":io.jetstream_sink_inflight,
            "semantics":"at_least_once_into_stream_pub_ack_confirmed; duplicates_possible_on_retry",
            "scope":"this_attempt; duplicates=server_reported_msg_id_duplicates"});
    }
    if let Some(ports) = &s.graph_ports {
        value["graph_ports"] = json!({"sources":ports.sources.iter().map(|(id,diag)|graph_port_json(*id,true,diag)).collect::<Vec<_>>(),"sinks":ports.sinks.iter().map(|(id,diag)|graph_port_json(*id,false,diag)).collect::<Vec<_>>()});
        value["source"] = json!({"available":false,"reason":"multiple_ports_see_graph_ports"});
        value["sink"] = json!({"available":false,"reason":"multiple_ports_see_graph_ports"});
        value["delivery"] = json!({"available":false,"reason":"per_sink_see_graph_ports"});
        value["latency_contract"]["local_origin"] =
            json!("graph_port_specific; no_single_source_origin");
        value["diagnosis_reasons"] = json!(["graph_inspect_each_port_and_physical_input_progress"]);
    }
    value
}

fn graph_port_json(id: u32, source: bool, diag: &sparrow_connectors::IoDiagnostics) -> Value {
    let endpoints = diag.observation.endpoints();
    let endpoint = endpoints
        .as_ref()
        .map(|(input, output)| endpoint_json(if source { input } else { output }));
    let queue = boundary_queue_json(if source {
        diag.source_queue.get()
    } else {
        diag.sink_queue.get()
    });
    let delivery = diag.observation.delivery().unwrap_or_default();
    let io = diag.snapshot();
    json!({"operator_id":id,"role":if source{"source"}else{"sink"},"endpoint":endpoint,"queue":queue,
        "decode_errors":io.decode_errors,"http_failed":io.http_failed,"http_posted":io.http_posted,
        "delivery":{"active_input_batches":delivery.active_groups,"active_rows":delivery.active_rows,"failed_or_cancelled_rows":delivery.failed_rows,"completed_rows":delivery.completed_rows},
        "latency":diag.observation.histograms().into_iter().map(|(name,h)|(name.to_string(),histogram_json(&h))).collect::<serde_json::Map<_,_>>()})
}

fn mailbox_snapshot_json(s: &sparrow_control::supervisor::PipelineMailboxSnapshot) -> Value {
    let runtime = &s.runtime;
    json!({
        "available": runtime.initialized,
        "reason": if runtime.initialized { "observed" } else { "initializing" },
        "scope": "runtime_mailboxes",
        "running_revision": s.running_revision,
        "plan_revision": runtime.plan_revision,
        "pipeline_id": runtime.pipeline_id,
        "runtime_attempt_id": runtime.runtime_attempt_id,
        "execution_finished": s.finished,
        "cancel_requested": s.cancel_requested,
        "clock": "process_monotonic",
        "age_origin": "mailbox_enqueue",
        "coverage": {"source_inbox": false, "sink_outbox": false, "transport": false, "end_to_end_age": false},
        "edges": runtime.edges.iter().map(|e| {
            let q = &e.queue;
            let mut value = json!({
                "edge_index": e.edge_index, "from_stage": e.from_stage, "to_stage": e.to_stage,
                "from_kind": e.from_kind, "to_kind": e.to_kind,
                "max_items": q.max_items, "max_bytes": q.max_bytes, "metadata_bytes": q.metadata_bytes,
                "receiver_open": q.receiver_open,
                "queued": queue_depth_json(&q.queued), "consumer_held": queue_depth_json(&q.consumer_held),
                "credits_reserved_bytes": q.credits_reserved_bytes,
                "peak_queued_items": q.peak_queued_items, "peak_queued_bytes": q.peak_queued_bytes,
                "oldest_queued_age_us": q.oldest_queued_age_us, "oldest_queued_data_age_us": q.oldest_queued_data_age_us,
                "residence": histogram_json(&q.residence),
                "send_attempts_total": q.send_attempts_total, "enqueued_items_total": q.enqueued_items_total,
                "received_items_total": q.received_items_total, "discarded_on_close_total": q.discarded_on_close_total,
                "aborted_sends_total": q.aborted_sends_total, "blocked_sends_total": q.blocked_sends_total,
                "waiting_senders": q.waiting_senders, "completed_waits_total": q.completed_waits_total,
                "completed_wait_us_total": q.completed_wait_us_total, "max_completed_wait_us": q.max_completed_wait_us,
            });
            value["accounting_valid"] = json!(q.accounting_errors_total == 0);
            value["input_progress"] = json!(q.input_progress.as_ref().map(input_progress_json));
            value["accounting_errors_total"] = json!(q.accounting_errors_total);
            value
        }).collect::<Vec<_>>(),
    })
}

fn input_progress_json(p: &sparrow_runtime::mailbox_observe::InputProgress) -> Value {
    json!({"scope":"physical_edge_received_not_committed","rows_received":p.rows_received,"watermark_micros":p.watermark_micros,"idle":p.idle,"eof":p.eof,"barrier_received":p.barrier_received,"barrier_blocked":p.barrier_blocked})
}

async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let snap = state.supervisor.kernel().metrics.snapshot();
    let io = state.supervisor.io_snapshot().await;
    let credits = state.supervisor.kernel().process_owner().usage();
    let mailbox_jobs = state.supervisor.mailbox_snapshots().await;
    let flow_jobs = state.supervisor.flow_snapshots().await;
    let mut io_fields = json!({
            "mqtt_received": io.mqtt_received,
            "mqtt_pending_bytes": io.mqtt_pending_bytes,
            "mqtt_dropped_budget": io.mqtt_dropped_budget,
            "mqtt_dropped_oversize": io.mqtt_dropped_oversize,
            "mqtt_accounted_sources": io.mqtt_accounted_sources,
            "mqtt_inbox_items": io.mqtt_inbox_items,
            "mqtt_inbox_bytes": io.mqtt_inbox_bytes,
            "mqtt_inbox_metadata_bytes": io.mqtt_inbox_metadata_bytes,
            "mqtt_inbox_peak_items": io.mqtt_inbox_peak_items,
            "mqtt_inbox_peak_bytes": io.mqtt_inbox_peak_bytes,
            "mqtt_decoded": io.mqtt_decoded,
            "mqtt_dropped_bad": io.mqtt_dropped_bad,
            "mqtt_dropped_full": io.mqtt_dropped_full,
            "mqtt_backpressure_waits": io.mqtt_backpressure_waits,
            "mqtt_backpressure_recovered": io.mqtt_backpressure_recovered,
            "mqtt_reconnects": io.mqtt_reconnects,
            "mqtt_feed_probes": io.mqtt_feed_probes,
            "mqtt_feed_breaks": io.mqtt_feed_breaks,
            "mqtt_ping_timeouts": io.mqtt_ping_timeouts,
            "mqtt_retained_ignored": io.mqtt_retained_ignored,
            "mqtt_quickack_calls": io.mqtt_quickack_calls,
            "mqtt_quickack_errors": io.mqtt_quickack_errors,
            "http_posted": io.http_posted,
            "http_acked_batches": io.http_acked_batches,
            "http_encode_errors": io.http_encode_errors,
            "http_budget_drops": io.http_budget_drops,
            "http_failed": io.http_failed,
            "http_retries": io.http_retries,
            "http_dropped": io.http_dropped,
            "http_inflight": io.http_inflight,
            "log_written": io.log_written,
            "file_written": io.file_written,
            "file_bytes": io.file_bytes,
            "file_segments": io.file_segments,
            "file_syncs": io.file_syncs,
            "file_failed": io.file_failed,
            "plugin_source_rows":io.plugin_source_rows,"plugin_sink_rows":io.plugin_sink_rows,
            "plugin_polls":io.plugin_polls,"plugin_failed":io.plugin_failed,
            "decode_errors": io.decode_errors,
    });
    io_fields["lookup_update_failed"]=json!(io.lookup_update_failed);
    io_fields["outbox_persisted_batches"]=json!(io.outbox_persisted_batches);
    io_fields["input_quarantined"]=json!(io.input_quarantined);
    for (name, value) in [
        ("http_poll_requests", io.http_poll_requests),
        ("http_poll_ok", io.http_poll_ok),
        ("http_poll_not_modified", io.http_poll_not_modified),
        ("http_poll_failed", io.http_poll_failed),
        ("http_poll_timeouts", io.http_poll_timeouts),
        ("http_poll_status_errors", io.http_poll_status_errors),
        ("http_poll_oversize", io.http_poll_oversize),
        ("http_poll_bad_responses", io.http_poll_bad_responses),
        ("http_poll_rows", io.http_poll_rows),
        ("http_poll_dropped_bad", io.http_poll_dropped_bad),
        ("http_poll_dropped_oversize", io.http_poll_dropped_oversize),
        ("http_poll_dropped_budget", io.http_poll_dropped_budget),
        ("http_poll_skipped_ticks", io.http_poll_skipped_ticks),
        (
            "http_poll_backpressure_waits",
            io.http_poll_backpressure_waits,
        ),
        ("http_poll_inflight", io.http_poll_inflight),
        ("http_poll_inbox_items", io.http_poll_inbox_items),
        ("http_poll_inbox_bytes", io.http_poll_inbox_bytes),
        ("nats_source_received", io.nats_source_received),
        ("nats_source_rows", io.nats_source_rows),
        ("nats_source_dropped_bad", io.nats_source_dropped_bad),
        (
            "nats_source_dropped_oversize",
            io.nats_source_dropped_oversize,
        ),
        ("nats_source_dropped_budget", io.nats_source_dropped_budget),
        (
            "nats_source_backpressure_waits",
            io.nats_source_backpressure_waits,
        ),
        ("nats_source_slow_consumer", io.nats_source_slow_consumer),
        ("nats_source_disconnects", io.nats_source_disconnects),
        ("nats_source_reconnects", io.nats_source_reconnects),
        ("nats_source_client_errors", io.nats_source_client_errors),
        ("nats_source_inbox_items", io.nats_source_inbox_items),
        ("nats_source_inbox_bytes", io.nats_source_inbox_bytes),
        ("nats_sink_published", io.nats_sink_published),
        ("nats_sink_failed", io.nats_sink_failed),
        ("nats_sink_dropped_bad", io.nats_sink_dropped_bad),
        ("nats_sink_dropped_oversize", io.nats_sink_dropped_oversize),
        (
            "nats_sink_discarded_on_close",
            io.nats_sink_discarded_on_close,
        ),
        ("nats_sink_flushes", io.nats_sink_flushes),
        ("nats_sink_flush_failed", io.nats_sink_flush_failed),
        ("nats_sink_disconnects", io.nats_sink_disconnects),
        ("nats_sink_reconnects", io.nats_sink_reconnects),
        ("nats_sink_client_errors", io.nats_sink_client_errors),
        ("nats_sink_sessions", io.nats_sink_sessions),
        ("jetstream_sink_acked", io.jetstream_sink_acked),
        ("jetstream_sink_duplicates", io.jetstream_sink_duplicates),
        ("jetstream_sink_retries", io.jetstream_sink_retries),
        (
            "jetstream_sink_ack_timeouts",
            io.jetstream_sink_ack_timeouts,
        ),
        ("jetstream_sink_failed", io.jetstream_sink_failed),
        ("jetstream_sink_batches", io.jetstream_sink_batches),
        (
            "jetstream_sink_discarded_on_close",
            io.jetstream_sink_discarded_on_close,
        ),
        ("jetstream_sink_dropped_bad", io.jetstream_sink_dropped_bad),
        (
            "jetstream_sink_dropped_oversize",
            io.jetstream_sink_dropped_oversize,
        ),
        (
            "jetstream_sink_msg_id_missing",
            io.jetstream_sink_msg_id_missing,
        ),
        ("jetstream_sink_disconnects", io.jetstream_sink_disconnects),
        ("jetstream_sink_reconnects", io.jetstream_sink_reconnects),
        (
            "jetstream_sink_client_errors",
            io.jetstream_sink_client_errors,
        ),
        ("jetstream_sink_sessions", io.jetstream_sink_sessions),
        ("jetstream_sink_fatal", io.jetstream_sink_fatal),
        ("jetstream_sink_inflight", io.jetstream_sink_inflight),
        ("databus_source_received", io.databus_source_received),
        ("databus_source_rows", io.databus_source_rows),
        ("databus_source_dropped_bad", io.databus_source_dropped_bad),
        (
            "databus_source_dropped_oversize",
            io.databus_source_dropped_oversize,
        ),
        (
            "databus_source_dropped_budget",
            io.databus_source_dropped_budget,
        ),
        (
            "databus_source_dropped_oldest",
            io.databus_source_dropped_oldest,
        ),
        (
            "databus_source_dropped_newest",
            io.databus_source_dropped_newest,
        ),
        (
            "databus_source_block_timeouts",
            io.databus_source_block_timeouts,
        ),
        (
            "databus_source_backpressure_waits",
            io.databus_source_backpressure_waits,
        ),
        (
            "databus_source_discarded_on_close",
            io.databus_source_discarded_on_close,
        ),
        (
            "databus_source_buffer_items",
            io.databus_source_buffer_items,
        ),
        (
            "databus_source_buffer_bytes",
            io.databus_source_buffer_bytes,
        ),
        (
            "databus_source_subscriptions",
            io.databus_source_subscriptions,
        ),
        ("databus_source_inbox_items", io.databus_source_inbox_items),
        ("databus_source_inbox_bytes", io.databus_source_inbox_bytes),
        ("databus_sink_published", io.databus_sink_published),
        ("databus_sink_deliveries", io.databus_sink_deliveries),
        (
            "databus_sink_no_subscribers",
            io.databus_sink_no_subscribers,
        ),
        ("databus_sink_dropped_bad", io.databus_sink_dropped_bad),
        (
            "databus_sink_dropped_oversize",
            io.databus_sink_dropped_oversize,
        ),
        (
            "databus_sink_dropped_budget",
            io.databus_sink_dropped_budget,
        ),
        (
            "databus_sink_blocked_publishes",
            io.databus_sink_blocked_publishes,
        ),
        (
            "databus_sink_discarded_on_close",
            io.databus_sink_discarded_on_close,
        ),
        ("databus_sink_batches", io.databus_sink_batches),
        ("databus_sink_fatal", io.databus_sink_fatal),
        ("influxdb_sink_rows_written", io.influxdb_sink_rows_written),
        ("influxdb_sink_requests_ok", io.influxdb_sink_requests_ok),
        ("influxdb_sink_bytes_sent", io.influxdb_sink_bytes_sent),
        (
            "influxdb_sink_gzip_fallbacks",
            io.influxdb_sink_gzip_fallbacks,
        ),
        ("influxdb_sink_retries", io.influxdb_sink_retries),
        (
            "influxdb_sink_retry_after_waits",
            io.influxdb_sink_retry_after_waits,
        ),
        (
            "influxdb_sink_retry_after_capped",
            io.influxdb_sink_retry_after_capped,
        ),
        ("influxdb_sink_dropped_bad", io.influxdb_sink_dropped_bad),
        (
            "influxdb_sink_dropped_oversize",
            io.influxdb_sink_dropped_oversize,
        ),
        (
            "influxdb_sink_dropped_budget",
            io.influxdb_sink_dropped_budget,
        ),
        (
            "influxdb_sink_rejected_requests",
            io.influxdb_sink_rejected_requests,
        ),
        (
            "influxdb_sink_rejected_rows",
            io.influxdb_sink_rejected_rows,
        ),
        (
            "influxdb_sink_partial_writes",
            io.influxdb_sink_partial_writes,
        ),
        (
            "influxdb_sink_discarded_on_close",
            io.influxdb_sink_discarded_on_close,
        ),
        ("influxdb_sink_fatal", io.influxdb_sink_fatal),
        ("csv_malformed", io.csv_malformed),
        ("csv_oversize", io.csv_oversize),
        ("csv_type_errors", io.csv_type_errors),
        ("csv_header_errors", io.csv_header_errors),
        ("csv_encode_errors", io.csv_encode_errors),
        ("protobuf_malformed", io.protobuf_malformed),
        ("protobuf_oversize", io.protobuf_oversize),
        ("protobuf_type_errors", io.protobuf_type_errors),
        ("protobuf_unknown_fields", io.protobuf_unknown_fields),
        ("protobuf_encode_errors", io.protobuf_encode_errors),
        ("websocket_source_received", io.websocket_source_received),
        ("websocket_source_rows", io.websocket_source_rows),
        (
            "websocket_source_dropped_bad",
            io.websocket_source_dropped_bad,
        ),
        (
            "websocket_source_dropped_oversize",
            io.websocket_source_dropped_oversize,
        ),
        (
            "websocket_source_dropped_binary",
            io.websocket_source_dropped_binary,
        ),
        (
            "websocket_source_dropped_budget",
            io.websocket_source_dropped_budget,
        ),
        ("websocket_source_dropped_overflow", io.websocket_source_dropped_overflow),
        (
            "websocket_source_backpressure_waits",
            io.websocket_source_backpressure_waits,
        ),
        ("websocket_source_connects", io.websocket_source_connects),
        (
            "websocket_source_reconnects",
            io.websocket_source_reconnects,
        ),
        (
            "websocket_source_disconnects",
            io.websocket_source_disconnects,
        ),
        (
            "websocket_source_connect_failures",
            io.websocket_source_connect_failures,
        ),
        (
            "websocket_source_heartbeat_timeouts",
            io.websocket_source_heartbeat_timeouts,
        ),
        (
            "websocket_source_pings_sent",
            io.websocket_source_pings_sent,
        ),
        ("websocket_sink_sent", io.websocket_sink_sent),
        ("websocket_sink_dropped_bad", io.websocket_sink_dropped_bad),
        (
            "websocket_sink_dropped_oversize",
            io.websocket_sink_dropped_oversize,
        ),
        (
            "websocket_sink_dropped_overflow",
            io.websocket_sink_dropped_overflow,
        ),
        (
            "websocket_sink_dropped_budget",
            io.websocket_sink_dropped_budget,
        ),
        (
            "websocket_sink_backpressure_waits",
            io.websocket_sink_backpressure_waits,
        ),
        ("websocket_sink_send_failed", io.websocket_sink_send_failed),
        (
            "websocket_sink_send_timeouts",
            io.websocket_sink_send_timeouts,
        ),
        (
            "websocket_sink_discarded_on_close",
            io.websocket_sink_discarded_on_close,
        ),
        ("websocket_sink_connects", io.websocket_sink_connects),
        ("websocket_sink_reconnects", io.websocket_sink_reconnects),
        ("websocket_sink_disconnects", io.websocket_sink_disconnects),
        (
            "websocket_sink_connect_failures",
            io.websocket_sink_connect_failures,
        ),
        (
            "websocket_sink_heartbeat_timeouts",
            io.websocket_sink_heartbeat_timeouts,
        ),
        ("websocket_sink_pings_sent", io.websocket_sink_pings_sent),
        (
            "websocket_sink_ignored_frames",
            io.websocket_sink_ignored_frames,
        ),
        ("websocket_sink_closes", io.websocket_sink_closes),
        (
            "websocket_sink_close_failed",
            io.websocket_sink_close_failed,
        ),
        ("websocket_sink_fatal", io.websocket_sink_fatal),
        ("websocket_sink_queue_items", io.websocket_sink_queue_items),
        (
            "websocket_source_inbox_items",
            io.websocket_source_inbox_items,
        ),
        (
            "websocket_source_inbox_bytes",
            io.websocket_source_inbox_bytes,
        ),
        ("redis_sink_commands_ok", io.redis_sink_commands_ok),
        ("redis_sink_pipelines", io.redis_sink_pipelines),
        ("redis_sink_bytes_sent", io.redis_sink_bytes_sent),
        ("redis_sink_command_errors", io.redis_sink_command_errors),
        (
            "redis_sink_publish_no_receivers",
            io.redis_sink_publish_no_receivers,
        ),
        ("redis_sink_dropped_bad", io.redis_sink_dropped_bad),
        (
            "redis_sink_dropped_oversize",
            io.redis_sink_dropped_oversize,
        ),
        ("redis_sink_dropped_budget", io.redis_sink_dropped_budget),
        ("redis_sink_retries", io.redis_sink_retries),
        ("redis_sink_connects", io.redis_sink_connects),
        (
            "redis_sink_connect_failures",
            io.redis_sink_connect_failures,
        ),
        ("redis_sink_unknown_outcome", io.redis_sink_unknown_outcome),
        (
            "redis_sink_discarded_on_close",
            io.redis_sink_discarded_on_close,
        ),
        ("redis_sink_fatal", io.redis_sink_fatal),
        ("postgres_source_rows", io.postgres_source_rows),
        ("postgres_source_queries", io.postgres_source_queries),
        (
            "postgres_source_dropped_bad",
            io.postgres_source_dropped_bad,
        ),
        (
            "postgres_source_dropped_oversize",
            io.postgres_source_dropped_oversize,
        ),
        (
            "postgres_source_backpressure_waits",
            io.postgres_source_backpressure_waits,
        ),
        (
            "postgres_source_budget_waits",
            io.postgres_source_budget_waits,
        ),
        ("postgres_source_connects", io.postgres_source_connects),
        (
            "postgres_source_connect_failures",
            io.postgres_source_connect_failures,
        ),
        (
            "postgres_source_query_failures",
            io.postgres_source_query_failures,
        ),
        ("postgres_source_timeouts", io.postgres_source_timeouts),
        (
            "postgres_source_inbox_items",
            io.postgres_source_inbox_items,
        ),
        (
            "postgres_source_inbox_bytes",
            io.postgres_source_inbox_bytes,
        ),
        ("postgres_sink_rows", io.postgres_sink_rows),
        ("postgres_sink_transactions", io.postgres_sink_transactions),
        ("postgres_sink_statements", io.postgres_sink_statements),
        ("postgres_sink_batch_errors", io.postgres_sink_batch_errors),
        ("postgres_sink_dropped_bad", io.postgres_sink_dropped_bad),
        (
            "postgres_sink_dropped_oversize",
            io.postgres_sink_dropped_oversize,
        ),
        ("postgres_sink_budget_waits", io.postgres_sink_budget_waits),
        ("postgres_sink_retries", io.postgres_sink_retries),
        ("postgres_sink_connects", io.postgres_sink_connects),
        (
            "postgres_sink_connect_failures",
            io.postgres_sink_connect_failures,
        ),
        ("postgres_sink_timeouts", io.postgres_sink_timeouts),
        (
            "postgres_sink_unknown_outcome",
            io.postgres_sink_unknown_outcome,
        ),
        (
            "postgres_sink_discarded_on_close",
            io.postgres_sink_discarded_on_close,
        ),
        ("postgres_sink_fatal", io.postgres_sink_fatal),
        ("tcp_source_received", io.tcp_source_received),
        ("tcp_source_rows", io.tcp_source_rows),
        ("tcp_source_bytes_read", io.tcp_source_bytes_read),
        ("tcp_source_dropped_bad", io.tcp_source_dropped_bad),
        (
            "tcp_source_dropped_oversize",
            io.tcp_source_dropped_oversize,
        ),
        ("tcp_source_dropped_partial", io.tcp_source_dropped_partial),
        ("tcp_source_dropped_budget", io.tcp_source_dropped_budget),
        (
            "tcp_source_backpressure_waits",
            io.tcp_source_backpressure_waits,
        ),
        ("tcp_source_connects", io.tcp_source_connects),
        ("tcp_source_reconnects", io.tcp_source_reconnects),
        ("tcp_source_disconnects", io.tcp_source_disconnects),
        (
            "tcp_source_connect_failures",
            io.tcp_source_connect_failures,
        ),
        ("tcp_source_idle_timeouts", io.tcp_source_idle_timeouts),
        ("tcp_sink_sent", io.tcp_sink_sent),
        ("tcp_sink_bytes_written", io.tcp_sink_bytes_written),
        ("tcp_sink_dropped_bad", io.tcp_sink_dropped_bad),
        ("tcp_sink_dropped_oversize", io.tcp_sink_dropped_oversize),
        ("tcp_sink_dropped_budget", io.tcp_sink_dropped_budget),
        ("tcp_sink_dropped_overflow", io.tcp_sink_dropped_overflow),
        (
            "tcp_sink_backpressure_waits",
            io.tcp_sink_backpressure_waits,
        ),
        ("tcp_sink_send_failed", io.tcp_sink_send_failed),
        ("tcp_sink_send_timeouts", io.tcp_sink_send_timeouts),
        (
            "tcp_sink_discarded_on_close",
            io.tcp_sink_discarded_on_close,
        ),
        ("tcp_sink_connects", io.tcp_sink_connects),
        ("tcp_sink_reconnects", io.tcp_sink_reconnects),
        ("tcp_sink_disconnects", io.tcp_sink_disconnects),
        ("tcp_sink_connect_failures", io.tcp_sink_connect_failures),
        ("tcp_sink_ignored_bytes", io.tcp_sink_ignored_bytes),
        ("tcp_sink_closes", io.tcp_sink_closes),
        ("tcp_sink_close_failed", io.tcp_sink_close_failed),
        ("tcp_sink_fatal", io.tcp_sink_fatal),
        ("tcp_sink_queue_items", io.tcp_sink_queue_items),
        ("tcp_source_inbox_items", io.tcp_source_inbox_items),
        ("tcp_source_inbox_bytes", io.tcp_source_inbox_bytes),
    ] {
        io_fields[name] = json!(value);
    }
    let mut value = json!({
        "jobs_started": snap.jobs_started,
        "state_accounting_errors_total": state.supervisor.kernel().process_owner().accounting_errors_total(),
        "process_credits": {"scope":"tracked_job_credits_not_process_RSS; concurrent_fields_are_not_atomic",
            "reservation_bytes":credits.reservation_bytes,"retention_bytes":credits.retention_bytes,
            "queue_bytes":credits.queue_bytes,"physical_bytes":credits.physical_bytes,
            "peak_physical_bytes":credits.peak_physical_bytes,"live_handles":credits.live_handles},
        "histogram_contract": histogram_contract_json(),
        "jobs_stopped": snap.jobs_stopped,
        "jobs_failed": snap.jobs_failed,
        "ingested_rows": snap.ingested_rows,
        "emitted_rows": snap.emitted_rows,
        "queue_items": snap.queue_items,
        "queue_bytes": snap.queue_bytes,
        "queue_metrics_available": false,
        "mailboxes": {"available": true, "scope": "runtime_mailboxes", "retention": "active_handles_only",
            "jobs": mailbox_jobs.iter().map(|(name, snapshot)| json!({"name": name, "observation": mailbox_snapshot_json(snapshot)})).collect::<Vec<_>>()},
        "io_scope": "running_attempts",
        "observations":{"retention":"active_handles_only","jobs":flow_jobs.iter().map(|(name,s)|json!({"name":name,"observation":flow_snapshot_json(s)})).collect::<Vec<_>>()},
        "state_keys": snap.state_keys,
        "state_bytes": snap.state_bytes,
        "state_sample_scope": "last_window_sample_not_process_total",
        "live_samples": snap.live_samples,
        "watermark_lag_micros": snap.watermark_lag_micros,
        "checkpoint_duration_micros": snap.checkpoint_duration_micros,
        "checkpoint_bytes": snap.checkpoint_bytes,
        "checkpoint_commits": snap.checkpoint_commits,
        "checkpoint_aborts": snap.checkpoint_aborts,
        "future_dropped": snap.future_dropped,
        "timers_live": snap.timers_live,
        "timers_cancelled": snap.timers_cancelled,
        "io": io_fields,
        "log": snap.log_line(),
        "label_budget": "job_and_connector_only; no per-event labels",
        "honesty": HONESTY,
    });
    value["graph_dropped_rows"] = json!(snap.graph_dropped_rows);
    value["graph_side_rows"] = json!(snap.graph_side_rows);
    value["graph_detached_branches"] = json!(snap.graph_detached_branches);
    value["graph_branch_failures"] = json!(snap.graph_branch_failures);
    value["iot_input_rows"] = json!(snap.iot_input_rows);
    value["iot_emitted_rows"] = json!(snap.iot_emitted_rows);
    value["iot_filtered_rows"] = json!(snap.iot_filtered_rows);
    value["iot_invalid_rows"] = json!(snap.iot_invalid_rows);
    value["iot_expired_keys"] = json!(snap.iot_expired_keys);
    value["alarm_notifications_expired"] = json!(snap.alarm_notifications_expired);
    value["alarm_notifications_cancelled"] = json!(snap.alarm_notifications_cancelled);
    value["alarm_notifications_deferred"] = json!(snap.alarm_notifications_deferred);
    value["resample_discarded_inputs"] = json!(snap.resample_discarded_inputs);
    value["resample_missing_outputs"] = json!(snap.resample_missing_outputs);
    value["resample_interpolated_outputs"] = json!(snap.resample_interpolated_outputs);
    value["iot_state_keys"] = json!(snap.iot_state_keys);
    value["iot_state_bytes"] = json!(snap.iot_state_bytes);
    value["iot_metrics_scope"] =
        json!("all_iot_operator_instances; one input may be counted by multiple IoT nodes");
    value["iot_filtered_contract"] = json!("includes_invalid_and_suppressed; invalid_is_subset");
    Ok(Json(value))
}

async fn list_pipelines(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let names = state.store.list_pipeline_names().map_err(ApiError::from)?;
        Ok(Json(json!({"pipelines": names})))
    })
    .await
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
    blocking_api(move || {
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
            let parsed: StartBody = serde_json::from_slice(&body).map_err(|e| {
                ApiError::from(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("start body JSON: {e}"),
                ))
            })?;
            parsed.revision
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
    })
    .await
}

async fn checkpoint_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let result = state.supervisor.checkpoint_named(&name).await;
    let store = state.store.clone();
    let audit_name = name.clone();
    let outcome = if result.is_ok() {
        "committed"
    } else {
        "failed_or_wait_timed_out"
    };
    let audit_recorded = store
        .clone()
        .run_blocking(move || store.audit(&actor, "checkpoint", Some(&audit_name), None, outcome))
        .await
        .is_ok();
    let id = result.map_err(ApiError::from)?;
    Ok(Json(json!({
        "checkpoint_id": id,
        "audit_recorded": audit_recorded,
        "exactly_once": false,
        "honesty": HONESTY,
    })))
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RestoreBody {
    snapshot_id: Option<u64>,
}

async fn restore_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let request: RestoreBody = if body.is_empty() {
        RestoreBody::default()
    } else {
        serde_json::from_slice(&body).map_err(|_| {
            ApiError::from(SparrowError::new(
                ErrorCode::InvalidArgument,
                "invalid restore request",
            ))
        })?
    };
    blocking_api(move || {
            let row = state.store.get_pipeline(&name).map_err(ApiError::from)?;
            let mut spec = row.spec;
            spec.restore = Some(RestoreSpec {
                kind: "checkpoint".into(),
                snapshot_id: Some(
                    request
                        .snapshot_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "aligned".into()),
                ),
                client_id: None,
            });
            spec.check_delivery().map_err(ApiError::from)?;
            let plan = sparrow_control::validate::bind_plan_with_store(
                &state.store,
                &spec,
                &name,
                row.latest_revision,
            )
            .map_err(ApiError::from)?;
            validate_aligned_plan(&spec, &plan).map_err(ApiError::from)?;
            // Reject malformed/unsupported/CAS-conflicting requests before stopping
            // the live attempt. The selected new revision is activated explicitly.
            let restore_revision=state.store.put_pipeline_and_start(&name,&spec,Some(&row.etag))
                .map_err(ApiError::from)?.latest_revision;
        let audit_recorded=state.store.audit(&actor,"restore",Some(&name),Some(&restore_revision.to_string()),"requested").is_ok();
        state.supervisor.wake();
        let mut body = status_body(&state, &name)?;
        if let Value::Object(map) = &mut body {
            map.insert("audit_recorded".into(),json!(audit_recorded));
            map.insert(
                "note".into(),
                json!("restore requested; supervisor starts the revision that contains restore=checkpoint. Not exactly-once."),
            );
        }
        Ok(Json(body))
    })
    .await
}

async fn kill_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let state_c = state.clone();
    let name_c = name.clone();
    blocking_api(move || request_stop(&state_c.store, &name_c, &actor).map_err(ApiError::from))
        .await?;
    state
        .supervisor
        .kill_named(&name)
        .await
        .map_err(ApiError::from)?;
    state.supervisor.wake();
    blocking_api(move || Ok(Json(status_body(&state, &name)?))).await
}

async fn stop_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    blocking_api(move || {
        request_stop(&state.store, &name, &actor).map_err(ApiError::from)?;
        state.supervisor.wake();
        Ok(Json(status_body(&state, &name)?))
    })
    .await
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
    blocking_api(move || {
        state
            .store
            .put_allow(&body.host, body.port)
            .map_err(ApiError::from)?;
        state
            .store
            .audit(
                &actor,
                "allowlist",
                Some(&format!("{}:{}", body.host, body.port)),
                None,
                "ok",
            )
            .map_err(ApiError::from)?;
        Ok(Json(json!({"host": body.host, "port": body.port})))
    })
    .await
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
    blocking_api(move || {
        if body.value.len() > 4096 {
            return Err(ApiError::from(SparrowError::new(
                ErrorCode::MaxRecordSize,
                "secret value exceeds 4KiB",
            )));
        }
        state
            .store
            .put_secret(&name, &body.value)
            .map_err(ApiError::from)?;
        state
            .store
            .audit(
                &actor,
                "put_secret",
                Some(&name),
                Some("value redacted"),
                "ok",
            )
            .map_err(ApiError::from)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

async fn list_audit(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
) -> ApiResult<Json<Value>> {
    let viewer = p.role == Role::Viewer;
    blocking_api(move || {
        let rows = state.store.list_audit(50).map_err(ApiError::from)?;
        Ok(Json(json!({
            "audit": rows.iter().map(|r| json!({
                "id": r.id,
                "at_ms": r.at_ms,
                "actor": r.actor,
                "action": r.action,
                "target": if viewer && r.action == "validate" { None } else { r.target.clone() },
                "detail": if viewer { None } else { r.detail.clone() },
                "outcome": r.outcome,
            })).collect::<Vec<_>>(),
            "cap": 200,
        })))
    })
    .await
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
    #[cfg(feature = "demo-io")]
    {
        let Some(demo) = state.supervisor.demo() else {
            return Err(ApiError::from(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "demo I/O is not enabled",
            )));
        };
        demo.publish_fixture().await.map_err(ApiError::from)?;
        return Ok(Json(json!({"published": 6, "topic": "sensors/json"})));
    }
    #[cfg(not(feature = "demo-io"))]
    {
        let _ = state;
        Err(ApiError::from(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "sparrow-server was built without the demo-io feature",
        )))
    }
}

async fn demo_capture(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    #[cfg(feature = "demo-io")]
    {
        let Some(demo) = state.supervisor.demo() else {
            return Err(ApiError::from(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "demo I/O is not enabled",
            )));
        };
        return Ok(Json(json!({"bodies": demo.http.body_strings()})));
    }
    #[cfg(not(feature = "demo-io"))]
    {
        let _ = state;
        Err(ApiError::from(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "sparrow-server was built without the demo-io feature",
        )))
    }
}

pub async fn boot(
    store: Arc<Store>,
    kernel: Arc<Kernel>,
    token: String,
    safe_mode: bool,
    demo_io: bool,
) -> Result<(AppState, Option<DemoIo>), SparrowError> {
    boot_with_auth(store, kernel, Auth::legacy(&token), safe_mode, demo_io).await
}

pub async fn boot_with_auth(
    store: Arc<Store>,
    kernel: Arc<Kernel>,
    auth: Auth,
    safe_mode: bool,
    demo_io: bool,
) -> Result<(AppState, Option<DemoIo>), SparrowError> {
    #[cfg(feature = "demo-io")]
    let demo = if demo_io {
        let h = Arc::new(DemoHarness::start().await?);
        store.put_allow(&h.broker.host(), h.broker.port())?;
        store.put_allow("127.0.0.1", h.http.port())?;
        Some(h)
    } else {
        None
    };
    #[cfg(not(feature = "demo-io"))]
    let demo = {
        if demo_io {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "sparrow-server was built without the demo-io feature",
            ));
        }
        None
    };
    let store_c = Arc::clone(&store);
    let demo_c = demo.clone();
    let supervisor =
        tokio::task::spawn_blocking(move || Supervisor::new(store_c, kernel, safe_mode, demo_c))
            .await
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("boot: {e}")))??;
    let state = AppState {
        store,
        supervisor: Arc::clone(&supervisor),
        token: Arc::new(auth),
        safe_mode,
    };
    tokio::spawn(supervisor.run_loop());
    Ok((state, demo))
}

pub async fn serve(state: AppState, addr: SocketAddr) -> Result<(), SparrowError> {
    serve_with_ui(state, addr, None).await
}

/// Management router plus the optional `/ui/` static workbench. UI routes sit
/// outside the management guard (static assets are anonymous; all data still
/// requires authentication). Without assets this is exactly `router`.
pub fn router_with_ui(state: AppState, ui: Option<Arc<UiAssets>>) -> Router {
    build_router(state, ui)
}

pub async fn serve_with_ui(
    state: AppState,
    addr: SocketAddr,
    ui: Option<Arc<UiAssets>>,
) -> Result<(), SparrowError> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("bind {addr}: {e}")))?;
    let supervisor = state.supervisor.clone();
    let stopping = supervisor.clone();
    let result = axum::serve(listener, router_with_ui(state, ui))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Completing this future stops accept first. Do not hide a long
            // source/commit join inside the shutdown notification itself.
            stopping.begin_shutdown();
        })
        .await
        .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("server: {e}")));
    let shutdown = supervisor.shutdown();
    tokio::pin!(shutdown);
    if tokio::time::timeout(Duration::from_secs(120), shutdown.as_mut())
        .await
        .is_err()
    {
        tracing::warn!("shutdown_join_exceeded_120s_waiting_for_owned_tasks; external_service_manager_may_force_kill");
        shutdown.await; // timeout borrowed the future; never abort/detach join
    }
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub async fn wait_status(
    state: &AppState,
    name: &str,
    actual: &str,
    timeout: Duration,
) -> Result<(), SparrowError> {
    let start = std::time::Instant::now();
    loop {
        let store = state.store.clone();
        let name_c = name.to_owned();
        let worker = store.clone();
        if let Ok(a) = store.run_blocking(move || worker.actual(&name_c)).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "demo-io"))]
    #[tokio::test]
    async fn r3_disabled_demo_boot_returns_feature_error_without_runtime_drop_panic() {
        let kernel = Arc::new(sparrow_control::compact_kernel().unwrap());
        let store = Arc::new(Store::open_memory().unwrap());
        match boot(store, kernel, "token".into(), false, true).await {
            Err(error) => assert_eq!(error.code, ErrorCode::FeatureUnavailable),
            Ok(_) => panic!("demo I/O must be unavailable"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r3_blocking_api_keeps_async_worker_responsive() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(blocking_api(move || {
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(150));
            Ok(())
        }));
        rx.await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(80),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .unwrap();
        assert!(
            !worker.is_finished(),
            "blocking work must not monopolize this thread"
        );
        worker.await.unwrap().unwrap();
    }

    #[test]
    fn p3_56_resource_exhausted_and_cancelled_not_500() {
        let r =
            ApiError::from(SparrowError::new(ErrorCode::ResourceExhausted, "cap")).into_response();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        let r = ApiError::from(SparrowError::new(ErrorCode::Cancelled, "gone")).into_response();
        assert_eq!(r.status(), StatusCode::CONFLICT);
        let r = ApiError::from(SparrowError::new(ErrorCode::Internal, "boom")).into_response();
        assert_eq!(r.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn k4_api_error_context_exposes_bounded_operator_identity_only() {
        let error = SparrowError::new(ErrorCode::TypeMismatch, "IoT value type")
            .at_operator(42.into())
            .context("path", "/sensitive/fixture")
            .context("field", "temperature");
        let context = public_error_context(&error);
        assert!(context
            .iter()
            .any(|entry| { entry["key"] == "operator" && entry["value"] == "OperatorId(42)" }));
        assert!(context
            .iter()
            .any(|entry| { entry["key"] == "field" && entry["value"] == "temperature" }));
        assert!(!context.iter().any(|entry| entry["key"] == "path"));
    }

    #[test]
    fn scripts_api_diagnostics_only_expose_safe_phase_and_coordinates() {
        let error = SparrowError::new(ErrorCode::JobFailed, "script failed")
            .context("script_phase", "call")
            .context("script_frames", "3:4,5:7")
            .context("plugin_package", "script_math")
            .context("plugin_version", "v1")
            .context("plugin_function", "double")
            .context("stack", "PRIVATE: input=secret")
            .context("script_frames", "1:2 PRIVATE")
            .context("script_phase", "PRIVATE")
            .context("plugin_function", "/private/path");
        let context = public_error_context(&error);
        assert_eq!(context.len(), 5);
        let encoded = serde_json::to_string(&context).unwrap();
        assert!(encoded.contains("3:4,5:7"));
        assert!(!encoded.contains("PRIVATE"));
        assert!(!encoded.contains("/private"));
    }
}

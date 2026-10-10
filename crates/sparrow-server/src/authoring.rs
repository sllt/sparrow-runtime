//! K5.2 author-side API: drafts (ETag CAS), draft publish with durable
//! receipts, revision history, connection templates and secret names.
//! Nothing here starts a job or opens a connection; publication goes through
//! the same full validation and catalog publication as `PUT /v1/pipelines`.
use crate::*;
use axum::extract::Query;
use sparrow_control::store::authoring as cat;
use sparrow_control::DraftInput;

/// Draft envelope (escaped text + metadata); text/metadata limits are checked
/// separately after decode. Other management routes keep the 64 KiB limit.
pub const MAX_DRAFT_BODY: usize = 512 * 1024;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/drafts", get(list_drafts))
        .route("/v1/drafts/{id}", get(get_draft).put(put_draft).delete(delete_draft))
        .layer(DefaultBodyLimit::max(MAX_DRAFT_BODY))
        .layer(RequestBodyLimitLayer::new(MAX_DRAFT_BODY))
        .merge(
            Router::new()
                .route("/v1/drafts/{id}/check", post(check_draft))
                .route("/v1/drafts/{id}/publish", post(publish_draft))
                .route("/v1/publications/{id}", get(get_publication))
                .route("/v1/pipelines/{name}/revisions", get(list_revisions))
                .route("/v1/pipelines/{name}/revisions/{revision}", get(get_revision))
                .route("/v1/secrets", get(list_secrets))
                .route("/v1/connections", get(list_connections))
                .route("/v1/connections/{name}", get(get_connection).put(put_connection).delete(delete_connection))
                .route("/v1/connections/{name}/test", post(test_connection))
                .layer(DefaultBodyLimit::max(MAX_BODY))
                .layer(RequestBodyLimitLayer::new(MAX_BODY)),
        )
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::from(SparrowError::new(ErrorCode::InvalidArgument, msg.into()))
}

fn with_etag<T: IntoResponse>(etag: &str, body: T) -> Response {
    let mut r = body.into_response();
    if let Ok(v) = axum::http::HeaderValue::from_str(&format!("\"{etag}\"")) {
        r.headers_mut().insert(axum::http::header::ETAG, v);
    }
    r
}

fn draft_json(d: &sparrow_control::DraftRow, full: bool) -> Value {
    let mut v = json!({
        "id": d.id, "etag": d.etag, "pipeline": d.pipeline, "mode": d.mode,
        "base_etag": d.base_etag, "updated_by": d.updated_by, "updated_at_ms": d.updated_at,
        "created_at_ms": d.created_at, "text_bytes": d.text.len(), "metadata_bytes": d.metadata.len(),
    });
    if full {
        v["text"] = json!(d.text);
        // Metadata is an opaque JSON object owned by the UI (canvas layout).
        v["metadata"] = if d.metadata.is_empty() { json!({}) } else { serde_json::from_str(&d.metadata).unwrap_or(json!({})) };
    }
    v
}

async fn list_drafts(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let rows = state.store.list_drafts().map_err(ApiError::from)?;
        Ok(Json(json!({
            "drafts": rows.iter().map(|d| draft_json(d, false)).collect::<Vec<_>>(),
            "limits": {"max_drafts": cat::MAX_DRAFTS, "max_text_bytes": cat::MAX_DRAFT_TEXT, "max_metadata_bytes": cat::MAX_DRAFT_METADATA, "max_total_bytes": cat::MAX_DRAFT_TOTAL},
        })))
    })
    .await
}

async fn get_draft(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    blocking_api(move || {
        let d = state.store.get_draft(&id).map_err(ApiError::from)?;
        Ok(with_etag(&d.etag, Json(draft_json(&d, true))))
    })
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftBody {
    pipeline: String,
    mode: String,
    text: String,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    base_etag: Option<String>,
}

async fn put_draft(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    blocking_api(move || {
        let b: DraftBody = serde_json::from_slice(&body).map_err(|e| bad(format!("draft body: {}", e)))?;
        let metadata = match &b.metadata {
            None | Some(Value::Null) => String::new(),
            Some(v @ Value::Object(_)) => serde_json::to_string(v).map_err(|e| bad(e.to_string()))?,
            Some(_) => return Err(bad("metadata must be a JSON object")),
        };
        let expected = if_match(&headers);
        let row = state
            .store
            .put_draft(&id, expected.as_deref(), &DraftInput { pipeline: &b.pipeline, mode: &b.mode, text: &b.text, metadata: &metadata, base_etag: b.base_etag.as_deref() }, &p.actor)
            .map_err(ApiError::from)?;
        let _ = state.store.audit(&p.actor, "put_draft", Some(&id), Some(&row.etag), "ok");
        Ok(with_etag(&row.etag, (if expected.is_none() { StatusCode::CREATED } else { StatusCode::OK }, Json(draft_json(&row, false)))))
    })
    .await
}

async fn delete_draft(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    blocking_api(move || {
        let want = if_match(&headers).ok_or_else(|| ApiError { status: StatusCode::PRECONDITION_REQUIRED, err: SparrowError::new(ErrorCode::Conflict, "If-Match is required to delete a draft") })?;
        state.store.delete_draft(&id, &want).map_err(ApiError::from)?;
        let _ = state.store.audit(&p.actor, "delete_draft", Some(&id), None, "ok");
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

fn error_json(e: &ApiError) -> Value {
    json!({"code": e.err.code.as_str(), "message": e.err.message, "status": e.status.as_u16(), "context": public_error_context(&e.err)})
}

/// Full validate + explain of the draft's current text without publishing.
/// Each stage reports independently; an invalid draft is a normal response.
async fn check_draft(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let d = state.store.get_draft(&id).map_err(ApiError::from)?;
        let parsed = PipelineSpec::from_json(d.text.as_bytes()).map_err(ApiError::from);
        let mut out = json!({"draft_etag": d.etag, "pipeline": d.pipeline});
        match parsed {
            Err(e) => {
                out["parse"] = json!({"ok": false, "error": error_json(&e)});
            }
            Ok(spec) => {
                out["parse"] = json!({"ok": true, "authoring_mode": if spec.graph.is_some() { "graph" } else { "sql" }});
                out["validate"] = match run_validate(&state, &spec) { Ok(v) => json!({"ok": true, "report": v}), Err(e) => json!({"ok": false, "error": error_json(&e)}) };
                out["explain"] = match run_explain(&state, &spec) { Ok(v) => json!({"ok": true, "report": v}), Err(e) => json!({"ok": false, "error": error_json(&e)}) };
                if let Ok(cur) = state.store.get_pipeline(&d.pipeline) {
                    out["current"] = json!({"revision": cur.latest_revision, "etag": cur.etag, "spec": cur.spec});
                } else {
                    out["current"] = Value::Null;
                }
            }
        }
        out["scope"] = json!("validate checks the full PipelineSpec binding, I/O policy, SecretRefs and recovery admission at this moment; it does not connect to endpoints and publish re-validates");
        Ok(Json(out))
    })
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishBody {
    operation_id: String,
    draft_etag: String,
    #[serde(default)]
    base_etag: Option<String>,
}

fn publish_digest(id: &str, b: &PublishBody) -> String {
    let s = format!("{id}\n{}\n{}", b.draft_etag.trim_matches('"'), b.base_etag.as_deref().unwrap_or("absent").trim_matches('"'));
    sparrow_plugin::sha256(s.as_bytes())
}

async fn publish_draft(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    blocking_api(move || {
        let b: PublishBody = serde_json::from_slice(&body).map_err(|e| bad(format!("publish body: {e}")))?;
        cat::check_operation_id(&b.operation_id).map_err(ApiError::from)?;
        // Same operation id + same request → original receipt (no second publish);
        // same id with a different request is rejected.
        let digest = publish_digest(&id, &b);
        if let Some(r) = state.store.replay_publication(&b.operation_id, &digest).map_err(ApiError::from)? {
            return Ok((StatusCode::OK, Json(json!({"receipt": r, "started": false}))));
        }
        let d = state.store.get_draft(&id).map_err(ApiError::from)?;
        if d.etag != b.draft_etag.trim_matches('"') {
            return Err(ApiError::from(cat::conflict("draft changed since review; reload and review again", Some(d.etag))));
        }
        let spec = PipelineSpec::from_json(d.text.as_bytes()).map_err(ApiError::from)?;
        if serde_json::to_vec(&spec).map(|v| v.len()).unwrap_or(usize::MAX) > MAX_BODY {
            return Err(ApiError::from(SparrowError::new(ErrorCode::BoundExceeded, "published PipelineSpec exceeds 64 KiB")));
        }
        // Re-validate now: a draft that once passed is not trusted.
        run_validate(&state, &spec)?;
        let r = state
            .store
            .publish_draft(&b.operation_id, &digest, &p.actor, &id, &b.draft_etag, &d.pipeline, &spec, b.base_etag.as_deref())
            .map_err(ApiError::from)?;
        let _ = state.store.audit(&p.actor, "publish_draft", Some(&d.pipeline), Some(&format!("{} draft={id}", r.etag)), "ok");
        Ok((if r.replayed { StatusCode::OK } else { StatusCode::CREATED }, Json(json!({"receipt": r, "started": false, "note": "published only; start is a separate explicit action"}))))
    })
    .await
}

async fn get_publication(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let r = state.store.get_receipt(&id).map_err(ApiError::from)?;
        if r.actor != p.actor && p.role != Role::Admin {
            // Bound to the publishing actor; do not reveal other actors' receipts.
            return Err(ApiError::from(SparrowError::new(ErrorCode::NotFound, format!("unknown publication `{id}`"))));
        }
        Ok(Json(json!({"receipt": r, "retention": cat::RECEIPT_RETENTION, "note": "receipts beyond the retention window are deleted; an unknown id is not proof that nothing was published"})))
    })
    .await
}

#[derive(serde::Deserialize)]
struct RevQuery {
    before: Option<u64>,
    limit: Option<usize>,
}

async fn list_revisions(State(state): State<AppState>, Path(name): Path<String>, Query(q): Query<RevQuery>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let latest = state.store.get_pipeline(&name).map_err(ApiError::from)?;
        let revs = state.store.list_pipeline_revisions(&name, q.before, q.limit.unwrap_or(20)).map_err(ApiError::from)?;
        let desired = state.store.desired(&name).ok();
        let actual = state.store.actual(&name).ok();
        Ok(Json(json!({
            "name": name,
            "latest_revision": latest.latest_revision,
            "latest_etag": latest.etag,
            "desired_revision": desired.as_ref().and_then(|d| d.revision),
            "actual_revision": actual.as_ref().and_then(|a| a.revision),
            "revisions": revs,
            "scope": "all retained catalog revisions; the audit log is bounded and is not a publication history",
        })))
    })
    .await
}

async fn get_revision(State(state): State<AppState>, Path((name, revision)): Path<(String, u64)>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let row = state.store.get_pipeline_revision(&name, revision).map_err(ApiError::from)?;
        Ok(Json(json!({"name": row.name, "revision": row.latest_revision, "etag": row.etag, "spec": row.spec})))
    })
    .await
}

async fn list_secrets(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let names = state.store.list_secret_names().map_err(ApiError::from)?;
        Ok(Json(json!({"secrets": names.iter().map(|n| json!({"name": n, "present": true})).collect::<Vec<_>>(), "note": "values are write-only and never returned"})))
    })
    .await
}

fn connection_json(c: &sparrow_control::ConnectionRow) -> Value {
    json!({"name": c.name, "role": c.role, "kind": c.kind, "version": c.version, "etag": c.etag,
        "spec": serde_json::from_str::<Value>(&c.spec_json).unwrap_or(Value::Null), "updated_by": c.updated_by, "updated_at_ms": c.updated_at})
}

async fn list_connections(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let rows = state.store.list_connections().map_err(ApiError::from)?;
        Ok(Json(json!({"connections": rows.iter().map(connection_json).collect::<Vec<_>>(), "note": "author-side templates; published pipelines keep their expanded copy"})))
    })
    .await
}

async fn get_connection(State(state): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    blocking_api(move || {
        let c = state.store.get_connection(&name).map_err(ApiError::from)?;
        Ok(with_etag(&c.etag, Json(connection_json(&c))))
    })
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionBody {
    role: String,
    spec: Value,
}

/// Parse a template as the real SourceSpec/SinkSpec (deny_unknown_fields) so
/// a template can never hold fields a pipeline would reject.
fn check_connection_spec(role: &str, spec: &Value) -> ApiResult<String> {
    let kind = spec.get("kind").and_then(|k| k.as_str()).ok_or_else(|| bad("spec.kind is required"))?.to_string();
    match role {
        "source" => serde_json::from_value::<sparrow_control::SourceSpec>(spec.clone()).map(|_| ()).map_err(|e| bad(format!("source spec: {e}")))?,
        "sink" => serde_json::from_value::<sparrow_control::SinkSpec>(spec.clone()).map(|_| ()).map_err(|e| bad(format!("sink spec: {e}")))?,
        _ => return Err(bad("role must be source or sink")),
    }
    Ok(kind)
}

async fn put_connection(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    blocking_api(move || {
        let b: ConnectionBody = serde_json::from_slice(&body).map_err(|e| bad(format!("connection body: {e}")))?;
        let kind = check_connection_spec(&b.role, &b.spec)?;
        let raw = serde_json::to_string(&b.spec).map_err(|e| bad(e.to_string()))?;
        let expected = if_match(&headers);
        let row = state.store.put_connection(&name, expected.as_deref(), &b.role, &kind, &raw, &p.actor).map_err(ApiError::from)?;
        let _ = state.store.audit(&p.actor, "put_connection", Some(&name), Some(&row.etag), "ok");
        Ok(with_etag(&row.etag, (if expected.is_none() { StatusCode::CREATED } else { StatusCode::OK }, Json(connection_json(&row)))))
    })
    .await
}

async fn delete_connection(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<StatusCode> {
    blocking_api(move || {
        let want = if_match(&headers).ok_or_else(|| ApiError { status: StatusCode::PRECONDITION_REQUIRED, err: SparrowError::new(ErrorCode::Conflict, "If-Match is required") })?;
        state.store.delete_connection(&name, &want).map_err(ApiError::from)?;
        let _ = state.store.audit(&p.actor, "delete_connection", Some(&name), None, "ok");
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TestBody {
    #[serde(default)]
    stage: Option<String>,
}

/// Stage 1 `config`: strict SourceSpec/SinkSpec parse plus I/O policy
/// (allowlist / SecretRef presence) via a synthetic validate. Stage 2
/// `handshake` (admin only) is not implemented for any kind yet and always
/// answers `probe_unavailable`; no network connection is opened.
async fn test_connection(
    State(state): State<AppState>,
    axum::Extension(p): axum::Extension<Principal>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let b: TestBody = if body.is_empty() { TestBody::default() } else { serde_json::from_slice(&body).map_err(|e| bad(format!("test body: {e}")))? };
        let c = state.store.get_connection(&name).map_err(ApiError::from)?;
        let stage = b.stage.as_deref().unwrap_or("config");
        let spec: Value = serde_json::from_str(&c.spec_json).unwrap_or(Value::Null);
        match stage {
            "config" => {
                let parsed = check_connection_spec(&c.role, &spec);
                Ok(Json(json!({
                    "stage": "config", "connection": name, "kind": c.kind,
                    "ok": parsed.is_ok(),
                    "error": parsed.err().map(|e| error_json(&e)),
                    "scope": "schema check of the template only; destination policy and SecretRefs are checked by full pipeline validate",
                })))
            }
            "handshake" => {
                if p.role != Role::Admin {
                    return Err(ApiError { status: StatusCode::FORBIDDEN, err: SparrowError::new(ErrorCode::PolicyDenied, "network handshake probes require admin") });
                }
                Ok(Json(json!({"stage": "handshake", "connection": name, "kind": c.kind, "ok": false, "result": "probe_unavailable",
                    "reason": "no handshake-only probe is implemented for this kind; nothing was dialed"})))
            }
            _ => Err(bad("stage must be config or handshake")),
        }
    })
    .await
}

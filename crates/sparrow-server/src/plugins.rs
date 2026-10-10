use crate::*;
use axum::Extension;
use sparrow_control::plugins as control;
static PLUGIN_WORK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
type Permit = Arc<tokio::sync::SemaphorePermit<'static>>;
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/plugins", get(list))
        .route("/v1/plugins/{digest}/enable", post(enable))
        .route("/v1/plugins/{digest}/disable", post(disable))
        .route("/v1/plugins/{digest}/uninstall", post(uninstall))
        .route("/v1/plugins/{digest}/attest", post(attest))
        .route("/v1/plugins/{digest}/references", get(references))
        .route("/v1/pipelines/{name}/retire", post(retire))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(RequestBodyLimitLayer::new(MAX_BODY))
        .merge(
            Router::new()
                .route("/v1/plugins/install", post(install))
                .layer(DefaultBodyLimit::max(control::MAX_INSTALL_BODY))
                .layer(RequestBodyLimitLayer::new(control::MAX_INSTALL_BODY)),
        )
        .layer(axum::middleware::from_fn(admit))
}
async fn admit(mut request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let permit = match PLUGIN_WORK.try_acquire() {
        Ok(p) => Arc::new(p),
        Err(_) => {
            return ApiError::from(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "plugin management is busy; retry later",
            ))
            .into_response()
        }
    };
    request.extensions_mut().insert(permit);
    next.run(request).await
}
async fn list(
    State(state): State<AppState>,
    Extension(permit): Extension<Permit>,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let _permit = permit;
        let manager = control::manager(&state.store)?;
        Ok(Json(json!({"packages":manager.list()?,"native_allowed":manager.native_allowed(),
            "script_allowed":manager.script_allowed(),"wasm_allowed":manager.wasm_allowed(),"isolated_worker_slots":control::script_worker_slots(),"script_worker_slots":control::script_worker_slots(),
            "external_allowed":manager.external_allowed(),"external_sessions":control::external_sessions(),
            "signature_required":manager.signature_required(),"resident_generations":control::resident_count(),"hot_unload":false,"script_hot_unload":true})))
    }).await
}
async fn install(
    State(state): State<AppState>,
    Extension(permit): Extension<Permit>,
    Extension(principal): Extension<Principal>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let _permit = permit;
        let info = control::install(&state.store, &body)?;
        state.store.audit(
            &principal.actor,
            "plugin_install",
            Some(&info.manifest_sha256),
            None,
            "ok",
        )?;
        Ok(Json(json!(info)))
    })
    .await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Approval {
    approve_manifest_sha256: String,
}
async fn enable(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    Extension(permit): Extension<Permit>,
    Extension(principal): Extension<Principal>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let approval: Approval = serde_json::from_slice(&body).map_err(|_| {
        ApiError::from(SparrowError::new(
            ErrorCode::InvalidArgument,
            "enable requires exact hash approval",
        ))
    })?;
    blocking_api(move || {
        let _permit = permit;
        if state.safe_mode {
            return Err(ApiError::from(SparrowError::new(
                ErrorCode::PolicyDenied,
                "safe mode refuses plugin code activation",
            )));
        }
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            || approval.approve_manifest_sha256 != digest
        {
            return Err(ApiError::from(SparrowError::new(
                ErrorCode::InvalidArgument,
                "activation requires the exact manifest hash approval",
            )));
        }
        let manager = control::manager(&state.store)?;
        // Constructors may fail or terminate the process. Record intent before
        // crossing the trusted machine-code boundary, never just afterwards.
        state.store.audit(
            &principal.actor,
            "plugin_enable_attempt",
            Some(&digest),
            None,
            "requested",
        )?;
        let info = match manager.enable(&digest, &approval.approve_manifest_sha256) {
            Ok(info) => info,
            Err(error) => {
                let _ = state
                    .store
                    .audit(&principal.actor, "plugin_enable", Some(&digest), None, "failed");
                return Err(ApiError::from(error));
            }
        };
        state
            .store
            .audit(&principal.actor, "plugin_enable", Some(&digest), None, "ok")?;
        Ok(Json(json!(info)))
    })
    .await
}
async fn disable(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    Extension(permit): Extension<Permit>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let _permit = permit;
        let info = control::manager(&state.store)?.disable(&digest)?;
        state
            .store
            .audit(&principal.actor, "plugin_disable", Some(&digest), None, "ok")?;
        Ok(Json(json!(info)))
    })
    .await
}
async fn uninstall(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    Extension(permit): Extension<Permit>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let _permit = permit;
        state.store.uninstall_plugin(&digest)?;
        state
            .store
            .audit(&principal.actor, "plugin_uninstall", Some(&digest), None, "ok")?;
        Ok(Json(json!({"uninstalled":digest})))
    })
    .await
}
async fn attest(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    Extension(permit): Extension<Permit>,
    Extension(principal): Extension<Principal>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let signature = serde_json::from_slice(&body).map_err(|_| {
        ApiError::from(SparrowError::new(
            ErrorCode::InvalidArgument,
            "invalid plugin attestation",
        ))
    })?;
    blocking_api(move || {
        let _permit = permit;
        let info = control::manager(&state.store)?.attest(&digest, signature)?;
        state
            .store
            .audit(&principal.actor, "plugin_attest", Some(&digest), None, "ok")?;
        Ok(Json(json!(info)))
    })
    .await
}
async fn references(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    Extension(permit): Extension<Permit>,
) -> ApiResult<Json<Value>> {
    blocking_api(move || {
        let _permit = permit;
        Ok(Json(state.store.plugin_references(&digest)?))
    })
    .await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retirement {
    approve_etag: String,
}
async fn retire(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(permit): Extension<Permit>,
    Extension(principal): Extension<Principal>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let approval: Retirement = serde_json::from_slice(&body).map_err(|_| {
        ApiError::from(SparrowError::new(
            ErrorCode::InvalidArgument,
            "retirement requires exact current ETag approval",
        ))
    })?;
    blocking_api(move || {
        let _permit = permit;
        state.store.retire_pipeline(&name, &approval.approve_etag)?;
        state
            .store
            .audit(&principal.actor, "pipeline_retire", Some(&name), None, "ok")?;
        Ok(Json(json!({"retired":name,"external_files_deleted":false})))
    })
    .await
}

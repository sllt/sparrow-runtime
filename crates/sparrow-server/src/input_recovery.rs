use super::*;
use axum::extract::Query;

pub(super) async fn preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<sparrow_control::recovery_ops::RecoveryRequest>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(
        state
            .supervisor
            .recovery_preview(&name, request)
            .await
            .map_err(ApiError::from)?,
    ))
}
pub(super) async fn execute(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<sparrow_control::recovery_ops::RecoveryRequest>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(
        state
            .supervisor
            .recovery_execute(&name, request)
            .await
            .map_err(ApiError::from)?,
    ))
}
pub(super) async fn operations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || Ok(Json(state.store.recovery_operations(&name)?))).await
}
pub(super) async fn operation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let v = state.store.recovery_operation(&id)?.ok_or_else(|| {
            ApiError::from(SparrowError::new(
                ErrorCode::InvalidArgument,
                "operation missing",
            ))
        })?;
        if v["parent"] != name {
            return Err(ApiError::from(SparrowError::new(
                ErrorCode::InvalidArgument,
                "operation parent differs",
            )));
        }
        Ok(Json(v))
    })
    .await
}
pub(super) async fn finish(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(
        state
            .supervisor
            .recovery_finish(&name, &id)
            .await
            .map_err(ApiError::from)?,
    ))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Abort {
    reason: String,
}
pub(super) async fn abort(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
    Json(request): Json<Abort>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(
        state
            .supervisor
            .recovery_abort(&name, &id, request.reason)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(super) async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move ||Ok(Json(json!({"name":name,"input_dlq":sparrow_control::input_dlq::configured(&state.store,&name)?.status()?})))).await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Page {
    #[serde(default)]
    after: u64,
    #[serde(default = "page_size")]
    limit: usize,
}
fn page_size() -> usize {
    50
}
pub(super) async fn entries(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(page): Query<Page>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        Ok(Json(
            sparrow_control::input_dlq::configured(&state.store, &name)?
                .entries(page.after, page.limit)?,
        ))
    })
    .await
}
pub(super) async fn entry(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, position)): Path<(String, u64)>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let queue = sparrow_control::input_dlq::configured(&state.store, &name)?;
        Ok(Json(queue.entry(position)?))
    })
    .await
}
pub(super) async fn purge(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<sparrow_control::input_dlq::PurgeRequest>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    Ok(Json(
        state
            .supervisor
            .input_dlq_purge(&name, request)
            .await
            .map_err(ApiError::from)?,
    ))
}

//! Authenticated, bounded delivery administration. Paths come only from the
//! stored pipeline config, never from a request; output inspection is explicit.
use super::*;
use axum::extract::Query;

pub(super) async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let sender = state.supervisor.outbox_sender_snapshot(&name).await;
    blocking_api(move ||{
        let queue=sparrow_control::outbox::configured(&state.store,&name)?;
        Ok(Json(json!({"name":name,"outbox":queue.status()?,"sender":sender,
            "lifecycle":"delivery_is_independent_of_input_job; pause_outbox_to_stop_delivery; server_shutdown_joins_sender"})))
    }).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Page {
    #[serde(default = "pending")]
    state: String,
    #[serde(default)]
    after: u64,
    #[serde(default = "page_size")]
    limit: usize,
}
fn pending() -> String {
    "pending".into()
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
            sparrow_control::outbox::configured(&state.store, &name)?.entries(
                &page.state,
                page.after,
                page.limit,
            )?,
        ))
    })
    .await
}
pub(super) async fn entry(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        Ok(Json(
            sparrow_control::outbox::configured(&state.store, &name)?.entry(&id)?,
        ))
    })
    .await
}
pub(super) async fn command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<sparrow_control::outbox::OutboxCommand>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let value = state
        .supervisor
        .outbox_command(&name, request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(value))
}

//! Authenticated HTTP surface for immutable reference-table revisions.
//!
//! The catalog owns validation, hashing, revision allocation, CAS, and pin
//! accounting.  This module deliberately only translates bounded HTTP JSON
//! into the catalog API; it must not implement a second table store.  In
//! particular, a failed request is converted to a small, non-data-bearing
//! error so malformed rows can never be reflected back by the management API.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use sparrow_control::{
    MutationSpec, ReferenceTableMetadata, ReferenceTableRow, ReferenceTableSpec, RollbackSpec,
};
use sparrow_model::{ErrorCode, SparrowError};

use crate::{blocking_api, require_auth, ApiError, ApiResult, AppState};

/// `RequestBodyLimitLayer` is the authoritative process-wide limit.  Keep a
/// smaller named limit here too so this endpoint does not accidentally grow
/// beyond the documented management budget if the global limit changes.
pub(crate) const MAX_TABLE_BODY: usize = 64 * 1024;

/// The wire shape is intentionally strict.  The catalog receives the nested
/// object as JSON because it is also used by the non-HTTP control path; this
/// layer still rejects missing top-level members and unknown members before a
/// write is attempted.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PutTableBody {
    expected_revision: u64,
    table: TableBody,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableBody {
    fields: Vec<sparrow_plan::graph::FieldSpec>,
    keys: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl TableBody {
    fn into_spec(self) -> ReferenceTableSpec {
        ReferenceTableSpec {
            fields: self.fields,
            keys: self.keys,
            rows: self.rows,
        }
    }
}

fn parse_table_request<T: serde::de::DeserializeOwned>(body: &[u8]) -> ApiResult<T> {
    if body.len() > MAX_TABLE_BODY {
        return Err(table_error(SparrowError::new(
            ErrorCode::MaxRecordSize,
            "reference table request exceeds the management body limit",
        )));
    }
    serde_json::from_slice(body).map_err(|_| {
        table_error(SparrowError::new(
            ErrorCode::InvalidArgument,
            "invalid reference table request JSON",
        ))
    })
}

/// POST `/v1/tables/{name}/mutate`.  The body is the exact bounded catalog
/// batch shape, not arbitrary SQL or a remote CDC command.
pub(crate) async fn mutate_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let mutation: MutationSpec = parse_table_request(&body)?;
    blocking_api(move || {
        let result = state.store.mutate_reference_table(&name, &mutation);
        audit_table_change(
            &state,
            &actor,
            &name,
            "mutate_reference_table",
            &format!(
                "based_on_revision={};operations={}",
                mutation.expected_revision,
                mutation.operations.len()
            ),
            result,
        )
    })
    .await
}

/// POST `/v1/tables/{name}/rollback`.  Rollback publishes a new revision
/// containing retained historical rows; it never decrements latest_revision.
pub(crate) async fn rollback_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    let rollback: RollbackSpec = parse_table_request(&body)?;
    blocking_api(move || {
        let result = state.store.rollback_reference_table(&name, &rollback);
        audit_table_change(
            &state,
            &actor,
            &name,
            "rollback_reference_table",
            &format!(
                "based_on_revision={};target_revision={}",
                rollback.expected_revision, rollback.target_revision
            ),
            result,
        )
    })
    .await
}

fn audit_table_change(
    state: &AppState,
    actor: &str,
    name: &str,
    action: &str,
    operation_detail: &str,
    result: sparrow_model::Result<ReferenceTableRow>,
) -> ApiResult<Json<Value>> {
    match result {
        Ok(record) => {
            let detail = format!(
                "{operation_detail};revision={};sha256={}",
                record.revision, record.sha256
            );
            state
                .store
                .audit(actor, action, Some(name), Some(&detail), "ok")
                .map_err(table_error)?;
            Ok(Json(metadata_value(ReferenceTableMetadata::from(&record))?))
        }
        Err(error) => {
            let detail = format!("error_code={}", error.code.as_str());
            let _ = state
                .store
                .audit(actor, action, Some(name), Some(&detail), "failed");
            Err(table_error(error))
        }
    }
}

/// PUT `/v1/tables/{name}`.
pub(crate) async fn put_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let actor = require_auth(&state, &headers)?;
    if body.len() > MAX_TABLE_BODY {
        return Err(table_error(SparrowError::new(
            ErrorCode::MaxRecordSize,
            "reference table request exceeds the management body limit",
        )));
    }
    let request: PutTableBody = serde_json::from_slice(&body).map_err(|error| {
        table_error(SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("reference table request JSON: {error}"),
        ))
    })?;
    let expected = request.expected_revision;
    let table = request.table.into_spec();
    blocking_api(move || {
        let result = state.store.publish_reference_table(&name, expected, &table);
        match result {
            Ok(record) => {
                let revision = record.revision;
                let sha256 = record.sha256.as_str();
                let detail = format!("revision={revision};sha256={sha256}");
                state
                    .store
                    .audit(
                        &actor,
                        "put_reference_table",
                        Some(&name),
                        Some(&detail),
                        "ok",
                    )
                    .map_err(table_error)?;
                // A PUT response is metadata only.  Returning all rows here
                // would make a large publication needlessly expensive and
                // would turn an audit/client retry into a data dump.
                let metadata = redact_rows(row_value(record)?);
                let status = if expected == 0 {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                };
                Ok((status, Json(table_response(&name, metadata))))
            }
            Err(error) => {
                // Failed CAS/validation is auditable, but its detail is only
                // the stable error class.  Never log/return the submitted
                // fields or rows.
                let detail = format!("error_code={}", error.code.as_str());
                let _ = state.store.audit(
                    &actor,
                    "put_reference_table",
                    Some(&name),
                    Some(&detail),
                    "failed",
                );
                Err(table_error(error))
            }
        }
    })
    .await
}

/// GET `/v1/tables` — metadata only, never row contents.
pub(crate) async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let records = state
            .store
            .list_reference_tables()
            .map_err(table_error)?
            .into_iter()
            .map(metadata_value)
            .collect::<ApiResult<Vec<_>>>()?;
        Ok(Json(json!({"tables": records})))
    })
    .await
}

/// GET `/v1/tables/{name}` — latest revision plus the catalog's dependency
/// preview.  The catalog decides which pipeline revisions pin a table.
pub(crate) async fn get_latest_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let record = state
            .store
            .get_reference_table(&name)
            .map_err(table_error)?;
        Ok(Json(table_response(&name, row_value(record)?)))
    })
    .await
}

/// GET `/v1/tables/{name}/revisions/{revision}` — an immutable, explicitly
/// selected revision.  Unlike list/latest metadata, this endpoint may return
/// rows on success because it is the table inspection/preview endpoint.
pub(crate) async fn get_table_revision(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, revision)): Path<(String, u64)>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let record = state
            .store
            .get_reference_table_revision(&name, revision)
            .map_err(table_error)?;
        Ok(Json(table_response(&name, row_value(record)?)))
    })
    .await
}

/// GET `/v1/tables/{name}/revisions` — finite retained history, metadata only.
pub(crate) async fn list_table_revisions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let revisions = state
            .store
            .list_reference_table_revisions(&name)
            .map_err(table_error)?;
        if revisions.is_empty() {
            return Err(table_error(SparrowError::new(
                ErrorCode::InvalidArgument,
                "unknown reference table",
            )));
        }
        let records = revisions
            .into_iter()
            .map(metadata_value)
            .collect::<ApiResult<Vec<_>>>()?;
        Ok(Json(json!({"name": name, "revisions": records})))
    })
    .await
}

/// GET `/v1/tables/{name}/dependencies` — a bounded, read-only preview of
/// every persistent pipeline-revision pin.  The Store reports truncation
/// explicitly; GC still evaluates the complete relation independently.
pub(crate) async fn table_dependencies(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let preview = state
            .store
            .reference_table_dependencies(&name)
            .map_err(table_error)?;
        if preview.get("latest_revision").is_none_or(Value::is_null) {
            return Err(table_error(SparrowError::new(
                ErrorCode::InvalidArgument,
                "unknown reference table",
            )));
        }
        Ok(Json(preview))
    })
    .await
}

/// POST `/v1/tables/{name}/gc` — conservative catalog GC.  The Store must
/// calculate pins and delete under the same transaction boundary as its
/// pipeline-revision reads; the HTTP layer only exposes the bounded report.
pub(crate) async fn gc_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let actor = require_auth(&state, &headers)?;
    blocking_api(move || {
        let result = state.store.gc_reference_table(&name);
        match result {
            Ok(report) => {
                if report.get("latest_revision").is_none_or(Value::is_null) {
                    return Err(table_error(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "unknown reference table",
                    )));
                }
                let detail = gc_detail(&report);
                state
                    .store
                    .audit(
                        &actor,
                        "gc_reference_table",
                        Some(&name),
                        Some(&detail),
                        "ok",
                    )
                    .map_err(table_error)?;
                Ok(Json(json!({
                    "name": name,
                    "gc": bounded_gc_report(report),
                    "honesty": crate::HONESTY,
                })))
            }
            Err(error) => {
                let detail = format!("error_code={}", error.code.as_str());
                let _ = state.store.audit(
                    &actor,
                    "gc_reference_table",
                    Some(&name),
                    Some(&detail),
                    "failed",
                );
                Err(table_error(error))
            }
        }
    })
    .await
}

fn table_response(name: &str, record: Value) -> Value {
    match record {
        Value::Object(mut object) => {
            // Keep a stable top-level identity even if the Store's internal
            // record grows nested metadata in a later catalog version.
            object
                .entry("name")
                .or_insert_with(|| Value::String(name.to_owned()));
            Value::Object(object)
        }
        other => json!({"name": name, "table": other}),
    }
}

fn row_value(row: ReferenceTableRow) -> ApiResult<Value> {
    serde_json::to_value(row).map_err(|error| {
        table_error(SparrowError::new(
            ErrorCode::Internal,
            format!("encode reference table response: {error}"),
        ))
    })
}

fn metadata_value(row: ReferenceTableMetadata) -> ApiResult<Value> {
    serde_json::to_value(row).map_err(|error| {
        table_error(SparrowError::new(
            ErrorCode::Internal,
            format!("encode reference table metadata response: {error}"),
        ))
    })
}

/// Remove row arrays recursively from metadata and mutation responses.  This
/// intentionally does not touch successful revision inspection responses.
fn redact_rows(value: Value) -> Value {
    match value {
        Value::Object(mut object) => {
            object.remove("rows");
            for nested in object.values_mut() {
                let current = std::mem::replace(nested, Value::Null);
                *nested = redact_rows(current);
            }
            Value::Object(object)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(redact_rows).collect()),
        other => other,
    }
}

fn bounded_gc_report(value: Value) -> Value {
    // The Store report is already bounded by the catalog.  Still redact any
    // accidental row-shaped diagnostics before they reach a management
    // client; GC never needs to return table data.
    redact_rows(value)
}

fn gc_detail(report: &Value) -> String {
    let deleted = report.get("deleted").and_then(Value::as_u64).unwrap_or(0);
    let pinned = report
        .get("pinned_revisions")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    format!("deleted={deleted};pinned={pinned}")
}

fn table_error(error: SparrowError) -> ApiError {
    let conflict = is_revision_conflict(&error);
    let status = if conflict {
        StatusCode::PRECONDITION_FAILED
    } else {
        ApiError::from(error.clone()).status
    };
    // Keep only stable, non-data-bearing context.  In particular, a Store
    // implementation must not be able to expose an entire malformed row via
    // an otherwise useful validation message.
    let mut safe =
        SparrowError::new(error.code, safe_message(error.code)).retryable(error.retryable);
    for (key, value) in error.context {
        if matches!(
            key.as_str(),
            "revision"
                | "expected_revision"
                | "current_revision"
                | "latest_revision"
                | "table"
                | "pipeline"
                | "pins"
        ) {
            safe = safe.context(key, value.chars().take(128).collect::<String>());
        }
    }
    ApiError { status, err: safe }
}

fn is_revision_conflict(error: &SparrowError) -> bool {
    if error
        .context
        .iter()
        .any(|(key, _)| matches!(key.as_str(), "expected_revision" | "latest_revision"))
    {
        return true;
    }
    let message = error.message.to_ascii_lowercase();
    message.contains("revision")
        && (message.contains("conflict")
            || message.contains("expected")
            || message.contains("match")
            || message.contains("cas"))
}

fn safe_message(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidArgument => "invalid reference table request",
        ErrorCode::InvalidSchema => "invalid reference table schema",
        ErrorCode::TypeMismatch => "reference table row type mismatch",
        ErrorCode::BoundExceeded | ErrorCode::MaxRecordSize => {
            "reference table exceeds configured limits"
        }
        ErrorCode::PolicyDenied => "reference table operation denied",
        ErrorCode::ResourceExhausted => "reference table operation temporarily unavailable",
        _ => "reference table operation failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_b_metadata_redaction_removes_rows_at_every_nesting_level() {
        let value = json!({
            "name": "sites",
            "rows": [["secret"]],
            "pins": [{"pipeline": "p", "rows": [["also-secret"]]}]
        });
        let redacted = redact_rows(value);
        assert!(redacted.get("rows").is_none());
        assert!(redacted["pins"][0].get("rows").is_none());
    }

    #[test]
    fn core_b_malformed_table_errors_never_echo_input_data() {
        let marker = "sensitive-row-marker";
        let error = SparrowError::new(
            ErrorCode::InvalidSchema,
            format!("row {marker} has the wrong type"),
        );
        let api = table_error(error);
        assert!(!api.err.message.contains(marker));
    }

    #[test]
    fn core_b_revision_conflict_is_precondition_failed() {
        let error = SparrowError::new(ErrorCode::InvalidArgument, "revision conflict")
            .context("expected_revision", "3")
            .context("latest_revision", "4");
        assert_eq!(table_error(error).status, StatusCode::PRECONDITION_FAILED);
    }

    #[test]
    fn core_b_put_body_requires_all_members_and_rejects_unknown_fields() {
        let ok = br#"{"expected_revision":0,"table":{"fields":[],"keys":[],"rows":[]}}"#;
        assert!(serde_json::from_slice::<PutTableBody>(ok).is_ok());
        let missing = br#"{"expected_revision":0,"table":{"fields":[],"keys":[]}}"#;
        assert!(serde_json::from_slice::<PutTableBody>(missing).is_err());
        let unknown =
            br#"{"expected_revision":0,"table":{"fields":[],"keys":[],"rows":[],"secret":"x"}}"#;
        assert!(serde_json::from_slice::<PutTableBody>(unknown).is_err());
    }
}

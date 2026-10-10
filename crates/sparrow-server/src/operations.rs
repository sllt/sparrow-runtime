//! Bounded, authenticated operational views. No arbitrary file/log export.
use super::*;

pub(super) fn storage_json(s: &sparrow_runtime::checkpoint::CheckpointInventory) -> Value {
    fn hex(id: [u8; 16]) -> String {
        id.iter().map(|b| format!("{b:02x}")).collect()
    }
    json!({"current":s.current,"pinned_for_attempt":s.pinned,"current_error_code":s.current_error.map(|e|e.as_str()),
        "state_generation_marker":s.state_generation_marker.map(hex),"marker_error_code":s.marker_error.map(|e|e.as_str()),
        "logical_file_bytes":s.bytes,"maintenance_error_code":s.maintenance_error.map(|e|e.as_str()),
        "generations":s.generations.iter().map(|g|json!({"id":g.id,"bytes":g.bytes,"publication_proven":g.published,
            "snapshot_version":g.metadata.as_ref().map(|m|m.version),
            "revision":g.metadata.as_ref().and_then(|m|m.revision),"attempt":g.metadata.as_ref().and_then(|m|m.attempt),
            "state_generation":g.metadata.as_ref().and_then(|m|m.generation).map(hex),
            "metadata_scope":"cached_header_only_not_integrity_or_restore_authorization",
            "current":g.current,"compatibility":"checked_on_restore_not_by_listing"})).collect::<Vec<_>>()})
}
pub(super) fn checkpoint_status(state: &AppState, name: &str) -> Value {
    let (revision, control) = match state.supervisor.checkpoint_snapshot(name) {
        Ok(Some(s)) => s,
        Ok(None) => return json!({"available":false,"reason":"no_active_aligned_attempt"}),
        Err(_) => return json!({"available":false,"reason":"registry_busy"}),
    };
    let s = control.snapshot();
    let mut value = json!({"available":true,"scope":"running_attempt","running_revision":revision,
        "runtime_attempt_id":control.attempt,"policy":control.policy,
        "active":s.active,"phase":s.phase,"last_trigger":s.last_trigger,
        "started_total":s.started,"succeeded_total":s.succeeded,"failed_or_cancelled_total":s.failed,
        "busy_requests_total":s.busy,"timed_out_waiters_total":s.timed_out_waiters,
        "last_success_id":s.last_success_id,"restored_from_checkpoint":s.restored_from,
        "state_generation":s.state_generation.map(|id|id.iter().map(|b|format!("{b:02x}")).collect::<String>()),
        "downstream_semantics_changed":s.downstream_semantics_changed,
        "restore_compatibility":"profile_specific_source_state_and_reference_dependencies; plain_CP01_full_plan_strict; RCP2_state_upstream_prefixes_only_on_legacy_linear_profiles; full_plan_for_IoT_DAG_and_reference_profiles; see_effective_checkpoint_participants",
        "last_error_code":s.last_error.map(|e|e.as_str())});
    value["last_success_age_ms"] =
        json!(s
            .last_success_at
            .map(|at| at.elapsed().as_millis().min(u64::MAX as u128) as u64));
    value["storage"] = s.storage.as_ref().map(storage_json).unwrap_or(Value::Null);
    value["observed_source"]=s.observed_source.as_ref().map(|r|json!({
        "scope":"historical_committed_cut_not_live_connection_or_device_health",
        "checkpoint_id":r.checkpoint_id,"decision_sequence":r.sequence,"logical_micros":r.logical_micros,
        "source_offset":r.source_offset,"coverage_since_micros":r.coverage_since,
        "last_fresh_observation_micros":r.last_fresh,
        "view_age_ms":r.recorded_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
        "view_age_is_observation_age":false,"device_online_claimed":false})).unwrap_or(Value::Null);
    value["reliable_source"]=s.reliable_source.as_ref().map(|r|json!({
        "redeliveries_total":r.redeliveries,"pull_requests_total":r.pull_requests,"ack_retries_total":r.ack_retries,
        "retention_available_bytes":r.retention_available,
        "restored_cut":r.restored_cut,"published_cut":r.published_cut,"committed_cut":r.committed_cut,
        "pending_messages":r.pending_messages,"pending_bytes":r.pending_bytes,
        "max_pending_messages":r.max_pending_messages,"max_pending_bytes":r.max_pending_bytes,
        "sample_age_ms":r.sampled_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
        "sample_scope":"start_checkpoint_ACK_completion_and_5s_progress; not_per_record",
        "pending_includes_unconfirmed_ACKs":true,"ack_basis":"durable_checkpoint_plus_required_HTTP_2xx",
        "business_completion_claimed":false})).unwrap_or(Value::Null);
    if s.reliable_source.is_some() {
        let ack_basis=match state.store.get_pipeline_revision(name,revision) {
            Ok(row) if row.spec.sink.durable_outbox.is_some()=>"durable_checkpoint_plus_local_outbox_FULL_commit",
            Ok(_)=>"durable_checkpoint_plus_required_HTTP_2xx",
            Err(_)=>"unknown_running_revision_receipt",
        };
        value["reliable_source"]["ack_basis"]=json!(ack_basis);
        value["restore_compatibility"] = json!(
            "full_computation_and_source_reader_binding; in_place_semantic_fork_rejected; explicit_new_lineage_via_recovery_operations"
        );
    }
    value["contract"] = json!({"missed_ticks":"skip","interval_is_rpo_guarantee":false,
        "waiter_timeout_cancels_blocking_commit":false,"automatic_replay_default":false,
        "metadata_scope":"bounded_control_plane_not_payload_memory","snapshot":"attempt_local_component"});
    value
}

pub(super) async fn checkpoints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    let view = state
        .supervisor
        .checkpoint_inventory(&name)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(
        json!({"name":name,"revision":view.revision,"runtime_attempt_id":view.attempt,
        "scope":view.scope,"storage_sample":view.storage_sample,"storage":storage_json(&view.storage),"listing_changes_current":false}),
    ))
}

pub(super) async fn diagnose(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    require_auth(&state, &headers)?;
    blocking_api(move || {
        let body = status_body(&state, &name)?;
        let audit = state.store.list_audit(64).map_err(ApiError::from)?;
        let events: Vec<_> = audit.into_iter().filter(|e|e.target.as_deref()==Some(&name)).take(32)
            .map(|e|json!({"at_ms":e.at_ms,"action":e.action,"outcome":e.outcome})).collect();
        // K5.6: structured, redacted config summary (kinds/flags only; no
        // destinations, topics, SQL text or paths) and the error-code history.
        let config_summary = state.store.get_pipeline(&name).ok().map(|row| {
            let s = &row.spec;
            json!({"latest_revision": row.latest_revision, "mode": if s.graph.is_some() {"graph"} else {"sql"},
                "source_kind": s.source.kind, "sink_kind": s.sink.kind, "delivery": s.delivery, "recovery": s.recovery,
                "checkpoint_policy_present": s.checkpoint.is_some(), "graph_nodes": s.graph.as_ref().map(|g| g.nodes.len()),
                "graph_node_kinds": s.graph.as_ref().map(|g| g.nodes.iter().map(|n| n.kind.clone()).collect::<std::collections::BTreeSet<_>>()),
                "reference_tables": s.reference_tables.len(), "external_lookups": s.external_lookups.len()})
        });
        // Allowlist fields. Raw specs/SQL, destinations, SecretRefs, last_error
        // strings and audit details can contain user data and are not exported.
        let value = json!({"format":"sparrow-diagnostic-v1","name":name,
            "build":{"package":env!("CARGO_PKG_VERSION"),"commit":option_env!("SPARROW_BUILD_COMMIT").unwrap_or("unknown"),
                "demo_enabled":cfg!(feature="demo-io")},
            "revision":body["revision"],
            "config_summary":config_summary,
            "error_codes":{"last_error_code":body["actual"]["last_error_code"],"checkpoint_last_error_code":body["checkpoint"]["last_error_code"]},
            "actual":{"revision":body["actual"]["revision"],"status":body["actual"]["status"],
                "consecutive_failures":body["actual"]["consecutive_failures"],"restart_blocked":body["actual"]["restart_blocked"]},
            "effective":{"aligned_eligible":body["effective"]["aligned_eligible"],"recovery":body["effective"]["recovery"]},
            "observation":body["observation"],"mailboxes":body["mailboxes"],"checkpoint":body["checkpoint"],
            "histogram_contract":body["histogram_contract"],
            "logs":{"kind":"bounded_audit_summaries","events":events,"free_form_logs_included":false,
                "selection_scope":"latest_64_global_audit_events_then_target_filter","returned_limit":32,"empty_does_not_imply_no_history":true},
            "redaction":"allowlisted_fields_no_raw_spec_sql_destinations_secrets_or_free_form_errors"});
        if serde_json::to_vec(&value).map_err(|_|ApiError::from(SparrowError::new(ErrorCode::Internal,"diagnostic encoding")))?.len() > 128 * 1024 {
            return Err(ApiError::from(SparrowError::new(ErrorCode::BoundExceeded,"diagnostic bundle exceeds 128 KiB")));
        }
        Ok(Json(value))
    }).await
}

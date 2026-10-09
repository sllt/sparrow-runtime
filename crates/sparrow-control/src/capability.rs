//! Static implementation inventory, not a substitute for binding or certification.
use serde_json::{json, Value};
pub fn inventory() -> Value {
    // NATS Core joins the live matrix only when the build carries the `nats` feature.
    let nats: &[&str] = if cfg!(feature = "nats") {
        &["nats"]
    } else {
        &[]
    };
    // The JetStream Sink (PubAck-confirmed) needs the `jetstream` feature.
    let jetstream_sink: &[&str] = if cfg!(feature = "jetstream") {
        &["jetstream"]
    } else {
        &[]
    };
    // The WebSocket client Source/Sink needs the `websocket` feature.
    let websocket: &[&str] = if cfg!(feature = "websocket") {
        &["websocket"]
    } else {
        &[]
    };
    // The PostgreSQL query Source and INSERT/UPSERT Sink need `postgres`.
    let postgres: &[&str] = if cfg!(feature = "postgres") {
        &["postgres"]
    } else {
        &[]
    };
    // The in-process Local DataBus and the Redis Sink (own RESP2 client)
    // are always built.
    let sinks: Vec<&str> = ["http", "mqtt", "log", "databus", "influxdb", "redis"]
        .iter()
        .chain(nats)
        .chain(jetstream_sink)
        .chain(websocket)
        .chain(postgres)
        .chain(&["tcp"])
        .copied()
        .collect();
    let mut combinations: Vec<_> = ["mqtt", "http_push", "http_poll", "file", "databus"]
        .iter()
        .chain(nats)
        .chain(websocket)
        .chain(postgres)
        .chain(&["tcp"])
        .copied()
        .flat_map(|source| {
            sinks.clone().into_iter().map(move |sink| {
                json!({"source":source,"sink":sink,
            "graph":"linear","delivery":"live_best_effort","recovery":"restart_fresh",
            "configuration":"requires_schema_target_secret_and_budget_validation",
            "time_modes":["none","count","processing_time_tumbling","event_time_tumbling","event_time_hopping"],
            "state":"current_linear_operators_only; aligned_requires_separate_participant_checks",
            "certification":"not_claimed_by_static_inventory"})
            })
        })
        .collect();
    if cfg!(feature = "jetstream") {
        combinations.push(json!({"source":"file","sink":"jetstream",
            "graph":"linear","delivery":"at_least_once_into_stream","recovery":"aligned",
            "configuration":"requires_existing_stream_binding_subject",
            "time_modes":["none"],
            "state":"checkpoint_commits_after_all_pre_barrier_pub_acks; duplicates_possible_unless_msg_id_column",
            "certification":"not_claimed_by_static_inventory"}));
    }
    let reference_tables=json!({"maturity":"preview","certified":false,"frontend":false,
            "managed_lookup":"static_reference_profiles8_9_10_11_or_explicit_follow_latest_fresh","selection":"explicit_revision_and_sha256; opt-in follow_latest observes compatible head before IO and at subsequent batch boundaries",
            "publication":"immutable_revision_with_atomic_CAS_head_switch",
            "running_job":"static_keeps_bound_snapshot; follow_latest_publishes_job_owned_snapshots_per_operator_batch",
            "incremental":{"upsert":true,"delete":true,"atomic_cas":true,"max_operations":256,"rollback":"retained_contents_into_new_revision","history":"bounded; GC excludes latest and all persisted revision pins"},
            "live":{"refresh":"supervisor_converge_approximately_200ms_not_hard_latency","schema_change":"fail_job_no_stale_fallback","recovery":"fresh_only_explicit_restart","observation_log":false},
            "gc":"non_latest_unreferenced_only; all_persisted_pipeline_revisions_pin",
            "checkpoint_dependencies":{"profiles":{"file_stateless":"v8","file_state":"v9","jetstream":"v10","file_graph":"v11"},"source":"File/file_replay/replay or JetStream; graph sources are File only","sink":"required_HTTP","scope":"static Lookup plus Count/IoT ttl0 state; no ET/PT/temporal/Dedup/side/lossy","table_rows_in_checkpoint":false},
            "hysteresis_without_references":{"file":"v12","jetstream":"v13","scope":"new IoT kind only; ttl_micros=0; no automatic migration from v6/v7"},
            "temporal_managed_lookup":false,"external_async_lookup":true,
            "external":{"provider":"fixed_url_http_post","providers":{"http":"fixed_url_http_post_one_key_per_request","redis":"resp2_get_json_or_hmget_hash_pipelined_batch_keys_up_to_64","postgres":if cfg!(feature = "postgres") {"one_select_per_batch_key_any_or_rows_from_unnest_up_to_64_keys_unique_match_required"} else {"requires_postgres_build_feature"}},"interface":"shared_operator_cache_options_error_policy","max_inflight":16,"concurrency_scope":"per_physical_operator","timeout_ms":[10,5000],"max_frame_bytes":65536,"cache_bytes":1048576,"max_cache_ttl_ms":60000,"order":"input_order","errors":"fail_default; transport_only_null_or_drop_opt_in","negative_cache":"default_on_opt_out","timeout_counter":"runtime_deadline_expirations; provider_timeouts_are_failures","redirects":false,"proxy":false,"retry":false,"redis_stale_pooled_connection":"one_fresh_connection_resend_before_any_reply","postgres_stale_pooled_connection":"one_fresh_connection_resend_after_connection_loss_read_only","checkpoint":false},
            "max_bindings_per_pipeline":8,
            "max_rows_per_revision":crate::reference_table::MAX_REFERENCE_TABLE_ROWS,
            "max_payload_bytes_per_revision":crate::reference_table::MAX_REFERENCE_TABLE_BYTES});
    json!({"version":1,"combinations":combinations,
        "plugins":{"maturity":"development_preview","native_scalar":true,"native_default_enabled":false,"target":"Linux ELF64 GNU x86_64/aarch64; exact manifest target required","abi":1,"script":true,
            "javascript":{"engine":"QuickJS-ng 0.16.2 / rquickjs 0.14.0","default_enabled":false,"execution":"persistent process; fresh runtime/context per call","hot_unload":true,"preemptible":true,"process_isolation":true,"hardened_os_sandbox":false,"max_worker_slots":4,"process_address_space_bytes":134217728,"engine_heap_bytes":67108864,"call_timeout_ms":100,"load_timeout_ms":2000,"max_source_bytes":32768,"max_argument_payload_bytes":65536,"integer_mapping":"BigInt","async":false,"filesystem":false,"network":false,"module_loading":false,"compile_cache":true,"cache":{"scope":"worker_local_generated_only; exact_artifact_sha256; no_external_bytecode","max_bytes":262144,"mutable_context_reuse":false},"diagnostics":"phase_and_up_to_8_numeric_source_coordinates; no_user_messages_or_getters; not_authenticated_provenance","finite_query":true,"finite_budget":{"scope":"shared_query_work_units_and_absolute_deadline","call_admission_units":10000,"plus":"argument_payload_bytes","vm_instruction_meter":false}},
            "wasm":true,"wasm_scalar":{"engine":"wasmi 2.0.0","execution":"isolated interpreter; immutable module; fresh instance per call","abi":"wasm32-sparrow-scalar-v1","default_enabled":false,"max_module_bytes":131072,"linear_memory_bytes":16777216,"fuel_per_call":1000000,"call_timeout_ms":100,"imports":false,"wasi":false,"jit":false,"start_function":false,"finite_query":true,"hot_unload":true,"shared_js_wasm_worker_slots":4,"compiled_module_memory":"bounded by worker 128MiB address-space; module metadata bytes is input size, not compiled IR size"},"external_source_sink":true,"external_transform":true,"external":{"protocol":"sparrow-extension-ipc-v1","default_enabled":false,"process_isolation":true,"os_sandbox":false,"permissions":"administrator_approved_claims_not_syscall_filters","max_sessions":8,"max_frame_bytes":65536,"max_rows":32,"max_columns":16,"max_config_bytes":4096,"call_timeout_ms":1000,"address_space_bytes":134217728,"source_ack":"volatile_queue_admission_only","sink_retry":false,"automatic_restart":false,"finite_query":false,"hot_unload":true,"preemptible":true},"sandbox":false,"preemptible":false,"hot_unload":false,"recovery":"restart_fresh_only","binding":"literal package/version/manifest_sha256/function; per-catalog registry","max_packages":16,"max_resident_generations_per_process":16,"max_artifact_bytes":4194304,"signature_verification":true,"package_formats":[1,2],"package_dependencies":{"exact_hash":true,"max_direct":8,"max_depth":8,"network_resolution":false},"persistent_references":"catalog v4; all retained revisions; uninstall protected; stopped fresh retirement requires exact ETag","trust":"explicit administrator hash approval plus optional/required Ed25519 publisher policy; not code-safety proof"},
        "actions":{"maturity":"development_preview","version":1,"certified":false,
            "sinks":["http","mqtt","log","file"],"body":"typed_json_tree_explicit_field_references",
            "destinations":"fixed_HTTP_origin_path_headers_with_query_templates; MQTT_single_level_variables",
            "multi_action":"existing_DAG_required_or_best_effort_sinks","recovery":"restart_fresh_only_no_output_ID",
            "http_per_row":"single_or_query_requires_max_inflight_1_batch_rows_1_linger_0; retries_may_duplicate",
            "file":{"enabled_by_platform":cfg!(target_os="linux"),"format":"ndjson","directory":"exclusive_existing_allowlisted_directory",
                "bounds":"row_bytes_segment_bytes_max_bytes_max_files","rotation":"new_segment_no_overwrite_no_auto_delete",
                "sync":"optional_sync_data_each_batch","restart":"new_segment; partial_tail_rejected; no_source_replay_deduplication"}},
        "jetstream":{"enabled_by_build":cfg!(feature="jetstream"),"maturity":"preview","requires_feature":"jetstream",
            "idle_backoff_max_ms":{"default":250,"range":[5,250],"scope":"regular_reliable_actor_not_durable_time_or_observed_profiles","tradeoff":"lower_idle_latency_more_empty_pulls"},
            "source":"jetstream","sink":"http","delivery":"checkpointed_at_least_once","recovery":"aligned","snapshot_version":4,
            "snapshot_version_scope":"legacy_zero_or_count_only; IoT uses profiles.reliable_iot; Hysteresis uses v13; references use v10",
            "state_shapes":["zero_state","single_count_window","two_count_windows","single_change_detect_ttl0","single_deadband_ttl0","count_plus_iot_ttl0","two_iot_ttl0"],
            "profiles":{"legacy_count":"v4","reliable_iot":"v7","reference_jetstream":"v10"},"consumer_scope":"cooperative_single_node_no_HA",
            "poison":"fail_finite_retry_or_held","dlq":false,"durable_outbox":false,"automatic_resume_required":true,
            "source_filtering":false,"semantic_fork_and_fixed_replay":false,"certified":false},
        "function_semantics":{"version":sparrow_expr::semantics::VERSION,"evaluation":sparrow_expr::semantics::EVALUATION,
            "pure_deterministic":true,"entries":sparrow_expr::semantics::FUNCTIONS.iter().map(|f|json!({"name":f.name,
                "min_args":f.min_args,"max_args":f.max_args,"input":f.input,"null_policy":f.null_policy,
                "output_bound":f.output_bound,"work_bound":f.work_bound})).collect::<Vec<_>>()},
        "aligned":{"scope":"legacy_linear_v3_profile","source":"file","state_shapes":["zero_state","single_count_window","single_et_tumbling_window","single_et_hopping_window","two_count_windows"],
            "snapshot_version":3,"max_state_participants":2,"old_snapshot_migration":"explicit_fresh_or_original_backup_binary_no_automatic_conversion",
            "excluded":["more_than_two_states","mixed_time_multiple_states","processing_time_window","deduplicate","lookup","multiple_sources","branching"],
            "additional_checks":["complete_state_semantics","source_identity","committed_checkpoint","required_sink_flush"],
            "periodic_checkpoint":true,"automatic_replay":"explicit_resume_latest_or_restore_only"},
        "backend":{"layout":"row","prepared":true,"fusion":true,"arrow":false,"jit":false,"dag":true},
        "dag":{"maturity":"preview","certified":false,"max_nodes":64,"max_edges":128,"max_ports":16,
            "operators":["branch","route_first_match","route_all_match","union_all","multiple_sources","multiple_sinks"],
            "source_binding":"explicit_graph_io_by_operator_id","source_time":"explicit_event_time_field_before_filter",
            "side_outputs":["file_decode_error","event_time_late","filter_rule_reject"],
            "aligned":"required_File_to_HTTP_stateless_or_Count_v5_or_IoT_v6_ttl0; immutable_reference_profile11_supports_Count_or_IoT_ttl0; separate_time_graph_v18_v19; side_outputs_not_admitted",
            "best_effort":"explicit_data_drop; control_overflow_detaches_branch; no_lossy_rejoin","designer":false},
        "time_graph":{"maturity":"preview","certified":false,"source":"File_only","sink":"all_required_verified_HTTP",
            "source_schema":"scalar_fields_only; full_input_fingerprint_before_projection",
            "profiles":{"processing_time":18,"event_time":19},"manifest":"CPL1/CP01DAG2","max_states":16,
            "processing_time":"paused; PT_tumbling_Count_and_IoT; positive_TTL_ChangeDetect_Deadband",
            "event_time":"tumbling_hopping_Count_and_TTL0_IoT; all_sources_explicit_time_binding",
            "ordering":"logged_global_decision; bounded_fixed_physical_edge_Union_order; not_original_cross_source_order",
            "recovery":"CURRENT_plus_one_TIME_PENDING; all_source_progress_Union_state_and_per_sink_output_cursors",
            "idle_after_ms":"optional_100..86400000; absent_does_not_infer_idle",
            "decision_interval_ms":"100..1000; checkpoint_after_each_input_EOF_or_idle_tick",
            "excluded":["PT_ET_mixed","JetStream_graph","references","Dedup","side_outputs","lossy_edges","historical_replay","exactly_once"],
            "capacity":"serialized_fsync_and_required_HTTP; bounded_Union_round_buffer; not_high_throughput"},
        "iot":{
            "maturity":"preview","certified":false,
            "operators":["change_detect","deadband","hysteresis"],
            "input_types":["bool","int64","uint64","float64","utf8","bytes","timestamp_micros_utc"],"key_fields_max":16,
            "state":"task_owned_bounded_key_state",
            "semantics":{
                "change_detect":"typed equality; emit_first and invalid policy are explicit",
                "deadband":"absolute_or_relative_threshold; baseline is explicit last_input_or_last_output",
                "hysteresis":"high/low Schmitt trigger; finite separated enter/exit thresholds; emits only initial/transition rows",
                "invalid":"error_or_ignore; ignored values do not update state",
                "ttl":"processing_time_only; aligned requires ttl_micros=0"
            },
            "fresh":{
                "sources":["file","mqtt","http_push"],"sinks":["http","mqtt","log"],
                "graph":true,"recovery":"restart_fresh","continuity":"state starts empty; no persisted state_generation"
            },
            "aligned":{
                "sources":["file","file_replay","replay"],"required_sinks":["http"],
                "linear_max_state_participants":2,"graph_max_state_participants":16,
                "snapshot_version":6,"profile":"iot_v6","profiles":{"legacy":"iot_v6","hysteresis":"hysteresis_v12"},
                "legacy_scope":"snapshot_version=6 is legacy change_detect/deadband only; Hysteresis uses profile hysteresis_v12",
                "ttl_micros":0,"source_time":false,"side_outputs":false,"lossy_edges":false,
                "continuity":"preserved only from a compatible matching profile checkpoint"
            },
            "jetstream":{"supported":cfg!(feature="jetstream"),"reason":if cfg!(feature="jetstream"){"IoT state uses independent reliable profiles; legacy v7 and Hysteresis v13; TTL must be zero"}else{"JetStream support requires the jetstream build feature"},"snapshot_version":7,"profile":"reliable_iot_v7","profiles":{"legacy":"reliable_iot_v7","hysteresis":"hysteresis_reliable_v13"},"legacy_scope":"snapshot_version=7 is legacy reliable change_detect/deadband only; Hysteresis uses profile hysteresis_reliable_v13","ttl_micros":0,"required_sink":"http"},
            "reset":"fresh/reset is explicit; prior state continuity is not implied",
            "resource_contract":"max_keys and state bytes are bounded; quota failure preserves the prior entry"
        },
        "alarm":{"maturity":"development_preview","certified":false,"operator":"alarm",
            "profiles":{"file":20,"jetstream":if cfg!(feature="jetstream"){json!(21)}else{json!("feature_required")},"required_file_graph":22},
            "clock":"paused_source_ordered; due_before_input","sink":"required_verified_HTTP_with_output_ID",
            "phases":["normal","pending","active","recovering"],"events":["activate","resolve","notify"],
            "state_events":"never_suppressed_by_cooldown","notifications":"one_latest_activation_pending_per_key; resolve_bypasses_and_cancels_pending",
            "identity":"generation+operator+key_values+episode; reset_requires_new_namespace",
            "bounds":"48_scalar_input_fields; max_keys_and_bytes; two_timer_slots_per_key; positive_notification_max_age",
            "not_enabled":["event_time","references","lossy_or_side_outputs","periodic_reminders","offline_detection","resample","exactly_once"]},
        "silence":{"maturity":"development_preview","certified":false,"operator":"silence",
            "mqtt_live":{"clock":"live","recovery":"restart_fresh","delivery":"live_best_effort",
                "topology":"MQTT_QoS0_clean_session_then_silence_then_optional_pure_transforms_then_HTTP",
                "coverage":"fresh_PINGRESP_and_FIFO; full_new_grace_after_discontinuity; not_broker_catchup",
                "durable":false,"retained":"ignored_and_breaks_coverage","gap_micros":[100000,120000000]},
            "profiles":{"file":23,"jetstream":if cfg!(feature="jetstream"){json!(24)}else{json!("feature_required")}},
            "topology":"append_only_File_or_JetStream -> silence_as_first_state -> optional_pure_transforms -> required_HTTP",
            "clock":"paused_source_observed; input_before_current_feed_fact; downtime_paused",
            "coverage":"fresh_caught_up_prefix_only; unknown_backlog_partial_inflight_slow_probe_or_restart_breaks_coverage; full_new_grace",
            "device_scope":"observed_keys_union_bounded_static_registry; silence_is_not_hardware_failure",
            "events":["silent","resumed"],"output":"keys_plus_seven_event_columns; never_seen_last_seen_is_null; stable_HTTP_output_ID",
            "identity":"generation+operator+key_values+episode; only_an_actual_record_resumes_the_episode",
            "durability":"OFC1_cut_and_OFD1_TIME_PENDING; persist_before_publish; required_flush_before_CURRENT_before_source_ACK",
            "diagnostics":"observed_source_is_a_historical_committed_cut_not_current_source_health",
            "bounds":"48_scalar_input_fields; max_keys_and_bytes; one_timer_per_key; registry_at_most_1024_keys_and_64KiB_canonical",
            "not_enabled":["mqtt_recovery","http_push","upstream_transforms","other_state_nodes","dag","event_time","references","historical_replay","exactly_once"]},
        "resample":{"maturity":"development_preview","certified":false,"operator":"resample",
            "modes":["last","mean","interpolate"],
            "profiles":{"file":25,"jetstream":if cfg!(feature="jetstream"){json!(26)}else{json!("feature_required")}},
            "topology":"append_only_File_or_JetStream -> optional_pure_transforms -> one_Resample -> optional_pure_transforms -> required_HTTP",
            "clock":"paused_source_ordered; due_before_input; downtime_paused; equality_expires_first",
            "values":"complete_numeric_vectors; last_preserves_integer_types; mean_and_interpolate_are_float64",
            "missing":"NULL_vector_for_known_keys_only; no_extrapolation_or_forward_fill",
            "bounds":"1..16_keys_and_values; 48_flat_fields; max_keys_bytes_timers_and_1..4096_emissions_per_decision; excessive_catch_up_fails",
            "durability":"v25/v26_with_PTC1_and_TPD1; stable_HTTP_output_ID; full_plan_match",
            "not_enabled":["MQTT","DAG","other_state_nodes","event_time","references","historical_replay","exactly_once"]},
        "paused_time_iot":{"maturity":"preview","certified":false,"operators":["hold_for","debounce"],
            "source_schema":"scalar_fields_only; full_input_fingerprint_before_projection",
            "sources":{"file":"v14","jetstream":if cfg!(feature="jetstream"){"v15"}else{"feature_required"}},
            "sink":"required_http_with_stable_output_identity","state":"one_timed_operator_linear_no_references",
            "clock":"source_ordered_processing_time; downtime_paused; due_before_input; no_event_time",
            "durability":"TIME_PENDING before publication; checkpoint after every decision; CURRENT_only_restore",
            "performance":"one_input_row_or_idle_tick_per_commit; serialized_fsync_and_HTTP_flush",
            "not_enabled":["timed_dag","historical_replay","exactly_once"],
            "linear_time_extension":{"file":"v16","jetstream":if cfg!(feature="jetstream"){"v17"}else{"feature_required"},
                "max_states":2,"windows":["processing_time_tumbling","count"],
                "iot":["change_detect","deadband","hysteresis_ttl0","hold_for","debounce"],
                "positive_ttl":"change_detect_and_deadband_only; last_valid_input; no_alarm_resolve_on_expiry",
                "ordering":"time_control_before_timer_rows; downstream_due_before_upstream_derived_rows",
                "excluded":["event_time","timed_dag","references","side_outputs","more_than_two_states"],
                "certified":false}},
        "reference_tables":reference_tables,
        "windows":{"implemented":["count","processing_time_tumbling","event_time_tumbling","event_time_hopping","processing_time_hopping","sliding_count","processing_time_sliding","event_time_sliding","processing_time_session","event_time_session"],
            "new_families":{"maturity":"development_preview","recovery":"restart_fresh_only","late_policy":"final_only_L0_no_retractions",
                "max_buffered_rows":{"default":1024,"maximum":16384},"budget":"per_key_aggregate_inputs_plus_job_memory_keys_timers; overflow_fails_without_silent_drop",
                "sliding":"one_trigger_per_event; exclusive_lower_inclusive_upper; PT_zero_delay_emits_arrival_prefix",
                "session":"gap_and_max_duration; equality_starts_new_session; open_ET_sessions_can_resegment"},
            "not_implemented":["session_late_corrections","new_family_checkpoint_restore","unbounded_global"],"new_window_policy":"add_only_with_workload_semantics_and_independent_reference"},
        "analysis":{"maturity":"development_preview","certified":false,"recovery":"restart_fresh_only",
            "unnest":{"max_rows":4096,"default_max_rows":1024,"null_empty":"zero_rows","ordinal":"one_based_per_operator_input; source_operator_preserved_when_known","controls":"after_all_expanded_rows"},
            "joins":{"kinds":["interval_inner","interval_left","tumbling_window_inner","tumbling_window_left"],"inputs":"two_direct_explicit_event_time_sources","late":"error_L0","idle":"does_not_authorize_cleanup_or_unmatched","max_rows_per_side":4096,"default_max_rows_per_side":1024,"max_matches_per_row":4096,"default_max_matches_per_row":1024},
            "finite_query":{"endpoint":"POST /v1/query","io":"inline_rows_or_client_prepared_history_only; no_source_sink_IO","concurrency":2,"kernel":"independent","request_bytes":65536,"sql_bytes":8192,"max_input_rows":4096,"max_output_rows":4096,"max_output_bytes":1048576,"max_timeout_ms":30000}},
        "sql":{"runtime_parser":"sparrow_sql_subset","aggregates":["count","sum","avg","min","max","first","last","var_pop","var_samp","stddev_pop","stddev_samp"],
            "extended_aggregates":{"recovery":"restart_fresh_only","order":"arrival_order_or_buffered_ET_timestamp_then_arrival","moments":"Welford_f64_finite; no_merge_codec; integers_may_lose_precision_above_2pow53"},
            "not_full_ansi_sql":true,"differential_test_parser":"sqlparser 0.62.0 (testkit only)"},
        "http":{"batch":true,"linger":true,"max_inflight":8,"max_outbox_items":1024,
            "durable_outbox":false,"exactly_once":false,"oversized_output":"bounded_rejection_not_implicit_splitting"},
        "formats":{"json":true,"protobuf":false,"csv":false,"arrow_ipc":false,"parquet":false},
        "limits":{"pipeline_spec_bytes":crate::spec::MAX_SPEC_BYTES,"sql_bytes":crate::spec::MAX_SQL_BYTES,
            "inbox_items_max":4096,"outbox_items_max":4096,"checkpoint_snapshot_bytes":sparrow_runtime::checkpoint::MAX_SNAPSHOT_BYTES},
        "time_contract":"local_observation_is_not_device_age_or_business_ack",
        "guarantees":"build availability, validation, plan eligibility, checkpoint compatibility, health and target certification are distinct"})
}

#[cfg(test)]
mod tests {
    #[test]
    fn production_inventory_does_not_claim_unimplemented_backends_or_certification() {
        let value = super::inventory();
        // 5 live/file sources (mqtt, http_push, http_poll, file, databus) x 5 sinks
        // (http, mqtt, log, databus, influxdb, redis), plus NATS Core as both a source and a sink
        // when the `nats` feature is built, plus the JetStream Sink (live matrix +
        // one aligned File profile) with `jetstream`, plus WebSocket as both a
        // source and a sink with `websocket`, plus PostgreSQL as both a source
        // and a sink with `postgres`.
        let (nats, jetstream, websocket, postgres) = (
            usize::from(cfg!(feature = "nats")),
            usize::from(cfg!(feature = "jetstream")),
            usize::from(cfg!(feature = "websocket")),
            usize::from(cfg!(feature = "postgres")),
        );
        let expected =
            (5 + nats + websocket + postgres + 1) * (6 + nats + jetstream + websocket + postgres + 1) + jetstream;
        assert_eq!(value["combinations"].as_array().unwrap().len(), expected);
        assert!(value["combinations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["source"] == "nats"
                || c["sink"] == "nats"
                || c["source"] == "websocket"
                || c["sink"] == "websocket"
                || c["source"] == "tcp"
                || c["sink"] == "tcp"
                || c["source"] == "databus"
                || c["sink"] == "databus"
                || c["sink"] == "redis"
                || c["source"] == "postgres"
                || c["sink"] == "postgres"
                || c["sink"] == "influxdb"
                || (c["sink"] == "jetstream" && c["source"] != "file"))
            .all(|c| c["delivery"] == "live_best_effort" && c["recovery"] == "restart_fresh"));
        assert!(value["combinations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["source"] == "http_poll")
            .all(|c| c["delivery"] == "live_best_effort" && c["recovery"] == "restart_fresh"));
        assert_eq!(value["backend"]["jit"], false);
        assert_eq!(value["backend"]["dag"], true);
        assert_eq!(value["dag"]["certified"], false);
        assert_eq!(value["iot"]["aligned"]["snapshot_version"], 6);
        assert_eq!(value["iot"]["aligned"]["ttl_micros"], 0);
        assert_eq!(
            value["iot"]["jetstream"]["supported"],
            cfg!(feature = "jetstream")
        );
        assert_eq!(value["iot"]["jetstream"]["snapshot_version"], 7);
        assert!(value["dag"]["aligned"]
            .as_str()
            .unwrap()
            .contains("IoT_v6_ttl0"));
        assert!(value["combinations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["certification"] == "not_claimed_by_static_inventory"));
    }

    #[test]
    fn k4_capability_inventory_matches_scalar_iot_contract() {
        let value = super::inventory();
        assert_eq!(
            value["iot"]["input_types"],
            serde_json::json!([
                "bool",
                "int64",
                "uint64",
                "float64",
                "utf8",
                "bytes",
                "timestamp_micros_utc"
            ])
        );
        assert_eq!(
            value["iot"]["fresh"]["continuity"],
            "state starts empty; no persisted state_generation"
        );
    }

    #[test]
    fn core_a_jetstream_iot_inventory_exposes_independent_v7_profile() {
        let value = super::inventory();
        assert_eq!(value["jetstream"]["profiles"]["legacy_count"], "v4");
        assert_eq!(value["jetstream"]["profiles"]["reliable_iot"], "v7");
        assert_eq!(value["iot"]["jetstream"]["snapshot_version"], 7);
        assert_eq!(value["iot"]["jetstream"]["ttl_micros"], 0);
        assert_eq!(value["iot"]["jetstream"]["required_sink"], "http");
        assert!(value["jetstream"]["state_shapes"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("two_iot_ttl0")));
        assert!(value["jetstream"]["snapshot_version_scope"]
            .as_str()
            .unwrap()
            .contains("legacy_zero_or_count_only"));
    }

    #[test]
    fn silence_inventory_keeps_observed_profiles_and_exclusions_explicit() {
        let value = super::inventory();
        let silence = &value["silence"];
        assert_eq!(silence["certified"], false);
        assert_eq!(silence["profiles"]["file"], 23);
        assert_eq!(
            silence["profiles"]["jetstream"],
            if cfg!(feature = "jetstream") {
                serde_json::json!(24)
            } else {
                serde_json::json!("feature_required")
            }
        );
        assert_eq!(silence["events"], serde_json::json!(["silent", "resumed"]));
        assert_eq!(silence["mqtt_live"]["durable"], false);
        for excluded in [
            "mqtt_recovery",
            "http_push",
            "dag",
            "event_time",
            "references",
            "upstream_transforms",
            "other_state_nodes",
        ] {
            assert!(silence["not_enabled"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(excluded)));
        }
        assert!(silence["diagnostics"]
            .as_str()
            .unwrap()
            .contains("not_current_source_health"));
        assert_eq!(value["alarm"]["profiles"]["file"], 20);
        assert_eq!(value["paused_time_iot"]["sources"]["file"], "v14");
    }
}

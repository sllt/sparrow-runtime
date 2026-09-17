//! Static implementation inventory, not a substitute for binding or certification.
use serde_json::{json, Value};
pub fn inventory() -> Value {
    let combinations: Vec<_> = ["mqtt", "http_push", "file"]
        .into_iter()
        .flat_map(|source| {
            ["http", "mqtt", "log"].into_iter().map(move |sink| {
                json!({"source":source,"sink":sink,
            "graph":"linear","delivery":"live_best_effort","recovery":"restart_fresh",
            "configuration":"requires_schema_target_secret_and_budget_validation",
            "time_modes":["none","count","processing_time_tumbling","event_time_tumbling","event_time_hopping"],
            "state":"current_linear_operators_only; aligned_requires_separate_participant_checks",
            "certification":"not_claimed_by_static_inventory"})
            })
        })
        .collect();
    json!({"version":1,"combinations":combinations,
        "jetstream":{"enabled_by_build":cfg!(feature="jetstream"),"maturity":"preview","requires_feature":"jetstream",
            "source":"jetstream","sink":"http","delivery":"checkpointed_at_least_once","recovery":"aligned","snapshot_version":4,
            "state_shapes":["zero_state","single_count_window","two_count_windows"],"consumer_scope":"cooperative_single_node_no_HA",
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
            "aligned":"required_File_to_HTTP_stateless_or_Count_v5_or_IoT_v6_ttl0; side_outputs_and_ET_not_admitted",
            "best_effort":"explicit_data_drop; control_overflow_detaches_branch; no_lossy_rejoin","designer":false},
        "iot":{
            "maturity":"preview","certified":false,
            "operators":["change_detect","deadband"],
            "input_types":["bool","int64","uint64","float64","utf8","bytes","timestamp_micros_utc"],"key_fields_max":16,
            "state":"task_owned_bounded_key_state",
            "semantics":{
                "change_detect":"typed equality; emit_first and invalid policy are explicit",
                "deadband":"absolute_or_relative_threshold; baseline is explicit last_input_or_last_output",
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
                "snapshot_version":6,"profile":"iot_v6",
                "ttl_micros":0,"source_time":false,"side_outputs":false,"lossy_edges":false,
                "continuity":"preserved only from a compatible committed v6 snapshot"
            },
            "jetstream":{"supported":false,"reason":"IoT state is not admitted on the JetStream v4 profile"},
            "reset":"fresh/reset is explicit; prior state continuity is not implied",
            "resource_contract":"max_keys and state bytes are bounded; quota failure preserves the prior entry"
        },
        "windows":{"implemented":["count","processing_time_tumbling","event_time_tumbling","event_time_hopping"],
            "not_implemented":["session","sliding_count","unbounded_global"],"new_window_policy":"add_only_with_workload_semantics_and_independent_reference"},
        "sql":{"runtime_parser":"sparrow_sql_subset","aggregates":["count","sum","avg","min","max"],
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
        assert_eq!(value["combinations"].as_array().unwrap().len(), 9);
        assert_eq!(value["backend"]["jit"], false);
        assert_eq!(value["backend"]["dag"], true);
        assert_eq!(value["dag"]["certified"], false);
        assert_eq!(value["iot"]["aligned"]["snapshot_version"], 6);
        assert_eq!(value["iot"]["aligned"]["ttl_micros"], 0);
        assert_eq!(value["iot"]["jetstream"]["supported"], false);
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
}

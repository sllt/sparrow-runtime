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
        "aligned":{"source":"file","state_shapes":["zero_state","single_count_window","single_et_tumbling_window","single_et_hopping_window","two_count_windows"],
            "snapshot_version":3,"max_state_participants":2,"old_snapshot_migration":"explicit_fresh_or_original_backup_binary_no_automatic_conversion",
            "excluded":["more_than_two_states","mixed_time_multiple_states","processing_time_window","deduplicate","lookup","multiple_sources","branching"],
            "additional_checks":["complete_state_semantics","source_identity","committed_checkpoint","required_sink_flush"],
            "periodic_checkpoint":true,"automatic_replay":"explicit_resume_latest_or_restore_only"},
        "backend":{"layout":"row","prepared":true,"fusion":true,"arrow":false,"jit":false,"dag":false},
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
        assert_eq!(value["backend"]["dag"], false);
        assert!(value["combinations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["certification"] == "not_claimed_by_static_inventory"));
    }
}

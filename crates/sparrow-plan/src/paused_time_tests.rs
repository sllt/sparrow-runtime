use crate::{bind_graph, physicalize, Catalog, CheckpointPlan, GraphSpec, IotSpec};
use serde_json::{json, Value};
fn hold() -> Value {
    json!({"kind":"hold_for","clock":"paused","duration_micros":1000000})
}
fn config(timing: Value) -> Value {
    json!({"keys":["id"],"fields":["value"],"emit_first":false,"ttl_micros":0,
    "max_keys":16,"invalid":"ignore","timing":timing})
}
fn graph(kind: &str, spec: Value) -> GraphSpec {
    serde_json::from_value(json!({"version":1,"pipeline_id":1,"revision_id":1,
    "catalog":[{"name":"s","fields":[{"name":"id","type":"utf8","nullable":false},{"name":"value","type":"bool","nullable":true}]}],
    "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":kind,"iot":spec,"out":[3]},
    {"id":3,"kind":"capture_sink","name":"http"}]})).unwrap()
}
fn plan(config: Value) -> crate::PhysicalPlan {
    physicalize(
        &bind_graph(&graph("hold_for", config), &Catalog::new()).unwrap(),
        &Default::default(),
    )
}
#[test]
fn paused_time_plan_binds_versioned_manifest_and_explain() {
    let plan = plan(config(hold()));
    let manifest = CheckpointPlan::from_physical(&plan).unwrap();
    assert!(plan.has_timed_iot());
    assert!(manifest.has_timed_iot());
    assert_eq!(manifest.states[0].freeze_kind(), 7);
    assert_eq!(
        CheckpointPlan::decode(&manifest.encode().unwrap()).unwrap(),
        manifest
    );
    assert!(crate::GraphExplain::from_plan(&plan)
        .state
        .contains("clock=paused_source_ordered"));
}
#[test]
fn paused_time_plan_rejects_missing_clock_mixed_modes_and_wrong_kind() {
    for mutation in 0..7 {
        let mut config = config(hold());
        match mutation {
            0 => config["timing"]["duration_micros"] = json!(0),
            1 => config["ttl_micros"] = json!(1),
            2 => config["emit_first"] = json!(true),
            3 => config["fields"] = json!(["id"]),
            4 => {
                config["deadband"] =
                    json!({"mode":"absolute","baseline":"last_input","threshold":1.0})
            }
            5 => config["timing"]["clock"] = json!("wall"),
            _ => {
                config["timing"].as_object_mut().unwrap().remove("clock");
            }
        }
        // Clock/mode syntax failures are rejected at deserialization, before binding.
        if let Ok(spec) = serde_json::from_value::<IotSpec>(config.clone()) {
            assert!(
                bind_graph(
                    &graph("hold_for", serde_json::to_value(spec).unwrap()),
                    &Catalog::new()
                )
                .is_err(),
                "mutation {mutation}"
            );
        }
    }
    for kind in ["change_detect", "deadband", "hysteresis", "debounce"] {
        assert!(bind_graph(&graph(kind, config(hold())), &Catalog::new()).is_err());
    }
}
#[test]
fn paused_time_plan_semantic_changes_invalidate_restore() {
    let old = CheckpointPlan::from_physical(&plan(config(hold()))).unwrap();
    let mut changed = config(hold());
    changed["timing"]["duration_micros"] = json!(1000001);
    assert!(old
        .check_compatible(&CheckpointPlan::from_physical(&plan(changed)).unwrap())
        .is_err());
    let mut legacy = config(hold());
    legacy.as_object_mut().unwrap().remove("timing");
    let legacy: IotSpec = serde_json::from_value(legacy).unwrap();
    assert!(serde_json::to_value(legacy)
        .unwrap()
        .get("timing")
        .is_none());
}
#[test]
fn paused_time_debounce_requires_explicit_bounded_modes() {
    let timing = json!({"kind":"debounce","clock":"paused","quiet_micros":100,"max_wait_micros":200,
        "leading":true,"trailing":true,"reset_on_repeat":true});
    let physical = physicalize(
        &bind_graph(&graph("debounce", config(timing.clone())), &Catalog::new()).unwrap(),
        &Default::default(),
    );
    assert_eq!(
        CheckpointPlan::from_physical(&physical).unwrap().states[0].freeze_kind(),
        8
    );
    for mutation in 0..4 {
        let mut timing = timing.clone();
        match mutation {
            0 => timing["quiet_micros"] = json!(-1),
            1 => timing["max_wait_micros"] = json!(99),
            2 => {
                timing["leading"] = json!(false);
                timing["trailing"] = json!(false);
            }
            _ => {
                timing.as_object_mut().unwrap().remove("reset_on_repeat");
            }
        }
        let parsed = serde_json::from_value::<IotSpec>(config(timing));
        assert!(
            parsed.is_err()
                || bind_graph(
                    &graph("debounce", serde_json::to_value(parsed.unwrap()).unwrap()),
                    &Catalog::new()
                )
                .is_err()
        );
    }
}

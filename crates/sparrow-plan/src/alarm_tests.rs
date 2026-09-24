use crate::{
    bind_graph, physicalize, Catalog, CheckpointPlan, GraphSpec, PhysicalStage, PlanOptions,
};
use serde_json::{json, Value};

fn graph() -> Value {
    json!({"version":1,"pipeline_id":1,"revision_id":1,"catalog":[{"name":"s","fields":[
        {"name":"device","type":"utf8","nullable":false},{"name":"enter","type":"bool","nullable":true},{"name":"clear","type":"bool","nullable":true}]}],
        "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},
        {"id":2,"kind":"alarm","iot":{"keys":["device"],"fields":["enter","clear"],"emit_first":false,"ttl_micros":0,"max_keys":16,"invalid":"ignore",
            "timing":{"kind":"alarm","clock":"paused","activate_micros":10,"resolve_micros":5,"cooldown_micros":100,"notification_max_age_micros":200}},"out":[3]},
        {"id":3,"kind":"capture_sink","name":"out"}]})
}
fn bind(value: Value) -> crate::PhysicalPlan {
    let graph: GraphSpec = serde_json::from_value(value).unwrap();
    physicalize(
        &bind_graph(&graph, &Catalog::default()).unwrap(),
        &PlanOptions::default(),
    )
}
#[test]
fn alarm_plan_typed_output_semantics_and_legacy_isolation() {
    let plan = bind(graph());
    let manifest = CheckpointPlan::from_physical(&plan).unwrap();
    assert!(manifest.has_alarm() && manifest.requires_paused_time());
    let PhysicalStage::Iot { input, output, .. } = &plan.stages[1] else {
        panic!()
    };
    assert_eq!(output.fields.len(), input.fields.len() + 7);
    assert_eq!(
        output
            .field_by_name("sparrow_alarm_notify")
            .unwrap()
            .data_type,
        sparrow_model::DataType::Bool
    );
    assert_eq!(manifest.states[0].window_kind, 11);
    for parameter in [
        "activate_micros",
        "resolve_micros",
        "cooldown_micros",
        "notification_max_age_micros",
    ] {
        let mut value = graph();
        value["nodes"][1]["iot"]["timing"][parameter] = json!(123);
        assert!(manifest
            .check_compatible(&CheckpointPlan::from_physical(&bind(value)).unwrap())
            .is_err());
    }
    let mut wrong = plan.clone();
    if let PhysicalStage::Iot { input, output, .. } = &mut wrong.stages[1] {
        *output = input.clone();
    }
    assert!(CheckpointPlan::from_physical(&wrong).is_err());
    let mut filtered = graph();
    filtered["nodes"][2] = json!({"id":3,"kind":"filter","predicate":{"k":"col","name":"sparrow_alarm_notify"},"out":[4]});
    filtered["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":4,"kind":"capture_sink","name":"notifications"}));
    assert!(CheckpointPlan::from_physical(&bind(filtered)).is_ok());
}
#[test]
fn alarm_plan_rejects_bad_durations_conditions_and_reserved_fields() {
    for mutation in 0..7 {
        let mut value = graph();
        match mutation {
            0 => value["nodes"][1]["iot"]["timing"]["activate_micros"] = json!(-1),
            1 => value["nodes"][1]["iot"]["timing"]["notification_max_age_micros"] = json!(0),
            2 => value["nodes"][1]["iot"]["ttl_micros"] = json!(1),
            3 => value["nodes"][1]["iot"]["fields"] = json!(["enter"]),
            4 => value["catalog"][0]["fields"][2]["type"] = json!("int64"),
            5 => value["catalog"][0]["fields"]
                .as_array_mut()
                .unwrap()
                .push(json!({"name":"sparrow_alarm_notify","type":"bool","nullable":false})),
            _ => value["nodes"][1]["kind"] = json!("hold_for"),
        }
        let graph: GraphSpec = serde_json::from_value(value).unwrap();
        assert!(
            bind_graph(&graph, &Catalog::default()).is_err(),
            "mutation {mutation}"
        );
    }
}

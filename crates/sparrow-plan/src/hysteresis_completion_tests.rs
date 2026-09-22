use crate::{bind_graph, physicalize, Catalog, CheckpointPlan, GraphSpec, PhysicalStage};
use serde_json::{json, Value};

fn configuration() -> Value {
    json!({"keys":["id"],"fields":["value"],"emit_first":true,"ttl_micros":0,"max_keys":16,
        "invalid":"ignore","hysteresis":{"direction":"high","enter":10.0,"exit":5.0}})
}
fn graph(iot: Value) -> GraphSpec {
    serde_json::from_value(json!({"version":1,"pipeline_id":9,"revision_id":1,
        "catalog":[{"name":"s","fields":[{"name":"id","type":"utf8","nullable":false},{"name":"value","type":"int64","nullable":true}]}],
        "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},
                 {"id":2,"kind":"hysteresis","iot":iot,"out":[3]},
                 {"id":3,"kind":"capture_sink","name":"out"}]})).unwrap()
}
fn plan(iot: Value) -> crate::PhysicalPlan {
    physicalize(
        &bind_graph(&graph(iot), &Catalog::new()).unwrap(),
        &Default::default(),
    )
}

#[test]
fn completion_hysteresis_binds_and_uses_distinct_state_identity() {
    let physical = plan(configuration());
    assert!(
        matches!(&physical.stages[1], PhysicalStage::Iot { spec, .. } if spec.kind_name()=="hysteresis")
    );
    let checkpoint = CheckpointPlan::from_physical(&physical).unwrap();
    assert!(checkpoint.has_hysteresis());
    assert!(checkpoint.has_iot());
    assert_eq!(checkpoint.states[0].window_kind, 6);
    assert_eq!(checkpoint.recovery_prefix_len, None);
    assert_eq!(
        CheckpointPlan::decode(&checkpoint.encode().unwrap()).unwrap(),
        checkpoint
    );
    assert!(crate::GraphExplain::from_plan(&physical)
        .state
        .contains("hysteresis direction=high enter=10 exit=5"));
}

#[test]
fn completion_hysteresis_rejects_ambiguous_threshold_type_and_expiry() {
    for mutation in 0..8 {
        let mut iot = configuration();
        match mutation {
            0 => iot["hysteresis"]["exit"] = json!(10.0),
            1 => iot["hysteresis"]["exit"] = json!(11.0),
            2 => iot["hysteresis"]["direction"] = json!("low"),
            3 => iot["fields"] = json!(["id"]),
            4 => iot["fields"] = json!(["value", "id"]),
            5 => iot["ttl_micros"] = json!(1),
            6 => {
                iot["deadband"] =
                    json!({"mode":"absolute","baseline":"last_output","threshold":1.0})
            }
            _ => {
                iot.as_object_mut().unwrap().remove("hysteresis");
            }
        }
        assert!(
            bind_graph(&graph(iot), &Catalog::new()).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn completion_hysteresis_changes_invalidate_full_recovery_semantics() {
    let checkpoint = CheckpointPlan::from_physical(&plan(configuration())).unwrap();
    for mutation in 0..4 {
        let mut iot = configuration();
        match mutation {
            0 => iot["hysteresis"]["enter"] = json!(11.0),
            1 => iot["hysteresis"]["exit"] = json!(4.0),
            2 => iot["emit_first"] = json!(false),
            _ => iot["hysteresis"] = json!({"direction":"low","enter":5.0,"exit":10.0}),
        }
        assert!(checkpoint
            .check_compatible(&CheckpointPlan::from_physical(&plan(iot)).unwrap())
            .is_err());
    }
}

#[test]
fn completion_hysteresis_old_iot_json_omits_new_field_and_kind_cannot_be_disguised() {
    let mut graph = graph(configuration());
    for kind in ["change_detect", "deadband"] {
        graph.nodes[1].kind = kind.into();
        assert!(bind_graph(&graph, &Catalog::new()).is_err());
    }
    graph.nodes[1].kind = "change_detect".into();
    graph.nodes[1].iot.as_mut().unwrap().hysteresis = None;
    let encoded = serde_json::to_value(&graph).unwrap();
    assert!(encoded["nodes"][1]["iot"].get("hysteresis").is_none());
    let old = physicalize(
        &bind_graph(&graph, &Catalog::new()).unwrap(),
        &Default::default(),
    );
    let checkpoint = CheckpointPlan::from_physical(&old).unwrap();
    assert!(!checkpoint.has_hysteresis());
    assert_eq!(checkpoint.states[0].window_kind, 4);
}

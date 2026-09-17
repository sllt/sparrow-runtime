use crate::*;
use serde_json::{json, Value};
fn graph(nodes: Value) -> GraphSpec {
    serde_json::from_value(json!({"version":1,"pipeline_id":1,"revision_id":1,"catalog":[{"name":"s","fields":[{"name":"v","type":"int64","nullable":false}]}],"nodes":nodes})).unwrap()
}
fn branch() -> Value {
    json!([
        {"id":1,"kind":"memory_source","table":"s","out":[2]},
        {"id":2,"kind":"branch","out":[3,4]},
        {"id":3,"kind":"capture_sink","name":"a"},
        {"id":4,"kind":"capture_sink","name":"b"}
    ])
}
#[test]
fn k3_graph_rejects_cycles_duplicates_untyped_fanout_and_lossy_required_paths() {
    for mutate in 0..6 {
        let mut nodes = branch();
        match mutate {
            0 => nodes[0]["out"] = json!([2, 3]),
            1 => nodes[1]["out"] = json!([3, 3]),
            2 => nodes[1]["out"] = json!([1, 3, 4]),
            3 => nodes[1]["best_effort"] = json!([3]),
            4 => nodes[1]["out"] = json!([3, 4, 999]),
            5 => nodes[3]["kind"] = json!("best_effort_sink"),
            _ => unreachable!(),
        }
        assert!(
            bind_graph(&graph(nodes), &Catalog::new()).is_err(),
            "invalid graph case {mutate} must fail"
        );
    }
}
#[test]
fn k3_fusion_only_crosses_single_forward_edges_and_checkpoint_is_strict() {
    let spec = graph(json!([
        {"id":1,"kind":"memory_source","table":"s","out":[2]},
        {"id":2,"kind":"filter","predicate":{"k":"lit","value":{"t":"bool","v":true}},"out":[3]},
        {"id":3,"kind":"project","exprs":[{"expr":{"k":"col","name":"v"},"alias":"v"}],"out":[4]},
        {"id":4,"kind":"branch","out":[5,6]},
        {"id":5,"kind":"capture_sink","name":"a"},{"id":6,"kind":"capture_sink","name":"b"}
    ]));
    let bound = bind_graph(&spec, &Catalog::new()).unwrap();
    let fused = physicalize(&bound, &PlanOptions { fuse: true });
    let unfused = physicalize(&bound, &PlanOptions { fuse: false });
    assert!(fused.fused());
    assert_eq!(unfused.stages.len(), fused.stages.len() + 1);
    assert_eq!(fused.mailbox_count(), 4);
    let manifest = CheckpointPlan::from_physical(&fused).unwrap();
    assert!(manifest.is_graph());
    assert_eq!(manifest.sink_ids().len(), 2);
    let mut changed = spec.clone();
    changed.nodes[1].predicate =
        Some(serde_json::from_value(json!({"k":"lit","value":{"t":"bool","v":false}})).unwrap());
    let changed = physicalize(
        &bind_graph(&changed, &Catalog::new()).unwrap(),
        &Default::default(),
    );
    assert!(manifest
        .check_compatible(&CheckpointPlan::from_physical(&changed).unwrap())
        .is_err());
}
#[test]
fn k3_route_requires_explicit_mode_default_and_unique_total_ports() {
    let mut nodes = branch();
    nodes[1] = json!({"id":2,"kind":"route","out":[3,4],"routes":[{"predicate":{"k":"lit","value":{"t":"bool","v":true}},"to":3}],"route_mode":"first_match","default_out":4});
    assert!(bind_graph(&graph(nodes.clone()), &Catalog::new()).is_ok());
    for field in ["route_mode", "default_out", "routes"] {
        let mut broken = nodes.clone();
        broken[1].as_object_mut().unwrap().remove(field);
        assert!(bind_graph(&graph(broken), &Catalog::new()).is_err());
    }
    nodes[1]["default_out"] = json!(3);
    assert!(bind_graph(&graph(nodes), &Catalog::new()).is_err());
}
#[test]
fn k3_union_schema_and_time_contracts_are_validated_before_runtime() {
    let mut spec = graph(json!([
        {"id":1,"kind":"memory_source","table":"s","out":[3]},
        {"id":2,"kind":"memory_source","table":"t","out":[3]},
        {"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"capture_sink","name":"out"}
    ]));
    spec.catalog.push(
        serde_json::from_value(
            json!({"name":"t","fields":[{"name":"v","type":"bool","nullable":false}]}),
        )
        .unwrap(),
    );
    assert!(bind_graph(&spec, &Catalog::new()).is_err());
    spec.catalog[1].fields[0].data_type = "int64".into();
    assert!(bind_graph(&spec, &Catalog::new()).is_ok());
    spec.nodes[0].event_time_field = Some("v".into());
    assert!(
        bind_graph(&spec, &Catalog::new()).is_err(),
        "UnionAll must not silently merge timed and untimed ports"
    );
}

#[test]
fn k3_legacy_linear_serialization_does_not_write_unknown_null_graph_fields() {
    let spec = graph(
        json!([{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"capture_sink","name":"out"}]),
    );
    let encoded: Value = serde_json::from_str(&spec.to_json().unwrap()).unwrap();
    for node in encoded["nodes"].as_array().unwrap() {
        for field in [
            "routes",
            "route_mode",
            "default_out",
            "best_effort",
            "side_output",
            "out_of_orderness_micros",
            "max_future_skew_micros",
        ] {
            assert!(
                !node.as_object().unwrap().contains_key(field),
                "unused new field {field} must not break old strict readers"
            );
        }
    }
    assert_eq!(
        GraphSpec::from_json(&spec.to_json().unwrap()).unwrap(),
        spec
    );
}

use crate::{
    bind_graph, physicalize, BoundKind, Catalog, CheckpointPlan, DeadbandBaseline, DeadbandMode,
    DeadbandSpec, GraphSpec, InvalidValuePolicy, IotSpec, ParticipantId, PhysicalStage,
    PlanOptions,
};
use serde_json::{json, Value};
use sparrow_model::{DataType, Field, FieldId, Schema, SchemaId};

fn graph(kind: &str, iot: Option<Value>) -> GraphSpec {
    let mut node = json!({"id":2,"kind":kind,"out":[3]});
    if let Some(iot) = iot {
        node["iot"] = iot;
    }
    serde_json::from_value(json!({
        "version":1,
        "pipeline_id":1,
        "revision_id":1,
        "catalog":[{"name":"s","fields":[
            {"name":"device_id","type":"utf8","nullable":false},
            {"name":"temp","type":"float64","nullable":true},
            {"name":"status","type":"bool","nullable":false},
            {"name":"seq","type":"int64","nullable":false}
        ]}],
        "nodes":[
            {"id":1,"kind":"memory_source","table":"s","out":[2]},
            node,
            {"id":3,"kind":"capture_sink","name":"out"}
        ]
    }))
    .unwrap()
}

fn change_iot() -> Value {
    json!({
        "keys":["device_id"],
        "fields":["temp","status"],
        "emit_first":true,
        "ttl_micros":0,
        "max_keys":64,
        "invalid":"ignore"
    })
}

fn deadband_iot() -> Value {
    json!({
        "keys":["device_id"],
        "fields":["temp"],
        "emit_first":true,
        "ttl_micros":0,
        "max_keys":64,
        "invalid":"error",
        "deadband":{"mode":"relative","baseline":"last_output","threshold":0.05}
    })
}

fn input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temp", DataType::Float64, true),
            Field::new(FieldId::new(3), "status", DataType::Bool, false),
            Field::new(FieldId::new(4), "seq", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn valid_change() -> IotSpec {
    IotSpec {
        keys: vec!["device_id".into()],
        fields: vec!["temp".into(), "status".into()],
        emit_first: true,
        ttl_micros: 0,
        max_keys: 64,
        invalid: InvalidValuePolicy::Ignore,
        deadband: None,
        hysteresis: None,
        timing: None,
    }
}

fn valid_deadband() -> IotSpec {
    IotSpec {
        keys: vec!["device_id".into()],
        fields: vec!["temp".into()],
        emit_first: true,
        ttl_micros: 0,
        max_keys: 64,
        invalid: InvalidValuePolicy::Error,
        deadband: Some(DeadbandSpec {
            mode: DeadbandMode::Relative,
            baseline: DeadbandBaseline::LastOutput,
            threshold: 0.05,
        }),
        hysteresis: None,
        timing: None,
    }
}

#[test]
fn k4_change_detect_binds_to_distinct_iot_physical_state() {
    let bound = bind_graph(&graph("change_detect", Some(change_iot())), &Catalog::new()).unwrap();
    assert!(matches!(
        bound
            .nodes
            .iter()
            .find(|node| node.id.raw() == 2)
            .unwrap()
            .kind,
        BoundKind::Iot { .. }
    ));
    let plan = physicalize(&bound, &PlanOptions::default());
    assert!(plan.has_iot());
    assert!(matches!(&plan.stages[1], PhysicalStage::Iot { .. }));
    let report = crate::explain_bound(&bound);
    assert!(report.state.contains("iot op=2"));
    assert!(report.stages.iter().any(|s| s == "iot:change_detect"));
}

#[test]
fn k4_deadband_requires_one_numeric_field_and_serializes_contract() {
    let bound = bind_graph(&graph("deadband", Some(deadband_iot())), &Catalog::new()).unwrap();
    let plan = physicalize(&bound, &PlanOptions::default());
    let participant = CheckpointPlan::from_physical(&plan)
        .unwrap()
        .states
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(participant.id, ParticipantId::iot(2.into()));
    assert_eq!(participant.codec, crate::checkpoint::IOT_STATE_CODEC);
    assert_eq!(participant.window_kind, 5);
    assert_eq!(participant.freeze_kind(), 5);

    let mut too_wide = valid_deadband();
    too_wide.fields.push("status".into());
    assert!(too_wide.validate(&input_schema()).is_err());
    let mut text = valid_deadband();
    text.fields = vec!["device_id".into()];
    assert!(text.validate(&input_schema()).is_err());
    let mut invalid_threshold = valid_deadband();
    invalid_threshold.deadband.as_mut().unwrap().threshold = f64::NAN;
    assert!(invalid_threshold.validate(&input_schema()).is_err());
}

#[test]
fn k4_iot_keys_values_and_budgets_fail_closed() {
    let schema = input_schema();
    let mut key = valid_change();
    key.keys = vec!["temp".into()];
    assert!(
        key.validate(&schema).is_err(),
        "nullable/float key must fail"
    );
    key = valid_change();
    key.keys = vec!["missing".into()];
    assert!(key.validate(&schema).is_err());
    key = valid_change();
    key.fields = vec!["missing".into()];
    assert!(key.validate(&schema).is_err());
    key = valid_change();
    key.max_keys = 0;
    assert!(key.validate(&schema).is_err());
    key = valid_change();
    key.ttl_micros = -1;
    assert!(key.validate(&schema).is_err());
    key = valid_change();
    key.fields = vec!["seq".into()];
    assert!(key.validate(&schema).is_ok());
    key.keys = vec!["device_id".into(), "device_id".into()];
    assert!(key.validate(&schema).is_err());
}

#[test]
fn k4_iot_null_schema_type_is_rejected_at_plan_admission() {
    let key_null = Schema::new(
        SchemaId::new(2),
        vec![Field::new(FieldId::new(1), "device_id", DataType::Null, false)],
    )
    .unwrap();
    let mut spec = valid_change();
    spec.keys = vec!["device_id".into()];
    spec.fields = vec!["device_id".into()];
    assert!(spec.validate(&key_null).is_err());

    let value_null = Schema::new(
        SchemaId::new(3),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "value", DataType::Null, false),
        ],
    )
    .unwrap();
    spec = valid_change();
    spec.fields = vec!["value".into()];
    assert!(spec.validate(&value_null).is_err());
}

#[test]
fn k4_node_kind_requires_matching_iot_configuration() {
    assert!(bind_graph(&graph("change_detect", None), &Catalog::new()).is_err());
    assert!(bind_graph(&graph("deadband", Some(change_iot())), &Catalog::new()).is_err());
    assert!(bind_graph(
        &graph("change_detect", Some(deadband_iot())),
        &Catalog::new()
    )
    .is_err());
    assert!(bind_graph(&graph("filter", Some(change_iot())), &Catalog::new()).is_err());
}

#[test]
fn k4_aligned_recovery_requires_zero_ttl_and_accepts_iot_codec() {
    let mut spec = graph("change_detect", Some(change_iot()));
    let bound = bind_graph(&spec, &Catalog::new()).unwrap();
    let plan = physicalize(&bound, &PlanOptions::default());
    let checkpoint = CheckpointPlan::from_physical(&plan).unwrap();
    assert!(checkpoint.has_iot());
    assert!(checkpoint.recovery_prefix_len.is_none());

    spec.nodes[1].iot.as_mut().unwrap().ttl_micros = 1_000;
    let plan = physicalize(
        &bind_graph(&spec, &Catalog::new()).unwrap(),
        &PlanOptions::default(),
    );
    assert!(CheckpointPlan::from_physical(&plan).is_err());
}

#[test]
fn k4_iot_checkpoint_rejects_recovery_prefix_relaxation() {
    let plan = physicalize(
        &bind_graph(
            &graph("change_detect", Some(change_iot())),
            &Catalog::new(),
        )
        .unwrap(),
        &PlanOptions::default(),
    );
    let mut forged = CheckpointPlan::from_physical(&plan).unwrap();
    forged.recovery_prefix_len = Some(4);
    assert!(
        forged.validate().is_err(),
        "IoT participants must never use the legacy downstream-prefix relaxation"
    );
}

#[test]
fn k4_iot_state_is_not_a_legacy_window_freeze_and_semantics_are_strict() {
    let spec = graph("deadband", Some(deadband_iot()));
    let plan = physicalize(
        &bind_graph(&spec, &Catalog::new()).unwrap(),
        &PlanOptions::default(),
    );
    assert!(plan.aligned_window().is_err());
    assert!(crate::compat::PlanLayout::from_physical(&plan).is_err());
    let saved = CheckpointPlan::from_physical(&plan).unwrap();

    let mut changed = spec.clone();
    changed.nodes[1]
        .iot
        .as_mut()
        .unwrap()
        .deadband
        .as_mut()
        .unwrap()
        .threshold = 0.10;
    let changed = physicalize(
        &bind_graph(&changed, &Catalog::new()).unwrap(),
        &PlanOptions::default(),
    );
    assert!(saved
        .check_compatible(&CheckpointPlan::from_physical(&changed).unwrap())
        .is_err());
}

#[test]
fn k4_legacy_graph_json_omits_unused_iot_object() {
    let spec = graph("capture_sink", None);
    let text = spec.to_json().unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    for node in value["nodes"].as_array().unwrap() {
        assert!(!node.as_object().unwrap().contains_key("iot"));
    }
    assert_eq!(GraphSpec::from_json(&text).unwrap(), spec);
}

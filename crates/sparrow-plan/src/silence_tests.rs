//! Silence (IOT-07) plan contract: configuration, canonical identity, output
//! schema and first-release admission.

use crate::{
    bind_graph, physicalize, Catalog, CheckpointPlan, GraphSpec, InvalidValuePolicy, IotSpec,
    IotTimingSpec, PhysicalPlan, PhysicalStage, PlanOptions, SilenceClockPolicy,
    ReferenceTableDependency,
};
use serde_json::{json, Value};
use sparrow_model::{
    DataType, ErrorCode, EventTimeBinding, Field, FieldId, OperatorId, PipelineId, RevisionId,
    Scalar, Schema, SchemaId,
};

const KEYS: [&str; 2] = ["device", "site"];

fn catalog() -> Value {
    json!([{"name":"s","fields":[
        {"name":"device","type":"utf8","nullable":false},
        {"name":"site","type":"int64","nullable":false},
        {"name":"blob","type":"bytes","nullable":false},
        {"name":"at","type":"timestamp_micros_utc","nullable":false},
        {"name":"armed","type":"bool","nullable":false},
        {"name":"temp","type":"float64","nullable":true}]}])
}

/// A silence `iot` object with the reviewed defaults.
fn silence_iot() -> Value {
    json!({
        "keys": KEYS, "fields": [], "emit_first": false, "ttl_micros": 0, "max_keys": 16,
        "invalid": "error",
        "timing": {"kind": "silence", "clock": "paused", "duration_micros": 1000000,
                   "max_observation_gap_micros": 300000,
                   "registered_keys": [["ghost", 7], ["other", 8]]}
    })
}

fn graph_with(catalog: Value, iot: Value) -> Value {
    json!({"version":1,"pipeline_id":1,"revision_id":1,"catalog":catalog,
        "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},
                 {"id":2,"kind":"silence","iot":iot,"out":[3]},
                 {"id":3,"kind":"capture_sink","name":"out"}]})
}

fn graph(iot: Value) -> Value {
    graph_with(catalog(), iot)
}

fn try_plan(value: Value) -> sparrow_model::Result<PhysicalPlan> {
    let spec: GraphSpec = serde_json::from_value(value).expect("fixture JSON");
    bind_graph(&spec, &Catalog::default()).map(|bound| physicalize(&bound, &PlanOptions::default()))
}

fn manifest(value: Value) -> CheckpointPlan {
    CheckpointPlan::from_physical(&try_plan(value).expect("bound silence plan")).unwrap()
}

fn edit(iot: &Value, pointer: &str, value: Value) -> Value {
    let mut edited = iot.clone();
    let (section, field) = pointer.split_once('/').expect("section/field pointer");
    edited[section][field] = value;
    edited
}

fn input_schema() -> Schema {
    Schema::new(
        SchemaId::new(7),
        vec![
            Field::new(FieldId::new(1), "device", DataType::Utf8, false),
            Field::new(FieldId::new(2), "site", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn spec(keys: &[&str], registered: Value, max_keys: usize) -> IotSpec {
    IotSpec {
        keys: keys.iter().map(|key| key.to_string()).collect(),
        fields: Vec::new(),
        emit_first: false,
        ttl_micros: 0,
        max_keys,
        invalid: InvalidValuePolicy::Error,
        deadband: None,
        hysteresis: None,
        timing: Some(IotTimingSpec::Silence {
            duration_micros: 1_000_000,
            max_observation_gap_micros: 250_000,
            registered_keys: Box::new(serde_json::from_value(registered).unwrap()),
            clock: SilenceClockPolicy::Paused,
        }),
    }
}

fn linear_plan(
    stages: Vec<PhysicalStage>,
    edges: Option<Vec<crate::physical::PhysicalEdge>>,
) -> PhysicalPlan {
    PhysicalPlan {
        pipeline: PipelineId::new(1),
        revision: RevisionId::new(1),
        stages,
        edges,
        side_outputs: Vec::new(),
        source_times: Vec::new(),
    }
}

#[test]
fn silence_plan_keeps_keys_only_and_reports_its_own_state_tag() {
    let plan = try_plan(graph(silence_iot())).expect("silence binds");
    let checkpoint = CheckpointPlan::from_physical(&plan).unwrap();
    assert!(
        checkpoint.has_silence() && checkpoint.requires_paused_time() && checkpoint.has_timed_iot()
    );
    assert!(
        !checkpoint.has_alarm()
            && !checkpoint.has_event_time_state()
            && !checkpoint.is_time_graph()
    );
    assert_eq!(checkpoint.states.len(), 1, "exactly one silence state");
    assert_eq!(checkpoint.states[0].window_kind, 12);
    assert_eq!(
        checkpoint.states[0].freeze_kind(),
        12,
        "new kind reuses slot 3 / codec 2"
    );
    assert_eq!(
        checkpoint.states[0].id,
        crate::ParticipantId::iot(OperatorId::new(2))
    );

    let PhysicalStage::Iot {
        input,
        output,
        spec,
        ..
    } = &plan.stages[1]
    else {
        panic!("silence must stay the first state")
    };
    assert!(spec.is_silence() && !spec.is_alarm());
    assert_eq!(output.fields.len(), spec.keys.len() + 7);
    for (position, name) in spec.keys.iter().enumerate() {
        let source = input.field_by_name(name).expect("declared key");
        let emitted = &output.fields[position];
        assert_eq!(&emitted.name, name, "keys keep their configured order");
        assert_eq!(emitted.data_type, source.data_type, "keys keep their type");
        assert!(!emitted.nullable && emitted.id == source.id);
    }
    assert!(
        output.field_by_name("temp").is_none(),
        "silence must not forward telemetry values it cannot observe"
    );
    for (name, ty, nullable) in [
        ("sparrow_silence_event", DataType::Utf8, false),
        ("sparrow_silence_generation", DataType::Utf8, false),
        ("sparrow_silence_operator", DataType::UInt64, false),
        ("sparrow_silence_episode", DataType::UInt64, false),
        ("sparrow_silence_time", DataType::Int64, false),
        ("sparrow_silence_last_seen", DataType::Int64, true),
        ("sparrow_silence_never_seen", DataType::Bool, false),
    ] {
        let field = output
            .field_by_name(name)
            .unwrap_or_else(|| panic!("missing lifecycle column {name}"));
        assert_eq!((field.data_type.clone(), field.nullable), (ty, nullable));
    }
    let mut ids = std::collections::HashSet::new();
    assert!(output.fields.iter().all(|field| ids.insert(field.id)));
}

#[test]
fn silence_configuration_bounds_and_mixing_are_rejected() {
    let cases: [(&str, fn(&mut Value)); 17] = [
        ("duration zero", |iot| {
            iot["timing"]["duration_micros"] = json!(0)
        }),
        ("duration negative", |iot| {
            iot["timing"]["duration_micros"] = json!(-1)
        }),
        ("gap zero", |iot| {
            iot["timing"]["max_observation_gap_micros"] = json!(0)
        }),
        ("gap negative", |iot| {
            iot["timing"]["max_observation_gap_micros"] = json!(-5)
        }),
        ("gap equals duration", |iot| {
            iot["timing"]["duration_micros"] = json!(100);
            iot["timing"]["max_observation_gap_micros"] = json!(100);
        }),
        ("gap doubling overflows", |iot| {
            iot["timing"]["max_observation_gap_micros"] = json!(i64::MAX);
        }),
        ("fields are not empty", |iot| {
            iot["fields"] = json!(["temp"])
        }),
        ("emit_first", |iot| iot["emit_first"] = json!(true)),
        ("ttl", |iot| iot["ttl_micros"] = json!(1)),
        ("invalid ignore", |iot| iot["invalid"] = json!("ignore")),
        ("max_keys zero", |iot| iot["max_keys"] = json!(0)),
        ("keys empty", |iot| iot["keys"] = json!([])),
        ("keys duplicated", |iot| {
            iot["keys"] = json!(["device", "device"])
        }),
        ("key missing from schema", |iot| {
            iot["keys"] = json!(["device", "absent"])
        }),
        ("deadband mixing", |iot| {
            iot["deadband"] = json!({"mode":"absolute","baseline":"last_input","threshold":1.0});
        }),
        ("hysteresis mixing", |iot| {
            iot["hysteresis"] = json!({"direction":"high","enter":2.0,"exit":1.0});
        }),
        ("timing missing", |iot| {
            iot.as_object_mut().unwrap().remove("timing");
        }),
    ];
    for (label, edit_case) in cases {
        let mut iot = silence_iot();
        edit_case(&mut iot);
        assert!(try_plan(graph(iot)).is_err(), "must reject: {label}");
    }
    // A narrow Project before the state or a second state would hide or delay
    // observations, so they are not admitted either.
    let projected = json!({"version":1,"pipeline_id":1,"revision_id":1,"catalog":catalog(),
        "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},
                 {"id":2,"kind":"project","exprs":[
                    {"alias":"device","expr":{"k":"col","name":"device"}},
                    {"alias":"site","expr":{"k":"col","name":"site"}}],"out":[3]},
                 {"id":3,"kind":"silence","iot":silence_iot(),"out":[4]},
                 {"id":4,"kind":"capture_sink","name":"out"}]});
    assert!(
        CheckpointPlan::from_physical(
            &try_plan(projected).expect("valid graph binds before profile admission")
        )
        .is_err(),
        "upstream project must not be admitted"
    );
    let doubled = json!({"version":1,"pipeline_id":1,"revision_id":1,"catalog":catalog(),
        "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},
                 {"id":2,"kind":"silence","iot":silence_iot(),"out":[3]},
                 {"id":3,"kind":"window_agg","window":{"kind":"count","size":2},"keys":["device"],
                  "aggs":[{"fn":"count","alias":"c"}],"out":[4]},
                 {"id":4,"kind":"capture_sink","name":"out"}]});
    assert!(
        CheckpointPlan::from_physical(
            &try_plan(doubled).expect("valid graph binds before profile admission")
        )
        .is_err(),
        "a second state must not be admitted"
    );
}

#[test]
fn silence_node_kind_must_match_its_timing() {
    let mut wrong_kind = graph(silence_iot());
    wrong_kind["nodes"][1]["kind"] = json!("debounce");
    assert!(try_plan(wrong_kind).is_err());

    let mut wrong_timing = silence_iot();
    wrong_timing["timing"] = json!({"kind":"hold_for","clock":"paused","duration_micros":10});
    assert!(try_plan(graph(wrong_timing)).is_err());

    // Legacy value kinds keep their mandatory value fields: empty `fields`
    // cannot smuggle a change_detect/deadband/hysteresis/alarm past admission.
    for kind in ["change_detect", "deadband", "hysteresis"] {
        let mut legacy = silence_iot();
        legacy.as_object_mut().unwrap().remove("timing");
        let mut value = graph(legacy);
        value["nodes"][1]["kind"] = json!(kind);
        assert!(try_plan(value).is_err(), "{kind} with empty fields");
    }
    let mut alarm = silence_iot();
    alarm["timing"] = json!({"kind":"alarm","clock":"paused","activate_micros":1,"resolve_micros":1,"cooldown_micros":0,
               "notification_max_age_micros":10});
    let mut alarm_graph = graph(alarm);
    alarm_graph["nodes"][1]["kind"] = json!("alarm");
    assert!(try_plan(alarm_graph).is_err(), "alarm with empty fields");
}

#[test]
fn silence_registered_keys_are_strictly_typed_and_bounded() {
    let rows = |value: Value| edit(&silence_iot(), "timing/registered_keys", value);
    for (label, registered) in [
        ("arity", json!([["ghost"]])),
        (
            "duplicate canonical key",
            json!([["ghost", 7], ["ghost", 7]]),
        ),
        ("string for int64", json!([["ghost", "7"]])),
        ("float for int64", json!([["ghost", 7.0]])),
        ("float for int64 exponent", json!([["ghost", 1e2]])),
        ("null key", json!([[null, 7]])),
        ("bool for utf8", json!([[true, 7]])),
        ("number for utf8", json!([[42, 7]])),
        ("object for utf8", json!([[{"a": 1}, 7]])),
        ("array for int64", json!([[[1], 7]])),
        (
            "number out of range",
            json!([["ghost", 9223372036854775808u64]]),
        ),
    ] {
        assert!(
            try_plan(graph(rows(registered))).is_err(),
            "must reject: {label}"
        );
    }
    // Registry rows are bounded by min(max_keys, 1024) ...
    let many: Vec<Value> = (0..17).map(|n| json!([format!("k{n}"), 1])).collect();
    assert!(
        try_plan(graph(rows(json!(many)))).is_err(),
        "above max_keys"
    );
    let cap: Vec<Value> = (0..1025).map(|n| json!([format!("k{n}"), 1])).collect();
    let mut wide = edit(&silence_iot(), "timing/registered_keys", json!(cap));
    wide["max_keys"] = json!(2000);
    assert!(
        try_plan(graph(wide)).is_err(),
        "above the registry count cap"
    );
    // ... and the complete canonical encoding by 64 KiB.
    assert!(
        try_plan(graph(rows(json!([["x".repeat(70 * 1024), 7]])))).is_err(),
        "registry exceeds its encoded bound"
    );

    // Accepted shapes: canonical order, declared key order, and every supported
    // key type with strict JSON typing.
    let mut bytes_keys = silence_iot();
    bytes_keys["keys"] = json!(["blob", "site"]);
    bytes_keys["timing"]["registered_keys"] = json!([[[1, 2, 255], 7]]);
    assert!(try_plan(graph(bytes_keys)).is_ok(), "bytes key array");
    let mut timestamp_keys = silence_iot();
    timestamp_keys["keys"] = json!(["device", "at"]);
    timestamp_keys["timing"]["registered_keys"] = json!([["ghost", -5]]);
    assert!(
        try_plan(graph(timestamp_keys)).is_ok(),
        "timestamp micros key"
    );
    let mut bool_keys = silence_iot();
    bool_keys["keys"] = json!(["armed", "site"]);
    bool_keys["timing"]["registered_keys"] = json!([[true, 9]]);
    assert!(try_plan(graph(bool_keys)).is_ok(), "bool key");
    let mut reordered = silence_iot();
    reordered["timing"]["registered_keys"] = json!([["other", 8], ["ghost", 7]]);
    assert!(
        try_plan(graph(reordered)).is_ok(),
        "row order is not part of the contract"
    );
}

#[test]
fn silence_registry_row_order_is_canonically_identical() {
    let forward = manifest(graph(silence_iot()));
    let mut reordered = silence_iot();
    reordered["timing"]["registered_keys"] = json!([["other", 8], ["ghost", 7]]);
    let reverse = manifest(graph(reordered));
    assert_eq!(forward.states, reverse.states);
    assert_eq!(
        forward.semantics, reverse.semantics,
        "the registered set is sorted canonically, so JSON row order cannot change identity"
    );
    assert!(forward.check_compatible(&reverse).is_ok());

    // A different set (or a different parameter) is a different state identity.
    let mut changed = silence_iot();
    changed["timing"]["registered_keys"] = json!([["other", 8], ["ghost", 9]]);
    assert!(forward.check_compatible(&manifest(graph(changed))).is_err());
    // Distinct device sets must not collapse into one identity, even when they
    // keep the same row count and the same encoded sizes.
    let mut swapped = silence_iot();
    swapped["timing"]["registered_keys"] = json!([["other", 7], ["ghost", 8]]);
    let swapped = manifest(graph(swapped));
    assert_ne!(forward.semantics, swapped.semantics);
    assert!(forward.check_compatible(&swapped).is_err());
    // The runtime-facing helper still returns rows in configuration order.
    let keys = spec(&KEYS, json!([["other", 8], ["ghost", 7]]), 16)
        .silence_registered_keys(&input_schema())
        .unwrap();
    assert_eq!(keys[0], vec![Scalar::utf8("other"), Scalar::Int64(8)]);
    assert_eq!(keys[1], vec![Scalar::utf8("ghost"), Scalar::Int64(7)]);
}

#[test]
fn silence_canonical_digest_covers_parameters_and_registry() {
    let base = manifest(graph(silence_iot()));
    for (label, pointer, value) in [
        ("duration", "timing/duration_micros", json!(1000001)),
        ("gap", "timing/max_observation_gap_micros", json!(250001)),
        (
            "registered key value",
            "timing/registered_keys",
            json!([["ghost", 8], ["other", 8]]),
        ),
        (
            "registered key count",
            "timing/registered_keys",
            json!([["ghost", 7]]),
        ),
    ] {
        let changed = manifest(graph(edit(&silence_iot(), pointer, value)));
        assert!(
            base.check_compatible(&changed).is_err(),
            "state identity must cover {label}"
        );
    }
    // The clock policy is part of the hashed bytes and currently admits only
    // the source-ordered paused clock (the only variant), so it cannot be
    // mutated into a different state identity from configuration alone.
    assert_ne!(
        base.states[0].window_kind, 11,
        "silence is not an alarm profile"
    );
}

#[test]
fn silence_registered_keys_helper_is_strict() {
    let schema = input_schema();
    let typed = spec(&["device", "site"], json!([["g", 7], ["h", -1]]), 4)
        .silence_registered_keys(&schema)
        .unwrap();
    assert_eq!(
        typed,
        vec![
            vec![Scalar::utf8("g"), Scalar::Int64(7)],
            vec![Scalar::utf8("h"), Scalar::Int64(-1)],
        ]
    );
    assert!(
        spec(&["device"], json!([[1]]), 4)
            .silence_registered_keys(&schema)
            .unwrap_err()
            .code
            == ErrorCode::TypeMismatch
    );
    assert!(
        spec(&["device"], json!([["g"], ["g"]]), 4)
            .silence_registered_keys(&schema)
            .unwrap_err()
            .code
            == ErrorCode::InvalidArgument
    );
    assert!(
        spec(&["device", "site"], json!([["g"]]), 4)
            .silence_registered_keys(&schema)
            .unwrap_err()
            .code
            == ErrorCode::InvalidArgument
    );
    assert!(
        spec(&["device"], json!([["a"], ["b"]]), 1)
            .silence_registered_keys(&schema)
            .unwrap_err()
            .code
            == ErrorCode::BoundExceeded
    );
    assert!(
        spec(&["device"], json!([["x".repeat(70 * 1024)]]), 4)
            .silence_registered_keys(&schema)
            .unwrap_err()
            .code
            == ErrorCode::BoundExceeded
    );
    // Only a silence timing configuration exposes a registry.
    let mut legacy = spec(&["device"], json!([]), 4);
    legacy.timing = None;
    assert!(legacy.silence_registered_keys(&schema).is_err());
    assert!(
        legacy.silence_registered_keys(&schema).unwrap_err().code == ErrorCode::InvalidArgument
    );
    // A nullable key is not a valid silence key even when the registry is empty.
    let nullable = Schema::new(
        SchemaId::new(8),
        vec![Field::new(FieldId::new(1), "device", DataType::Utf8, true)],
    )
    .unwrap();
    assert!(spec(&["device"], json!([]), 4)
        .silence_registered_keys(&nullable)
        .is_err());
}

#[test]
fn silence_registry_that_overflows_the_descriptor_is_rejected() {
    let wide = |count: usize| {
        let rows: Vec<Value> = (0..count)
            .map(|n| json!([format!("k{n:0>48}"), 1]))
            .collect();
        let mut iot = silence_iot();
        iot["max_keys"] = json!(1024);
        iot["timing"]["registered_keys"] = json!(rows);
        iot
    };
    // The registry keeps its own independent 64 KiB bound: half of it still
    // leaves room for the surrounding schema metadata.
    let small = try_plan(graph(wide(512))).expect("registry within its own bound");
    CheckpointPlan::from_physical(&small).expect("descriptor still fits");
    // A registry that fills its own budget no longer fits the computation
    // descriptor. That is an explicit rejection, never a shortened identity.
    let large = try_plan(graph(wide(1024))).expect("registry still within its own bound");
    let error = CheckpointPlan::from_physical(&large).unwrap_err();
    assert_eq!(error.code, ErrorCode::BoundExceeded);
}

#[test]
fn silence_input_bound_stays_conservative() {
    let catalog_with = |total: usize, kind: &str| {
        let mut fields = vec![
            json!({"name":"device","type":"utf8","nullable":false}),
            json!({"name":"site","type":"int64","nullable":false}),
        ];
        for index in fields.len()..total {
            fields.push(json!({"name":format!("f{index}"),"type":kind,"nullable":false}));
        }
        json!([{"name":"s","fields":fields}])
    };
    // 48 flat scalar columns stay admitted (keys included in the count).
    assert!(try_plan(graph_with(catalog_with(48, "int64"), silence_iot())).is_ok());
    assert!(try_plan(graph_with(catalog_with(49, "int64"), silence_iot())).is_err());
    // The new profile keeps the existing scalar-only time-journal contract.
    assert!(try_plan(graph_with(catalog_with(3, "dynamic"), silence_iot())).is_err());
}

#[test]
fn silence_embedded_plan_shape_and_schema_are_enforced() {
    let input = input_schema();
    let silence = spec(&KEYS, json!([["ghost", 7]]), 16);
    let output = silence.output_schema(&input).unwrap();
    let source = PhysicalStage::MemorySource {
        operator: OperatorId::new(1),
        name: "s".into(),
        schema: input.clone(),
    };
    let sink = |schema: Schema| PhysicalStage::CaptureSink {
        operator: OperatorId::new(9),
        name: "out".into(),
        schema,
    };
    let state = |output: Schema| PhysicalStage::Iot {
        operator: OperatorId::new(2),
        spec: silence.clone(),
        input: input.clone(),
        output,
    };

    let good = linear_plan(
        vec![source.clone(), state(output.clone()), sink(output.clone())],
        None,
    );
    let checkpoint = CheckpointPlan::from_physical(&good).unwrap();
    assert!(checkpoint.has_silence());

    // An embedded caller cannot claim the silence output is the input row.
    let wrong_schema = linear_plan(
        vec![source.clone(), state(input.clone()), sink(output.clone())],
        None,
    );
    assert!(CheckpointPlan::from_physical(&wrong_schema).is_err());

    // A required DAG, immutable references and a source event-time binding are
    // all outside the first-release silence contract.
    let dag = linear_plan(
        vec![source.clone(), state(output.clone()), sink(output.clone())],
        Some(vec![crate::physical::PhysicalEdge {
            from: 0,
            to: 1,
            port: OperatorId::new(2),
            best_effort: false,
        }]),
    );
    assert!(CheckpointPlan::from_physical(&dag).is_err());
    assert!(CheckpointPlan::from_physical_with_references(
        &good,
        vec![ReferenceTableDependency {
            name: "refs".into(),
            revision: 1,
            canonical_sha256: [7u8; 32],
            runtime_crc32: 0,
        }],
    )
    .is_err());
    let mut with_event_time = good.clone();
    with_event_time
        .source_times
        .push((OperatorId::new(1), EventTimeBinding::new("at")));
    assert!(CheckpointPlan::from_physical(&with_event_time).is_err());
    let mut with_side_output = good.clone();
    with_side_output.side_outputs.push((
        0usize,
        crate::graph::SideOutputSpec {
            kind: crate::graph::SideOutputKind::RuleReject,
            to: 9,
            full: crate::graph::SideOutputFull::Backpressure,
        },
    ));
    assert!(CheckpointPlan::from_physical(&with_side_output).is_err());
}

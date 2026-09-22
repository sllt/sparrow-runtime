//! Control-plane profile selection for immutable reference-table Lookup.
//!
//! The runtime owns the checkpoint codec.  These tests exercise the control
//! admission envelope and make sure the new reference profiles do not fall
//! back to the old stateless v8 or the unrelated v3..v7 profiles.

use crate::PipelineSpec;
use serde_json::{json, Value};
use sparrow_plan::{
    AggCall, CheckpointPlan, LookupSpec, PhysicalPlan, PhysicalStage,
    ReferenceTableDependency, WindowSpec,
};
use sparrow_plan::physical::PhysicalEdge;
use sparrow_model::{DataType, Field, FieldId, OperatorId, PipelineId, RevisionId, Schema, SchemaId, WindowKind};

fn dependency(name: &str) -> ReferenceTableDependency {
    ReferenceTableDependency {
        name: name.into(),
        revision: 1,
        canonical_sha256: [0x41; 32],
        runtime_crc32: 0,
    }
}

fn input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "id", DataType::Utf8, false)],
    )
    .unwrap()
}

fn table_schema() -> Schema {
    Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "value", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn reference_plan(with_count: bool, graph: bool) -> PhysicalPlan {
    let input = input_schema();
    let table = table_schema();
    let lookup = LookupSpec::static_table(
        "limits",
        vec!["id".into()],
        vec!["id".into()],
        vec!["value".into()],
    );
    let lookup_output = sparrow_plan::lookup_output_schema(&input, &table, &lookup.keep).unwrap();
    let mut stages = vec![
        PhysicalStage::MemorySource {
            operator: OperatorId::new(1),
            name: "sensors".into(),
            schema: input.clone(),
        },
        PhysicalStage::Lookup {
            operator: OperatorId::new(2),
            spec: lookup,
            input,
            output: lookup_output.clone(),
        },
    ];
    let mut sink_schema = lookup_output;
    if with_count {
        let window = WindowSpec::new(
            WindowKind::Count { size: 2 },
            vec!["id".into()],
            vec![AggCall::count_star("n")],
        );
        let output = sparrow_plan::window_output_schema(&sink_schema, &window).unwrap();
        stages.push(PhysicalStage::WindowAgg {
            operator: OperatorId::new(3),
            spec: window,
            input: sink_schema,
            output: output.clone(),
        });
        sink_schema = output;
    }
    let sink_id = if with_count { 4 } else { 3 };
    stages.push(PhysicalStage::CaptureSink {
        operator: OperatorId::new(sink_id),
        name: "http".into(),
        schema: sink_schema,
    });
    let edges = graph.then(|| {
        stages
            .windows(2)
            .enumerate()
            .map(|(index, pair)| PhysicalEdge {
                from: index,
                to: index + 1,
                port: match &pair[1] {
                    PhysicalStage::Lookup { operator, .. }
                    | PhysicalStage::WindowAgg { operator, .. }
                    | PhysicalStage::CaptureSink { operator, .. } => *operator,
                    _ => OperatorId::new(0),
                },
                best_effort: false,
            })
            .collect()
    });
    PhysicalPlan {
        pipeline: PipelineId::new(if graph { 11 } else { 9 }),
        revision: RevisionId::new(1),
        stages,
        edges,
        side_outputs: Vec::new(),
        source_times: Vec::new(),
    }
}

fn spec_value(source_kind: &str, graph: bool) -> Value {
    let data_root = sparrow_connectors::ensure_default_data_root();
    let input_path = data_root.join("reference-completion.ndjson");
    let checkpoint_path = data_root.join("reference-completion-checkpoint");
    let source = if source_kind == "jetstream" {
        json!({
            "kind":"jetstream",
            "jetstream":{
                "servers":["nats://127.0.0.1:4222"],
                "namespace":"default",
                "stream":"events",
                "consumer":"completion",
                "ownership_bucket":"completion-bucket"
            }
        })
    } else {
        json!({"kind":"file","path":input_path,"file_contract":"append_only"})
    };
    let sink = json!({"kind":"http","url":"http://127.0.0.1:1/unused"});
    let mut value = json!({
        "version":1,
        "stream":"sensors",
        "reference_tables":{"limits":{"revision":1,"sha256":Value::String("41".repeat(32))}},
        "source":source,
        "sink":sink,
        "delivery":if source_kind == "jetstream" {"checkpointed_at_least_once"} else {"live_best_effort"},
        "recovery":"aligned",
        "checkpoint_dir":checkpoint_path,
        "checkpoint":{"resume_latest":true,"interval_ms":100},
        "graph":{
            "version":1,"pipeline_id":11,"revision_id":1,
            "nodes":[
                {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
                {"id":2,"kind":"lookup","table":"limits","on":[{"stream":"id","table":"id"}],"keep":["value"],"out":[3]},
                {"id":3,"kind":"capture_sink","name":"http"}
            ]
        }
    });
    if graph {
        value["graph_io"] = json!({
            "sources":{"1":source_for_graph()},
            "sinks":{"3":sink_for_graph()}
        });
    }
    value
}

fn source_for_graph() -> Value {
    json!({"kind":"file","path":sparrow_connectors::ensure_default_data_root().join("reference-completion.ndjson"),"file_contract":"append_only"})
}

fn sink_for_graph() -> Value {
    json!({"kind":"http","url":"http://127.0.0.1:1/unused"})
}

fn parse(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

#[test]
fn completion_reference_file_count_uses_profile9_and_not_v8() {
    let spec = parse(spec_value("file", false));
    let plan = reference_plan(true, false);
    crate::validate::validate_aligned_plan(&spec, &plan).unwrap();
    let layout = CheckpointPlan::from_physical_with_references(&plan, vec![dependency("limits")]).unwrap();
    assert_eq!(
        sparrow_runtime::pipeline_checkpoint::snapshot_version_for(&layout, "file").unwrap(),
        sparrow_runtime::pipeline_checkpoint::REFERENCE_LINEAR_SNAPSHOT_VERSION
    );
    let effective = crate::validate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(effective["reference_tables"]["checkpoint_profile"], "v9");
    assert_eq!(effective["checkpoint_participants"]["snapshot_version"], 9);
}

#[test]
fn completion_reference_file_graph_uses_profile11_and_required_edges() {
    let spec = parse(spec_value("file", true));
    let plan = reference_plan(true, true);
    crate::validate::validate_aligned_plan(&spec, &plan).unwrap();
    let layout = CheckpointPlan::from_physical_with_references(&plan, vec![dependency("limits")]).unwrap();
    assert_eq!(
        sparrow_runtime::pipeline_checkpoint::snapshot_version_for(&layout, "file-dag-v1").unwrap(),
        sparrow_runtime::pipeline_checkpoint::REFERENCE_GRAPH_SNAPSHOT_VERSION
    );
    let effective = crate::validate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(effective["reference_tables"]["checkpoint_profile"], "v11");
    assert_eq!(effective["checkpoint_participants"]["snapshot_version"], 11);
}

#[cfg(feature = "jetstream")]
#[test]
fn completion_reference_jetstream_uses_profile10_with_same_dependencies() {
    let spec = parse(spec_value("jetstream", false));
    let plan = reference_plan(false, false);
    crate::validate::validate_aligned_plan(&spec, &plan).unwrap();
    let layout = CheckpointPlan::from_physical_with_references(&plan, vec![dependency("limits")]).unwrap();
    assert_eq!(
        sparrow_runtime::pipeline_checkpoint::snapshot_version_for(&layout, "jetstream-v1").unwrap(),
        sparrow_runtime::pipeline_checkpoint::REFERENCE_RELIABLE_SNAPSHOT_VERSION
    );
    let effective = crate::validate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(effective["reference_tables"]["checkpoint_profile"], "v10");
    assert_eq!(effective["checkpoint_participants"]["snapshot_version"], 10);
}

#[test]
fn completion_reference_profiles_still_reject_time_and_lossy_contracts() {
    let spec = parse(spec_value("file", false));
    let mut plan = reference_plan(true, false);
    if let PhysicalStage::WindowAgg { spec, .. } = &mut plan.stages[2] {
        spec.kind = WindowKind::TumblingEventTime { size_micros: 1000 };
        spec.event_time_field = Some("id".into());
    }
    assert!(crate::validate::validate_aligned_plan(&spec, &plan).is_err());

    let mut graph = reference_plan(false, true);
    if let Some(edges) = &mut graph.edges {
        edges[0].best_effort = true;
    }
    let graph_spec = parse(spec_value("file", true));
    assert!(crate::validate::validate_aligned_plan(&graph_spec, &graph).is_err());
}

#[test]
fn completion_reference_graph_source_cannot_shadow_bound_table() {
    let mut value = spec_value("file", true);
    value["graph"]["nodes"][0]["table"] = json!("limits");
    assert!(PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn completion_reference_capability_inventory_lists_mixed_profiles() {
    let inventory = crate::capability::inventory();
    assert_eq!(inventory["reference_tables"]["checkpoint_dependencies"]["profiles"]["file_stateless"], "v8");
    assert_eq!(inventory["reference_tables"]["checkpoint_dependencies"]["profiles"]["file_state"], "v9");
    assert_eq!(inventory["reference_tables"]["checkpoint_dependencies"]["profiles"]["jetstream"], "v10");
    assert_eq!(inventory["reference_tables"]["checkpoint_dependencies"]["profiles"]["file_graph"], "v11");
    assert_eq!(inventory["reference_tables"]["hysteresis_without_references"]["file"], "v12");
    assert_eq!(inventory["reference_tables"]["hysteresis_without_references"]["jetstream"], "v13");
}

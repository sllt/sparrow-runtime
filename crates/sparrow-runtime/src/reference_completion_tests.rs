//! Focused manifest/profile coverage for immutable references mixed with the
//! already-supported Count state and graph topology.  The wider process
//! matrix lives outside the crate; these tests keep the public profile
//! selector and CPL3 shape honest without touching the old v3..v8 fixtures.

use crate::pipeline_checkpoint::{
    snapshot_version_for, REFERENCE_GRAPH_SNAPSHOT_VERSION, REFERENCE_LINEAR_SNAPSHOT_VERSION,
    REFERENCE_RELIABLE_SNAPSHOT_VERSION, REFERENCE_SNAPSHOT_VERSION,
};
use crate::{ParticipantAcks, PipelineSnapshot};
use sparrow_model::{
    DataType, Field, FieldId, MemoryOwner, OperatorId, OutputSequence, PipelineId, ResourceBudget,
    RevisionId, Schema, SchemaId, WindowKind,
};
use sparrow_plan::{
    AggCall, CheckpointPlan, LookupSpec, ParticipantId, PhysicalPlan, PhysicalStage,
    ReferenceTableDependency, StateParticipant,
};
use sparrow_plan::physical::PhysicalEdge;

fn dependency(name: &str) -> ReferenceTableDependency {
    ReferenceTableDependency {
        name: name.into(),
        revision: 1,
        canonical_sha256: [0x31; 32],
        runtime_crc32: 7,
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

fn linear_reference_plan(with_count: bool) -> PhysicalPlan {
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
    let output = if with_count {
        let spec = sparrow_plan::WindowSpec::new(
            WindowKind::Count { size: 2 },
            vec!["id".into()],
            vec![AggCall::count_star("n")],
        );
        let output = sparrow_plan::window_output_schema(&lookup_output, &spec).unwrap();
        stages.push(PhysicalStage::WindowAgg {
            operator: OperatorId::new(3),
            spec,
            input: lookup_output,
            output: output.clone(),
        });
        output
    } else {
        lookup_output
    };
    stages.push(PhysicalStage::CaptureSink {
        operator: OperatorId::new(if with_count { 4 } else { 3 }),
        name: "http".into(),
        schema: output,
    });
    PhysicalPlan {
        pipeline: PipelineId::new(1),
        revision: RevisionId::new(1),
        stages,
        edges: None,
        side_outputs: Vec::new(),
        source_times: Vec::new(),
    }
}

fn graph_reference_plan() -> PhysicalPlan {
    let input = input_schema();
    let table = table_schema();
    let spec = LookupSpec::static_table(
        "limits",
        vec!["id".into()],
        vec!["id".into()],
        vec!["value".into()],
    );
    let output = sparrow_plan::lookup_output_schema(&input, &table, &spec.keep).unwrap();
    PhysicalPlan {
        pipeline: PipelineId::new(2),
        revision: RevisionId::new(1),
        stages: vec![
            PhysicalStage::MemorySource {
                operator: OperatorId::new(10),
                name: "sensors".into(),
                schema: input.clone(),
            },
            PhysicalStage::Lookup {
                operator: OperatorId::new(11),
                spec,
                input,
                output: output.clone(),
            },
            PhysicalStage::CaptureSink {
                operator: OperatorId::new(12),
                name: "http".into(),
                schema: output,
            },
        ],
        edges: Some(vec![
            PhysicalEdge {
                from: 0,
                to: 1,
                port: OperatorId::new(11),
                best_effort: false,
            },
            PhysicalEdge {
                from: 1,
                to: 2,
                port: OperatorId::new(12),
                best_effort: false,
            },
        ]),
        side_outputs: Vec::new(),
        source_times: Vec::new(),
    }
}

fn hysteresis_manifest(graph: bool) -> CheckpointPlan {
    let semantics = if graph {
        let mut bytes = b"CP01DAG1".to_vec();
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&10u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&12u32.to_le_bytes());
        bytes
    } else {
        b"CP01hysteresis".to_vec()
    };
    CheckpointPlan {
        source: OperatorId::new(10),
        sink: OperatorId::new(12),
        states: vec![StateParticipant {
            id: ParticipantId::iot(OperatorId::new(11)),
            codec: sparrow_plan::checkpoint::IOT_STATE_CODEC,
            window_kind: 6,
        }],
        reference_tables: Vec::new(),
        semantics,
        recovery_prefix_len: None,
    }
}

fn source(kind: &str) -> sparrow_io::SourcePosition {
    sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity {
        kind: kind.into(),
        path: "reference-completion".into(),
        size: 0,
        fingerprint: 1,
    })
}

#[test]
fn completion_reference_profiles_select_without_reinterpreting_old_profiles() {
    let stateless = CheckpointPlan::from_physical_with_references(
        &linear_reference_plan(false),
        vec![dependency("limits")],
    )
    .unwrap();
    assert_eq!(
        snapshot_version_for(&stateless, "file").unwrap(),
        REFERENCE_SNAPSHOT_VERSION
    );
    assert_eq!(
        snapshot_version_for(&stateless, "jetstream-v1").unwrap(),
        REFERENCE_RELIABLE_SNAPSHOT_VERSION
    );

    let stateful = CheckpointPlan::from_physical_with_references(
        &linear_reference_plan(true),
        vec![dependency("limits")],
    )
    .unwrap();
    assert_eq!(stateful.states.len(), 1);
    assert_eq!(
        snapshot_version_for(&stateful, "file").unwrap(),
        REFERENCE_LINEAR_SNAPSHOT_VERSION
    );
    assert_eq!(
        snapshot_version_for(&stateful, "jetstream-v1").unwrap(),
        REFERENCE_RELIABLE_SNAPSHOT_VERSION
    );

    let graph = CheckpointPlan::from_physical_with_references(
        &graph_reference_plan(),
        vec![dependency("limits")],
    )
    .unwrap();
    assert!(graph.is_graph());
    assert_eq!(
        snapshot_version_for(&graph, "file-dag-v1").unwrap(),
        REFERENCE_GRAPH_SNAPSHOT_VERSION
    );
}

#[test]
fn completion_reference_graph_requires_real_edges_and_exact_lookup_schema() {
    let mut plan = graph_reference_plan();
    plan.edges = Some(Vec::new());
    assert!(
        CheckpointPlan::from_physical_with_references(&plan, vec![dependency("limits")]).is_err()
    );

    let mut plan = graph_reference_plan();
    if let PhysicalStage::Lookup { output, .. } = &mut plan.stages[1] {
        output.fields[1].nullable = false;
    }
    assert!(
        CheckpointPlan::from_physical_with_references(&plan, vec![dependency("limits")]).is_err()
    );
}

#[test]
fn completion_cpl3_decoder_rejects_event_and_processing_time_state() {
    let plan = CheckpointPlan::from_physical_with_references(
        &linear_reference_plan(true),
        vec![dependency("limits")],
    )
    .unwrap();
    let encoded = plan.encode().unwrap();
    // CPL3 header (14 bytes) + operator/slot/shard/codec (10 bytes) places
    // the WindowFreeze kind tag at byte 24. The legal reference state is
    // Count=1; ET=2 and PT=3 must be rejected even when bytes are hand-built.
    for kind in [2u8, 3] {
        let mut corrupt = encoded.clone();
        corrupt[24] = kind;
        assert!(
            CheckpointPlan::decode(&corrupt).is_err(),
            "window kind {kind}"
        );
    }
}

#[test]
fn completion_reference_selector_rejects_raw_time_participant() {
    let mut plan = CheckpointPlan::from_physical_with_references(
        &linear_reference_plan(true),
        vec![dependency("limits")],
    )
    .unwrap();
    for kind in [2u8, 3] {
        plan.states[0].window_kind = kind;
        assert!(
            snapshot_version_for(&plan, "file").is_err(),
            "window kind {kind}"
        );
    }
}

#[test]
fn completion_hysteresis_profiles_are_separate_from_legacy_iot_profiles() {
    let linear = hysteresis_manifest(false);
    assert_eq!(snapshot_version_for(&linear, "file").unwrap(), 12);
    assert_eq!(snapshot_version_for(&linear, "jetstream-v1").unwrap(), 13);

    let graph = hysteresis_manifest(true);
    assert_eq!(snapshot_version_for(&graph, "file-dag-v1").unwrap(), 12);
    assert!(snapshot_version_for(&graph, "jetstream-v1").is_err());
}

#[test]
fn completion_hysteresis_snapshot_decode_rejects_legacy_profile_downgrade() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for (kind, version, legacy) in [("file", 12u16, 6u16), ("jetstream-v1", 13, 7)] {
        let frame = crate::iot::IotFreeze::new(OperatorId::new(11), 6, Vec::new())
            .unwrap()
            .encode()
            .unwrap();
        let lease = owner
            .acquire(sparrow_model::CreditKind::Reservation, frame.capacity())
            .unwrap();
        let acks = ParticipantAcks {
            attempt: 1,
            generation: [0x73; 16],
            freezes: vec![crate::barrier::EncodedFreeze {
                bytes: frame,
                lease,
                ext: false,
                buffered: false,
            }],
            next_output: (kind == "jetstream-v1")
                .then(|| OutputSequence::new([0x73; 16], 1).unwrap()),
        };
        let encoded = PipelineSnapshot::encode_frozen(
            1,
            &source(kind),
            0,
            1,
            &hysteresis_manifest(false),
            acks,
            &owner,
            16,
        )
        .unwrap();
        assert_eq!(
            u16::from_le_bytes(encoded.bytes()[4..6].try_into().unwrap()),
            version
        );
        let restored = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        assert_eq!(restored.iot[0].kind, 6);
        let mut corrupt = encoded.bytes().to_vec();
        corrupt[4..6].copy_from_slice(&legacy.to_le_bytes());
        assert!(PipelineSnapshot::decode(&corrupt, 16).is_err());
        assert!(PipelineSnapshot::decode_mode(&corrupt, 16, false).is_err());
        // The inverse spoof keeps the new outer profile but changes both
        // participant and frame to legacy Change. A profile must require its
        // new semantics, not merely permit them.
        let mut corrupt = encoded.bytes().to_vec();
        let plan_start = 98
            + kind.len()
            + "reference-completion".len()
            + if kind == "jetstream-v1" { 24 } else { 0 };
        corrupt[plan_start + 24] = 4;
        let frame_kind = corrupt.len() - 5;
        corrupt[frame_kind] = 4;
        assert!(PipelineSnapshot::decode(&corrupt, 16).is_err());
    }
}

#[test]
fn completion_profile_header_changes_cannot_cross_reference_versions() {
    let plan = CheckpointPlan::from_physical_with_references(
        &linear_reference_plan(false),
        vec![dependency("limits")],
    )
    .unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let acks = ParticipantAcks {
        attempt: 1,
        generation: [0x71; 16],
        freezes: Vec::new(),
        next_output: None,
    };
    let encoded =
        PipelineSnapshot::encode_frozen(1, &source("file"), 0, 1, &plan, acks, &owner, 16).unwrap();
    assert_eq!(
        u16::from_le_bytes(encoded.bytes()[4..6].try_into().unwrap()),
        REFERENCE_SNAPSHOT_VERSION
    );
    for version in [3u16, 5, 9, 10, 11, 12, 13] {
        let mut corrupt = encoded.bytes().to_vec();
        corrupt[4..6].copy_from_slice(&version.to_le_bytes());
        assert!(
            PipelineSnapshot::decode(&corrupt, 16).is_err(),
            "version {version}"
        );
    }

    let reliable_acks = ParticipantAcks {
        attempt: 1,
        generation: [0x72; 16],
        freezes: Vec::new(),
        next_output: Some(OutputSequence::new([0x72; 16], 1).unwrap()),
    };
    let reliable = PipelineSnapshot::encode_frozen(
        1,
        &source("jetstream-v1"),
        0,
        1,
        &plan,
        reliable_acks,
        &owner,
        16,
    )
    .unwrap();
    assert_eq!(
        u16::from_le_bytes(reliable.bytes()[4..6].try_into().unwrap()),
        REFERENCE_RELIABLE_SNAPSHOT_VERSION
    );
    let mut wrong_epoch = reliable.bytes().to_vec();
    // The epoch follows the fixed source/attempt/revision/generation header.
    let epoch = 94 + "jetstream-v1".len() + "reference-completion".len();
    wrong_epoch[epoch] ^= 1;
    assert!(PipelineSnapshot::decode(&wrong_epoch, 16).is_err());
}

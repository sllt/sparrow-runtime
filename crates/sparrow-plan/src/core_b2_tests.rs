use crate::{
    CheckpointPlan, LookupSpec, ParticipantId, PhysicalPlan, PhysicalStage,
    ReferenceTableDependency, TransformStep,
};
use sparrow_expr::Expr;
use sparrow_model::{
    DataType, Field, FieldId, OperatorId, PipelineId, RevisionId, Schema, SchemaId,
};

fn dependency(name: &str) -> ReferenceTableDependency {
    ReferenceTableDependency {
        name: name.into(),
        revision: 1,
        canonical_sha256: [7; 32],
        runtime_crc32: 42,
    }
}

fn lookup_plan() -> PhysicalPlan {
    let input = Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "id", DataType::Utf8, false)],
    )
    .unwrap();
    let table = Schema::new(
        SchemaId::new(2),
        vec![Field::new(FieldId::new(1), "site", DataType::Utf8, false)],
    )
    .unwrap();
    let spec = LookupSpec::static_table(
        "sites",
        vec!["id".into()],
        vec!["key".into()],
        vec!["site".into()],
    );
    let output = crate::lookup_output_schema(&input, &table, &spec.keep).unwrap();
    PhysicalPlan {
        pipeline: PipelineId::new(1),
        revision: RevisionId::new(1),
        edges: None,
        source_times: vec![],
        side_outputs: vec![],
        stages: vec![
            PhysicalStage::MemorySource {
                operator: OperatorId::new(1),
                name: "s".into(),
                schema: input.clone(),
            },
            PhysicalStage::Lookup {
                operator: OperatorId::new(2),
                spec,
                input,
                output: output.clone(),
            },
            PhysicalStage::CaptureSink {
                operator: OperatorId::new(3),
                name: "out".into(),
                schema: output,
            },
        ],
    }
}

fn layout() -> CheckpointPlan {
    CheckpointPlan::from_physical_with_references(&lookup_plan(), vec![dependency("sites")])
        .unwrap()
}

#[test]
fn core_b2_cpl3_roundtrip_references_are_not_state_participants() {
    let plan = layout();
    assert!(plan.has_references());
    assert!(plan.states.is_empty());
    assert_eq!(plan.recovery_prefix_len, None);
    assert_eq!(
        plan.participants().into_iter().collect::<Vec<_>>(),
        vec![
            ParticipantId::Source(plan.source),
            ParticipantId::Sink(plan.sink)
        ]
    );
    let bytes = plan.encode().unwrap();
    assert_eq!(&bytes[..4], b"CPL3");
    assert_eq!(CheckpointPlan::decode(&bytes).unwrap(), plan);
}

#[test]
fn core_b2_legacy_cpl1_bytes_and_lookup_rejection_remain() {
    assert!(CheckpointPlan::from_physical(&lookup_plan()).is_err());
    let mut physical = lookup_plan();
    physical.stages.remove(1);
    let schema = physical.source_schema().unwrap().clone();
    if let PhysicalStage::CaptureSink { schema: output, .. } = &mut physical.stages[1] {
        *output = schema;
    }
    let plan = CheckpointPlan::from_physical(&physical).unwrap();
    assert!(!plan.has_references());
    let bytes = plan.encode().unwrap();
    // The old outer grammar contains no table section at all.
    let mut expected = b"CPL1".to_vec();
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.extend_from_slice(&3u32.to_le_bytes());
    expected.extend_from_slice(&0u16.to_le_bytes());
    expected.extend_from_slice(&((plan.semantics.len() + 13) as u32).to_le_bytes());
    expected.extend_from_slice(b"CP01\0RCP2");
    expected.extend_from_slice(&(plan.recovery_prefix_len.unwrap() as u32).to_le_bytes());
    expected.extend_from_slice(&plan.semantics);
    assert_eq!(bytes, expected);
    assert_eq!(CheckpointPlan::decode(&bytes).unwrap(), plan);
    let mut abandoned = bytes;
    abandoned[..4].copy_from_slice(b"CPL2");
    assert!(CheckpointPlan::decode(&abandoned).is_err());
}

#[test]
fn core_b2_dependencies_require_exact_used_set() {
    let physical = lookup_plan();
    for deps in [
        vec![],
        vec![dependency("unused")],
        vec![dependency("sites"), dependency("unused")],
        vec![dependency("sites"), dependency("sites")],
    ] {
        assert!(CheckpointPlan::from_physical_with_references(&physical, deps).is_err());
    }
    let mut bad = layout();
    bad.reference_tables.push(dependency("sites"));
    assert!(bad.validate().is_err());
}

#[test]
fn core_b2_dependency_identity_changes_refuse_restore() {
    let original = layout();
    for change in 0..4 {
        let mut changed = original.clone();
        let dep = &mut changed.reference_tables[0];
        match change {
            0 => dep.name = "other".into(),
            1 => dep.revision += 1,
            2 => dep.canonical_sha256[0] ^= 1,
            _ => dep.runtime_crc32 ^= 1,
        }
        assert!(original.check_compatible(&changed).is_err());
    }
    let mut zero_crc = original.clone();
    zero_crc.reference_tables[0].runtime_crc32 = 0;
    assert!(zero_crc.validate().is_ok());
}

#[test]
fn core_b2_dependency_bounds_and_canonical_order() {
    for change in 0..7 {
        let mut bad = layout();
        let dep = &mut bad.reference_tables[0];
        match change {
            0 => dep.name.clear(),
            1 => dep.name = "x".repeat(65),
            2 => dep.name = "../table/name".into(),
            3 => dep.revision = 0,
            4 => dep.revision = u64::MAX,
            5 => dep.canonical_sha256 = [0; 32],
            _ => bad.reference_tables = (0..9).map(|i| dependency(&format!("t{i}"))).collect(),
        }
        assert!(bad.validate().is_err());
        assert!(bad.encode().is_err());
    }
    let mut bad = layout();
    bad.reference_tables = vec![dependency("z"), dependency("a")];
    assert!(bad.validate().is_err());
}

#[test]
fn core_b2_cpl3_decoder_rejects_every_truncation_and_trailing_data() {
    let bytes = layout().encode().unwrap();
    for n in 0..bytes.len() {
        assert!(
            CheckpointPlan::decode(&bytes[..n]).is_err(),
            "truncation at {n}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(CheckpointPlan::decode(&trailing).is_err());
    for count in [0u16, 9, u16::MAX] {
        let mut bad = bytes.clone();
        bad[14..16].copy_from_slice(&count.to_le_bytes());
        assert!(CheckpointPlan::decode(&bad).is_err());
    }
    let mut invalid_utf8 = bytes.clone();
    invalid_utf8[18] = 0xff;
    assert!(CheckpointPlan::decode(&invalid_utf8).is_err());
    let mut state = bytes;
    state[12..14].copy_from_slice(&1u16.to_le_bytes());
    assert!(CheckpointPlan::decode(&state).is_err());
}

#[test]
fn core_b2_lookup_semantics_reject_downstream_relaxation() {
    let original = layout();
    let mut physical = lookup_plan();
    let input = match &physical.stages[2] {
        PhysicalStage::CaptureSink { schema, .. } => schema.clone(),
        _ => unreachable!(),
    };
    physical.stages.insert(
        2,
        PhysicalStage::Transform {
            steps: vec![TransformStep::Filter {
                operator: OperatorId::new(4),
                input,
                predicate: Expr::Literal(sparrow_model::Scalar::Bool(true)),
            }],
        },
    );
    let changed =
        CheckpointPlan::from_physical_with_references(&physical, vec![dependency("sites")])
            .unwrap();
    assert!(original.check_compatible(&changed).is_err());
    let mut relaxed = original.clone();
    relaxed.recovery_prefix_len = Some(4);
    assert!(relaxed.validate().is_err());
}

#[test]
fn core_b2_lookup_rejects_temporal_graph_and_malformed_schema() {
    for change in 0..6 {
        let mut physical = lookup_plan();
        if change == 0 {
            physical.edges = Some(vec![]);
        }
        if let PhysicalStage::Lookup {
            spec,
            input,
            output,
            ..
        } = &mut physical.stages[1]
        {
            match change {
                1 => spec.temporal = true,
                2 => spec.as_of_field = Some("id".into()),
                3 => output.fields[1].nullable = false,
                4 => input.fields[0].name = "changed".into(),
                5 => spec.stream_keys[0] = "missing".into(),
                _ => {}
            }
        }
        assert!(CheckpointPlan::from_physical_with_references(
            &physical,
            vec![dependency("sites")]
        )
        .is_err());
    }
}

#[test]
fn core_b2_multiple_dependencies_sort_independently_of_lookup_order() {
    let mut physical = lookup_plan();
    let input = match &physical.stages[2] {
        PhysicalStage::CaptureSink { schema, .. } => schema.clone(),
        _ => unreachable!(),
    };
    physical.stages.insert(
        2,
        PhysicalStage::Lookup {
            operator: OperatorId::new(4),
            spec: LookupSpec::static_table("alerts", vec!["id".into()], vec!["key".into()], vec![]),
            input: input.clone(),
            output: input,
        },
    );
    let a = CheckpointPlan::from_physical_with_references(
        &physical,
        vec![dependency("sites"), dependency("alerts")],
    )
    .unwrap();
    let b = CheckpointPlan::from_physical_with_references(
        &physical,
        vec![dependency("alerts"), dependency("sites")],
    )
    .unwrap();
    assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    assert_eq!(a.reference_tables[0].name, "alerts");
    let mut changed = physical;
    if let PhysicalStage::Lookup { spec, .. } = &mut changed.stages[1] {
        spec.table_keys[0] = "another_key".into();
    }
    let changed =
        CheckpointPlan::from_physical_with_references(&changed, a.reference_tables.clone())
            .unwrap();
    assert!(a.check_compatible(&changed).is_err());
}

#[test]
fn core_b2_reference_checkpoint_expands_count_but_excludes_time() {
    let mut physical = lookup_plan();
    let input = match &physical.stages[2] {
        PhysicalStage::CaptureSink { schema, .. } => schema.clone(),
        _ => unreachable!(),
    };
    let spec = crate::WindowSpec::new(
        sparrow_model::WindowKind::Count { size: 2 },
        vec![],
        vec![crate::AggCall::count_star("n")],
    );
    let output = crate::window_output_schema(&input, &spec).unwrap();
    physical.stages.insert(
        2,
        PhysicalStage::WindowAgg {
            operator: OperatorId::new(4),
            spec,
            input,
            output: output.clone(),
        },
    );
    if let PhysicalStage::CaptureSink { schema, .. } = &mut physical.stages[3] {
        *schema = output;
    }
    let manifest = CheckpointPlan::from_physical_with_references(
        &physical,
        vec![dependency("sites")],
    )
    .unwrap();
    assert_eq!(manifest.states.len(), 1);
    if let PhysicalStage::WindowAgg { spec, .. } = &mut physical.stages[2] {
        spec.kind = sparrow_model::WindowKind::TumblingProcessingTime { size_micros: 1_000 };
    }
    assert!(CheckpointPlan::from_physical_with_references(&physical, vec![dependency("sites")]).is_err());
}

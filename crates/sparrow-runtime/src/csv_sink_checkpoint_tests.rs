//! Independent v27/JSI1 compatibility and target-bound v28/JSI2 contracts.
use crate::pipeline_checkpoint::{
    StoredSnapshot, FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION, FILE_JETSTREAM_SINK_SNAPSHOT_VERSION,
};
use crate::{
    sink_snapshot_version_for, CheckpointStore, ParticipantAcks, PipelineRestore, PipelineSnapshot,
    SinkRestoreBinding,
};
use sparrow_io::{
    CsvEncodeIdentity, OwnedSinkIdentity, SinkEncoding, SinkIdentity, SourceIdentity,
    SourcePosition,
};
use sparrow_model::{DataType, ErrorCode, Field, MemoryOwner, ResourceBudget, Scalar, Schema};
use sparrow_plan::{CheckpointPlan, PhysicalPlan, PhysicalStage, TransformStep};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn json_target() -> SinkIdentity {
    SinkIdentity::jetstream(
        &["tls://localhost:4222".into()],
        Some("env:JS_TOKEN"),
        "OUT",
        1_700_000_000_000_000_001,
        "out.rows",
        Some("v"),
    )
    .unwrap()
}
fn csv_target() -> SinkIdentity {
    json_target()
        .with_csv(CsvEncodeIdentity::new(b',', b'"', true, "").unwrap())
        .unwrap()
}
fn plan(keep: bool) -> PhysicalPlan {
    let schema = Schema::new(1, vec![Field::new(1, "v", DataType::Int64, false)]).unwrap();
    PhysicalPlan {
        pipeline: 1.into(),
        revision: 1.into(),
        edges: None,
        side_outputs: vec![],
        source_times: vec![],
        stages: vec![
            PhysicalStage::MemorySource {
                operator: 1.into(),
                name: "input".into(),
                schema: schema.clone(),
            },
            PhysicalStage::Transform {
                steps: vec![TransformStep::Filter {
                    operator: 2.into(),
                    predicate: sparrow_expr::Expr::Literal(Scalar::Bool(keep)),
                    input: schema.clone(),
                }],
            },
            PhysicalStage::CaptureSink {
                operator: 3.into(),
                name: "out".into(),
                schema,
            },
        ],
    }
}
fn source() -> SourcePosition {
    SourcePosition {
        offset_bytes: 36,
        record_index: 4,
        identity: SourceIdentity {
            kind: "file".into(),
            path: "fixture.ndjson".into(),
            size: 48,
            fingerprint: 17,
        },
    }
}
fn encode(
    id: u64,
    target: &SinkIdentity,
    owner: &Arc<MemoryOwner>,
) -> crate::checkpoint::EncodedSnapshot {
    PipelineSnapshot::encode_frozen_with_sink(
        id,
        &source(),
        4,
        9,
        &CheckpointPlan::from_physical(&plan(true)).unwrap(),
        target,
        ParticipantAcks {
            attempt: 5,
            generation: [7; 16],
            freezes: vec![],
            next_output: None,
        },
        owner,
        16,
    )
    .unwrap()
}
fn put_string(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
}
fn put_option(out: &mut Vec<u8>, text: Option<&str>) {
    out.push(u8::from(text.is_some()));
    if let Some(text) = text {
        put_string(out, text);
    }
}
/// Original JSI1 layout, deliberately assembled without the current encoder.
fn legacy_target_wire(target: &SinkIdentity) -> Vec<u8> {
    let mut out = b"JSI1".to_vec();
    out.push(target.endpoints.len() as u8);
    for endpoint in &target.endpoints {
        put_string(&mut out, endpoint);
    }
    put_option(&mut out, target.token_secret.as_deref());
    put_string(&mut out, &target.stream);
    out.extend_from_slice(&target.created_nanos.to_le_bytes());
    put_string(&mut out, &target.subject);
    put_option(&mut out, target.msg_id_column.as_deref());
    out
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "sparrow-v28-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn csv_identity_extension_preserves_every_v27_json_byte() {
    let target = json_target();
    let identity = legacy_target_wire(&target);
    let mut actual = Vec::new();
    target.encode_into(&mut actual).unwrap();
    assert_eq!(actual, identity);
    assert_eq!(target.encoded_len().unwrap(), identity.len());
    assert_eq!(
        SinkIdentity::decode(&identity).unwrap().encoding,
        SinkEncoding::Json
    );
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let manifest = CheckpointPlan::from_physical(&plan(true)).unwrap();
    let descriptor = manifest.encode().unwrap();
    let payload = encode(1, &target, &owner);
    // Original v27 envelope, including its exact CPL1 descriptor and framing.
    let mut expected = b"SPV1".to_vec();
    expected.extend_from_slice(&27u16.to_le_bytes());
    expected.extend_from_slice(&1u64.to_le_bytes());
    expected.extend_from_slice(&4u64.to_le_bytes());
    crate::checkpoint::encode_position(&source(), &mut expected).unwrap();
    expected.extend_from_slice(&5u64.to_le_bytes());
    expected.extend_from_slice(&9u64.to_le_bytes());
    expected.extend_from_slice(&[7; 16]);
    expected.extend_from_slice(&(identity.len() as u32).to_le_bytes());
    expected.extend_from_slice(&identity);
    expected.extend_from_slice(&(descriptor.len() as u32).to_le_bytes());
    expected.extend_from_slice(&descriptor);
    expected.extend_from_slice(&0u16.to_le_bytes());
    assert_eq!(payload.bytes(), expected);
    assert_eq!(
        sink_snapshot_version_for(&manifest, "file", &target).unwrap(),
        FILE_JETSTREAM_SINK_SNAPSHOT_VERSION
    );
}

#[test]
fn jsi2_csv_options_roundtrip_with_strict_tags_limits_and_truncation() {
    for options in [
        CsvEncodeIdentity::new(b',', b'"', true, "").unwrap(),
        CsvEncodeIdentity::new(b'\t', b'\'', false, "\\N").unwrap(),
        CsvEncodeIdentity::new(b';', b'"', true, &"n".repeat(64)).unwrap(),
    ] {
        let target = json_target().with_csv(options.clone()).unwrap();
        let mut actual = Vec::new();
        target.encode_into(&mut actual).unwrap();
        let mut expected = legacy_target_wire(&target);
        let extension = expected.len();
        expected[..4].copy_from_slice(b"JSI2");
        expected.extend_from_slice(&[
            1,
            options.delimiter,
            options.quote,
            u8::from(options.header),
        ]);
        put_string(&mut expected, &options.null_value);
        assert_eq!(actual, expected);
        assert_eq!(target.encoded_len().unwrap(), actual.len());
        assert_eq!(SinkIdentity::decode(&actual).unwrap(), target);
        for end in 0..actual.len() {
            assert!(SinkIdentity::decode(&actual[..end]).is_err());
        }
        for (index, value) in [(0, 0), (0, 2), (1, b'\n'), (2, options.delimiter), (3, 2)] {
            let mut malformed = actual.clone();
            malformed[extension + index] = value;
            assert!(SinkIdentity::decode(&malformed).is_err());
        }
        let mut malformed = actual.clone();
        malformed[extension + 4..extension + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(SinkIdentity::decode(&malformed).is_err());
        let mut malformed = actual;
        malformed.push(0);
        assert!(SinkIdentity::decode(&malformed).is_err());
    }
    for (delimiter, quote, marker) in [
        (b',', b',', ""),
        (b'a', b'"', ""),
        (b',', b'\t', ""),
        (b',', b'"', "a,b"),
        (b',', b'"', "a b"),
        (b',', b'"', "x\n"),
    ] {
        assert!(CsvEncodeIdentity::new(delimiter, quote, true, marker).is_err());
    }
    assert!(CsvEncodeIdentity::new(b',', b'"', true, &"n".repeat(65)).is_err());
}

#[test]
fn v28_requires_jsi2_and_never_accepts_v27_jsi1_as_csv() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = csv_target();
    let manifest = CheckpointPlan::from_physical(&plan(true)).unwrap();
    let encoded = encode(1, &target, &owner);
    assert_eq!(&encoded.bytes()[4..6], &28u16.to_le_bytes());
    assert_eq!(
        sink_snapshot_version_for(&manifest, "file", &target).unwrap(),
        FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION
    );
    let snapshot = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
    assert_eq!(snapshot.sink_identity.as_ref(), Some(&target));
    assert_eq!(snapshot.source, source());
    assert!(snapshot.next_output.is_none());
    assert_eq!(
        PipelineSnapshot::provenance(encoded.bytes()).unwrap(),
        (5, 9, [7; 16])
    );
    snapshot
        .check_compatible_with_sink(&manifest, &target)
        .unwrap();
    assert!(snapshot.check_compatible(&manifest).is_err());
    assert!(snapshot
        .check_compatible_with_sink(&manifest, &json_target())
        .is_err());
    for end in 0..encoded.bytes().len() {
        assert!(PipelineSnapshot::decode(&encoded.bytes()[..end], 16).is_err());
        assert!(StoredSnapshot::decode(&encoded.bytes()[..end], 16, false).is_err());
    }
    for (target, outer) in [(json_target(), 28u16), (csv_target(), 27u16)] {
        let mut wrong = encode(1, &target, &owner);
        wrong.bytes[4..6].copy_from_slice(&outer.to_le_bytes());
        let error = PipelineSnapshot::decode(wrong.bytes(), 16).unwrap_err();
        assert_eq!(error.code, ErrorCode::UnsupportedRestore);
        assert!(error
            .context
            .iter()
            .any(|(_, value)| value == "sink_profile_mismatch"));
        assert!(PipelineSnapshot::encoded_sink_identity(wrong.bytes()).is_err());
    }
}

#[test]
fn v28_binds_each_encode_option_and_full_output_semantics_at_both_checks() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = csv_target();
    let manifest = Arc::new(CheckpointPlan::from_physical(&plan(true)).unwrap());
    let snapshot = PipelineSnapshot::decode(encode(1, &target, &owner).bytes(), 16).unwrap();
    let mutations: [fn(&mut CsvEncodeIdentity); 4] = [
        |csv| csv.delimiter = b';',
        |csv| csv.quote = b'\'',
        |csv| csv.header = false,
        |csv| csv.null_value = "\\N".into(),
    ];
    let saved = OwnedSinkIdentity::new(target.clone(), &owner).unwrap();
    for mutate in mutations {
        let mut changed = target.clone();
        let SinkEncoding::Csv(csv) = &mut changed.encoding else {
            unreachable!()
        };
        mutate(csv);
        changed.validate().unwrap();
        assert_eq!(
            snapshot
                .check_compatible_with_sink(&manifest, &changed)
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedRestore
        );
        let restored = PipelineRestore { buffered: Vec::new(),
            plan: manifest.clone(),
            generation: [7; 16],
            restore: Some(vec![]),
            iot: vec![],
            sink: Some(SinkRestoreBinding::restored(
                OwnedSinkIdentity::new(changed, &owner).unwrap(),
                saved.clone(),
            )),
        };
        assert_eq!(
            restored
                .check_compatible(&manifest, &owner)
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedRestore
        );
    }
    let changed = CheckpointPlan::from_physical(&plan(false)).unwrap();
    manifest.check_compatible(&changed).unwrap(); // Legacy downstream relaxation.
    assert!(snapshot
        .check_compatible_with_sink(&changed, &target)
        .is_err());
    let restored = PipelineRestore { buffered: Vec::new(),
        plan: manifest,
        generation: [7; 16],
        restore: Some(vec![]),
        iot: vec![],
        sink: Some(SinkRestoreBinding::restored(saved.clone(), saved)),
    };
    assert!(restored.check_compatible(&changed, &owner).is_err());
}

#[test]
fn v28_identity_credit_covers_csv_capacity_and_shares_the_exact_owner() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = OwnedSinkIdentity::new(csv_target(), &owner).unwrap();
    let baseline = owner.usage().physical_bytes;
    assert!(baseline >= target.identity().resident_bytes());
    let clone = target.clone();
    assert_eq!(owner.usage().physical_bytes, baseline);
    let other = MemoryOwner::new(ResourceBudget::compact());
    assert!(!target.belongs_to(&other));
    let restored = PipelineRestore { buffered: Vec::new(),
        plan: Arc::new(CheckpointPlan::from_physical(&plan(true)).unwrap()),
        generation: [7; 16],
        restore: Some(vec![]),
        iot: vec![],
        sink: Some(SinkRestoreBinding::restored(
            target.clone(),
            OwnedSinkIdentity::new(csv_target(), &other).unwrap(),
        )),
    };
    assert!(restored.check_compatible(&restored.plan, &owner).is_err());
    drop(restored);
    drop(target);
    assert_eq!(owner.usage().physical_bytes, baseline);
    drop(clone);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(other.usage().physical_bytes, 0);
    let mut roomy_capacity = String::with_capacity(64 * 1024);
    roomy_capacity.push('n');
    let target = json_target()
        .with_csv(CsvEncodeIdentity {
            delimiter: b',',
            quote: b'"',
            header: true,
            null_value: roomy_capacity,
        })
        .unwrap();
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = 8192;
    let limited = MemoryOwner::new(budget);
    assert_eq!(
        OwnedSinkIdentity::new(target, &limited).unwrap_err().code,
        ErrorCode::ResourceExhausted
    );
    budget.reservation_bytes = 1;
    let limited = MemoryOwner::new(budget);
    assert_eq!(
        PipelineSnapshot::encode_frozen_with_sink(
            1,
            &source(),
            4,
            9,
            &CheckpointPlan::from_physical(&plan(true)).unwrap(),
            &csv_target(),
            ParticipantAcks {
                attempt: 5,
                generation: [7; 16],
                freezes: vec![],
                next_output: None
            },
            &limited,
            16,
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(limited.usage().physical_bytes, 0);
}

#[test]
fn json_csv_directories_and_each_csv_option_cannot_change_in_place() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let manifest = CheckpointPlan::from_physical(&plan(true)).unwrap();
    for initial in [json_target(), csv_target()] {
        let dir = Directory::new();
        let owned = OwnedSinkIdentity::new(initial.clone(), &owner).unwrap();
        let mut store = CheckpointStore::open_file_jetstream_sink_exclusive(
            &dir.0,
            16,
            Default::default(),
            &manifest,
            owned.clone(),
        )
        .unwrap();
        store.activate_state_generation([7; 16]).unwrap();
        store.commit_prepared(&encode(1, &initial, &owner)).unwrap();
        let current = std::fs::read(dir.0.join("CURRENT")).unwrap();
        let marker = std::fs::read(dir.0.join("STATE_GENERATION")).unwrap();
        let foreign = match &initial.encoding {
            SinkEncoding::Json => csv_target(),
            SinkEncoding::Csv(_) => json_target(),
        };
        let payload = encode(2, &foreign, &owner);
        assert_eq!(
            store.commit_prepared(&payload).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        assert_eq!(
            store.commit_encoded(2, payload.bytes()).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        drop(store);
        assert_eq!(
            CheckpointStore::open_file_jetstream_sink_exclusive(
                &dir.0,
                16,
                Default::default(),
                &manifest,
                OwnedSinkIdentity::new(foreign, &owner).unwrap(),
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::UnsupportedRestore
        );
        if let SinkEncoding::Csv(_) = &initial.encoding {
            let mutations: [fn(&mut CsvEncodeIdentity); 4] = [
                |csv| csv.delimiter = b';',
                |csv| csv.quote = b'\'',
                |csv| csv.header = false,
                |csv| csv.null_value = "NULL".into(),
            ];
            for mutate in mutations {
                let mut changed = initial.clone();
                let SinkEncoding::Csv(csv) = &mut changed.encoding else {
                    unreachable!()
                };
                mutate(csv);
                assert_eq!(
                    CheckpointStore::open_file_jetstream_sink_exclusive(
                        &dir.0,
                        16,
                        Default::default(),
                        &manifest,
                        OwnedSinkIdentity::new(changed, &owner).unwrap(),
                    )
                    .err()
                    .unwrap()
                    .code,
                    ErrorCode::UnsupportedRestore
                );
            }
        }
        assert_eq!(std::fs::read(dir.0.join("CURRENT")).unwrap(), current);
        assert_eq!(
            std::fs::read(dir.0.join("STATE_GENERATION")).unwrap(),
            marker
        );
        assert!(!dir.0.join("chk-00000002").exists());
        let store = CheckpointStore::open_file_jetstream_sink_exclusive(
            &dir.0,
            16,
            Default::default(),
            &manifest,
            owned,
        )
        .unwrap();
        assert_eq!(
            store
                .recover_pipeline_required()
                .unwrap()
                .sink_identity
                .as_ref(),
            Some(&initial)
        );
        assert_eq!(
            store.inventory().unwrap().generations[0]
                .metadata
                .as_ref()
                .unwrap()
                .version,
            match initial.encoding {
                SinkEncoding::Json => 27,
                SinkEncoding::Csv(_) => 28,
            }
        );
    }
}

#[test]
fn valid_outer_v28_with_jsi1_is_incompatibility_not_corruption_fallback() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = csv_target();
    let manifest = CheckpointPlan::from_physical(&plan(true)).unwrap();
    let dir = Directory::new();
    let mut loose = CheckpointStore::open_exclusive(&dir.0, 16, Default::default()).unwrap();
    loose.commit_prepared(&encode(1, &target, &owner)).unwrap();
    // Only internal tests can mutate trusted encoder bytes. Simulate a
    // checksum-valid future/foreign generation without authorizing its codec.
    let mut wrong = encode(2, &json_target(), &owner);
    wrong.bytes[4..6].copy_from_slice(&28u16.to_le_bytes());
    loose.commit_prepared(&wrong).unwrap();
    drop(loose);
    let before = std::fs::read(dir.0.join("CURRENT")).unwrap();
    let readonly = CheckpointStore::open_readonly(&dir.0).unwrap();
    assert_eq!(
        readonly.recover_pipeline_required().unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    assert_eq!(
        CheckpointStore::open_file_jetstream_sink_exclusive(
            &dir.0,
            16,
            Default::default(),
            &manifest,
            OwnedSinkIdentity::new(target, &owner).unwrap(),
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::UnsupportedRestore
    );
    assert_eq!(std::fs::read(dir.0.join("CURRENT")).unwrap(), before);
}

#[test]
fn valid_crc_foreign_source_and_manifest_scope_never_fall_back_in_v27_or_v28() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let admitted = CheckpointPlan::from_physical(&plan(true)).unwrap();
    let mut graph = plan(true);
    graph.edges = Some(vec![
        sparrow_plan::physical::PhysicalEdge {
            from: 0,
            to: 1,
            port: 2.into(),
            best_effort: false,
        },
        sparrow_plan::physical::PhysicalEdge {
            from: 1,
            to: 2,
            port: 3.into(),
            best_effort: false,
        },
    ]);
    let graph = CheckpointPlan::from_physical(&graph).unwrap();
    assert!(graph.is_graph());
    let mut iot = plan(true);
    let schema = match &iot.stages[0] {
        PhysicalStage::MemorySource { schema, .. } => schema.clone(),
        _ => unreachable!(),
    };
    iot.stages[1] = PhysicalStage::Iot {
        operator: 2.into(),
        input: schema.clone(),
        output: schema,
        spec: sparrow_plan::IotSpec {
            keys: vec!["v".into()],
            fields: vec!["v".into()],
            emit_first: true,
            ttl_micros: 0,
            max_keys: 16,
            invalid: sparrow_plan::InvalidValuePolicy::Error,
            deadband: None,
            hysteresis: None,
            timing: None,
        },
    };
    let iot = CheckpointPlan::from_physical(&iot).unwrap();
    assert!(iot.has_iot());

    for target in [json_target(), csv_target()] {
        let version = sink_snapshot_version_for(&admitted, "file", &target).unwrap();
        for (kind, foreign_plan) in [
            ("xxxx", &admitted),
            ("jetstream-v1", &admitted),
            ("file", &graph),
            ("file", &iot),
        ] {
            // Assemble a checksum-valid foreign artifact independently of
            // the admitted encoder. The scope gate must run before legacy
            // cursor/IoT/graph checks or absent foreign-state frames.
            let mut position = source();
            position.identity.kind = kind.into();
            let descriptor = foreign_plan.encode().unwrap();
            let mut identity = Vec::new();
            target.encode_into(&mut identity).unwrap();
            let mut bytes = b"SPV1".to_vec();
            bytes.extend_from_slice(&version.to_le_bytes());
            bytes.extend_from_slice(&2u64.to_le_bytes());
            bytes.extend_from_slice(&4u64.to_le_bytes());
            crate::checkpoint::encode_position(&position, &mut bytes).unwrap();
            bytes.extend_from_slice(&5u64.to_le_bytes());
            bytes.extend_from_slice(&9u64.to_le_bytes());
            bytes.extend_from_slice(&[7; 16]);
            bytes.extend_from_slice(&(identity.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&identity);
            bytes.extend_from_slice(&(descriptor.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&descriptor);
            bytes.extend_from_slice(&0u16.to_le_bytes());
            let lease = owner
                .acquire(sparrow_model::CreditKind::Reservation, bytes.capacity())
                .unwrap();
            let foreign = crate::checkpoint::EncodedSnapshot::prepared(bytes, lease, 2, 0);
            assert_eq!(
                PipelineSnapshot::decode(foreign.bytes(), 16)
                    .unwrap_err()
                    .code,
                ErrorCode::UnsupportedRestore
            );

            let dir = Directory::new();
            let mut loose =
                CheckpointStore::open_exclusive(&dir.0, 16, Default::default()).unwrap();
            loose.activate_state_generation([7; 16]).unwrap();
            loose.commit_prepared(&encode(1, &target, &owner)).unwrap();
            loose.commit_prepared(&foreign).unwrap();
            drop(loose);
            let current = std::fs::read(dir.0.join("CURRENT")).unwrap();
            let marker = std::fs::read(dir.0.join("STATE_GENERATION")).unwrap();
            let readonly = CheckpointStore::open_readonly(&dir.0).unwrap();
            assert_eq!(
                readonly.recover_pipeline_required().unwrap_err().code,
                ErrorCode::UnsupportedRestore
            );
            assert_eq!(
                CheckpointStore::open_file_jetstream_sink_exclusive(
                    &dir.0,
                    16,
                    Default::default(),
                    &admitted,
                    OwnedSinkIdentity::new(target.clone(), &owner).unwrap(),
                )
                .err()
                .unwrap()
                .code,
                ErrorCode::UnsupportedRestore
            );
            assert_eq!(std::fs::read(dir.0.join("CURRENT")).unwrap(), current);
            assert_eq!(
                std::fs::read(dir.0.join("STATE_GENERATION")).unwrap(),
                marker
            );
        }
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

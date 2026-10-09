//! v27 is a target-bound File profile, not an extension of legacy v3/CPL1.
use crate::barrier::{EncodedFreeze, RuntimeAligned};
use crate::checkpoint::CheckpointRetention;
use crate::pipeline_checkpoint::{StoredSnapshot, FILE_JETSTREAM_SINK_SNAPSHOT_VERSION};
use crate::window::WindowOperator;
use crate::{
    AlignedAcks, AlignedJob, CheckpointStore, IngressEvent, JobRequest, Kernel, KernelOptions,
    MailboxConfig, ParticipantAcks, PipelineRestore, PipelineSnapshot, SharedCapture,
    SinkRestoreBinding, StreamControl,
};
use sparrow_io::{OwnedSinkIdentity, SinkIdentity, SourceIdentity, SourcePosition};
use sparrow_model::{
    AggFn, DataType, ErrorCode, Field, InflightCounter, MemoryOwner, ResourceBudget, Row, Scalar,
    Schema, WindowKind,
};
use sparrow_plan::{
    AggCall, CheckpointPlan, PhysicalPlan, PhysicalStage, TransformStep, WindowSpec,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn identity() -> SinkIdentity {
    SinkIdentity::jetstream(
        &["tls://LOCALHOST/".into()],
        Some("env:JS_TOKEN"),
        "OUT",
        1_700_000_000_000_000_001,
        "out.rows",
        None,
    )
    .unwrap()
}

fn plan(stateful: bool, keep: bool) -> PhysicalPlan {
    let mut input = Schema::new(1, vec![Field::new(1, "v", DataType::Int64, false)]).unwrap();
    let mut stages = vec![PhysicalStage::MemorySource {
        operator: 1.into(),
        name: "sensors".into(),
        schema: input.clone(),
    }];
    if stateful {
        let spec = WindowSpec::new(
            WindowKind::Count { size: 3 },
            vec![],
            vec![AggCall::new(
                AggFn::Sum,
                Some(sparrow_expr::Expr::Column { name: "v".into() }),
                "s",
            )],
        );
        let output = sparrow_plan::window_output_schema(&input, &spec).unwrap();
        stages.push(PhysicalStage::WindowAgg {
            operator: 2.into(),
            spec,
            input,
            output: output.clone(),
        });
        input = output;
    }
    stages.push(PhysicalStage::Transform {
        steps: vec![TransformStep::Filter {
            operator: 3.into(),
            predicate: sparrow_expr::Expr::Literal(Scalar::Bool(keep)),
            input: input.clone(),
        }],
    });
    stages.push(PhysicalStage::CaptureSink {
        operator: 4.into(),
        name: "out".into(),
        schema: input,
    });
    PhysicalPlan {
        pipeline: 1.into(),
        revision: 1.into(),
        stages,
        edges: None,
        side_outputs: vec![],
        source_times: vec![],
    }
}

fn source() -> SourcePosition {
    SourcePosition::start(SourceIdentity {
        kind: "file".into(),
        path: "fixture.ndjson".into(),
        size: 0,
        fingerprint: 0,
    })
}

fn acks(physical: &PhysicalPlan, owner: &Arc<MemoryOwner>) -> ParticipantAcks {
    let freezes = physical
        .stages
        .iter()
        .filter_map(|stage| match stage {
            PhysicalStage::WindowAgg {
                operator,
                spec,
                input,
                ..
            } => {
                let operator = WindowOperator::new(
                    *operator,
                    spec.clone(),
                    input.clone(),
                    owner.clone(),
                    16,
                    16,
                )
                .unwrap();
                Some(EncodedFreeze::from_operator(&operator, owner, 16).unwrap())
            }
            _ => None,
        })
        .collect();
    ParticipantAcks {
        attempt: 1,
        generation: [7; 16],
        freezes,
        next_output: None,
    }
}

fn encode(
    id: u64,
    physical: &PhysicalPlan,
    target: &SinkIdentity,
    owner: &Arc<MemoryOwner>,
) -> crate::checkpoint::EncodedSnapshot {
    let manifest = CheckpointPlan::from_physical(physical).unwrap();
    PipelineSnapshot::encode_frozen_with_sink(
        id,
        &source(),
        0,
        1,
        &manifest,
        target,
        acks(physical, owner),
        owner,
        16,
    )
    .unwrap()
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "sparrow-v27-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
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

fn kernel() -> Kernel {
    Kernel::new_with_job_budget(
        KernelOptions {
            budget: ResourceBudget::performance(),
            mailbox: MailboxConfig {
                max_items: 4,
                max_bytes: 64 * 1024,
            },
            rows_per_batch: 2,
            worker_threads: 2,
        },
        ResourceBudget::compact(),
    )
    .unwrap()
}

#[test]
fn sink_identity_is_canonical_bounded_and_keeps_exact_nanoseconds() {
    let target = identity();
    assert_eq!(target.endpoints, ["tls://localhost:4222"]);
    let equivalent = SinkIdentity::jetstream(
        &["tls://localhost:4222".into(), "TLS://LocalHost./".into()],
        Some("env:JS_TOKEN"),
        "OUT",
        target.created_nanos,
        "out.rows",
        None,
    )
    .unwrap();
    assert_eq!(target, equivalent);
    let ipv6 = SinkIdentity::jetstream(
        &["nats://[0:0:0:0:0:0:0:1]".into()],
        None,
        "OUT",
        1,
        "out.rows",
        None,
    )
    .unwrap();
    assert_eq!(ipv6.endpoints, ["nats://[::1]:4222"]);
    let mut exact = target;
    exact.created_nanos = (1i128 << 75) + 123;
    let mut bytes = Vec::new();
    exact.encode_into(&mut bytes).unwrap();
    assert_eq!(bytes.len(), exact.encoded_len().unwrap());
    assert_eq!(SinkIdentity::decode(&bytes).unwrap(), exact);
    for end in 0..bytes.len() {
        assert!(SinkIdentity::decode(&bytes[..end]).is_err());
    }
    bytes.push(0);
    assert!(SinkIdentity::decode(&bytes).is_err());
}

#[test]
fn sink_identity_rejects_credentials_noncanonical_encodings_and_oversized_fields() {
    for server in [
        "tls://user:secret@localhost:4222",
        "http://localhost:4222",
        "nats://localhost:4222/path",
        "nats://localhost:4222?token=secret",
        "nats://localhost:0",
        "nats://[::1",
        "nats://localhost:99999",
    ] {
        let error = SinkIdentity::jetstream(&[server.into()], None, "OUT", 1, "out.rows", None)
            .unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
    let mut target = identity();
    target.endpoints[0] = "tls://LOCALHOST".into();
    assert!(target.validate().is_err());
    let mut target = identity();
    target.endpoints.push(target.endpoints[0].clone());
    assert!(target.validate().is_err());
    for subject in ["out.*".to_owned(), "out..rows".to_owned(), "s".repeat(4097)] {
        assert!(SinkIdentity::jetstream(
            &["nats://localhost".into()],
            None,
            "OUT",
            1,
            &subject,
            None
        )
        .is_err());
    }
    assert!(SinkIdentity::jetstream(
        &["nats://localhost".into()],
        Some("env:TOKEN"),
        "OUT",
        1,
        "out.rows",
        None
    )
    .is_err());
    assert!(SinkIdentity::jetstream(
        &["tls://localhost".into()],
        Some(&"x".repeat(1025)),
        "OUT",
        1,
        "out.rows",
        None
    )
    .is_err());
    assert!(SinkIdentity::jetstream(
        &["nats://localhost".into()],
        None,
        &"x".repeat(256),
        1,
        "out.rows",
        None
    )
    .is_err());
    assert!(SinkIdentity::jetstream(
        &["nats://localhost".into()],
        None,
        "OUT",
        0,
        "out.rows",
        None
    )
    .is_err());
    assert!(SinkIdentity::jetstream(
        &["nats://localhost".into()],
        None,
        "OUT",
        1,
        "out.rows",
        Some(&"x".repeat(257))
    )
    .is_err());
    let mut bytes = Vec::new();
    identity().encode_into(&mut bytes).unwrap();
    bytes[5..9].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(SinkIdentity::decode(&bytes).is_err());
}

#[test]
fn owned_sink_identity_arc_keeps_one_credit_and_refuses_unfunded_identity() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = OwnedSinkIdentity::new(identity(), &owner).unwrap();
    let held = owner.usage().physical_bytes;
    assert!(held >= target.identity().resident_bytes());
    let clone = target.clone();
    assert_eq!(owner.usage().physical_bytes, held);
    drop(target);
    assert_eq!(owner.usage().physical_bytes, held);
    drop(clone);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(owner.accounting_errors_total(), 0);
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = 1;
    let tiny = MemoryOwner::new(budget);
    assert_eq!(
        OwnedSinkIdentity::new(identity(), &tiny).unwrap_err().code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(tiny.usage().physical_bytes, 0);
}

#[test]
fn v27_roundtrip_truncation_provenance_and_legacy_api_rejection() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for stateful in [false, true] {
        let physical = plan(stateful, true);
        let manifest = CheckpointPlan::from_physical(&physical).unwrap();
        let target = identity();
        let encoded = encode(1, &physical, &target, &owner);
        assert_eq!(
            &encoded.bytes()[4..6],
            &FILE_JETSTREAM_SINK_SNAPSHOT_VERSION.to_le_bytes()
        );
        let decoded = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        assert_eq!(decoded.sink_identity.as_ref(), Some(&target));
        assert_eq!(decoded.source, source());
        assert_eq!(decoded.windows.len(), usize::from(stateful));
        assert_eq!(decoded.next_output, None);
        assert_eq!(
            PipelineSnapshot::provenance(encoded.bytes()).unwrap(),
            (1, 1, [7; 16])
        );
        assert_eq!(
            decoded.check_compatible(&manifest).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        decoded
            .check_compatible_with_sink(&manifest, &target)
            .unwrap();
        for end in 0..encoded.bytes().len() {
            assert!(
                PipelineSnapshot::decode(&encoded.bytes()[..end], 16).is_err(),
                "truncation at {end}"
            );
            assert!(
                StoredSnapshot::decode(&encoded.bytes()[..end], 16, false).is_err(),
                "cold truncation at {end}"
            );
        }
        let mut malformed = encoded.bytes().to_vec();
        malformed.push(0);
        assert!(PipelineSnapshot::decode(&malformed, 16).is_err());
        let at = 94 + source().identity.kind.len() + source().identity.path.len();
        let mut malformed = encoded.bytes().to_vec();
        malformed[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(PipelineSnapshot::decode(&malformed, 16).is_err());
        let mut malformed = encoded.bytes().to_vec();
        malformed[at + 4] ^= 1;
        assert!(PipelineSnapshot::decode(&malformed, 16).is_err());
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn v27_requires_full_output_semantics_while_v3_bytes_and_prefix_contract_stay_unchanged() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for stateful in [false, true] {
        let physical = plan(stateful, true);
        let saved = CheckpointPlan::from_physical(&physical).unwrap();
        let changed = CheckpointPlan::from_physical(&plan(stateful, false)).unwrap();
        saved.check_compatible(&changed).unwrap();
        let before = saved.encode().unwrap();
        let target = identity();
        let encoded = encode(1, &physical, &target, &owner);
        let decoded = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        assert_eq!(decoded.plan.encode().unwrap(), before);
        assert!(decoded
            .check_compatible_with_sink(&changed, &target)
            .is_err());
        let legacy = PipelineSnapshot::encode_frozen(
            1,
            &source(),
            0,
            1,
            &saved,
            acks(&physical, &owner),
            &owner,
            16,
        )
        .unwrap();
        let old = PipelineSnapshot::decode(legacy.bytes(), 16).unwrap();
        assert!(old.sink_identity.is_none());
        old.check_compatible(&changed).unwrap();
        assert!(old.check_compatible_with_sink(&saved, &target).is_err());
        if !stateful {
            // Independent original v3 envelope assembly, including unchanged
            // CPL1 bytes/RCP2 boundary. v27 must not rewrite this format.
            let mut expected = b"SPV1".to_vec();
            expected.extend_from_slice(&3u16.to_le_bytes());
            expected.extend_from_slice(&1u64.to_le_bytes());
            expected.extend_from_slice(&0u64.to_le_bytes());
            crate::checkpoint::encode_position(&source(), &mut expected).unwrap();
            expected.extend_from_slice(&1u64.to_le_bytes());
            expected.extend_from_slice(&1u64.to_le_bytes());
            expected.extend_from_slice(&[7; 16]);
            expected.extend_from_slice(&(before.len() as u32).to_le_bytes());
            expected.extend_from_slice(&before);
            expected.extend_from_slice(&0u16.to_le_bytes());
            assert_eq!(legacy.bytes(), expected);
        }
    }
}

#[test]
fn v27_rejects_every_target_mutation_including_sub_millisecond_recreation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let physical = plan(false, true);
    let manifest = CheckpointPlan::from_physical(&physical).unwrap();
    let target = identity();
    let snapshot =
        PipelineSnapshot::decode(encode(1, &physical, &target, &owner).bytes(), 16).unwrap();
    let mutations: [fn(&mut SinkIdentity); 6] = [
        |sink| sink.endpoints[0] = "tls://elsewhere:4222".into(),
        |sink| sink.token_secret = Some("env:OTHER_TOKEN".into()),
        |sink| sink.stream = "OTHER".into(),
        |sink| sink.created_nanos += 1,
        |sink| sink.subject = "out.other".into(),
        |sink| sink.msg_id_column = Some("v".into()),
    ];
    for mutate in mutations {
        let mut changed = target.clone();
        mutate(&mut changed);
        changed.validate().unwrap();
        assert_eq!(
            snapshot
                .check_compatible_with_sink(&manifest, &changed)
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedRestore
        );
    }
}

#[test]
fn v27_refuses_memory_other_sources_output_cursors_and_unfunded_metadata() {
    let physical = plan(false, true);
    let manifest = CheckpointPlan::from_physical(&physical).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for kind in [
        "memory",
        "jetstream-v1",
        "file-dag-v1",
        crate::processing_cut::FILE_KIND,
    ] {
        let mut position = source();
        position.identity.kind = kind.into();
        assert!(PipelineSnapshot::encode_frozen_with_sink(
            1,
            &position,
            0,
            1,
            &manifest,
            &identity(),
            acks(&physical, &owner),
            &owner,
            16,
        )
        .is_err());
    }
    let mut output = acks(&physical, &owner);
    output.next_output = Some(sparrow_model::OutputSequence::new([7; 16], 1).unwrap());
    assert!(PipelineSnapshot::encode_frozen_with_sink(
        1,
        &source(),
        0,
        1,
        &manifest,
        &identity(),
        output,
        &owner,
        16,
    )
    .is_err());
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = 8192;
    let tiny = MemoryOwner::new(budget);
    let mut target = identity();
    target.subject = "s".repeat(4096);
    let error = PipelineSnapshot::encode_frozen_with_sink(
        1,
        &source(),
        0,
        1,
        &manifest,
        &target,
        acks(&physical, &tiny),
        &tiny,
        16,
    )
    .err()
    .unwrap();
    assert_eq!(error.code, ErrorCode::ResourceExhausted);
    assert_eq!(tiny.usage().physical_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn v27_store_rejects_all_old_history_and_old_writers_reject_v27() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = OwnedSinkIdentity::new(identity(), &owner).unwrap();
    let physical = plan(false, true);
    let manifest = CheckpointPlan::from_physical(&physical).unwrap();
    for version in 1u16..=26 {
        for name in ["0000.bin", "0000.bin.part"] {
            let dir = Directory::new();
            let generation = dir.0.join("chk-00000001");
            std::fs::create_dir(&generation).unwrap();
            let mut prefix = b"SPV1".to_vec();
            prefix.extend_from_slice(&version.to_le_bytes());
            std::fs::write(generation.join(name), prefix).unwrap();
            assert_eq!(
                CheckpointStore::open_file_jetstream_sink_exclusive(
                    &dir.0,
                    16,
                    Default::default(),
                    &manifest,
                    target.clone(),
                )
                .err()
                .unwrap()
                .code,
                ErrorCode::UnsupportedRestore
            );
            assert!(!dir.0.join("CURRENT").exists());
            assert!(!dir.0.join("STATE_GENERATION").exists());
        }
    }
    let dir = Directory::new();
    let mut store = CheckpointStore::open_file_jetstream_sink_exclusive(
        &dir.0,
        16,
        Default::default(),
        &manifest,
        target.clone(),
    )
    .unwrap();
    store
        .commit_prepared(&encode(1, &physical, target.identity(), &owner))
        .unwrap();
    drop(store);
    let before = std::fs::read(dir.0.join("CURRENT")).unwrap();
    for version in 3u16..=26 {
        let result = match version {
            3 => CheckpointStore::open_pipeline_exclusive(&dir.0, 16, Default::default()),
            4 => CheckpointStore::open_reliable_exclusive(&dir.0, 16, Default::default()),
            _ => continue,
        };
        assert_eq!(result.err().unwrap().code, ErrorCode::UnsupportedRestore);
    }
    assert_eq!(std::fs::read(dir.0.join("CURRENT")).unwrap(), before);
    let readonly = CheckpointStore::open_readonly(&dir.0).unwrap();
    assert!(readonly
        .recover_pipeline_required()
        .unwrap()
        .check_compatible(&manifest)
        .is_err());
}

#[test]
fn v27_store_fixed_binding_refuses_prepared_and_raw_foreign_targets_and_owners() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let target = OwnedSinkIdentity::new(identity(), &owner).unwrap();
    let physical = plan(false, true);
    let manifest = CheckpointPlan::from_physical(&physical).unwrap();
    let dir = Directory::new();
    let mut store = CheckpointStore::open_file_jetstream_sink_exclusive(
        &dir.0,
        16,
        Default::default(),
        &manifest,
        target.clone(),
    )
    .unwrap();
    store.activate_state_generation([7; 16]).unwrap();
    store
        .commit_prepared(&encode(1, &physical, target.identity(), &owner))
        .unwrap();
    let current = std::fs::read(dir.0.join("CURRENT")).unwrap();
    let marker = std::fs::read(dir.0.join("STATE_GENERATION")).unwrap();
    let mut foreign = identity();
    foreign.created_nanos += 1;
    let encoded = encode(2, &physical, &foreign, &owner);
    assert_eq!(
        store.commit_prepared(&encoded).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    assert_eq!(
        store.commit_encoded(2, encoded.bytes()).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    let other = MemoryOwner::new(ResourceBudget::compact());
    assert_eq!(
        store
            .commit_prepared(&encode(2, &physical, target.identity(), &other))
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedRestore
    );
    assert_eq!(std::fs::read(dir.0.join("CURRENT")).unwrap(), current);
    assert_eq!(
        std::fs::read(dir.0.join("STATE_GENERATION")).unwrap(),
        marker
    );
    assert!(!dir.0.join("chk-00000002").exists());
    assert_eq!(store.recover_pipeline_required().unwrap().checkpoint_id, 1);
}

#[test]
fn v27_valid_target_mismatch_is_never_corruption_fallback() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let physical = plan(false, true);
    let manifest = CheckpointPlan::from_physical(&physical).unwrap();
    let target = OwnedSinkIdentity::new(identity(), &owner).unwrap();
    let mut foreign = identity();
    foreign.stream = "OTHER".into();
    let dir = Directory::new();
    // Simulate unsafe foreign/mixed history with the generic inspection API.
    // New fixed-target writers cannot create this directory themselves.
    let mut loose =
        CheckpointStore::open_exclusive(&dir.0, 16, CheckpointRetention::default()).unwrap();
    loose
        .commit_prepared(&encode(1, &physical, target.identity(), &owner))
        .unwrap();
    loose
        .commit_prepared(&encode(2, &physical, &foreign, &owner))
        .unwrap();
    drop(loose);
    let before = std::fs::read(dir.0.join("CURRENT")).unwrap();
    assert_eq!(
        CheckpointStore::open_file_jetstream_sink_exclusive(
            &dir.0,
            16,
            Default::default(),
            &manifest,
            target.clone(),
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::UnsupportedRestore
    );
    assert_eq!(std::fs::read(dir.0.join("CURRENT")).unwrap(), before);
    std::fs::write(dir.0.join("CURRENT"), b"corrupt").unwrap();
    assert_eq!(
        CheckpointStore::open_file_jetstream_sink_exclusive(
            &dir.0,
            16,
            Default::default(),
            &manifest,
            target,
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::UnsupportedRestore
    );
}

#[test]
fn kernel_rejects_v27_memory_paths_missing_admission_cursor_and_saved_semantic_changes() {
    for case in 0..5 {
        let kernel = kernel();
        let physical = plan(false, case != 4);
        let saved = Arc::new(CheckpointPlan::from_physical(&plan(false, true)).unwrap());
        let admission = kernel.prepare_source_admission(physical.pipeline).unwrap();
        let owner = admission.owner();
        let live = OwnedSinkIdentity::new(identity(), &owner).unwrap();
        let (events, incoming) = sparrow_io::observed::channel(4);
        let (outgoing, received) = sparrow_io::observed::channel(4);
        let capture = SharedCapture::new();
        let mut request = JobRequest::new(physical, vec![], capture.clone())
            .with_source_admission(admission)
            .with_live_events(incoming)
            .with_live_out(outgoing)
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    plan: saved,
                    generation: [7; 16],
                    restore: Some(vec![]),
                    iot: vec![],
                    sink: Some(SinkRestoreBinding::restored(live.clone(), live)),
                }),
                acks: if case == 3 {
                    AlignedAcks::default()
                        .with_output_sequence(
                            sparrow_model::OutputSequence::new([7; 16], 1).unwrap(),
                        )
                        .unwrap()
                } else {
                    AlignedAcks::default()
                },
                outbox: Arc::new(InflightCounter::new()),
            });
        match case {
            0 => request.rows.push(Row {
                values: vec![Scalar::Int64(1)],
            }),
            1 => {
                request.live_events.take();
            }
            2 => {
                request.source_admission.take();
            }
            _ => {}
        }
        assert_eq!(
            kernel.submit(request).err().unwrap().code,
            ErrorCode::UnsupportedRestore
        );
        assert_eq!(capture.row_count(), 0);
        assert_eq!(kernel.live_tasks(), 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        drop(events);
        drop(received);
    }
}

#[test]
fn runtime_aligned_independently_rejects_saved_plan_target_and_foreign_owner() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let other = MemoryOwner::new(ResourceBudget::compact());
    let saved = Arc::new(CheckpointPlan::from_physical(&plan(false, true)).unwrap());
    let live = OwnedSinkIdentity::new(identity(), &owner).unwrap();
    for case in 0..3 {
        let mut changed_target = identity();
        changed_target.created_nanos += 1;
        let checkpoint = if case == 1 {
            OwnedSinkIdentity::new(changed_target, &owner).unwrap()
        } else {
            live.clone()
        };
        let job = AlignedJob {
            restore: None,
            pipeline: Some(PipelineRestore {
                plan: saved.clone(),
                generation: [7; 16],
                restore: Some(vec![]),
                iot: vec![],
                sink: Some(SinkRestoreBinding::restored(live.clone(), checkpoint)),
            }),
            acks: AlignedAcks::default(),
            outbox: Arc::new(InflightCounter::new()),
        };
        let result = RuntimeAligned::prepare(
            job,
            &plan(false, case != 0),
            if case == 2 { &other } else { &owner },
            1,
            16,
            16,
            None,
        );
        assert_eq!(result.err().unwrap().code, ErrorCode::UnsupportedRestore);
    }
}

#[test]
fn admitted_v27_kernel_barrier_and_owner_lifetime_use_existing_puback_receipts() {
    let kernel = kernel();
    kernel.block_on(async {
        let physical = plan(false, true);
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let admission = kernel.prepare_source_admission(physical.pipeline).unwrap();
        let owner = admission.owner();
        let target = OwnedSinkIdentity::new(identity(), &owner).unwrap();
        let baseline = owner.usage().physical_bytes;
        let (events, incoming) = sparrow_io::observed::channel(4);
        let (outgoing, mut received) = sparrow_io::observed::channel(4);
        let counter = Arc::new(InflightCounter::new());
        let acks = AlignedAcks::default();
        let job = kernel
            .submit(
                JobRequest::new(physical, vec![], SharedCapture::disabled())
                    .with_source_admission(admission)
                    .with_live_events(incoming)
                    .with_live_out(outgoing)
                    .with_aligned(AlignedJob {
                        restore: None,
                        pipeline: Some(PipelineRestore {
                            plan: manifest.clone(),
                            generation: [7; 16],
                            restore: None,
                            iot: vec![],
                            sink: Some(SinkRestoreBinding::fresh(target.clone())),
                        }),
                        acks: acks.clone(),
                        outbox: counter.clone(),
                    }),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&owner, &job.memory_owner()));
        let receipt = tokio::spawn(async move {
            let mut count = 0;
            while let Some(batch) = received.recv().await {
                assert!(batch.output_sequence().is_none());
                count += batch.num_rows();
                counter.ack(); // Mock only the already-confirmed PubAck boundary.
            }
            count
        });
        let barrier = acks.begin(1).unwrap();
        events
            .send(IngressEvent::Row(Row {
                values: vec![Scalar::Int64(9)],
            }))
            .await
            .unwrap();
        events
            .send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: 1,
            }))
            .await
            .unwrap();
        let aligned = barrier
            .wait_participants(Duration::from_secs(2))
            .await
            .unwrap();
        let payload = PipelineSnapshot::encode_frozen_with_sink(
            1,
            &source(),
            1,
            1,
            &manifest,
            target.identity(),
            aligned,
            &owner,
            16,
        )
        .unwrap();
        PipelineSnapshot::decode(payload.bytes(), 16)
            .unwrap()
            .check_compatible_with_sink(&manifest, target.identity())
            .unwrap();
        drop(payload);
        events
            .send(IngressEvent::Control(StreamControl::EndOfInput))
            .await
            .unwrap();
        drop(events);
        job.wait().await.unwrap();
        assert_eq!(receipt.await.unwrap(), 1);
        assert_eq!(kernel.live_tasks(), 0);
        assert_eq!(owner.usage().physical_bytes, baseline);
        drop(target);
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(owner.accounting_errors_total(), 0);
    });
}

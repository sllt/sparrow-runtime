//! B2-A static immutable Lookup aligned-profile coverage.
//!
//! The profile intentionally stays narrow: ordinary File identity, a linear
//! stateless plan, static (non-temporal) Lookup, and a required HTTP-shaped
//! CaptureSink.  These tests exercise the runtime manifest/codec boundary and
//! the real ordered Kernel barrier path without opening the older v3..v7
//! profiles to reference-table dependencies.

use crate::lookup::ReferenceTable;
use crate::*;
use sparrow_model::{
    DataType, Field, FieldId, InflightCounter, MemoryOwner, PipelineId, ResourceBudget, RevisionId,
    Row, Scalar, Schema, SchemaId,
};
use sparrow_plan::{CheckpointPlan, LookupSpec, PhysicalPlan, PhysicalStage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "value", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn table_schema() -> Schema {
    Schema::new(
        SchemaId::new(90),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "threshold", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn output_schema() -> Schema {
    sparrow_plan::lookup_output_schema(&input_schema(), &table_schema(), &["threshold".into()])
        .unwrap()
}

fn lookup_plan() -> PhysicalPlan {
    let input = input_schema();
    let output = output_schema();
    PhysicalPlan {
        pipeline: PipelineId::new(702),
        revision: RevisionId::new(1),
        stages: vec![
            PhysicalStage::MemorySource {
                operator: 1.into(),
                name: "sensors".into(),
                schema: input.clone(),
            },
            PhysicalStage::Lookup {
                operator: 2.into(),
                spec: LookupSpec::static_table(
                    "limits",
                    vec!["device_id".into()],
                    vec!["device_id".into()],
                    vec!["threshold".into()],
                ),
                input,
                output: output.clone(),
            },
            PhysicalStage::CaptureSink {
                operator: 3.into(),
                name: "http-output".into(),
                schema: output,
            },
        ],
        edges: None,
        side_outputs: Vec::new(),
        source_times: Vec::new(),
    }
}

fn owner() -> Arc<MemoryOwner> {
    MemoryOwner::new(ResourceBudget::compact())
}

fn table(owner: &Arc<MemoryOwner>, digest: [u8; 32]) -> Arc<ReferenceTable> {
    ReferenceTable::snapshot_owned_verified(
        "limits",
        1,
        table_schema(),
        vec!["device_id".into()],
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(10)],
        }],
        16,
        owner.budget().retention_bytes,
        digest,
        owner,
    )
    .unwrap()
}

fn manifest(plan: &PhysicalPlan, table: &ReferenceTable) -> CheckpointPlan {
    CheckpointPlan::from_physical_with_references(plan, vec![table.verified_dependency().unwrap()])
        .unwrap()
}

fn request_tables(table: Arc<ReferenceTable>) -> HashMap<String, Arc<ReferenceTable>> {
    HashMap::from([("limits".into(), table)])
}

fn kernel() -> Kernel {
    Kernel::new_with_job_budget(
        KernelOptions {
            budget: ResourceBudget::performance(),
            mailbox: MailboxConfig {
                max_items: 4,
                max_bytes: 16 * 1024,
            },
            rows_per_batch: 2,
            worker_threads: 2,
        },
        ResourceBudget::compact(),
    )
    .unwrap()
}

fn aligned_job(plan: Arc<CheckpointPlan>, restored: bool, acks: AlignedAcks) -> AlignedJob {
    AlignedJob {
        restore: None,
        pipeline: Some(PipelineRestore {
            sink: None,
            plan,
            generation: [0x42; 16],
            restore: restored.then_some(Vec::new()),
            iot: Vec::new(),
        }),
        acks,
        outbox: Arc::new(InflightCounter::new()),
    }
}

fn row(value: i64) -> Row {
    Row {
        values: vec![Scalar::utf8("a"), Scalar::Int64(value)],
    }
}

#[test]
fn core_b2_verified_dependency_requires_digest_and_crc() {
    let table_owner = owner();
    let verified = table(&table_owner, [1; 32]);
    let dependency = verified.verified_dependency().unwrap();
    assert_eq!(dependency.name, "limits");
    assert_eq!(dependency.revision, 1);
    assert_eq!(dependency.canonical_sha256, [1; 32]);
    assert_eq!(dependency.runtime_crc32, verified.checksum());

    let legacy = ReferenceTable::snapshot(
        "limits",
        1,
        table_schema(),
        vec!["device_id".into()],
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(10)],
        }],
        16,
        1024 * 1024,
    )
    .unwrap();
    let error = legacy.verified_dependency().unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore);

    let corrupted = Arc::new(verified.with_corrupted_first_row());
    let error = corrupted.verified_dependency().unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::CodecViolation);
}

#[test]
fn core_b2_verified_snapshot_failure_refunds_owner() {
    let owner = owner();
    let error = ReferenceTable::snapshot_owned_verified(
        "limits",
        1,
        table_schema(),
        vec!["device_id".into()],
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(10)],
        }],
        16,
        64,
        [1; 32],
        &owner,
    )
    .unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::BoundExceeded);
    assert_eq!(owner.usage().retention_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);

    let table = table(&owner, [1; 32]);
    assert!(owner.usage().retention_bytes > 0);
    drop(table);
    assert_eq!(owner.usage().retention_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn core_b2_cpl3_reference_manifest_roundtrip_is_deterministic() {
    let owner = owner();
    let table = table(&owner, [1; 32]);
    let plan = lookup_plan();
    let manifest = manifest(&plan, &table);
    assert!(manifest.has_references());
    assert!(manifest.states.is_empty());

    let encoded = manifest.encode().unwrap();
    assert_eq!(&encoded[..4], b"CPL3");
    let decoded = CheckpointPlan::decode(&encoded).unwrap();
    assert_eq!(decoded, manifest);

    let mut old_magic = encoded.clone();
    old_magic[..4].copy_from_slice(b"CPL1");
    assert!(CheckpointPlan::decode(&old_magic).is_err());
}

#[test]
fn core_b2_kernel_rejects_missing_extra_hash_crc_and_schema_bindings() {
    let plan = lookup_plan();
    let table_owner = owner();
    let good = table(&table_owner, [1; 32]);
    let checkpoint = Arc::new(manifest(&plan, &good));

    let submit = |tables: HashMap<String, Arc<ReferenceTable>>| {
        let kernel = kernel();
        kernel.submit(
            JobRequest::new(plan.clone(), Vec::new(), SharedCapture::disabled())
                .with_tables(tables)
                .with_aligned(aligned_job(
                    Arc::clone(&checkpoint),
                    true,
                    AlignedAcks::default(),
                )),
        )
    };

    assert!(submit(HashMap::new()).is_err());
    assert!(submit(HashMap::from([(
        "other".into(),
        table(&table_owner, [2; 32])
    )]))
    .is_err());
    assert!(submit(request_tables(table(&table_owner, [2; 32]))).is_err());

    let crc_corrupt = Arc::new(good.with_stored_checksum(good.checksum() ^ 1));
    assert!(submit(request_tables(crc_corrupt)).is_err());

    let mut wrong_output = plan.clone();
    if let PhysicalStage::Lookup { output, .. } = &mut wrong_output.stages[1] {
        *output = input_schema();
    }
    assert!(kernel()
        .submit(
            JobRequest::new(wrong_output, Vec::new(), SharedCapture::disabled())
                .with_tables(request_tables(Arc::clone(&good)))
                .with_aligned(aligned_job(
                    Arc::clone(&checkpoint),
                    true,
                    AlignedAcks::default(),
                )),
        )
        .is_err());
}

#[test]
fn core_b2_kernel_rejects_versioned_and_legacy_unverified_tables() {
    let plan = lookup_plan();
    let table_owner = owner();
    let verified = table(&table_owner, [1; 32]);
    let checkpoint = Arc::new(manifest(&plan, &verified));

    let versioned = VersionedReferenceTable::from_static(Arc::clone(&verified), 4).unwrap();
    let error = match kernel().submit(
        JobRequest::new(plan.clone(), Vec::new(), SharedCapture::disabled())
            .with_tables(request_tables(Arc::clone(&verified)))
            .with_versioned_tables(HashMap::from([("limits".into(), versioned)]))
            .with_aligned(aligned_job(
                Arc::clone(&checkpoint),
                true,
                AlignedAcks::default(),
            )),
    ) {
        Err(error) => error,
        Ok(handle) => {
            handle.cancel();
            panic!("versioned table was admitted into aligned reference profile")
        }
    };
    assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore);

    let legacy = ReferenceTable::snapshot(
        "limits",
        1,
        table_schema(),
        vec!["device_id".into()],
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(10)],
        }],
        16,
        1024 * 1024,
    )
    .unwrap();
    assert!(kernel()
        .submit(
            JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
                .with_tables(request_tables(legacy))
                .with_aligned(aligned_job(checkpoint, true, AlignedAcks::default(),)),
        )
        .is_err());
}

#[test]
fn core_b2_aligned_rejects_foreign_table_owner_before_activation() {
    let kernel = kernel();
    let plan = lookup_plan();
    let foreign_owner = owner();
    let table = table(&foreign_owner, [1; 32]);
    let checkpoint = Arc::new(manifest(&plan, &table));
    let admission = kernel.prepare_source_admission(plan.pipeline).unwrap();
    let job_owner = admission.owner();
    let error = match kernel.submit(
        JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
            .with_tables(request_tables(table.clone()))
            .with_source_admission(admission)
            .with_aligned(aligned_job(checkpoint, true, AlignedAcks::default())),
    ) {
        Err(error) => error,
        Ok(handle) => {
            handle.cancel();
            panic!("foreign-owner reference table was admitted")
        }
    };
    assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(error.message.contains("Job owner"));
    assert_eq!(kernel.admitted_jobs(), 0);
    assert_eq!(job_owner.usage().physical_bytes, 0);
    drop(table);
    assert_eq!(foreign_owner.usage().retention_bytes, 0);
}

#[test]
fn core_b2_reference_snapshot_profile_is_v8_and_rejects_old_profiles() {
    let table_owner = owner();
    let table = table(&table_owner, [1; 32]);
    let physical = lookup_plan();
    let manifest = manifest(&physical, &table);
    let owner = owner();
    let acks = ParticipantAcks {
        attempt: 1,
        generation: [7; 16],
        freezes: Vec::new(),
        next_output: None,
    };
    let source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::file(
        "core-b2.ndjson",
        32,
        7,
    ));
    let encoded =
        PipelineSnapshot::encode_frozen(1, &source, 0, 1, &manifest, acks, &owner, 16).unwrap();
    assert_eq!(
        &encoded.bytes()[4..6],
        &crate::pipeline_checkpoint::REFERENCE_SNAPSHOT_VERSION.to_le_bytes()
    );
    assert!(encoded.bytes().windows(4).any(|part| part == b"CPL3"));
    let decoded = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
    assert_eq!(decoded.plan.reference_tables, manifest.reference_tables);

    let mut old = encoded.bytes().to_vec();
    old[4..6].copy_from_slice(&crate::pipeline_checkpoint::PIPELINE_SNAPSHOT_VERSION.to_le_bytes());
    assert!(PipelineSnapshot::decode(&old, 16).is_err());

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "sparrow-core-b2-profile-{}-{stamp}",
        std::process::id()
    ));
    let mut reference =
        CheckpointStore::open_reference_exclusive(&dir, 16, Default::default()).unwrap();
    reference.commit_prepared(&encoded).unwrap();
    drop(reference);
    assert!(CheckpointStore::open_pipeline_exclusive(&dir, 16, Default::default()).is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn core_b2_kernel_ordered_barrier_restore_preserves_static_lookup_and_source_cut() {
    let plan = lookup_plan();
    let kernel = kernel();
    kernel.block_on(async {
        let admission = kernel.prepare_source_admission(plan.pipeline).unwrap();
        let attached = table(&admission.owner(), [1; 32]);
        let manifest = Arc::new(manifest(&plan, &attached));
        let capture = SharedCapture::new();
        let (tx, rx) = sparrow_io::observed::channel(4);
        let acks = AlignedAcks::default();
        let handle = kernel
            .submit(
                JobRequest::new(plan.clone(), Vec::new(), capture.clone())
                    .with_tables(request_tables(Arc::clone(&attached)))
                    .with_source_admission(admission)
                    .with_live_events(rx)
                    .with_aligned(aligned_job(Arc::clone(&manifest), false, acks.clone())),
            )
            .unwrap();
        tx.send(IngressEvent::Row(row(1))).await.unwrap();
        let checkpoint = acks.begin(1).unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 1,
        }))
        .await
        .unwrap();
        let frozen = checkpoint
            .wait_participants(Duration::from_secs(3))
            .await
            .unwrap();
        let source = sparrow_io::SourcePosition {
            offset_bytes: 17,
            record_index: 1,
            identity: sparrow_io::SourceIdentity::file("core-b2.ndjson", 32, 7),
        };
        let encoded = PipelineSnapshot::encode_frozen(
            1,
            &source,
            1,
            1,
            &manifest,
            frozen,
            &handle.memory_owner(),
            16,
        )
        .unwrap();
        let snapshot = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        assert_eq!(snapshot.source.record_index, 1);
        assert_eq!(snapshot.plan.reference_tables, manifest.reference_tables);
        assert_eq!(capture.rows().len(), 1);
        assert_eq!(capture.rows()[0].last(), Some(&Scalar::Int64(10)));

        handle.cancel();
        drop(tx);
        handle.wait().await.unwrap();

        let admission2 = kernel.prepare_source_admission(plan.pipeline).unwrap();
        let attached2 = table(&admission2.owner(), [1; 32]);
        let resumed_capture = SharedCapture::new();
        let (tx2, rx2) = sparrow_io::observed::channel(4);
        let resumed_acks = AlignedAcks::default();
        let resumed = kernel
            .submit(
                JobRequest::new(plan, Vec::new(), resumed_capture.clone())
                    .with_tables(request_tables(attached2))
                    .with_source_admission(admission2)
                    .with_live_events(rx2)
                    .with_aligned(aligned_job(Arc::new(snapshot.plan), true, resumed_acks)),
            )
            .unwrap();
        tx2.send(IngressEvent::Row(row(2))).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while resumed_capture.row_count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(resumed_capture.rows()[0].last(), Some(&Scalar::Int64(10)));
        resumed.cancel();
        drop(tx2);
        resumed.wait().await.unwrap();
    });
    assert_eq!(kernel.live_tasks(), 0);
}

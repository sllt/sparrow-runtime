use crate::processing_cut::{ProcessingCut, FILE_KIND, JETSTREAM_KIND};
use crate::{IotFreeze, IotOperator, ParticipantAcks, PipelineSnapshot};
use sparrow_model::{
    CreditKind, DataType, Field, MemoryOwner, OutputSequence, ResourceBudget, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema,
};
use sparrow_plan::{
    CheckpointPlan, InvalidValuePolicy, IotSpec, IotTimingSpec, ParticipantId,
    ProcessingTimePolicy, StateParticipant,
};
use std::sync::Arc;

fn schema() -> Schema {
    Schema::new(
        sparrow_model::SchemaId::new(1),
        vec![
            Field::new(sparrow_model::FieldId::new(1), "id", DataType::Utf8, false),
            Field::new(sparrow_model::FieldId::new(2), "value", DataType::Bool, true),
            Field::new(sparrow_model::FieldId::new(3), "detail", DataType::Utf8, true),
        ],
    )
    .unwrap()
}
fn hold() -> IotTimingSpec {
    IotTimingSpec::HoldFor {
        duration_micros: 100,
        clock: ProcessingTimePolicy::Paused,
    }
}
fn debounce(leading: bool, trailing: bool, reset: bool) -> IotTimingSpec {
    IotTimingSpec::Debounce {
        quiet_micros: 100,
        max_wait_micros: 250,
        leading,
        trailing,
        reset_on_repeat: reset,
        clock: ProcessingTimePolicy::Paused,
    }
}
fn spec(timing: IotTimingSpec) -> IotSpec {
    IotSpec {
        keys: vec!["id".into()],
        fields: vec!["value".into()],
        emit_first: false,
        ttl_micros: 0,
        max_keys: 16,
        invalid: InvalidValuePolicy::Ignore,
        deadband: None,
        hysteresis: None,
        timing: Some(timing),
    }
}
fn operator(owner: &Arc<MemoryOwner>, timing: IotTimingSpec) -> IotOperator {
    IotOperator::new(2.into(), spec(timing), schema(), owner.clone()).unwrap()
}
fn batch(owner: &Arc<MemoryOwner>, id: &str, value: Scalar, detail: Option<&str>) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        1,
        owner.budget().reservation_bytes,
    )
    .unwrap();
    builder
        .push(Row {
            values: vec![
                Scalar::utf8(id),
                value,
                detail.map(Scalar::utf8).unwrap_or(Scalar::Null),
            ],
        })
        .unwrap();
    builder.finish().unwrap()
}
fn send(op: &mut IotOperator, owner: &Arc<MemoryOwner>, now: i64, value: bool) -> Option<RowBatch> {
    op.on_batch(&batch(owner, "a", Scalar::Bool(value), None), now)
        .unwrap()
}
fn advance(op: &mut IotOperator, now: i64) -> Vec<Row> {
    op.set_processing_time(now).unwrap();
    let mut rows = Vec::new();
    while op.next_deadline().is_some_and(|at| at <= now) {
        if let Some(batch) = op.take_timed_due(now).unwrap() {
            rows.extend(batch.rows().iter().cloned());
        }
    }
    rows
}
#[test]
fn paused_time_hold_idle_repeat_false_and_equal_boundary() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, hold());
    assert!(send(&mut op, &owner, 10, true).is_none());
    assert!(send(&mut op, &owner, 60, true).is_none());
    assert_eq!(op.next_deadline(), Some(110));
    assert!(advance(&mut op, 109).is_empty());
    assert_eq!(advance(&mut op, 110).len(), 1);
    assert!(send(&mut op, &owner, 110, false).is_none());
    assert_eq!(op.key_count(), 0);
    send(&mut op, &owner, 111, true);
    send(&mut op, &owner, 200, false);
    assert!(advance(&mut op, 300).is_empty());
    assert_eq!(op.pending_timers(), 0);
    send(&mut op, &owner, 301, true);
    assert_eq!(advance(&mut op, 401).len(), 1);
    send(&mut op, &owner, 402, true);
    assert!(advance(&mut op, 1000).is_empty());
    assert_eq!(op.key_count(), 1);
    assert_eq!(op.pending_timers(), 0);
}
#[test]
fn paused_time_hold_ignored_null_does_not_cancel_or_refresh() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, hold());
    send(&mut op, &owner, 0, true);
    op.on_batch(&batch(&owner, "a", Scalar::Null, None), 50)
        .unwrap();
    assert_eq!(op.next_deadline(), Some(100));
    assert_eq!(advance(&mut op, 100).len(), 1);
    assert_eq!(op.stats().invalid_rows, 1);
}
#[test]
fn paused_time_debounce_modes_and_max_wait_end_burst() {
    for (leading, trailing) in [(false, true), (true, false), (true, true)] {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut op = operator(&owner, debounce(leading, trailing, true));
        assert_eq!(send(&mut op, &owner, 0, true).is_some(), leading);
        assert!(send(&mut op, &owner, 90, true).is_none());
        assert_eq!(op.next_deadline(), Some(190));
        send(&mut op, &owner, 180, false);
        assert_eq!(op.next_deadline(), Some(250));
        send(&mut op, &owner, 240, true);
        assert_eq!(op.next_deadline(), Some(250));
        assert_eq!(advance(&mut op, 250).len(), usize::from(trailing));
        assert_eq!(op.key_count(), 0);
        assert_eq!(send(&mut op, &owner, 250, true).is_some(), leading);
        assert_eq!(
            advance(&mut op, 350).len(),
            usize::from(trailing && !leading)
        );
    }
}
#[test]
fn paused_time_debounce_repeat_policy_retains_latest_full_row() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, debounce(false, true, false));
    send(&mut op, &owner, 0, true);
    op.on_batch(&batch(&owner, "a", Scalar::Bool(true), Some("latest")), 90)
        .unwrap();
    assert_eq!(op.next_deadline(), Some(100));
    let rows = advance(&mut op, 100);
    assert_eq!(rows[0].values[2], Scalar::utf8("latest"));
}
#[test]
fn paused_time_nullable_freeze_scanned_cut_and_atomic_restore() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, hold());
    send(&mut op, &owner, 10, true);
    let freeze = op.freeze().unwrap();
    let bytes = freeze.encode().unwrap();
    let mut encoded = Vec::new();
    op.encode_freeze_into(&mut encoded, 16).unwrap();
    assert_eq!(encoded, bytes);
    assert_eq!(IotFreeze::decode(&bytes, 16).unwrap(), freeze);
    assert!(IotFreeze::decode_at_cut(&mut bytes.as_slice(), 16, false, Some(100)).is_ok());
    for now in [0, 110, 111] {
        assert!(IotFreeze::decode_at_cut(&mut bytes.as_slice(), 16, false, Some(now)).is_err());
    }
    let mut restored = operator(&owner, hold());
    restored.restore(&freeze).unwrap();
    restored.validate_processing_cut(100).unwrap();
    let mut corrupt = freeze.clone();
    corrupt.entries[0].values[1] = Scalar::Int64(109);
    assert!(restored.restore(&corrupt).is_err());
    assert_eq!(restored.freeze().unwrap(), freeze);
    assert_eq!(advance(&mut restored, 110), advance(&mut op, 110));
    for length in 0..bytes.len() {
        assert!(
            IotFreeze::decode(&bytes[..length], 16).is_err(),
            "length {length}"
        );
    }
    let mut debounce = operator(&owner, debounce(false, true, true));
    send(&mut debounce, &owner, 0, true);
    let saved = debounce.freeze().unwrap();
    let mut early = saved.clone();
    early.entries[0].values[1] = Scalar::Int64(99);
    assert!(debounce.restore(&early).is_err());
    let mut silent = saved.clone();
    silent.entries[0].values[2] = Scalar::Bool(false);
    assert!(debounce.restore(&silent).is_err());
    assert_eq!(debounce.freeze().unwrap(), saved);
}
#[test]
fn paused_time_overflow_budget_failure_and_drop_release() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let baseline = owner.usage();
    {
        let mut op = operator(&owner, hold());
        assert!(op
            .on_batch(&batch(&owner, "a", Scalar::Bool(true), None), i64::MAX - 50)
            .is_err());
        assert_eq!(op.key_count(), 0);
        assert_eq!(op.pending_timers(), 0);
    }
    let after = owner.usage();
    assert_eq!(
        (
            after.reservation_bytes,
            after.retention_bytes,
            after.queue_bytes,
            after.physical_bytes,
            after.live_handles
        ),
        (
            baseline.reservation_bytes,
            baseline.retention_bytes,
            baseline.queue_bytes,
            baseline.physical_bytes,
            baseline.live_handles
        )
    );
    let mut budget = ResourceBudget::compact();
    budget.max_timers = 1;
    let owner = MemoryOwner::new(budget);
    let mut op = operator(&owner, hold());
    send(&mut op, &owner, 0, true);
    let before = op.freeze().unwrap();
    assert!(op
        .on_batch(&batch(&owner, "b", Scalar::Bool(true), None), 1)
        .is_err());
    assert_eq!(op.freeze().unwrap(), before);
    assert!(op.set_processing_time(0).is_err());
    op.cleanup();
    assert_eq!(op.retention_bytes(), 0);
}
fn cut(kind: &str) -> ProcessingCut {
    ProcessingCut {
        sequence: 1,
        micros: 50,
        source: sparrow_io::SourcePosition {
            offset_bytes: 12,
            record_index: 1,
            identity: sparrow_io::SourceIdentity {
                kind: kind.into(),
                path: "fixture".into(),
                size: 0,
                fingerprint: 0,
            },
        },
    }
}
#[test]
fn paused_time_source_cut_roundtrip_bounds_and_mirror_guards() {
    for kind in ["file", "jetstream-v1"] {
        let cut = cut(kind);
        let wrapped = cut.wrap().unwrap();
        assert_eq!(ProcessingCut::unwrap(&wrapped).unwrap(), cut);
        assert_eq!(
            wrapped.identity.kind,
            if kind == "file" {
                FILE_KIND
            } else {
                JETSTREAM_KIND
            }
        );
        for n in 0..wrapped.identity.path.len() {
            let mut truncated = wrapped.clone();
            truncated.identity.path.truncate(n);
            assert!(ProcessingCut::unwrap(&truncated).is_err());
        }
        let mut changed = wrapped;
        changed.offset_bytes += 1;
        assert!(ProcessingCut::unwrap(&changed).is_err());
    }
    let mut invalid = cut("file");
    invalid.sequence = 0;
    assert!(invalid.wrap().is_err());
    invalid.sequence = 1;
    invalid.source.identity.path = "x".repeat(31 * 1024);
    assert!(invalid.wrap().is_err());
}
fn manifest() -> CheckpointPlan {
    CheckpointPlan {
        source: 1.into(),
        sink: 3.into(),
        states: vec![StateParticipant {
            id: ParticipantId::iot(2.into()),
            codec: sparrow_plan::checkpoint::IOT_STATE_CODEC,
            window_kind: 7,
        }],
        reference_tables: vec![],
        semantics: b"CP01paused-test".to_vec(),
        recovery_prefix_len: None,
    }
}
#[test]
fn paused_time_snapshot_profiles_and_downgrade_rejection() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, hold());
    send(&mut op, &owner, 0, true);
    let manifest = manifest();
    for (kind, version) in [("file", 14u16), ("jetstream-v1", 15)] {
        let source = cut(kind).wrap().unwrap();
        let acks = ParticipantAcks {
            attempt: 1,
            generation: [1; 16],
            next_output: Some(OutputSequence::new([1; 16], 1).unwrap()),
            freezes: vec![crate::barrier::EncodedFreeze::from_iot(&op, &owner, 16).unwrap()],
        };
        let encoded =
            PipelineSnapshot::encode_frozen(1, &source, 1, 1, &manifest, acks, &owner, 16).unwrap();
        assert_eq!(&encoded.bytes()[4..6], &version.to_le_bytes());
        let restored = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        assert_eq!(restored.source, source);
        for old in 3u16..=13 {
            let mut bytes = encoded.bytes().to_vec();
            bytes[4..6].copy_from_slice(&old.to_le_bytes());
            assert!(PipelineSnapshot::decode(&bytes, 16).is_err());
        }
    }
    for kind in ["file", "jetstream-v1", "file-dag-v1"] {
        assert!(crate::snapshot_version_for(&manifest, kind).is_err());
    }
    let mut mixed = manifest.clone();
    mixed.states.push(StateParticipant {
        id: ParticipantId::iot(4.into()),
        codec: 2,
        window_kind: 4,
    });
    assert!(mixed.validate().is_err());
}

use crate::{
    alarm::{EventKind, Phase, Policy, State},
    IotFreeze, IotOperator, ParticipantAcks, PipelineSnapshot,
};
use sparrow_model::{
    CreditKind, DataType, Field, MemoryOwner, OutputSequence, ResourceBudget, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema,
};
use sparrow_plan::{
    CheckpointPlan, InvalidValuePolicy, IotSpec, IotTimingSpec, ParticipantId,
    ProcessingTimePolicy, StateParticipant,
};
use std::sync::Arc;

fn policy() -> Policy {
    Policy {
        activate_micros: 10,
        resolve_micros: 5,
        cooldown_micros: 100,
        notification_max_age_micros: 200,
    }
}
fn schema() -> Schema {
    Schema::new(
        sparrow_model::SchemaId::new(1),
        vec![
            Field::new(
                sparrow_model::FieldId::new(1),
                "device",
                DataType::Utf8,
                false,
            ),
            Field::new(
                sparrow_model::FieldId::new(2),
                "enter",
                DataType::Bool,
                true,
            ),
            Field::new(
                sparrow_model::FieldId::new(3),
                "clear",
                DataType::Bool,
                true,
            ),
            Field::new(
                sparrow_model::FieldId::new(4),
                "detail",
                DataType::Utf8,
                true,
            ),
        ],
    )
    .unwrap()
}
fn spec(p: Policy) -> IotSpec {
    IotSpec {
        keys: vec!["device".into()],
        fields: vec!["enter".into(), "clear".into()],
        emit_first: false,
        ttl_micros: 0,
        max_keys: 16,
        invalid: InvalidValuePolicy::Ignore,
        deadband: None,
        hysteresis: None,
        timing: Some(IotTimingSpec::Alarm {
            activate_micros: p.activate_micros,
            resolve_micros: p.resolve_micros,
            cooldown_micros: p.cooldown_micros,
            notification_max_age_micros: p.notification_max_age_micros,
            clock: ProcessingTimePolicy::Paused,
        }),
    }
}
fn op(owner: &Arc<MemoryOwner>, p: Policy) -> IotOperator {
    let mut op = IotOperator::new(2.into(), spec(p), schema(), owner.clone()).unwrap();
    op.bind_generation([7; 16]).unwrap();
    op
}
fn batch(
    owner: &Arc<MemoryOwner>,
    key: &str,
    enter: Scalar,
    clear: Scalar,
    detail: Option<&str>,
) -> RowBatch {
    let mut b = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        1,
        owner.budget().reservation_bytes,
    )
    .unwrap();
    b.push(Row {
        values: vec![
            Scalar::utf8(key),
            enter,
            clear,
            detail.map(Scalar::utf8).unwrap_or(Scalar::Null),
        ],
    })
    .unwrap();
    b.finish().unwrap()
}
fn send(
    op: &mut IotOperator,
    owner: &Arc<MemoryOwner>,
    now: i64,
    enter: bool,
    clear: bool,
) -> Vec<Row> {
    op.on_batch(
        &batch(owner, "a", Scalar::Bool(enter), Scalar::Bool(clear), None),
        now,
    )
    .unwrap()
    .map(|b| b.rows().to_vec())
    .unwrap_or_default()
}
fn advance(op: &mut IotOperator, now: i64) -> Vec<Row> {
    op.set_processing_time(now).unwrap();
    let mut rows = Vec::new();
    while op.next_deadline().is_some_and(|at| at <= now) {
        if let Some(b) = op.take_timed_due(now).unwrap() {
            rows.extend_from_slice(b.rows());
        }
    }
    rows
}
fn event(row: &Row) -> (&Scalar, &Scalar, &Scalar) {
    (&row.values[4], &row.values[8], &row.values[10])
}

#[test]
fn alarm_lifecycle_state_machine_boundaries_and_recovery_cancellation() {
    let p = policy();
    let (state, effects) = State::new(0).unwrap().observe(p, 0, true, false).unwrap();
    assert_eq!(state.phase, Phase::Pending);
    assert!(effects.event.is_none());
    let (state, _) = state.observe(p, 9, true, false).unwrap();
    assert_eq!(state.deadline, Some(10));
    assert!(
        state.observe(p, 10, false, true).is_err(),
        "equal-cut timer must precede input"
    );
    let (state, effects) = state.due(p, 10).unwrap();
    assert_eq!(effects.event.unwrap().kind, EventKind::Activate);
    assert_eq!(state.episode, 1);
    let (state, _) = state.observe(p, 10, false, true).unwrap();
    assert_eq!(state.phase, Phase::Recovering);
    let (state, _) = state.observe(p, 14, false, false).unwrap();
    assert_eq!(state.phase, Phase::Active);
    let (state, _) = state.observe(p, 15, false, true).unwrap();
    assert_eq!(state.deadline, Some(20));
    let (state, effects) = state.due(p, 20).unwrap();
    assert_eq!(state.phase, Phase::Normal);
    assert_eq!(effects.event.unwrap().episode, 1);
    state.validate(p, 20, false, true).unwrap();
    let (state, _) = state.observe(p, 21, true, false).unwrap();
    let (state, effects) = state.due(p, 31).unwrap();
    assert!(!effects.event.unwrap().notify);
    assert_eq!(state.episode, 2);
    let (state, effects) = state.due(p, 120).unwrap();
    assert_eq!(effects.event.unwrap().kind, EventKind::Notify);
    assert_eq!(state.episode, 2);
}

#[test]
fn alarm_notifications_expire_cancel_and_resolve_bypasses_cooldown() {
    for age in [20, 100, 200] {
        let p = Policy {
            activate_micros: 0,
            resolve_micros: 0,
            notification_max_age_micros: age,
            ..policy()
        };
        let (state, _) = State::new(0).unwrap().observe(p, 0, true, false).unwrap();
        let (state, effects) = state.observe(p, 1, false, true).unwrap();
        assert!(effects.event.unwrap().notify);
        let (state, effects) = state.observe(p, 2, true, false).unwrap();
        assert!(!effects.event.unwrap().notify);
        let saved = state;
        let (resolved, effects) = state.observe(p, 3, false, true).unwrap();
        assert!(effects.notification_cancelled && effects.event.unwrap().notify);
        assert_eq!(resolved.next_deadline(p).unwrap(), None);
        let (_, effects) = saved.due(p, 101).unwrap();
        assert_eq!(effects.notification_expired, age == 20);
        let (_, effects) = saved.due(p, 202).unwrap();
        assert!(
            effects.notification_expired && effects.event.is_none(),
            "long tick must not notify stale activation"
        );
    }
}

#[test]
fn alarm_invalid_overflow_and_candidate_failure_leave_state_unchanged() {
    let p = Policy {
        activate_micros: 0,
        ..policy()
    };
    let state = State::new(0).unwrap();
    assert!(state.observe(p, 0, true, true).is_err());
    let mut exhausted = state;
    exhausted.episode = u64::MAX;
    assert!(exhausted.observe(p, 1, true, false).is_err());
    assert_eq!(exhausted.episode, u64::MAX);
    assert!(state.observe(p, i64::MAX, true, false).is_err());
    assert_eq!(state, State::new(0).unwrap());
    assert!(state.observe(policy(), -1, true, false).is_err());
}

#[test]
fn alarm_operator_idle_full_row_null_ignored_and_stable_episode() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = op(&owner, policy());
    assert!(send(&mut op, &owner, 0, true, false).is_empty());
    op.on_batch(
        &batch(
            &owner,
            "a",
            Scalar::Null,
            Scalar::Bool(false),
            Some("ignored"),
        ),
        5,
    )
    .unwrap();
    op.on_batch(
        &batch(
            &owner,
            "a",
            Scalar::Bool(true),
            Scalar::Bool(false),
            Some("latest"),
        ),
        9,
    )
    .unwrap();
    let out = advance(&mut op, 10);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].values[3], Scalar::utf8("latest"));
    assert_eq!(
        event(&out[0]),
        (
            &Scalar::utf8("activate"),
            &Scalar::UInt64(1),
            &Scalar::Bool(true)
        )
    );
    assert_eq!(out[0].values[6], Scalar::utf8("07".repeat(16)));
    assert!(send(&mut op, &owner, 10, false, true).is_empty());
    let out = advance(&mut op, 15);
    assert_eq!(
        event(&out[0]),
        (
            &Scalar::utf8("resolve"),
            &Scalar::UInt64(1),
            &Scalar::Bool(true)
        )
    );
    send(&mut op, &owner, 16, true, false);
    let out = advance(&mut op, 26);
    assert_eq!(
        event(&out[0]),
        (
            &Scalar::utf8("activate"),
            &Scalar::UInt64(2),
            &Scalar::Bool(false)
        )
    );
    assert_eq!(op.stats().notifications_deferred, 1);
    let out = advance(&mut op, 115);
    assert_eq!(
        event(&out[0]),
        (
            &Scalar::utf8("notify"),
            &Scalar::UInt64(2),
            &Scalar::Bool(true)
        )
    );
    assert_eq!(op.stats().invalid_rows, 1);
    op.cleanup();
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn alarm_restore_every_cut_matches_continuous_output_and_latest_payload() {
    let p = Policy {
        activate_micros: 2,
        resolve_micros: 2,
        cooldown_micros: 5,
        notification_max_age_micros: 9,
    };
    // Independent sequence includes cancellation, repeated value, band and an
    // ignored contradictory condition. Restore after every committed decision.
    let decisions = [
        (0, Some((true, false))),
        (1, Some((true, false))),
        (2, None),
        (3, Some((false, true))),
        (4, Some((false, false))),
        (5, Some((false, true))),
        (7, Some((true, false))),
        (9, None),
        (10, Some((true, true))),
        (12, None),
        (13, Some((false, true))),
        (15, None),
        (16, Some((true, false))),
        (18, None),
        (20, None),
        (30, None),
    ];
    let run = |restore: bool| {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut op = op(&owner, p);
        let mut result = Vec::new();
        for (now, input) in decisions {
            result.extend(advance(&mut op, now));
            if let Some((enter, clear)) = input {
                result.extend(send(&mut op, &owner, now, enter, clear));
            }
            op.validate_processing_cut(now).unwrap();
            if restore {
                let freeze = op.freeze().unwrap();
                let encoded = freeze.encode().unwrap();
                let decoded = IotFreeze::decode(&encoded, 16).unwrap();
                assert_eq!(freeze, decoded);
                let mut scanned = encoded.as_slice();
                IotFreeze::decode_at_cut(&mut scanned, 16, false, Some(now)).unwrap();
                assert!(scanned.is_empty());
                drop(op);
                op = IotOperator::new(2.into(), spec(p), schema(), owner.clone()).unwrap();
                op.bind_generation([7; 16]).unwrap();
                op.restore(&decoded).unwrap();
                op.validate_processing_cut(now).unwrap();
            }
        }
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
        result
    };
    let continuous = run(false);
    assert_eq!(continuous, run(true));
    let events = continuous
        .iter()
        .map(|row| row.values[4].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        vec![
            Scalar::utf8("activate"),
            Scalar::utf8("resolve"),
            Scalar::utf8("activate"),
            Scalar::utf8("notify"),
            Scalar::utf8("resolve"),
            Scalar::utf8("activate"),
            Scalar::utf8("notify")
        ]
    );
}

#[test]
fn alarm_budget_failure_freeze_corruption_and_generation_guards() {
    let mut budget = ResourceBudget::compact();
    budget.max_timers = 2;
    let owner = MemoryOwner::new(budget);
    let mut op = op(&owner, policy());
    send(&mut op, &owner, 0, true, false);
    let before = op.freeze().unwrap();
    assert!(op
        .on_batch(
            &batch(&owner, "b", Scalar::Bool(true), Scalar::Bool(false), None),
            1
        )
        .is_err());
    assert_eq!(op.freeze().unwrap(), before);
    assert!(op.bind_generation([8; 16]).is_err());
    let bytes = before.encode().unwrap();
    for end in 0..bytes.len() {
        assert!(IotFreeze::decode(&bytes[..end], 16).is_err());
    }
    for (index, value) in [
        (0, Scalar::UInt64(99)),
        (2, Scalar::Int64(0)),
        (5, Scalar::Int64(1000)),
    ] {
        let mut bad = before.clone();
        bad.entries[0].values[index] = value;
        assert!(bad.encode().is_err());
        assert!(op.restore(&bad).is_err());
        assert_eq!(op.freeze().unwrap(), before);
    }
    let mut scan = bytes.as_slice();
    assert!(IotFreeze::decode_at_cut(&mut scan, 16, false, Some(10)).is_err());
    op.cleanup();
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut unbound = IotOperator::new(2.into(), spec(policy()), schema(), owner.clone()).unwrap();
    assert!(unbound
        .on_batch(
            &batch(&owner, "a", Scalar::Bool(true), Scalar::Bool(false), None),
            0
        )
        .is_err());
    assert!(unbound.bind_generation([0; 16]).is_err());
}

#[test]
fn alarm_new_snapshot_profiles_reject_downgrade_and_wrong_generation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = op(&owner, policy());
    send(&mut op, &owner, 0, true, false);
    let plan = CheckpointPlan {
        source: 1.into(),
        sink: 3.into(),
        states: vec![StateParticipant {
            id: ParticipantId::iot(2.into()),
            codec: 2,
            window_kind: 11,
        }],
        reference_tables: vec![],
        semantics: b"CP01alarm-test".to_vec(),
        recovery_prefix_len: None,
    };
    for (kind, version) in [("file", 20u16), ("jetstream-v1", 21u16)] {
        let cut = crate::processing_cut::ProcessingCut {
            sequence: 1,
            micros: 5,
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
        .wrap()
        .unwrap();
        let acks = |generation| ParticipantAcks {
            attempt: 1,
            generation,
            next_output: Some(OutputSequence::new([7; 16], 1).unwrap()),
            freezes: vec![crate::barrier::EncodedFreeze::from_iot(&op, &owner, 16).unwrap()],
        };
        assert!(
            PipelineSnapshot::encode_frozen(1, &cut, 1, 1, &plan, acks([8; 16]), &owner, 16)
                .is_err()
        );
        let encoded =
            PipelineSnapshot::encode_frozen(1, &cut, 1, 1, &plan, acks([7; 16]), &owner, 16)
                .unwrap();
        assert_eq!(&encoded.bytes()[4..6], &version.to_le_bytes());
        assert_eq!(
            PipelineSnapshot::decode(encoded.bytes(), 16).unwrap().iot[0],
            op.freeze().unwrap()
        );
        for old in 3u16..=19 {
            let mut bytes = encoded.bytes().to_vec();
            bytes[4..6].copy_from_slice(&old.to_le_bytes());
            assert!(PipelineSnapshot::decode(&bytes, 16).is_err());
        }
    }
}

#[test]
fn alarm_review_atomic_restore_budget_and_reset_namespace() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = op(&owner, policy());
    send(&mut op, &owner, 0, true, false);
    let before = op.freeze().unwrap();
    let retained = op.retention_bytes();
    for corruption in 0..2 {
        let mut bad = before.clone();
        if corruption == 0 {
            bad.entries[0].values[2] = Scalar::Int64(999);
        } else {
            bad.entries[0].values[crate::alarm_iot::PREFIX + 1] = Scalar::Bool(false);
        }
        assert!(
            bad.encode().is_ok(),
            "shape valid but policy/condition invalid"
        );
        assert!(op.restore(&bad).is_err());
        assert_eq!(op.freeze().unwrap(), before);
        assert_eq!(op.retention_bytes(), retained);
    }
    let full = owner
        .acquire(
            CreditKind::Retention,
            owner.budget().retention_bytes - owner.usage().retention_bytes,
        )
        .unwrap();
    assert!(op
        .on_batch(
            &batch(&owner, "a", Scalar::Bool(false), Scalar::Bool(true), None),
            1
        )
        .is_err());
    assert_eq!(op.freeze().unwrap(), before);
    drop(full);
    op.reset();
    assert!(op.bind_generation([7; 16]).is_err());
    assert!(op
        .on_batch(
            &batch(&owner, "a", Scalar::Bool(true), Scalar::Bool(false), None),
            1
        )
        .is_err());
    op.bind_generation([8; 16]).unwrap();
    send(&mut op, &owner, 1, true, false);
    let rows = advance(&mut op, 11);
    assert_eq!(rows[0].values[6], Scalar::utf8("08".repeat(16)));
    assert_eq!(rows[0].values[8], Scalar::UInt64(1));
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

use crate::{IotFreeze, IotOperator};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, MemoryOwner, ResourceBudget, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema,
};
use sparrow_plan::{
    InvalidValuePolicy, IotSpec, IotTimingSpec, ProcessingTimePolicy, ResampleMode, ResampleSpec,
};
use std::sync::Arc;

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "device", DataType::Utf8, false),
            Field::new(2, "u", DataType::UInt64, true),
            Field::new(3, "v", DataType::Float64, true),
        ],
    )
    .unwrap()
}
fn spec(mode: ResampleMode) -> IotSpec {
    IotSpec {
        keys: vec!["device".into()],
        fields: vec!["u".into(), "v".into()],
        emit_first: false,
        ttl_micros: 0,
        max_keys: 4,
        invalid: InvalidValuePolicy::Ignore,
        deadband: None,
        hysteresis: None,
        timing: Some(IotTimingSpec::Resample(Box::new(ResampleSpec {
            mode,
            period_micros: 10,
            max_wait_micros: if mode == ResampleMode::Interpolate {
                5
            } else {
                0
            },
            max_gap_micros: if mode == ResampleMode::Interpolate {
                20
            } else {
                0
            },
            max_emissions_per_decision: 8,
            clock: ProcessingTimePolicy::Paused,
        }))),
    }
}
fn operator(owner: &Arc<MemoryOwner>, spec: IotSpec) -> IotOperator {
    let mut op = IotOperator::new(2.into(), spec, schema(), owner.clone()).unwrap();
    op.bind_generation([7; 16]).unwrap();
    op
}
fn batch(owner: &Arc<MemoryOwner>, key: &str, u: Scalar, v: Scalar) -> RowBatch {
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
            values: vec![Scalar::utf8(key), u, v],
        })
        .unwrap();
    builder.finish().unwrap()
}
fn input(
    op: &mut IotOperator,
    owner: &Arc<MemoryOwner>,
    at: i64,
    key: &str,
    u: u64,
    v: f64,
) -> Vec<Row> {
    op.on_batch(
        &batch(owner, key, Scalar::UInt64(u), Scalar::Float64(v)),
        at,
    )
    .unwrap()
    .map(|b| b.rows().to_vec())
    .unwrap_or_default()
}
fn advance(op: &mut IotOperator, at: i64) -> Vec<Row> {
    op.set_processing_time(at).unwrap();
    let mut rows = Vec::new();
    while op.next_deadline().is_some_and(|deadline| deadline <= at) {
        if let Some(b) = op.take_timed_due(at).unwrap() {
            rows.extend_from_slice(b.rows());
        }
    }
    rows
}
fn check(row: &Row, grid: i64, emitted: i64, samples: u64, u: Scalar, v: Scalar) {
    assert_eq!(row.values.len(), 10);
    assert_eq!(&row.values[1..3], &[u, v]);
    assert_eq!(row.values[4], Scalar::Int64(grid));
    assert_eq!(row.values[5], Scalar::Int64(emitted));
    assert_eq!(row.values[6], Scalar::Bool(samples == 0));
    assert_eq!(row.values[7], Scalar::UInt64(samples));
    assert_eq!(
        row.values[8],
        Scalar::utf8("07070707070707070707070707070707")
    );
    assert_eq!(row.values[9], Scalar::UInt64(2));
}

#[test]
fn resample_last_keeps_uint64_precision_and_half_open_empty_intervals() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, spec(ResampleMode::Last));
    assert!(advance(&mut op, 1).is_empty());
    input(&mut op, &owner, 1, "a", 1, 1.0);
    advance(&mut op, 9);
    input(&mut op, &owner, 9, "a", u64::MAX, 9.0);
    let rows = advance(&mut op, 10);
    assert_eq!(rows.len(), 1);
    check(
        &rows[0],
        10,
        10,
        2,
        Scalar::UInt64(u64::MAX),
        Scalar::Float64(9.0),
    );
    input(&mut op, &owner, 10, "a", 2, 2.0);
    let rows = advance(&mut op, 30);
    assert_eq!(rows.len(), 2);
    check(&rows[0], 20, 30, 1, Scalar::UInt64(2), Scalar::Float64(2.0));
    check(&rows[1], 30, 30, 0, Scalar::Null, Scalar::Null);
    assert_eq!(op.resample_stats().discarded_inputs, 1);
    assert_eq!(op.resample_stats().missing_outputs, 1);
    assert_eq!(op.key_count(), 1);
}

#[test]
fn resample_mean_uses_complete_vectors_and_survives_finite_extremes() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, spec(ResampleMode::Mean));
    advance(&mut op, 1);
    input(&mut op, &owner, 1, "a", 2, f64::MAX);
    advance(&mut op, 2);
    assert!(op
        .on_batch(&batch(&owner, "a", Scalar::UInt64(999), Scalar::Null), 2)
        .unwrap()
        .is_none());
    input(&mut op, &owner, 2, "a", 4, -f64::MAX);
    let rows = advance(&mut op, 10);
    check(
        &rows[0],
        10,
        10,
        2,
        Scalar::Float64(3.0),
        Scalar::Float64(0.0),
    );
    assert_eq!(op.resample_stats().discarded_inputs, 1);
    assert_eq!(op.stats().invalid_rows, 1);
    input(&mut op, &owner, 10, "a", 4, f64::MAX);
    advance(&mut op, 11);
    input(&mut op, &owner, 11, "a", 4, f64::MAX);
    let rows = advance(&mut op, 20);
    check(
        &rows[0],
        20,
        20,
        2,
        Scalar::Float64(4.0),
        Scalar::Float64(f64::MAX),
    );
}

#[test]
fn resample_interpolation_exact_bracket_and_expiry_equality() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, spec(ResampleMode::Interpolate));
    advance(&mut op, 0);
    let rows = input(&mut op, &owner, 0, "a", 0, 0.0);
    check(
        &rows[0],
        0,
        0,
        1,
        Scalar::Float64(0.0),
        Scalar::Float64(0.0),
    );
    advance(&mut op, 8);
    input(&mut op, &owner, 8, "a", 8, 8.0);
    assert!(advance(&mut op, 10).is_empty());
    advance(&mut op, 12);
    let rows = input(&mut op, &owner, 12, "a", 12, 12.0);
    check(
        &rows[0],
        10,
        12,
        2,
        Scalar::Float64(10.0),
        Scalar::Float64(10.0),
    );
    assert!(advance(&mut op, 20).is_empty());
    let rows = advance(&mut op, 25);
    check(&rows[0], 20, 25, 0, Scalar::Null, Scalar::Null);
    assert!(
        input(&mut op, &owner, 25, "a", 25, 25.0).is_empty(),
        "expired result is never rewritten"
    );
    advance(&mut op, 30);
    let rows = input(&mut op, &owner, 30, "a", 30, 30.0);
    check(
        &rows[0],
        30,
        30,
        1,
        Scalar::Float64(30.0),
        Scalar::Float64(30.0),
    );
    assert!(
        input(&mut op, &owner, 30, "a", 99, 99.0).is_empty(),
        "duplicate exact sample cannot repeat a grid"
    );
    assert_eq!(op.resample_stats().interpolated_outputs, 1);
}

#[test]
fn resample_interpolation_max_gap_first_right_and_no_extrapolation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut configuration = spec(ResampleMode::Interpolate);
    if let Some(IotTimingSpec::Resample(c)) = &mut configuration.timing {
        c.max_gap_micros = 3;
    }
    let mut op = operator(&owner, configuration);
    advance(&mut op, 8);
    input(&mut op, &owner, 8, "a", 8, 8.0);
    advance(&mut op, 12);
    let rows = input(&mut op, &owner, 12, "a", 12, 12.0);
    check(&rows[0], 10, 12, 0, Scalar::Null, Scalar::Null);
    advance(&mut op, 13);
    assert!(input(&mut op, &owner, 13, "a", 13, 13.0).is_empty());
    let rows = advance(&mut op, 25);
    assert_eq!(rows.len(), 1);
    check(&rows[0], 20, 25, 0, Scalar::Null, Scalar::Null);
    advance(&mut op, 29);
    input(&mut op, &owner, 29, "a", 29, 29.0);
    advance(&mut op, 32);
    let rows = input(&mut op, &owner, 32, "a", 32, 32.0);
    check(
        &rows[0],
        30,
        32,
        2,
        Scalar::Float64(30.0),
        Scalar::Float64(30.0),
    );
}

#[test]
fn resample_invalid_vectors_do_not_create_keys_or_refresh_left_point() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for mode in [
        ResampleMode::Last,
        ResampleMode::Mean,
        ResampleMode::Interpolate,
    ] {
        let mut op = operator(&owner, spec(mode));
        advance(&mut op, 1);
        for bad in [
            Scalar::Null,
            Scalar::Float64(f64::NAN),
            Scalar::Float64(f64::INFINITY),
        ] {
            assert!(op
                .on_batch(&batch(&owner, "missing", Scalar::UInt64(1), bad), 1)
                .unwrap()
                .is_none());
        }
        assert_eq!(op.key_count(), 0);
        assert_eq!(op.stats().invalid_rows, 3);
        input(&mut op, &owner, 1, "a", 1, 1.0);
        let before = op.freeze().unwrap();
        assert!(op
            .on_batch(&batch(&owner, "a", Scalar::Null, Scalar::Float64(99.0)), 1)
            .unwrap()
            .is_none());
        assert_eq!(op.freeze().unwrap(), before);
    }
}

#[test]
fn resample_timer_order_and_budget_preflight_are_atomic() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for mode in [
        ResampleMode::Last,
        ResampleMode::Mean,
        ResampleMode::Interpolate,
    ] {
        let mut c = spec(mode);
        c.max_keys = 1;
        if let Some(IotTimingSpec::Resample(t)) = &mut c.timing {
            t.max_emissions_per_decision = 2;
        }
        let mut op = operator(&owner, c);
        advance(&mut op, 1);
        input(&mut op, &owner, 1, "a", 1, 1.0);
        let before = op.freeze().unwrap();
        if mode == ResampleMode::Interpolate {
            // Two expired grids would fill the cap, leaving no slot for an
            // exact/new input. Refuse before advancing or emitting either.
            assert_eq!(
                op.set_processing_time(25).unwrap_err().code,
                ErrorCode::BoundExceeded
            );
            assert_eq!(op.freeze().unwrap(), before);
        }
        assert_eq!(
            op.set_processing_time(40).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        assert_eq!(op.freeze().unwrap(), before);
        op.set_processing_time(10).unwrap();
        assert!(op
            .on_batch(
                &batch(&owner, "a", Scalar::UInt64(2), Scalar::Float64(2.0)),
                10
            )
            .is_err());
        while op.next_deadline().is_some_and(|t| t <= 10) {
            op.take_timed_due(10).unwrap();
        }
        input(&mut op, &owner, 10, "a", 2, 2.0);
        assert!(op.set_processing_time(9).is_err());
    }
}

#[test]
fn resample_wait_equal_period_expires_then_opens_next_grid() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut c = spec(ResampleMode::Interpolate);
    if let Some(IotTimingSpec::Resample(t)) = &mut c.timing {
        t.max_wait_micros = 10;
    }
    let mut op = operator(&owner, c);
    advance(&mut op, 1);
    input(&mut op, &owner, 1, "a", 1, 1.0);
    advance(&mut op, 10);
    let rows = advance(&mut op, 20);
    check(&rows[0], 10, 20, 0, Scalar::Null, Scalar::Null);
    let rows = input(&mut op, &owner, 20, "a", 20, 20.0);
    check(
        &rows[0],
        20,
        20,
        1,
        Scalar::Float64(20.0),
        Scalar::Float64(20.0),
    );
}

#[test]
fn resample_freeze_roundtrip_modes_strict_scan_and_continuation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for mode in [
        ResampleMode::Last,
        ResampleMode::Mean,
        ResampleMode::Interpolate,
    ] {
        let mut live = operator(&owner, spec(mode));
        advance(&mut live, 8);
        input(&mut live, &owner, 8, "a", 8, 8.0);
        if mode == ResampleMode::Interpolate {
            advance(&mut live, 10);
        }
        let cut = if mode == ResampleMode::Interpolate {
            10
        } else {
            8
        };
        let saved = live.freeze().unwrap();
        let bytes = saved.encode().unwrap();
        let mut bool_key = saved.clone();
        bool_key.entries[0].key = vec![Scalar::Bool(true)];
        let mut bad_key = bool_key.encode().unwrap();
        bad_key[14] = 2;
        assert_eq!(IotFreeze::decode(&bytes, 4).unwrap(), saved);
        for materialize in [false, true] {
            assert!(
                IotFreeze::decode_at_cut(&mut bad_key.as_slice(), 4, materialize, Some(cut))
                    .is_err()
            );
            let prefix = 11
                + 2
                + saved.entries[0]
                    .key
                    .iter()
                    .map(|v| v.encoded_value_len().unwrap())
                    .sum::<usize>()
                + 2;
            let mut noncanonical = bytes.clone();
            noncanonical[prefix + 37] = 2;
            assert!(IotFreeze::decode_at_cut(
                &mut noncanonical.as_slice(),
                4,
                materialize,
                Some(cut)
            )
            .is_err());
            let mut nonfinite = bytes.clone();
            let n = nonfinite.len();
            nonfinite[n - 8..].copy_from_slice(&f64::NAN.to_le_bytes());
            assert!(
                IotFreeze::decode_at_cut(&mut nonfinite.as_slice(), 4, materialize, Some(cut))
                    .is_err()
            );
            let mut src = bytes.as_slice();
            IotFreeze::decode_at_cut(&mut src, 4, materialize, Some(cut)).unwrap();
            assert!(src.is_empty());
            for end in 0..bytes.len() {
                assert!(
                    IotFreeze::decode_at_cut(&mut &bytes[..end], 4, materialize, Some(cut))
                        .is_err()
                );
            }
            assert!(
                IotFreeze::decode_at_cut(&mut bytes.as_slice(), 4, materialize, Some(20)).is_err()
            );
        }
        let mut resumed = operator(&owner, spec(mode));
        resumed.restore(&saved).unwrap();
        resumed.validate_processing_cut(cut).unwrap();
        assert_eq!(resumed.freeze().unwrap(), saved);
        assert_eq!(advance(&mut live, 12), advance(&mut resumed, 12));
        assert_eq!(
            input(&mut live, &owner, 12, "a", 12, 12.0),
            input(&mut resumed, &owner, 12, "a", 12, 12.0)
        );
        assert_eq!(advance(&mut live, 25), advance(&mut resumed, 25));
        assert_eq!(live.freeze().unwrap(), resumed.freeze().unwrap());
    }
}

#[test]
fn resample_corrupt_restore_is_atomic_and_rejects_wrong_grid_type_and_mode() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, spec(ResampleMode::Last));
    advance(&mut op, 8);
    input(&mut op, &owner, 8, "a", 8, 8.0);
    let before = op.freeze().unwrap();
    for field in 0..8 {
        let mut bad = before.clone();
        match field {
            0 => bad.entries[0].values[0] = Scalar::Int64(11),
            1 => bad.entries[0].values[1] = Scalar::Int64(9),
            2 => bad.entries[0].values[2] = Scalar::Int64(10),
            3 => bad.entries[0].values[3] = Scalar::UInt64(0),
            4 => bad.entries[0].values[4] = Scalar::Bool(true),
            5 => bad.entries[0].values[5] = Scalar::Float64(8.0),
            6 => bad.entries[0].key[0] = Scalar::Null,
            _ => bad.kind = 14,
        }
        assert!(op.restore(&bad).is_err(), "mutation {field}");
        assert_eq!(op.freeze().unwrap(), before);
    }
    let mut duplicate = before.clone();
    duplicate.entries.push(before.entries[0].clone());
    assert!(op.restore(&duplicate).is_err());
    assert_eq!(op.freeze().unwrap(), before);
}

#[test]
fn resample_expired_pending_rejected_at_restore_cut_and_cleanup_rotates_namespace() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, spec(ResampleMode::Interpolate));
    advance(&mut op, 8);
    input(&mut op, &owner, 8, "a", 8, 8.0);
    advance(&mut op, 10);
    let saved = op.freeze().unwrap();
    let mut restored = operator(&owner, spec(ResampleMode::Interpolate));
    restored.restore(&saved).unwrap();
    assert!(restored.validate_processing_cut(15).is_err());
    restored.validate_processing_cut(10).unwrap();
    assert_eq!(restored.freeze().unwrap(), saved);
    restored.cleanup();
    assert_eq!(restored.retention_bytes(), 0);
    assert!(restored.bind_generation([7; 16]).is_err());
    restored.bind_generation([8; 16]).unwrap();
    advance(&mut restored, 0);
    let rows = input(&mut restored, &owner, 0, "a", 1, 1.0);
    assert_eq!(
        rows[0].values[8],
        Scalar::utf8("08080808080808080808080808080808")
    );
}

#[test]
fn resample_refused_output_and_restore_refund_without_consuming_state() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let baseline = owner.usage();
    {
        let mut op = operator(&owner, spec(ResampleMode::Last));
        advance(&mut op, 1);
        input(&mut op, &owner, 1, "a", 1, 1.0);
        let saved = op.freeze().unwrap();
        let usage = owner.usage();
        let free = owner.budget().reservation_bytes - usage.reservation_bytes;
        let burned = owner.acquire(CreditKind::Reservation, free).unwrap();
        assert_eq!(
            op.restore(&saved).unwrap_err().code,
            ErrorCode::ResourceExhausted
        );
        op.set_processing_time(10).unwrap();
        assert_eq!(
            op.take_timed_due(10).unwrap_err().code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(op.next_deadline(), Some(10));
        drop(burned);
        let output = op.take_timed_due(10).unwrap().unwrap();
        check(
            &output.rows()[0],
            10,
            10,
            1,
            Scalar::UInt64(1),
            Scalar::Float64(1.0),
        );
    }
    let after = owner.usage();
    assert_eq!(after.reservation_bytes, baseline.reservation_bytes);
    assert_eq!(after.retention_bytes, baseline.retention_bytes);
    assert_eq!(after.physical_bytes, baseline.physical_bytes);
    assert_eq!(after.live_handles, baseline.live_handles);
}

#[test]
fn resample_snapshot_profiles_preserve_output_epoch_and_reject_downgrade() {
    use crate::{ParticipantAcks, PipelineSnapshot};
    use sparrow_plan::{CheckpointPlan, ParticipantId, StateParticipant};
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for mode in [
        ResampleMode::Last,
        ResampleMode::Mean,
        ResampleMode::Interpolate,
    ] {
        let mut op = operator(&owner, spec(mode));
        advance(&mut op, 1);
        input(&mut op, &owner, 1, "a", 1, 1.0);
        let plan = CheckpointPlan {
            source: 1.into(),
            sink: 3.into(),
            states: vec![StateParticipant {
                id: ParticipantId::iot(2.into()),
                codec: 2,
                window_kind: mode.state_kind(),
            }],
            reference_tables: vec![],
            semantics: b"CP01resample-test".to_vec(),
            recovery_prefix_len: None,
        };
        for (kind, version) in [("file", 25u16), ("jetstream-v1", 26u16)] {
            let cut = crate::processing_cut::ProcessingCut {
                sequence: 1,
                micros: 1,
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
                next_output: Some(sparrow_model::OutputSequence::new([7; 16], 1).unwrap()),
                freezes: vec![crate::barrier::EncodedFreeze::from_iot(&op, &owner, 4).unwrap()],
            };
            assert!(PipelineSnapshot::encode_frozen(
                1,
                &cut,
                1,
                1,
                &plan,
                acks([8; 16]),
                &owner,
                4
            )
            .is_err());
            let encoded =
                PipelineSnapshot::encode_frozen(1, &cut, 1, 1, &plan, acks([7; 16]), &owner, 4)
                    .unwrap();
            assert_eq!(&encoded.bytes()[4..6], &version.to_le_bytes());
            assert_eq!(
                PipelineSnapshot::decode(encoded.bytes(), 4).unwrap().iot[0],
                op.freeze().unwrap()
            );
            for old in 3u16..=24 {
                let mut bytes = encoded.bytes().to_vec();
                bytes[4..6].copy_from_slice(&old.to_le_bytes());
                assert!(
                    PipelineSnapshot::decode(&bytes, 4).is_err(),
                    "downgrade {old}"
                );
            }
        }
    }
}

#[test]
fn resample_key_and_time_and_sample_count_limits_fail_closed() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut c = spec(ResampleMode::Last);
    c.max_keys = 1;
    let mut op = operator(&owner, c);
    advance(&mut op, 1);
    input(&mut op, &owner, 1, "a", 1, 1.0);
    let before = op.freeze().unwrap();
    assert_eq!(
        op.on_batch(
            &batch(&owner, "b", Scalar::UInt64(2), Scalar::Float64(2.0)),
            1
        )
        .unwrap_err()
        .code,
        ErrorCode::ResourceExhausted
    );
    assert!(op.set_processing_time(i64::MAX).is_err());
    assert_eq!(op.freeze().unwrap(), before);
    let mut exhausted = before.clone();
    exhausted.entries[0].values[3] = Scalar::UInt64(u64::MAX);
    op.restore(&exhausted).unwrap();
    op.validate_processing_cut(1).unwrap();
    assert!(op
        .on_batch(
            &batch(&owner, "a", Scalar::UInt64(2), Scalar::Float64(2.0)),
            1
        )
        .is_err());
    assert_eq!(op.freeze().unwrap(), exhausted);
}

#[test]
fn resample_metrics_distinguish_missing_and_discard_from_interpolation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner, spec(ResampleMode::Last));
    advance(&mut op, 1);
    input(&mut op, &owner, 1, "a", 1, 1.0);
    input(&mut op, &owner, 1, "a", 2, 2.0);
    advance(&mut op, 20);
    let metrics = crate::RuntimeMetrics::new();
    op.report_resample_metrics(&metrics);
    op.report_resample_metrics(&metrics);
    let snap = metrics.snapshot();
    assert_eq!(snap.resample_discarded_inputs, 1);
    assert_eq!(snap.resample_missing_outputs, 1);
    assert_eq!(snap.resample_interpolated_outputs, 0);
    assert!(snap.log_line().contains("\"resample_missing_outputs\":1"));
    assert_eq!(snap.alarm_notifications_deferred, 0);
}

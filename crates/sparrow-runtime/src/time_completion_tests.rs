//! Deterministic time/state oracles independent of the live source clock.
use crate::{
    AlignedAcks, AlignedJob, IngressEvent, IotOperator, JobRequest, Kernel, KernelOptions,
    PipelineRestore, PipelineSnapshot, SharedCapture, StreamControl,
};
use sparrow_model::{
    AggFn, CreditKind, DataType, Field, InflightCounter, MemoryOwner, OutputSequence,
    ResourceBudget, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SharedVirtualClock, WindowKind,
};
use sparrow_plan::{
    AggCall, CheckpointPlan, InvalidValuePolicy, IotSpec, IotTimingSpec, PhysicalPlan,
    PhysicalStage, ProcessingTimePolicy, WindowSpec,
};
use std::{sync::Arc, time::Duration};

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "id", DataType::Utf8, false),
            Field::new(2, "v", DataType::Int64, true),
        ],
    )
    .unwrap()
}
fn iot(kind: &str) -> IotSpec {
    IotSpec {
        keys: vec!["id".into()],
        fields: vec!["v".into()],
        emit_first: kind != "debounce",
        ttl_micros: if kind == "ttl" { 100 } else { 0 },
        max_keys: 8,
        invalid: InvalidValuePolicy::Ignore,
        deadband: None,
        hysteresis: None,
        timing: (kind == "debounce").then_some(IotTimingSpec::Debounce {
            quiet_micros: 100,
            max_wait_micros: 1000,
            leading: false,
            trailing: true,
            reset_on_repeat: true,
            clock: ProcessingTimePolicy::Paused,
        }),
    }
}
fn physical(kinds: &[&str]) -> PhysicalPlan {
    let mut input = schema();
    let mut stages = vec![PhysicalStage::MemorySource {
        operator: 1.into(),
        name: "s".into(),
        schema: input.clone(),
    }];
    for (i, kind) in kinds.iter().enumerate() {
        let operator = (10 + i as u32).into();
        if matches!(*kind, "pt" | "count") {
            let spec = WindowSpec::new(
                if *kind == "pt" {
                    WindowKind::TumblingProcessingTime { size_micros: 100 }
                } else {
                    WindowKind::Count { size: 2 }
                },
                vec!["id".into()],
                vec![AggCall::new(
                    AggFn::Sum,
                    Some(sparrow_expr::Expr::Column { name: "v".into() }),
                    "v",
                )],
            );
            let output = sparrow_plan::window_output_schema(&input, &spec).unwrap();
            stages.push(PhysicalStage::WindowAgg {
                operator,
                spec,
                input,
                output: output.clone(),
            });
            input = output;
        } else {
            stages.push(PhysicalStage::Iot {
                operator,
                spec: iot(kind),
                input: input.clone(),
                output: input.clone(),
            });
        }
    }
    stages.push(PhysicalStage::CaptureSink {
        operator: 30.into(),
        name: "out".into(),
        schema: input,
    });
    PhysicalPlan {
        pipeline: 7.into(),
        revision: 1.into(),
        stages,
        edges: None,
        source_times: vec![],
        side_outputs: vec![],
    }
}
fn row(v: i64) -> Row {
    Row {
        values: vec![Scalar::utf8("a"), Scalar::Int64(v)],
    }
}
fn batch(owner: &Arc<MemoryOwner>, v: Scalar) -> RowBatch {
    let mut out = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        1,
        owner.budget().reservation_bytes,
    )
    .unwrap();
    out.push(Row {
        values: vec![Scalar::utf8("a"), v],
    })
    .unwrap();
    out.finish().unwrap()
}

/// Commit every deterministic decision through the real FIFO + required sink.
/// The virtual clock stays at startup: accidental host/clock sampling is caught.
fn drive(
    kinds: &[&str],
    restored: Option<PipelineSnapshot>,
    decisions: &[(i64, Option<i64>)],
) -> (PipelineSnapshot, Vec<(Vec<u8>, Row)>) {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    kernel.block_on(async {
        let plan = physical(kinds);
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan).unwrap());
        let mut cut = restored
            .as_ref()
            .map(|s| crate::processing_cut::ProcessingCut::unwrap(&s.source).unwrap())
            .unwrap_or_else(|| crate::processing_cut::ProcessingCut {
                sequence: 0,
                micros: 0,
                source: sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity {
                    kind: "file".into(),
                    path: "fixture".into(),
                    size: 0,
                    fingerprint: 0,
                }),
            });
        let mut rows = restored.as_ref().map_or(0, |s| s.ingested_rows);
        let output = restored
            .as_ref()
            .and_then(|s| s.next_output)
            .unwrap_or(OutputSequence::new([5; 16], 1).unwrap());
        let (restore, restored_iot) = restored
            .map(|s| (Some(s.windows), s.iot))
            .unwrap_or((None, vec![]));
        let acks = AlignedAcks::default().with_output_sequence(output).unwrap();
        let (tx, rx) = sparrow_io::observed::channel(1);
        let (out, mut received) = sparrow_io::observed::channel::<RowBatch>(1);
        let inflight = Arc::new(InflightCounter::new());
        let count = inflight.clone();
        let job = kernel
            .submit(
                JobRequest::new(plan, vec![], SharedCapture::disabled())
                    .with_clock(crate::RuntimeClock::virtual_clock(SharedVirtualClock::new(
                        cut.micros,
                    )))
                    .with_live_events(rx)
                    .with_live_out(out)
                    .with_aligned(AlignedJob {
                        restore: None,
                        pipeline: Some(PipelineRestore { buffered: Vec::new(),
                            sink: None,
                            plan: manifest.clone(),
                            generation: [5; 16],
                            restore,
                            iot: restored_iot,
                        }),
                        acks: acks.clone(),
                        outbox: inflight,
                    }),
            )
            .unwrap();
        let owner = job.memory_owner();
        let sink = tokio::spawn(async move {
            let mut result = vec![];
            while let Some(batch) = received.recv().await {
                for (i, row) in batch.rows().iter().enumerate() {
                    result.push((
                        batch
                            .output_sequence()
                            .unwrap()
                            .id_ascii(i)
                            .unwrap()
                            .to_vec(),
                        row.detach_copy(),
                    ));
                }
                count.ack();
            }
            result
        });
        let mut snapshot = None;
        for &(now, v) in decisions {
            cut.sequence += 1;
            cut.micros = now;
            let request = acks.begin(cut.sequence).unwrap();
            tx.send(IngressEvent::Control(StreamControl::ProcessingTime {
                micros: now,
            }))
            .await
            .unwrap();
            if let Some(v) = v {
                tx.send(IngressEvent::Row(row(v))).await.unwrap();
                rows += 1;
                cut.source.record_index = rows;
                cut.source.offset_bytes = rows;
            }
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: cut.sequence,
            }))
            .await
            .unwrap();
            let acknowledged = request
                .wait_participants(Duration::from_secs(3))
                .await
                .unwrap();
            let encoded = PipelineSnapshot::encode_frozen(
                cut.sequence,
                &cut.wrap().unwrap(),
                rows,
                1,
                &manifest,
                acknowledged,
                &owner,
                1024,
            )
            .unwrap();
            assert!(PipelineSnapshot::decode_mode(encoded.bytes(), 1024, false).is_ok());
            snapshot = Some(PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap());
        }
        job.stop().await.unwrap();
        drop(tx);
        let output = sink.await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);
        (snapshot.unwrap(), output)
    })
}
fn values(kinds: &[&str], rows: &[(Vec<u8>, Row)]) -> Vec<Scalar> {
    let plan = physical(kinds);
    let PhysicalStage::CaptureSink { schema, .. } = plan.stages.last().unwrap() else {
        panic!()
    };
    let index = schema.index_of_name("v").unwrap();
    rows.iter().map(|(_, r)| r.values[index].clone()).collect()
}

#[test]
fn time_completion_pt_restores_boundary_without_wall_time() {
    let (saved, out) = drive(&["pt"], None, &[(10, Some(2)), (50, Some(3))]);
    assert!(out.is_empty());
    let (_, out) = drive(
        &["pt"],
        Some(saved),
        &[(99, None), (100, Some(7)), (200, None)],
    );
    assert_eq!(
        values(&["pt"], &out),
        vec![Scalar::Int64(5), Scalar::Int64(7)]
    );
}
#[test]
fn time_completion_ttl_refresh_equal_expiry_and_downtime() {
    let (saved, out) = drive(&["ttl"], None, &[(10, Some(5)), (90, Some(5))]);
    assert_eq!(out.len(), 1);
    let (saved, out) = drive(&["ttl"], Some(saved), &[(189, Some(5))]);
    assert!(out.is_empty());
    let (_, out) = drive(&["ttl"], Some(saved), &[(289, Some(5))]);
    assert_eq!(out.len(), 1);
}
#[test]
fn time_completion_downstream_pt_uses_new_cut_for_upstream_timer_row() {
    let (saved, out) = drive(&["pt", "pt"], None, &[(10, Some(3)), (100, None)]);
    assert!(out.is_empty());
    assert_eq!(saved.windows[1].entries[0].window_start, 100);
    let (_, out) = drive(&["pt", "pt"], Some(saved), &[(199, None), (200, None)]);
    assert_eq!(values(&["pt", "pt"], &out), vec![Scalar::Int64(3)]);
}
#[test]
fn time_completion_all_linear_pairs_replay_identical_payloads_and_ids() {
    for first in ["pt", "count", "ttl", "debounce"] {
        for second in ["pt", "count", "ttl", "debounce"] {
            if first == "count" && second == "count" {
                continue;
            }
            let kinds = [first, second];
            let timeline = [
                (10, Some(1)),
                (20, Some(2)),
                (100, None),
                (110, Some(3)),
                (120, Some(4)),
                (200, None),
                (220, None),
                (320, None),
                (500, None),
                (510, Some(5)),
                (520, Some(6)),
                (620, None),
                (700, None),
                (800, None),
            ];
            let (_, whole) = drive(&kinds, None, &timeline);
            let (saved, mut split) = drive(&kinds, None, &timeline[..4]);
            let (_, suffix) = drive(&kinds, Some(saved), &timeline[4..]);
            split.extend(suffix);
            assert_eq!(whole, split, "{kinds:?}");
            assert!(
                !whole.is_empty(),
                "oracle must exercise an output: {kinds:?}"
            );
        }
    }
}
#[test]
fn time_completion_ttl_codec_atomic_restore_invalid_input_and_leases() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    {
        let mut op = IotOperator::new(10.into(), iot("ttl"), schema(), owner.clone()).unwrap();
        op.validate_processing_cut(0).unwrap();
        op.set_processing_time(10).unwrap();
        assert!(op
            .on_batch(&batch(&owner, Scalar::Int64(1)), 10)
            .unwrap()
            .is_some());
        op.set_processing_time(90).unwrap();
        assert!(op
            .on_batch(&batch(&owner, Scalar::Null), 90)
            .unwrap()
            .is_none());
        assert_eq!(op.next_deadline(), Some(110));
        let freeze = op.freeze().unwrap();
        let bytes = freeze.encode().unwrap();
        let mut streaming = vec![];
        op.encode_freeze_into(&mut streaming, 8).unwrap();
        assert_eq!(bytes, streaming);
        for n in 0..bytes.len() {
            assert!(crate::IotFreeze::decode(&bytes[..n], 8).is_err());
        }
        assert!(
            crate::iot::IotFreeze::decode_at_cut(&mut bytes.as_slice(), 8, false, Some(9)).is_err()
        );
        let mut corrupt = freeze.clone();
        corrupt.entries[0].values[0] = Scalar::Int64(i64::MAX);
        assert!(op.restore(&corrupt).is_err());
        assert_eq!(op.freeze().unwrap(), freeze);
        let mut restored =
            IotOperator::new(10.into(), iot("ttl"), schema(), owner.clone()).unwrap();
        restored.restore(&freeze).unwrap();
        assert!(restored.validate_processing_cut(110).is_err());
        restored.validate_processing_cut(100).unwrap();
        assert_eq!(restored.pending_timers(), 1);
        restored.set_processing_time(110).unwrap();
        restored.take_timed_due(110).unwrap();
        assert_eq!(restored.key_count(), 0);
        assert!(restored.set_processing_time(109).is_err());
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}
#[test]
fn time_completion_profile_bounds_and_downgrade_guards() {
    for kinds in [vec!["pt"], vec!["ttl"], vec!["debounce", "ttl"]] {
        let p = CheckpointPlan::from_physical(&physical(&kinds)).unwrap();
        assert!(p.requires_paused_time());
        assert_eq!(CheckpointPlan::decode(&p.encode().unwrap()).unwrap(), p);
        for (kind, version) in [
            (crate::processing_cut::FILE_KIND, 16),
            (crate::processing_cut::JETSTREAM_KIND, 17),
        ] {
            assert_eq!(crate::snapshot_version_for(&p, kind).unwrap(), version);
        }
        for kind in ["file", "jetstream-v1", "file-dag-v1"] {
            assert!(crate::snapshot_version_for(&p, kind).is_err());
        }
        let mut relaxed = p.clone();
        relaxed.recovery_prefix_len = Some(4);
        assert!(relaxed.validate().is_err());
    }
    assert!(CheckpointPlan::from_physical(&physical(&["pt", "ttl", "debounce"])).is_err());
}
#[test]
fn time_completion_pt_restore_rejects_bad_bounds_and_overdue_cut() {
    let (saved, _) = drive(&["pt"], None, &[(10, Some(5))]);
    let freeze = &saved.windows[0];
    let plan = physical(&["pt"]);
    let PhysicalStage::WindowAgg {
        operator,
        spec,
        input,
        ..
    } = &plan.stages[1]
    else {
        panic!()
    };
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op =
        crate::window::WindowOperator::new(*operator, spec.clone(), input.clone(), owner, 8, 8)
            .unwrap();
    for mutation in 0..3 {
        let mut bad = freeze.clone();
        match mutation {
            0 => bad.entries[0].window_start = 1,
            1 => bad.entries[0].window_end = 99,
            _ => bad.entries[0].count = 1,
        }
        assert!(op.validate_participant_restore(&bad).is_err());
    }
    op.validate_participant_restore(freeze).unwrap();
    op.restore_freeze(freeze).unwrap();
    assert!(op.validate_processing_cut(100).is_err());
    op.validate_processing_cut(99).unwrap();
}

#[test]
fn time_completion_downstream_equal_deadline_precedes_upstream_timer_output() {
    let (_, out) = drive(
        &["debounce", "ttl"],
        None,
        &[(0, Some(1)), (100, Some(1)), (200, None)],
    );
    assert_eq!(
        values(&["debounce", "ttl"], &out),
        vec![Scalar::Int64(1), Scalar::Int64(1)]
    );
    let (_, out) = drive(
        &["pt", "debounce"],
        None,
        &[(0, Some(1)), (100, Some(2)), (200, None), (300, None)],
    );
    assert_eq!(
        values(&["pt", "debounce"], &out),
        vec![Scalar::Int64(1), Scalar::Int64(2)]
    );
}

#[test]
fn time_completion_deadband_ttl_preserves_baseline_and_bounded_restore() {
    for baseline in [
        sparrow_plan::DeadbandBaseline::LastInput,
        sparrow_plan::DeadbandBaseline::LastOutput,
    ] {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut spec = iot("ttl");
        spec.deadband = Some(sparrow_plan::DeadbandSpec {
            mode: sparrow_plan::DeadbandMode::Absolute,
            baseline,
            threshold: 10.0,
        });
        let mut op = IotOperator::new(10.into(), spec.clone(), schema(), owner.clone()).unwrap();
        assert!(op
            .on_batch(&batch(&owner, Scalar::Int64(10)), 10)
            .unwrap()
            .is_some());
        assert!(op
            .on_batch(&batch(&owner, Scalar::Int64(15)), 20)
            .unwrap()
            .is_none());
        let freeze = op.freeze().unwrap();
        assert_eq!(freeze.kind, 10);
        assert_eq!(freeze.entries[0].values[0], Scalar::Int64(20));
        let mut restored =
            IotOperator::new(10.into(), spec.clone(), schema(), owner.clone()).unwrap();
        restored.restore(&freeze).unwrap();
        restored.validate_processing_cut(119).unwrap();
        assert_eq!(restored.freeze().unwrap(), freeze);
        let mut overflow = freeze.clone();
        let mut extra = freeze.entries[0].clone();
        extra.key[0] = Scalar::utf8("b");
        overflow.entries.push(extra);
        let mut budget = ResourceBudget::compact();
        budget.max_timers = 1;
        let limited = MemoryOwner::new(budget);
        {
            let mut target = IotOperator::new(10.into(), spec, schema(), limited.clone()).unwrap();
            target.restore(&freeze).unwrap();
            assert!(target.restore(&overflow).is_err());
            assert_eq!(target.freeze().unwrap(), freeze);
        }
        assert_eq!(limited.usage().physical_bytes, 0);
        restored.set_processing_time(120).unwrap();
        restored.take_timed_due(120).unwrap();
        assert!(restored
            .on_batch(&batch(&owner, Scalar::Int64(15)), 120)
            .unwrap()
            .is_some());
    }
}

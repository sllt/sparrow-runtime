//! Independent raw-input expectations on the real Kernel participant path.
use crate::{
    AlignedAcks, AlignedJob, CheckpointStore, IngressEvent, JobRequest, Kernel, KernelOptions,
    MailboxConfig, PipelineRestore, PipelineSnapshot, SharedCapture, StreamControl,
};
use sparrow_model::{
    AggFn, CreditKind, DataType, Field, InflightCounter, MemoryOwner, OperatorId, ResourceBudget,
    Row, Scalar, Schema,
};
use sparrow_plan::{
    AggCall, CheckpointPlan, PhysicalPlan, PhysicalStage, TransformStep, WindowSpec,
};
use std::sync::Arc;
use std::time::Duration;

fn plan(windows: usize, keep: bool) -> PhysicalPlan {
    let schema = Schema::new(
        1,
        vec![
            Field::new(1, "device_id", DataType::Utf8, false),
            Field::new(2, "v", DataType::Int64, false),
        ],
    )
    .unwrap();
    let mut stages = vec![
        PhysicalStage::MemorySource {
            operator: 1.into(),
            name: "sensors".into(),
            schema: schema.clone(),
        },
        PhysicalStage::Transform {
            steps: vec![TransformStep::Filter {
                operator: 2.into(),
                predicate: sparrow_expr::Expr::Literal(Scalar::Bool(keep)),
                input: schema.clone(),
            }],
        },
    ];
    let mut input = schema;
    let mut field = "v";
    for i in 0..windows {
        let alias = if i == 0 { "s" } else { "total" };
        let spec = WindowSpec::new(
            sparrow_model::WindowKind::Count {
                size: if i == 0 { 3 } else { 2 },
            },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(sparrow_expr::Expr::Column { name: field.into() }),
                alias,
            )],
        );
        let output = sparrow_plan::window_output_schema(&input, &spec).unwrap();
        stages.push(PhysicalStage::WindowAgg {
            operator: OperatorId::new(10 + i as u32),
            spec,
            input,
            output: output.clone(),
        });
        input = output;
        field = alias;
    }
    stages.push(PhysicalStage::CaptureSink {
        operator: 20.into(),
        name: "out".into(),
        schema: input,
    });
    PhysicalPlan {
        edges: None,
        side_outputs: vec![],
        source_times: vec![],
        pipeline: 1.into(),
        revision: 1.into(),
        stages,
    }
}
fn kernel() -> Kernel {
    let budget = ResourceBudget::compact();
    Kernel::new_with_job_budget(
        KernelOptions {
            budget: ResourceBudget::performance(),
            mailbox: MailboxConfig {
                max_items: 2,
                max_bytes: 64 * 1024,
            },
            worker_threads: 2,
            rows_per_batch: 2,
        },
        budget,
    )
    .unwrap()
}
fn row(v: i64) -> Row {
    Row {
        values: vec![Scalar::utf8("d1"), Scalar::Int64(v)],
    }
}
fn values(capture: &SharedCapture) -> Vec<i64> {
    capture
        .rows()
        .iter()
        .map(|row| match row.last().unwrap() {
            Scalar::Int64(v) => *v,
            _ => panic!("unexpected output"),
        })
        .collect()
}
fn tmp() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "sparrow-k1-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn r11_freeze(operator: u32) -> crate::WindowFreeze {
    crate::WindowFreeze {
        operator: operator.into(),
        slot: 1.into(),
        kind: 1,
        entries: vec![crate::FrozenEntry {
            key: vec![Scalar::utf8("d1")],
            window_start: 0,
            window_end: 0,
            count: 1,
            accs: vec![crate::aggregate::Accumulator::SumI64 { sum: 1, n: 1 }],
        }],
        wm_in: None,
        wm_out: None,
        last_effective: None,
    }
}

#[test]
fn k2_source_bootstrap_uses_exact_job_owner_and_rejects_foreign_tokens() {
    let k=kernel();let other=kernel();
    let admission=k.prepare_source_admission(1.into()).unwrap();
    let owner=admission.owner();let attempt=admission.attempt();
    let guard=admission.lifecycle_guard();
    let lease=owner.acquire(CreditKind::Reservation,8192).unwrap();
    assert_eq!(k.admitted_jobs(),1,"async bootstrap reserves a real job quota");
    let job=k.submit(JobRequest::new(plan(0,true),vec![],SharedCapture::disabled()).with_source_admission(admission)).unwrap();
    assert_eq!(job.attempt,attempt);assert!(Arc::ptr_eq(&owner,&job.memory_owner()));
    k.block_on(job.stop()).unwrap();
    assert_eq!(k.admitted_jobs(),1,"Kernel completion must not free a still-closing SDK's quota");
    assert_eq!(owner.usage().physical_bytes,8192,"SDK bootstrap lease may outlive Kernel tasks and cannot be refunded early");
    drop(lease);drop(guard);assert_eq!(owner.usage().physical_bytes,0);
    let foreign=k.prepare_source_admission(1.into()).unwrap();
    assert!(other.submit(JobRequest::new(plan(0,true),vec![],SharedCapture::disabled()).with_source_admission(foreign)).is_err());
    let wrong=k.prepare_source_admission(2.into()).unwrap();
    assert!(k.submit(JobRequest::new(plan(0,true),vec![],SharedCapture::disabled()).with_source_admission(wrong)).is_err());
    assert_eq!(k.admitted_jobs(),0);assert_eq!(other.admitted_jobs(),0);
    assert_eq!(k.process_owner().usage().physical_bytes,0);
}

#[test]
fn k2_bootstrap_cannot_steal_live_job_quota_before_queue_admission() {
    let budget=ResourceBudget::compact();
    let k=Kernel::new_with_job_budget(KernelOptions{budget,mailbox:MailboxConfig{max_items:2,max_bytes:64*1024},worker_threads:2,rows_per_batch:2},budget).unwrap();
    let bootstrap=k.prepare_source_admission(1.into()).unwrap();
    assert_eq!(k.admitted_jobs(),1);assert_eq!(k.process_owner().usage().physical_bytes,0);
    let denied=k.prepare_source_admission(2.into()).err().unwrap();
    assert!(denied.context.iter().any(|(k,v)|k=="admission" && v=="capacity"));
    assert!(k.submit(JobRequest::new(plan(0,true),vec![],SharedCapture::disabled())).is_err());
    drop(bootstrap);assert_eq!(k.admitted_jobs(),0);
    k.run(JobRequest::new(plan(0,true),vec![],SharedCapture::disabled())).unwrap();
    assert_eq!(k.admitted_jobs(),0);assert_eq!(k.queue_reserved(),0);
}

#[test]
fn k2_admitted_batch_does_not_widen_legacy_events_and_bills_box_before_allocation() {
    #[allow(dead_code)]
    enum PreviousWideEvent {Row(Row),Batch(sparrow_model::RowBatch),Control(StreamControl)}
    assert!(std::mem::size_of::<IngressEvent>()<=std::mem::size_of::<Row>()+8);
    assert!(std::mem::size_of::<PreviousWideEvent>()>std::mem::size_of::<IngressEvent>());
    eprintln!("K2_INGRESS_LAYOUT inline_variant_bytes={} thin_variant_bytes={} row_batch_bytes={}",
        std::mem::size_of::<PreviousWideEvent>(),std::mem::size_of::<IngressEvent>(),std::mem::size_of::<sparrow_model::RowBatch>());
    let schema=Arc::new(Schema::new(1,vec![Field::new(1,"v",DataType::Int64,false)]).unwrap());
    for enough in [true,false] {
        let mut budget=ResourceBudget::compact();if !enough {budget.reservation_bytes=1;}
        let owner=MemoryOwner::new(budget);
        let batch=sparrow_model::RowBatchBuilder::new(schema.clone(),owner.clone(),CreditKind::Reservation,1,1).unwrap().finish().unwrap();
        let result=IngressEvent::admitted_batch(batch);
        assert_eq!(result.is_ok(),enough);
        if enough {assert!(owner.usage().reservation_bytes>=1+std::mem::size_of::<sparrow_model::RowBatch>());}
        drop(result);assert_eq!(owner.usage().physical_bytes,0);
    }
}

#[test]
fn k2_reliable_sink_cursor_is_cut_local_and_snapshot_v4_is_not_legacy() {
    for windows in 0..=2 {
        let kernel=kernel();
        kernel.block_on(async {
            let physical=plan(windows,true);
            let manifest=Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
            let first=sparrow_model::OutputSequence::new([3;16],1).unwrap();
            let acks=AlignedAcks::default().with_output_sequence(first).unwrap();
            let (tx,rx)=sparrow_io::observed::channel(2);
            let (out,mut received)=sparrow_io::observed::channel::<sparrow_model::RowBatch>(16);
            let counter=Arc::new(InflightCounter::new());
            let count=counter.clone();
            let handle=kernel.submit(JobRequest::new(physical,vec![],SharedCapture::disabled())
                .with_live_events(rx).with_live_out(out).with_aligned(AlignedJob{restore:None,
                    pipeline:Some(PipelineRestore{buffered:Vec::new(),sink:None,plan:manifest.clone(),generation:[3;16],restore:None,iot:vec![]}),acks:acks.clone(),outbox:counter})).unwrap();
            let sink=tokio::spawn(async move {
                let mut result=Vec::new();
                while let Some(batch)=received.recv().await {
                    let sequence=batch.output_sequence().expect("reliable output envelope");
                    for i in 0..batch.num_rows() {result.push(sequence.id_ascii(i).unwrap());}
                    count.ack();
                }
                result
            });
            let owner=handle.memory_owner();
            let request=acks.begin(1).unwrap();
            for v in 1..=6 {tx.send(IngressEvent::Row(row(v))).await.unwrap();}
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier{checkpoint_id:1})).await.unwrap();
            // Later inputs are deliberately queued before reading barrier ACKs.
            // They MUST NOT advance the checkpoint's Sink cursor.
            for v in 7..=12 {tx.send(IngressEvent::Row(row(v))).await.unwrap();}
            let aligned=request.wait_participants(Duration::from_secs(2)).await.unwrap();
            let expected=match windows {0=>6,1=>2,_=>1};
            assert_eq!(aligned.next_output().unwrap().first(),1+expected);
            let mut source=sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("fixture",0,0));
            source.identity.kind="jetstream-v1".into();source.offset_bytes=6;source.record_index=6;
            let encoded=PipelineSnapshot::encode_frozen(1,&source,6,1,&manifest,aligned,&owner,1024).unwrap();
            assert_eq!(&encoded.bytes()[4..6],&4u16.to_le_bytes());
            let decoded=PipelineSnapshot::decode(encoded.bytes(),1024).unwrap();
            assert_eq!(decoded.next_output,Some(first.advance(expected as usize).unwrap()));
            assert_eq!(decoded.windows.len(),windows);
            assert!(crate::CheckpointSnapshot::decode(encoded.bytes()).is_err());
            let dir=tmp();let mut store=CheckpointStore::open_reliable_exclusive(&dir,1024,Default::default()).unwrap();
            store.commit_prepared(&encoded).unwrap();
            assert_eq!(store.recover_pipeline_required().unwrap().next_output,decoded.next_output);
            assert_eq!(store.inventory().unwrap().generations[0].metadata.as_ref().unwrap().version,4);
            drop(tx);handle.wait().await.unwrap();let all=sink.await.unwrap();
            assert_eq!(all.len(),expected as usize*2);
            assert_eq!(all,(0..all.len()).map(|i|first.id_ascii(i).unwrap()).collect::<Vec<_>>());
            drop(encoded);drop(store);std::fs::remove_dir_all(dir).unwrap();assert_eq!(owner.usage().physical_bytes,0);
        });
    }
}

#[test]
fn k2_file_and_reliable_writer_profiles_refuse_mixing_and_invalid_output_identity() {
    for reliable in [false,true] {
        let owner=MemoryOwner::new(ResourceBudget::compact());let dir=tmp();
        let manifest=CheckpointPlan::from_physical(&plan(0,true)).unwrap();
        let mut source=sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("profile",0,0));
        if reliable {source.identity.kind="jetstream-v1".into();}
        let encoded=PipelineSnapshot::encode_frozen(1,&source,0,1,&manifest,crate::ParticipantAcks {
            attempt:1,generation:[3;16],freezes:vec![],
            next_output:reliable.then(||sparrow_model::OutputSequence::new([3;16],1).unwrap()),
        },&owner,1024).unwrap();
        if reliable {
            let at=94+source.identity.kind.len()+source.identity.path.len();
            let mut corrupt=encoded.bytes().to_vec();corrupt[at..at+16].fill(0);
            assert!(PipelineSnapshot::decode(&corrupt,1024).is_err());
            let mut corrupt=encoded.bytes().to_vec();corrupt[at+16..at+24].fill(0);
            assert!(PipelineSnapshot::decode(&corrupt,1024).is_err());
        }
        let mut wrong=if reliable {CheckpointStore::open_pipeline_exclusive(&dir,1024,Default::default())}else{CheckpointStore::open_reliable_exclusive(&dir,1024,Default::default())}.unwrap();
        assert!(wrong.commit_prepared(&encoded).is_err());drop(wrong);assert!(!dir.join("CURRENT").exists());
        let mut correct=if reliable {CheckpointStore::open_reliable_exclusive(&dir,1024,Default::default())}else{CheckpointStore::open_pipeline_exclusive(&dir,1024,Default::default())}.unwrap();
        correct.commit_prepared(&encoded).unwrap();drop(correct);
        let before=std::fs::read(dir.join("CURRENT")).unwrap();
        let wrong=if reliable {CheckpointStore::open_pipeline_exclusive(&dir,1024,Default::default())}else{CheckpointStore::open_reliable_exclusive(&dir,1024,Default::default())};
        assert!(wrong.err().unwrap().message.contains("source profile mismatch"));
        assert_eq!(before,std::fs::read(dir.join("CURRENT")).unwrap());
        drop(encoded);assert_eq!(owner.usage().physical_bytes,0);std::fs::remove_dir_all(dir).unwrap();
    }
}

fn r11_encoded(
    freeze: &crate::WindowFreeze,
    owner: &Arc<MemoryOwner>,
) -> crate::barrier::EncodedFreeze {
    let mut bytes = Vec::new();
    crate::checkpoint::encode_freeze(freeze, &mut bytes, 1024).unwrap();
    let lease = owner
        .acquire(CreditKind::Reservation, bytes.capacity())
        .unwrap();
    crate::barrier::EncodedFreeze { bytes, lease, ext: false, buffered: false }
}

#[test]
fn r11_restore_rejects_wrong_keys_accumulators_and_bounds_before_input() {
    use crate::aggregate::Accumulator as A;
    let kernel = kernel();
    for case in 0..13 {
        let physical = plan(1, true);
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let mut bad = r11_freeze(10);
        match case {
            0 => bad.operator = 99.into(),
            1 => bad.slot = 9.into(),
            2 => bad.kind = 0,
            3 => bad.entries[0].key.clear(),
            4 => bad.entries[0].key[0] = Scalar::Int64(1),
            5 => bad.entries[0].key[0] = Scalar::Null,
            6 => bad.entries[0].accs.clear(),
            7 => bad.entries[0].count = 0,
            8 => bad.entries[0].count = 3,
            9 => {
                bad.entries[0].accs[0] = A::Count {
                    rows: 1,
                    non_null: 1,
                    star: true,
                }
            }
            10 => bad.entries[0].accs[0] = A::SumI64 { sum: 1, n: 2 },
            11 => {
                bad.entries[0].accs[0] = A::Min {
                    v: Some(Scalar::Null),
                }
            }
            _ => {
                bad.entries[0].accs[0] = A::Max {
                    v: Some(Scalar::Float64(f64::NAN)),
                }
            }
        }
        let PhysicalStage::WindowAgg {
            operator,
            spec,
            input,
            ..
        } = &physical.stages[2]
        else {
            unreachable!()
        };
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let op = crate::window::WindowOperator::new(
            *operator,
            spec.clone(),
            input.clone(),
            owner.clone(),
            1024,
            1024,
        )
        .unwrap();
        assert!(
            op.validate_participant_restore(&bad)
                .unwrap_err()
                .message
                .contains("schema/accumulators/bounds"),
            "case {case}"
        );
        let capture = SharedCapture::new();
        let before = kernel.process_owner().usage().physical_bytes;
        let request =
            JobRequest::new(physical, vec![row(99)], capture.clone()).with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore { buffered: Vec::new(),
                    sink: None,
                    iot: Vec::new(),
                    plan: manifest,
                    generation: [1; 16],
                    restore: Some(vec![bad]),
                }),
                acks: AlignedAcks::default(),
                outbox: Arc::new(InflightCounter::new()),
            });
        let error = kernel
            .submit(request)
            .err()
            .expect("invalid state must reject admission");
        assert!(
            error.message.contains(if case < 2 {
                "unknown or duplicate"
            } else {
                "schema/accumulators/bounds"
            }),
            "case {case}: {error}"
        );
        assert_eq!(capture.row_count(), 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.process_owner().usage().physical_bytes, before);
    }
}

#[test]
fn r11_restore_count_star_min_max_and_event_time_validation_branches() {
    use crate::aggregate::Accumulator as A;
    use sparrow_model::WindowKind;
    let PhysicalStage::WindowAgg {
        operator,
        mut spec,
        input,
        ..
    } = plan(1, true).stages.remove(2)
    else {
        unreachable!()
    };
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for func in [AggFn::Count, AggFn::Min, AggFn::Max] {
        spec.aggs = vec![AggCall::new(
            func,
            if func == AggFn::Count {
                None
            } else {
                Some(sparrow_expr::Expr::Column { name: "v".into() })
            },
            "s",
        )];
        let op = crate::window::WindowOperator::new(
            operator,
            spec.clone(),
            input.clone(),
            owner.clone(),
            1024,
            1024,
        )
        .unwrap();
        for case in 0..4 {
            let mut freeze = r11_freeze(10);
            freeze.entries[0].accs[0] = match func {
                AggFn::Count => match case {
                    0 => A::Count {
                        rows: 1,
                        non_null: 1,
                        star: true,
                    },
                    1 => A::Count {
                        rows: 1,
                        non_null: 1,
                        star: false,
                    },
                    2 => A::Count {
                        rows: 1,
                        non_null: 2,
                        star: true,
                    },
                    _ => A::Count {
                        rows: 2,
                        non_null: 1,
                        star: true,
                    },
                },
                _ => {
                    let v = Some(match case {
                        0 => Scalar::Int64(1),
                        1 => Scalar::Null,
                        2 => Scalar::utf8("wrong"),
                        _ => Scalar::Float64(f64::NAN),
                    });
                    if func == AggFn::Min {
                        A::Min { v }
                    } else {
                        A::Max { v }
                    }
                }
            };
            assert_eq!(
                op.validate_participant_restore(&freeze).is_ok(),
                case == 0,
                "{func:?}/{case}"
            );
        }
    }
    spec.aggs = vec![AggCall::new(
        AggFn::Sum,
        Some(sparrow_expr::Expr::Column { name: "v".into() }),
        "s",
    )];
    spec.event_time_field = Some("v".into());
    for hop in [false, true] {
        spec.kind = if hop {
            WindowKind::HoppingEventTime {
                size_micros: 10,
                slide_micros: 5,
            }
        } else {
            WindowKind::TumblingEventTime { size_micros: 10 }
        };
        let op = crate::window::WindowOperator::new(
            operator,
            spec.clone(),
            input.clone(),
            owner.clone(),
            1024,
            1024,
        )
        .unwrap();
        for case in 0..5 {
            let mut freeze = r11_freeze(10);
            freeze.kind = 0;
            freeze.entries[0].key.push(Scalar::Int64(0));
            freeze.entries[0].window_end = 10;
            match case {
                0 => {}
                1 => freeze.entries[0].window_end = 9,
                2 => {
                    freeze.entries[0].window_start = 1;
                    freeze.entries[0].window_end = 11;
                    freeze.entries[0].key[1] = Scalar::Int64(1);
                }
                3 => freeze.entries[0].key[1] = Scalar::Int64(5),
                _ => {
                    freeze.entries[0].window_start = i64::MAX;
                    freeze.entries[0].window_end = i64::MIN;
                }
            }
            assert_eq!(
                op.validate_participant_restore(&freeze).is_ok(),
                case == 0,
                "ET {hop}/{case}"
            );
        }
    }
}

#[test]
fn r11_restore_duplicate_unknown_legacy_combination_and_generation_zero_reject() {
    let kernel = kernel();
    for case in 0..4 {
        let physical = plan(2, true);
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let mut states = vec![r11_freeze(10), r11_freeze(11)];
        match case {
            0 => states[1].operator = 10.into(),
            1 => states[1].operator = 99.into(),
            _ => {}
        }
        let before = kernel.process_owner().usage().physical_bytes;
        let result = kernel.submit(
            JobRequest::new(physical, vec![row(9)], SharedCapture::new()).with_aligned(
                AlignedJob {
                    restore: if case == 2 {
                        Some(r11_freeze(10))
                    } else {
                        None
                    },
                    pipeline: Some(PipelineRestore { buffered: Vec::new(),
                        sink: None,
                        iot: Vec::new(),
                        plan: manifest,
                        generation: if case == 3 { [0; 16] } else { [1; 16] },
                        restore: Some(states),
                    }),
                    acks: AlignedAcks::default(),
                    outbox: Arc::new(InflightCounter::new()),
                },
            ),
        );
        let error = result.err().expect("admission refused");
        assert!(
            error.message.contains(match case {
                0 | 1 => "unknown or duplicate",
                2 => "cannot combine",
                _ => "generation",
            }),
            "{error}"
        );
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.process_owner().usage().physical_bytes, before);
    }
}

#[test]
fn r11_state_ack_duplicates_conflicts_and_single_flight_are_explicit() {
    let kernel = kernel();
    kernel.block_on(async {
        for conflict in [false, true] {
            let registry = AlignedAcks::default();
            registry
                .configure(
                    Arc::new(CheckpointPlan::from_physical(&plan(1, true)).unwrap()),
                    7,
                    [7; 16],
                )
                .unwrap();
            let request = registry.begin(1).unwrap();
            assert!(registry
                .begin(2)
                .err()
                .unwrap()
                .message
                .contains("already active"));
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let f = r11_freeze(10);
            let mut other = f.clone();
            if conflict {
                other.entries[0].accs[0] = crate::aggregate::Accumulator::SumI64 { sum: 99, n: 1 };
            }
            let first = r11_encoded(&f, &owner);
            let second = r11_encoded(&other, &owner);
            let sender = registry.clone();
            let sending = tokio::spawn(async move {
                sender.source_cut(1).await;
                sender.state_frozen(1, 10.into(), Ok(first)).await;
                sender.state_frozen(1, 10.into(), Ok(second)).await;
                sender
                    .sink_flushed(
                        1,
                        crate::FlushOutcome {
                            ok: true,
                            dropped: 0,
                            timed_out: false,
                        },
                    )
                    .await;
            });
            let result = request.wait_participants(Duration::from_secs(1)).await;
            if conflict {
                assert!(result
                    .unwrap_err()
                    .message
                    .contains("conflicting repeated participant freeze"));
            } else {
                assert_eq!(result.unwrap().state_count(), 1);
            }
            sending.await.unwrap();
            assert!(!registry.is_active(1));
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    });
}

#[test]
fn r11_compatibility_last_state_prefix_fusion_revision_and_legacy_contract() {
    for windows in 0..=2 {
        let original = plan(windows, true);
        let layout = CheckpointPlan::from_physical(&original).unwrap();
        // Emulate the PRE-R11 manifest reader: its grammar accepts the entire
        // current manifest, including opaque CP01 bytes, then strict semantics
        // rejects restore. A format error here would permit Store fallback.
        let wire = layout.encode().unwrap();
        assert_eq!(
            wire.capacity(),
            wire.len(),
            "manifest size is reserved exactly under the metadata lease"
        );
        assert_eq!(&wire[..4], b"CPL1");
        let semantic_offset = 14 + layout.states.len() * 11;
        let semantic_len = u32::from_le_bytes(
            wire[semantic_offset..semantic_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            semantic_offset + 4 + semantic_len,
            wire.len(),
            "no new outer fields"
        );
        let mut old_reader = layout.clone();
        old_reader.recovery_prefix_len = None;
        old_reader.semantics = wire[semantic_offset + 4..].to_vec();
        old_reader.validate().unwrap();
        assert!(
            old_reader.check_compatible(&layout).is_err(),
            "old binary must reject current, not scan older generations"
        );
        let mut changed = original.clone();
        changed.revision = 99.into();
        let schema = match changed.stages.last().unwrap() {
            PhysicalStage::CaptureSink { schema, .. } => schema.clone(),
            _ => unreachable!(),
        };
        changed.stages.insert(
            changed.stages.len() - 1,
            PhysicalStage::Transform {
                steps: vec![TransformStep::Filter {
                    operator: 30.into(),
                    predicate: sparrow_expr::Expr::Literal(Scalar::Bool(false)),
                    input: schema,
                }],
            },
        );
        let new = CheckpointPlan::from_physical(&changed).unwrap();
        layout.check_compatible(&new).unwrap();
        assert_ne!(layout.semantics, new.semantics);
        let mut legacy = layout.clone();
        legacy.recovery_prefix_len = None;
        let legacy = CheckpointPlan::decode(&legacy.encode().unwrap()).unwrap();
        legacy.check_compatible(&layout).unwrap();
        assert!(legacy.check_compatible(&new).is_err());
        if windows == 0 {
            let PhysicalStage::Transform { steps: tail } = changed.stages.remove(2) else {
                unreachable!()
            };
            let PhysicalStage::Transform { steps } = &mut changed.stages[1] else {
                unreachable!()
            };
            steps.extend(tail);
            assert_eq!(
                CheckpointPlan::from_physical(&changed).unwrap(),
                new,
                "fusion grouping is not an identity"
            );
        }
    }
    let mut physical = plan(2, true);
    let original = CheckpointPlan::from_physical(&physical).unwrap();
    let PhysicalStage::WindowAgg { input, .. } = &physical.stages[3] else {
        unreachable!()
    };
    let input = input.clone();
    physical.stages.insert(
        3,
        PhysicalStage::Transform {
            steps: vec![TransformStep::Filter {
                operator: 30.into(),
                predicate: sparrow_expr::Expr::Literal(Scalar::Bool(false)),
                input,
            }],
        },
    );
    assert!(
        original
            .check_compatible(&CheckpointPlan::from_physical(&physical).unwrap())
            .is_err(),
        "between windows is upstream of W2"
    );
}

#[test]
fn r11_manifest_operator_limit_and_mixed_time_policy_reject() {
    let mut physical = plan(0, true);
    let PhysicalStage::Transform { steps } = &mut physical.stages[1] else {
        unreachable!()
    };
    let input = match &steps[0] {
        TransformStep::Filter { input, .. } => input.clone(),
        _ => unreachable!(),
    };
    for id in 100..161 {
        steps.push(TransformStep::Filter {
            operator: id.into(),
            predicate: sparrow_expr::Expr::Literal(Scalar::Bool(true)),
            input: input.clone(),
        });
    }
    CheckpointPlan::from_physical(&physical).unwrap();
    let PhysicalStage::Transform { steps } = &mut physical.stages[1] else {
        unreachable!()
    };
    steps.push(TransformStep::Filter {
        operator: 999.into(),
        predicate: sparrow_expr::Expr::Literal(Scalar::Bool(true)),
        input,
    });
    assert!(CheckpointPlan::from_physical(&physical)
        .unwrap_err()
        .message
        .contains("operator limit"));
    let mut manifest = CheckpointPlan::from_physical(&plan(2, true)).unwrap();
    manifest.states[1].window_kind = 2;
    assert!(manifest
        .validate()
        .unwrap_err()
        .message
        .contains("multi-state time policy"));
}

#[test]
fn r11_legacy_store_guard_readonly_generation_and_owner_isolation() {
    let physical = plan(1, true);
    let dir = tmp();
    let mut store = CheckpointStore::open(&dir).unwrap();
    let snapshot = crate::CheckpointSnapshot {
        checkpoint_id: 1,
        source: sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory(
            "fixture", 0, 0,
        )),
        window: r11_freeze(10),
        ingested_rows: 1,
        layout: sparrow_plan::PlanLayout::from_physical(&physical).unwrap(),
        table: None,
    };
    store.commit(&snapshot).unwrap();
    let current = std::fs::read(dir.join("CURRENT")).unwrap();
    assert!(store
        .recover_pipeline_required()
        .unwrap_err()
        .message
        .contains("legacy single-window snapshot"));
    drop(store);
    let error = CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default())
        .err()
        .unwrap();
    assert!(error.message.contains("refusing K1 writes"));
    assert_eq!(std::fs::read(dir.join("CURRENT")).unwrap(), current);
    assert!(!dir.join("STATE_GENERATION").exists());
    let reader = CheckpointStore::open_readonly(&dir).unwrap();
    assert!(reader.activate_state_generation([1; 16]).is_err());
    drop(reader);
    let empty = tmp();
    let store = CheckpointStore::open_pipeline_exclusive(&empty, 1024, Default::default()).unwrap();
    assert!(store.activate_state_generation([0; 16]).is_err());
    store.activate_state_generation([1; 16]).unwrap();
    std::fs::create_dir(empty.join("STATE_GENERATION.tmp")).unwrap();
    store.activate_state_generation([1; 16]).unwrap(); // Same durable marker is not rewritten.
    assert!(store.activate_state_generation([2; 16]).is_err());
    assert_eq!(
        store.inventory().unwrap().state_generation_marker,
        Some([1; 16])
    );
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let other = MemoryOwner::new(ResourceBudget::compact());
    let result = PipelineSnapshot::encode_frozen(
        2,
        &snapshot.source,
        1,
        1,
        &CheckpointPlan::from_physical(&physical).unwrap(),
        crate::ParticipantAcks {
            attempt: 1,
            generation: [1; 16],
            freezes: vec![r11_encoded(&r11_freeze(10), &other)],
            next_output: None,
        },
        &owner,
        1024,
    );
    assert!(result
        .err()
        .unwrap()
        .message
        .contains("different Job memory owner"));
    assert_eq!(other.usage().physical_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(empty).unwrap();
}

#[test]
fn k1_real_kernel_zero_single_double_windows_recover_every_cut_against_raw_input() {
    for windows in 0..=2 {
        for keep in [false, true] {
            for cut in [0, 1, 5, 6, 12] {
                let kernel = kernel();
                kernel.block_on(async {
                    let physical = plan(windows, keep);
                    let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
                    let before = SharedCapture::new();
                    let (tx, rx) = sparrow_io::observed::channel(2);
                    let acks = AlignedAcks::default();
                    let handle = kernel
                        .submit(
                            JobRequest::new(physical.clone(), vec![], before.clone())
                                .with_live_events(rx)
                                .with_aligned(AlignedJob {
                                    restore: None,
                                    pipeline: Some(PipelineRestore { buffered: Vec::new(),
                                        sink: None,
                                        iot: Vec::new(),
                                        plan: manifest.clone(),
                                        generation: [42; 16],
                                        restore: None,
                                    }),
                                    acks: acks.clone(),
                                    outbox: Arc::new(InflightCounter::new()),
                                }),
                        )
                        .unwrap();
                    let owner = handle.memory_owner();
                    let request = acks.begin(1).unwrap();
                    for v in 1..=cut {
                        tx.send(IngressEvent::Row(row(v))).await.unwrap();
                    }
                    tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                        checkpoint_id: 1,
                    }))
                    .await
                    .unwrap();
                    let aligned = request
                        .wait_participants(Duration::from_secs(2))
                        .await
                        .unwrap();
                    let source = sparrow_io::SourcePosition {
                        offset_bytes: cut as u64,
                        record_index: cut as u64,
                        identity: sparrow_io::SourceIdentity::memory("k1", 12, 7),
                    };
                    let encoded = PipelineSnapshot::encode_frozen(
                        1,
                        &source,
                        cut as u64,
                        1,
                        &manifest,
                        aligned,
                        &owner,
                        kernel.job_budget().max_state_keys,
                    )
                    .unwrap();
                    let dir = tmp();
                    let mut store = CheckpointStore::open(&dir).unwrap();
                    store.commit_prepared(&encoded).unwrap();
                    let snapshot = store.recover_pipeline_required().unwrap();
                    snapshot.check_compatible(&manifest).unwrap();
                    assert_eq!(snapshot.windows.len(), windows);
                    assert_eq!(snapshot.source.record_index, cut as u64);
                    assert!(
                        store.recover_required().is_err(),
                        "legacy reader must not silently convert K1 snapshots"
                    );
                    if windows == 2 && keep && cut == 5 {
                        assert_eq!(
                            snapshot
                                .windows
                                .iter()
                                .map(|w| w.entries[0].count)
                                .collect::<Vec<_>>(),
                            vec![2, 1]
                        );
                        assert_eq!(
                            snapshot
                                .windows
                                .iter()
                                .map(|w| w.entries[0].accs[0].finish())
                                .collect::<Vec<_>>(),
                            vec![Scalar::Int64(9), Scalar::Int64(6)]
                        );
                        assert_eq!(PipelineSnapshot::decode(encoded.bytes(),1).unwrap().windows.len(),2,
                            "max_state_keys is per operator; shared byte quotas must not halve legal key capacity");
                        for end in 0..encoded.bytes().len() {
                            assert!(
                                PipelineSnapshot::decode(&encoded.bytes()[..end], 1024).is_err()
                            );
                            assert!(PipelineSnapshot::decode_mode(
                                &encoded.bytes()[..end],
                                1024,
                                false
                            )
                            .is_err());
                        }
                        let validated =
                            PipelineSnapshot::decode_mode(encoded.bytes(), 1024, false).unwrap();
                        assert!(validated.windows.is_empty());
                        let mut bad = encoded.bytes().to_vec();
                        bad[6..14].copy_from_slice(&2u64.to_le_bytes());
                        let pos = bad.windows(4).position(|s| s == b"CPL1").unwrap();
                        bad[pos + 22..pos + 24].copy_from_slice(&99u16.to_le_bytes());
                        assert!(
                            store.commit_encoded(2, &bad).is_err(),
                            "unknown codec cannot publish CURRENT"
                        );
                        assert_eq!(store.recover_pipeline_required().unwrap().checkpoint_id, 1);
                        let mut next = encoded.bytes().to_vec();
                        next[6..14].copy_from_slice(&2u64.to_le_bytes());
                        store.fault.point = crate::FaultPoint::AfterManifestRename;
                        assert!(store.commit_encoded(2, &next).is_err());
                        assert_eq!(store.recover_pipeline_required().unwrap().checkpoint_id, 1);
                        assert!(
                            store.recover_pipeline_id(2).is_err(),
                            "unpublished generation cannot be selected"
                        );
                    }
                    drop(encoded);
                    handle.cancel();
                    drop(tx);
                    handle.wait().await.unwrap();
                    assert_eq!(owner.usage().physical_bytes, 0);
                    let after = SharedCapture::new();
                    let acks = AlignedAcks::default();
                    let handle = kernel
                        .submit(
                            JobRequest::new(
                                physical,
                                (cut + 1..=12).map(row).collect(),
                                after.clone(),
                            )
                            .with_aligned(AlignedJob {
                                restore: None,
                                pipeline: Some(PipelineRestore { buffered: Vec::new(),
                                    sink: None,
                                    iot: Vec::new(),
                                    plan: manifest,
                                    generation: snapshot.generation,
                                    restore: Some(snapshot.windows),
                                }),
                                acks,
                                outbox: Arc::new(InflightCounter::new()),
                            }),
                        )
                        .unwrap();
                    let owner = handle.memory_owner();
                    handle.wait().await.unwrap();
                    let mut got = values(&before);
                    got.extend(values(&after));
                    let raw: Vec<i64> = if keep { (1..=12).collect() } else { vec![] };
                    let expected = match windows {
                        0 => raw,
                        1 => raw
                            .chunks_exact(3)
                            .map(|chunk| chunk.iter().sum())
                            .collect(),
                        _ => raw
                            .chunks_exact(6)
                            .map(|chunk| chunk.iter().sum())
                            .collect(),
                    };
                    assert_eq!(got, expected, "windows={windows} keep={keep} cut={cut}");
                    assert_eq!(owner.usage().physical_bytes, 0);
                    assert_eq!(kernel.admitted_jobs(), 0);
                    std::fs::remove_dir_all(dir).unwrap();
                });
            }
        }
    }
}

#[test]
fn r11_pipeline_decode_identity_and_multichunk_metadata_are_bounded() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let manifest = CheckpointPlan::from_physical(&plan(2, true)).unwrap();
    let mut source =
        sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("r11", 0, 0));
    source.identity.path = "x".repeat(9000);
    let encoded = PipelineSnapshot::encode_frozen(
        1,
        &source,
        0,
        77,
        &manifest,
        crate::ParticipantAcks {
            attempt: 9,
            generation: [3; 16],
            next_output: None,
            freezes: vec![
                r11_encoded(&r11_freeze(10), &owner),
                r11_encoded(&r11_freeze(11), &owner),
            ],
        },
        &owner,
        1024,
    )
    .unwrap();
    let pos = encoded
        .bytes()
        .windows(4)
        .position(|s| s == b"CPL1")
        .unwrap()
        + manifest.encode().unwrap().len()
        + 2;
    let first_size = u32::from_le_bytes(encoded.bytes()[pos..pos + 4].try_into().unwrap()) as usize;
    for (offset, operator) in [
        (pos + 4, 99u32),
        (pos + 4, 11),
        (pos + 4 + first_size + 4, 10),
    ] {
        let mut bad = encoded.bytes().to_vec();
        bad[offset..offset + 4].copy_from_slice(&operator.to_le_bytes());
        for materialize in [false, true] {
            assert!(PipelineSnapshot::decode_mode(&bad, 1024, materialize)
                .unwrap_err()
                .message
                .contains("duplicate/unknown/misordered"));
        }
    }
    let dir = tmp();
    let mut store =
        CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default()).unwrap();
    store.activate_state_generation([3; 16]).unwrap();
    store.commit_prepared(&encoded).unwrap();
    let inventory = store.inventory().unwrap();
    let metadata = inventory.generations[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.revision, Some(77));
    assert_eq!(metadata.attempt, Some(9));
    assert_eq!(metadata.generation, Some([3; 16]));
    assert_eq!(inventory.state_generation_marker, Some([3; 16]));
    assert!(inventory.marker_error.is_none());
    assert_eq!(
        store
            .recover_pipeline_required()
            .unwrap()
            .source
            .identity
            .path,
        source.identity.path
    );
    drop(encoded);
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn r11_mixed_legacy_pipeline_history_is_refused_without_pruning_or_fallback() {
    let physical = plan(1, true);
    let dir = tmp();
    let mut store = CheckpointStore::open(&dir).unwrap();
    let source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("r11", 0, 0));
    store
        .commit(&crate::CheckpointSnapshot {
            checkpoint_id: 1,
            source: source.clone(),
            window: r11_freeze(10),
            ingested_rows: 1,
            layout: sparrow_plan::PlanLayout::from_physical(&physical).unwrap(),
            table: None,
        })
        .unwrap();
    // Deliberately simulate a directory made by the PRE-R11 generic writer.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let encoded = PipelineSnapshot::encode_frozen(
        2,
        &source,
        1,
        2,
        &CheckpointPlan::from_physical(&physical).unwrap(),
        crate::ParticipantAcks {
            attempt: 2,
            generation: [2; 16],
            next_output: None,
            freezes: vec![r11_encoded(&r11_freeze(10), &owner)],
        },
        &owner,
        1024,
    )
    .unwrap();
    store.commit_prepared(&encoded).unwrap();
    assert_eq!(store.recover_pipeline_required().unwrap().checkpoint_id, 2);
    assert!(store
        .recover_required()
        .unwrap_err()
        .message
        .contains("participant-aware"));
    let inventory = store.inventory().unwrap();
    assert_eq!(inventory.generations.len(), 2);
    assert_eq!(
        inventory
            .generations
            .iter()
            .map(|g| g.metadata.as_ref().unwrap().version)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let current = std::fs::read(dir.join("CURRENT")).unwrap();
    drop(store);
    assert!(
        CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default())
            .err()
            .unwrap()
            .message
            .contains("refusing K1 writes")
    );
    assert_eq!(std::fs::read(dir.join("CURRENT")).unwrap(), current);
    assert!(dir.join("chk-00000001/PUBLISHED").exists());
    assert!(dir.join("chk-00000002/PUBLISHED").exists());
    drop(encoded);
    assert_eq!(owner.usage().physical_bytes, 0);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn r11_event_time_restore_preserves_holdback_and_rejects_closed_late_rows() {
    use sparrow_model::{InputId, WindowKind};
    for hop in [false, true] {
        let PhysicalStage::WindowAgg {
            operator,
            mut spec,
            input,
            ..
        } = plan(1, true).stages.remove(2)
        else {
            unreachable!()
        };
        spec.kind = if hop {
            WindowKind::HoppingEventTime {
                size_micros: 10,
                slide_micros: 5,
            }
        } else {
            WindowKind::TumblingEventTime { size_micros: 10 }
        };
        spec.event_time_field = Some("v".into());
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let batch = |rows: Vec<Row>| {
            let mut builder = sparrow_model::RowBatchBuilder::new(
                Arc::new(input.clone()),
                owner.clone(),
                CreditKind::Reservation,
                rows.len(),
                65536,
            )
            .unwrap();
            for row in rows {
                builder.push(row).unwrap();
            }
            builder.finish().unwrap()
        };
        let mut original = crate::window::WindowOperator::new(
            operator,
            spec.clone(),
            input.clone(),
            owner.clone(),
            1024,
            1024,
        )
        .unwrap();
        let emission = original
            .on_batch(&batch(vec![row(11), row(12)]), 100)
            .unwrap();
        original.materialize_emission(emission).unwrap();
        original.observe_watermark(InputId::SINGLE, 10).unwrap();
        let freeze = original.freeze();
        // Event ingestion already advanced the binding to 12; a later lower
        // explicit watermark cannot rewind it.
        assert_eq!(freeze.wm_in, Some(12));
        assert_eq!(freeze.wm_out, Some(12));
        assert_eq!(freeze.last_effective, Some(12));
        let mut restored = crate::window::WindowOperator::new(
            operator,
            spec,
            input.clone(),
            owner.clone(),
            1024,
            1024,
        )
        .unwrap();
        restored.validate_participant_restore(&freeze).unwrap();
        restored.restore_freeze(&freeze).unwrap();
        assert_eq!(restored.freeze(), freeze);
        assert_eq!(restored.live_timers(), original.live_timers());
        let late = restored.on_batch(&batch(vec![row(2)]), 100).unwrap();
        assert!(late.finals.is_empty());
        assert!(!late.lates.is_empty());
        assert_eq!(
            restored.freeze(),
            freeze,
            "late rows must not recreate closed state"
        );
        let expected = original.observe_watermark(InputId::SINGLE, 30).unwrap();
        let expected = original.materialize_emission(expected).unwrap();
        let got = restored.observe_watermark(InputId::SINGLE, 30).unwrap();
        let got = restored.materialize_emission(got).unwrap();
        assert_eq!(got.finals, expected.finals);
        drop(original);
        drop(restored);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[test]
fn k1_manifest_identity_semantics_and_fusion_are_checked() {
    for windows in 0..=2 {
        let original = plan(windows, true);
        let layout = CheckpointPlan::from_physical(&original).unwrap();
        assert_eq!(
            CheckpointPlan::decode(&layout.encode().unwrap()).unwrap(),
            layout
        );
        assert_eq!(layout.participants().len(), windows + 2);
        let changed = CheckpointPlan::from_physical(&plan(windows, false)).unwrap();
        assert_eq!(layout.check_compatible(&changed).is_err(), windows > 0);
        let mut duplicate = original.clone();
        if let PhysicalStage::CaptureSink { operator, .. } = duplicate.stages.last_mut().unwrap() {
            *operator = 1.into();
        }
        assert!(CheckpointPlan::from_physical(&duplicate).is_err());
        let bytes = layout.encode().unwrap();
        for end in 0..bytes.len() {
            assert!(CheckpointPlan::decode(&bytes[..end]).is_err());
        }
        let mut unsupported = layout.clone();
        if let Some(state) = unsupported.states.first_mut() {
            state.codec = 99;
            assert!(unsupported.validate().is_err());
        }
    }
    let mut too_many = plan(3, true);
    assert!(CheckpointPlan::from_physical(&too_many).is_err());
    too_many.stages.swap(0, 1);
    assert!(CheckpointPlan::from_physical(&too_many).is_err());
}

#[test]
fn k1_partial_restore_rejected_before_input_and_failed_preparation_refunds_all() {
    let kernel = kernel();
    let physical = plan(2, true);
    let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
    for bad in [
        vec![],
        vec![crate::WindowFreeze {
            operator: 10.into(),
            slot: 1.into(),
            kind: 1,
            entries: vec![],
            wm_in: None,
            wm_out: None,
            last_effective: None,
        }],
    ] {
        let capture = SharedCapture::new();
        let before = kernel.process_owner().usage().physical_bytes;
        let request = JobRequest::new(physical.clone(), vec![row(1)], capture.clone())
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore { buffered: Vec::new(),
                    sink: None,
                    iot: Vec::new(),
                    plan: manifest.clone(),
                    generation: [42; 16],
                    restore: Some(bad),
                }),
                acks: AlignedAcks::default(),
                outbox: Arc::new(InflightCounter::new()),
            });
        assert!(kernel.submit(request).is_err());
        assert_eq!(capture.row_count(), 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.process_owner().usage().physical_bytes, before);
    }
    // Encoded data quota is a shared Job bound, not one bound per participant.
    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 512,
        ..ResourceBudget::compact()
    });
    let held = owner.acquire(CreditKind::Reservation, 500).unwrap();
    assert!(PipelineSnapshot::encode_frozen(
        1,
        &sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("k1", 0, 0)),
        0,
        1,
        &CheckpointPlan::from_physical(&plan(0, true)).unwrap(),
        crate::ParticipantAcks {
            attempt: 1,
            generation: [42; 16],
            freezes: vec![],
            next_output: None,
        },
        &owner,
        1024
    )
    .is_err());
    drop(held);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn k1_single_et_tumble_hop_restore_watermark_and_state_against_raw_buckets() {
    use sparrow_model::WindowKind;
    for hop in [false, true] {
        let kernel = kernel();
        kernel.block_on(async {
            let mut physical = plan(1, true);
            let PhysicalStage::WindowAgg {
                spec,
                input,
                output,
                ..
            } = &mut physical.stages[2]
            else {
                unreachable!()
            };
            spec.kind = if hop {
                WindowKind::HoppingEventTime {
                    size_micros: 10,
                    slide_micros: 5,
                }
            } else {
                WindowKind::TumblingEventTime { size_micros: 10 }
            };
            spec.event_time_field = Some("v".into());
            *output = sparrow_plan::window_output_schema(input, spec).unwrap();
            let end = output.clone();
            if let PhysicalStage::CaptureSink { schema, .. } = physical.stages.last_mut().unwrap() {
                *schema = end;
            }
            let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
            let capture = SharedCapture::new();
            let acks = AlignedAcks::default();
            let (tx, rx) = sparrow_io::observed::channel(4);
            let job = kernel
                .submit(
                    JobRequest::new(physical.clone(), vec![], capture.clone())
                        .with_live_events(rx)
                        .with_aligned(AlignedJob {
                            restore: None,
                            pipeline: Some(PipelineRestore { buffered: Vec::new(),
                                sink: None,
                                iot: Vec::new(),
                                plan: manifest.clone(),
                                generation: [42; 16],
                                restore: None,
                            }),
                            acks: acks.clone(),
                            outbox: Arc::new(InflightCounter::new()),
                        }),
                )
                .unwrap();
            let owner = job.memory_owner();
            let request = acks.begin(1).unwrap();
            for v in 1..=5 {
                tx.send(IngressEvent::Row(row(v))).await.unwrap();
            }
            tx.send(IngressEvent::Control(StreamControl::Watermark {
                input: 0,
                wm_micros: 5,
            }))
            .await
            .unwrap();
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: 1,
            }))
            .await
            .unwrap();
            let aligned = request
                .wait_participants(Duration::from_secs(2))
                .await
                .unwrap();
            let encoded = PipelineSnapshot::encode_frozen(
                1,
                &sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("et", 12, 0)),
                5,
                1,
                &manifest,
                aligned,
                &owner,
                1024,
            )
            .unwrap();
            let snapshot = PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap();
            assert_eq!(snapshot.windows[0].wm_in, Some(5));
            drop(encoded);
            job.cancel();
            drop(tx);
            job.wait().await.unwrap();
            assert_eq!(owner.usage().physical_bytes, 0);
            let after = SharedCapture::new();
            let job = kernel
                .submit(
                    JobRequest::new(physical, (6..=12).map(row).collect(), after.clone())
                        .with_controls(vec![StreamControl::Watermark {
                            input: 0,
                            wm_micros: 20,
                        }])
                        .with_aligned(AlignedJob {
                            restore: None,
                            pipeline: Some(PipelineRestore { buffered: Vec::new(),
                                sink: None,
                                iot: Vec::new(),
                                plan: manifest,
                                generation: snapshot.generation,
                                restore: Some(snapshot.windows),
                            }),
                            acks: AlignedAcks::default(),
                            outbox: Arc::new(InflightCounter::new()),
                        }),
                )
                .unwrap();
            let owner = job.memory_owner();
            job.wait().await.unwrap();
            let mut got = values(&capture);
            got.extend(values(&after));
            let starts = if hop { vec![-5, 0, 5, 10] } else { vec![0, 10] };
            let expected: Vec<i64> = starts
                .into_iter()
                .map(|s| (1..=12).filter(|v| *v >= s && *v < s + 10).sum())
                .collect();
            assert_eq!(got, expected, "hop={hop}");
            assert_eq!(owner.usage().physical_bytes, 0);
        });
    }
}

#[test]
fn k1_two_windows_checkpoint_waits_for_real_slow_sink_and_cancellation_releases_acks() {
    for cancel in [false, true] {
        let kernel = kernel();
        kernel.block_on(async {
            let physical = plan(2, true);
            let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
            let acks = AlignedAcks::default();
            let outbox = Arc::new(InflightCounter::new());
            let (tx, rx) = sparrow_io::observed::channel(2);
            let (out, mut received) = sparrow_io::observed::channel(1);
            let job = kernel
                .submit(
                    JobRequest::new(physical, vec![], SharedCapture::disabled())
                        .with_live_events(rx)
                        .with_live_out(out)
                        .with_aligned(AlignedJob {
                            restore: None,
                            pipeline: Some(PipelineRestore { buffered: Vec::new(),
                                sink: None,
                                iot: Vec::new(),
                                plan: manifest,
                                generation: [42; 16],
                                restore: None,
                            }),
                            acks: acks.clone(),
                            outbox: outbox.clone(),
                        }),
                )
                .unwrap();
            let owner = job.memory_owner();
            let request = acks.begin(1).unwrap();
            for v in 1..=6 {
                tx.send(IngressEvent::Row(row(v))).await.unwrap();
            }
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: 1,
            }))
            .await
            .unwrap();
            let batch = received.recv().await.unwrap();
            assert_eq!(outbox.pending(), 1);
            let mut waiting = tokio::spawn(request.wait_participants(Duration::from_secs(2)));
            assert!(
                tokio::time::timeout(Duration::from_millis(30), &mut waiting)
                    .await
                    .is_err(),
                "state ACKs alone must not commit"
            );
            if cancel {
                waiting.abort();
                assert!(waiting.await.unwrap_err().is_cancelled());
                outbox.fail();
            } else {
                outbox.ack();
                let result = waiting.await.unwrap().unwrap();
                assert_eq!(result.state_count(), 2);
                drop(result);
            }
            assert!(!acks.is_active(1));
            drop(batch);
            job.cancel();
            drop(tx);
            drop(received);
            job.wait().await.unwrap();
            assert_eq!(owner.usage().physical_bytes, 0);
        });
    }
}

#[test]
fn k1_partial_freeze_budget_failure_is_retryable_without_job_loss_or_leases() {
    let kernel = kernel();
    kernel.block_on(async {
        let physical = plan(2, true);
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let acks = AlignedAcks::default();
        let (tx, rx) = sparrow_io::observed::channel(2);
        let job = kernel
            .submit(
                JobRequest::new(physical, vec![], SharedCapture::disabled())
                    .with_live_events(rx)
                    .with_aligned(AlignedJob {
                        restore: None,
                        pipeline: Some(PipelineRestore { buffered: Vec::new(),
                            sink: None,
                            iot: Vec::new(),
                            plan: manifest,
                            generation: [42; 16],
                            restore: None,
                        }),
                        acks: acks.clone(),
                        outbox: Arc::new(InflightCounter::new()),
                    }),
            )
            .unwrap();
        let owner = job.memory_owner();
        for v in 1..=5 {
            tx.send(IngressEvent::Row(row(v))).await.unwrap();
        }
        let first = acks.begin(1).unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 1,
        }))
        .await
        .unwrap();
        drop(
            first
                .wait_participants(Duration::from_secs(2))
                .await
                .unwrap(),
        );
        let usage = owner.usage().reservation_bytes;
        let pressure = owner
            .acquire(
                CreditKind::Reservation,
                owner.budget().reservation_bytes - usage - 512,
            )
            .unwrap();
        let second = acks.begin(2).unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 2,
        }))
        .await
        .unwrap();
        let error = second
            .wait_participants(Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(error.code, sparrow_model::ErrorCode::ResourceExhausted);
        assert!(
            error.message.contains("credits exhausted"),
            "must be freeze pressure, not timeout: {error}"
        );
        tokio::time::timeout(Duration::from_secs(2),async {
            while owner.usage().reservation_bytes!=usage+pressure.bytes() {tokio::task::yield_now().await;}
        }).await.expect("in-flight sibling freezes must release after abandoned checkpoint, not only on job exit");
        drop(pressure);
        let third = acks.begin(3).unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 3,
        }))
        .await
        .unwrap();
        let complete = third
            .wait_participants(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(complete.state_count(), 2);
        drop(complete);
        job.cancel();
        drop(tx);
        job.wait().await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);
    });
}

#[test]
fn k1_two_full_keyspaces_share_bytes_without_halving_per_operator_cardinality() {
    const KEYS: usize = 1024;
    let kernel = kernel();
    kernel.block_on(async {
        let physical = plan(2, true);
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let acks = AlignedAcks::default();
        let (tx, rx) = sparrow_io::observed::channel(8);
        let job = kernel
            .submit(
                JobRequest::new(physical.clone(), vec![], SharedCapture::disabled())
                    .with_live_events(rx)
                    .with_aligned(AlignedJob {
                        restore: None,
                        pipeline: Some(PipelineRestore { buffered: Vec::new(),
                            sink: None,
                            iot: Vec::new(),
                            plan: manifest.clone(),
                            generation: [42; 16],
                            restore: None,
                        }),
                        acks: acks.clone(),
                        outbox: Arc::new(InflightCounter::new()),
                    }),
            )
            .unwrap();
        let owner = job.memory_owner();
        for key in 0..KEYS {
            for v in 1..=4 {
                tx.send(IngressEvent::Row(Row {
                    values: vec![Scalar::utf8(format!("d{key}")), Scalar::Int64(v)],
                }))
                .await
                .unwrap();
            }
        }
        let request = acks.begin(1).unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 1,
        }))
        .await
        .unwrap();
        let frozen = request
            .wait_participants(Duration::from_secs(3))
            .await
            .unwrap();
        let encoded = PipelineSnapshot::encode_frozen(
            1,
            &sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory(
                "full-keys",
                4096,
                0,
            )),
            4096,
            1,
            &manifest,
            frozen,
            &owner,
            KEYS,
        )
        .unwrap();
        let snapshot = PipelineSnapshot::decode(encoded.bytes(), KEYS).unwrap();
        assert!(PipelineSnapshot::decode(encoded.bytes(), KEYS - 1).is_err());
        assert!(snapshot.windows.iter().all(|w| w.entries.len() == KEYS));
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.set_max_state_keys(KEYS);
        store.commit_prepared(&encoded).unwrap();
        assert_eq!(store.recover_pipeline_required().unwrap().windows.len(), 2);
        drop(encoded);
        job.cancel();
        drop(tx);
        job.wait().await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);
        let capture = SharedCapture::new();
        let rows = (0..KEYS)
            .flat_map(|key| {
                (5..=6).map(move |v| Row {
                    values: vec![Scalar::utf8(format!("d{key}")), Scalar::Int64(v)],
                })
            })
            .collect();
        let job = kernel
            .submit(
                JobRequest::new(physical, rows, capture.clone()).with_aligned(AlignedJob {
                    restore: None,
                    pipeline: Some(PipelineRestore { buffered: Vec::new(),
                        sink: None,
                        iot: Vec::new(),
                        plan: manifest,
                        generation: snapshot.generation,
                        restore: Some(snapshot.windows),
                    }),
                    acks: AlignedAcks::default(),
                    outbox: Arc::new(InflightCounter::new()),
                }),
            )
            .unwrap();
        let owner = job.memory_owner();
        job.wait().await.unwrap();
        assert_eq!(capture.row_count(), KEYS);
        let mut keys = std::collections::BTreeSet::new();
        for row in capture.rows() {
            assert_eq!(row.last(), Some(&Scalar::Int64(21)));
            keys.insert(format!("{:?}", row[0]));
        }
        assert_eq!(keys.len(), KEYS);
        assert_eq!(owner.usage().physical_bytes, 0);
        std::fs::remove_dir_all(dir).unwrap();
    });
}

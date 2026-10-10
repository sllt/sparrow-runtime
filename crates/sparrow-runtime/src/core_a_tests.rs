//! Core-A reliable JetStream + TTL=0 IoT envelope coverage.
//!
//! These tests intentionally stop at the runtime boundary.  The control-plane
//! JetStream admission and broker/source actor are covered by their own
//! package; here we prove that a reliable output cursor and keyed IoT state
//! share one strict, versioned checkpoint cut.

use crate::*;
use sparrow_model::{
    CreditKind, InflightCounter, MemoryOwner, OutputSequence, ResourceBudget, Row, Scalar,
    StateSlotId,
};
use sparrow_plan::{
    bind_graph, physicalize, AggCall, Catalog, CheckpointPlan, GraphSpec, PhysicalPlan,
    PhysicalStage, PlanOptions, WindowSpec,
};
use std::sync::Arc;
use std::time::Duration;

fn iot_plan(deadband: bool, emit_first: bool, threshold: i64) -> PhysicalPlan {
    let kind = if deadband {
        "deadband"
    } else {
        "change_detect"
    };
    let extra = if deadband {
        format!(
            r#", "deadband":{{"mode":"absolute","baseline":"last_output","threshold":{threshold}}}"#
        )
    } else {
        String::new()
    };
    let graph = format!(
        r#"{{"version":1,"pipeline_id":404,"revision_id":1,
        "catalog":[{{"name":"s","fields":[
            {{"name":"device_id","type":"utf8","nullable":false}},
            {{"name":"v","type":"int64","nullable":false}}
        ]}}],"nodes":[
            {{"id":1,"kind":"memory_source","table":"s","out":[2]}},
            {{"id":2,"kind":"{kind}","iot":{{"keys":["device_id"],"fields":["v"],
              "emit_first":{emit_first},"ttl_micros":0,"max_keys":16,"invalid":"error"{extra}}},"out":[3]}},
            {{"id":3,"kind":"capture_sink","name":"out"}}
        ]}}"#
    );
    physicalize(
        &bind_graph(&GraphSpec::from_json(&graph).unwrap(), &Catalog::new()).unwrap(),
        &PlanOptions::default(),
    )
}

fn two_deadband_plan() -> PhysicalPlan {
    let graph = r#"{
        "version":1,
        "pipeline_id":405,
        "revision_id":1,
        "catalog":[{"name":"s","fields":[
            {"name":"device_id","type":"utf8","nullable":false},
            {"name":"v","type":"int64","nullable":false}
        ]}],
        "nodes":[
            {"id":1,"kind":"memory_source","table":"s","out":[2]},
            {"id":2,"kind":"deadband","iot":{
                "keys":["device_id"], "fields":["v"], "emit_first":true,
                "ttl_micros":0, "max_keys":16, "invalid":"error",
                "deadband":{"mode":"absolute","baseline":"last_output","threshold":2}
            },"out":[3]},
            {"id":3,"kind":"deadband","iot":{
                "keys":["device_id"], "fields":["v"], "emit_first":true,
                "ttl_micros":0, "max_keys":16, "invalid":"error",
                "deadband":{"mode":"absolute","baseline":"last_output","threshold":5}
            },"out":[4]},
            {"id":4,"kind":"capture_sink","name":"out"}
        ]
    }"#;
    physicalize(
        &bind_graph(&GraphSpec::from_json(&graph).unwrap(), &Catalog::new()).unwrap(),
        &PlanOptions::default(),
    )
}

fn row(key: &str, value: i64) -> Row {
    Row {
        values: vec![Scalar::utf8(key), Scalar::Int64(value)],
    }
}

fn kernel() -> Kernel {
    Kernel::new_with_job_budget(
        KernelOptions {
            budget: ResourceBudget::performance(),
            mailbox: MailboxConfig {
                max_items: 4,
                max_bytes: 16 * 1024,
            },
            worker_threads: 2,
            rows_per_batch: 2,
        },
        ResourceBudget::compact(),
    )
    .unwrap()
}

fn aligned_job(
    plan: Arc<CheckpointPlan>,
    acks: AlignedAcks,
    generation: [u8; 16],
    restore: Option<PipelineSnapshot>,
    outbox: Arc<InflightCounter>,
) -> AlignedJob {
    let (restore_windows, restore_iot, generation) = match restore {
        Some(snapshot) => (Some(snapshot.windows), snapshot.iot, snapshot.generation),
        None => (None, Vec::new(), generation),
    };
    AlignedJob {
        restore: None,
        pipeline: Some(PipelineRestore { buffered: Vec::new(),
            sink: None,
            plan,
            generation,
            restore: restore_windows,
            iot: restore_iot,
        }),
        acks,
        outbox,
    }
}

async fn collect_ids(
    mut rx: sparrow_io::observed::Receiver<sparrow_model::RowBatch>,
    outbox: Arc<InflightCounter>,
) -> Vec<[u8; 48]> {
    let mut ids = Vec::new();
    while let Some(batch) = rx.recv().await {
        if let Some(sequence) = batch.output_sequence() {
            for offset in 0..batch.num_rows() {
                ids.push(sequence.id_ascii(offset).unwrap());
            }
        }
        outbox.ack();
    }
    ids
}

async fn collect_outputs(
    mut rx: sparrow_io::observed::Receiver<sparrow_model::RowBatch>,
    outbox: Arc<InflightCounter>,
) -> Vec<(i64, [u8; 48])> {
    let mut outputs = Vec::new();
    while let Some(batch) = rx.recv().await {
        let sequence = batch.output_sequence();
        for (offset, row) in batch.rows().iter().enumerate() {
            let value = match row.values.last() {
                Some(Scalar::Int64(value)) => *value,
                other => panic!("mixed reliable output aggregate: {other:?}"),
            };
            let id = sequence
                .expect("reliable mixed output envelope")
                .id_ascii(offset)
                .unwrap();
            outputs.push((value, id));
        }
        outbox.ack();
    }
    outputs
}

fn jetstream_position(offset: u64) -> sparrow_io::SourcePosition {
    let mut position =
        sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("core-a", 0, 0));
    position.identity.kind = "jetstream-v1".into();
    position.identity.path = "account:stream:created:bucket:consumer".into();
    position.offset_bytes = offset;
    position.record_index = offset;
    position
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "sparrow-core-a-{label}-{}-{stamp}",
        std::process::id()
    ))
}

#[test]
fn core_a_reliable_iot_v7_cut_restores_state_and_output_sequence() {
    let kernel = kernel();
    kernel.block_on(async {
        let physical = iot_plan(false, true, 0);
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let first = OutputSequence::new([7; 16], 1).unwrap();
        let acks = AlignedAcks::default().with_output_sequence(first).unwrap();
        let (tx, rx) = sparrow_io::observed::channel(8);
        let (out, out_rx) = sparrow_io::observed::channel(8);
        let outbox = Arc::new(InflightCounter::new());
        let handle = kernel
            .submit(
                JobRequest::new(physical.clone(), vec![], SharedCapture::disabled())
                    .with_live_events(rx)
                    .with_live_out(out)
                    .with_aligned(aligned_job(
                        manifest.clone(),
                        acks.clone(),
                        [7; 16],
                        None,
                        outbox.clone(),
                    )),
            )
            .unwrap();
        let collector = tokio::spawn(collect_ids(out_rx, outbox.clone()));

        let request = acks.begin(1).unwrap();
        tx.send(IngressEvent::Row(row("device-a", 10)))
            .await
            .unwrap();
        tx.send(IngressEvent::Row(row("device-a", 10)))
            .await
            .unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 1,
        }))
        .await
        .unwrap();
        let frozen = request
            .wait_participants(Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(frozen.next_output(), Some(first.advance(1).unwrap()));

        let owner = handle.memory_owner();
        let source = jetstream_position(2);
        let encoded =
            PipelineSnapshot::encode_frozen(1, &source, 2, 1, &manifest, frozen, &owner, 16)
                .unwrap();
        assert_eq!(
            &encoded.bytes()[4..6],
            &crate::pipeline_checkpoint::RELIABLE_IOT_SNAPSHOT_VERSION.to_le_bytes()
        );
        let snapshot = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        assert_eq!(snapshot.iot.len(), 1);
        assert_eq!(snapshot.windows.len(), 0);
        assert_eq!(snapshot.next_output, Some(first.advance(1).unwrap()));

        // The v7 store is intentionally not interchangeable with either
        // output-only v4 or IoT-only v6 history.
        let dir = temp_dir("cut");
        let mut store =
            CheckpointStore::open_reliable_iot_exclusive(&dir, 16, Default::default()).unwrap();
        store.commit_prepared(&encoded).unwrap();
        let recovered = store.recover_pipeline_required().unwrap();
        assert_eq!(recovered.next_output, snapshot.next_output);
        assert_eq!(recovered.iot, snapshot.iot);
        drop(store);
        assert!(CheckpointStore::open_reliable_exclusive(&dir, 16, Default::default()).is_err());
        assert!(CheckpointStore::open_iot_exclusive(&dir, 16, Default::default()).is_err());
        drop(encoded);

        handle.cancel();
        drop(tx);
        handle.wait().await.unwrap();
        let first_ids = collector.await.unwrap();
        assert_eq!(first_ids, vec![first.id_ascii(0).unwrap()]);

        let next = recovered.next_output.unwrap();
        let (out2, out2_rx) = sparrow_io::observed::channel(8);
        let outbox2 = Arc::new(InflightCounter::new());
        let acks2 = AlignedAcks::default().with_output_sequence(next).unwrap();
        let handle2 = kernel
            .submit(
                JobRequest::new(
                    physical,
                    vec![row("device-a", 10), row("device-a", 11)],
                    SharedCapture::disabled(),
                )
                .with_live_out(out2)
                .with_aligned(aligned_job(
                    manifest,
                    acks2,
                    [7; 16],
                    Some(recovered),
                    outbox2.clone(),
                )),
            )
            .unwrap();
        let collector2 = tokio::spawn(collect_ids(out2_rx, outbox2.clone()));
        handle2.wait().await.unwrap();
        let second_ids = collector2.await.unwrap();
        assert_eq!(second_ids, vec![first.id_ascii(1).unwrap()]);
        std::fs::remove_dir_all(dir).unwrap();
    });
}

#[test]
fn core_a_reliable_iot_v7_deadband_multi_key_and_all_suppressed() {
    let kernel = kernel();
    let capture = SharedCapture::new();
    kernel
        .run(JobRequest::new(
            iot_plan(true, true, 100),
            vec![
                row("device-a", 10),
                row("device-b", 20),
                row("device-a", 11),
                row("device-b", 21),
            ],
            capture.clone(),
        ))
        .unwrap();
    let mut values = capture
        .rows()
        .into_iter()
        .filter_map(|row| match row[1] {
            Scalar::Int64(value) => Some(value),
            _ => None,
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![10, 20]);

    let suppressed = SharedCapture::new();
    kernel
        .run(JobRequest::new(
            iot_plan(true, false, 100),
            vec![
                row("device-a", 10),
                row("device-b", 20),
                row("device-a", 11),
                row("device-b", 21),
            ],
            suppressed.clone(),
        ))
        .unwrap();
    assert_eq!(suppressed.row_count(), 0);
}

fn mixed_plan(iot_first: bool) -> PhysicalPlan {
    let mut physical = iot_plan(false, true, 0);
    let input = physical.source_schema().unwrap().clone();
    let spec = WindowSpec::new(
        sparrow_model::WindowKind::Count { size: 2 },
        vec!["device_id".into()],
        vec![AggCall::new(
            sparrow_model::AggFn::Sum,
            Some(sparrow_expr::Expr::Column { name: "v".into() }),
            "v",
        )],
    );
    let output = sparrow_plan::window_output_schema(&input, &spec).unwrap();
    let window = PhysicalStage::WindowAgg {
        operator: 8.into(),
        spec,
        input,
        output: output.clone(),
    };
    if iot_first {
        physical.stages.insert(2, window);
    } else {
        if let PhysicalStage::Iot { input, output: iot_output, .. } = &mut physical.stages[1] {
            *input = output.clone();
            *iot_output = output.clone();
        }
        physical.stages.insert(1, window);
    }
    if let Some(PhysicalStage::CaptureSink { schema, .. }) = physical.stages.last_mut() {
        *schema = output;
    }
    physical
}

fn encoded_iot(
    operator: sparrow_model::OperatorId,
    kind: u8,
    owner: &Arc<MemoryOwner>,
) -> crate::barrier::EncodedFreeze {
    let freeze = IotFreeze::new(
        operator,
        kind,
        vec![crate::iot::IotEntry {
            key: vec![Scalar::utf8("device-a")],
            values: vec![Scalar::Int64(10)],
        }],
    )
    .unwrap();
    let mut bytes = Vec::new();
    freeze.encode_into(&mut bytes, 16).unwrap();
    let lease = owner
        .acquire(CreditKind::Reservation, bytes.capacity().max(1))
        .unwrap();
    crate::barrier::EncodedFreeze { bytes, lease, ext: false, buffered: false }
}

fn encoded_count(
    operator: sparrow_model::OperatorId,
    owner: &Arc<MemoryOwner>,
) -> crate::barrier::EncodedFreeze {
    let freeze = crate::window::WindowFreeze {
        operator,
        slot: StateSlotId::new(1),
        kind: 1,
        entries: Vec::new(),
        wm_in: None,
        wm_out: None,
        last_effective: None,
    };
    let mut bytes = Vec::new();
    crate::checkpoint::encode_freeze(&freeze, &mut bytes, 16).unwrap();
    let lease = owner
        .acquire(CreditKind::Reservation, bytes.capacity().max(1))
        .unwrap();
    crate::barrier::EncodedFreeze { bytes, lease, ext: false, buffered: false }
}

#[test]
fn core_a_reliable_iot_v7_mixed_count_and_iot_orderings() {
    for iot_first in [false, true] {
        for cut in 0..=6usize {
            let kernel = kernel();
            kernel.block_on(async {
                let physical = mixed_plan(iot_first);
                let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
                assert_eq!(manifest.states.len(), 2);
                let first = OutputSequence::new([11; 16], 1).unwrap();
                let acks = AlignedAcks::default().with_output_sequence(first).unwrap();
                let (tx, rx) = sparrow_io::observed::channel(8);
                let (out, out_rx) = sparrow_io::observed::channel(16);
                let outbox = Arc::new(InflightCounter::new());
                let handle = kernel
                    .submit(
                        JobRequest::new(physical.clone(), vec![], SharedCapture::disabled())
                            .with_live_events(rx)
                            .with_live_out(out)
                            .with_aligned(aligned_job(
                                manifest.clone(),
                                acks.clone(),
                                [11; 16],
                                None,
                                outbox.clone(),
                            )),
                    )
                    .unwrap();
                let collector = tokio::spawn(collect_outputs(out_rx, outbox.clone()));
                let inputs = [10, 10, 11, 11, 20, 20];
                let request = acks.begin(1).unwrap();
                for value in inputs[..cut].iter().copied() {
                    tx.send(IngressEvent::Row(row("device-a", value)))
                        .await
                        .unwrap();
                }
                tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                    checkpoint_id: 1,
                }))
                .await
                .unwrap();
                // Queue the suffix before observing the ACK.  The ordered
                // ingress must still freeze only the prefix at this barrier.
                for value in inputs[cut..].iter().copied() {
                    tx.send(IngressEvent::Row(row("device-a", value)))
                        .await
                        .unwrap();
                }
                let frozen = request
                    .wait_participants(Duration::from_secs(3))
                    .await
                    .unwrap();
                let next = frozen.next_output().unwrap();
                // Derive the cut independently, not from the production
                // cursor under test (which could incorrectly include suffix).
                let prefix_outputs = if iot_first {
                    usize::from(cut >= 3)
                } else {
                    cut / 2
                };
                assert_eq!(next, first.advance(prefix_outputs).unwrap());
                let encoded = PipelineSnapshot::encode_frozen(
                    1,
                    &jetstream_position(cut as u64),
                    cut as u64,
                    1,
                    &manifest,
                    frozen,
                    &handle.memory_owner(),
                    16,
                )
                .unwrap();
                assert_eq!(
                    &encoded.bytes()[4..6],
                    &crate::pipeline_checkpoint::RELIABLE_IOT_SNAPSHOT_VERSION.to_le_bytes()
                );
                let snapshot = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
                assert_eq!(snapshot.next_output, Some(next));
                drop(encoded);
                handle.cancel();
                drop(tx);
                handle.wait().await.unwrap();
                let first_attempt = collector.await.unwrap();
                assert!(first_attempt.len() >= prefix_outputs);
                let mut combined = first_attempt[..prefix_outputs].to_vec();

                let next = snapshot.next_output.unwrap();
                let (out2, out2_rx) = sparrow_io::observed::channel(16);
                let outbox2 = Arc::new(InflightCounter::new());
                let acks2 = AlignedAcks::default().with_output_sequence(next).unwrap();
                let handle2 = kernel
                    .submit(
                        JobRequest::new(
                            physical,
                            inputs[cut..]
                                .iter()
                                .copied()
                                .map(|value| row("device-a", value))
                                .collect(),
                            SharedCapture::disabled(),
                        )
                        .with_live_out(out2)
                        .with_aligned(aligned_job(
                            manifest,
                            acks2,
                            [11; 16],
                            Some(snapshot),
                            outbox2.clone(),
                        )),
                    )
                    .unwrap();
                let collector2 = tokio::spawn(collect_outputs(out2_rx, outbox2.clone()));
                handle2.wait().await.unwrap();
                let replayed = collector2.await.unwrap();
                let old_suffix = &first_attempt[prefix_outputs..];
                assert!(old_suffix.len() <= replayed.len());
                assert_eq!(
                    old_suffix,
                    &replayed[..old_suffix.len()],
                    "uncommitted outputs must replay with the same values and IDs"
                );
                combined.extend(replayed);

                let expected_values = if iot_first {
                    vec![21]
                } else {
                    vec![20, 22, 40]
                };
                let expected_ids = (0..expected_values.len())
                    .map(|offset| first.id_ascii(offset).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    combined.iter().map(|(value, _)| *value).collect::<Vec<_>>(),
                    expected_values,
                    "iot_first={iot_first} cut={cut}"
                );
                assert_eq!(
                    combined.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
                    expected_ids,
                    "reliable IDs must remain cut-local and contiguous"
                );
                assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
            });
        }
    }
}

#[test]
fn core_a_reliable_iot_v7_two_deadbands_keep_operator_state_isolated() {
    let kernel = kernel();
    kernel.block_on(async {
        let physical = two_deadband_plan();
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        assert_eq!(manifest.states.len(), 2);
        let first = OutputSequence::new([15; 16], 1).unwrap();
        let acks = AlignedAcks::default().with_output_sequence(first).unwrap();
        let (tx, rx) = sparrow_io::observed::channel(8);
        let (out, out_rx) = sparrow_io::observed::channel(16);
        let outbox = Arc::new(InflightCounter::new());
        let handle = kernel
            .submit(
                JobRequest::new(physical.clone(), vec![], SharedCapture::disabled())
                    .with_live_events(rx)
                    .with_live_out(out)
                    .with_aligned(aligned_job(
                        manifest.clone(),
                        acks.clone(),
                        [15; 16],
                        None,
                        outbox.clone(),
                    )),
            )
            .unwrap();
        let collector = tokio::spawn(collect_outputs(out_rx, outbox.clone()));
        let inputs = [10, 11, 13, 14, 16, 19];
        let request = acks.begin(1).unwrap();
        for value in inputs[..3].iter().copied() {
            tx.send(IngressEvent::Row(row("device-a", value)))
                .await
                .unwrap();
        }
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: 1,
        }))
        .await
        .unwrap();
        // Keep the post-cut suffix in the ordered source queue.  The snapshot
        // must retain A=13 and B=10, not the values produced after the barrier.
        for value in inputs[3..].iter().copied() {
            tx.send(IngressEvent::Row(row("device-a", value)))
                .await
                .unwrap();
        }
        let frozen = request
            .wait_participants(Duration::from_secs(3))
            .await
            .unwrap();
        let next = frozen.next_output().unwrap();
        assert_eq!(next.first(), 2);
        let encoded = PipelineSnapshot::encode_frozen(
            1,
            &jetstream_position(3),
            3,
            1,
            &manifest,
            frozen,
            &handle.memory_owner(),
            16,
        )
        .unwrap();
        assert_eq!(
            &encoded.bytes()[4..6],
            &crate::pipeline_checkpoint::RELIABLE_IOT_SNAPSHOT_VERSION.to_le_bytes()
        );
        let snapshot = PipelineSnapshot::decode(encoded.bytes(), 16).unwrap();
        let a = snapshot
            .iot
            .iter()
            .find(|freeze| freeze.operator.raw() == 2)
            .unwrap();
        let b = snapshot
            .iot
            .iter()
            .find(|freeze| freeze.operator.raw() == 3)
            .unwrap();
        assert_eq!(a.entries[0].values, vec![Scalar::Int64(13)]);
        assert_eq!(b.entries[0].values, vec![Scalar::Int64(10)]);
        drop(encoded);
        handle.cancel();
        drop(tx);
        handle.wait().await.unwrap();
        let first_attempt = collector.await.unwrap();
        assert!(first_attempt.len() >= 1);
        let mut combined = first_attempt[..1].to_vec();

        let next = snapshot.next_output.unwrap();
        let (out2, out2_rx) = sparrow_io::observed::channel(16);
        let outbox2 = Arc::new(InflightCounter::new());
        let acks2 = AlignedAcks::default().with_output_sequence(next).unwrap();
        let handle2 = kernel
            .submit(
                JobRequest::new(
                    physical,
                    inputs[3..]
                        .iter()
                        .copied()
                        .map(|value| row("device-a", value))
                        .collect(),
                    SharedCapture::disabled(),
                )
                .with_live_out(out2)
                .with_aligned(aligned_job(
                    manifest,
                    acks2,
                    [15; 16],
                    Some(snapshot),
                    outbox2.clone(),
                )),
            )
            .unwrap();
        let collector2 = tokio::spawn(collect_outputs(out2_rx, outbox2.clone()));
        handle2.wait().await.unwrap();
        let replayed = collector2.await.unwrap();
        let old_suffix = &first_attempt[1..];
        assert!(old_suffix.len() <= replayed.len());
        assert_eq!(old_suffix, &replayed[..old_suffix.len()]);
        combined.extend(replayed);
        assert_eq!(
            combined.iter().map(|(value, _)| *value).collect::<Vec<_>>(),
            vec![10, 16]
        );
        assert_eq!(
            combined.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
            vec![first.id_ascii(0).unwrap(), first.id_ascii(1).unwrap()]
        );
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

#[test]
fn core_a_reliable_iot_v7_profiles_and_corruption_fail_closed() {
    let physical = iot_plan(false, true, 0);
    let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mismatched_epoch = PipelineSnapshot::encode_frozen(
        1,
        &jetstream_position(1),
        1,
        1,
        &manifest,
        crate::barrier::ParticipantAcks {
            attempt: 1,
            generation: [13; 16],
            freezes: vec![encoded_iot(2.into(), 4, &owner)],
            next_output: Some(OutputSequence::new([14; 16], 1).unwrap()),
        },
        &owner,
        16,
    );
    assert!(mismatched_epoch.is_err());
    let encoded = PipelineSnapshot::encode_frozen(
        1,
        &jetstream_position(1),
        1,
        1,
        &manifest,
        crate::barrier::ParticipantAcks {
            attempt: 1,
            generation: [13; 16],
            freezes: vec![encoded_iot(2.into(), 4, &owner)],
            next_output: Some(OutputSequence::new([13; 16], 1).unwrap()),
        },
        &owner,
        16,
    )
    .unwrap();
    let bytes = encoded.bytes().to_vec();
    assert!(PipelineSnapshot::decode(&bytes, 16).is_ok());
    assert!(PipelineSnapshot::decode(&bytes[..bytes.len() - 1], 16).is_err());
    assert!(CheckpointSnapshot::decode(&bytes).is_err());
    for old_version in [3u16, 4, 5, 6] {
        let mut corrupt = bytes.clone();
        corrupt[4..6].copy_from_slice(&old_version.to_le_bytes());
        assert!(PipelineSnapshot::decode(&corrupt, 16).is_err());
    }

    let dir = temp_dir("profiles");
    let mut store =
        CheckpointStore::open_reliable_iot_exclusive(&dir, 16, Default::default()).unwrap();
    store.commit_prepared(&encoded).unwrap();
    assert_eq!(
        store.inventory().unwrap().generations[0]
            .metadata
            .as_ref()
            .unwrap()
            .version,
        crate::pipeline_checkpoint::RELIABLE_IOT_SNAPSHOT_VERSION
    );
    drop(store);
    assert!(CheckpointStore::open_pipeline_exclusive(&dir, 16, Default::default()).is_err());
    assert!(CheckpointStore::open_reliable_exclusive(&dir, 16, Default::default()).is_err());
    assert!(CheckpointStore::open_iot_exclusive(&dir, 16, Default::default()).is_err());
    assert!(CheckpointStore::open_reliable_iot_exclusive(&dir, 16, Default::default()).is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}

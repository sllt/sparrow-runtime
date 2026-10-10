//! K4 integration: independent output expectations across real Kernel cuts.
use crate::*;
use sparrow_model::{InflightCounter, ResourceBudget, Row, Scalar};
use sparrow_plan::{bind_graph, physicalize, Catalog, CheckpointPlan, GraphSpec, PhysicalPlan};
use std::sync::Arc;
use std::time::Duration;

fn plan(deadband: bool, ttl: i64, branch: bool) -> PhysicalPlan {
    let kind = if deadband {
        "deadband"
    } else {
        "change_detect"
    };
    let config = if deadband {
        r#", "deadband":{"mode":"absolute","baseline":"last_output","threshold":2}"#
    } else {
        ""
    };
    let ending = if branch {
        r#"{"id":3,"kind":"branch","out":[4,5]},{"id":4,"kind":"capture_sink","name":"a"},{"id":5,"kind":"capture_sink","name":"b"}"#
    } else {
        r#"{"id":3,"kind":"capture_sink","name":"out"}"#
    };
    let json = format!(
        r#"{{"version":1,"pipeline_id":404,"revision_id":1,"catalog":[{{"name":"s","fields":[{{"name":"device_id","type":"utf8","nullable":false}},{{"name":"v","type":"int64","nullable":false}}]}}],"nodes":[{{"id":1,"kind":"memory_source","table":"s","out":[2]}},{{"id":2,"kind":"{kind}","iot":{{"keys":["device_id"],"fields":["v"],"emit_first":true,"ttl_micros":{ttl},"max_keys":16,"invalid":"error"{config}}},"out":[3]}},{ending}]}}"#
    );
    physicalize(
        &bind_graph(&GraphSpec::from_json(&json).unwrap(), &Catalog::new()).unwrap(),
        &Default::default(),
    )
}
fn row(v: i64) -> Row {
    Row {
        values: vec![Scalar::utf8("a"), Scalar::Int64(v)],
    }
}
fn values(capture: &SharedCapture) -> Vec<i64> {
    capture
        .rows()
        .into_iter()
        .map(|row| match row[1] {
            Scalar::Int64(v) => v,
            _ => panic!("integer output"),
        })
        .collect()
}
fn kernel() -> Kernel {
    Kernel::new_with_job_budget(
        KernelOptions {
            budget: ResourceBudget::performance(),
            mailbox: MailboxConfig {
                max_items: 2,
                max_bytes: 8192,
            },
            rows_per_batch: 2,
            worker_threads: 2,
        },
        ResourceBudget::compact(),
    )
    .unwrap()
}
fn aligned(
    manifest: Arc<CheckpointPlan>,
    acks: AlignedAcks,
    restore: Option<PipelineSnapshot>,
) -> AlignedJob {
    let (generation, windows, iot) = match restore {
        Some(snapshot) => (snapshot.generation, Some(snapshot.windows), snapshot.iot),
        None => ([44; 16], None, vec![]),
    };
    AlignedJob {
        restore: None,
        pipeline: Some(PipelineRestore { buffered: Vec::new(),
            sink: None,
            plan: manifest,
            generation,
            restore: windows,
            iot,
        }),
        acks,
        outbox: Arc::new(InflightCounter::new()),
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observable K4 progress");
}

#[test]
fn k4_kernel_change_and_deadband_preserve_rows_and_graph_broadcast() {
    for (deadband, expected) in [(false, vec![10, 11, 12, 13, 16]), (true, vec![10, 13, 16])] {
        for branch in [false, true] {
            let kernel = kernel();
            let capture = SharedCapture::new();
            let request = JobRequest::new(
                plan(deadband, 0, branch),
                [10, 10, 11, 12, 13, 16].into_iter().map(row).collect(),
                capture.clone(),
            );
            kernel.run(request).unwrap();
            let mut actual = values(&capture);
            let mut expected = if branch {
                expected
                    .iter()
                    .copied()
                    .chain(expected.iter().copied())
                    .collect::<Vec<_>>()
            } else {
                expected.clone()
            };
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
            let metrics = kernel.metrics.snapshot();
            assert_eq!(metrics.iot_input_rows, 6);
            assert_eq!(metrics.iot_emitted_rows, if deadband { 3 } else { 5 });
            assert_eq!(metrics.iot_filtered_rows, if deadband { 3 } else { 1 });
            assert_eq!(metrics.iot_state_keys, 0);
            assert_eq!(metrics.iot_state_bytes, 0);
            assert_eq!(kernel.live_tasks(), 0);
            assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        }
    }
}

#[test]
fn k4_ttl_expires_without_a_new_message_and_state_gauges_clear_on_stop() {
    let kernel = kernel();
    let capture = SharedCapture::new();
    let clock = RuntimeClock::virtual_clock(sparrow_model::SharedVirtualClock::new(0));
    kernel.block_on(async {
        let (tx, rx) = sparrow_io::observed::channel(2);
        let handle = kernel
            .submit(
                JobRequest::new(plan(false, 10, false), vec![], capture.clone())
                    .with_clock(clock.clone())
                    .with_live_events(rx),
            )
            .unwrap();
        let owner = handle.memory_owner();
        tx.send(IngressEvent::Row(row(1))).await.unwrap();
        until(|| capture.row_count() == 1 && kernel.metrics.snapshot().iot_state_keys == 1).await;
        clock.advance_virtual(10);
        until(|| {
            kernel.metrics.snapshot().iot_expired_keys == 1
                && kernel.metrics.snapshot().iot_state_keys == 0
        })
        .await;
        tx.send(IngressEvent::Row(row(1))).await.unwrap();
        until(|| capture.row_count() == 2).await;
        handle.cancel();
        drop(tx);
        handle.wait().await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(kernel.metrics.snapshot().iot_state_keys, 0);
        assert_eq!(kernel.metrics.snapshot().iot_state_bytes, 0);
        assert_eq!(kernel.metrics.snapshot().timers_live, 0);
    });
}

#[test]
fn k4_v6_cut_restore_matches_uninterrupted_golden_and_rejects_wrong_profile() {
    for deadband in [false, true] {
        for cut in 0..=6usize {
            let kernel = kernel();
            let inputs = [10, 10, 11, 12, 13, 16];
            let expected = if deadband {
                vec![10, 13, 16]
            } else {
                vec![10, 11, 12, 13, 16]
            };
            kernel.block_on(async {
                let physical = plan(deadband, 0, false);
                let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
                assert!(manifest.has_iot());
                let capture = SharedCapture::new();
                let (tx, rx) = sparrow_io::observed::channel(2);
                let acks = AlignedAcks::default();
                let handle = kernel
                    .submit(
                        JobRequest::new(physical.clone(), vec![], capture.clone())
                            .with_live_events(rx)
                            .with_aligned(aligned(manifest.clone(), acks.clone(), None)),
                    )
                    .unwrap();
                let owner = handle.memory_owner();
                for &v in &inputs[..cut] {
                    tx.send(IngressEvent::Row(row(v))).await.unwrap();
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
                let position = sparrow_io::SourcePosition {
                    identity: sparrow_io::SourceIdentity::memory("k4-fixture", 6, 1),
                    offset_bytes: cut as u64,
                    record_index: cut as u64,
                };
                let encoded = PipelineSnapshot::encode_frozen(
                    1, &position, cut as u64, 1, &manifest, frozen, &owner, 1024,
                )
                .unwrap();
                assert_eq!(&encoded.bytes()[4..6], &6u16.to_le_bytes());
                let stamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let dir = std::env::temp_dir()
                    .join(format!("sparrow-k4-{}-{stamp}-{cut}", std::process::id()));
                let mut store =
                    CheckpointStore::open_iot_exclusive(&dir, 1024, Default::default()).unwrap();
                store.commit_prepared(&encoded).unwrap();
                let snapshot = store.recover_pipeline_required().unwrap();
                assert!(snapshot.windows.is_empty());
                assert_eq!(snapshot.iot.len(), 1);
                assert_eq!(snapshot.source.record_index, cut as u64);
                snapshot.check_compatible(&manifest).unwrap();
                if cut == 3 {
                    for end in 0..encoded.bytes().len() {
                        assert!(PipelineSnapshot::decode(&encoded.bytes()[..end], 1024).is_err());
                        assert!(PipelineSnapshot::decode_mode(
                            &encoded.bytes()[..end],
                            1024,
                            false
                        )
                        .is_err());
                    }
                    for version in [3u16, 4, 5] {
                        let mut bad = encoded.bytes().to_vec();
                        bad[4..6].copy_from_slice(&version.to_le_bytes());
                        assert!(PipelineSnapshot::decode(&bad, 1024).is_err());
                    }
                }
                drop(encoded);
                drop(store);
                for profile in [3, 4, 5] {
                    let result = match profile {
                        3 => {
                            CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default())
                        }
                        4 => {
                            CheckpointStore::open_reliable_exclusive(&dir, 1024, Default::default())
                        }
                        _ => CheckpointStore::open_graph_exclusive(&dir, 1024, Default::default()),
                    };
                    assert!(
                        result.is_err(),
                        "v{profile} must refuse v6 writable history"
                    );
                }
                handle.cancel();
                drop(tx);
                handle.wait().await.unwrap();
                assert_eq!(owner.usage().physical_bytes, 0);
                let remaining = inputs[cut..].iter().copied().map(row).collect();
                let handle = kernel
                    .submit(
                        JobRequest::new(physical, remaining, capture.clone()).with_aligned(
                            aligned(manifest, AlignedAcks::default(), Some(snapshot)),
                        ),
                    )
                    .unwrap();
                let owner = handle.memory_owner();
                handle.wait().await.unwrap();
                assert_eq!(values(&capture), expected, "deadband={deadband} cut={cut}");
                assert_eq!(owner.usage().physical_bytes, 0);
                std::fs::remove_dir_all(dir).unwrap();
            });
        }
    }
}

#[test]
fn k4_admission_refuses_ttl_recovery_before_ingress_or_state_allocation() {
    let kernel = kernel();
    let mut physical = plan(false, 0, false);
    let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
    if let sparrow_plan::PhysicalStage::Iot { spec, .. } = &mut physical.stages[1] {
        spec.ttl_micros = 1;
    } else {
        panic!("IoT stage");
    }
    let before = kernel.process_owner().usage().physical_bytes;
    let result = kernel.submit(
        JobRequest::new(physical, vec![row(99)], SharedCapture::new()).with_aligned(aligned(
            manifest,
            AlignedAcks::default(),
            None,
        )),
    );
    assert!(result.is_err());
    assert_eq!(kernel.metrics.snapshot().ingested_rows, 0);
    assert_eq!(kernel.metrics.snapshot().jobs_started, 0);
    assert_eq!(kernel.admitted_jobs(), 0);
    assert_eq!(kernel.process_owner().usage().physical_bytes, before);
}

#[test]
fn k4_mixed_count_and_iot_participants_restore_both_orderings() {
    use sparrow_plan::{AggCall, PhysicalStage, WindowSpec};
    for iot_first in [false, true] {
        for cut in [0, 3, 4, 6] {
            let kernel = kernel();
            let mut physical = plan(false, 0, false);
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
            let inputs = [10, 10, 11, 11, 20, 20];
            let expected = if iot_first {
                vec![21]
            } else {
                vec![20, 22, 40]
            };
            kernel.block_on(async {
                let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
                assert_eq!(manifest.states.len(), 2);
                let capture = SharedCapture::new();
                let (tx, rx) = sparrow_io::observed::channel(2);
                let acks = AlignedAcks::default();
                let handle = kernel
                    .submit(
                        JobRequest::new(physical.clone(), vec![], capture.clone())
                            .with_live_events(rx)
                            .with_aligned(aligned(manifest.clone(), acks.clone(), None)),
                    )
                    .unwrap();
                let owner = handle.memory_owner();
                for &v in &inputs[..cut] {
                    tx.send(IngressEvent::Row(row(v))).await.unwrap();
                }
                let pending = acks.begin(1).unwrap();
                tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                    checkpoint_id: 1,
                }))
                .await
                .unwrap();
                let frozen = pending
                    .wait_participants(Duration::from_secs(3))
                    .await
                    .unwrap();
                let source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory(
                    "k4-mixed", 6, 1,
                ));
                let encoded = PipelineSnapshot::encode_frozen(
                    1, &source, cut as u64, 1, &manifest, frozen, &owner, 1024,
                )
                .unwrap();
                let snapshot = PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap();
                assert_eq!(snapshot.iot.len(), 1);
                assert_eq!(snapshot.windows.len(), 1);
                drop(encoded);
                handle.cancel();
                drop(tx);
                handle.wait().await.unwrap();
                assert_eq!(owner.usage().physical_bytes, 0);
                let remaining = inputs[cut..].iter().copied().map(row).collect();
                kernel
                    .submit(
                        JobRequest::new(physical, remaining, capture.clone()).with_aligned(
                            aligned(manifest, AlignedAcks::default(), Some(snapshot)),
                        ),
                    )
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                let actual: Vec<_> = capture
                    .rows()
                    .iter()
                    .map(|row| match row.last().unwrap() {
                        Scalar::Int64(v) => *v,
                        _ => panic!("sum output"),
                    })
                    .collect();
                assert_eq!(actual, expected, "iot_first={iot_first} cut={cut}");
                assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
            });
        }
    }
}

fn dag_iot_plan() -> PhysicalPlan {
    let json = r#"{"version":1,"pipeline_id":404,"revision_id":1,"catalog":[{"name":"s","fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}],"nodes":[{"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"change_detect","iot":{"keys":["device_id"],"fields":["v"],"emit_first":true,"ttl_micros":0,"max_keys":16,"invalid":"error"},"out":[5]},{"id":5,"kind":"branch","out":[6,7]},{"id":6,"kind":"capture_sink","name":"a"},{"id":7,"kind":"capture_sink","name":"b"}]}"#;
    physicalize(
        &bind_graph(&GraphSpec::from_json(json).unwrap(), &Catalog::new()).unwrap(),
        &Default::default(),
    )
}

#[test]
fn k4_dag_iot_checkpoint_restores_per_key_baselines_to_all_required_sinks() {
    let physical = dag_iot_plan();
    let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
    let kernel = kernel();
    let capture = SharedCapture::new();
    kernel.block_on(async {
        let mut snapshot = None;
        for attempt in 0..2 {
            let (a, ar) = sparrow_io::observed::channel(4);
            let (b, br) = sparrow_io::observed::channel(4);
            let acks = AlignedAcks::default();
            let mut request = JobRequest::new(physical.clone(), vec![], capture.clone())
                .with_aligned(aligned(manifest.clone(), acks.clone(), snapshot.take()));
            request.graph_inputs.insert(
                1.into(),
                GraphInput {
                    events: Some(ar),
                    ..Default::default()
                },
            );
            request.graph_inputs.insert(
                2.into(),
                GraphInput {
                    events: Some(br),
                    ..Default::default()
                },
            );
            let handle = kernel.submit(request).unwrap();
            let owner = handle.memory_owner();
            for (key, tx) in [("a", &a), ("b", &b)] {
                tx.send(IngressEvent::Row(Row {
                    values: vec![Scalar::utf8(key), Scalar::Int64(10)],
                }))
                .await
                .unwrap();
            }
            if attempt == 1 {
                a.send(IngressEvent::Row(row(20))).await.unwrap();
            }
            let pending = acks.begin(1).unwrap();
            for tx in [&a, &b] {
                tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                    checkpoint_id: 1,
                }))
                .await
                .unwrap();
            }
            let frozen = pending
                .wait_participants(Duration::from_secs(3))
                .await
                .unwrap();
            let source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity {
                kind: "file-dag-v1".into(),
                path: "runtime-fixture".into(),
                size: 0,
                fingerprint: 0,
            });
            let encoded =
                PipelineSnapshot::encode_frozen(1, &source, 2, 1, &manifest, frozen, &owner, 1024)
                    .unwrap();
            let decoded = PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap();
            assert_eq!(decoded.iot[0].entries.len(), 2);
            assert_eq!(
                capture.row_count(),
                if attempt == 0 { 4 } else { 6 },
                "restored equal values must be suppressed on both required outputs"
            );
            snapshot = Some(decoded);
            drop(encoded);
            handle.cancel();
            drop(a);
            drop(b);
            handle.wait().await.unwrap();
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    });
}

#[test]
fn k4_aligned_graph_rejects_finite_and_unordered_inputs_before_admission() {
    let kernel = kernel();
    for mode in 0..3 {
        let physical = if mode == 2 { plan(false, 0, true) } else { dag_iot_plan() };
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let mut request = JobRequest::new(physical, vec![], SharedCapture::disabled())
            .with_aligned(aligned(manifest, AlignedAcks::default(), None));
        if mode != 2 {
            for id in [1, 2] {
                let input = if mode == 0 {
                    GraphInput { rows: vec![row(1)], ..Default::default() }
                } else {
                    let (_tx, rx) = sparrow_io::observed::channel(1);
                    GraphInput { live: Some(rx), ..Default::default() }
                };
                request.graph_inputs.insert(id.into(), input);
            }
        } else {
            request.rows = vec![row(1)];
        }
        let error = match kernel.submit(request) { Err(error) => error, Ok(_) => panic!("unordered aligned graph accepted") };
        assert!(error.message.contains("ordered event inputs"));
        assert_eq!(kernel.live_tasks(), 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    }
}

#[test]
fn k4_union_closed_input_never_satisfies_a_checkpoint_barrier() {
    let kernel = kernel();
    kernel.block_on(async {
        let physical = dag_iot_plan();
        let manifest = Arc::new(CheckpointPlan::from_physical(&physical).unwrap());
        let acks = AlignedAcks::default();
        let mut request = JobRequest::new(physical, vec![], SharedCapture::disabled())
            .with_aligned(aligned(manifest, acks.clone(), None));
        let (a, ar) = sparrow_io::observed::channel(4);
        let (b, br) = sparrow_io::observed::channel(4);
        for (id, rx) in [(1, ar), (2, br)] {
            request.graph_inputs.insert(id.into(), GraphInput { events: Some(rx), ..Default::default() });
        }
        let handle = kernel.submit(request).unwrap();
        let owner = handle.memory_owner();
        let cut = acks.begin(1).unwrap();
        a.send(IngressEvent::Control(StreamControl::CheckpointBarrier { checkpoint_id: 1 })).await.unwrap();
        b.send(IngressEvent::Control(StreamControl::EndOfInput)).await.unwrap();
        drop(b);
        assert!(tokio::time::timeout(Duration::from_secs(3), handle.wait()).await.unwrap().is_err());
        assert!(cut.wait_participants(Duration::from_millis(20)).await.is_err());
        drop(a);
        assert_eq!(kernel.live_tasks(), 0);
        assert_eq!(owner.usage().physical_bytes, 0);
    });
}

#[test]
fn k4_v6_encode_refuses_mismatched_source_topology_before_publication() {
    let owner = sparrow_model::MemoryOwner::new(ResourceBudget::compact());
    for graph in [false, true] {
        let physical = if graph { dag_iot_plan() } else { plan(false, 0, false) };
        let manifest = CheckpointPlan::from_physical(&physical).unwrap();
        let mut source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("mismatch", 0, 0));
        if !graph { source.identity.kind = "file-dag-v1".into(); }
        let acks = crate::barrier::ParticipantAcks { attempt: 1, generation: [1; 16], freezes: vec![], next_output: None };
        let error = match PipelineSnapshot::encode_frozen(1, &source, 0, 1, &manifest, acks, &owner, 16) {
            Err(error) => error, Ok(_) => panic!("mismatched topology published"),
        };
        assert!(error.message.contains("source topology mismatch"));
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[test]
fn k4_v5_graph_source_profile_is_checked_on_encode_and_decode() {
    let json = r#"{"version":1,"pipeline_id":404,"revision_id":1,"catalog":[{"name":"s","fields":[{"name":"v","type":"int64","nullable":false}]}],"nodes":[{"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"capture_sink","name":"out"}]}"#;
    let physical = physicalize(&bind_graph(&GraphSpec::from_json(json).unwrap(), &Catalog::new()).unwrap(), &Default::default());
    let manifest = CheckpointPlan::from_physical(&physical).unwrap();
    let owner = sparrow_model::MemoryOwner::new(ResourceBudget::compact());
    let acks = || crate::barrier::ParticipantAcks { attempt: 1, generation: [1; 16], freezes: vec![], next_output: None };
    let mut source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("profile", 0, 0));
    assert!(PipelineSnapshot::encode_frozen(1, &source, 0, 1, &manifest, acks(), &owner, 16).is_err());
    source.identity.kind = "file-dag-v1".into();
    let encoded = PipelineSnapshot::encode_frozen(1, &source, 0, 1, &manifest, acks(), &owner, 16).unwrap();
    assert_eq!(&encoded.bytes()[4..6], &5u16.to_le_bytes());
    assert!(PipelineSnapshot::decode(encoded.bytes(), 16).is_ok());
    let mut invalid = encoded.bytes().to_vec();
    // Same-length corruption changes only the declared source profile.
    let offset = invalid.windows(11).position(|part| part == b"file-dag-v1").unwrap();
    invalid[offset] = b'x';
    assert!(PipelineSnapshot::decode(&invalid, 16).unwrap_err().message.contains("source topology mismatch"));
}

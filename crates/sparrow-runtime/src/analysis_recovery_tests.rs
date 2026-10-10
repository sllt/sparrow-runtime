use super::*;
use crate::{
    analysis_state::{AnalysisData, AnalysisFreeze, UnnestState},
    graph_cut::{GraphCut, GraphRuntime, Progress, SourceProgress, UnionProgress},
};
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{InflightCounter, OperatorId, OutputSequence, SharedVirtualClock};
use sparrow_plan::{CheckpointPlan, PhysicalPlan, PhysicalStage, UnnestSpec};
use std::{collections::BTreeMap, sync::Arc};

#[path = "business_recovery_tests.rs"]
mod business_recovery_tests;

fn unnest_plan() -> PhysicalPlan {
    let input = Schema::new(1, vec![Field::new(1, "items", DataType::Dynamic, true)]).unwrap();
    let analysis = AnalysisPlan::unnest(
        UnnestSpec {
            expr: sparrow_expr::Expr::Column {
                name: "items".into(),
            },
            as_field: "item".into(),
            max_rows: 16,
            max_bytes: 65536,
        },
        input.clone(),
    )
    .unwrap();
    PhysicalPlan {
        pipeline: 7.into(),
        revision: 1.into(),
        edges: None,
        source_times: vec![],
        side_outputs: vec![],
        stages: vec![
            PhysicalStage::MemorySource {
                operator: 1.into(),
                name: "s".into(),
                schema: input,
            },
            PhysicalStage::Analysis {
                operator: 3.into(),
                plan: Box::new(analysis.clone()),
            },
            PhysicalStage::CaptureSink {
                operator: 6.into(),
                name: "out".into(),
                schema: analysis.output().clone(),
            },
        ],
    }
}
fn array(values: &[i64]) -> Row {
    Row {
        values: vec![Scalar::Dynamic(D::Array(
            values
                .iter()
                .copied()
                .map(D::Int64)
                .collect::<Vec<_>>()
                .into(),
        ))],
    }
}
fn tmp() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "sparrow-analysis-recovery-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}
type Output = Vec<(Option<Vec<u8>>, Row)>;

fn linear_segment(
    js: bool,
    restored: Option<PipelineSnapshot>,
    rows: Vec<Row>,
) -> (PipelineSnapshot, Output) {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    kernel.block_on(async {
        let plan = unnest_plan();
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan).unwrap());
        let n = restored.as_ref().map_or(0, |s| s.ingested_rows) + rows.len() as u64;
        let output = restored
            .as_ref()
            .and_then(|s| s.next_output)
            .unwrap_or(OutputSequence::new([7; 16], 1).unwrap());
        let (windows, analysis) = restored
            .map(|s| (Some(s.windows), s.analysis))
            .unwrap_or((None, vec![]));
        let acks = if js {
            AlignedAcks::default().with_output_sequence(output).unwrap()
        } else {
            AlignedAcks::default()
        };
        let (tx, rx) = sparrow_io::observed::channel(1);
        let (out, mut received) = sparrow_io::observed::channel::<sparrow_model::RowBatch>(1);
        let count = Arc::new(InflightCounter::new());
        let counter = count.clone();
        let job = kernel
            .submit(
                JobRequest::new(plan, vec![], SharedCapture::disabled())
                    .with_live_events(rx)
                    .with_live_out(out)
                    .with_aligned(AlignedJob {
                        restore: None,
                        pipeline: Some(PipelineRestore {
                            plan: manifest.clone(),
                            generation: [7; 16],
                            restore: windows,
                            analysis,
                            buffered: vec![],
                            iot: vec![],
                            sink: None,
                        }),
                        acks: acks.clone(),
                        outbox: count,
                    }),
            )
            .unwrap();
        let sink = tokio::spawn(async move {
            let mut rows = vec![];
            while let Some(batch) = received.recv().await {
                for (i, row) in batch.rows().iter().enumerate() {
                    rows.push((
                        batch
                            .output_sequence()
                            .map(|p| p.id_ascii(i).unwrap().to_vec()),
                        row.detach_copy(),
                    ));
                }
                counter.ack();
            }
            rows
        });
        for row in rows {
            tx.send(IngressEvent::Row(row)).await.unwrap();
        }
        let request = acks.begin(n).unwrap();
        tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
            checkpoint_id: n,
        }))
        .await
        .unwrap();
        let frozen = request
            .wait_participants(Duration::from_secs(3))
            .await
            .unwrap();
        let source = SourcePosition {
            identity: SourceIdentity {
                kind: if js { "jetstream-v1" } else { "file" }.into(),
                path: "fixture".into(),
                size: 0,
                fingerprint: 0,
            },
            offset_bytes: n,
            record_index: n,
        };
        let bytes = PipelineSnapshot::encode_frozen(
            n,
            &source,
            n,
            1,
            &manifest,
            frozen,
            &job.memory_owner(),
            1024,
        )
        .unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes.bytes()[4..6].try_into().unwrap()),
            if js { 37 } else { 36 }
        );
        let snapshot = PipelineSnapshot::decode(bytes.bytes(), 1024).unwrap();
        drop(bytes);
        let owner = job.memory_owner();
        job.stop().await.unwrap();
        drop(tx);
        let rows = sink.await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);
        (snapshot, rows)
    })
}

#[test]
fn analysis_recovery_unnest_preserves_empty_input_ordinals_and_reliable_output_ids() {
    let rows = vec![array(&[7, 8]), array(&[]), array(&[9]), array(&[10, 11])];
    for js in [false, true] {
        let (_, whole) = linear_segment(js, None, rows.clone());
        let (snap, mut head) = linear_segment(js, None, rows[..2].to_vec());
        let (_, tail) = linear_segment(js, Some(snap), rows[2..].to_vec());
        head.extend(tail);
        assert_eq!(head, whole);
        let pairs: Vec<_> = whole
            .iter()
            .map(|(_, r)| (r.values[3].clone(), r.values[4].clone()))
            .collect();
        assert_eq!(
            pairs,
            [(1, 1), (1, 2), (3, 1), (4, 1), (4, 2)]
                .map(|(i, o)| (Scalar::Int64(i), Scalar::Int64(o)))
        );
    }
}

#[derive(Clone)]
struct Decision {
    row: Option<(u32, Row, Option<i64>)>,
    eof: Option<u32>,
    idle: Option<u32>,
}
fn input(id: u32, row: Row, wm: Option<i64>) -> Decision {
    Decision {
        row: Some((id, row, wm)),
        eof: None,
        idle: None,
    }
}
fn end(id: u32) -> Decision {
    Decision {
        row: None,
        eof: Some(id),
        idle: None,
    }
}
fn initial(plan: &PhysicalPlan, manifest: &CheckpointPlan) -> GraphCut {
    GraphCut {
        sequence: 0,
        micros: 0,
        observed_micros: 10000,
        ingested: 0,
        next_source: 0,
        idle_micros: None,
        sources: manifest
            .source_ids()
            .into_iter()
            .map(|id| {
                (
                    id.raw(),
                    SourceProgress {
                        position: SourcePosition::start(SourceIdentity {
                            kind: "file".into(),
                            path: format!("source{}", id.raw()),
                            size: 0,
                            fingerprint: 0,
                        }),
                        progress: Progress::default(),
                        last_input: 0,
                        contract: 1,
                    },
                )
            })
            .collect(),
        unions: plan
            .stages
            .iter()
            .enumerate()
            .filter_map(|(i, s)| match s {
                PhysicalStage::UnionAll { operator, .. } => Some((
                    operator.raw(),
                    UnionProgress {
                        inputs: vec![
                            Progress::default();
                            plan.edges
                                .as_ref()
                                .unwrap()
                                .iter()
                                .filter(|e| e.to == i)
                                .count()
                        ],
                        emitted: Progress::default(),
                    },
                )),
                _ => None,
            })
            .collect(),
        outputs: manifest
            .sink_ids()
            .into_iter()
            .map(|id| (id.raw(), 1))
            .collect(),
    }
}
fn graph_segment(
    plan: PhysicalPlan,
    restored: Option<PipelineSnapshot>,
    decisions: &[Decision],
    reverse: bool,
) -> (PipelineSnapshot, Output) {
    graph_segment_with_table(plan, restored, decisions, reverse, None)
}
fn graph_segment_with_table(
    plan: PhysicalPlan,
    restored: Option<PipelineSnapshot>,
    decisions: &[Decision],
    reverse: bool,
    reference: Option<(Schema, Vec<Row>)>,
) -> (PipelineSnapshot, Output) {
    let mut options = KernelOptions::default();
    options.mailbox.max_items = 1;
    options.mailbox.max_bytes = 4096;
    options.rows_per_batch = 1;
    let kernel = Kernel::new(options).unwrap();
    kernel.block_on(async {
        let admission = kernel.prepare_source_admission(plan.pipeline).unwrap();
        let owner = admission.owner();
        let tables: HashMap<_, _> = reference
            .into_iter()
            .map(|(schema, rows)| {
                let table = crate::ReferenceTable::snapshot_owned_verified(
                    "limits",
                    1,
                    schema,
                    vec!["k".into()],
                    rows,
                    16,
                    owner.budget().retention_bytes,
                    [42; 32],
                    &owner,
                )
                .unwrap();
                ("limits".into(), table)
            })
            .collect();
        let manifest = Arc::new(if tables.is_empty() {
            CheckpointPlan::from_physical(&plan).unwrap()
        } else {
            CheckpointPlan::from_physical_with_references(
                &plan,
                tables
                    .values()
                    .map(|t| t.verified_dependency().unwrap())
                    .collect(),
            )
            .unwrap()
        });
        if let Some(saved) = &restored {
            saved.check_compatible(&manifest).unwrap();
        }
        let mut cut = restored
            .as_ref()
            .map(|s| GraphCut::unwrap(&s.source).unwrap())
            .unwrap_or_else(|| initial(&plan, &manifest));
        let graph = GraphRuntime::new_with_manifest(cut.clone(), &plan, &manifest, [7; 16], &owner)
            .unwrap();
        let acks = AlignedAcks::default()
            .with_graph_time(graph.clone())
            .unwrap();
        let (windows, analysis, iot) = restored
            .map(|s| (Some(s.windows), s.analysis, s.iot))
            .unwrap_or((None, vec![], vec![]));
        let mut request = JobRequest::new(plan.clone(), vec![], SharedCapture::disabled())
            .with_tables(tables)
            .with_source_admission(admission)
            .with_clock(RuntimeClock::virtual_clock(SharedVirtualClock::new(
                cut.micros,
            )))
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    plan: manifest.clone(),
                    generation: [7; 16],
                    restore: windows,
                    analysis,
                    buffered: vec![],
                    iot,
                    sink: None,
                }),
                acks: acks.clone(),
                outbox: Arc::new(InflightCounter::new()),
            });
        let mut senders = BTreeMap::new();
        for id in manifest.source_ids() {
            let (tx, rx) = sparrow_io::observed::channel(1);
            senders.insert(id.raw(), tx);
            request.graph_inputs.insert(
                id,
                GraphInput {
                    events: Some(rx),
                    ..Default::default()
                },
            );
        }
        let (out, mut received) = sparrow_io::observed::channel::<sparrow_model::RowBatch>(1);
        let counter = Arc::new(InflightCounter::new());
        request.graph_outputs.insert(
            manifest.sink,
            GraphOutput {
                capture: SharedCapture::disabled(),
                live: Some(out),
                outbox: Some(counter.clone()),
            },
        );
        let job = kernel.submit(request).unwrap();
        let sink = tokio::spawn(async move {
            let mut rows = vec![];
            while let Some(batch) = received.recv().await {
                for (i, r) in batch.rows().iter().enumerate() {
                    rows.push((
                        Some(
                            batch
                                .output_sequence()
                                .unwrap()
                                .id_ascii(i)
                                .unwrap()
                                .to_vec(),
                        ),
                        r.detach_copy(),
                    ));
                }
                counter.ack();
            }
            rows
        });
        let mut snapshot = None;
        for decision in decisions {
            let before = cut.clone();
            cut.sequence += 1;
            cut.micros += 100;
            if let Some((id, _, wm)) = &decision.row {
                cut.ingested += 1;
                let s = cut.sources.get_mut(id).unwrap();
                s.position.offset_bytes += 1;
                s.position.record_index += 1;
                s.last_input = cut.micros;
                s.progress.idle = false;
                if let Some(wm) = wm {
                    s.progress.watermark = s.progress.watermark.max(Some(*wm));
                }
            }
            if let Some(id) = decision.eof {
                let s = &mut cut.sources.get_mut(&id).unwrap().progress;
                s.eof = true;
                s.idle = true;
            }
            if let Some(id) = decision.idle {
                cut.sources.get_mut(&id).unwrap().progress.idle = true;
            }
            graph.begin_round(&cut).unwrap();
            let req = acks.begin(cut.sequence).unwrap();
            let mut ids: Vec<_> = senders.keys().copied().collect();
            if reverse {
                ids.reverse();
            }
            for id in ids {
                let tx = &senders[&id];
                tx.send(IngressEvent::Control(StreamControl::ProcessingTime {
                    micros: cut.micros,
                }))
                .await
                .unwrap();
                let mut p = before.sources[&id].progress.clone();
                if decision.row.as_ref().is_some_and(|r| r.0 == id) {
                    p.idle = false;
                }
                tx.send(IngressEvent::Control(p.control())).await.unwrap();
                if let Some((source, row, _)) = &decision.row {
                    if *source == id {
                        tx.send(IngressEvent::Row(row.clone())).await.unwrap();
                    }
                }
                tx.send(IngressEvent::Control(cut.sources[&id].progress.control()))
                    .await
                    .unwrap();
                tx.send(IngressEvent::Control(StreamControl::GraphRoundEnd {
                    sequence: cut.sequence,
                }))
                .await
                .unwrap();
            }
            for tx in senders.values() {
                tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                    checkpoint_id: cut.sequence,
                }))
                .await
                .unwrap();
            }
            let frozen = req.wait_participants(Duration::from_secs(3)).await.unwrap();
            graph.complete(cut.sequence, &mut cut).unwrap();
            cut.check_plan_with_manifest(&plan, &manifest).unwrap();
            let bytes = PipelineSnapshot::encode_frozen(
                cut.sequence,
                &cut.wrap().unwrap(),
                cut.ingested,
                1,
                &manifest,
                frozen,
                &owner,
                1024,
            )
            .unwrap();
            let version = if manifest.is_business_graph() {
                39u16
            } else {
                38u16
            };
            assert_eq!(&bytes.bytes()[4..6], &version.to_le_bytes());
            if manifest.is_business_graph() {
                let mut foreign = bytes.bytes().to_vec();
                foreign[4..6].copy_from_slice(&38u16.to_le_bytes());
                assert_eq!(
                    PipelineSnapshot::decode(&foreign, 1024).unwrap_err().code,
                    ErrorCode::UnsupportedRestore
                );
            }
            let dir = tmp();
            let mut store = CheckpointStore::open_for_plan_exclusive(
                &dir,
                1024,
                Default::default(),
                &manifest,
                crate::graph_cut::KIND,
            )
            .unwrap();
            store.commit_prepared(&bytes).unwrap();
            let (decoded, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
            assert_eq!(
                decoded.analysis.len() + decoded.windows.len() + decoded.iot.len(),
                manifest.states.len()
            );
            snapshot = Some(decoded);
            drop((credit, store));
            std::fs::remove_dir_all(dir).unwrap();
        }
        job.stop().await.unwrap();
        drop(senders);
        drop(graph);
        drop(acks);
        let rows = sink.await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);
        (snapshot.unwrap(), rows)
    })
}

#[test]
fn analysis_recovery_join_matches_oracle_across_restore_idle_eof_and_port_order() {
    let d = vec![
        input(1, row("a", 1, 12), Some(2)),
        input(2, row("a", 101, 15), Some(5)),
        input(1, row("b", 2, 22), Some(12)),
        input(2, row("a", 102, 16), Some(6)),
        Decision {
            row: None,
            eof: None,
            idle: Some(2),
        },
        input(1, row("c", 3, 50), Some(40)),
        input(2, row("q", 103, 60), Some(50)),
        end(2),
        end(1),
    ];
    for window in [false, true] {
        for left in [false, true] {
            let mut plan = graph(window, left);
            for (_, binding) in &mut plan.source_times {
                binding.out_of_orderness_micros = 10;
            }
            let (_, whole) = graph_segment(plan.clone(), None, &d, false);
            let pairs: Vec<_> = whole
                .iter()
                .map(|(_, r)| (r.values[1].clone(), r.values[4].clone()))
                .collect();
            let mut want = vec![
                (Scalar::Int64(1), Scalar::Int64(101)),
                (Scalar::Int64(1), Scalar::Int64(102)),
            ];
            if left {
                want.extend([
                    (Scalar::Int64(2), Scalar::Null),
                    (Scalar::Int64(3), Scalar::Null),
                ]);
            }
            assert_eq!(pairs, want);
            for cut in [2, 5, 8] {
                let (saved, mut head) = graph_segment(plan.clone(), None, &d[..cut], true);
                let (_, tail) = graph_segment(plan.clone(), Some(saved), &d[cut..], false);
                head.extend(tail);
                assert_eq!(head, whole, "window={window} left={left} cut={cut}");
            }
        }
    }
}

#[test]
fn analysis_recovery_multiple_sources_union_unnest_resume_nested_rows() {
    let graph = sparrow_plan::GraphSpec::from_json(&json!({"version":1,"pipeline_id":7,"revision_id":1,
        "catalog":[{"name":"s","fields":[{"name":"items","type":"dynamic","nullable":true}]}],
        "nodes":[{"id":1,"kind":"memory_source","table":"s","out":[3]},
            {"id":2,"kind":"memory_source","table":"s","out":[3]}, {"id":3,"kind":"union_all","out":[4]},
            {"id":4,"kind":"unnest","unnest":{"expr":{"k":"col","name":"items"}},"out":[6]},
            {"id":6,"kind":"capture_sink"}]}).to_string()).unwrap();
    let plan = sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &sparrow_plan::Catalog::new()).unwrap(),
        &Default::default(),
    );
    let d = vec![
        input(1, array(&[7, 8]), None),
        input(2, array(&[]), None),
        input(1, array(&[9]), None),
        input(2, array(&[10]), None),
        end(1),
        end(2),
    ];
    let (_, whole) = graph_segment(plan.clone(), None, &d, true);
    let (saved, mut head) = graph_segment(plan.clone(), None, &d[..2], false);
    let (_, tail) = graph_segment(plan, Some(saved), &d[2..], true);
    head.extend(tail);
    assert_eq!(head, whole);
    let ordinals: Vec<_> = whole
        .iter()
        .map(|(_, r)| (r.values[2].clone(), r.values[3].clone()))
        .collect();
    assert_eq!(
        ordinals,
        [(1, 1), (1, 1), (1, 2), (2, 2)].map(|(s, n)| (Scalar::Int64(s), Scalar::Int64(n)))
    );
}

#[test]
fn analysis_recovery_codec_bounds_legacy_refusal_and_credit_refund() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut state = UnnestState::new(&owner).unwrap();
    state.sequences.insert(None, 2);
    state.sequences.insert(Some(OperatorId::new(9)), 3);
    let mut bytes = vec![];
    state.encode(3.into(), &mut bytes, 64).unwrap();
    for materialize in [false, true] {
        let decoded = AnalysisFreeze::decode(&mut bytes.as_slice(), 64, materialize).unwrap();
        assert_eq!(decoded.resident_bytes(), crate::analysis_state::FIXED);
        for n in 0..bytes.len() {
            assert!(AnalysisFreeze::decode(&mut &bytes[..n], 64, materialize).is_err());
        }
        assert!(AnalysisFreeze::decode(&mut bytes.as_slice(), 1, materialize).is_err());
    }
    let plan = unnest_plan();
    let manifest = CheckpointPlan::from_physical(&plan).unwrap();
    assert!(manifest.recovery_prefix_len.is_none());
    let frozen =
        crate::barrier::EncodedFreeze::from_analysis(&owner, crate::analysis_state::FIXED, |out| {
            state.encode(3.into(), out, 64)
        })
        .unwrap();
    let snapshot = PipelineSnapshot::encode_frozen(
        1,
        &SourcePosition::start(SourceIdentity {
            kind: "file".into(),
            path: "s".into(),
            size: 0,
            fingerprint: 0,
        }),
        0,
        1,
        &manifest,
        crate::barrier::ParticipantAcks {
            attempt: 1,
            generation: [7; 16],
            next_output: None,
            freezes: vec![frozen],
        },
        &owner,
        64,
    )
    .unwrap();
    let dir = tmp();
    let mut store =
        CheckpointStore::open_for_plan_exclusive(&dir, 64, Default::default(), &manifest, "file")
            .unwrap();
    store.commit_prepared(&snapshot).unwrap();
    let mut small = ResourceBudget::compact();
    small.reservation_bytes = 1024;
    let small = MemoryOwner::new(small);
    assert!(store.recover_pipeline_owned(None, &small).is_err());
    assert_eq!(small.usage().physical_bytes, 0);
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    assert!(matches!(snap.analysis[0].data, AnalysisData::Unnest(_)));
    let mut foreign = snapshot.bytes().to_vec();
    foreign[4..6].copy_from_slice(&3u16.to_le_bytes());
    assert!(PipelineSnapshot::decode(&foreign, 64).is_err());
    drop((state, snap, credit, snapshot, store));
    std::fs::remove_dir_all(dir).unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

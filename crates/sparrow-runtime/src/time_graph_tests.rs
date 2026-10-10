use crate::graph_cut::{GraphCut, GraphRuntime, Progress, SourceProgress, UnionProgress};
use crate::*;
use sparrow_model::{InflightCounter, Row, Scalar, SharedVirtualClock};
use sparrow_plan::{
    bind_graph, physicalize, Catalog, CheckpointPlan, GraphSpec, PhysicalPlan, PhysicalStage,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

fn plan(event: bool) -> PhysicalPlan {
    let time = if event {
        r#", "event_time_field":"ts", "out_of_orderness_micros":0"#
    } else {
        ""
    };
    let window = if event {
        r#"{"kind":"event_time","size_micros":100,"event_time_field":"ts","lateness_micros":10}"#
    } else {
        r#"{"kind":"processing_time","size_micros":100}"#
    };
    let json = format!(
        r#"{{"version":1,"pipeline_id":818,"revision_id":1,
        "catalog":[{{"name":"s","fields":[{{"name":"v","type":"int64","nullable":false}},{{"name":"ts","type":"int64","nullable":false}}]}}],
        "nodes":[
        {{"id":1,"kind":"memory_source","table":"s"{time},"out":[3]}},
        {{"id":2,"kind":"memory_source","table":"s"{time},"out":[3]}},
        {{"id":3,"kind":"union_all","out":[4]}},{{"id":4,"kind":"branch","out":[5,6]}},
        {{"id":5,"kind":"window_agg","window":{window},"keys":[],"aggs":[{{"fn":"sum","expr":{{"k":"col","name":"v"}},"alias":"v"}}],"out":[7]}},
        {{"id":6,"kind":"window_agg","window":{window},"keys":[],"aggs":[{{"fn":"sum","expr":{{"k":"col","name":"v"}},"alias":"v"}}],"out":[7]}},
        {{"id":7,"kind":"union_all","out":[8]}},
        {{"id":8,"kind":"window_agg","window":{{"kind":"count","size":2}},"keys":[],"aggs":[{{"fn":"sum","expr":{{"k":"col","name":"v"}},"alias":"v"}}],"out":[9]}},
        {{"id":9,"kind":"branch","out":[10,11]}},{{"id":10,"kind":"capture_sink","name":"a"}},{{"id":11,"kind":"capture_sink","name":"b"}}]}}"#
    );
    physicalize(
        &bind_graph(&GraphSpec::from_json(&json).unwrap(), &Catalog::new()).unwrap(),
        &Default::default(),
    )
}
fn initial(plan: &PhysicalPlan) -> GraphCut {
    GraphCut {
        sequence: 0,
        micros: 0,
        observed_micros: 0,
        ingested: 0,
        next_source: 0,
        idle_micros: None,
        sources: [1, 2]
            .into_iter()
            .map(|id| {
                (
                    id,
                    SourceProgress {
                        position: sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity {
                            kind: "file".into(),
                            path: format!("source{id}"),
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
            .filter_map(|(index, stage)| match stage {
                PhysicalStage::UnionAll { operator, .. } => Some((
                    operator.raw(),
                    UnionProgress {
                        inputs: vec![
                            Progress::default();
                            plan.edges
                                .as_ref()
                                .unwrap()
                                .iter()
                                .filter(|e| e.to == index)
                                .count()
                        ],
                        emitted: Progress::default(),
                    },
                )),
                _ => None,
            })
            .collect(),
        outputs: [(10, 1), (11, 1)].into_iter().collect(),
    }
}
#[derive(Clone)]
struct Frame {
    micros: i64,
    row: Option<(u32, i64, i64)>,
    idle: Vec<u32>,
    eof: Vec<u32>,
}
fn row(micros: i64, id: u32, v: i64, ts: i64) -> Frame {
    Frame {
        micros,
        row: Some((id, v, ts)),
        idle: vec![],
        eof: vec![],
    }
}
fn tick(micros: i64) -> Frame {
    Frame {
        micros,
        row: None,
        idle: vec![],
        eof: vec![],
    }
}
type Outputs = BTreeMap<u32, Vec<(Vec<u8>, Row)>>;
fn drive(
    event: bool,
    restored: Option<&PipelineSnapshot>,
    frames: &[Frame],
    reverse: bool,
) -> (PipelineSnapshot, Outputs) {
    let mut options = KernelOptions::default();
    options.mailbox.max_items = 1;
    options.mailbox.max_bytes = 4096;
    options.rows_per_batch = 1;
    let kernel = Kernel::new(options).unwrap();
    kernel.block_on(async {
        let plan = plan(event);
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan).unwrap());
        let mut cut = restored
            .as_ref()
            .map(|s| GraphCut::unwrap(&s.source).unwrap())
            .unwrap_or_else(|| initial(&plan));
        let admission = kernel.prepare_source_admission(plan.pipeline).unwrap();
        let owner = admission.owner();
        let graph = GraphRuntime::new(cut.clone(), &plan, [7; 16], &owner).unwrap();
        let acks = AlignedAcks::default()
            .with_graph_time(graph.clone())
            .unwrap();
        let (windows, iot) = restored
            .map(|s| (Some(s.windows.clone()), s.iot.clone()))
            .unwrap_or((None, vec![]));
        let mut request = JobRequest::new(plan, vec![], SharedCapture::disabled())
            .with_source_admission(admission)
            .with_clock(RuntimeClock::virtual_clock(SharedVirtualClock::new(
                cut.micros,
            )))
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore { buffered: Vec::new(),
                    sink: None,
                    plan: manifest.clone(),
                    generation: [7; 16],
                    restore: windows,
                    iot,
                }),
                acks: acks.clone(),
                outbox: Arc::new(InflightCounter::new()),
            });
        let mut senders = BTreeMap::new();
        let mut sinks = Vec::new();
        for id in [1u32, 2] {
            let (tx, rx) = sparrow_io::observed::channel(1);
            senders.insert(id, tx);
            request.graph_inputs.insert(
                id.into(),
                GraphInput {
                    events: Some(rx),
                    ..Default::default()
                },
            );
        }
        for id in [10u32, 11] {
            let (tx, rx) = sparrow_io::observed::channel::<sparrow_model::RowBatch>(1);
            let counter = Arc::new(InflightCounter::new());
            request.graph_outputs.insert(
                id.into(),
                GraphOutput {
                    capture: SharedCapture::disabled(),
                    live: Some(tx),
                    outbox: Some(counter.clone()),
                },
            );
            sinks.push((id, rx, counter));
        }
        let job = kernel.submit(request).unwrap();
        let sinks = sinks
            .into_iter()
            .map(|(id, mut rx, counter)| {
                tokio::spawn(async move {
                    let mut rows = vec![];
                    while let Some(batch) = rx.recv().await {
                        for (i, row) in batch.rows().iter().enumerate() {
                            rows.push((
                                batch
                                    .output_sequence()
                                    .unwrap()
                                    .id_ascii(i)
                                    .unwrap()
                                    .to_vec(),
                                row.detach_copy(),
                            ));
                        }
                        counter.ack();
                    }
                    (id, rows)
                })
            })
            .collect::<Vec<_>>();
        let mut saved = None;
        for frame in frames {
            let before = cut.clone();
            cut.sequence += 1;
            cut.micros = frame.micros;
            cut.observed_micros = 10_000;
            if let Some((id, _, ts)) = frame.row {
                cut.ingested += 1;
                let source = cut.sources.get_mut(&id).unwrap();
                source.position.record_index += 1;
                source.position.offset_bytes += 1;
                source.last_input = frame.micros;
                source.progress.idle = false;
                if event {
                    source.progress.watermark =
                        Some(source.progress.watermark.map_or(ts, |wm| wm.max(ts)));
                }
            }
            for id in &frame.idle {
                cut.sources.get_mut(id).unwrap().progress.idle = true;
            }
            for id in &frame.eof {
                let p = &mut cut.sources.get_mut(id).unwrap().progress;
                p.eof = true;
                p.idle = true;
            }
            graph.begin_round(&cut).unwrap();
            let request = acks.begin(cut.sequence).unwrap();
            for id in if reverse { [2, 1] } else { [1, 2] } {
                let tx = &senders[&id];
                tx.send(IngressEvent::Control(StreamControl::ProcessingTime {
                    micros: cut.micros,
                }))
                .await
                .unwrap();
                let mut progress = before.sources[&id].progress.clone();
                if frame.row.is_some_and(|r| r.0 == id) {
                    progress.idle = false;
                }
                tx.send(IngressEvent::Control(progress.control()))
                    .await
                    .unwrap();
                if let Some((source, v, ts)) = frame.row {
                    if source == id {
                        tx.send(IngressEvent::Row(Row {
                            values: vec![Scalar::Int64(v), Scalar::Int64(ts)],
                        }))
                        .await
                        .unwrap();
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
            let frozen = request
                .wait_participants(Duration::from_secs(3))
                .await
                .unwrap();
            graph.complete(cut.sequence, &mut cut).unwrap();
            let encoded = PipelineSnapshot::encode_frozen(
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
            saved = Some(PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap());
        }
        job.stop().await.unwrap();
        drop(senders);
        let mut outputs = BTreeMap::new();
        for task in sinks {
            let (id, rows) = task.await.unwrap();
            outputs.insert(id, rows);
        }
        drop(acks);
        drop(graph);
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.live_tasks(), 0);
        (saved.unwrap(), outputs)
    })
}
#[test]
fn time_graph_pt_fanout_rejoin_replay_order_and_sink_ids() {
    let (saved, outputs) = drive(false, None, &[row(10, 1, 3, 10), row(20, 2, 7, 20)], false);
    assert!(outputs.values().all(Vec::is_empty));
    let (_, forward) = drive(false, Some(&saved), &[tick(100)], false);
    let (_, reverse) = drive(false, Some(&saved), &[tick(100)], true);
    assert_eq!(forward, reverse);
    for rows in forward.values() {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values.last(), Some(&Scalar::Int64(20)));
    }
    assert_ne!(forward[&10][0].0, forward[&11][0].0);
}
#[test]
fn time_graph_et_fanout_rejoin_lateness_eof_restore_and_replay() {
    let (saved, outputs) = drive(true, None, &[row(10, 1, 3, 10), row(20, 2, 7, 20)], false);
    assert!(outputs.values().all(Vec::is_empty));
    let mut eof = tick(50);
    eof.eof = vec![1, 2];
    let frames = [row(30, 1, 5, 110), row(40, 2, 9, 120), eof.clone()];
    let (ended, forward) = drive(true, Some(&saved), &frames, false);
    let (_, reverse) = drive(true, Some(&saved), &frames, true);
    assert_eq!(forward, reverse);
    for rows in forward.values() {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1.values.last(), Some(&Scalar::Int64(20)));
        assert_eq!(rows[1].1.values.last(), Some(&Scalar::Int64(28)));
    }
    let (_, out) = drive(true, Some(&ended), &[eof], true);
    assert!(out.values().all(Vec::is_empty));
}
#[test]
fn time_graph_cut_codec_rejects_truncation_mirrors_and_invalid_progress() {
    let cut = initial(&plan(true));
    let position = cut.wrap().unwrap();
    assert_eq!(GraphCut::unwrap(&position).unwrap(), cut);
    for len in (0..position.identity.path.len()).step_by(2) {
        let mut p = position.clone();
        p.identity.path.truncate(len);
        assert!(GraphCut::unwrap(&p).is_err());
    }
    for case in 0..6 {
        let mut p = position.clone();
        match case {
            0 => p.record_index = 1,
            1 => p.identity.size = 1,
            2 => p.offset_bytes = 1,
            3 => p.identity.fingerprint = 1,
            4 => p.identity.path.push_str("00"),
            _ => p.identity.path.replace_range(0..2, "GG"),
        };
        assert!(GraphCut::unwrap(&p).is_err());
    }
    for (wm, flags) in [(-2, 0), (0, 2), (0, 4), (0, 255)] {
        assert!(Progress::from_control(wm, flags).is_err());
    }
    let mut bad = cut.clone();
    bad.sources.get_mut(&1).unwrap().last_input = 1;
    assert!(bad.wrap().is_err());
    let mut bad = cut.clone();
    bad.outputs.insert(10, 0);
    assert!(bad.wrap().is_err());
    let mut bad = cut.clone();
    bad.sources.get_mut(&1).unwrap().position.identity.path = "x".repeat(graph_cut::MAX_BYTES);
    assert!(
        bad.validate().is_err(),
        "direct embedding metadata must be bounded before retention"
    );
    let mut bad = cut.clone();
    bad.ingested = 1;
    assert!(bad.validate().is_err());
    let mut bad = cut.clone();
    for source in bad.sources.values_mut() {
        source.progress.eof = true;
        source.progress.idle = true;
    }
    for union in bad.unions.values_mut() {
        for input in &mut union.inputs {
            input.eof = true;
            input.idle = true;
        }
        union.emitted.eof = true;
        union.emitted.idle = true;
    }
    assert!(bad.validate().is_ok());
    assert!(
        bad.check_plan(&plan(true)).is_err(),
        "permanent ET EOF needs final watermark"
    );
    let mut bad = cut;
    bad.unions.get_mut(&3).unwrap().emitted.idle = true;
    assert!(bad.wrap().is_err());
}

#[test]
fn time_graph_context_checks_semantics_rounds_owners_and_ack_cursors() {
    let (legacy, ordered) = crate::kernel::time_graph_window_future_sizes();
    eprintln!("TIME_GRAPH_WINDOW_FUTURE_BYTES legacy={legacy} ordered={ordered}");
    #[cfg(all(target_arch = "x86_64", target_os = "linux", not(debug_assertions)))]
    assert!(
        legacy <= 3328,
        "cold time-graph state inflated the legacy window future"
    );
    assert!(
        std::mem::size_of::<StreamControl>() <= 24,
        "do not widen legacy ingress hot path"
    );
    let plan = plan(false);
    let cut = initial(&plan);
    let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
    let other = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
    let graph = GraphRuntime::new(cut.clone(), &plan, [7; 16], &owner).unwrap();
    assert!(graph.belongs_to(&owner));
    assert!(!graph.belongs_to(&other));
    let mut changed = plan.clone();
    for stage in &mut changed.stages {
        if let PhysicalStage::WindowAgg { spec, .. } = stage {
            if matches!(
                spec.kind,
                sparrow_model::WindowKind::TumblingProcessingTime { .. }
            ) {
                spec.kind = sparrow_model::WindowKind::TumblingProcessingTime { size_micros: 101 };
                break;
            }
        }
    }
    assert!(graph.check_plan(&changed).is_err());
    let mut mixed = plan.clone();
    if let PhysicalStage::WindowAgg { spec, .. } = &mut mixed.stages[4] {
        spec.kind = sparrow_model::WindowKind::TumblingEventTime { size_micros: 100 };
        spec.event_time_field = Some("ts".into());
    } else {
        panic!("test topology changed");
    }
    assert!(
        CheckpointPlan::from_physical(&mixed).is_err(),
        "mixed domains must not be admitted"
    );
    assert!(graph.begin_round(&cut).is_err());
    let mut next = cut.clone();
    next.sequence = 1;
    next.micros = 10;
    graph.begin_round(&next).unwrap();
    assert!(graph.begin_round(&next).is_err());
    assert!(
        graph.complete(1, &mut next).is_err(),
        "missing required ACKs"
    );
    for (&id, state) in &next.unions {
        graph.record_union(id, 1, state.clone()).unwrap();
    }
    for id in [10, 11] {
        graph
            .record_sink(id, 1, graph_cut::output_sequence([7; 16], id, 2).unwrap())
            .unwrap();
    }
    graph.complete(1, &mut next).unwrap();
    assert_eq!(next.outputs[&10], 2);
    assert!(graph
        .record_sink(10, 2, graph_cut::output_sequence([7; 16], 10, 1).unwrap())
        .is_err());
    assert!(graph
        .record_sink(10, 2, graph_cut::output_sequence([8; 16], 10, 2).unwrap())
        .is_err());
    next.sequence = 2;
    next.micros = 9;
    assert!(graph.begin_round(&next).is_err());
    next.micros = 10;
    graph.begin_round(&next).unwrap();
    drop(graph);
    assert_eq!(owner.usage().physical_bytes, 0);
    let mut bad = cut;
    bad.sources.get_mut(&1).unwrap().progress.idle = true;
    assert!(bad.validate().is_ok());
    assert!(bad.check_plan(&plan).is_err());
}

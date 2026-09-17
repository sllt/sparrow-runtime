use crate::*;
use sparrow_model::{ErrorCode, OperatorId, ResourceBudget, Row, Scalar};
use sparrow_plan::{bind_graph, physicalize, Catalog, GraphSpec, PhysicalPlan, PlanOptions};
use std::time::Duration;

fn plan(nodes: &str, fuse: bool) -> PhysicalPlan {
    let json = format!(
        r#"{{"version":1,"pipeline_id":90,"revision_id":1,"catalog":[{{"name":"s","fields":[{{"name":"v","type":"int64","nullable":false}}]}}],"nodes":{nodes}}}"#
    );
    let graph = GraphSpec::from_json(&json).unwrap();
    physicalize(
        &bind_graph(&graph, &Catalog::new()).unwrap(),
        &PlanOptions { fuse },
    )
}
fn kernel() -> Kernel {
    let mut options = KernelOptions::default();
    options.mailbox = MailboxConfig {
        max_items: 2,
        max_bytes: 4096,
    };
    options.rows_per_batch = 2;
    Kernel::new_with_job_budget(options, ResourceBudget::compact()).unwrap()
}
fn rows(values: &[i64]) -> Vec<Row> {
    values
        .iter()
        .map(|v| Row {
            values: vec![Scalar::Int64(*v)],
        })
        .collect()
}
fn output(capture: &SharedCapture) -> GraphOutput {
    GraphOutput {
        capture: capture.clone(),
        live: None,
        outbox: None,
    }
}
fn branch() -> PhysicalPlan {
    plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"branch","out":[3,4]},{"id":3,"kind":"capture_sink","name":"a"},{"id":4,"kind":"capture_sink","name":"b"}]"#,
        true,
    )
}

#[test]
fn k3_broadcast_delivers_all_rows_to_each_sink() {
    let kernel = kernel();
    let a = SharedCapture::new();
    let b = SharedCapture::new();
    let mut request = JobRequest::new(branch(), rows(&[1, 2, 3, 4, 5]), SharedCapture::disabled());
    request.graph_outputs.insert(3.into(), output(&a));
    request.graph_outputs.insert(4.into(), output(&b));
    let stats = kernel.run(request).unwrap();
    assert_eq!(a.rows(), b.rows());
    assert_eq!(a.row_count(), 5);
    assert_eq!(stats.ingested_rows, 5);
    assert_eq!(kernel.live_tasks(), 0);
    assert_eq!(kernel.admitted_jobs(), 0);
}

#[test]
fn k3_route_first_all_and_default_are_explicit() {
    for (mode, expected_b) in [("first_match", 1), ("all_match", 3)] {
        let nodes = format!(
            r#"[{{"id":1,"kind":"memory_source","table":"s","out":[2]}},{{"id":2,"kind":"route","route_mode":"{mode}","routes":[{{"predicate":{{"k":"bin","op":">","left":{{"k":"col","name":"v"}},"right":{{"k":"lit","value":{{"t":"int64","v":2}}}}}},"to":3}},{{"predicate":{{"k":"bin","op":">","left":{{"k":"col","name":"v"}},"right":{{"k":"lit","value":{{"t":"int64","v":1}}}}}},"to":4}}],"default_out":5,"out":[3,4,5]}},{{"id":3,"kind":"capture_sink","name":"a"}},{{"id":4,"kind":"capture_sink","name":"b"}},{{"id":5,"kind":"capture_sink","name":"default"}}]"#
        );
        let kernel = kernel();
        let a = SharedCapture::new();
        let b = SharedCapture::new();
        let d = SharedCapture::new();
        let mut request = JobRequest::new(
            plan(&nodes, true),
            rows(&[1, 2, 3, 4]),
            SharedCapture::disabled(),
        );
        for (id, c) in [(3, &a), (4, &b), (5, &d)] {
            request.graph_outputs.insert(id.into(), output(c));
        }
        kernel.run(request).unwrap();
        assert_eq!(a.row_count(), 2);
        assert_eq!(b.row_count(), expected_b);
        assert_eq!(d.rows(), vec![vec![Scalar::Int64(1)]]);
    }
}

#[test]
fn k3_union_preserves_each_source_order_and_accepts_empty_source() {
    let nodes = r#"[{"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"capture_sink","name":"out"}]"#;
    for second in [vec![], vec![2, 4, 6]] {
        let kernel = kernel();
        let capture = SharedCapture::new();
        let mut request = JobRequest::new(plan(nodes, true), vec![], capture.clone());
        request.graph_inputs.insert(
            1.into(),
            GraphInput {
                rows: rows(&[1, 3, 5]),
                ..Default::default()
            },
        );
        request.graph_inputs.insert(
            2.into(),
            GraphInput {
                rows: rows(&second),
                ..Default::default()
            },
        );
        kernel.run(request).unwrap();
        let values: Vec<_> = capture
            .rows()
            .into_iter()
            .map(|r| {
                if let Scalar::Int64(v) = r[0] {
                    v
                } else {
                    panic!()
                }
            })
            .collect();
        assert_eq!(
            values
                .iter()
                .copied()
                .filter(|v| v % 2 == 1)
                .collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        assert_eq!(
            values
                .iter()
                .copied()
                .filter(|v| v % 2 == 0)
                .collect::<Vec<_>>(),
            second
        );
    }
}

#[test]
fn k3_broadcast_cancel_while_required_branch_is_full_joins_and_refunds() {
    let kernel = kernel();
    let capture = SharedCapture::new();
    capture.stall.stall();
    let mut request = JobRequest::new(
        branch(),
        rows(&(0..40).collect::<Vec<_>>()),
        SharedCapture::disabled(),
    );
    request.graph_outputs.insert(3.into(), output(&capture));
    let handle = kernel.submit(request).unwrap();
    let owner = handle.memory_owner();
    let observer = handle.mailbox_observer();
    kernel.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observer
                    .snapshot()
                    .edges
                    .iter()
                    .any(|e| e.queue.waiting_senders > 0)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), handle.stop())
            .await
            .unwrap()
            .unwrap();
    });
    assert_eq!(kernel.live_tasks(), 0);
    assert_eq!(kernel.admitted_jobs(), 0);
    assert!(observer
        .snapshot()
        .edges
        .iter()
        .all(|e| e.queue.credits_reserved_bytes == 0 && e.queue.accounting_errors_total == 0));
    drop(observer);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn k3_embedded_invalid_topology_and_missing_inputs_refused_before_admission() {
    let kernel = kernel();
    let mut p = branch();
    p.edges.as_mut().unwrap()[0].to = 999;
    let err = kernel
        .submit(JobRequest::new(p, vec![], SharedCapture::disabled()))
        .err()
        .unwrap();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert_eq!(kernel.admitted_jobs(), 0);
    let mut request = JobRequest::new(branch(), vec![], SharedCapture::disabled());
    request
        .graph_inputs
        .insert(OperatorId::new(99), GraphInput::default());
    assert!(kernel.submit(request).is_err());
    assert_eq!(kernel.admitted_jobs(), 0);
}

#[test]
fn k3_real_edges_are_exposed_not_linear_neighbors() {
    let kernel = kernel();
    let handle = kernel
        .submit(JobRequest::new(
            branch(),
            rows(&[1]),
            SharedCapture::disabled(),
        ))
        .unwrap();
    let observer = handle.mailbox_observer();
    kernel.block_on(handle.wait()).unwrap();
    let edges = observer.snapshot().edges;
    assert_eq!(edges.len(), 3);
    assert_eq!(edges.iter().filter(|e| e.from_kind == "branch").count(), 2);
    assert!(edges
        .iter()
        .filter(|e| e.from_kind == "branch")
        .all(|e| e.to_kind == "sink"));
}

#[test]
fn k3_union_barrier_blocks_fast_input_and_abandonment_unblocks_it() {
    use std::sync::Arc;
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"capture_sink","name":"out"}]"#,
        true,
    );
    for abandon in [false, true] {
        let kernel = kernel();
        let capture = SharedCapture::new();
        let manifest = Arc::new(sparrow_plan::CheckpointPlan::from_physical(&p).unwrap());
        assert_eq!(manifest.participants().len(), 3);
        assert_eq!(
            sparrow_plan::CheckpointPlan::decode(&manifest.encode().unwrap()).unwrap(),
            *manifest
        );
        kernel.block_on(async {
            let (a, ar) = sparrow_io::observed::channel(4);
            let (b, br) = sparrow_io::observed::channel(4);
            let acks = AlignedAcks::default();
            let mut request =
                JobRequest::new(p.clone(), vec![], capture.clone()).with_aligned(AlignedJob {
                    restore: None,
                    pipeline: Some(PipelineRestore {
                        iot: Vec::new(),
                        plan: manifest.clone(),
                        generation: [4; 16],
                        restore: None,
                    }),
                    acks: acks.clone(),
                    outbox: Arc::new(sparrow_model::InflightCounter::new()),
                });
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
            let cut = acks.begin(1).unwrap();
            a.send(IngressEvent::Row(rows(&[1]).remove(0)))
                .await
                .unwrap();
            a.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: 1,
            }))
            .await
            .unwrap();
            a.send(IngressEvent::Row(rows(&[2]).remove(0)))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while capture.row_count() < 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                capture.row_count(),
                1,
                "fast input after barrier must not pass UnionAll"
            );
            let cut = if abandon {
                drop(cut);
                tokio::time::timeout(Duration::from_secs(2), async {
                    while capture.row_count() < 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let cut = acks.begin(2).unwrap();
                b.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                    checkpoint_id: 1,
                }))
                .await
                .unwrap();
                a.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                    checkpoint_id: 2,
                }))
                .await
                .unwrap();
                cut
            } else {
                cut
            };
            b.send(IngressEvent::Row(rows(&[10]).remove(0)))
                .await
                .unwrap();
            b.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: if abandon { 2 } else { 1 },
            }))
            .await
            .unwrap();
            let frozen = cut.wait_participants(Duration::from_secs(2)).await.unwrap();
            let mut source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory(
                "graph", 0, 0,
            ));
            source.identity.kind = "file-dag-v1".into();
            let encoded =
                PipelineSnapshot::encode_frozen(1, &source, 0, 1, &manifest, frozen, &owner, 1024)
                    .unwrap();
            let restored = PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap();
            assert!(restored.windows.is_empty());
            assert_eq!(restored.plan.participants().len(), 3);
            a.send(IngressEvent::Control(StreamControl::EndOfInput))
                .await
                .unwrap();
            b.send(IngressEvent::Control(StreamControl::EndOfInput))
                .await
                .unwrap();
            drop(a);
            drop(b);
            tokio::time::timeout(Duration::from_secs(2), handle.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(capture.row_count(), 3);
            drop(encoded);
            assert_eq!(owner.usage().physical_bytes, 0);
        });
    }
}

#[test]
fn k3_best_effort_slow_branch_drops_and_control_overflow_detaches_without_stopping_required() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"branch","best_effort":[4],"out":[3,4]},{"id":3,"kind":"capture_sink","name":"required"},{"id":4,"kind":"best_effort_sink","name":"optional"}]"#,
        true,
    );
    let kernel = kernel();
    let required = SharedCapture::new();
    let optional = SharedCapture::new();
    optional.stall.stall();
    let mut request = JobRequest::new(
        p,
        rows(&(0..40).collect::<Vec<_>>()),
        SharedCapture::disabled(),
    )
    .with_controls(vec![StreamControl::Watermark {
        input: 0,
        wm_micros: 100,
    }]);
    request.graph_outputs.insert(3.into(), output(&required));
    request.graph_outputs.insert(4.into(), output(&optional));
    let handle = kernel.submit(request).unwrap();
    kernel.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), handle.wait())
            .await
            .unwrap()
            .unwrap();
    });
    assert_eq!(required.row_count(), 40);
    assert!(kernel.metrics.snapshot().graph_dropped_rows > 0);
    assert!(kernel.metrics.snapshot().graph_detached_branches > 0);
    assert_eq!(kernel.live_tasks(), 0);
}

#[test]
fn k3_rule_reject_side_port_has_original_schema_and_rows() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"filter","predicate":{"k":"bin","op":">","left":{"k":"col","name":"v"},"right":{"k":"lit","value":{"t":"int64","v":1}}},"side_output":{"kind":"rule_reject","to":4,"full":"backpressure"},"out":[3,4]},{"id":3,"kind":"capture_sink","name":"accepted"},{"id":4,"kind":"capture_sink","name":"rejected"}]"#,
        true,
    );
    let kernel = kernel();
    let accepted = SharedCapture::new();
    let rejected = SharedCapture::new();
    let mut request = JobRequest::new(p, rows(&[1, 2, 3]), SharedCapture::disabled());
    request.graph_outputs.insert(3.into(), output(&accepted));
    request.graph_outputs.insert(4.into(), output(&rejected));
    kernel.run(request).unwrap();
    assert_eq!(accepted.row_count(), 2);
    assert_eq!(rejected.rows(), vec![vec![Scalar::Int64(1)]]);
    assert_eq!(kernel.metrics.snapshot().graph_side_rows, 1);
}

#[test]
fn k3_late_side_port_preserves_input_schema_and_control_order() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","event_time_field":"v","out":[2]},{"id":2,"kind":"window_agg","window":{"kind":"tumble_et","size_micros":10},"event_time_field":"v","keys":[],"aggs":[{"fn":"count","alias":"n"}],"side_output":{"kind":"late","to":4,"full":"backpressure"},"out":[3,4]},{"id":3,"kind":"capture_sink","name":"finals"},{"id":4,"kind":"capture_sink","name":"late"}]"#,
        true,
    );
    let kernel = kernel();
    let finals = SharedCapture::new();
    let late = SharedCapture::new();
    let mut request = JobRequest::new(p, rows(&[1, 20, 2]), SharedCapture::disabled())
        .with_controls(vec![StreamControl::Watermark {
            input: 0,
            wm_micros: 100,
        }]);
    request.graph_outputs.insert(3.into(), output(&finals));
    request.graph_outputs.insert(4.into(), output(&late));
    kernel.run(request).unwrap();
    assert_eq!(finals.row_count(), 2);
    assert_eq!(late.rows(), vec![vec![Scalar::Int64(2)]]);
    assert_eq!(kernel.metrics.snapshot().graph_side_rows, 1);
}

#[test]
fn k3_multi_input_provenance_survives_union_and_projection() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"project","exprs":[{"expr":{"k":"col","name":"v"},"alias":"v"}],"out":[5]},{"id":5,"kind":"capture_sink","name":"out"}]"#,
        true,
    );
    let kernel = kernel();
    kernel.block_on(async {
        let (tx, mut rx) = sparrow_io::observed::channel(8);
        let mut request = JobRequest::new(p, vec![], SharedCapture::disabled());
        request.graph_inputs.insert(
            1.into(),
            GraphInput {
                rows: rows(&[1, 3]),
                ..Default::default()
            },
        );
        request.graph_inputs.insert(
            2.into(),
            GraphInput {
                rows: rows(&[2, 4]),
                ..Default::default()
            },
        );
        request.graph_outputs.insert(
            5.into(),
            GraphOutput {
                capture: SharedCapture::disabled(),
                live: Some(tx),
                outbox: None,
            },
        );
        let handle = kernel.submit(request).unwrap();
        let mut seen = std::collections::BTreeMap::new();
        while let Some(batch) = rx.recv().await {
            seen.insert(
                batch.source_operator().unwrap().raw(),
                batch
                    .rows()
                    .iter()
                    .map(|r| r.values[0].clone())
                    .collect::<Vec<_>>(),
            );
        }
        handle.wait().await.unwrap();
        assert_eq!(seen[&1], vec![Scalar::Int64(1), Scalar::Int64(3)]);
        assert_eq!(seen[&2], vec![Scalar::Int64(2), Scalar::Int64(4)]);
    });
}

#[test]
fn k3_event_time_union_does_not_advance_past_an_uninitialized_input() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","event_time_field":"v","out":[3]},{"id":2,"kind":"memory_source","table":"s","event_time_field":"v","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"window_agg","window":{"kind":"tumble_et","size_micros":10},"event_time_field":"v","keys":[],"aggs":[{"fn":"count","alias":"n"}],"out":[5]},{"id":5,"kind":"capture_sink","name":"out"}]"#,
        true,
    );
    let kernel = kernel();
    let capture = SharedCapture::new();
    kernel.block_on(async {
        let (a, ar) = sparrow_io::observed::channel(4);
        let (b, br) = sparrow_io::observed::channel(4);
        let mut request = JobRequest::new(p, vec![], capture.clone());
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
        a.send(IngressEvent::Row(rows(&[100]).remove(0)))
            .await
            .unwrap();
        for _ in 0..30 {
            tokio::task::yield_now().await;
        }
        assert_eq!(capture.row_count(), 0);
        b.send(IngressEvent::Row(rows(&[1]).remove(0)))
            .await
            .unwrap();
        b.send(IngressEvent::Row(rows(&[20]).remove(0)))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while capture.row_count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        a.send(IngressEvent::Control(StreamControl::EndOfInput))
            .await
            .unwrap();
        b.send(IngressEvent::Control(StreamControl::EndOfInput))
            .await
            .unwrap();
        drop(a);
        drop(b);
        tokio::time::timeout(Duration::from_secs(2), handle.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            capture.seen_count(),
            3,
            "all three distinct windows, including the initially slow source, must survive"
        );
    });
}

#[test]
fn k3_channel_failure_is_not_eof_and_all_idle_does_not_finalize_windows() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","event_time_field":"v","out":[3]},{"id":2,"kind":"memory_source","table":"s","event_time_field":"v","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"window_agg","window":{"kind":"tumble_et","size_micros":10},"event_time_field":"v","keys":[],"aggs":[{"fn":"count","alias":"n"}],"out":[5]},{"id":5,"kind":"capture_sink","name":"out"}]"#,
        true,
    );
    for clean in [false, true] {
        let kernel = kernel();
        let capture = SharedCapture::new();
        kernel.block_on(async {
            let (a, ar) = sparrow_io::observed::channel(4);
            let (b, br) = sparrow_io::observed::channel(4);
            let mut request = JobRequest::new(p.clone(), vec![], capture.clone());
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
            let observer = handle.mailbox_observer();
            for tx in [&a, &b] {
                tx.send(IngressEvent::Row(rows(&[1]).remove(0)))
                    .await
                    .unwrap();
                tx.send(IngressEvent::Control(StreamControl::Idle { input: 0 }))
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let state = observer.snapshot();
                    if state
                        .edges
                        .iter()
                        .filter(|e| e.to_kind == "union_all")
                        .all(|e| e.queue.input_progress.as_ref().unwrap().idle)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                capture.row_count(),
                0,
                "all idle must not invent final time"
            );
            if clean {
                for tx in [&a, &b] {
                    tx.send(IngressEvent::Control(StreamControl::EndOfInput))
                        .await
                        .unwrap();
                }
            }
            drop(a);
            drop(b);
            let result = tokio::time::timeout(Duration::from_secs(2), handle.wait())
                .await
                .unwrap();
            if clean {
                result.unwrap();
                assert_eq!(capture.row_count(), 1);
            } else {
                assert!(result.is_err());
                assert_eq!(
                    capture.row_count(),
                    0,
                    "unmarked input failure must never finalize state"
                );
            }
            drop(observer);
            assert_eq!(kernel.live_tasks(), 0);
        });
    }
}

#[test]
fn k3_permanent_eof_still_participates_in_aligned_barriers() {
    use std::sync::Arc;
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},{"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"window_agg","window":{"kind":"count","size":3},"keys":[],"aggs":[{"fn":"count","alias":"n"}],"out":[5]},{"id":5,"kind":"capture_sink","name":"out"}]"#,
        true,
    );
    let manifest = Arc::new(sparrow_plan::CheckpointPlan::from_physical(&p).unwrap());
    let kernel = kernel();
    kernel.block_on(async {
        let (a, ar) = sparrow_io::observed::channel(4);
        let (b, br) = sparrow_io::observed::channel(4);
        let acks = AlignedAcks::default();
        let mut request =
            JobRequest::new(p, vec![], SharedCapture::disabled()).with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    iot: Vec::new(),
                    plan: manifest.clone(),
                    generation: [6; 16],
                    restore: None,
                }),
                acks: acks.clone(),
                outbox: Arc::new(sparrow_model::InflightCounter::new()),
            });
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
        for tx in [&a, &b] {
            tx.send(IngressEvent::Row(rows(&[1]).remove(0)))
                .await
                .unwrap();
            tx.send(IngressEvent::Control(StreamControl::EndOfInput))
                .await
                .unwrap();
        }
        let cut = acks.begin(1).unwrap();
        for tx in [&a, &b] {
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: 1,
            }))
            .await
            .unwrap();
        }
        let frozen = cut.wait_participants(Duration::from_secs(2)).await.unwrap();
        let mut position =
            sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("fixture", 0, 0));
        position.identity.kind = "file-dag-v1".into();
        let encoded =
            PipelineSnapshot::encode_frozen(1, &position, 2, 1, &manifest, frozen, &owner, 1024)
                .unwrap();
        let restored = PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap();
        assert_eq!(restored.windows[0].entries[0].count, 2);
        drop(a);
        drop(b);
        handle.wait().await.unwrap();
    });
}

#[test]
fn k3_processing_time_eof_waits_for_timer_output_before_downstream_eof() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"branch","out":[3,4]},{"id":3,"kind":"window_agg","window":{"kind":"tumble_pt","size_micros":10},"keys":[],"aggs":[{"fn":"count","alias":"n"}],"out":[5]},{"id":4,"kind":"capture_sink","name":"raw"},{"id":5,"kind":"capture_sink","name":"window"}]"#,
        true,
    );
    let kernel = kernel();
    let capture = SharedCapture::new();
    let clock = RuntimeClock::virtual_clock(sparrow_model::SharedVirtualClock::new(0));
    let mut request =
        JobRequest::new(p, rows(&[1, 2]), SharedCapture::disabled()).with_clock(clock.clone());
    request.graph_outputs.insert(5.into(), output(&capture));
    let handle = kernel.submit(request).unwrap();
    let observer = handle.mailbox_observer();
    kernel.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observer
                    .snapshot()
                    .edges
                    .iter()
                    .any(|e| e.to_kind == "window" && e.queue.input_progress.as_ref().unwrap().eof)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(capture.row_count(), 0);
        assert!(!handle.is_finished());
        clock.advance_virtual(10);
        tokio::time::timeout(Duration::from_secs(2), handle.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(capture.row_count(), 1);
    });
}

#[test]
fn k3_nested_best_effort_cancellation_reaches_stalled_descendants() {
    let p = plan(
        r#"[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"branch","best_effort":[4],"out":[3,4]},{"id":3,"kind":"capture_sink","name":"required"},{"id":4,"kind":"branch","best_effort":[5],"out":[5,6]},{"id":5,"kind":"best_effort_sink","name":"nested"},{"id":6,"kind":"best_effort_sink","name":"sibling"}]"#,
        true,
    );
    let leaf_indices: Vec<_> = p
        .stages
        .iter()
        .enumerate()
        .filter_map(|(i, stage)| {
            matches!(stage, sparrow_plan::PhysicalStage::BestEffortSink { .. }).then_some(i)
        })
        .collect();
    let kernel = kernel();
    let main = SharedCapture::new();
    let a = SharedCapture::new();
    let b = SharedCapture::new();
    a.stall.stall();
    b.stall.stall();
    kernel.block_on(async {
        let (tx, rx) = sparrow_io::observed::channel(8);
        let mut request = JobRequest::new(p, vec![], SharedCapture::disabled());
        request.graph_inputs.insert(
            1.into(),
            GraphInput {
                events: Some(rx),
                ..Default::default()
            },
        );
        for (id, capture) in [(3, &main), (5, &a), (6, &b)] {
            request.graph_outputs.insert(id.into(), output(capture));
        }
        let handle = kernel.submit(request).unwrap();
        let cancel = handle.cancellation();
        let observer = handle.mailbox_observer();
        for row in rows(&[1, 2]) {
            tx.send(IngressEvent::Row(row)).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = observer.snapshot();
                if leaf_indices.iter().all(|i| {
                    snapshot
                        .edges
                        .iter()
                        .any(|e| e.to_stage == *i && e.queue.consumer_held.rows > 0)
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for row in rows(&(3..=42).collect::<Vec<_>>()) {
            tx.send(IngressEvent::Row(row)).await.unwrap();
        }
        tx.send(IngressEvent::Control(StreamControl::EndOfInput))
            .await
            .unwrap();
        drop(tx);
        let mut waiting = Box::pin(handle.wait());
        let completed = match tokio::time::timeout(Duration::from_secs(2), waiting.as_mut()).await {
            Ok(result) => {
                result.unwrap();
                true
            }
            Err(_) => {
                cancel.cancel();
                let _ = waiting.await;
                false
            }
        };
        assert!(
            completed,
            "outer optional branch cancellation must reach its stalled nested child"
        );
        assert_eq!(main.row_count(), 42);
        drop(observer);
        assert_eq!(kernel.live_tasks(), 0);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k3_stall_release_registration_race_does_not_lose_wakeup() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..1000 {
            let gate = StallGate::default();
            gate.stall();
            let waiter = gate.clone();
            let task = tokio::spawn(async move {
                waiter
                    .wait_if_stalled(&tokio_util::sync::CancellationToken::new())
                    .await;
            });
            tokio::task::yield_now().await;
            gate.release();
            task.await.unwrap();
        }
    })
    .await
    .expect("stalled capture must observe release even during waiter registration");
}

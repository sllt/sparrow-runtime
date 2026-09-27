//! Independent golden boundaries and finite mathematical reference checks.
use crate::buffered_window::{BufferedWindow, Ingest};
use crate::*;
use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, ErrorCode, Field, InputId, MemoryOwner, OperatorId, ResourceBudget, Row,
    Scalar, Schema, SharedVirtualClock, WindowKind,
};
use sparrow_plan::{AggCall, WindowSpec};
use std::sync::Arc;
use std::time::Duration;

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "k", DataType::Utf8, false),
            Field::new(2, "v", DataType::Int64, true),
            Field::new(3, "ts", DataType::Int64, false),
        ],
    )
    .unwrap()
}
fn row(k: &str, v: i64, ts: i64) -> Row {
    Row {
        values: vec![Scalar::utf8(k), Scalar::Int64(v), Scalar::Int64(ts)],
    }
}
fn spec(kind: WindowKind) -> WindowSpec {
    let mut spec = WindowSpec::new(
        kind,
        vec!["k".into()],
        vec![
            AggCall::count_star("n"),
            AggCall::new(AggFn::Sum, Some(Expr::Column { name: "v".into() }), "total"),
            AggCall::new(
                AggFn::Min,
                Some(Expr::Column { name: "v".into() }),
                "minimum",
            ),
            AggCall::new(
                AggFn::Max,
                Some(Expr::Column { name: "v".into() }),
                "maximum",
            ),
        ],
    );
    if kind.uses_event_time() {
        spec = spec.event_time("ts", 0);
    }
    spec
}
fn buffered(kind: WindowKind) -> (BufferedWindow, Arc<MemoryOwner>) {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    (
        BufferedWindow::new(spec(kind), schema(), owner.clone(), 64, 64, true).unwrap(),
        owner,
    )
}
fn values(batch: sparrow_model::RowBatch) -> Vec<Vec<Scalar>> {
    batch.rows().iter().map(|r| r.values.clone()).collect()
}
fn take(op: &mut BufferedWindow, time: i64) -> Vec<Vec<Scalar>> {
    let mut output = vec![];
    let mut steps = 0;
    while op.due(time) {
        steps += 1;
        assert!(steps < 10000, "deadline must make progress");
        if let Some(batch) = op.take_due(time).unwrap() {
            output.extend(values(batch));
        }
    }
    output
}
fn accepted(op: &mut BufferedWindow, row: &Row, now: i64) -> Vec<Vec<Scalar>> {
    match op.push(row, now).unwrap() {
        Ingest::Accepted(Some(batch)) => values(batch),
        Ingest::Accepted(None) => vec![],
        _ => panic!("unexpected dropped row"),
    }
}
fn golden(k: &str, start: i64, end: i64, data: &[i64]) -> Vec<Scalar> {
    vec![
        Scalar::utf8(k),
        Scalar::Int64(start),
        Scalar::Int64(end),
        Scalar::Int64(data.len() as i64),
        Scalar::Int64(data.iter().sum()),
        Scalar::Int64(*data.iter().min().unwrap()),
        Scalar::Int64(*data.iter().max().unwrap()),
    ]
}

#[test]
fn windows_sliding_count_size_step_independent_keys_and_last_full_only() {
    let (mut op, owner) = buffered(WindowKind::sliding_count(5, 2).unwrap());
    let mut output = vec![];
    for n in 1..=9 {
        output.extend(accepted(&mut op, &row("a", n, 0), -n)); // count is independent of clocks
        if n <= 3 {
            assert!(accepted(&mut op, &row("b", 100, 0), 0).is_empty());
        }
    }
    assert_eq!(
        output,
        vec![
            golden("a", 1, 6, &[2, 3, 4, 5, 6]),
            golden("a", 3, 8, &[4, 5, 6, 7, 8])
        ]
    );
    assert!(!op.due(i64::MAX));
    assert_eq!(op.retention_bytes(), owner.usage().retention_bytes);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_sliding_pt_prefix_equality_eviction_and_clock_rollback() {
    let (mut op, owner) = buffered(WindowKind::sliding(10, 0, false).unwrap());
    assert_eq!(
        accepted(&mut op, &row("a", 1, 99), 10),
        vec![golden("a", 1, 11, &[1])]
    );
    assert_eq!(
        accepted(&mut op, &row("a", 2, 99), 10),
        vec![golden("a", 1, 11, &[1, 2])]
    );
    assert_eq!(
        accepted(&mut op, &row("a", 3, 99), 9),
        vec![golden("a", 1, 11, &[1, 2, 3])]
    );
    assert!(take(&mut op, 20).is_empty());
    assert_eq!(op.key_count(), 0);
    assert_eq!(
        accepted(&mut op, &row("a", 4, 99), 20),
        vec![golden("a", 11, 21, &[4])]
    );
    assert!(take(&mut op, 30).is_empty());
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_sliding_delay_includes_upper_equality_before_exclusive_close() {
    for et in [false, true] {
        let (mut op, owner) = buffered(WindowKind::sliding(10, 5, et).unwrap());
        for (t, value) in [(10, 1), (15, 2), (16, 4)] {
            // At t=16, emit t=10's trigger before accepting this outside row.
            if !et && t == 16 {
                assert_eq!(take(&mut op, t), vec![golden("a", 1, 16, &[1, 2])]);
            }
            assert!(accepted(&mut op, &row("a", value, t), t).is_empty());
        }
        if et {
            assert!(take(&mut op, 15).is_empty());
            assert_eq!(take(&mut op, 16), vec![golden("a", 1, 16, &[1, 2])]);
        }
        assert_eq!(
            take(&mut op, i64::MAX),
            vec![
                golden("a", 6, 21, &[1, 2, 4]),
                golden("a", 7, 22, &[1, 2, 4])
            ]
        );
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[test]
fn windows_et_sliding_permutations_match_finite_interval_reference() {
    for delay in [0, 3, 15] {
        let (mut op, owner) = buffered(WindowKind::sliding(10, delay, true).unwrap());
        let records: Vec<_> = (0..31).map(|i| (i * 3, i + 1)).collect();
        for i in 0..31 {
            let (ts, v) = records[(i * 17) % 31];
            accepted(&mut op, &row("a", v, ts), 1000);
        }
        let actual = take(&mut op, i64::MAX);
        let expected: Vec<_> = records
            .iter()
            .map(|(ts, _)| {
                let members: Vec<_> = records
                    .iter()
                    .filter(|(t, _)| *t > ts - 10 && *t <= ts + delay)
                    .map(|(_, v)| *v)
                    .collect();
                golden("a", ts - 9, ts + delay + 1, &members)
            })
            .collect();
        assert_eq!(actual, expected, "delay={delay}");
        assert_eq!(op.key_count(), 0);
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[test]
fn windows_session_out_of_order_bridge_and_max_duration_resegment() {
    let (mut op, owner) = buffered(WindowKind::session(10, 100, true).unwrap());
    for (ts, v) in [(0, 1), (18, 3), (9, 2)] {
        accepted(&mut op, &row("a", v, ts), 100);
    }
    assert!(take(&mut op, 27).is_empty());
    assert_eq!(take(&mut op, 28), vec![golden("a", 0, 28, &[1, 2, 3])]);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);

    let (mut op, owner) = buffered(WindowKind::session(10, 20, true).unwrap());
    // An earlier arrival moves the maximum endpoint from 35 to 25, splitting
    // the previously connected [15,35) session; accumulator merge alone fails.
    for ts in [15, 24, 30, 5, 14] {
        accepted(&mut op, &row("a", ts, ts), 100);
    }
    assert_eq!(
        take(&mut op, i64::MAX),
        vec![
            golden("a", 5, 25, &[5, 14, 15, 24]),
            golden("a", 30, 40, &[30])
        ]
    );
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_session_pt_gap_and_max_equality_create_new_sessions() {
    let (mut op, owner) = buffered(WindowKind::session(10, 20, false).unwrap());
    for t in [0, 9, 18] {
        assert!(take(&mut op, t).is_empty());
        accepted(&mut op, &row("a", t, 0), t);
    }
    assert_eq!(take(&mut op, 20), vec![golden("a", 0, 20, &[0, 9, 18])]);
    accepted(&mut op, &row("a", 20, 0), 20);
    assert_eq!(take(&mut op, 30), vec![golden("a", 20, 30, &[20])]);
    accepted(&mut op, &row("a", 30, 0), 30);
    assert_eq!(take(&mut op, 40), vec![golden("a", 30, 40, &[30])]);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_buffer_bounds_watermarks_future_overflow_and_refunds() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut s = spec(WindowKind::session(10, 100, true).unwrap());
    s.max_buffered_rows = 2;
    let mut op = BufferedWindow::new(s, schema(), owner.clone(), 1, 1, true).unwrap();
    accepted(&mut op, &row("a", 1, 10), 10);
    op.watermark(InputId::SINGLE, 10).unwrap();
    assert!(matches!(
        op.push(&row("a", 0, 9), 10).unwrap(),
        Ingest::Late
    ));
    assert!(matches!(
        op.push(&row("a", 0, i64::MAX), 10).unwrap(),
        Ingest::Future
    ));
    accepted(&mut op, &row("a", 2, 10), 10);
    assert_eq!(
        op.push(&row("a", 3, 11), 11).err().unwrap().code,
        ErrorCode::BoundExceeded
    );
    assert_eq!(
        op.push(&row("b", 3, 11), 11).err().unwrap().code,
        ErrorCode::BoundExceeded
    );
    assert_eq!(take(&mut op, 20), vec![golden("a", 10, 20, &[1, 2])]);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
    let (mut op, owner) = buffered(WindowKind::sliding(10, 0, false).unwrap());
    assert_eq!(
        op.push(&row("a", 1, 0), i64::MAX).err().unwrap().code,
        ErrorCode::IntegerOverflow
    );
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_low_credit_rejects_without_retaining_uncredited_payload() {
    let owner = MemoryOwner::new(ResourceBudget {
        retention_bytes: 100,
        ..ResourceBudget::compact()
    });
    let mut op = BufferedWindow::new(
        spec(WindowKind::sliding_count(2, 1).unwrap()),
        schema(),
        owner.clone(),
        1,
        1,
        true,
    )
    .unwrap();
    assert!(op.push(&row("large-key", 1, 0), 0).is_err());
    assert_eq!(op.key_count(), 0);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(owner.accounting_errors_total(), 0);
}

#[test]
fn windows_buffered_null_avg_min_payload_and_integer_overflow() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut s = spec(WindowKind::sliding_count(2, 1).unwrap());
    s.aggs = vec![
        AggCall::count_star("all"),
        AggCall::new(
            AggFn::Count,
            Some(Expr::Column { name: "v".into() }),
            "present",
        ),
        AggCall::new(AggFn::Avg, Some(Expr::Column { name: "v".into() }), "mean"),
        AggCall::new(AggFn::Min, Some(Expr::Column { name: "k".into() }), "text"),
    ];
    let mut op = BufferedWindow::new(s, schema(), owner.clone(), 2, 2, true).unwrap();
    let mut null = row("detached", 0, 0);
    null.values[1] = Scalar::Null;
    accepted(&mut op, &null, 0);
    drop(null);
    assert_eq!(
        accepted(&mut op, &row("detached", 6, 0), 0),
        vec![vec![
            Scalar::utf8("detached"),
            Scalar::Int64(0),
            Scalar::Int64(2),
            Scalar::Int64(2),
            Scalar::Int64(1),
            Scalar::Float64(6.0),
            Scalar::utf8("detached")
        ]]
    );
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
    let (mut op, owner) = buffered(WindowKind::sliding_count(2, 1).unwrap());
    accepted(&mut op, &row("a", i64::MAX, 0), 0);
    assert_eq!(
        op.push(&row("a", 1, 0), 0).err().unwrap().code,
        ErrorCode::IntegerOverflow
    );
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_uninitialized_idle_active_watermarks_never_reopen_finals() {
    let (mut op, owner) = buffered(WindowKind::session(10, 100, true).unwrap());
    accepted(&mut op, &row("a", 1, 10), 100);
    assert_eq!(op.progress(), None);
    op.activity(InputId(1), true).unwrap();
    op.watermark(InputId::SINGLE, 30).unwrap();
    assert_eq!(op.progress(), None);
    op.activity(InputId(1), false).unwrap();
    assert_eq!(op.progress(), Some(30));
    assert_eq!(take(&mut op, 30), vec![golden("a", 10, 20, &[1])]);
    op.watermark(InputId(1), 0).unwrap();
    op.activity(InputId(1), true).unwrap();
    assert_eq!(op.progress(), Some(30));
    assert!(matches!(
        op.push(&row("a", 2, 20), 100).unwrap(),
        Ingest::Late
    ));
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn windows_hopping_pt_overlapping_outputs_have_no_hidden_key_suffix() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = crate::window::WindowOperator::new(
        OperatorId::new(2),
        spec(WindowKind::hopping_pt(10, 5).unwrap()),
        schema(),
        owner.clone(),
        64,
        64,
    )
    .unwrap();
    let batch = crate::window::finish_rows_metered(&schema(), vec![row("a", 1, 0)], &owner)
        .unwrap()
        .unwrap();
    assert!(op.on_batch(&batch, 7).unwrap().finals.is_empty());
    drop(batch);
    assert_eq!(
        op.fire_due(10)
            .unwrap()
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![golden("a", 0, 10, &[1])]
    );
    assert_eq!(
        op.fire_due(15)
            .unwrap()
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![golden("a", 5, 15, &[1])]
    );
    let batch = crate::window::finish_rows_metered(&schema(), vec![row("a", 2, 0)], &owner)
        .unwrap()
        .unwrap();
    assert!(op.on_batch(&batch, 4).unwrap().finals.is_empty());
    drop(batch);
    assert_eq!(
        op.fire_due(20)
            .unwrap()
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![golden("a", 10, 20, &[2])]
    );
    assert_eq!(
        op.fire_due(25)
            .unwrap()
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![golden("a", 15, 25, &[2])]
    );
    assert_eq!(
        op.restore_freeze(&op.freeze()).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    op.cleanup();
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

fn graph(window: serde_json::Value, et: bool, branch: bool) -> sparrow_plan::PhysicalPlan {
    use serde_json::json;
    let mut source = json!({"id":1,"kind":"memory_source","table":"s","out":[2]});
    if et {
        source["event_time_field"] = json!("ts");
        source["out_of_orderness_micros"] = json!(1000);
    }
    let mut node = json!({"id":2,"kind":"window_agg","keys":["k"],"window":window,
        "aggs":[{"fn":"count","alias":"n"}],"out":[3]});
    if et {
        node["event_time_field"] = json!("ts");
    }
    let mut nodes = vec![source, node];
    if branch {
        nodes.extend([
            json!({"id":3,"kind":"branch","out":[4,5]}),
            json!({"id":4,"kind":"capture_sink"}),
            json!({"id":5,"kind":"capture_sink"}),
        ]);
    } else {
        nodes.push(json!({"id":3,"kind":"capture_sink"}));
    }
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert("s", schema());
    let raw = json!({"version":1,"pipeline_id":900,"revision_id":1,"nodes":nodes});
    let graph = sparrow_plan::GraphSpec::from_json(&raw.to_string()).unwrap();
    sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &catalog).unwrap(),
        &sparrow_plan::PlanOptions::default(),
    )
}

#[test]
fn windows_kernel_graph_et_and_linear_count_complete_and_refund() {
    use serde_json::json;
    for (window, et, expected) in [
        (
            json!({"kind":"session_et","gap_micros":10,"max_duration_micros":100}),
            true,
            1,
        ),
        (json!({"kind":"sliding_et","size_micros":10}), true, 3),
        (json!({"kind":"sliding_count","size":2,"step":1}), false, 2),
    ] {
        for branch in [false, true] {
            let kernel = Kernel::new(KernelOptions::default()).unwrap();
            let capture = SharedCapture::new();
            let second = SharedCapture::new();
            let mut request = JobRequest::new(
                graph(window.clone(), et, branch),
                vec![row("a", 1, 5), row("a", 2, 9), row("a", 3, 13)],
                capture.clone(),
            );
            if branch {
                request.graph_outputs.insert(
                    4.into(),
                    GraphOutput {
                        capture: capture.clone(),
                        live: None,
                        outbox: None,
                    },
                );
                request.graph_outputs.insert(
                    5.into(),
                    GraphOutput {
                        capture: second.clone(),
                        live: None,
                        outbox: None,
                    },
                );
            }
            let handle = kernel.submit(request).unwrap();
            let owner = handle.memory_owner();
            kernel
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(3), handle.wait()).await
                })
                .unwrap()
                .unwrap();
            assert_eq!(capture.row_count(), expected);
            if branch {
                assert_eq!(capture.rows(), second.rows());
            }
            assert_eq!(owner.usage().physical_bytes, 0);
            assert_eq!(owner.accounting_errors_total(), 0);
        }
    }
}

#[test]
fn windows_kernel_pt_session_timer_after_eof_and_cancel_refund() {
    use serde_json::json;
    for cancel in [false, true] {
        let kernel = Kernel::new(KernelOptions::default()).unwrap();
        let capture = SharedCapture::new();
        let clock = RuntimeClock::virtual_clock(SharedVirtualClock::new(100));
        let mut request = JobRequest::new(
            graph(
                json!({"kind":"session_pt","gap_micros":10,"max_duration_micros":100}),
                false,
                true,
            ),
            vec![row("a", 1, 0)],
            capture.clone(),
        );
        request.clock = clock.clone();
        let handle = kernel.submit(request).unwrap();
        let owner = handle.memory_owner();
        kernel.block_on(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                while owner.usage().retention_bytes == 0 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(capture.row_count(), 0);
                if cancel {
                    handle.cancel();
                } else {
                    clock.set_virtual(200);
                }
                let result = handle.wait().await;
                if !cancel {
                    result.unwrap();
                }
            })
            .await
            .unwrap();
        });
        if !cancel {
            assert!(capture.row_count() > 0);
        }
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(kernel.live_tasks(), 0);
    }
}

#[test]
fn windows_kernel_cancel_backpressured_sliding_output_refunds() {
    let mut options = KernelOptions::default();
    options.mailbox.max_items = 1;
    options.rows_per_batch = 2;
    let kernel = Kernel::new(options).unwrap();
    let capture = SharedCapture::new();
    capture.stall.stall();
    let request = JobRequest::new(
        graph(
            serde_json::json!({"kind":"sliding_count","size":1,"step":1}),
            false,
            false,
        ),
        (0..40).map(|n| row("a", n, 0)).collect(),
        capture,
    );
    let handle = kernel.submit(request).unwrap();
    let owner = handle.memory_owner();
    let observer = handle.mailbox_observer();
    kernel.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !observer
                .snapshot()
                .edges
                .iter()
                .any(|e| e.queue.waiting_senders > 0)
            {
                tokio::task::yield_now().await;
            }
            let result = handle.stop().await;
            if let Err(error) = result {
                assert_eq!(error.code, ErrorCode::Cancelled);
            }
        })
        .await
        .unwrap();
    });
    assert!(observer
        .snapshot()
        .edges
        .iter()
        .all(|e| e.queue.credits_reserved_bytes == 0 && e.queue.accounting_errors_total == 0));
    drop(observer); // the test's retained observer owns its queue-metadata credit
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(owner.accounting_errors_total(), 0);
    assert_eq!(kernel.live_tasks(), 0);
}

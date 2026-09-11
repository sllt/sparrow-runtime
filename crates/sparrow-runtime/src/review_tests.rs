//! Regression tests for ChatGPT static-review items that live in the kernel.

use super::*;
use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::{
    DataType, Field, FieldId, PipelineId, ResourceBudget, RevisionId, Row, Scalar, Schema, SchemaId,
};
use sparrow_plan::{bind_linear, physicalize, PlanOptions};

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Int64, false),
            Field::new(FieldId::new(2), "temp", DataType::Float64, true),
        ],
    )
    .unwrap()
}

fn rows() -> Vec<Row> {
    vec![
        Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(10.0)],
        },
        Row {
            values: vec![Scalar::Int64(2), Scalar::Float64(30.0)],
        },
    ]
}

fn kernel() -> Kernel {
    Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 4,
    })
    .unwrap()
}

#[test]
fn byte_accounted_ingress_releases_queues_on_eof_and_cancel() {
    use sparrow_model::{QueuedRow, QueueOccupancy};
    use std::sync::{Arc, atomic::Ordering};
    for stop in [false, true] {
        let k = kernel();
        let bound = bind_linear(PipelineId::new(1), RevisionId::new(1), "s".into(), schema(), None, None, None, "c".into()).unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let (out, mut output) = tokio::sync::mpsc::channel(8);
        let job = k.submit(JobRequest::new(physicalize(&bound, &PlanOptions { fuse: true }), Vec::new(), SharedCapture::disabled()).with_budgeted_live_io(rx, out)).unwrap();
        let owner = job.memory_owner();
        let occupancy = Arc::new(QueueOccupancy::default());
        k.block_on(async {
            for row in rows() { tx.send(QueuedRow::try_new(row, &owner, &occupancy).unwrap()).await.unwrap(); }
            if stop { job.cancel(); }
            drop(tx);
            let drain = async { while output.recv().await.is_some() {} };
            let wait = async { tokio::time::timeout(std::time::Duration::from_secs(2), job.wait()).await.unwrap().unwrap() };
            let (_, stats) = tokio::join!(drain, wait);
            if !stop { assert_eq!(stats.ingested_rows, 2); }
        });
        assert_eq!(occupancy.items.load(Ordering::Relaxed), 0);
        assert_eq!(owner.usage().live_handles, 0);
        assert_eq!(k.process_owner().usage().physical_bytes, 0);
    }
}

#[test]
fn ingest_is_counted_once_for_memory_live_eof_and_live_stop() {
    for mode in 0..5 {
        let k = kernel();
        let bound = bind_linear(PipelineId::new(1), RevisionId::new(1),
            "s".into(), schema(), None, None, None, "c".into()).unwrap();
        let plan = physicalize(&bound, &PlanOptions { fuse: true });
        if mode == 0 {
            let stats = k.run(JobRequest::new(plan, rows(), SharedCapture::new())).unwrap();
            assert_eq!(stats.ingested_rows, 2);
            assert_eq!(k.metrics.snapshot().ingested_rows, 2);
            continue;
        }
        let ordered = mode == 2 || mode == 4;
        let stop = mode >= 3;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(4);
        let (out, _output) = tokio::sync::mpsc::channel(4);
        let req = JobRequest::new(plan, Vec::new(), SharedCapture::new());
        let req = if ordered { req.with_live_events(events_rx) } else { req.with_live_io(rx, out) };
        let handle = k.submit(req).unwrap();
        k.block_on(async {
            for row in rows() {
                if ordered { events_tx.send(IngressEvent::Row(row)).await.unwrap(); }
                else { tx.send(row).await.unwrap(); }
            }
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while k.metrics.snapshot().ingested_rows < 2 {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            assert_eq!(k.metrics.snapshot().ingested_rows, 2);
            if stop { handle.cancel(); }
            drop(tx);
            drop(events_tx);
            let stats = tokio::time::timeout(std::time::Duration::from_secs(2), handle.wait())
                .await.unwrap().unwrap();
            assert_eq!(stats.ingested_rows, 2);
            assert_eq!(k.metrics.snapshot().ingested_rows, 2, "mode {mode}: completion double-counted ingress");
        });
    }
}

#[test]
fn r01_stage_error_is_job_failed_not_ok_cancelled() {
    let output = schema();
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        Some((
            vec![
                Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Literal(Scalar::Int64(i64::MAX))),
                    right: Box::new(Expr::Literal(Scalar::Int64(1))),
                },
                Expr::Column {
                    name: "temp".into(),
                },
            ],
            output,
        )),
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    let err = k
        .run(JobRequest::new(plan, rows(), SharedCapture::new()))
        .unwrap_err();
    assert_ne!(
        err.code,
        sparrow_model::ErrorCode::Cancelled,
        "primary stage failure must not be reported as cancel: {err}"
    );
    assert!(
        err.code == sparrow_model::ErrorCode::IntegerOverflow
            || err.code == sparrow_model::ErrorCode::JobFailed
            || err.message.contains("overflow"),
        "unexpected error: {err}"
    );
}

#[test]
fn r01_user_stop_is_cancelled_ok() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let (out_tx, _out_rx) = tokio::sync::mpsc::channel(4);
    let h = k
        .submit(
            JobRequest::new(plan, Vec::new(), SharedCapture::new()).with_live_io(rx, out_tx),
        )
        .unwrap();
    let _ = tx;
    let stats = k.block_on(h.stop()).unwrap();
    assert!(stats.cancelled);
    assert_eq!(stats.live_tasks_after, 0);
}

#[test]
fn r02_quantum_allows_more_events_than_old_lifetime_cap() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        Some(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temp".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(-1.0))),
        }),
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let mut many = Vec::new();
    for i in 0..200 {
        many.push(Row {
            values: vec![Scalar::Int64(i), Scalar::Float64(1.0)],
        });
    }
    let k = Kernel::new(KernelOptions {
        budget: ResourceBudget {
            work_units: 8,
            ..ResourceBudget::compact()
        },
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 2,
    })
    .unwrap();
    let stats = k
        .run(JobRequest::new(plan, many, SharedCapture::new()))
        .unwrap();
    assert!(stats.ingested_rows >= 200);
    assert!(!stats.cancelled);
}

#[test]
fn r03_supervisor_style_capture_is_disabled() {
    let c = SharedCapture::disabled();
    assert!(!c.retains_rows());
}

#[test]
fn r18_ordered_ingress_emits_watermark_after_rows() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let capture = SharedCapture::new();
    let h = k
        .submit(JobRequest::new(plan, Vec::new(), capture.clone()).with_live_events(rx))
        .unwrap();
    k.block_on(async {
        tx.send(IngressEvent::Row(Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(1.0)],
        }))
        .await
        .unwrap();
        tx.send(IngressEvent::Control(StreamControl::Watermark {
            input: 0,
            wm_micros: 10,
        }))
        .await
        .unwrap();
        drop(tx);
        let stats = h.wait().await.unwrap();
        assert_eq!(stats.ingested_rows, 1);
        assert_eq!(capture.row_count(), 1);
    });
}

#[test]
fn r01_stage_panic_is_job_failed() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    let err = k
        .run(JobRequest::new(plan, rows(), SharedCapture::new()).with_inject_panic())
        .unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::JobFailed);
    assert!(err.message.contains("panic"));
}

#[test]
fn r01_downstream_fail_while_upstream_idle_shuts_down() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let (out_tx, out_rx) = tokio::sync::mpsc::channel(1);
    let h = k
        .submit(JobRequest::new(plan, Vec::new(), SharedCapture::disabled()).with_live_io(rx, out_tx))
        .unwrap();
    k.block_on(async {
        tx.send(Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(1.0)],
        })
        .await
        .unwrap();
        // Close the sink while the source is idle (no more rows).
        drop(out_rx);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        tx.send(Row {
            values: vec![Scalar::Int64(2), Scalar::Float64(2.0)],
        })
        .await
        .ok();
        drop(tx);
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), h.wait()).await;
        assert!(
            outcome.is_ok(),
            "downstream close must not hang while upstream is idle"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        match outcome.unwrap() {
            Ok(stats) => assert_eq!(stats.live_tasks_after, 0),
            Err(e) => assert_ne!(e.code, sparrow_model::ErrorCode::Cancelled),
        }
        assert_eq!(k.live_tasks(), 0);
    });
}

#[test]
fn r04_small_reservation_rejects_unfinished_builder() {
    let schema = std::sync::Arc::new(schema());
    let owner = sparrow_model::MemoryOwner::new(ResourceBudget {
        reservation_bytes: 80,
        ..ResourceBudget::compact()
    });
    let err = match sparrow_model::RowBatchBuilder::new(
        schema,
        owner,
        sparrow_model::CreditKind::Reservation,
        32,
        64,
    ) {
        Ok(mut builder) => {
            let mut last = None;
            for i in 0..64 {
                match builder.push(Row {
                    values: vec![Scalar::Int64(i), Scalar::Float64(1.0)],
                }) {
                    Ok(()) => {}
                    Err(e) => {
                        last = Some(e);
                        break;
                    }
                }
            }
            last.expect("tiny reservation must reject unfinished builder growth")
        }
        Err(e) => e,
    };
    assert!(
        err.code == sparrow_model::ErrorCode::BoundExceeded
            || err.code == sparrow_model::ErrorCode::ResourceExhausted,
        "{err}"
    );
}

#[test]
fn r25_blocked_sink_records_queue_pressure() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 2,
            max_bytes: 1024,
        },
        worker_threads: 2,
        rows_per_batch: 1,
    })
    .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(1);
    let h = k
        .submit(JobRequest::new(plan, Vec::new(), SharedCapture::disabled()).with_live_io(rx, out_tx))
        .unwrap();
    k.block_on(async {
        for i in 0..6 {
            let _ = tx
                .send(Row {
                    values: vec![Scalar::Int64(i), Scalar::Float64(1.0)],
                })
                .await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        let snap = k.metrics.snapshot();
        assert!(
            snap.queue_items > 0 || snap.queue_bytes > 0 || snap.live_samples > 0,
            "blocked sink must show queue/live samples: {snap:?}"
        );
        while out_rx.try_recv().is_ok() {}
        drop(tx);
        drop(out_rx);
        let _ = h.wait().await;
    });
}

#[test]
fn r25_live_path_records_real_metrics() {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    k.run(JobRequest::new(plan, rows(), SharedCapture::new()))
        .unwrap();
    let snap = k.metrics.snapshot();
    assert!(snap.ingested_rows >= 2, "ingest must be measured: {snap:?}");
    assert!(snap.emitted_rows >= 2, "emit must be measured: {snap:?}");
    assert!(snap.live_samples > 0);
}

fn count_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn count_rows(vs: &[i64]) -> Vec<Row> {
    vs.iter()
        .map(|v| Row {
            values: vec![Scalar::utf8("d1"), Scalar::Int64(*v)],
        })
        .collect()
}

fn filter_gt_v(thr: i64) -> Expr {
    Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column { name: "v".into() }),
        right: Box::new(Expr::Literal(Scalar::Int64(thr))),
    }
}

fn count_window_plan(filter: Option<Expr>, size: u64) -> sparrow_plan::PhysicalPlan {
    use sparrow_model::WindowKind;
    use sparrow_plan::{bind_window_linear, AggCall, WindowSpec};
    let spec = WindowSpec::new(
        WindowKind::Count { size },
        vec!["device_id".into()],
        vec![AggCall::count_star("n")],
    );
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "sensors".into(),
        count_schema(),
        filter,
        spec,
        "c".into(),
    )
    .unwrap();
    physicalize(&bound, &PlanOptions { fuse: true })
}

#[test]
fn r3_live_timer_metrics_and_job_cancel_count_are_real() {
    use sparrow_model::{SharedVirtualClock, WindowKind};
    use sparrow_plan::{bind_window_linear, AggCall, WindowSpec};
    let spec = WindowSpec::new(WindowKind::tumbling_pt(1_000_000).unwrap(),
        vec!["device_id".into()], vec![AggCall::count_star("n")]);
    let bound = bind_window_linear(PipelineId::new(1), RevisionId::new(1), "sensors".into(),
        count_schema(), None, spec, "out".into()).unwrap();
    let plan = physicalize(&bound, &PlanOptions::default());
    let kernel = kernel();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let handle = kernel.submit(JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
        .with_live_events(rx).with_clock(crate::RuntimeClock::virtual_clock(SharedVirtualClock::new(0)))).unwrap();
    kernel.block_on(async {
        tx.send(IngressEvent::Row(count_rows(&[1]).pop().unwrap())).await.unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while kernel.metrics.snapshot().timers_live == 0 {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let stats = handle.stop().await.unwrap();
        assert_eq!(stats.timers_live, 0);
        assert!(stats.timers_cancelled > 0);
        assert_eq!(kernel.metrics.snapshot().timers_live, 0);
        assert!(kernel.metrics.snapshot().timers_cancelled > 0);
    });
}

#[test]
fn n9_future_drop_visible_in_metrics() {
    use sparrow_model::{SharedVirtualClock, WindowKind};
    use sparrow_plan::{bind_window_linear, AggCall, WindowSpec};

    let schema = Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(3), "ts", DataType::Int64, false),
        ],
    )
    .unwrap();
    let spec = WindowSpec::new(
        WindowKind::tumbling_et(1_000_000).unwrap(),
        vec!["device_id".into()],
        vec![AggCall::count_star("n")],
    )
    .event_time("ts", 0);
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "sensors".into(),
        schema,
        None,
        spec,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let year_2100 = 4_102_444_800_000_000i64;
    let rows = vec![
        Row {
            values: vec![
                Scalar::utf8("d1"),
                Scalar::Float64(99.0),
                Scalar::Int64(year_2100),
            ],
        },
        Row {
            values: vec![
                Scalar::utf8("d1"),
                Scalar::Float64(10.0),
                Scalar::Int64(500_000),
            ],
        },
        Row {
            values: vec![
                Scalar::utf8("d1"),
                Scalar::Float64(20.0),
                Scalar::Int64(1_500_000),
            ],
        },
    ];
    let k = kernel();
    let capture = SharedCapture::new();
    // `now` is processing time (virtual origin 0 here), not event time.
    let clock = crate::RuntimeClock::virtual_clock(SharedVirtualClock::new(0));
    k.run(
        JobRequest::new(plan, rows, capture.clone())
            .with_clock(clock)
            .with_controls(vec![StreamControl::Watermark {
                input: 0,
                wm_micros: 3_000_000,
            }]),
    )
    .unwrap();
    let snap = k.metrics.snapshot();
    assert!(
        snap.future_dropped >= 1,
        "year-2100 drop must increment kernel future_dropped (exported on /v1/metrics): {snap:?}"
    );
    assert!(
        capture.row_count() >= 1,
        "later windows must still form after the future drop: {:?}",
        capture.rows()
    );
}

#[test]
fn n10_nan_compare_does_not_fail_job() {
    use sparrow_expr::filter_mask;
    use sparrow_model::{AggFn, WindowKind};
    use sparrow_plan::{bind_window_linear, AggCall, WindowSpec};

    let schema = schema();
    // Compound predicate so filter_mask cannot take the CmpF64 fast path and
    // must eval — the path that used to return Err on NaN and fail the job.
    let pred = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temp".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(0.0))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(Expr::Column {
                name: "temp".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(100.0))),
        }),
    };
    let nan_rows = vec![
        Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(f64::NAN)],
        },
        Row {
            values: vec![Scalar::Int64(2), Scalar::Float64(30.0)],
        },
        Row {
            values: vec![Scalar::Int64(3), Scalar::Float64(-1.0)],
        },
    ];
    let mask = filter_mask(&pred, &schema, &nan_rows).expect("NaN compare must not error");
    assert_eq!(
        mask,
        vec![false, true, false],
        "NaN is unknown/false in WHERE; only 30.0 is kept"
    );

    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema.clone(),
        Some(pred),
        None,
        None,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let k = kernel();
    let capture = SharedCapture::new();
    k.run(JobRequest::new(plan, nan_rows, capture.clone()))
        .expect("WHERE over NaN must not JobFailed");
    assert_eq!(
        capture.row_count(),
        1,
        "only the finite in-range row must pass: {:?}",
        capture.rows()
    );

    let win = WindowSpec::new(
        WindowKind::Count { size: 3 },
        vec!["id".into()],
        vec![AggCall::new(
            AggFn::Min,
            Some(Expr::Column {
                name: "temp".into(),
            }),
            "mn",
        )],
    );
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema,
        None,
        win,
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    let min_rows = vec![
        Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(10.0)],
        },
        Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(f64::NAN)],
        },
        Row {
            values: vec![Scalar::Int64(1), Scalar::Float64(30.0)],
        },
    ];
    let capture = SharedCapture::new();
    k.run(JobRequest::new(plan, min_rows, capture.clone()))
        .expect("MIN over NaN must not JobFailed");
    assert_eq!(capture.row_count(), 1, "COUNT_WINDOW(3) emits one row");
    let out = capture.rows();
    let mn = out[0]
        .iter()
        .find_map(|s| match s {
            Scalar::Float64(v) if !v.is_nan() => Some(*v),
            _ => None,
        })
        .unwrap_or_else(|| panic!("MIN must skip NaN and emit a finite min: {out:?}"));
    assert!(
        (mn - 10.0).abs() < 1e-9,
        "MIN skips NaN and keeps 10.0, got {mn} from {out:?}"
    );
}

#[test]
fn a1_aligned_runs_full_plan_through_kernel() {
    let plan = count_window_plan(Some(filter_gt_v(1)), 2);
    assert!(
        plan.stages
            .iter()
            .any(|s| matches!(s, sparrow_plan::PhysicalStage::Transform { .. })),
        "aligned Kernel path must keep the Filter stage: {:?}",
        plan.stages
    );
    let k = kernel();
    let capture = SharedCapture::new();
    k.run(JobRequest::new(plan, count_rows(&[1, 2, 3, 4]), capture.clone()))
        .unwrap();
    assert_eq!(
        capture.row_count(),
        1,
        "Filter+COUNT_WINDOW(2) on v=1,2,3,4 must emit one window [2,3], not two stripped windows: {:?}",
        capture.rows()
    );
}

#[test]
fn p0_4_barrier_waits_for_real_sink_flush() {
    let plan = count_window_plan(None, 1);
    let k = kernel();
    let (ev_tx, ev_rx) = tokio::sync::mpsc::channel(8);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(8);
    let ack_tx = crate::AlignedAcks::default();
    let mut ack_rx = ack_tx.begin(1).unwrap();
    let outbox = std::sync::Arc::new(sparrow_model::InflightCounter::new());
    let handle = k
        .submit(
            JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
                .with_live_events(ev_rx)
                .with_live_out(out_tx)
                .with_aligned(crate::AlignedJob {
                    restore: None,
                    acks: ack_tx,
                    outbox: std::sync::Arc::clone(&outbox),
                }),
        )
        .unwrap();
    let outbox_sink = std::sync::Arc::clone(&outbox);
    k.block_on(async {
        let sink = tokio::spawn(async move {
            while let Some(_batch) = out_rx.recv().await {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                outbox_sink.ack();
            }
        });
        ev_tx
            .send(IngressEvent::Row(count_rows(&[1]).pop().unwrap()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        ev_tx
            .send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: 1,
            }))
            .await
            .unwrap();
        let t0 = std::time::Instant::now();
        let mut frozen = false;
        let mut flushed = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline && !(frozen && flushed) {
            match tokio::time::timeout(
                deadline.saturating_duration_since(tokio::time::Instant::now()),
                ack_rx.recv(),
            )
            .await
            {
                Ok(Some(crate::AlignedAck::WindowFrozen { .. })) => frozen = true,
                Ok(Some(crate::AlignedAck::SinkFlushed { .. })) => flushed = true,
                Ok(Some(crate::AlignedAck::FreezeFailed { error, .. })) => panic!("{error}"),
                Ok(None) | Err(_) => break,
            }
        }
        let elapsed = t0.elapsed();
        assert!(frozen, "window must freeze on the Kernel barrier");
        assert!(flushed, "sink must ack after the real flush, not channel-empty");
        assert!(
            elapsed >= std::time::Duration::from_millis(200),
            "SinkFlushed in {elapsed:?} — barrier did not wait for sink ack"
        );
        drop(ev_tx);
        let _ = handle.stop().await;
        let _ = sink.await;
    });
}

fn pass_through_plan() -> sparrow_plan::PhysicalPlan {
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        None,
        None,
        "c".into(),
    )
    .unwrap();
    physicalize(&bound, &PlanOptions { fuse: true })
}

#[test]
fn p1_20_second_job_refused_when_process_queue_reserved() {
    let plan = pass_through_plan();
    assert!(plan.mailbox_count() >= 1);
    let k = Kernel::new(KernelOptions {
        budget: ResourceBudget {
            queue_bytes: 4 * 1024,
            ..ResourceBudget::compact()
        },
        mailbox: MailboxConfig {
            max_items: 4,
            max_bytes: 3 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 4,
    })
    .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let (out_tx, _out_rx) = tokio::sync::mpsc::channel(4);
    let first = k
        .submit(
            JobRequest::new(plan.clone(), Vec::new(), SharedCapture::new())
                .with_live_io(rx, out_tx),
        )
        .expect("first job must admit");
    assert_eq!(k.admitted_jobs(), 1);
    assert!(k.queue_reserved() > 0);
    let err = match k.submit(JobRequest::new(plan, rows(), SharedCapture::new())) {
        Ok(h) => {
            k.block_on(h.stop()).ok();
            panic!(
                "second job was admitted (reserved={} cap={})",
                k.queue_reserved(),
                k.budget().queue_bytes
            );
        }
        Err(e) => e,
    };
    assert_eq!(err.code, sparrow_model::ErrorCode::ResourceExhausted);
    assert!(err.context.iter().any(|(key, value)| key == "admission" && value == "capacity"));
    assert!(
        err.message.contains("process queue") || err.message.contains("process memory"),
        "N jobs must not each get a full queue budget: {err}"
    );
    drop(tx);
    k.block_on(first.stop()).unwrap();
    assert_eq!(k.admitted_jobs(), 0);
    assert_eq!(k.queue_reserved(), 0);
}

#[test]
fn p1_20_jobs_share_process_memory_owner() {
    let k = kernel();
    let owner = k.process_owner().clone();
    let ret = owner
        .acquire(
            sparrow_model::CreditKind::Retention,
            owner.budget().retention_bytes,
        )
        .unwrap();
    let q = owner
        .acquire(
            sparrow_model::CreditKind::Queue,
            owner.budget().queue_bytes,
        )
        .unwrap();
    let plan = pass_through_plan();
    let err = match k.submit(JobRequest::new(plan, rows(), SharedCapture::new())) {
        Ok(h) => {
            k.block_on(h.stop()).ok();
            panic!("job admitted while process retention+queue were full");
        }
        Err(e) => e,
    };
    assert_eq!(err.code, sparrow_model::ErrorCode::ResourceExhausted);
    assert!(
        err.message.contains("process memory") || err.message.contains("process queue"),
        "{err}"
    );
    drop(ret);
    drop(q);
}

#[test]
fn a2_transform_builder_charges_owner_not_shadow_vec() {
    use sparrow_model::{CreditKind, MemoryOwner, OperatorId, RowBatchBuilder, WorkBudget};
    use sparrow_plan::TransformStep;

    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 512,
        retention_bytes: 512,
        queue_bytes: 512,
        max_rows: 8,
        work_units: 1_000,
        max_state_keys: 8,
        max_timers: 8,
    });
    let schema = schema();
    let mut b = RowBatchBuilder::new(
        std::sync::Arc::new(schema.clone()),
        std::sync::Arc::clone(&owner),
        CreditKind::Reservation,
        4,
        400,
    )
    .unwrap();
    b.push(Row {
        values: vec![Scalar::Int64(1), Scalar::Float64(10.0)],
    })
    .unwrap();
    b.push(Row {
        values: vec![Scalar::Int64(2), Scalar::Float64(30.0)],
    })
    .unwrap();
    let batch = b.finish().unwrap();
    let work = WorkBudget::new(1_000);
    let out = crate::transform::apply_steps(
        &batch,
        &[TransformStep::Filter {
            operator: OperatorId::new(1),
            predicate: Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column {
                    name: "temp".into(),
                }),
                right: Box::new(Expr::Literal(Scalar::Float64(15.0))),
            },
            input: schema,
        }],
        &owner,
        &work,
    )
    .unwrap()
    .expect("one row kept");
    assert_eq!(out.num_rows(), 1);
    assert!(owner.peak_builder_bytes() > 0);
    drop(batch);
    drop(out);
    assert_eq!(owner.usage().reservation_bytes, 0);
}

#[test]
fn a2_utf8_tracked_used_on_runtime_path() {
    let owner = sparrow_model::MemoryOwner::new(ResourceBudget {
        reservation_bytes: 24,
        retention_bytes: 24,
        queue_bytes: 24,
        max_rows: 2,
        work_units: 10,
        max_state_keys: 2,
        max_timers: 2,
    });
    let err = Scalar::utf8_tracked(&owner, "0123456789abcdef extra")
        .unwrap_err();
    assert_eq!(err.code, sparrow_model::ErrorCode::ResourceExhausted);
}

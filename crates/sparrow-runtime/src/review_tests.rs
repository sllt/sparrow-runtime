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
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(8);
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

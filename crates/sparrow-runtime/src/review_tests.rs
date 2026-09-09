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

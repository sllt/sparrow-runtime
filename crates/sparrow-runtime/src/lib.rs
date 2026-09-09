//! In-process streaming kernel.
//!
//! M1: ExecutionChain (one Tokio task per physical stage), bounded
//! mailboxes, work budget, job supervisor with cancel/join. Arrow, Axum,
//! SQLite, MQTT, and `sqlparser` stay out of this crate.

pub mod capture;
pub mod kernel;
pub mod linear;
pub mod mailbox;
pub mod transform;

pub use capture::{SharedCapture, StallGate};
pub use kernel::{JobHandle, JobRequest, JobStats, Kernel, KernelOptions};
pub use linear::{drain, LinearExecutor, RuntimeConfig};
pub use mailbox::MailboxConfig;

#[cfg(test)]
mod g2_tests {
    use super::*;
    use sparrow_expr::{BinaryOp, Expr};
    use sparrow_model::{
        DataType, Field, FieldId, PipelineId, ResourceBudget, RevisionId, Row, Scalar, Schema,
        SchemaId,
    };
    use sparrow_plan::{bind_linear, physicalize, PlanOptions};
    use std::time::Duration;

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
            Row {
                values: vec![Scalar::Int64(3), Scalar::Float64(26.0)],
            },
            Row {
                values: vec![Scalar::Int64(4), Scalar::Float64(12.0)],
            },
        ]
    }

    fn bound() -> sparrow_plan::BoundLogicalPlan {
        let out = Schema::new(
            SchemaId::new(2),
            vec![
                Field::new(FieldId::new(1), "id", DataType::Int64, false),
                Field::new(FieldId::new(2), "temp", DataType::Float64, true),
            ],
        )
        .unwrap();
        bind_linear(
            PipelineId::new(1),
            RevisionId::new(1),
            "t".into(),
            schema(),
            Some(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column {
                    name: "temp".into(),
                }),
                right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
            }),
            Some((
                vec![
                    Expr::Column { name: "id".into() },
                    Expr::Column {
                        name: "temp".into(),
                    },
                ],
                out,
            )),
            None,
            "capture".into(),
        )
        .unwrap()
    }

    fn kernel(mailbox_items: usize) -> Kernel {
        Kernel::new(KernelOptions {
            budget: ResourceBudget::compact(),
            mailbox: MailboxConfig {
                max_items: mailbox_items,
                max_bytes: 64 * 1024,
            },
            worker_threads: 2,
            rows_per_batch: 1,
        })
        .unwrap()
    }

    #[test]
    fn fused_and_unfused_same_rows() {
        let k = kernel(8);
        let plan = bound();
        let fused = physicalize(&plan, &PlanOptions { fuse: true });
        let unfused = physicalize(&plan, &PlanOptions { fuse: false });
        assert!(fused.fused());
        assert!(!unfused.fused());

        let a = SharedCapture::new();
        let b = SharedCapture::new();
        k.run(JobRequest::new(fused, rows(), a.clone())).unwrap();
        k.run(JobRequest::new(unfused, rows(), b.clone())).unwrap();
        assert_eq!(a.rows(), b.rows());
        assert_eq!(a.row_count(), 2);
        assert_eq!(k.live_tasks(), 0);
    }

    #[test]
    fn full_queue_stop_does_not_deadlock() {
        let k = kernel(1);
        let capture = SharedCapture::new();
        capture.stall.stall();
        let handle = k
            .submit(JobRequest::new(
                physicalize(&bound(), &PlanOptions { fuse: true }),
                rows(),
                capture.clone(),
            ))
            .unwrap();
        // Sink is stalled and mailbox is 1-deep; cancel must still join.
        std::thread::sleep(Duration::from_millis(30));
        let stats = k
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), handle.stop())
                    .await
                    .expect("stop timed out")
            })
            .unwrap();
        capture.stall.release();
        assert_eq!(stats.live_tasks_after, 0);
        assert_eq!(k.live_tasks(), 0);
    }

    #[test]
    fn stalled_job_does_not_freeze_peer() {
        let k = kernel(2);
        let stalled = SharedCapture::new();
        stalled.stall.stall();
        let live = SharedCapture::new();
        let a = k
            .submit(JobRequest::new(
                physicalize(&bound(), &PlanOptions::default()),
                rows(),
                stalled.clone(),
            ))
            .unwrap();
        let b = k
            .submit(JobRequest::new(
                physicalize(&bound(), &PlanOptions::default()),
                rows(),
                live.clone(),
            ))
            .unwrap();
        let b_stats = k.block_on(b.wait()).expect("peer job froze behind stalled sink");
        assert_eq!(b_stats.captured_rows, 2);
        stalled.stall.release();
        k.block_on(a.wait()).unwrap();
        assert_eq!(k.live_tasks(), 0);
    }

    #[test]
    fn live_channels_round_trip() {
        let k = kernel(8);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(8);
        let capture = SharedCapture::new();
        let handle = k
            .submit(
                JobRequest::new(
                    physicalize(&bound(), &PlanOptions { fuse: true }),
                    Vec::new(),
                    capture.clone(),
                )
                .with_live_io(rx, out_tx),
            )
            .unwrap();
        let batch = k.block_on(async {
            tx.send(rows()[1].clone()).await.unwrap();
            tx.send(rows()[0].clone()).await.unwrap();
            drop(tx);
            let batch = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
                .await
                .expect("live_out timed out")
                .expect("live_out closed");
            handle.wait().await.unwrap();
            batch
        });
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(capture.row_count(), 1);
        assert_eq!(k.live_tasks(), 0);
    }
}

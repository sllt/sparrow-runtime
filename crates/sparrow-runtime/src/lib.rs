//! In-process streaming kernel.
//!
//! V1: production aligned checkpoint on top of V0.3 event-time.
//! Arrow, Axum, SQLite, MQTT, and `sqlparser` stay out of this crate.

pub mod aggregate;
mod alarm;
mod alarm_iot;
pub mod aligned;
pub mod barrier;
pub mod capture;
pub mod checkpoint;
pub mod clock;
pub mod coordinator;
pub mod dedup;
pub mod iot;
mod resample_iot;
mod silence_iot;
mod timed_iot;
mod timed_state;
pub use resample_iot::ResampleStats;
#[cfg(test)]
mod analysis_tests;
mod bounded_join;
mod buffered_window;
pub mod external_lookup;
#[cfg(test)]
mod external_lookup_tests;
#[cfg(test)]
mod external_lookup_batch_tests;
pub mod finite;
pub mod graph_cut;
pub mod kernel;
pub mod linear;
pub mod lookup;
pub mod mailbox;
pub mod mailbox_observe;
pub mod metrics;
pub mod observed_cut;
pub mod pipeline_checkpoint;
pub mod processing_cut;
pub mod state;
pub mod timer;
pub mod transform;
pub mod watermark;
pub mod window;
#[cfg(test)]
mod window_completion_tests;
pub use iot::{IotFreeze, IotOperator};
pub use pipeline_checkpoint::{
    sink_snapshot_version_for, snapshot_version_for, PipelineSnapshot,
    EXT_AGG_FILE_SNAPSHOT_VERSION, EXT_AGG_RELIABLE_SNAPSHOT_VERSION,
};
#[cfg(test)]
mod sink_checkpoint_tests;
#[cfg(test)]
mod csv_sink_checkpoint_tests;
#[cfg(test)]
mod alarm_tests;
#[cfg(test)]
mod core_a_tests;
#[cfg(test)]
mod core_b2_tests;
#[cfg(test)]
mod core_b_tests;
#[cfg(test)]
mod hysteresis_completion_tests;
#[cfg(test)]
mod k1_tests;
#[cfg(test)]
mod k3_tests;
#[cfg(test)]
mod k4_tests;
#[cfg(test)]
mod paused_time_tests;
#[cfg(test)]
mod reference_completion_tests;
#[cfg(test)]
mod resample_tests;
#[cfg(test)]
mod silence_tests;
#[cfg(test)]
mod time_completion_tests;
#[cfg(test)]
mod time_graph_tests;

pub use aligned::{run_until, AlignedSession};
pub use barrier::{
    wait_aligned_acks, wait_outbox, AlignedAck, AlignedAcks, AlignedJob, BarrierAcks,
    CheckpointAcks, FlushOutcome, ParticipantAcks, ParticipantOutcome, PipelineRestore, SinkRestoreBinding,
};
pub use capture::{CaptureMode, SharedCapture, StallGate};
pub use checkpoint::{
    freeze_entry_cap, CheckpointSnapshot, CheckpointStore, FaultHook, FaultPoint, RestoreCredit,
    TableRevisionBind, MAX_FREEZE_ENTRIES,
};
pub use clock::RuntimeClock;
pub use coordinator::{CheckpointCoordinator, CheckpointPhase};
pub use external_lookup::{
    ExternalLookup, ExternalLookupBinding, ExternalLookupOptions, LookupDiagnostics,
    LookupDiagnosticsSnapshot, LookupErrorPolicy,
};
pub use kernel::{
    GraphInput, GraphOutput, IngressEvent, JobHandle, JobRequest, JobStats, Kernel, KernelOptions,
    SourceAdmission,
};
pub use linear::{drain, LinearExecutor, RuntimeConfig};
pub use lookup::{LiveReferenceTable, ReferenceTable, VersionedReferenceTable};
pub use mailbox::{MailboxConfig, StreamControl};
pub use metrics::{MetricsSnapshot, RuntimeMetrics};
pub use state::{MemoryState, StateKey};
pub use watermark::{OutputHoldback, WatermarkHub};
pub use window::{FrozenEntry, WindowFreeze};

#[cfg(test)]
mod time_tests;

#[cfg(test)]
mod g3_tests;

#[cfg(test)]
mod review_tests;

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
        let b_stats = k
            .block_on(b.wait())
            .expect("peer job froze behind stalled sink");
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

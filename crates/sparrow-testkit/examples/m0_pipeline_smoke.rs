//! M0 end-to-end smoke: finite fixtures → filter/project → capture.
//!
//! ```text
//! cargo run -p sparrow-testkit --example m0_pipeline_smoke
//! ```
//!
//! Exits 0 when the hot records are captured and leases return to the pool.

use std::sync::Arc;

use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::{
    CreditKind, DataType, DeliveryContract, Field, FieldId, JobAttemptId, MemoryOwner, OperatorId,
    PipelineId, RecoveryPolicy, ResourceBudget, RestoreClaim, RevisionId, RowBatchBuilder, Scalar,
    Schema, SchemaId,
};
use sparrow_plan::{LogicalOp, LogicalPlan};
use sparrow_runtime::{drain, LinearExecutor, RuntimeConfig};
use sparrow_testkit::{sensor_fixture, sensor_schema, CaptureSink, Clock, VirtualClock};

fn main() {
    if let Err(err) = run() {
        eprintln!("m0_pipeline_smoke failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    DeliveryContract::V0_1.validate_restore(&RestoreClaim::None)?;
    assert_eq!(
        DeliveryContract::V0_1.recovery,
        RecoveryPolicy::RestartFresh
    );

    let clock = VirtualClock::default();
    let in_schema = sensor_schema();
    let out_schema = Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(6), "payload_temp", DataType::Float64, true),
            Field::new(FieldId::new(4), "ts", DataType::TimestampMicrosUTC, false),
        ],
    )?;

    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut builder = RowBatchBuilder::new(
        Arc::new(in_schema.clone()),
        Arc::clone(&owner),
        CreditKind::Reservation,
        16,
        32 * 1024,
    )?;
    for rec in sensor_fixture() {
        builder.push(rec.to_row())?;
    }
    let batch = builder.finish()?;
    println!(
        "ingest clock={}us rows={} lease={}B physical={}B",
        clock.now_micros(),
        batch.num_rows(),
        batch.tracked_bytes(),
        owner.usage().physical_bytes
    );

    let filter = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column {
            name: "temperature".into(),
        }),
        right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
    };
    let project = vec![
        Expr::Column {
            name: "device_id".into(),
        },
        Expr::Column {
            name: "temperature".into(),
        },
        Expr::DynamicGet {
            expr: Box::new(Expr::Column {
                name: "payload".into(),
            }),
            key: "temp".into(),
        },
        Expr::Column { name: "ts".into() },
    ];

    let plan = LogicalPlan::linear(
        PipelineId::new(1),
        RevisionId::new(1),
        LogicalOp::Source {
            operator: OperatorId::new(1),
            name: "sensors".into(),
            schema: in_schema,
        },
        Some(filter),
        Some((project, out_schema)),
        LogicalOp::Sink {
            operator: OperatorId::new(4),
            name: "capture".into(),
        },
    );

    let exec = LinearExecutor::from_plan(
        RuntimeConfig {
            pipeline: PipelineId::new(1),
            attempt: JobAttemptId::new(1),
            owner: Arc::clone(&owner),
            credit_kind: CreditKind::Reservation,
        },
        &plan,
    )?;

    let mut sink = CaptureSink::new();
    let ingested = drain(&exec, [batch], &mut sink)?;
    clock.advance_micros(1_000);

    println!("ingested_rows={ingested}");
    println!("captured_rows={}", sink.row_count());
    println!("captured:");
    for (i, row) in sink.rows_as_debug().iter().enumerate() {
        println!("  {i}: {}", row.join(" | "));
    }

    drop(sink);
    let usage = owner.usage();
    println!(
        "after_drop physical={}B reservation={}B handles={}",
        usage.physical_bytes, usage.reservation_bytes, usage.live_handles
    );
    if usage.physical_bytes != 0 || usage.live_handles != 0 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            "leases leaked after capture drop",
        ));
    }
    if ingested != 6 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected 6 fixture rows, got {ingested}"),
        ));
    }
    println!("m0_pipeline_smoke: ok");
    Ok(())
}

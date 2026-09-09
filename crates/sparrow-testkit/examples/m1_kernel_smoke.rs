//! M1 kernel smoke: MemorySource → Filter/Project → CaptureSink.
//!
//! ```text
//! cargo run -p sparrow-testkit --example m1_kernel_smoke
//! ```

use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::{
    DataType, Field, FieldId, PipelineId, RevisionId, Scalar, Schema, SchemaId,
};
use sparrow_plan::{bind_linear, physicalize, PlanOptions};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, SharedCapture};
use sparrow_testkit::{sensor_fixture, sensor_schema};

fn main() {
    if let Err(err) = run() {
        eprintln!("m1_kernel_smoke failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    let in_schema = sensor_schema();
    let out_schema = Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(4), "ts", DataType::TimestampMicrosUTC, false),
        ],
    )?;
    let bound = bind_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "sensors".into(),
        in_schema,
        Some(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temperature".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
        }),
        Some((
            vec![
                Expr::Column {
                    name: "device_id".into(),
                },
                Expr::Column {
                    name: "temperature".into(),
                },
                Expr::Column { name: "ts".into() },
            ],
            out_schema,
        )),
        None,
        "capture".into(),
    )?;
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    println!(
        "physical stages={} fused={} mailboxes={}",
        plan.stages.len(),
        plan.fused(),
        plan.mailbox_count()
    );

    let kernel = Kernel::new(KernelOptions::default())?;
    let capture = SharedCapture::new();
    let stats = kernel.run(JobRequest {
        plan,
        rows: sensor_fixture().into_iter().map(|r| r.to_row()).collect(),
        capture: capture.clone(),
    })?;

    println!(
        "attempt={} ingested={} captured={} cancelled={} live_tasks={}",
        stats.attempt,
        stats.ingested_rows,
        stats.captured_rows,
        stats.cancelled,
        stats.live_tasks_after
    );
    for (i, row) in capture.rows_as_debug().iter().enumerate() {
        println!("  {i}: {}", row.join(" | "));
    }
    if stats.live_tasks_after != 0 || kernel.live_tasks() != 0 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            "orphan tasks after job completion",
        ));
    }
    if capture.row_count() != 3 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected 3 hot rows, got {}", capture.row_count()),
        ));
    }

    // Graceful stop of a second attempt (source still running against a stalled sink).
    let capture2 = SharedCapture::new();
    capture2.stall.stall();
    let handle = kernel.submit(JobRequest {
        plan: physicalize(&bound, &PlanOptions { fuse: true }),
        rows: sensor_fixture().into_iter().map(|r| r.to_row()).collect(),
        capture: capture2.clone(),
    })?;
    capture2.stall.release();
    let stopped = kernel.block_on(handle.stop())?;
    println!(
        "stop: cancelled={} live_tasks={}",
        stopped.cancelled, stopped.live_tasks_after
    );
    if kernel.live_tasks() != 0 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            "orphan tasks after stop",
        ));
    }
    println!("m1_kernel_smoke: ok");
    Ok(())
}

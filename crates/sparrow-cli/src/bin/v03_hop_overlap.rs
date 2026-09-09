//! Hopping window overlap: planner-enforced bound + a legal hop run.

use sparrow_expr::Expr;
use sparrow_model::{
    check_hop_overlap_bound, AggFn, DataType, ErrorCode, Field, FieldId, InputId, PipelineId,
    ResourceBudget, RevisionId, Row, Scalar, Schema, SchemaId, WindowKind,
};
use sparrow_plan::{bind_window_linear, physicalize, AggCall, PlanOptions, WindowSpec};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture, StreamControl};

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(3), "ts", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v03_hop_overlap failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.3 hopping overlap (planner-enforced) ===");
    let rejected = check_hop_overlap_bound(90_000_000, 10_000_000, 8);
    match rejected {
        Err(e) if e.code == ErrorCode::BoundExceeded => {
            println!("planner rejected hop overlap 9 > max 8");
        }
        other => {
            return Err(sparrow_model::SparrowError::new(
                ErrorCode::Internal,
                format!("expected overlap reject, got {other:?}"),
            ));
        }
    }
    let overlap = check_hop_overlap_bound(10_000_000, 5_000_000, 8)?;
    println!("hop size=10s slide=5s overlap={overlap} accepted");

    let spec = WindowSpec::new(
        WindowKind::hopping_et(10_000_000, 5_000_000)?,
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Avg,
            Some(Expr::Column {
                name: "temperature".into(),
            }),
            "avg_temp",
        )],
    )
    .event_time("ts", 3_000_000);
    spec.validate()?;
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema(),
        None,
        spec,
        "c".into(),
    )?;
    let plan = physicalize(&bound, &PlanOptions::default());
    let kernel = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 16,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 1,
    })?;
    let rows = vec![Row {
        values: vec![
            Scalar::utf8("d1"),
            Scalar::Float64(80.0),
            Scalar::Int64(6_000_000),
        ],
    }];
    let capture = SharedCapture::new();
    kernel.run(
        JobRequest::new(plan, rows, capture.clone()).with_controls(vec![StreamControl::Watermark {
            input: InputId(0).raw(),
            wm_micros: 13_000_000,
        }]),
    )?;
    let finals = capture.rows();
    println!("emitted {} hop finals at wm_out=10s", finals.len());
    for r in &finals {
        println!("  {r:?}");
    }
    let has_0_10 = finals.iter().any(|r| {
        matches!(
            r.as_slice(),
            [_, Scalar::Int64(0), Scalar::Int64(10_000_000), Scalar::Float64(v)]
                if (*v - 80.0).abs() < 1e-9
        )
    });
    if !has_0_10 {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            format!("missing hop FINAL [0,10) avg=80: {finals:?}"),
        ));
    }
    println!("FINAL hop [0,10) avg=80");
    println!("v03_hop_overlap: ok");
    Ok(())
}

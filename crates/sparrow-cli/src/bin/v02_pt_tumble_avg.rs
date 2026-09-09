//! Virtual-clock processing-time tumbling AVG. Prints expected finals and exits 0.

use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, DeliveryContract, Field, FieldId, PipelineId, ResourceBudget, RevisionId, Row,
    Scalar, Schema, SchemaId, WindowKind,
};
use sparrow_plan::{bind_window_linear, physicalize, AggCall, PlanOptions, WindowSpec};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, MailboxConfig, RuntimeClock, SharedCapture};

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
        ],
    )
    .unwrap()
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v02_pt_tumble_avg failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.2 PT tumbling AVG (virtual clock) ===");
    println!("recovery={} ({})", DeliveryContract::V0_2.recovery.none_label(), DeliveryContract::V0_2.recovery.as_str());
    println!("{}", DeliveryContract::PT_WINDOW_HONESTY);

    let spec = WindowSpec::new(
        WindowKind::tumbling_pt(1_000_000)?,
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Avg,
            Some(Expr::Column {
                name: "temperature".into(),
            }),
            "avg_temp",
        )],
    );
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "sensors".into(),
        schema(),
        None,
        spec,
        "capture".into(),
    )?;
    let plan = physicalize(&bound, &PlanOptions::default());
    assert!(plan.has_processing_time_window());
    assert_eq!(plan.recovery_label(), "none");

    let expected = [("a", 15.0), ("b", 30.0)];
    println!("expected finals: a.avg=15.0  b.avg=30.0");

    let kernel = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 16,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 8,
    })?;
    let clock = RuntimeClock::virtual_clock(sparrow_model::SharedVirtualClock::new(0));
    let rows = vec![
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(10.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(20.0)],
        },
        Row {
            values: vec![Scalar::utf8("b"), Scalar::Float64(30.0)],
        },
    ];
    let capture = SharedCapture::new();
    let handle = kernel.submit(
        JobRequest::new(plan, rows, capture.clone()).with_clock(clock.clone()),
    )?;
    std::thread::sleep(std::time::Duration::from_millis(50));
    clock.advance_virtual(1_000_000);
    kernel.block_on(handle.wait())?;

    let got = capture.rows();
    println!("emitted {} window finals", got.len());
    for row in &got {
        println!("  {row:?}");
    }
    for (dev, avg) in expected {
        let ok = got.iter().any(|r| {
            matches!((r.first(), r.last()), (Some(Scalar::Utf8(d)), Some(Scalar::Float64(v))) if d.as_ref() == dev && (*v - avg).abs() < 1e-9)
        });
        if !ok {
            return Err(sparrow_model::SparrowError::new(
                sparrow_model::ErrorCode::Internal,
                format!("missing expected {dev}={avg} in {got:?}"),
            ));
        }
        println!("FINAL {dev} avg={avg}");
    }
    println!("v02_pt_tumble_avg: ok");
    Ok(())
}

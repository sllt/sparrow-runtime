//! Count-window demo: emit AVG every 2 rows per key.

use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, Field, FieldId, PipelineId, ResourceBudget, RevisionId, Row, Scalar, Schema,
    SchemaId, WindowKind,
};
use sparrow_plan::{bind_window_linear, physicalize, AggCall, PlanOptions, WindowSpec};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture};

fn main() {
    if let Err(e) = run() {
        eprintln!("v02_count_window failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.2 count window ===");
    let schema = Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
        ],
    )?;
    let spec = WindowSpec {
        kind: WindowKind::count(2)?,
        keys: vec!["device_id".into()],
        aggs: vec![AggCall::new(
            AggFn::Avg,
            Some(Expr::Column {
                name: "temperature".into(),
            }),
            "avg_temp",
        )],
    };
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema,
        None,
        spec,
        "c".into(),
    )?;
    let rows = vec![
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(10.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(20.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(40.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(50.0)],
        },
    ];
    println!("expected: two finals avg=15 and avg=45");
    let k = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 1,
    })?;
    let capture = SharedCapture::new();
    k.run(JobRequest::new(
        physicalize(&bound, &PlanOptions::default()),
        rows,
        capture.clone(),
    ))?;
    let got = capture.rows();
    println!("emitted {}", got.len());
    for r in &got {
        println!("  {r:?}");
    }
    let avgs: Vec<f64> = got
        .iter()
        .filter_map(|r| match r.last() {
            Some(Scalar::Float64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    if avgs.len() != 2 || !avgs.contains(&15.0) || !avgs.contains(&45.0) {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected 15 and 45, got {avgs:?}"),
        ));
    }
    println!("FINAL avgs={avgs:?}");
    println!("v02_count_window: ok");
    Ok(())
}

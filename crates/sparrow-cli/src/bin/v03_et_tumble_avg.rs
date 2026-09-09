//! §51.2-style event-time tumble AVG with holdback and late side output.
//! Injected event times (not wall clock). Window [0,10s), L=3s, key d1.

use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, DeliveryContract, Field, FieldId, InputId, PipelineId, ResourceBudget,
    RevisionId, Row, Scalar, Schema, SchemaId, WindowKind,
};
use sparrow_plan::{bind_window_linear, physicalize, AggCall, PlanOptions, WindowSpec};
use sparrow_runtime::{
    JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture, StreamControl,
};

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

fn row(temp: f64, ts: i64) -> Row {
    Row {
        values: vec![Scalar::utf8("d1"), Scalar::Float64(temp), Scalar::Int64(ts)],
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v03_et_tumble_avg failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.3 event-time tumble AVG (injected event times) ===");
    println!(
        "recovery={} ({})",
        DeliveryContract::V0_3.recovery.none_label(),
        DeliveryContract::V0_3.recovery.as_str()
    );
    println!("{}", DeliveryContract::ET_WINDOW_HONESTY);
    println!("window [0,10s)  L=3s  key=d1  wm_out <= wm_in - L");

    let spec = WindowSpec::new(
        WindowKind::tumbling_et(10_000_000)?,
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
    assert!(plan.has_event_time_window());
    assert_eq!(plan.recovery_label(), "none");

    let kernel = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 16,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 1,
    })?;
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (ctx, crx) = tokio::sync::mpsc::channel(8);
    let (out_tx, _out_rx) = tokio::sync::mpsc::channel(8);
    let capture = SharedCapture::new();
    let handle = kernel.submit(
        JobRequest::new(plan, Vec::new(), capture.clone())
            .with_live_io(rx, out_tx)
            .with_live_ctrl(crx),
    )?;

    println!("STEP inject t=1s v=100");
    kernel.block_on(tx.send(row(100.0, 1_000_000))).unwrap();
    println!("STEP inject t=4s v=60");
    kernel.block_on(tx.send(row(60.0, 4_000_000))).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(40));
    if capture.row_count() != 0 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            "must not emit FINAL before e+L",
        ));
    }
    println!("STEP advance wm_in=13s (e+L = 10s+3s)");
    kernel
        .block_on(ctx.send(StreamControl::Watermark {
            input: InputId(0).raw(),
            wm_micros: 13_000_000,
        }))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let finals = capture.rows();
    let ok = finals.iter().any(|r| {
        matches!(
            r.as_slice(),
            [Scalar::Utf8(d), Scalar::Int64(0), Scalar::Int64(10_000_000), Scalar::Float64(v)]
                if d.as_ref() == "d1" && (*v - 80.0).abs() < 1e-9
        )
    });
    if !ok || finals.len() != 1 {
        drop(tx);
        drop(ctx);
        let _ = kernel.block_on(handle.wait());
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected one FINAL d1 avg=80 at e+L, got {finals:?}"),
        ));
    }
    println!("FINAL d1 avg=80");

    println!("STEP inject late t=8s after close");
    kernel.block_on(tx.send(row(99.0, 8_000_000))).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(60));
    drop(tx);
    drop(ctx);
    kernel.block_on(handle.wait())?;

    if capture.row_count() != 1 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("FINAL must stay once, got {} rows", capture.row_count()),
        ));
    }
    if capture.late_count() != 1 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected late side output, got {}", capture.late_count()),
        ));
    }
    let late = &capture.late_rows()[0];
    if late.get(2) != Some(&Scalar::Int64(8_000_000)) {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("late row ts mismatch: {late:?}"),
        ));
    }
    println!("LATE t=8 d1 (side output after close; no retract)");
    println!("v03_et_tumble_avg: ok");
    Ok(())
}

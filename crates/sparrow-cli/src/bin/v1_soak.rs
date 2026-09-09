//! Finite soak / fault loops for V1 (not a 72h wall clock).

use sparrow_expr::Expr;
use sparrow_io::{MemoryReplaySource, ReplayableSource};
use sparrow_model::{
    AggFn, DataType, ErrorCode, Field, FieldId, OperatorId, ResourceBudget, Schema, SchemaId,
    WindowKind,
};
use sparrow_plan::{AggCall, WindowSpec};
use sparrow_runtime::{run_until, AlignedSession, CheckpointStore, FaultPoint};

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn spec() -> WindowSpec {
    WindowSpec::new(
        WindowKind::Count { size: 3 },
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Sum,
            Some(Expr::Column { name: "v".into() }),
            "s",
        )],
    )
}

fn lines() -> Vec<String> {
    (1..=6)
        .map(|i| format!(r#"{{"device_id":"d1","v":{}}}"#, i * 10))
        .collect()
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v1_soak failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "sparrow-v1-soak-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let owned = lines();
    let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();

    println!("=== Sparrow V1 soak (finite) ===");

    for i in 0..6 {
        let dir = root.join(format!("start-stop-{i}"));
        let mut src = MemoryReplaySource::from_lines("soak", &text);
        let mut session = AlignedSession::open(
            CheckpointStore::open(&dir)?,
            spec(),
            schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            src.position(),
        )?;
        run_until(&mut session, &mut src, 0, Some(2))?;
        session.request_stop();
        assert!(session.checkpoint_barrier().is_err());
        println!("start/stop loop {i} ok (stop aborted checkpoint)");
    }

    let chk = root.join("loop");
    let mut src = MemoryReplaySource::from_lines("soak", &text);
    let mut session = AlignedSession::open(
        CheckpointStore::open(&chk)?,
        spec(),
        schema(),
        OperatorId::new(2),
        ResourceBudget::compact(),
        src.position(),
    )?;
    run_until(&mut session, &mut src, 0, Some(2))?;
    session.checkpoint_barrier()?;
    for i in 0..5 {
        let mut src2 = MemoryReplaySource::from_lines("soak", &text);
        let mut restored = AlignedSession::restore(
            CheckpointStore::open(&chk)?,
            spec(),
            schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            &mut src2,
        )?;
        assert_eq!(restored.ingested, 2);
        restored.checkpoint_barrier()?;
        println!("checkpoint/restore loop {i} ingested={}", restored.ingested);
    }

    for (label, point) in [
        ("disk-full", FaultPoint::DuringChunkWrite),
        ("corrupt-manifest", FaultPoint::CorruptChecksum),
    ] {
        let dir = root.join(label);
        let mut store = CheckpointStore::open(&dir)?;
        store.fault.point = point;
        let mut src = MemoryReplaySource::from_lines("soak", &text);
        let mut session = AlignedSession::open(
            store,
            spec(),
            schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            src.position(),
        )?;
        run_until(&mut session, &mut src, 0, Some(2))?;
        if point == FaultPoint::CorruptChecksum {
            session.checkpoint_barrier()?;
            let rec = CheckpointStore::open(&dir)?.recover_committed();
            assert!(rec.is_err(), "corrupt MANIFEST must reject restore");
            println!("{label}: restore rejected");
        } else {
            let err = session.checkpoint_barrier().unwrap_err();
            assert_eq!(err.code, ErrorCode::ResourceExhausted);
            assert!(CheckpointStore::open(&dir)?.recover_committed()?.is_none());
            println!("{label}: commit rejected ({})", err.code);
        }
    }

    let _ = std::fs::remove_dir_all(&root);
    println!("v1_soak: ok");
    Ok(())
}

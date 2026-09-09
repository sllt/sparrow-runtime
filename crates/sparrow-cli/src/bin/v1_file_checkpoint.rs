//! V1 production file/replay checkpoint: committed snapshot, restore in a
//! new process, prove count / ET window state match gold. Not exactly-once.

use std::path::PathBuf;

use sparrow_connectors::{FileReplayConfig, FileReplaySource};
use sparrow_expr::Expr;
use sparrow_io::ReplayableSource;
use sparrow_model::{
    AggFn, DataType, DeliveryContract, Field, FieldId, OperatorId, RecoveryPolicy, ResourceBudget,
    Schema, SchemaId, WindowKind,
};
use sparrow_plan::{AggCall, WindowSpec};
use sparrow_runtime::{run_until, AlignedSession, CheckpointStore};

fn count_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn count_spec() -> WindowSpec {
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

fn et_schema() -> Schema {
    Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, false),
            Field::new(FieldId::new(3), "ts", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn et_spec() -> WindowSpec {
    WindowSpec::new(
        WindowKind::TumblingEventTime {
            size_micros: 1_000_000,
        },
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Avg,
            Some(Expr::Column {
                name: "temperature".into(),
            }),
            "avg_t",
        )],
    )
    .event_time("ts", 0)
}

fn usage() -> ! {
    eprintln!(
        "usage: v1_file_checkpoint --data FILE --chk DIR --mode gold|checkpoint|restore [--until N] [--window count|et]"
    );
    std::process::exit(2);
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v1_file_checkpoint failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut data = None;
    let mut chk = None;
    let mut mode = "gold".to_string();
    let mut until = None;
    let mut window = "count".to_string();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => {
                data = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--chk" => {
                chk = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--mode" => {
                mode = args[i + 1].clone();
                i += 2;
            }
            "--until" => {
                until = Some(args[i + 1].parse::<u64>().unwrap_or(0));
                i += 2;
            }
            "--window" => {
                window = args[i + 1].clone();
                i += 2;
            }
            _ => usage(),
        }
    }
    let data = data.unwrap_or_else(|| usage());
    let chk = chk.unwrap_or_else(|| usage());

    println!("=== Sparrow V1 production file/replay checkpoint ===");
    println!("label={}", CheckpointStore::label());
    println!("recovery={}", RecoveryPolicy::Aligned.as_str());
    println!("{}", DeliveryContract::ALIGNED_CHECKPOINT_HONESTY);
    println!("exactly-once=rejected");
    println!("window={window}");

    let (schema, spec) = if window == "et" {
        (et_schema(), et_spec())
    } else {
        (count_schema(), count_spec())
    };

    let cfg = FileReplayConfig {
        path: data.clone(),
        schema: schema.clone(),
        restore: sparrow_model::RestoreClaim::Checkpoint {
            snapshot_id: "aligned".into(),
        },
        recovery: RecoveryPolicy::Aligned,
        contract: sparrow_connectors::FileContract::Immutable,
    };
    let mut source = FileReplaySource::open(&cfg).map_err(|e| {
        sparrow_model::SparrowError::new(e.code(), e.to_string())
    })?;
    let store = CheckpointStore::open(&chk)?;

    let mut session = if mode == "restore" {
        AlignedSession::restore(
            store,
            spec,
            schema,
            OperatorId::new(2),
            ResourceBudget::compact(),
            &mut source,
        )?
    } else {
        let start = source.position();
        AlignedSession::open(
            store,
            spec,
            schema,
            OperatorId::new(2),
            ResourceBudget::compact(),
            start,
        )?
    };

    if mode == "restore" {
        println!(
            "RESTORE position offset={} records={} ingested={}",
            session.source_pos.offset_bytes,
            session.source_pos.record_index,
            session.ingested
        );
        println!("RESTORE state {}", session.state_fingerprint());
        println!(
            "METRICS {}",
            session.metrics.snapshot().log_line()
        );
    }

    let lim = match mode.as_str() {
        "checkpoint" => until.or(Some(2)),
        _ => None,
    };
    run_until(&mut session, &mut source, 0, lim)?;
    if window == "et" && mode != "checkpoint" {
        session.observe_watermark(0, 3_000_000)?;
    }

    if mode == "checkpoint" {
        let id = session.checkpoint_barrier()?;
        println!(
            "CHECKPOINT id={id} position offset={} records={} ingested={}",
            session.source_pos.offset_bytes,
            session.source_pos.record_index,
            session.ingested
        );
        println!("CHECKPOINT state {}", session.state_fingerprint());
        println!(
            "METRICS {}",
            session.metrics.snapshot().log_line()
        );
        println!("v1_file_checkpoint: checkpoint ok");
        return Ok(());
    }

    println!(
        "FINALS count={} ingested={} position records={}",
        session.finals.len(),
        session.ingested,
        session.source_pos.record_index
    );
    for r in &session.finals {
        println!("FINAL {:?}", r.values);
    }
    println!("METRICS {}", session.metrics.snapshot().log_line());
    println!("v1_file_checkpoint: {mode} ok");
    Ok(())
}

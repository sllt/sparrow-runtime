//! Versioned reference table: as-of event-time lookup (beyond V0.2 freeze).

use sparrow_model::{
    DataType, Field, FieldId, MemoryOwner, PipelineId, ResourceBudget, RevisionId, Row, Scalar,
    Schema, SchemaId,
};
use sparrow_plan::{bind_lookup_linear, physicalize, LookupSpec, PlanOptions};
use sparrow_runtime::{
    lookup::table_from_pairs, JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture,
    VersionedReferenceTable,
};

fn stream() -> Schema {
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
        eprintln!("v03_versioned_lookup failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.3 versioned ReferenceTable (as-of event time) ===");
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let v1 = table_from_pairs(
        "sites",
        1,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("d1"), Scalar::utf8("west"))],
        &owner,
    )?;
    let v2 = table_from_pairs(
        "sites",
        2,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("d1"), Scalar::utf8("east"))],
        &owner,
    )?;
    let table = VersionedReferenceTable::new("sites", 8)?;
    table.publish(1, 0, v1.clone())?;
    table.publish(2, 5_000_000, v2)?;
    println!("v1 valid_from=0 site=west; v2 valid_from=5s site=east");

    let spec = LookupSpec {
        table: "sites".into(),
        stream_keys: vec!["device_id".into()],
        table_keys: vec!["device_id".into()],
        keep: vec!["site".into()],
        temporal: true,
        as_of_field: Some("ts".into()),
    };
    let bound = bind_lookup_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        stream(),
        spec,
        v1.schema.clone(),
        "c".into(),
    )?;
    let plan = physicalize(&bound, &PlanOptions::default());
    let kernel = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 8,
    })?;
    let rows = vec![
        Row {
            values: vec![
                Scalar::utf8("d1"),
                Scalar::Float64(1.0),
                Scalar::Int64(1_000_000),
            ],
        },
        Row {
            values: vec![
                Scalar::utf8("d1"),
                Scalar::Float64(1.0),
                Scalar::Int64(6_000_000),
            ],
        },
    ];
    let capture = SharedCapture::new();
    let mut tables = std::collections::HashMap::new();
    tables.insert("sites".into(), table);
    kernel.run(JobRequest::new(plan, rows, capture.clone()).with_versioned_tables(tables))?;
    let got = capture.rows();
    let s0 = match got[0].last() {
        Some(Scalar::Utf8(s)) => s.as_ref(),
        _ => "",
    };
    let s1 = match got[1].last() {
        Some(Scalar::Utf8(s)) => s.as_ref(),
        _ => "",
    };
    println!("as-of t=1s site={s0}");
    println!("as-of t=6s site={s1}");
    if s0 != "west" || s1 != "east" {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected west then east, got {got:?}"),
        ));
    }
    println!("v03_versioned_lookup: ok");
    Ok(())
}

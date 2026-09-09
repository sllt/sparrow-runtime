//! Static ReferenceTable enrichment. New Job sees a new snapshot.

use sparrow_model::{
    DataType, Field, FieldId, MemoryOwner, PipelineId, ResourceBudget, RevisionId, Row, Scalar,
    Schema, SchemaId,
};
use sparrow_plan::{bind_lookup_linear, physicalize, LookupSpec, PlanOptions};
use sparrow_runtime::{
    lookup::table_from_pairs, JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture,
};

fn sensor() -> Schema {
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
        eprintln!("v02_static_table failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.2 static ReferenceTable ===");
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let v1 = table_from_pairs(
        "sites",
        1,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("west"))],
        &owner,
    )?;
    let v2 = table_from_pairs(
        "sites",
        2,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("east"))],
        &owner,
    )?;
    println!("snapshot v1 site=west; snapshot v2 site=east");

    let spec = LookupSpec {
        table: "sites".into(),
        stream_keys: vec!["device_id".into()],
        table_keys: vec!["device_id".into()],
        keep: vec!["site".into()],
    };
    let bound = bind_lookup_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        sensor(),
        spec,
        v1.schema.clone(),
        "c".into(),
    )?;
    let plan = physicalize(&bound, &PlanOptions::default());
    let rows = vec![Row {
        values: vec![Scalar::utf8("a"), Scalar::Float64(21.0)],
    }];
    let k = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 4,
    })?;
    let a = SharedCapture::new();
    let b = SharedCapture::new();
    let mut t1 = std::collections::HashMap::new();
    t1.insert("sites".into(), v1);
    let mut t2 = std::collections::HashMap::new();
    t2.insert("sites".into(), v2);
    k.run(JobRequest::new(plan.clone(), rows.clone(), a.clone()).with_tables(t1))?;
    k.run(JobRequest::new(plan, rows, b.clone()).with_tables(t2))?;
    let site = |cap: &SharedCapture| match cap.rows()[0].last() {
        Some(Scalar::Utf8(s)) => s.to_string(),
        _ => String::new(),
    };
    println!("job1 (running keeps v1) site={}", site(&a));
    println!("job2 (new job uses v2) site={}", site(&b));
    if site(&a) != "west" || site(&b) != "east" {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!("expected west/east, got {}/{}", site(&a), site(&b)),
        ));
    }
    println!("v02_static_table: ok");
    Ok(())
}

//! Bounded dedup demo: reject unbounded config, then run a TTL-scoped dedup.

use sparrow_model::{
    DataType, ErrorCode, Field, FieldId, PipelineId, ResourceBudget, RevisionId, Row, Scalar,
    Schema, SchemaId,
};
use sparrow_plan::{bind_dedup_linear, physicalize, DedupSpec, PlanOptions};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, MailboxConfig, RuntimeClock, SharedCapture};

fn main() {
    if let Err(e) = run() {
        eprintln!("v02_bounded_dedup failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.2 bounded dedup ===");
    let forever = DedupSpec {
        keys: vec!["device_id".into()],
        ttl_micros: 0,
        max_keys: 100,
    }
    .validate();
    match forever {
        Err(e) => {
            println!("rejected unbounded forever-dedup: {e}");
            if e.code != ErrorCode::InvalidArgument {
                return Err(e);
            }
        }
        Ok(()) => {
            return Err(sparrow_model::SparrowError::new(
                ErrorCode::Internal,
                "forever-dedup must be rejected",
            ));
        }
    }
    let no_cap = DedupSpec {
        keys: vec!["device_id".into()],
        ttl_micros: 1_000,
        max_keys: 0,
    }
    .validate()
    .unwrap_err();
    println!("rejected max_keys=0: {no_cap}");

    let schema = Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
        ],
    )?;
    let spec = DedupSpec {
        keys: vec!["device_id".into()],
        ttl_micros: 5_000_000,
        max_keys: 64,
    };
    let bound = bind_dedup_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        schema,
        spec,
        "c".into(),
    )?;
    let rows = vec![
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(1.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(2.0)],
        },
        Row {
            values: vec![Scalar::utf8("b"), Scalar::Float64(3.0)],
        },
    ];
    let k = Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 8,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 8,
    })?;
    let clock = RuntimeClock::virtual_clock(sparrow_model::SharedVirtualClock::new(0));
    let capture = SharedCapture::new();
    k.run(
        JobRequest::new(physicalize(&bound, &PlanOptions::default()), rows, capture.clone())
            .with_clock(clock),
    )?;
    println!("kept {} rows (expect 2: first a, first b)", capture.row_count());
    if capture.row_count() != 2 {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            format!("expected 2 kept rows, got {}", capture.row_count()),
        ));
    }
    println!("v02_bounded_dedup: ok");
    Ok(())
}

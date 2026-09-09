//! G3 evidence: incremental accumulators, quotas, cleanup, virtual clock.

use super::*;
use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, ErrorCode, Field, FieldId, MemoryOwner, OperatorId, PipelineId, ResourceBudget,
    RevisionId, Row, Scalar, Schema, SchemaId, WindowKind,
};
use sparrow_plan::{
    bind_dedup_linear, bind_lookup_linear, bind_window_linear, physicalize, AggCall, DedupSpec,
    LookupSpec, PlanOptions, WindowSpec,
};
use std::sync::Arc;

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

fn kernel() -> Kernel {
    Kernel::new(KernelOptions {
        budget: ResourceBudget::compact(),
        mailbox: MailboxConfig {
            max_items: 16,
            max_bytes: 64 * 1024,
        },
        worker_threads: 2,
        rows_per_batch: 8,
    })
    .unwrap()
}

fn avg_spec() -> WindowSpec {
    WindowSpec {
        kind: WindowKind::tumbling_pt(1_000_000).unwrap(),
        keys: vec!["device_id".into()],
        aggs: vec![AggCall::new(
            AggFn::Avg,
            Some(Expr::Column {
                name: "temperature".into(),
            }),
            "avg_temp",
        )],
    }
}

#[test]
fn incremental_accumulator_smaller_than_raw_rows() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = crate::window::WindowOperator::new(
        OperatorId::new(2),
        avg_spec(),
        sensor(),
        Arc::clone(&owner),
        64,
        64,
    )
    .unwrap();
    let mut raw = 0usize;
    let mut rows = Vec::new();
    for i in 0..200 {
        let row = Row {
            values: vec![Scalar::utf8("edge-a"), Scalar::Float64(i as f64)],
        };
        raw += row.tracked_bytes();
        rows.push(row);
    }
    let schema = Arc::new(sensor());
    let mut b = sparrow_model::RowBatchBuilder::new(
        schema,
        Arc::clone(&owner),
        sparrow_model::CreditKind::Reservation,
        200,
        64 * 1024,
    )
    .unwrap();
    for r in rows {
        b.push(r).unwrap();
    }
    let batch = b.finish().unwrap();
    let _ = op.on_batch(&batch, 500_000).unwrap();
    let state_bytes = op.retention_bytes();
    assert_eq!(op.key_count(), 1, "one key, not 200 raw rows");
    assert!(
        state_bytes < raw / 4,
        "accumulator {state_bytes}B should be << raw {raw}B"
    );
}

#[test]
fn key_and_timer_quotas_fail_closed() {
    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 4 * 1024 * 1024,
        retention_bytes: 4 * 1024 * 1024,
        queue_bytes: 1024 * 1024,
        max_rows: 256,
        work_units: 50_000,
        max_state_keys: 2,
        max_timers: 1,
    });
    let mut op = crate::window::WindowOperator::new(
        OperatorId::new(2),
        avg_spec(),
        sensor(),
        Arc::clone(&owner),
        2,
        1,
    )
    .unwrap();
    let mk = |id: &str| {
        let schema = Arc::new(sensor());
        let mut b = sparrow_model::RowBatchBuilder::new(
            schema,
            Arc::clone(&owner),
            sparrow_model::CreditKind::Reservation,
            1,
            1024,
        )
        .unwrap();
        b.push(Row {
            values: vec![Scalar::utf8(id), Scalar::Float64(1.0)],
        })
        .unwrap();
        b.finish().unwrap()
    };
    op.on_batch(&mk("a"), 100).unwrap();
    op.on_batch(&mk("b"), 100).unwrap();
    let err = op.on_batch(&mk("c"), 100).unwrap_err();
    assert_eq!(err.code, ErrorCode::ResourceExhausted);
}

#[test]
fn cleanup_releases_keys_and_timers() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = crate::window::WindowOperator::new(
        OperatorId::new(2),
        avg_spec(),
        sensor(),
        Arc::clone(&owner),
        64,
        64,
    )
    .unwrap();
    let schema = Arc::new(sensor());
    let mut b = sparrow_model::RowBatchBuilder::new(
        schema,
        Arc::clone(&owner),
        sparrow_model::CreditKind::Reservation,
        1,
        1024,
    )
    .unwrap();
    b.push(Row {
        values: vec![Scalar::utf8("a"), Scalar::Float64(2.0)],
    })
    .unwrap();
    let batch = b.finish().unwrap();
    op.on_batch(&batch, 10).unwrap();
    assert!(op.key_count() > 0);
    assert!(op.live_timers() > 0);
    let emitted = op.fire_due(1_000_000).unwrap();
    assert_eq!(emitted.len(), 1);
    assert_eq!(op.key_count(), 0);
    op.cleanup();
    assert_eq!(op.live_timers(), 0);
    assert_eq!(op.retention_bytes(), 0);
}

#[test]
fn virtual_clock_pt_tumble_avg() {
    let k = kernel();
    let clock = RuntimeClock::virtual_clock(sparrow_model::SharedVirtualClock::new(0));
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        sensor(),
        None,
        avg_spec(),
        "capture".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions::default());
    assert!(plan.has_processing_time_window());
    assert_eq!(plan.recovery_label(), "none");
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
    let handle = k
        .submit(JobRequest::new(plan, rows, capture.clone()).with_clock(clock.clone()))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(40));
    clock.advance_virtual(1_000_000);
    let stats = k.block_on(handle.wait()).unwrap();
    assert_eq!(stats.live_tasks_after, 0);
    let got = capture.rows();
    assert_eq!(got.len(), 2, "{got:?}");
    // avg a = 15, avg b = 30
    let avgs: Vec<f64> = got
        .iter()
        .map(|r| match r.last() {
            Some(Scalar::Float64(v)) => *v,
            _ => -1.0,
        })
        .collect();
    assert!(avgs.contains(&15.0), "{avgs:?}");
    assert!(avgs.contains(&30.0), "{avgs:?}");
}

#[test]
fn count_window_emits_on_size() {
    let k = kernel();
    let spec = WindowSpec {
        kind: WindowKind::count(2).unwrap(),
        keys: vec!["device_id".into()],
        aggs: vec![AggCall::count_star("n")],
    };
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        sensor(),
        None,
        spec,
        "c".into(),
    )
    .unwrap();
    let rows = vec![
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(1.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(2.0)],
        },
        Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(3.0)],
        },
    ];
    let capture = SharedCapture::new();
    k.run(JobRequest::new(
        physicalize(&bound, &PlanOptions::default()),
        rows,
        capture.clone(),
    ))
    .unwrap();
    assert_eq!(capture.row_count(), 1);
}

#[test]
fn dedup_rejects_unbounded() {
    let err = DedupSpec {
        keys: vec!["device_id".into()],
        ttl_micros: 0,
        max_keys: 10,
    }
    .validate()
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    let err = DedupSpec {
        keys: vec!["device_id".into()],
        ttl_micros: 1000,
        max_keys: 0,
    }
    .validate()
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BoundExceeded);
}

#[test]
fn static_table_snapshot_is_job_local() {
    let k = kernel();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let t1 = crate::lookup::table_from_pairs(
        "sites",
        1,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("west"))],
        &owner,
    )
    .unwrap();
    let t2 = crate::lookup::table_from_pairs(
        "sites",
        2,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("east"))],
        &owner,
    )
    .unwrap();
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
        t1.schema.clone(),
        "c".into(),
    )
    .unwrap();
    let plan = physicalize(&bound, &PlanOptions::default());
    let rows = vec![Row {
        values: vec![Scalar::utf8("a"), Scalar::Float64(1.0)],
    }];
    let a = SharedCapture::new();
    let b = SharedCapture::new();
    let mut tables1 = std::collections::HashMap::new();
    tables1.insert("sites".into(), t1);
    let mut tables2 = std::collections::HashMap::new();
    tables2.insert("sites".into(), t2);
    k.run(JobRequest::new(plan.clone(), rows.clone(), a.clone()).with_tables(tables1))
        .unwrap();
    k.run(JobRequest::new(plan, rows, b.clone()).with_tables(tables2))
        .unwrap();
    let site_a = match a.rows()[0].last() {
        Some(Scalar::Utf8(s)) => s.to_string(),
        _ => String::new(),
    };
    let site_b = match b.rows()[0].last() {
        Some(Scalar::Utf8(s)) => s.to_string(),
        _ => String::new(),
    };
    assert_eq!(site_a, "west");
    assert_eq!(site_b, "east");
}

#[test]
fn still_rejects_checkpoint_restore() {
    use sparrow_model::RestoreClaim;
    assert_eq!(
        RestoreClaim::Checkpoint {
            snapshot_id: "x".into()
        }
        .validate()
        .unwrap_err()
        .code,
        ErrorCode::UnsupportedRestore
    );
}

#[test]
fn bind_dedup_linear_runs() {
    let k = kernel();
    let spec = DedupSpec {
        keys: vec!["device_id".into()],
        ttl_micros: 5_000_000,
        max_keys: 16,
    };
    let bound = bind_dedup_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "s".into(),
        sensor(),
        spec,
        "c".into(),
    )
    .unwrap();
    let clock = RuntimeClock::virtual_clock(sparrow_model::SharedVirtualClock::new(0));
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
    let capture = SharedCapture::new();
    k.run(JobRequest::new(physicalize(&bound, &PlanOptions::default()), rows, capture.clone()).with_clock(clock))
        .unwrap();
    assert_eq!(capture.row_count(), 2);
}

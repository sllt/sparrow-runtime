//! Trials for the fixed-size-accumulator `working_credit` fast path.
//!
//! Included by `window.rs` as `mod credit_tests`, so `super::*` exposes the
//! operator and its private helpers. Every assertion here is about the
//! *budget/behavior contract* — the final lease bytes, the owner ledgers, and
//! the emitted rows — never about which internal path produced them. The
//! expected reservation is recomputed by [`legacy_reservation_bytes`], a plain
//! transcription of the pre-trial formula that never calls `working_credit`.

use super::*;

use sparrow_expr::allocation::AllocationBound;
use sparrow_expr::{bind, Expr};
use sparrow_model::{AggFn, CreditUsage, DataType, Field, FieldId, ResourceBudget, SchemaId};
use sparrow_plan::AggCall;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn budget_with(reservation_bytes: usize) -> ResourceBudget {
    ResourceBudget {
        reservation_bytes,
        retention_bytes: 16 * 1024 * 1024,
        queue_bytes: 1024 * 1024,
        ..ResourceBudget::compact()
    }
}

/// Reservation headroom for fixtures that must not be credit-limited.
fn roomy_owner() -> Arc<MemoryOwner> {
    MemoryOwner::new(budget_with(4 * 1024 * 1024))
}

/// Current credits only. `peak_physical_bytes` is monotone history and never
/// falls, so it is excluded from refund equality checks and verified separately
/// by [`assert_peak_never_regresses`].
fn credits(owner: &Arc<MemoryOwner>) -> CreditUsage {
    let mut usage = owner.usage();
    usage.peak_physical_bytes = 0;
    usage
}

/// The historical peak must not go backwards and must always cover what the
/// owner holds right now. Returns the observed peak for the next check.
fn assert_peak_never_regresses(owner: &Arc<MemoryOwner>, seen_peak: usize) -> usize {
    let usage = owner.usage();
    assert!(
        usage.peak_physical_bytes >= seen_peak,
        "peak_physical_bytes regressed: {} < {seen_peak}",
        usage.peak_physical_bytes
    );
    assert!(
        usage.peak_physical_bytes >= usage.physical_bytes,
        "peak_physical_bytes {} must cover the held credits {}",
        usage.peak_physical_bytes,
        usage.physical_bytes
    );
    usage.peak_physical_bytes
}

fn window(spec: &WindowSpec, schema: &Schema, owner: &Arc<MemoryOwner>) -> WindowOperator {
    WindowOperator::new(
        OperatorId::new(1),
        spec.clone(),
        schema.clone(),
        Arc::clone(owner),
        256,
        256,
    )
    .expect("fixture window construction")
}

fn build_batch(owner: &Arc<MemoryOwner>, schema: &Schema, rows: Vec<Row>) -> RowBatch {
    let max_rows = rows.len().max(1);
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema.clone()),
        Arc::clone(owner),
        CreditKind::Reservation,
        max_rows,
        1 << 20,
    )
    .expect("fixture batch builder");
    for row in rows {
        builder.push(row).expect("fixture batch row");
    }
    builder.finish().expect("fixture batch")
}

// ---------------------------------------------------------------------------
// Independent legacy oracle (never calls the code under test)
// ---------------------------------------------------------------------------

/// Legacy `base` term: input resident bytes, output width per row, per-input
/// -field index vector, hop overlap allowance, fixed slack.
fn legacy_base_bytes(
    spec: &WindowSpec,
    input: &Schema,
    output: &Schema,
    batch: Option<&RowBatch>,
) -> usize {
    let input_bytes = batch
        .map(|b| b.rows().iter().map(Row::resident_bytes).sum::<usize>())
        .unwrap_or(0);
    let rows = batch.map(RowBatch::num_rows).unwrap_or(0);
    input_bytes
        .saturating_mul(4)
        .saturating_add(
            rows.saturating_mul(output.fields.len().saturating_mul(96).saturating_add(128)),
        )
        .saturating_add(
            input
                .fields
                .len()
                .saturating_mul(std::mem::size_of::<usize>()),
        )
        .saturating_add((spec.max_overlap as usize).saturating_mul(32))
        .saturating_add(256)
}

/// Legacy final reservation for a concrete batch: `base` plus the folded
/// aggregate bound (per-column max resident bytes, not a per-row sum).
fn legacy_reservation_bytes(
    spec: &WindowSpec,
    input: &Schema,
    output: &Schema,
    batch: &RowBatch,
) -> usize {
    let base = legacy_base_bytes(spec, input, output, Some(batch));
    let mut columns = vec![0usize; input.fields.len()];
    for row in batch.rows() {
        for (size, value) in columns.iter_mut().zip(&row.values) {
            *size = (*size).max(value.resident_bytes());
        }
    }
    let mut allocation = 0usize;
    let mut values = 0usize;
    for agg in &spec.aggs {
        let Some(expr) = &agg.input else {
            continue;
        };
        let bound = bind(expr, input).expect("agg input binds against the fixture schema");
        let estimate = AllocationBound::for_expr(&bound).estimate(&columns);
        allocation = allocation.saturating_add(estimate.allocated);
        values = values.saturating_add(estimate.value);
    }
    base.saturating_add(allocation.saturating_mul(2))
        .saturating_add(values.saturating_mul(batch.num_rows()).saturating_mul(4))
}

// ---------------------------------------------------------------------------
// Narrow fixtures: 1..=65 columns, nullable UTF8, NULL rows, allocating call
// ---------------------------------------------------------------------------

fn narrow_schema(fields: usize) -> Schema {
    let fields = (0..fields)
        .map(|i| {
            let id = FieldId::new((i + 1) as u16);
            match i {
                0 => Field::new(id, "c0", DataType::Int64, false),
                // i % 3 == 1: nullable UTF8 so a row may carry NULL.
                _ if i % 3 == 1 => Field::new(id, format!("c{i}"), DataType::Utf8, true),
                _ if i % 3 == 2 => Field::new(id, format!("c{i}"), DataType::Utf8, false),
                _ => Field::new(id, format!("c{i}"), DataType::Int64, false),
            }
        })
        .collect();
    Schema::new(SchemaId::new(1), fields).expect("narrow fixture schema")
}

/// One fixture row. `utf8_len` varies per row so the credit must reflect the
/// per-column *max* (not the first row, not the sum). Row 0 puts NULL in `c1`.
fn narrow_row(fields: usize, salt: i64, utf8_len: usize, null_c1: bool) -> Row {
    let mut values = Vec::with_capacity(fields);
    for i in 0..fields {
        values.push(match i {
            0 => Scalar::Int64(salt),
            _ if i % 3 == 1 => {
                if null_c1 && i == 1 {
                    Scalar::Null
                } else {
                    Scalar::utf8(format!("{salt}{}", "u".repeat(utf8_len + i)))
                }
            }
            _ if i % 3 == 2 => Scalar::utf8(format!("{salt}{}", "v".repeat(utf8_len))),
            _ => Scalar::Int64(salt.saturating_mul(i as i64)),
        });
    }
    Row { values }
}

/// Fixed-size accumulators only: SUM/AVG have no retained variable payload.
/// COUNT(lower(c1)) adds a call that *allocates* a fresh UTF8 buffer.
fn narrow_aggs(fields: usize) -> Vec<AggCall> {
    let mut aggs = vec![
        AggCall::new(AggFn::Sum, Some(Expr::Column { name: "c0".into() }), "s"),
        AggCall::new(AggFn::Avg, Some(Expr::Column { name: "c0".into() }), "a"),
    ];
    if fields >= 2 {
        aggs.push(AggCall::new(
            AggFn::Count,
            Some(Expr::Call {
                name: "lower".into(),
                args: vec![Expr::Column { name: "c1".into() }],
            }),
            "cl",
        ));
    }
    aggs
}

fn narrow_batch(owner: &Arc<MemoryOwner>, schema: &Schema, fields: usize) -> RowBatch {
    build_batch(
        owner,
        schema,
        vec![
            narrow_row(fields, 1, 0, true),
            narrow_row(fields, 2, 4096, false),
            narrow_row(fields, 3, 64, false),
        ],
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The fixed-size-accumulator path (1 / 64 / 65 columns, COUNT / PT / ET) must
/// take exactly the reservation the legacy formula computed — byte for byte.
#[test]
fn omp_trial_narrow_accumulator_lease_matches_legacy_oracle() {
    let batches = roomy_owner();
    for fields in [1usize, 64, 65] {
        let schema = narrow_schema(fields);
        let aggs = narrow_aggs(fields);
        let stages = [
            WindowSpec::new(
                WindowKind::count(4).unwrap(),
                vec!["c0".into()],
                aggs.clone(),
            ),
            WindowSpec::new(
                WindowKind::tumbling_pt(1_000).unwrap(),
                vec!["c0".into()],
                aggs.clone(),
            ),
            WindowSpec::new(
                WindowKind::tumbling_et(1_000).unwrap(),
                vec!["c0".into()],
                aggs,
            )
            .event_time("c0", 0),
        ];
        let batch = narrow_batch(&batches, &schema, fields);
        for spec in stages.into_iter() {
            let owner = roomy_owner();
            let window = window(&spec, &schema, &owner);
            let output = window.output_schema().clone();
            let expected = legacy_reservation_bytes(&spec, &schema, &output, &batch);
            let base = legacy_base_bytes(&spec, &schema, &output, Some(&batch));
            let before = credits(&owner);
            let before_peak = owner.usage().peak_physical_bytes;

            let lease = window
                .working_credit(Some(&batch))
                .expect("narrow path admits the fixture batch");
            assert_eq!(lease.kind(), CreditKind::Reservation);
            assert_eq!(
                lease.bytes(),
                expected,
                "fields={fields} kind={:?}: final reservation must equal the legacy formula",
                spec.kind
            );
            assert!(
                lease.bytes() > base,
                "fields={fields} kind={:?}: the folded allocation/value terms must be charged",
                spec.kind
            );
            let peak_held = assert_peak_never_regresses(&owner, before_peak);
            drop(lease);
            assert_eq!(
                credits(&owner),
                before,
                "fields={fields} kind={:?}: credit must be fully returned",
                spec.kind
            );
            assert_peak_never_regresses(&owner, peak_held);
        }
    }
}

/// `None` and `Some(empty batch)` are different calls: `None` charges only the
/// base term, an empty batch still pays for the (eagerly allocating) agg calls.
#[test]
fn omp_trial_empty_batch_and_none_use_distinct_formulas() {
    let schema = narrow_schema(4);
    let spec = WindowSpec::new(
        WindowKind::count(4).unwrap(),
        vec!["c0".into()],
        narrow_aggs(4),
    );
    let owner = roomy_owner();
    let window = window(&spec, &schema, &owner);
    let output = window.output_schema().clone();
    let before = credits(&owner);
    let peak = owner.usage().peak_physical_bytes;

    let base = legacy_base_bytes(&spec, &schema, &output, None);
    let none_lease = window.working_credit(None).expect("unbatched credit");
    assert_eq!(
        none_lease.bytes(),
        base,
        "None must not fold any per-batch aggregate term"
    );
    drop(none_lease);

    let empty = build_batch(&roomy_owner(), &schema, Vec::new());
    let empty_expected = legacy_reservation_bytes(&spec, &schema, &output, &empty);
    let empty_lease = window
        .working_credit(Some(&empty))
        .expect("empty-batch credit");
    assert_eq!(
        empty_lease.bytes(),
        empty_expected,
        "an empty batch still reserves for the bound aggregate expressions"
    );
    assert!(
        empty_lease.bytes() > base,
        "empty batch must be more expensive than None when a call allocates"
    );
    drop(empty_lease);

    assert_eq!(credits(&owner), before, "both leases must be returned");
    assert_peak_never_regresses(&owner, peak);
}

/// A reservation that exactly fits is admitted; one byte short is refused with
/// `ResourceExhausted` and leaves state, credits and handles untouched.
#[test]
fn omp_trial_exact_budget_admitted_one_byte_short_leaves_no_trace() {
    let schema = narrow_schema(3);
    let spec = WindowSpec::new(
        WindowKind::count(8).unwrap(),
        vec!["c0".into()],
        narrow_aggs(3),
    );
    let batch = narrow_batch(&roomy_owner(), &schema, 3);

    // Learn the required size on a probe fixture (the batch lives on an
    // unrelated owner so only the window lease is measured).
    let probe_owner = roomy_owner();
    let probe = window(&spec, &schema, &probe_owner);
    let ctor_baseline = probe_owner.usage();
    let want = {
        let lease = probe.working_credit(Some(&batch)).expect("probe credit");
        lease.bytes()
    };
    assert!(want > ctor_baseline.reservation_bytes);
    drop(probe);
    assert_eq!(probe_owner.usage().reservation_bytes, 0);

    let exact = ctor_baseline.reservation_bytes + want;
    let tight_owner = MemoryOwner::new(budget_with(exact));
    let tight = window(&spec, &schema, &tight_owner);
    assert_eq!(
        tight_owner.usage().reservation_bytes,
        ctor_baseline.reservation_bytes
    );
    let baseline = credits(&tight_owner);
    let baseline_peak = tight_owner.usage().peak_physical_bytes;
    let lease = tight
        .working_credit(Some(&batch))
        .expect("a reservation that exactly fits must be admitted");
    assert_eq!(lease.bytes(), want);
    assert_eq!(
        tight_owner.usage().reservation_bytes,
        baseline.reservation_bytes + want
    );
    assert_eq!(tight_owner.usage().live_handles, baseline.live_handles + 1);
    let tight_peak = assert_peak_never_regresses(&tight_owner, baseline_peak);
    drop(lease);
    assert_eq!(credits(&tight_owner), baseline);
    assert_peak_never_regresses(&tight_owner, tight_peak);

    let short_owner = MemoryOwner::new(budget_with(exact - 1));
    let short = window(&spec, &schema, &short_owner);
    let short_baseline = credits(&short_owner);
    let short_peak = short_owner.usage().peak_physical_bytes;
    let err = short
        .working_credit(Some(&batch))
        .expect_err("one byte short must fail closed");
    assert_eq!(err.code, ErrorCode::ResourceExhausted);
    assert_eq!(
        credits(&short_owner),
        short_baseline,
        "a refused reservation must not charge reservation or live handles"
    );
    assert_peak_never_regresses(&short_owner, short_peak);
    assert_eq!(short.key_count(), 0, "state must be unchanged");
    assert_eq!(short.retention_bytes(), 0, "no retention may be taken");
    assert_eq!(short_owner.usage().physical_bytes, 0);
}

/// Parent (process) and child (job) caps both refuse without leaking; a lease
/// that succeeds returns to *both* ledgers when dropped.
#[test]
fn omp_trial_parent_and_child_refusal_leak_nothing() {
    let schema = narrow_schema(3);
    let spec = WindowSpec::new(
        WindowKind::count(8).unwrap(),
        vec!["c0".into()],
        narrow_aggs(3),
    );
    let batch = narrow_batch(&roomy_owner(), &schema, 3);

    // (a) Child quota refuses.
    let parent = roomy_owner();
    let child = MemoryOwner::child(parent.clone(), budget_with(512), "trial-child");
    let window_a = window(&spec, &schema, &child);
    let parent_before = credits(&parent);
    let child_before = credits(&child);
    let child_peak = child.usage().peak_physical_bytes;
    let err = window_a
        .working_credit(Some(&batch))
        .expect_err("child quota must refuse");
    assert_eq!(err.code, ErrorCode::ResourceExhausted);
    assert_eq!(
        credits(&child),
        child_before,
        "child ledger must be untouched"
    );
    assert_eq!(
        credits(&parent),
        parent_before,
        "parent ledger must be untouched"
    );
    assert_peak_never_regresses(&child, child_peak);

    // (b) Child has room, the shared parent cap refuses.
    let tight_parent = MemoryOwner::new(budget_with(512));
    let roomy_child = MemoryOwner::child(
        tight_parent.clone(),
        budget_with(4 * 1024 * 1024),
        "trial-child",
    );
    let window_b = window(&spec, &schema, &roomy_child);
    let parent_before = credits(&tight_parent);
    let child_before = credits(&roomy_child);
    let child_peak = roomy_child.usage().peak_physical_bytes;
    let err = window_b
        .working_credit(Some(&batch))
        .expect_err("parent quota must refuse");
    assert_eq!(err.code, ErrorCode::ResourceExhausted);
    assert_eq!(
        credits(&roomy_child),
        child_before,
        "child must not be charged when the parent refuses"
    );
    assert_eq!(credits(&tight_parent), parent_before);
    assert_peak_never_regresses(&roomy_child, child_peak);

    // (c) Both have room: success charges both, drop refunds both.
    let parent = roomy_owner();
    let child = MemoryOwner::child(parent.clone(), budget_with(4 * 1024 * 1024), "trial-child");
    let mut window_c = window(&spec, &schema, &child);
    let parent_before = credits(&parent);
    let child_before = credits(&child);
    let child_peak_before = child.usage().peak_physical_bytes;
    let parent_peak_before = parent.usage().peak_physical_bytes;
    let lease = window_c
        .working_credit(Some(&batch))
        .expect("both ledgers have room");
    assert!(child.usage().reservation_bytes > child_before.reservation_bytes);
    assert!(parent.usage().reservation_bytes > parent_before.reservation_bytes);
    let child_peak = assert_peak_never_regresses(&child, child_peak_before);
    let parent_peak = assert_peak_never_regresses(&parent, parent_peak_before);
    drop(lease);
    assert_eq!(credits(&child), child_before);
    assert_eq!(credits(&parent), parent_before);
    assert_peak_never_regresses(&child, child_peak);
    assert_peak_never_regresses(&parent, parent_peak);
    window_c.cleanup();
    drop(window_c);
    assert_eq!(parent.usage().physical_bytes, 0);
}

/// MIN/MAX keep the retained candidate alive; the call that is about to emit
/// (and drop it) must reserve for those retained bytes first (COUNT and PT).
#[test]
fn omp_trial_minmax_retained_value_charged_before_emit() {
    let schema = Schema::new(
        SchemaId::new(9),
        vec![
            Field::new(FieldId::new(1), "k", DataType::Int64, false),
            Field::new(FieldId::new(2), "v", DataType::Utf8, false),
        ],
    )
    .expect("min/max fixture schema");
    let batches = roomy_owner();
    let wide = "a".repeat(4096);

    let min_agg = vec![AggCall::new(
        AggFn::Min,
        Some(Expr::Column { name: "v".into() }),
        "m",
    )];

    // COUNT window: the second arrival completes the window and emits.
    let count_spec = WindowSpec::new(
        WindowKind::count(2).unwrap(),
        vec!["k".into()],
        min_agg.clone(),
    );
    let owner = roomy_owner();
    let mut count_window = window(&count_spec, &schema, &owner);
    let first = build_batch(
        &batches,
        &schema,
        vec![Row {
            values: vec![Scalar::Int64(7), Scalar::utf8(&wide)],
        }],
    );
    {
        let _scratch = count_window.working_credit(Some(&first)).unwrap();
        count_window.on_batch(&first, 0).unwrap();
    }
    assert_eq!(count_window.key_count(), 1);
    let retained: usize = count_window
        .freeze()
        .entries
        .iter()
        .map(|e| e.accs.iter().map(Accumulator::tracked_bytes).sum::<usize>())
        .sum();
    assert!(
        retained >= wide.len(),
        "fixture must retain the wide candidate (got {retained})"
    );

    let second = build_batch(
        &batches,
        &schema,
        vec![Row {
            values: vec![Scalar::Int64(7), Scalar::utf8("b")],
        }],
    );
    let output = count_window.output_schema().clone();
    let without_touched = legacy_reservation_bytes(&count_spec, &schema, &output, &second);
    let lease = count_window.working_credit(Some(&second)).unwrap();
    assert_eq!(
        lease.bytes(),
        without_touched.saturating_add(retained.saturating_mul(2)),
        "the emitting call must pay for the retained variable accumulator it is about to drop"
    );
    assert!(lease.bytes() > without_touched);
    let emission = count_window.on_batch(&second, 0).unwrap();
    assert_eq!(emission.finals.len(), 1);
    assert_eq!(
        emission.finals[0].values,
        vec![
            Scalar::Int64(7),
            Scalar::Int64(0),
            Scalar::Int64(2),
            Scalar::utf8(&wide)
        ]
    );
    assert_eq!(
        count_window.retention_bytes(),
        0,
        "emitted candidate released"
    );
    drop(lease);
    count_window.cleanup();
    drop(count_window);
    assert_eq!(owner.usage().physical_bytes, 0);

    // Processing-time window: the due timer closes the same retained candidate.
    let pt_spec = WindowSpec::new(
        WindowKind::tumbling_pt(1_000).unwrap(),
        vec!["k".into()],
        min_agg,
    );
    let owner = roomy_owner();
    let mut pt_window = window(&pt_spec, &schema, &owner);
    {
        let _scratch = pt_window.working_credit(Some(&first)).unwrap();
        pt_window.on_batch(&first, 0).unwrap();
    }
    let retained: usize = pt_window
        .freeze()
        .entries
        .iter()
        .map(|e| e.accs.iter().map(Accumulator::tracked_bytes).sum::<usize>())
        .sum();
    assert!(retained >= wide.len());
    let output = pt_window.output_schema().clone();
    let without_touched = legacy_reservation_bytes(&pt_spec, &schema, &output, &second);
    let lease = pt_window.working_credit(Some(&second)).unwrap();
    assert_eq!(
        lease.bytes(),
        without_touched.saturating_add(retained.saturating_mul(2)),
        "a due-time emit must still reserve for the retained candidate"
    );
    let emission = pt_window
        .on_batch(&second, 1_000)
        .expect("processing-time emit");
    assert_eq!(emission.finals.len(), 1);
    assert_eq!(
        emission.finals[0].values,
        vec![
            Scalar::Int64(7),
            Scalar::Int64(0),
            Scalar::Int64(1_000),
            Scalar::utf8(&wide)
        ]
    );
    // The closed window is gone; the second arrival opened the next one.
    let frozen = pt_window.freeze();
    assert_eq!(frozen.entries.len(), 1);
    assert_eq!(frozen.entries[0].window_start, 1_000);
    drop(lease);
    pt_window.cleanup();
    drop(pt_window);
    assert_eq!(owner.usage().physical_bytes, 0);
}

/// Emitted rows and cleanup are unchanged: exact COUNT and PT output, and a
/// fully refunded owner afterwards (only the historical peak stays set).
#[test]
fn omp_trial_emitted_rows_and_cleanup_unchanged() {
    let schema = Schema::new(
        SchemaId::new(3),
        vec![
            Field::new(FieldId::new(1), "k", DataType::Int64, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .expect("output fixture schema");
    let batches = roomy_owner();

    // COUNT(3) with SUM/MIN/MAX/COUNT forces the variable-accumulator path.
    let count_spec = WindowSpec::new(
        WindowKind::count(3).unwrap(),
        vec!["k".into()],
        vec![
            AggCall::new(AggFn::Sum, Some(Expr::Column { name: "v".into() }), "s"),
            AggCall::new(AggFn::Min, Some(Expr::Column { name: "v".into() }), "mn"),
            AggCall::new(AggFn::Max, Some(Expr::Column { name: "v".into() }), "mx"),
            AggCall::new(AggFn::Count, Some(Expr::Column { name: "v".into() }), "n"),
        ],
    );
    let owner = roomy_owner();
    let mut count_window = window(&count_spec, &schema, &owner);
    let mut finals = Vec::new();
    for v in [5i64, 3, 9] {
        let batch = build_batch(
            &batches,
            &schema,
            vec![Row {
                values: vec![Scalar::Int64(7), Scalar::Int64(v)],
            }],
        );
        let scratch = count_window.working_credit(Some(&batch)).unwrap();
        finals.extend(count_window.on_batch(&batch, 0).unwrap().finals);
        drop(scratch);
    }
    assert_eq!(finals.len(), 1);
    assert_eq!(
        finals[0].values,
        vec![
            Scalar::Int64(7),
            Scalar::Int64(0),
            Scalar::Int64(3),
            Scalar::Int64(17),
            Scalar::Int64(3),
            Scalar::Int64(9),
            Scalar::Int64(3),
        ]
    );
    assert_eq!(count_window.key_count(), 0);

    // PT tumbling: the boundary arrival fires the timer and flushes the window.
    let pt_spec = WindowSpec::new(
        WindowKind::tumbling_pt(1_000).unwrap(),
        vec!["k".into()],
        vec![AggCall::new(
            AggFn::Sum,
            Some(Expr::Column { name: "v".into() }),
            "s",
        )],
    );
    let mut pt_window = window(&pt_spec, &schema, &owner);
    let open = build_batch(
        &batches,
        &schema,
        vec![
            Row {
                values: vec![Scalar::Int64(1), Scalar::Int64(10)],
            },
            Row {
                values: vec![Scalar::Int64(1), Scalar::Int64(20)],
            },
        ],
    );
    {
        let _scratch = pt_window.working_credit(Some(&open)).unwrap();
        assert!(pt_window.on_batch(&open, 0).unwrap().finals.is_empty());
    }
    let boundary = build_batch(
        &batches,
        &schema,
        vec![Row {
            values: vec![Scalar::Int64(1), Scalar::Int64(5)],
        }],
    );
    let lease = pt_window.working_credit(Some(&boundary)).unwrap();
    let emission = pt_window.on_batch(&boundary, 1_000).unwrap();
    assert_eq!(emission.finals.len(), 1);
    assert_eq!(
        emission.finals[0].values,
        vec![
            Scalar::Int64(1),
            Scalar::Int64(0),
            Scalar::Int64(1_000),
            Scalar::Int64(30),
        ]
    );
    drop(lease);

    count_window.cleanup();
    pt_window.cleanup();
    for w in [&count_window, &pt_window] {
        assert_eq!(w.key_count(), 0, "cleanup must drop every key");
        assert_eq!(w.retention_bytes(), 0, "cleanup must refund retention");
    }
    drop(count_window);
    drop(pt_window);
    let usage = owner.usage();
    assert_eq!(usage.reservation_bytes, 0);
    assert_eq!(usage.retention_bytes, 0);
    assert_eq!(usage.physical_bytes, 0);
    assert_eq!(usage.live_handles, 0);
    assert!(
        usage.peak_physical_bytes > 0,
        "the historical peak is not expected to reset"
    );
}

/// The narrow path must take one reservation for the final amount: no
/// transient double charge, one extra handle, exact refund on drop.
#[test]
fn omp_trial_narrow_lease_taken_once_without_double_charge() {
    let schema = narrow_schema(3);
    let spec = WindowSpec::new(
        WindowKind::count(8).unwrap(),
        vec!["c0".into()],
        narrow_aggs(3),
    );
    let owner = roomy_owner();
    let window = window(&spec, &schema, &owner);
    assert_eq!(
        owner.usage().physical_bytes,
        0,
        "a fixed-size accumulator window reserves nothing at construction"
    );

    let batch = narrow_batch(&roomy_owner(), &schema, 3);
    let before = credits(&owner);
    let lease = window.working_credit(Some(&batch)).unwrap();
    let after = owner.usage();
    assert_eq!(
        after.reservation_bytes,
        before.reservation_bytes + lease.bytes()
    );
    assert_eq!(after.physical_bytes, before.physical_bytes + lease.bytes());
    assert_eq!(
        after.peak_physical_bytes, after.physical_bytes,
        "the call must not hold the base term and the final term at once"
    );
    assert_eq!(after.live_handles, before.live_handles + 1);
    let peak_held = after.peak_physical_bytes;
    drop(lease);
    assert_eq!(credits(&owner), before, "drop must refund exactly once");
    assert_peak_never_regresses(&owner, peak_held);
}

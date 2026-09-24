//! Contract tests for the identity-projection fast path in `CompiledTransform`.
//!
//! Included from `transform.rs` as `mod identity_projection_tests`. `apply` may
//! alias the input batch only when a non-empty chain is entirely positional
//! Project/Map steps of equal width; work is still consumed row by row and step
//! by step, and every other shape (reorder, truncation, filter, computed or
//! narrowing output) keeps the generic path unchanged. `identity_width` is read
//! (and forced to `None`) to build a generic oracle of the same configuration.

use super::*;

use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::observation::OriginSpan;
use sparrow_model::{
    CreditKind, CreditUsage, DataType, Field, FieldId, MemoryOwner, OperatorId, OutputSequence,
    ResourceBudget, SchemaId,
};
use std::time::Instant;

fn owner_with(reservation_bytes: usize, max_rows: usize) -> Arc<MemoryOwner> {
    MemoryOwner::new(ResourceBudget {
        reservation_bytes,
        max_rows,
        ..ResourceBudget::compact()
    })
}

/// Current credits only: `peak_physical_bytes` is monotone and never refunds.
fn credits(owner: &Arc<MemoryOwner>) -> CreditUsage {
    let mut usage = owner.usage();
    usage.peak_physical_bytes = 0;
    usage
}

fn f(id: u16, name: &str, ty: DataType, nullable: bool) -> Field {
    Field::new(FieldId::new(id), name, ty, nullable)
}

fn schema(id: u32, fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(SchemaId::new(id), fields).expect("fixture schema"))
}

fn c(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}

fn row_of(values: Vec<Scalar>) -> Row {
    Row { values }
}

fn ints(values: &[i64]) -> Row {
    row_of(values.iter().map(|v| Scalar::Int64(*v)).collect())
}

/// Column 0 Int64, column 1 nullable Utf8, remaining columns Utf8.
fn schema_n(tag: &str, width: usize) -> Arc<Schema> {
    let fields = (0..width)
        .map(|i| {
            let name = format!("{tag}{i}");
            if i == 0 {
                f(i as u16 + 1, &name, DataType::Int64, false)
            } else {
                f(i as u16 + 1, &name, DataType::Utf8, i == 1)
            }
        })
        .collect();
    schema(30 + tag.len() as u32, fields)
}

fn int_schema(tag: &str, width: usize) -> Arc<Schema> {
    let fields = (0..width)
        .map(|i| f(i as u16 + 1, &format!("{tag}{i}"), DataType::Int64, false))
        .collect();
    schema(40 + tag.len() as u32, fields)
}

fn columns(schema: &Arc<Schema>) -> Vec<Expr> {
    schema.fields.iter().map(|f| c(&f.name)).collect()
}

fn project(input: &Arc<Schema>, output: &Arc<Schema>, exprs: Vec<Expr>) -> TransformStep {
    TransformStep::Project {
        operator: OperatorId::new(1),
        input: (**input).clone(),
        output: (**output).clone(),
        exprs,
    }
}

fn map(input: &Arc<Schema>, output: &Arc<Schema>, exprs: Vec<Expr>) -> TransformStep {
    TransformStep::Map {
        operator: OperatorId::new(2),
        input: (**input).clone(),
        output: (**output).clone(),
        exprs,
    }
}

fn batch(schema: &Arc<Schema>, owner: &Arc<MemoryOwner>, rows: Vec<Row>) -> RowBatch {
    let max_rows = rows.len().max(1);
    let mut builder = RowBatchBuilder::new(
        Arc::clone(schema),
        Arc::clone(owner),
        CreditKind::Reservation,
        max_rows,
        1 << 20,
    )
    .expect("fixture builder");
    for row in rows {
        builder.push(row).expect("fixture row");
    }
    builder.finish().expect("fixture batch")
}

fn row(salt: i64, width: usize) -> Row {
    row_of(
        (0..width)
            .map(|i| {
                if i == 0 {
                    Scalar::Int64(salt)
                } else {
                    Scalar::utf8(format!("{salt}-{i}"))
                }
            })
            .collect(),
    )
}

/// One NULL in the nullable column, to prove sharing does not filter or copy.
fn null_row(width: usize) -> Row {
    let mut values = row(9, width).values;
    values[1] = Scalar::Null;
    row_of(values)
}

/// A fallback shape must not be treated as identity and must not alias rows.
fn fallback(
    name: &str,
    steps: &[TransformStep],
    input: &RowBatch,
    owner: &Arc<MemoryOwner>,
    expected: Vec<Scalar>,
    expect_rows: usize,
) {
    let compiled = CompiledTransform::new(steps).expect("compile");
    assert!(
        compiled.identity_width.is_none(),
        "{name} must not be identity"
    );
    let out = compiled
        .apply(input, owner, &WorkBudget::new(1_000))
        .expect("apply")
        .expect("output");
    assert_eq!(out.num_rows(), expect_rows, "{name}");
    assert_eq!(out.rows()[0].values, expected, "{name}");
    assert_ne!(
        out.lease().alloc_id(),
        input.lease().alloc_id(),
        "{name} must copy"
    );
}

#[test]
fn omp_trial_identity_projection_fast_path_matches_generic_values_and_work() {
    let owner = owner_with(1 << 20, 64);
    let (s0, s1, s2) = (schema_n("a", 3), schema_n("b", 3), schema_n("c", 3));
    let steps = vec![
        project(&s0, &s1, columns(&s0)),
        map(&s1, &s2, columns(&s1)),
        project(&s2, &s2, columns(&s2)),
    ];
    let compiled = CompiledTransform::new(&steps).expect("compile");
    assert_eq!(compiled.identity_width, Some(3));
    let compiled_output = compiled.output.as_ref().expect("compiled output schema");
    let mut generic = CompiledTransform::new(&steps).expect("compile");
    generic.identity_width = None;
    let input = batch(&s0, &owner, vec![row(1, 3), null_row(3), row(2, 3)])
        .with_origin(OriginSpan::at(Instant::now()))
        .with_output_sequence(OutputSequence::new([3u8; 16], 1).expect("sequence"))
        .expect("output sequence envelope")
        .with_source_operator(Some(OperatorId::new(4)));
    let rows = input.num_rows();
    let fast_budget = WorkBudget::new(1_000);
    let fast = compiled
        .apply(&input, &owner, &fast_budget)
        .expect("apply")
        .expect("shared alias");
    let generic_budget = WorkBudget::new(1_000);
    let copied = generic
        .apply(&input, &owner, &generic_budget)
        .expect("apply")
        .expect("generic output");

    assert_eq!(fast.rows(), input.rows());
    assert_eq!(fast.rows(), copied.rows());
    assert_eq!(fast.schema(), s2.as_ref());
    assert_eq!(fast.schema(), copied.schema());
    assert!(
        Arc::ptr_eq(&fast.schema_arc(), compiled_output),
        "the alias carries the compiled step's own output schema Arc"
    );
    assert_eq!(
        (fast.rows().as_ptr(), fast.lease().alloc_id()),
        (input.rows().as_ptr(), input.lease().alloc_id()),
        "the alias must share the input rows and lease"
    );
    assert_ne!(copied.rows().as_ptr(), input.rows().as_ptr());
    assert_ne!(copied.lease().alloc_id(), input.lease().alloc_id());
    assert_eq!(fast.origin().first, input.origin().first);
    assert!(fast.output_sequence().is_none() && fast.source_operator().is_none());

    // Work is consumed per row and per step by both paths, in the same amount.
    assert_eq!(fast_budget.remaining(), generic_budget.remaining());
    assert!(compiled.work_units(rows) > 0);
    assert_eq!(1_000 - fast_budget.remaining(), compiled.work_units(rows));
}

#[test]
fn omp_trial_identity_projection_needs_no_headroom_and_never_crosses_owners() {
    let owner = owner_with(1 << 20, 64);
    let s = schema_n("a", 2);
    let out_s = schema_n("b", 2);
    let steps = vec![
        project(&s, &out_s, columns(&s)),
        map(&out_s, &out_s, columns(&out_s)),
    ];
    let compiled = CompiledTransform::new(&steps).expect("compile");
    let mut generic = CompiledTransform::new(&steps).expect("compile");
    generic.identity_width = None;
    let input = batch(&s, &owner, vec![row(1, 2), null_row(2)]);

    // Burn every remaining reservation credit: the alias must need none.
    let free = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    assert!(free > 0);
    let burned = owner.acquire(CreditKind::Reservation, free).expect("burn");
    let base = credits(&owner);
    let fast = compiled
        .apply(&input, &owner, &WorkBudget::new(1_000))
        .expect("apply on a full ledger")
        .expect("shared alias");
    assert_eq!(fast.rows().as_ptr(), input.rows().as_ptr());
    // The alias is alive: one extra handle, but no new reservation or bytes.
    let held = credits(&owner);
    assert_eq!(
        held.reservation_bytes, base.reservation_bytes,
        "no new credit"
    );
    assert_eq!(
        held.physical_bytes, base.physical_bytes,
        "no new physical bytes"
    );
    assert_eq!(
        held.live_handles,
        base.live_handles + 1,
        "one shared handle"
    );
    assert_eq!(
        generic
            .apply(&input, &owner, &WorkBudget::new(1_000))
            .unwrap_err()
            .code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(
        credits(&owner),
        held,
        "a refused copy must refund its scratch"
    );
    drop(fast);
    assert_eq!(
        credits(&owner),
        base,
        "dropping the alias refunds its handle"
    );
    drop(burned);
    let baseline = credits(&owner);

    // A different owner must not be handed the input's lease: the output is a
    // real copy billed to the caller, and the input keeps its single handle.
    let other = owner_with(1 << 20, 64);
    let copied = compiled
        .apply(&input, &other, &WorkBudget::new(1_000))
        .expect("apply")
        .expect("output billed to the caller's owner");
    assert!(
        Arc::ptr_eq(copied.lease().owner(), &other),
        "the copy is billed to the caller's owner"
    );
    assert!(
        Arc::ptr_eq(input.lease().owner(), &owner),
        "the input lease stays with its owner"
    );
    assert_ne!(
        copied.rows().as_ptr(),
        input.rows().as_ptr(),
        "no aliasing across owners"
    );
    assert_eq!(
        input.lease().refcount(),
        1,
        "the input allocation is not shared"
    );
    assert_eq!(copied.rows(), input.rows());
    assert_eq!(
        credits(&owner),
        baseline,
        "the input ledger stays untouched"
    );
    drop(copied);
    assert_eq!(credits(&other).reservation_bytes, 0, "the copy is refunded");
    assert_eq!(credits(&other).live_handles, 0);
}

#[test]
fn omp_trial_identity_projection_work_failures_match_generic_and_refund() {
    let owner = owner_with(1 << 20, 64);
    let s = schema_n("a", 3);
    let steps = vec![project(&s, &s, columns(&s)), map(&s, &s, columns(&s))];
    let compiled = CompiledTransform::new(&steps).expect("compile");
    let mut generic = CompiledTransform::new(&steps).expect("compile");
    generic.identity_width = None;
    let input = batch(&s, &owner, vec![row(1, 3), row(2, 3)]);
    let need = compiled.work_units(input.num_rows());
    assert!(need > 2);

    // Exhausted quantum: same code, same leftover credit, handle refunded.
    for quantum in [need - 1, 1] {
        let fast_budget = WorkBudget::new(quantum);
        let generic_budget = WorkBudget::new(quantum);
        let base = credits(&owner);
        let fast_err = compiled
            .apply(&input, &owner, &fast_budget)
            .expect_err("quantum exhaustion");
        let generic_err = generic
            .apply(&input, &owner, &generic_budget)
            .expect_err("quantum exhaustion");
        assert_eq!(fast_err.code, ErrorCode::ResourceExhausted);
        assert_eq!(
            (fast_err.code, fast_err.retryable),
            (generic_err.code, generic_err.retryable)
        );
        assert_eq!(fast_budget.remaining(), generic_budget.remaining());
        assert_eq!(
            credits(&owner),
            base,
            "a failed step must refund its scratch"
        );
    }

    // Preview lifetime cap: non-retryable, identical for both paths.
    let cap = need - 1;
    let fast_budget = WorkBudget::new(need).with_lifetime_cap(cap);
    let generic_budget = WorkBudget::new(need).with_lifetime_cap(cap);
    let fast_err = compiled
        .apply(&input, &owner, &fast_budget)
        .expect_err("lifetime exhaustion");
    let generic_err = generic
        .apply(&input, &owner, &generic_budget)
        .expect_err("lifetime exhaustion");
    assert!(!fast_err.retryable);
    assert_eq!(
        (fast_err.code, fast_err.retryable),
        (generic_err.code, generic_err.retryable)
    );
    assert_eq!(fast_budget.remaining(), generic_budget.remaining());
}

#[test]
fn omp_trial_identity_projection_falls_back_for_reorder_truncate_filter_and_math() {
    let owner = owner_with(1 << 20, 64);

    // Reorder: same width and types, different order.
    let two = int_schema("i", 2);
    let two_out = int_schema("o", 2);
    let reordered = batch(&two, &owner, vec![ints(&[1, 2])]);
    fallback(
        "reorder",
        &[project(&two, &two_out, vec![c("i1"), c("i0")])],
        &reordered,
        &owner,
        vec![Scalar::Int64(2), Scalar::Int64(1)],
        1,
    );

    // Truncation: identity prefix of a wider input.
    let three = schema_n("t", 3);
    let three_out = schema_n("u", 2);
    let wide = batch(&three, &owner, vec![row(7, 3)]);
    fallback(
        "truncate",
        &[project(&three, &three_out, vec![c("t0"), c("t1")])],
        &wide,
        &owner,
        vec![Scalar::Int64(7), Scalar::utf8("7-1")],
        1,
    );

    // Filter: a predicate step is never an identity projection.
    let keep = schema(
        60,
        vec![
            f(1, "n", DataType::Int64, false),
            f(2, "keep", DataType::Bool, false),
        ],
    );
    let filtered = batch(
        &keep,
        &owner,
        vec![
            row_of(vec![Scalar::Int64(1), Scalar::Bool(true)]),
            row_of(vec![Scalar::Int64(2), Scalar::Bool(false)]),
        ],
    );
    fallback(
        "filter",
        &[TransformStep::Filter {
            operator: OperatorId::new(3),
            predicate: c("keep"),
            input: (*keep).clone(),
        }],
        &filtered,
        &owner,
        vec![Scalar::Int64(1), Scalar::Bool(true)],
        1,
    );

    // Computed projection: a positional width match is still not identity.
    let one = int_schema("n", 1);
    let one_out = int_schema("m", 1);
    let single = batch(&one, &owner, vec![ints(&[2])]);
    let add = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(c("n0")),
        right: Box::new(Expr::Literal(Scalar::Int64(1))),
    };
    fallback(
        "arithmetic",
        &[project(&one, &one_out, vec![add])],
        &single,
        &owner,
        vec![Scalar::Int64(3)],
        1,
    );
}

#[test]
fn omp_trial_identity_projection_keeps_width_change_and_tightening_errors() {
    let owner = owner_with(1 << 20, 64);

    // Step 1 truncates to one column, step 2 assumes two: the runtime error
    // must survive instead of being skipped as an identity chain.
    let two = int_schema("i", 2);
    let one = int_schema("m", 1);
    let two_out = int_schema("o", 2);
    let steps = vec![
        project(&two, &one, vec![c("i0")]),
        project(&two, &two_out, vec![c("i0"), c("i1")]),
    ];
    let compiled = CompiledTransform::new(&steps).expect("compile");
    assert!(
        compiled.identity_width.is_none(),
        "unequal step widths are never identity"
    );
    let mut generic = CompiledTransform::new(&steps).expect("compile");
    generic.identity_width = None;
    let input = batch(&two, &owner, vec![ints(&[1, 2])]);
    let base = credits(&owner);
    let err = compiled
        .apply(&input, &owner, &WorkBudget::new(1_000))
        .expect_err("a truncated intermediate cannot feed a two-column step");
    assert_eq!(
        err.code,
        generic
            .apply(&input, &owner, &WorkBudget::new(1_000))
            .expect_err("same error on the generic path")
            .code
    );
    assert_eq!(
        credits(&owner),
        base,
        "the failed step must refund its scratch"
    );

    // Nullable tightening: the alias must not swallow a NULL, and must not
    // alias the rows even when the data happens to contain no NULL.
    let nullable = schema(70, vec![f(1, "y0", DataType::Utf8, true)]);
    let tightened = schema(71, vec![f(1, "y0", DataType::Utf8, false)]);
    let narrowing = vec![project(&nullable, &tightened, vec![c("y0")])];
    let compiled = CompiledTransform::new(&narrowing).expect("compile");
    let with_null = batch(&nullable, &owner, vec![row_of(vec![Scalar::Null])]);
    let base = credits(&owner);
    assert_eq!(
        compiled
            .apply(&with_null, &owner, &WorkBudget::new(1_000))
            .unwrap_err()
            .code,
        ErrorCode::TypeMismatch,
        "a stricter output must reject the NULL, not alias it away"
    );
    assert_eq!(credits(&owner), base);
    let clean = batch(&nullable, &owner, vec![row_of(vec![Scalar::utf8("v")])]);
    let out = compiled
        .apply(&clean, &owner, &WorkBudget::new(1_000))
        .expect("apply")
        .expect("output");
    assert_eq!(out.rows()[0].values, vec![Scalar::utf8("v")]);
    assert_ne!(out.lease().alloc_id(), clean.lease().alloc_id());
}

#[test]
fn omp_trial_identity_projection_empty_batch_and_row_cap_semantics() {
    let owner = owner_with(1 << 20, 64);
    let s = schema_n("a", 2);
    let steps = vec![project(&s, &s, columns(&s))];
    let compiled = CompiledTransform::new(&steps).expect("compile");
    let mut generic = CompiledTransform::new(&steps).expect("compile");
    generic.identity_width = None;

    // An empty batch keeps the "no output" contract on both paths.
    let empty = batch(&s, &owner, Vec::new());
    let base = credits(&owner);
    assert!(compiled
        .apply(&empty, &owner, &WorkBudget::new(1_000))
        .expect("apply")
        .is_none());
    assert!(generic
        .apply(&empty, &owner, &WorkBudget::new(1_000))
        .expect("apply")
        .is_none());
    assert_eq!(
        credits(&owner),
        base,
        "an empty batch must not leak scratch"
    );

    // A batch above the owner's max_rows budget is refused, never aliased.
    let tight = owner_with(1 << 20, 1);
    let oversize = batch(&s, &tight, vec![row(1, 2), row(2, 2)]);
    assert_eq!(oversize.num_rows(), 2);
    assert_eq!(tight.budget().max_rows, 1);
    let base = credits(&tight);
    assert_eq!(
        compiled
            .apply(&oversize, &tight, &WorkBudget::new(1_000))
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
    assert_eq!(
        credits(&tight),
        base,
        "a refused row cap must refund its scratch"
    );
}

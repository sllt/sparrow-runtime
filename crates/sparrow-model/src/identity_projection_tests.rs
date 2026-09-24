//! Contract tests for `RowBatch::try_identity_projection`.
//!
//! Included from `batch.rs` as `mod identity_projection_tests`. The projection
//! may only alias an existing batch: same owner Arc, Reservation credit, row
//! count within `budget.max_rows`, equal field count and identical per-field
//! `DataType`, and an output `nullable` that is not stricter than the input's.
//! Anything else returns `None` without charging credit or adding a handle.

use super::*;

use crate::observation::OriginSpan;
use crate::{
    CreditUsage, DataType, DynamicValue, Field, FieldId, OperatorId, OutputSequence,
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

/// Int64, nullable Utf8, Utf8, typed nested Array.
fn input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "n", DataType::Int64, false),
            Field::new(FieldId::new(2), "s", DataType::Utf8, true),
            Field::new(FieldId::new(3), "t", DataType::Utf8, false),
            Field::new(
                FieldId::new(4),
                "arr",
                DataType::Array(Box::new(DataType::Int64)),
                false,
            ),
        ],
    )
    .expect("input schema")
}

/// Same types and nullability (with one legal widening), different names/ids:
/// renaming and relaxing NULL-ability are still identity projections.
fn alias_schema() -> Schema {
    Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(11), "count", DataType::Int64, false),
            Field::new(FieldId::new(12), "label", DataType::Utf8, true),
            Field::new(FieldId::new(13), "note", DataType::Utf8, true),
            Field::new(
                FieldId::new(14),
                "items",
                DataType::Array(Box::new(DataType::Int64)),
                false,
            ),
        ],
    )
    .expect("alias schema")
}

fn schema_with(fields: Vec<Field>) -> Schema {
    Schema::new(SchemaId::new(9), fields).expect("variant schema")
}

fn typed_array(items: &[i64]) -> Scalar {
    Scalar::Dynamic(DynamicValue::Array(
        items
            .iter()
            .map(|v| DynamicValue::Int64(*v))
            .collect::<Vec<_>>()
            .into(),
    ))
}

fn input_rows() -> Vec<Row> {
    vec![
        Row {
            values: vec![
                Scalar::Int64(1),
                Scalar::Null,
                Scalar::utf8("t1"),
                typed_array(&[1, 2]),
            ],
        },
        Row {
            values: vec![
                Scalar::Int64(2),
                Scalar::utf8("s2"),
                Scalar::utf8("t2"),
                typed_array(&[]),
            ],
        },
    ]
}

fn batch_of(
    schema: &Schema,
    owner: &Arc<MemoryOwner>,
    kind: CreditKind,
    max_rows: usize,
    rows: Vec<Row>,
) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema.clone()),
        Arc::clone(owner),
        kind,
        max_rows,
        1 << 20,
    )
    .expect("fixture builder");
    for row in rows {
        builder.push(row).expect("fixture row");
    }
    builder.finish().expect("fixture batch")
}

#[test]
fn omp_trial_identity_projection_aliases_rows_schema_and_lease() {
    let owner = owner_with(1 << 20, 64);
    let batch = batch_of(
        &input_schema(),
        &owner,
        CreditKind::Reservation,
        8,
        input_rows(),
    );
    let output = Arc::new(alias_schema());
    let before = credits(&owner);
    let handles = batch.lease().refcount();

    let out = batch
        .try_identity_projection(Arc::clone(&output), &owner)
        .expect("compatible alias must project");

    assert!(Arc::ptr_eq(&out.schema_arc(), &output), "alias schema Arc");
    assert_eq!(out.rows().as_ptr(), batch.rows().as_ptr(), "shared rows");
    assert_eq!(out.lease().alloc_id(), batch.lease().alloc_id());
    assert_eq!(out.lease().refcount(), handles + 1);
    assert_eq!(out.num_rows(), batch.num_rows());
    assert_eq!(out.rows(), batch.rows(), "NULL and nested values survive");
    assert_eq!(out.rows()[1].values[3], typed_array(&[]));
    let after = credits(&owner);
    assert_eq!(after.physical_bytes, before.physical_bytes, "no new bytes");
    assert_eq!(after.reservation_bytes, before.reservation_bytes);
    assert_eq!(after.live_handles, before.live_handles + 1);

    // Dropping the input leaves the alias valid; the last handle refunds.
    drop(batch);
    assert_eq!(out.rows()[0].values[1], Scalar::Null);
    assert_eq!(out.schema().fields.len(), 4);
    drop(out);
    assert_eq!(credits(&owner), CreditUsage::default());
    assert_eq!(owner.usage().live_handles, 0);
}

#[test]
fn omp_trial_identity_projection_rejects_incompatible_schemas() {
    let owner = owner_with(1 << 20, 64);
    let batch = batch_of(
        &input_schema(),
        &owner,
        CreditKind::Reservation,
        8,
        input_rows(),
    );
    let int64 = || Field::new(FieldId::new(1), "a", DataType::Int64, false);
    let utf8_null = || Field::new(FieldId::new(2), "b", DataType::Utf8, true);
    let utf8_plain = || Field::new(FieldId::new(3), "c", DataType::Utf8, false);
    let array = || {
        Field::new(
            FieldId::new(4),
            "d",
            DataType::Array(Box::new(DataType::Int64)),
            false,
        )
    };
    let variants = [
        // fewer fields
        schema_with(vec![int64(), utf8_null(), utf8_plain()]),
        // same width, different DataType at index 1
        schema_with(vec![
            int64(),
            Field::new(FieldId::new(2), "b", DataType::Int64, false),
            utf8_plain(),
            array(),
        ]),
        // same width, stricter nullability than the nullable input column
        schema_with(vec![
            int64(),
            Field::new(FieldId::new(2), "b", DataType::Utf8, false),
            utf8_plain(),
            array(),
        ]),
        // same width, reordered types
        schema_with(vec![utf8_null(), int64(), utf8_plain(), array()]),
    ];
    for (i, variant) in variants.into_iter().enumerate() {
        let before = credits(&owner);
        assert!(
            batch
                .try_identity_projection(Arc::new(variant), &owner)
                .is_none(),
            "variant {i} must be refused"
        );
        assert_eq!(
            credits(&owner),
            before,
            "variant {i} must not charge a handle"
        );
        assert_eq!(
            batch.lease().refcount(),
            1,
            "variant {i} must not share the input allocation"
        );
    }
}

#[test]
fn omp_trial_identity_projection_guards_owner_credit_and_row_cap() {
    let owner = owner_with(1 << 20, 64);
    let other = owner_with(1 << 20, 64);
    let alias = Arc::new(alias_schema());
    let batch = batch_of(
        &input_schema(),
        &owner,
        CreditKind::Reservation,
        8,
        input_rows(),
    );

    // A foreign owner must not be handed this lease.
    let before = credits(&owner);
    assert!(batch
        .try_identity_projection(Arc::clone(&alias), &other)
        .is_none());
    assert_eq!(credits(&owner), before);
    assert_eq!(credits(&other), CreditUsage::default());

    // Parent and child are distinct owners, even for one job tree.
    let parent = owner_with(1 << 20, 64);
    let child = MemoryOwner::child(
        parent.clone(),
        ResourceBudget {
            reservation_bytes: 1 << 20,
            max_rows: 64,
            ..ResourceBudget::compact()
        },
        "trial-child",
    );
    let child_batch = batch_of(
        &input_schema(),
        &child,
        CreditKind::Reservation,
        8,
        input_rows(),
    );
    assert!(child_batch
        .try_identity_projection(Arc::clone(&alias), &child)
        .is_some());
    let before = credits(&child);
    assert!(child_batch
        .try_identity_projection(Arc::clone(&alias), &parent)
        .is_none());
    assert_eq!(credits(&child), before, "refusal must not add a handle");

    // Queue and Retention leases are not Reservation credit.
    for kind in [CreditKind::Queue, CreditKind::Retention] {
        let leased = batch_of(&input_schema(), &owner, kind, 8, input_rows());
        let before = credits(&owner);
        assert!(leased
            .try_identity_projection(Arc::clone(&alias), &owner)
            .is_none());
        assert_eq!(credits(&owner), before, "{kind:?} lease must not project");
    }

    // More rows than the owner's max_rows budget.
    let tight = owner_with(1 << 20, 1);
    let wide = batch_of(
        &input_schema(),
        &tight,
        CreditKind::Reservation,
        2,
        input_rows(),
    );
    assert_eq!(wide.num_rows(), 2);
    let before = credits(&tight);
    assert!(wide
        .try_identity_projection(Arc::clone(&alias), &tight)
        .is_none());
    assert_eq!(credits(&tight), before);
}

#[test]
fn omp_trial_identity_projection_keeps_origin_and_clears_delivery_metadata() {
    let owner = owner_with(1 << 20, 64);
    let stamp = Instant::now();
    let batch = batch_of(
        &input_schema(),
        &owner,
        CreditKind::Reservation,
        8,
        input_rows(),
    )
    .with_origin(OriginSpan::at(stamp))
    .with_output_sequence(OutputSequence::new([7u8; 16], 1).expect("sequence"))
    .expect("output sequence envelope")
    .with_source_operator(Some(OperatorId::new(5)));
    assert!(batch.output_sequence().is_some() && batch.source_operator().is_some());

    let out = batch
        .try_identity_projection(Arc::new(alias_schema()), &owner)
        .expect("compatible alias");

    let (projected, source) = (out.origin(), batch.origin());
    assert_eq!(
        (projected.first, projected.last, projected.unknown),
        (source.first, source.last, source.unknown)
    );
    assert!(
        out.output_sequence().is_none(),
        "a shared alias is not a final-output envelope"
    );
    assert!(
        out.source_operator().is_none(),
        "a shared alias is not a UnionAll record"
    );
    assert!(
        batch.output_sequence().is_some(),
        "the input envelope is kept"
    );
    assert_eq!(batch.source_operator(), Some(OperatorId::new(5)));
}

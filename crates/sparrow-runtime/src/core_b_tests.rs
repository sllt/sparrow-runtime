//! Core-B static reference-table and Lookup boundary coverage.
//!
//! These tests intentionally stay on the restart-fresh Lookup path.  Aligned
//! Lookup recovery remains rejected until a table dependency is persisted and
//! resolved as part of the checkpoint contract.

use crate::lookup::{LookupOperator, ReferenceTable};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, ResourceBudget, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_plan::LookupSpec;
use std::sync::Arc;

fn table_schema(key_type: DataType, value_type: DataType) -> Schema {
    Schema::new(
        SchemaId::new(90),
        vec![
            Field::new(FieldId::new(1), "id", key_type, false),
            Field::new(FieldId::new(2), "site", value_type, true),
        ],
    )
    .unwrap()
}

fn table(owner: &Arc<MemoryOwner>, schema: Schema, rows: Vec<Row>) -> Arc<ReferenceTable> {
    ReferenceTable::snapshot_owned(
        "sites",
        1,
        schema,
        vec!["id".into()],
        rows,
        16,
        1024 * 1024,
        owner,
    )
    .unwrap()
}

fn site_table(owner: &Arc<MemoryOwner>) -> Arc<ReferenceTable> {
    table(
        owner,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::utf8("west")],
        }],
    )
}

fn input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
        ],
    )
    .unwrap()
}

fn nullable_input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Utf8, true),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
        ],
    )
    .unwrap()
}

fn temporal_input_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(3), "ts", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn lookup_spec(table_keys: Vec<String>, keep: Vec<String>) -> LookupSpec {
    LookupSpec::static_table("sites", vec!["id".into()], table_keys, keep)
}

fn batch(owner: &Arc<MemoryOwner>, schema: Schema, rows: Vec<Row>) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema),
        Arc::clone(owner),
        CreditKind::Reservation,
        rows.len().max(1),
        owner.budget().reservation_bytes,
    )
    .unwrap();
    for row in rows {
        builder.push(row).unwrap();
    }
    builder.finish().unwrap()
}

#[test]
fn core_b_snapshot_rejects_short_row_without_panic() {
    let schema = table_schema(DataType::Utf8, DataType::Utf8);
    let result = std::panic::catch_unwind(|| {
        ReferenceTable::snapshot(
            "sites",
            1,
            schema,
            vec!["id".into()],
            vec![Row {
                values: vec![Scalar::utf8("a")],
            }],
            16,
            1024 * 1024,
        )
    });
    assert!(result.is_ok(), "malformed reference row must not panic");
    assert_eq!(result.unwrap().unwrap_err().code, ErrorCode::InvalidSchema);
}

#[test]
fn core_b_snapshot_rejects_type_null_and_nondeterministic_keys() {
    let wrong_type = ReferenceTable::snapshot(
        "sites",
        1,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec!["id".into()],
        vec![Row {
            values: vec![Scalar::Int64(1), Scalar::utf8("west")],
        }],
        16,
        1024 * 1024,
    )
    .unwrap_err();
    assert_eq!(wrong_type.code, ErrorCode::TypeMismatch);

    let null_key = ReferenceTable::snapshot(
        "sites",
        1,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec!["id".into()],
        vec![Row {
            values: vec![Scalar::Null, Scalar::utf8("west")],
        }],
        16,
        1024 * 1024,
    )
    .unwrap_err();
    assert_eq!(null_key.code, ErrorCode::TypeMismatch);

    let dynamic_key = ReferenceTable::snapshot(
        "sites",
        1,
        table_schema(DataType::Dynamic, DataType::Utf8),
        vec!["id".into()],
        vec![Row {
            values: vec![
                Scalar::Dynamic(sparrow_model::DynamicValue::Int64(1)),
                Scalar::utf8("west"),
            ],
        }],
        16,
        1024 * 1024,
    )
    .unwrap_err();
    assert_eq!(dynamic_key.code, ErrorCode::FeatureUnavailable);

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let non_finite_key = ReferenceTable::snapshot(
            "sites",
            1,
            table_schema(DataType::Float64, DataType::Utf8),
            vec!["id".into()],
            vec![Row {
                values: vec![Scalar::Float64(value), Scalar::utf8("west")],
            }],
            16,
            1024 * 1024,
        )
        .unwrap_err();
        assert_eq!(non_finite_key.code, ErrorCode::FeatureUnavailable);
    }
}

#[test]
fn core_b_snapshot_rejects_duplicate_key_and_refunds_owner() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let error = ReferenceTable::snapshot_owned(
        "sites",
        1,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec!["id".into()],
        vec![
            Row {
                values: vec![Scalar::utf8("a"), Scalar::utf8("west")],
            },
            Row {
                values: vec![Scalar::utf8("a"), Scalar::utf8("east")],
            },
        ],
        16,
        1024 * 1024,
        &owner,
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert_eq!(owner.usage().retention_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn core_b_snapshot_owned_holds_and_refunds_resident_budget() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let table = site_table(&owner);
    assert!(owner.usage().retention_bytes > 0);
    assert!(table.verify().is_ok());
    drop(table);
    assert_eq!(owner.usage().retention_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);

    let error = ReferenceTable::snapshot_owned(
        "sites",
        1,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec!["id".into()],
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::utf8("west")],
        }],
        16,
        64,
        &owner,
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::BoundExceeded);
    assert_eq!(owner.usage().retention_bytes, 0);
}

#[test]
fn core_b_snapshot_crc_covers_schema_mutation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut table = site_table(&owner);
    Arc::get_mut(&mut table).unwrap().schema.fields[1].nullable = false;
    let error = table.verify().unwrap_err();
    assert_eq!(error.code, ErrorCode::CodecViolation);
    drop(table);
    assert_eq!(owner.usage().retention_bytes, 0);
}

#[test]
fn core_b_lookup_requires_matching_table_key_contract() {
    let table_owner = MemoryOwner::new(ResourceBudget::compact());
    let table = site_table(&table_owner);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let error = match LookupOperator::new(
        lookup_spec(vec!["other".into()], vec!["site".into()]),
        table,
        input_schema(),
        owner,
    ) {
        Ok(_) => panic!("lookup with a different table key must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::InvalidArgument);
}

#[test]
fn core_b_lookup_rejects_foreign_or_wrong_schema_batch() {
    let table_owner = MemoryOwner::new(ResourceBudget::compact());
    let table = site_table(&table_owner);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let op = LookupOperator::new(
        lookup_spec(vec!["id".into()], vec!["site".into()]),
        table,
        input_schema(),
        owner.clone(),
    )
    .unwrap();

    let foreign_owner = MemoryOwner::new(ResourceBudget::compact());
    let foreign = batch(
        &foreign_owner,
        input_schema(),
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(1.0)],
        }],
    );
    let error = op.on_batch_into(&foreign).unwrap_err();
    assert_eq!(error.code, ErrorCode::PolicyDenied);
    drop(foreign);
    assert_eq!(foreign_owner.usage().physical_bytes, 0);

    let wrong_schema = Schema::new(
        SchemaId::new(2),
        vec![Field::new(FieldId::new(1), "id", DataType::Utf8, false)],
    )
    .unwrap();
    let malformed = batch(
        &owner,
        wrong_schema,
        vec![Row {
            values: vec![Scalar::utf8("a")],
        }],
    );
    let error = op.on_batch_into(&malformed).unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidSchema);
    drop(malformed);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn core_b_lookup_direct_builder_preserves_hit_and_miss_null() {
    let table_owner = MemoryOwner::new(ResourceBudget::compact());
    let table = site_table(&table_owner);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let op = LookupOperator::new(
        lookup_spec(vec!["id".into()], vec!["site".into()]),
        table,
        input_schema(),
        owner.clone(),
    )
    .unwrap();
    let input = batch(
        &owner,
        input_schema(),
        vec![
            Row {
                values: vec![Scalar::utf8("a"), Scalar::Float64(1.0)],
            },
            Row {
                values: vec![Scalar::utf8("missing"), Scalar::Float64(2.0)],
            },
        ],
    );
    let output = op.on_batch_into(&input).unwrap().unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(output.rows()[0].values.last(), Some(&Scalar::utf8("west")));
    assert_eq!(output.rows()[1].values.last(), Some(&Scalar::Null));
    drop(output);
    drop(input);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn core_b_lookup_nullable_stream_key_is_a_miss() {
    let table_owner = MemoryOwner::new(ResourceBudget::compact());
    let table = site_table(&table_owner);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let op = LookupOperator::new(
        lookup_spec(vec!["id".into()], vec!["site".into()]),
        table,
        nullable_input_schema(),
        owner.clone(),
    )
    .unwrap();
    let input = batch(
        &owner,
        nullable_input_schema(),
        vec![Row {
            values: vec![Scalar::Null, Scalar::Float64(1.0)],
        }],
    );
    let output = op.on_batch_into(&input).unwrap().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.rows()[0].values.last(), Some(&Scalar::Null));
    drop(output);
    drop(input);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn core_b_lookup_output_budget_failure_refunds_scratch() {
    let table = ReferenceTable::snapshot(
        "sites",
        1,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec!["id".into()],
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::utf8("x".repeat(8192))],
        }],
        16,
        1024 * 1024,
    )
    .unwrap();
    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 4096,
        ..ResourceBudget::compact()
    });
    let op = LookupOperator::new(
        lookup_spec(vec!["id".into()], vec!["site".into()]),
        table,
        input_schema(),
        owner.clone(),
    )
    .unwrap();
    let input = batch(
        &owner,
        input_schema(),
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Float64(1.0)],
        }],
    );
    let error = op.on_batch_into(&input).unwrap_err();
    assert_eq!(error.code, ErrorCode::ResourceExhausted);
    drop(input);
    assert_eq!(owner.usage().reservation_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn core_b_versioned_publish_guards_order_and_schema() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let first = crate::lookup::table_from_pairs(
        "sites",
        1,
        "id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("west"))],
        &owner,
    )
    .unwrap();
    let second = crate::lookup::table_from_pairs(
        "sites",
        2,
        "id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("east"))],
        &owner,
    )
    .unwrap();
    let changed_schema = crate::lookup::table_from_pairs(
        "sites",
        3,
        "id",
        "site",
        DataType::Int64,
        vec![(Scalar::utf8("a"), Scalar::Int64(1))],
        &owner,
    )
    .unwrap();
    let versions = crate::VersionedReferenceTable::new("sites", 8).unwrap();
    versions.publish(1, 0, first.clone()).unwrap();

    let duplicate_version = versions.publish(1, 10, second.clone()).unwrap_err();
    assert_eq!(duplicate_version.code, ErrorCode::InvalidArgument);
    let backward_version = versions.publish(0, 10, second.clone()).unwrap_err();
    assert_eq!(backward_version.code, ErrorCode::InvalidArgument);
    let backward_time = versions.publish(2, 0, second.clone()).unwrap_err();
    assert_eq!(backward_time.code, ErrorCode::InvalidArgument);
    let schema_error = versions.publish(2, 10, changed_schema).unwrap_err();
    assert_eq!(schema_error.code, ErrorCode::InvalidSchema);

    versions.publish(2, 10, second).unwrap();
    assert!(versions.lookup_as_of(&[Scalar::utf8("a")], 9).is_some());
    assert!(versions.lookup_as_of(&[Scalar::utf8("a")], 10).is_some());
}

#[test]
fn core_b_reference_key_arity_mismatch_is_a_miss() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let table = site_table(&owner);
    assert!(table.get(&[]).is_none());
    assert!(table
        .get(&[Scalar::utf8("a"), Scalar::utf8("extra")])
        .is_none());
}

#[test]
fn core_b_snapshot_large_sparse_key_is_retained_and_lookupable() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let key = "sparse-".to_owned() + &"x".repeat(32 * 1024);
    let table = ReferenceTable::snapshot_owned(
        "sites",
        1,
        table_schema(DataType::Utf8, DataType::Utf8),
        vec!["id".into()],
        vec![Row {
            values: vec![Scalar::utf8(&key), Scalar::utf8("west")],
        }],
        16,
        1024 * 1024,
        &owner,
    )
    .unwrap();
    assert!(owner.usage().retention_bytes >= key.len());
    assert!(table.get(&[Scalar::utf8(&key)]).is_some());
    drop(table);
    assert_eq!(owner.usage().retention_bytes, 0);
}

#[test]
fn core_b_temporal_publish_boundary_keeps_selected_snapshot_stable() {
    let table_owner = MemoryOwner::new(ResourceBudget::compact());
    let first = crate::lookup::table_from_pairs(
        "sites",
        1,
        "id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("a"), Scalar::utf8("site-1"))],
        &table_owner,
    )
    .unwrap();
    let mut later = Vec::new();
    for version in 2..=33u64 {
        later.push(
            crate::lookup::table_from_pairs(
                "sites",
                version,
                "id",
                "site",
                DataType::Utf8,
                vec![(Scalar::utf8("a"), Scalar::utf8(format!("site-{version}")))],
                &table_owner,
            )
            .unwrap(),
        );
    }
    let versions = Arc::new(crate::VersionedReferenceTable::new("sites", 64).unwrap());
    versions.publish(1, 0, first).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let op = LookupOperator::new_versioned(
        LookupSpec {
            table: "sites".into(),
            stream_keys: vec!["id".into()],
            table_keys: vec!["id".into()],
            keep: vec!["site".into()],
            temporal: true,
            as_of_field: Some("ts".into()),
        },
        versions.as_ref().clone(),
        temporal_input_schema(),
        owner.clone(),
    )
    .unwrap();
    let input = batch(
        &owner,
        temporal_input_schema(),
        vec![Row {
            values: vec![
                Scalar::utf8("a"),
                Scalar::Float64(1.0),
                Scalar::Int64(10_000),
            ],
        }],
    );
    let publisher = Arc::clone(&versions);
    let publisher_thread = std::thread::spawn(move || {
        for (offset, table) in later.into_iter().enumerate() {
            publisher
                .publish((offset as u64) + 2, (offset as i64) + 1, table)
                .unwrap();
        }
    });
    for _ in 0..64 {
        let output = op.on_batch_into(&input).unwrap().unwrap();
        assert_eq!(output.num_rows(), 1);
        assert!(matches!(
            output.rows()[0].values.last(),
            Some(Scalar::Utf8(_))
        ));
        drop(output);
    }
    publisher_thread.join().unwrap();
    drop(input);
    assert_eq!(owner.usage().physical_bytes, 0);
}

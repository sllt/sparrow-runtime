use crate::iot::{IotFreeze, IotOperator, IOT_HYSTERESIS_KIND};
use sparrow_model::{
    CreditKind, DataType, Field, FieldId, MemoryOwner, OperatorId, ResourceBudget, Row,
    RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_plan::{HysteresisDirection, HysteresisSpec, InvalidValuePolicy, IotSpec};
use std::sync::Arc;

fn schema(ty: DataType) -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "value", ty, true),
        ],
    )
    .unwrap()
}
fn spec(direction: HysteresisDirection, enter: f64, exit: f64, emit_first: bool) -> IotSpec {
    IotSpec {
        keys: vec!["id".into()],
        fields: vec!["value".into()],
        emit_first,
        ttl_micros: 0,
        max_keys: 16,
        invalid: InvalidValuePolicy::Ignore,
        deadband: None,
        hysteresis: Some(HysteresisSpec {
            direction,
            enter,
            exit,
        }),
        timing: None,
    }
}
fn owner() -> Arc<MemoryOwner> {
    MemoryOwner::new(ResourceBudget::compact())
}
fn operator(owner: &Arc<MemoryOwner>, spec: IotSpec, ty: DataType) -> IotOperator {
    IotOperator::new(OperatorId::new(17), spec, schema(ty), owner.clone()).unwrap()
}
fn input(
    op: &mut IotOperator,
    owner: &Arc<MemoryOwner>,
    ty: DataType,
    rows: Vec<(&str, Scalar)>,
) -> Vec<Scalar> {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema(ty)),
        owner.clone(),
        CreditKind::Reservation,
        rows.len().max(1),
        owner.budget().reservation_bytes,
    )
    .unwrap();
    for (key, value) in rows {
        builder
            .push(Row {
                values: vec![Scalar::utf8(key), value],
            })
            .unwrap();
    }
    op.on_batch(&builder.finish().unwrap(), 0)
        .unwrap()
        .map(|batch| {
            batch
                .rows()
                .iter()
                .map(|row| row.values[1].clone())
                .collect()
        })
        .unwrap_or_default()
}
fn integers(values: &[i64]) -> Vec<(&str, Scalar)> {
    values.iter().map(|v| ("a", Scalar::Int64(*v))).collect()
}

#[test]
fn completion_hysteresis_high_equal_thresholds_and_in_band_latch() {
    let owner = owner();
    let mut op = operator(
        &owner,
        spec(HysteresisDirection::High, 10.0, 5.0, true),
        DataType::Int64,
    );
    let result = input(
        &mut op,
        &owner,
        DataType::Int64,
        integers(&[7, 10, 9, 6, 5, 6, 10, 10, 4]),
    );
    assert_eq!(
        result,
        vec![7, 10, 5, 10, 4]
            .into_iter()
            .map(Scalar::Int64)
            .collect::<Vec<_>>()
    );
    assert_eq!(op.stats().input_rows, 9);
    assert_eq!(op.stats().emitted_rows, 5);
    assert_eq!(op.stats().filtered_rows, 4);
}

#[test]
fn completion_hysteresis_low_and_suppressed_initial_state() {
    let owner = owner();
    let mut op = operator(
        &owner,
        spec(HysteresisDirection::Low, 5.0, 10.0, false),
        DataType::Int64,
    );
    let result = input(
        &mut op,
        &owner,
        DataType::Int64,
        integers(&[4, 6, 9, 10, 7, 5]),
    );
    assert_eq!(result, vec![Scalar::Int64(10), Scalar::Int64(5)]);
}

#[test]
fn completion_hysteresis_invalid_rows_never_reset_active_or_initialize_unknown() {
    let owner = owner();
    let mut op = operator(
        &owner,
        spec(HysteresisDirection::High, 10.0, 5.0, true),
        DataType::Float64,
    );
    let result = input(
        &mut op,
        &owner,
        DataType::Float64,
        vec![
            ("unknown", Scalar::Null),
            ("a", Scalar::Float64(10.0)),
            ("a", Scalar::Null),
            ("a", Scalar::Float64(f64::NAN)),
            ("a", Scalar::Float64(f64::INFINITY)),
            ("a", Scalar::Float64(7.0)),
            ("a", Scalar::Float64(5.0)),
        ],
    );
    assert_eq!(result, vec![Scalar::Float64(10.0), Scalar::Float64(5.0)]);
    assert_eq!(op.key_count(), 1);
    assert_eq!(op.stats().invalid_rows, 4);
}

#[test]
fn completion_hysteresis_integer_comparison_does_not_round_above_two_pow_53() {
    let owner = owner();
    let mut op = operator(
        &owner,
        spec(
            HysteresisDirection::High,
            9_007_199_254_740_992.0,
            9_007_199_254_740_990.0,
            true,
        ),
        DataType::UInt64,
    );
    let values = [
        9_007_199_254_740_991u64,
        9_007_199_254_740_992,
        9_007_199_254_740_991,
        9_007_199_254_740_990,
    ];
    let result = input(
        &mut op,
        &owner,
        DataType::UInt64,
        values.iter().map(|v| ("a", Scalar::UInt64(*v))).collect(),
    );
    assert_eq!(
        result,
        vec![
            Scalar::UInt64(values[0]),
            Scalar::UInt64(values[1]),
            Scalar::UInt64(values[3])
        ]
    );
    let mut high = operator(
        &owner,
        spec(HysteresisDirection::High, u64::MAX as f64, 0.0, false),
        DataType::UInt64,
    );
    assert!(input(
        &mut high,
        &owner,
        DataType::UInt64,
        vec![("a", Scalar::UInt64(0)), ("a", Scalar::UInt64(u64::MAX))]
    )
    .is_empty());
}

#[test]
fn completion_hysteresis_negative_fractional_thresholds_are_exact_for_integers() {
    let owner = owner();
    let mut op = operator(
        &owner,
        spec(HysteresisDirection::High, -0.5, -2.5, false),
        DataType::Int64,
    );
    let result = input(&mut op, &owner, DataType::Int64, integers(&[-1, 0, -2, -3]));
    assert_eq!(result, vec![Scalar::Int64(0), Scalar::Int64(-3)]);
}

#[test]
fn completion_hysteresis_freeze_restore_preserves_active_and_keys() {
    let owner = owner();
    let spec = spec(HysteresisDirection::High, 10.0, 5.0, true);
    let mut first = operator(&owner, spec.clone(), DataType::Int64);
    input(
        &mut first,
        &owner,
        DataType::Int64,
        vec![("a", Scalar::Int64(10)), ("b", Scalar::Int64(7))],
    );
    let frozen = first.freeze().unwrap();
    assert_eq!(frozen.kind, IOT_HYSTERESIS_KIND);
    let bytes = frozen.encode().unwrap();
    let decoded = IotFreeze::decode(&bytes, 16).unwrap();
    assert_eq!(decoded, frozen);
    let mut restored = operator(&owner, spec, DataType::Int64);
    restored.restore(&decoded).unwrap();
    let result = input(
        &mut restored,
        &owner,
        DataType::Int64,
        vec![
            ("a", Scalar::Int64(7)),
            ("b", Scalar::Int64(10)),
            ("a", Scalar::Int64(5)),
        ],
    );
    assert_eq!(result, vec![Scalar::Int64(10), Scalar::Int64(5)]);
}

#[test]
fn completion_hysteresis_corrupt_latch_codec_and_restore_are_atomic() {
    let owner = owner();
    let mut op = operator(
        &owner,
        spec(HysteresisDirection::High, 10.0, 5.0, true),
        DataType::Int64,
    );
    input(&mut op, &owner, DataType::Int64, integers(&[10]));
    let good = op.freeze().unwrap();
    let bytes = good.encode().unwrap();
    for length in 0..bytes.len() {
        assert!(IotFreeze::decode(&bytes[..length], 16).is_err());
    }
    let mut corrupt = good.clone();
    corrupt.entries[0].values = vec![Scalar::Int64(1)];
    assert!(corrupt.encode().is_err());
    assert!(op.restore(&corrupt).is_err());
    assert_eq!(op.freeze().unwrap(), good);
    let mut noncanonical = bytes.clone();
    *noncanonical.last_mut().unwrap() = 2;
    assert!(IotFreeze::decode(&noncanonical, 16).is_err());
    assert!(IotFreeze::decode_mode(&mut noncanonical.as_slice(), 16, false).is_err());
}

#[test]
fn completion_hysteresis_key_budget_failure_and_reset_refund() {
    let owner = owner();
    let mut configuration = spec(HysteresisDirection::High, 10.0, 5.0, true);
    configuration.max_keys = 1;
    let mut op = operator(&owner, configuration, DataType::Int64);
    input(&mut op, &owner, DataType::Int64, integers(&[10]));
    let original = op.freeze().unwrap();
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema(DataType::Int64)),
        owner.clone(),
        CreditKind::Reservation,
        1,
        owner.budget().reservation_bytes,
    )
    .unwrap();
    builder
        .push(Row {
            values: vec![Scalar::utf8("other"), Scalar::Int64(11)],
        })
        .unwrap();
    assert!(op.on_batch(&builder.finish().unwrap(), 0).is_err());
    assert_eq!(op.freeze().unwrap(), original);
    op.reset();
    assert_eq!(op.key_count(), 0);
    assert_eq!(owner.usage().retention_bytes, 0);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

//! V0.3 time anti-examples: uninitialized WM, idle/active, all-idle,
//! future timestamp, monotonic WM, holdback, late side output.

use super::*;
use sparrow_expr::Expr;
use sparrow_model::{
    AggFn, DataType, ErrorCode, Field, FieldId, InputId, MemoryOwner, OperatorId, ResourceBudget,
    Row, Scalar, Schema, SchemaId, WindowKind, DEFAULT_MAX_HOP_OVERLAP,
};
use sparrow_plan::{AggCall, WindowSpec};
use std::sync::Arc;

fn sensor_et() -> Schema {
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

fn et_avg(size: i64, lateness: i64) -> WindowSpec {
    WindowSpec::new(
        WindowKind::tumbling_et(size).unwrap(),
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Avg,
            Some(Expr::Column {
                name: "temperature".into(),
            }),
            "avg_temp",
        )],
    )
    .event_time("ts", lateness)
}

fn batch(owner: &Arc<MemoryOwner>, rows: Vec<Row>) -> sparrow_model::RowBatch {
    let schema = Arc::new(sensor_et());
    let mut b = sparrow_model::RowBatchBuilder::new(
        schema,
        Arc::clone(owner),
        sparrow_model::CreditKind::Reservation,
        rows.len().max(1),
        64 * 1024,
    )
    .unwrap();
    for r in rows {
        b.push(r).unwrap();
    }
    b.finish().unwrap()
}

fn row(dev: &str, temp: f64, ts: i64) -> Row {
    Row {
        values: vec![Scalar::utf8(dev), Scalar::Float64(temp), Scalar::Int64(ts)],
    }
}

fn op(spec: WindowSpec) -> (WindowOperator, Arc<MemoryOwner>) {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let w = WindowOperator::new(
        OperatorId::new(2),
        spec,
        sensor_et(),
        Arc::clone(&owner),
        64,
        64,
    )
    .unwrap();
    (w, owner)
}

#[test]
fn multi_input_uninitialized_wm_blocks() {
    let mut hub = WatermarkHub::new();
    hub.register(InputId(0)).unwrap();
    hub.register(InputId(1)).unwrap();
    hub.set_watermark(InputId(0), 10_000_000).unwrap();
    assert!(
        hub.effective().is_none(),
        "active uninitialized input must block effective WM"
    );
}

#[test]
fn idle_active_excludes_idle() {
    let mut hub = WatermarkHub::new();
    hub.register(InputId(0)).unwrap();
    hub.register(InputId(1)).unwrap();
    hub.set_watermark(InputId(0), 20_000_000).unwrap();
    hub.mark_idle(InputId(1)).unwrap();
    assert_eq!(hub.effective(), Some(20_000_000));
    hub.mark_active(InputId(1)).unwrap();
    assert!(hub.effective().is_none());
}

#[test]
fn all_idle_does_not_advance() {
    let mut hub = WatermarkHub::new();
    hub.register(InputId(0)).unwrap();
    hub.register(InputId(1)).unwrap();
    hub.set_watermark(InputId(0), 5).unwrap();
    hub.mark_idle(InputId(0)).unwrap();
    hub.mark_idle(InputId(1)).unwrap();
    assert!(hub.all_idle());
    assert!(hub.effective().is_none());
    assert!(hub.progress().is_none());
}

#[test]
fn future_timestamp_is_rejected() {
    let bind = sparrow_model::EventTimeBinding {
        field: "ts".into(),
        out_of_orderness_micros: 0,
        max_future_skew_micros: Some(1_000_000),
    };
    let mut hub = WatermarkHub::new().with_binding(bind).unwrap();
    let err = hub.observe_event(InputId(0), 9_000_000, 0).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("future timestamp"), "{}", err.message);
}

#[test]
fn watermark_never_goes_backward() {
    let mut hub = WatermarkHub::new();
    hub.set_watermark(InputId(0), 10).unwrap();
    hub.set_watermark(InputId(0), 4).unwrap();
    assert_eq!(hub.input_wm(InputId(0)), Some(10));
    assert_eq!(hub.effective(), Some(10));
}

#[test]
fn holdback_wm_out_le_wm_in_minus_l() {
    let mut hb = OutputHoldback::new(3_000_000).unwrap();
    assert_eq!(hb.on_wm_in(13_000_000).unwrap(), Some(10_000_000));
    hb.advance_out(10_000_000).unwrap();
    assert_eq!(hb.wm_out(), Some(10_000_000));
    assert!(hb.advance_out(11_000_000).is_err());
}

#[test]
fn final_avg_80_then_late_side_output() {
    // §51.2-style: window [0,10s), L=3s, key d1, AVG=80 once at e+L, then t=8 late.
    let spec = et_avg(10_000_000, 3_000_000);
    let (mut op, owner) = op(spec);
    let b = batch(
        &owner,
        vec![
            row("d1", 100.0, 1_000_000),
            row("d1", 60.0, 4_000_000),
        ],
    );
    let mid = op.on_batch(&b, 0).unwrap();
    assert!(mid.finals.is_empty(), "must not emit before e+L: {:?}", mid.finals);
    let closed = op.observe_watermark(InputId(0), 13_000_000).unwrap();
    assert_eq!(closed.finals.len(), 1, "{:?}", closed.finals);
    match closed.finals[0].values.as_slice() {
        [Scalar::Utf8(d), Scalar::Int64(0), Scalar::Int64(10_000_000), Scalar::Float64(avg)] => {
            assert_eq!(d.as_ref(), "d1");
            assert!((avg - 80.0).abs() < 1e-9, "avg={avg}");
        }
        other => panic!("unexpected final {other:?}"),
    }
    assert_eq!(op.wm_out(), Some(10_000_000));
    let late = op
        .on_batch(&batch(&owner, vec![row("d1", 99.0, 8_000_000)]), 0)
        .unwrap();
    assert!(late.finals.is_empty(), "no retract / update after close");
    assert_eq!(late.lates.len(), 1);
    assert_eq!(late.lates[0].values[2], Scalar::Int64(8_000_000));
}

#[test]
fn count_window_cannot_impersonate_event_time() {
    let spec = WindowSpec {
        kind: WindowKind::count(2).unwrap(),
        keys: vec!["device_id".into()],
        aggs: vec![AggCall::count_star("n")],
        event_time_field: Some("ts".into()),
        lateness_micros: 3,
        max_overlap: DEFAULT_MAX_HOP_OVERLAP,
    };
    let err = spec.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("impersonate"), "{}", err.message);
}

#[test]
fn hop_overlap_planner_bound() {
    let ok = WindowSpec::new(
        WindowKind::hopping_et(10_000_000, 5_000_000).unwrap(),
        vec!["device_id".into()],
        vec![AggCall::count_star("n")],
    )
    .event_time("ts", 0);
    ok.validate().unwrap();
    let bad = WindowSpec {
        kind: WindowKind::hopping_et(90_000_000, 10_000_000).unwrap(),
        keys: vec!["device_id".into()],
        aggs: vec![AggCall::count_star("n")],
        event_time_field: Some("ts".into()),
        lateness_micros: 0,
        max_overlap: 8,
    };
    assert_eq!(bad.validate().unwrap_err().code, ErrorCode::BoundExceeded);
}

#[test]
fn versioned_lookup_as_of_event_time() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let v1 = crate::lookup::table_from_pairs(
        "sites",
        1,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("d1"), Scalar::utf8("west"))],
        &owner,
    )
    .unwrap();
    let v2 = crate::lookup::table_from_pairs(
        "sites",
        2,
        "device_id",
        "site",
        DataType::Utf8,
        vec![(Scalar::utf8("d1"), Scalar::utf8("east"))],
        &owner,
    )
    .unwrap();
    let table = VersionedReferenceTable::new("sites", 8).unwrap();
    table.publish(1, 0, v1).unwrap();
    table.publish(2, 5_000_000, v2).unwrap();
    let spec = sparrow_plan::LookupSpec {
        table: "sites".into(),
        stream_keys: vec!["device_id".into()],
        table_keys: vec!["device_id".into()],
        keep: vec!["site".into()],
        temporal: true,
        as_of_field: Some("ts".into()),
    };
    let op = crate::lookup::LookupOperator::new_versioned(
        spec,
        table,
        sensor_et(),
        Arc::clone(&owner),
    )
    .unwrap();
    let b = batch(
        &owner,
        vec![row("d1", 1.0, 1_000_000), row("d1", 1.0, 6_000_000)],
    );
    let out = op.on_batch(&b).unwrap();
    match out[0].values.last() {
        Some(Scalar::Utf8(s)) => assert_eq!(s.as_ref(), "west"),
        other => panic!("{other:?}"),
    }
    match out[1].values.last() {
        Some(Scalar::Utf8(s)) => assert_eq!(s.as_ref(), "east"),
        other => panic!("{other:?}"),
    }
}

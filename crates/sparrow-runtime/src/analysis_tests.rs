use crate::bounded_join::BoundedJoin;
use crate::*;
use serde_json::json;
use sparrow_model::{
    AggFn, DataType, DynamicValue as D, ErrorCode, Field, MemoryOwner, ResourceBudget, Row, Scalar,
    Schema,
};
use sparrow_plan::{AnalysisPlan, JoinMode, StreamJoinSpec};
use std::{collections::HashMap, time::Duration};

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "k", DataType::Utf8, true),
            Field::new(2, "v", DataType::Int64, false),
            Field::new(3, "ts", DataType::Int64, false),
        ],
    )
    .unwrap()
}
fn row(key: &str, value: i64, time: i64) -> Row {
    Row {
        values: vec![Scalar::utf8(key), Scalar::Int64(value), Scalar::Int64(time)],
    }
}
fn spec(mode: JoinMode, window: bool) -> StreamJoinSpec {
    StreamJoinSpec {
        left_input: 1,
        right_input: 2,
        left_keys: vec!["k".into()],
        right_keys: vec!["k".into()],
        left_time: "ts".into(),
        right_time: "ts".into(),
        mode,
        before_micros: (!window).then_some(3),
        after_micros: (!window).then_some(5),
        window_size_micros: window.then_some(10),
        left_prefix: "l_".into(),
        right_prefix: "r_".into(),
        max_rows_per_side: 128,
        max_matches_per_row: 128,
        max_output_bytes_per_row: 65536,
    }
}
fn batch_rows(b: sparrow_model::RowBatch) -> Vec<Vec<Scalar>> {
    b.rows().iter().map(|r| r.values.clone()).collect()
}
fn insert(op: &mut BoundedJoin, side: usize, row: &Row) -> Vec<Vec<Scalar>> {
    let mut pending = op.prepare(side, row, 10000).unwrap().unwrap();
    op.check_fanout(side, &pending).unwrap();
    let mut rows = vec![];
    let mut cursor = 0;
    while let Some(id) = op.next_match(side, &pending, cursor) {
        cursor = id;
        rows.extend(batch_rows(op.emit_match(side, &mut pending, id).unwrap()));
    }
    op.insert(side, pending);
    rows
}
fn drain(op: &mut BoundedJoin) -> Vec<Vec<Scalar>> {
    let mut rows = vec![];
    while let Some((side, id)) = op.expired() {
        if let Some(b) = op.close(side, id).unwrap() {
            rows.extend(batch_rows(b));
        }
    }
    rows
}

#[test]
fn analysis_joins_match_independent_cross_product_in_both_arrival_orders() {
    let left = [("a", 1, 10), ("a", 2, 20), ("b", 3, 30), ("c", 4, 40)];
    let right = [
        ("a", 101, 7),
        ("a", 102, 15),
        ("a", 103, 20),
        ("b", 104, 36),
    ];
    for mode in [JoinMode::Inner, JoinMode::Left] {
        for window in [false, true] {
            for right_first in [false, true] {
                let owner = MemoryOwner::new(ResourceBudget::compact());
                let mut op =
                    BoundedJoin::new(spec(mode, window), schema(), schema(), owner.clone(), 64)
                        .unwrap();
                let mut output = vec![];
                for side in if right_first { [1, 0] } else { [0, 1] } {
                    for (k, v, t) in if side == 0 { &left } else { &right } {
                        output.extend(insert(&mut op, side, &row(k, *v, *t)));
                    }
                }
                op.advance(0, i64::MAX).unwrap();
                op.advance(1, i64::MAX).unwrap();
                output.extend(drain(&mut op));
                let mut actual: Vec<_> = output
                    .iter()
                    .map(|r| match (&r[1], &r[4]) {
                        (Scalar::Int64(l), Scalar::Int64(r)) => (*l, Some(*r)),
                        (Scalar::Int64(l), Scalar::Null) => (*l, None),
                        _ => panic!(),
                    })
                    .collect();
                let mut expected = vec![];
                for (key, lv, lt) in left {
                    let mut matched = false;
                    for (rk, rv, rt) in right {
                        if key == rk
                            && if window {
                                lt / 10 == rt / 10
                            } else {
                                rt >= lt - 3 && rt <= lt + 5
                            }
                        {
                            expected.push((lv, Some(rv)));
                            matched = true;
                        }
                    }
                    if !matched && mode == JoinMode::Left {
                        expected.push((lv, None));
                    }
                }
                actual.sort();
                expected.sort();
                assert_eq!(
                    actual, expected,
                    "{mode:?} window={window} right_first={right_first}"
                );
                assert_eq!(op.state(), (0, 0));
                drop(op);
                assert_eq!(owner.usage().physical_bytes, 0);
                assert_eq!(owner.accounting_errors_total(), 0);
            }
        }
    }
}

#[test]
fn analysis_left_waits_for_strict_upper_bound_and_holds_output_watermark() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut s = spec(JoinMode::Left, false);
    s.before_micros = Some(0);
    s.after_micros = Some(0);
    let mut op = BoundedJoin::new(s, schema(), schema(), owner.clone(), 4).unwrap();
    assert!(insert(&mut op, 0, &row("a", 1, 10)).is_empty());
    op.advance(0, 20).unwrap();
    op.advance(1, 10).unwrap();
    assert!(drain(&mut op).is_empty());
    assert_eq!(op.progress(), Some(10));
    // Equality remains eligible while right WM == 10.
    let output = insert(&mut op, 1, &row("a", 2, 10));
    assert_eq!(output.len(), 1);
    op.advance(1, 11).unwrap();
    assert!(drain(&mut op).is_empty());
    assert_eq!(op.progress(), Some(11));
    assert!(op.prepare(0, &row("a", 3, 19), 100).is_err());
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn analysis_join_null_keys_fanout_admission_and_refund() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut s = spec(JoinMode::Left, false);
    s.max_matches_per_row = 1;
    let mut op = BoundedJoin::new(s, schema(), schema(), owner.clone(), 4).unwrap();
    insert(&mut op, 1, &row("a", 2, 10));
    insert(&mut op, 1, &row("a", 3, 10));
    let before = owner.usage().physical_bytes;
    let candidate = op.prepare(0, &row("a", 1, 10), 100).unwrap().unwrap();
    assert_eq!(
        op.check_fanout(0, &candidate).unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    drop(candidate);
    assert_eq!(owner.usage().physical_bytes, before);
    let mut null = row("a", 9, 10);
    null.values[0] = Scalar::Null;
    assert!(insert(&mut op, 0, &null).is_empty());
    op.advance(1, 16).unwrap();
    let out = drain(&mut op);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][4], Scalar::Null);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

fn graph(window: bool, left_join: bool) -> sparrow_plan::PhysicalPlan {
    let s = spec(
        if left_join {
            JoinMode::Left
        } else {
            JoinMode::Inner
        },
        window,
    );
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert("l", schema());
    catalog.insert("r", schema());
    let graph=sparrow_plan::GraphSpec::from_json(&json!({"version":1,"pipeline_id":9,"revision_id":1,"nodes":[
        {"id":1,"kind":"memory_source","table":"l","event_time_field":"ts","out_of_orderness_micros":1000,"out":[3]},
        {"id":2,"kind":"memory_source","table":"r","event_time_field":"ts","out_of_orderness_micros":1000,"out":[3]},
        {"id":3,"kind":if window{"window_join"}else{"interval_join"},"stream_join":s,"out":[4]},
        {"id":4,"kind":"capture_sink"}]}).to_string()).unwrap();
    sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &catalog).unwrap(),
        &Default::default(),
    )
}
#[test]
fn analysis_kernel_two_ports_eof_and_finite_query_refund() {
    for window in [false, true] {
        let inputs = HashMap::from([
            (1.into(), vec![row("a", 1, 10), row("b", 2, 30)]),
            (2.into(), vec![row("a", 3, 15)]),
        ]);
        let result = crate::finite::execute(
            graph(window, true),
            inputs,
            Default::default(),
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(result.output_rows, 2);
        assert_eq!(result.input_rows, 3);
        let owner = result.owner();
        drop(result);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
    let mut limits = crate::finite::FiniteLimits::default();
    limits.work_units = 1;
    assert!(crate::finite::execute(
        graph(false, true),
        HashMap::from([
            (1.into(), vec![row("a", 1, 10)]),
            (2.into(), vec![row("a", 2, 10)])
        ]),
        limits,
        tokio_util::sync::CancellationToken::new()
    )
    .is_err());
}

#[test]
fn analysis_join_distinct_time_fields_feed_event_time_window() {
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert("l", schema());
    let mut right = schema();
    right.fields[2].name = "right_ts".into();
    catalog.insert("r", right);
    let mut join = spec(JoinMode::Left, false);
    join.right_time = "right_ts".into();
    let graph=sparrow_plan::GraphSpec::from_json(&json!({"version":1,"pipeline_id":9,"revision_id":1,"nodes":[
        {"id":1,"kind":"memory_source","table":"l","event_time_field":"ts","out_of_orderness_micros":1000,"out":[3]},
        {"id":2,"kind":"memory_source","table":"r","event_time_field":"right_ts","out_of_orderness_micros":1000,"out":[3]},
        {"id":3,"kind":"interval_join","stream_join":join,"out":[4]},
        {"id":4,"kind":"window_agg","event_time_field":"join_time","window":{"kind":"tumble_et","size_micros":10},"aggs":[{"fn":"count","alias":"n"}],"out":[5]},
        {"id":5,"kind":"capture_sink"}]}).to_string()).unwrap();
    let plan = sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &catalog).unwrap(),
        &Default::default(),
    );
    let result = crate::finite::execute(
        plan,
        HashMap::from([
            (1.into(), vec![row("a", 1, 10), row("b", 2, 30)]),
            (2.into(), vec![row("a", 3, 15)]),
        ]),
        Default::default(),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    let col = result.schema.index_of_name("n").unwrap();
    let counts: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| b.rows().iter().map(|r| r.values[col].clone()))
        .collect();
    assert_eq!(counts, vec![Scalar::Int64(1), Scalar::Int64(1)]);
    let owner = result.owner();
    drop(result);
    assert_eq!(owner.usage().physical_bytes, 0);
}

fn unnest(max_rows: usize, max_bytes: usize) -> sparrow_plan::PhysicalPlan {
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.insert(
        "s",
        Schema::new(
            1,
            vec![
                Field::new(1, "id", DataType::Int64, false),
                Field::new(2, "items", DataType::Dynamic, true),
            ],
        )
        .unwrap(),
    );
    let graph=sparrow_plan::GraphSpec::from_json(&json!({"version":1,"pipeline_id":9,"revision_id":1,"nodes":[
        {"id":1,"kind":"memory_source","table":"s","out":[2]},
        {"id":2,"kind":"unnest","unnest":{"expr":{"k":"col","name":"items"},"as_field":"item","max_rows":max_rows,"max_bytes":max_bytes},"out":[3]},
        {"id":3,"kind":"capture_sink"}]}).to_string()).unwrap();
    sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &catalog).unwrap(),
        &Default::default(),
    )
}
fn array_row(id: i64, items: Vec<D>) -> Row {
    Row {
        values: vec![Scalar::Int64(id), Scalar::Dynamic(D::Array(items.into()))],
    }
}
#[test]
fn analysis_unnest_empty_null_order_bounds_and_control_admission() {
    let rows = vec![
        array_row(1, vec![D::Int64(7), D::Null, D::Int64(9)]),
        array_row(2, vec![]),
        Row {
            values: vec![Scalar::Int64(3), Scalar::Null],
        },
        array_row(4, vec![D::Int64(11)]),
    ];
    let result = crate::finite::execute(
        unnest(4, 65536),
        HashMap::from([(1.into(), rows.clone())]),
        Default::default(),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    let out: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| b.rows().iter().map(|r| r.values.clone()))
        .collect();
    assert_eq!(out.len(), 4);
    assert_eq!(out[0][2], Scalar::Dynamic(D::Int64(7)));
    assert_eq!(out[1][2], Scalar::Null);
    assert_eq!(out[2][4..], vec![Scalar::Int64(1), Scalar::Int64(3)]);
    assert_eq!(out[3][4..], vec![Scalar::Int64(4), Scalar::Int64(1)]);
    for plan in [unnest(2, 65536), unnest(4, 32)] {
        assert!(crate::finite::execute(
            plan,
            HashMap::from([(1.into(), rows.clone())]),
            Default::default(),
            tokio_util::sync::CancellationToken::new()
        )
        .is_err());
    }
    assert!(sparrow_plan::CheckpointPlan::from_physical(&unnest(4, 65536)).is_err());
    let mut bad = unnest(4, 65536);
    if let sparrow_plan::PhysicalStage::Analysis { plan, .. } = &mut bad.stages[1] {
        if let AnalysisPlan::Unnest { output, .. } = plan.as_mut() {
            output.fields.pop();
        }
    }
    let kernel = Kernel::new(Default::default()).unwrap();
    assert!(kernel
        .submit(JobRequest::new(bad, rows, SharedCapture::new()))
        .is_err());
}

#[test]
fn analysis_extended_aggregates_have_independent_numeric_goldens_and_no_codec() {
    use crate::aggregate::Accumulator;
    for (kind, expected) in [
        (AggFn::VarPop, 4.0),
        (AggFn::VarSamp, 32.0 / 7.0),
        (AggFn::StddevPop, 2.0),
        (AggFn::StddevSamp, (32.0f64 / 7.0).sqrt()),
    ] {
        let mut acc = Accumulator::new(kind, DataType::Int64, false).unwrap();
        assert_eq!(acc.finish(), Scalar::Null);
        for v in [2, 4, 4, 4, 5, 5, 7, 9] {
            acc.update(&Scalar::Int64(v)).unwrap();
        }
        acc.update(&Scalar::Null).unwrap();
        let Scalar::Float64(actual) = acc.finish() else {
            panic!()
        };
        assert!((actual - expected).abs() < 1e-12);
        assert!(acc.encode(&mut vec![]).is_err());
        let before = acc.clone();
        assert!(acc.update(&Scalar::Float64(f64::INFINITY)).is_err());
        assert_eq!(acc, before);
    }
    for (kind, expected) in [(AggFn::First, "a"), (AggFn::Last, "b")] {
        let mut acc = Accumulator::new(kind, DataType::Utf8, false).unwrap();
        for value in [
            Scalar::Null,
            Scalar::utf8("a"),
            Scalar::Null,
            Scalar::utf8("b"),
        ] {
            acc.update(&value).unwrap();
        }
        assert_eq!(acc.finish(), Scalar::utf8(expected));
    }
}

#[test]
fn analysis_unnest_typed_timestamp_preserves_type_and_budget() {
    let input = Schema::new(
        1,
        vec![Field::new(
            1,
            "items",
            DataType::Array(Box::new(DataType::TimestampMicrosUTC)),
            false,
        )],
    )
    .unwrap();
    let analysis = AnalysisPlan::unnest(
        sparrow_plan::UnnestSpec {
            expr: sparrow_expr::Expr::Column {
                name: "items".into(),
            },
            as_field: "item".into(),
            max_rows: 4,
            max_bytes: 65536,
        },
        input.clone(),
    )
    .unwrap();
    let output = analysis.output().clone();
    let mut plan = unnest(4, 65536);
    if let sparrow_plan::PhysicalStage::MemorySource { schema, .. } = &mut plan.stages[0] {
        *schema = input;
    }
    if let sparrow_plan::PhysicalStage::Analysis { plan, .. } = &mut plan.stages[1] {
        *plan = Box::new(analysis);
    }
    if let sparrow_plan::PhysicalStage::CaptureSink { schema, .. } = &mut plan.stages[2] {
        *schema = output;
    }
    let result = crate::finite::execute(
        plan,
        HashMap::from([(
            1.into(),
            vec![Row {
                values: vec![Scalar::Dynamic(D::Array(
                    vec![D::Int64(42), D::Null].into(),
                ))],
            }],
        )]),
        Default::default(),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    let values: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| b.rows().iter().map(|r| r.values[1].clone()))
        .collect();
    assert_eq!(values, vec![Scalar::TimestampMicrosUTC(42), Scalar::Null]);
    let owner = result.owner();
    drop(result);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn analysis_cancel_full_expansion_output_joins_and_refunds() {
    let mut options = KernelOptions::default();
    options.mailbox.max_items = 1;
    let kernel = Kernel::new(options).unwrap();
    let capture = SharedCapture::new();
    capture.stall.stall();
    let handle = kernel
        .submit(JobRequest::new(
            unnest(64, 65536),
            vec![array_row(1, (0..20).map(D::Int64).collect())],
            capture,
        ))
        .unwrap();
    let owner = handle.memory_owner();
    let observer = handle.mailbox_observer();
    kernel.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !observer
                .snapshot()
                .edges
                .iter()
                .any(|e| e.queue.waiting_senders > 0)
            {
                tokio::task::yield_now().await;
            }
            let _ = handle.stop().await;
        })
        .await
        .unwrap();
    });
    drop(observer);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(kernel.live_tasks(), 0);
}

#[test]
fn analysis_cancel_blocked_join_refunds_both_sides() {
    let mut options = KernelOptions::default();
    options.mailbox.max_items = 1;
    let kernel = Kernel::new(options).unwrap();
    let capture = SharedCapture::new();
    capture.stall.stall();
    let mut request = JobRequest::new(graph(false, false), vec![], capture);
    for id in [1, 2] {
        request.graph_inputs.insert(
            id.into(),
            GraphInput {
                rows: (0..8).map(|v| row("a", v, 10)).collect(),
                ..Default::default()
            },
        );
    }
    let handle = kernel.submit(request).unwrap();
    let owner = handle.memory_owner();
    let observer = handle.mailbox_observer();
    kernel.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !observer
                .snapshot()
                .edges
                .iter()
                .any(|e| e.queue.waiting_senders > 0)
            {
                tokio::task::yield_now().await;
            }
            let _ = handle.stop().await;
        })
        .await
        .unwrap();
    });
    drop(observer);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(kernel.live_tasks(), 0);
    assert_eq!(owner.accounting_errors_total(), 0);
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let result = crate::finite::execute(
        graph(false, false),
        HashMap::from([
            (1.into(), vec![row("a", 1, 10)]),
            (2.into(), vec![row("a", 2, 10)]),
        ]),
        Default::default(),
        cancel,
    );
    assert!(matches!(result,Err(e) if e.code==ErrorCode::Cancelled));
}

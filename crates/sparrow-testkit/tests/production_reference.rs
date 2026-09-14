//! Independent finite-input specifications. Expected results never call the
//! production evaluator, accumulator or window implementation. Seeds and the
//! first failing prefix are printed so failures have a deterministic replay.
use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::{
    DataType, DynamicValue, ErrorCode, Field, PipelineId, RevisionId, Row, Scalar, Schema,
};
use sparrow_plan::{bind_graph, physicalize, Catalog, GraphSpec, PlanOptions};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, SharedCapture, StreamControl};
use std::collections::BTreeMap;

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "device_id", DataType::Utf8, false),
            Field::new(2, "v", DataType::Int64, false),
            Field::new(3, "ts", DataType::Int64, false),
        ],
    )
    .unwrap()
}
fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.insert("sensors", schema());
    c
}
fn lit(s: Scalar) -> Expr {
    Expr::Literal(s)
}
fn binary(op: BinaryOp, a: Scalar, b: Scalar) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(lit(a)),
        right: Box::new(lit(b)),
    }
}
fn normalize(rows: Vec<Vec<Scalar>>) -> Vec<(String, i64)> {
    rows.into_iter()
        .map(|r| {
            let Scalar::Utf8(key) = &r[0] else {
                panic!("key {r:?}")
            };
            let Scalar::Int64(value) = r[1] else {
                panic!("value {r:?}")
            };
            (key.to_string(), value)
        })
        .collect()
}
fn inputs(mut seed: u64, n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            Row {
                values: vec![
                    Scalar::utf8(format!("d{}", (seed >> 32) % 4)),
                    Scalar::Int64((seed % 31) as i64 - 10),
                    Scalar::Int64(i as i64),
                ],
            }
        })
        .collect()
}

#[test]
fn production_expression_handwritten_golden_and_eager_error_contract() {
    use BinaryOp::*;
    use Scalar::*;
    let cases = vec![
        (binary(Add, Null, Int64(2)), Ok(Null)),
        (binary(And, Bool(false), Null), Ok(Bool(false))),
        (binary(Or, Bool(true), Null), Ok(Bool(true))),
        (
            binary(Eq, Float64(f64::NAN), Float64(f64::NAN)),
            Ok(Bool(false)),
        ),
        (binary(Eq, Float64(-0.0), Float64(0.0)), Ok(Bool(true))),
        (binary(Div, Int64(1), Int64(0)), Ok(Null)),
        (
            binary(Add, Int64(i64::MAX), Int64(1)),
            Err(ErrorCode::IntegerOverflow),
        ),
        (
            binary(Div, Int64(i64::MIN), Int64(-1)),
            Err(ErrorCode::IntegerOverflow),
        ),
        (
            binary(Add, UInt64(u64::MAX), Int64(-1)),
            Err(ErrorCode::TypeMismatch),
        ),
        (
            Expr::TryCast {
                expr: Box::new(lit(Scalar::utf8("bad"))),
                target: DataType::Int64,
            },
            Ok(Null),
        ),
        (
            Expr::DynamicGet {
                expr: Box::new(lit(Dynamic(DynamicValue::Object(vec![].into())))),
                key: "missing".into(),
            },
            Ok(Null),
        ),
        (
            Expr::Call {
                name: "lower".into(),
                args: vec![lit(Scalar::utf8("ÉA"))],
            },
            Ok(Scalar::utf8("Éa")),
        ),
        (
            Expr::Call {
                name: "length".into(),
                args: vec![lit(Scalar::utf8("界a"))],
            },
            Ok(Int64(2)),
        ),
        (
            Expr::Call {
                name: "coalesce".into(),
                args: vec![lit(Int64(7)), binary(Add, Int64(i64::MAX), Int64(1))],
            },
            Err(ErrorCode::IntegerOverflow),
        ),
    ];
    for (index, (expression, expected)) in cases.into_iter().enumerate() {
        let actual = sparrow_expr::eval(&expression, &schema(), &[]).map_err(|e| e.code);
        assert_eq!(actual, expected, "golden={index} expression={expression:?}");
    }
    for function in sparrow_expr::semantics::FUNCTIONS {
        let expression = Expr::Call {
            name: function.name.into(),
            args: vec![],
        };
        assert_eq!(
            sparrow_expr::bind(&expression, &schema()).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
    }
}

#[test]
fn production_filter_prevents_projection_error_and_failed_batch_publishes_nothing() {
    use sparrow_model::{CreditKind, MemoryOwner, ResourceBudget, RowBatchBuilder, WorkBudget};
    use sparrow_plan::TransformStep;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let input = schema();
    let output = Schema::new(2, vec![Field::new(1, "out", DataType::Int64, false)]).unwrap();
    let project = TransformStep::Project {
        operator: 1.into(),
        input: input.clone(),
        output,
        exprs: vec![Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Column { name: "v".into() }),
            right: Box::new(lit(Scalar::Int64(1))),
        }],
    };
    let filtered = sparrow_runtime::transform::CompiledTransform::new(&[
        TransformStep::Filter {
            operator: 2.into(),
            input: input.clone(),
            predicate: Expr::Binary {
                op: BinaryOp::Lt,
                left: Box::new(Expr::Column { name: "v".into() }),
                right: Box::new(lit(Scalar::Int64(0))),
            },
        },
        project.clone(),
    ])
    .unwrap();
    let mut builder = RowBatchBuilder::new(
        std::sync::Arc::new(input),
        owner.clone(),
        CreditKind::Reservation,
        2,
        4096,
    )
    .unwrap();
    for v in [1, i64::MAX] {
        builder
            .push(Row {
                values: vec![Scalar::utf8("d"), Scalar::Int64(v), Scalar::Int64(0)],
            })
            .unwrap();
    }
    let batch = builder.finish().unwrap();
    let before = owner.usage().physical_bytes;
    assert!(filtered
        .apply(&batch, &owner, &WorkBudget::new(100))
        .unwrap()
        .is_none());
    let unfiltered = sparrow_runtime::transform::CompiledTransform::new(&[project]).unwrap();
    assert_eq!(
        unfiltered
            .apply(&batch, &owner, &WorkBudget::new(100))
            .unwrap_err()
            .code,
        ErrorCode::IntegerOverflow
    );
    assert_eq!(
        owner.usage().physical_bytes,
        before,
        "failed partial builder and scratch are released"
    );
    drop(batch);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn production_linear_sql_graph_fusion_match_independent_finite_reference() {
    let graph=GraphSpec::from_json(r#"{"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
      {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
      {"id":2,"kind":"filter","predicate":{"k":"bin","op":"gt","left":{"k":"col","name":"v"},"right":{"k":"lit","value":{"t":"int64","v":0}}},"out":[3]},
      {"id":3,"kind":"project","exprs":[{"alias":"device_id","expr":{"k":"col","name":"device_id"}},{"alias":"out","expr":{"k":"bin","op":"add","left":{"k":"bin","op":"mul","left":{"k":"col","name":"v"},"right":{"k":"lit","value":{"t":"int64","v":3}}},"right":{"k":"lit","value":{"t":"int64","v":2}}}}],"out":[4]},
      {"id":4,"kind":"capture_sink","name":"out"}]}"#).unwrap();
    let sql = sparrow_sql::bind_sql(
        "SELECT device_id, v * 3 + 2 AS out FROM sensors WHERE v > 0",
        &catalog(),
        PipelineId::new(1),
        RevisionId::new(1),
    )
    .unwrap();
    let graph = bind_graph(&graph, &catalog()).unwrap();
    for seed in [1, 7, 0xcafe, 0x5eed] {
        let rows = inputs(seed, 512);
        let expected: Vec<_> = rows
            .iter()
            .filter_map(|r| {
                let Scalar::Utf8(k) = &r.values[0] else {
                    unreachable!()
                };
                let Scalar::Int64(v) = r.values[1] else {
                    unreachable!()
                };
                if v > 0 {
                    Some((k.to_string(), v * 3 + 2))
                } else {
                    None
                }
            })
            .collect();
        for fuse in [false, true] {
            for bound in [&sql, &graph] {
                let kernel = Kernel::new(KernelOptions::default()).unwrap();
                let capture = SharedCapture::new();
                let stats = kernel
                    .run(JobRequest::new(
                        physicalize(bound, &PlanOptions { fuse }),
                        rows.clone(),
                        capture.clone(),
                    ))
                    .unwrap();
                let got = normalize(capture.rows());
                let first = got.iter().zip(&expected).position(|(a, b)| a != b);
                assert_eq!(
                    got, expected,
                    "seed={seed} fuse={fuse} first_output_mismatch={first:?}"
                );
                assert_eq!(stats.ingested_rows, 512);
                assert_eq!(kernel.live_tasks(), 0);
                assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
            }
        }
    }
}

#[test]
fn production_count_windows_match_independent_per_key_accumulator() {
    let bound=sparrow_sql::bind_sql("SELECT device_id, SUM(v) AS s FROM sensors WHERE v > 0 GROUP BY device_id, COUNT_WINDOW(4)",
        &catalog(),PipelineId::new(1),RevisionId::new(1)).unwrap();
    for seed in [1, 7, 0xcafe, 0x5eed] {
        let rows = inputs(seed, 1024);
        let mut state: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        let mut expected = Vec::new();
        for row in &rows {
            let Scalar::Utf8(key) = &row.values[0] else {
                unreachable!()
            };
            let Scalar::Int64(value) = row.values[1] else {
                unreachable!()
            };
            if value <= 0 {
                continue;
            }
            let window = state.entry(key.to_string()).or_default();
            window.push(value);
            if window.len() == 4 {
                expected.push((key.to_string(), window.iter().sum::<i64>()));
                window.clear();
            }
        }
        expected.sort();
        for fuse in [false, true] {
            let kernel = Kernel::new(KernelOptions::default()).unwrap();
            let capture = SharedCapture::new();
            let stats = kernel
                .run(JobRequest::new(
                    physicalize(&bound, &PlanOptions { fuse }),
                    rows.clone(),
                    capture.clone(),
                ))
                .unwrap();
            let mut got = normalize(capture.rows());
            got.sort();
            assert_eq!(got, expected, "seed={seed} fuse={fuse}");
            assert_eq!(stats.ingested_rows, 1024);
            assert_eq!(
                kernel.metrics.snapshot().state_keys as usize,
                state.values().filter(|values| !values.is_empty()).count(),
                "state key count must not count memory/scratch lease handles"
            );
            assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        }
    }
}

#[test]
fn production_event_time_boundaries_have_independent_final_results() {
    use sparrow_model::{AggFn, WindowKind};
    use sparrow_plan::{bind_window_linear, AggCall, WindowSpec};
    let mut spec = WindowSpec::new(
        WindowKind::tumbling_et(10).unwrap(),
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Sum,
            Some(Expr::Column { name: "v".into() }),
            "s",
        )],
    );
    spec.event_time_field = Some("ts".into());
    let bound = bind_window_linear(
        PipelineId::new(1),
        RevisionId::new(1),
        "sensors".into(),
        schema(),
        None,
        spec,
        "out".into(),
    )
    .unwrap();
    let rows = vec![("a", 1, 1), ("b", 5, 2), ("a", 2, 4), ("a", 3, 11)]
        .into_iter()
        .map(|(k, v, ts)| Row {
            values: vec![Scalar::utf8(k), Scalar::Int64(v), Scalar::Int64(ts)],
        })
        .collect();
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    let capture = SharedCapture::new();
    kernel
        .run(
            JobRequest::new(
                physicalize(&bound, &PlanOptions::default()),
                rows,
                capture.clone(),
            )
            .with_controls(vec![StreamControl::Watermark {
                input: 0,
                wm_micros: 20,
            }]),
        )
        .unwrap();
    let mut actual: Vec<_> = capture
        .rows()
        .into_iter()
        .map(|r| {
            let Scalar::Utf8(k) = &r[0] else {
                panic!("key")
            };
            let (Scalar::Int64(start), Scalar::Int64(end), Scalar::Int64(sum)) =
                (&r[1], &r[2], &r[3])
            else {
                panic!("window {r:?}")
            };
            (k.to_string(), *start, *end, *sum)
        })
        .collect();
    actual.sort();
    assert_eq!(
        actual,
        vec![
            ("a".into(), 0, 10, 3),
            ("a".into(), 10, 20, 3),
            ("b".into(), 0, 10, 5)
        ]
    );
}

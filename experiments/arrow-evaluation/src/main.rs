//! G1a microbench: same schema / expression suite on RowBatch vs Arrow.
//!
//! DataFusion is feature-gated (`--features datafusion`) and never enters
//! default runtime dependencies.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::compute::{filter, kernels::cmp::gt};
use sparrow_expr::{eval, BinaryOp, Expr};
use sparrow_model::{
    CreditKind, DataType, DynamicValue, Field, FieldId, MemoryOwner, ResourceBudget, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};

const WARMUP: u32 = 50;
const ITERS_LAT: u32 = 2_000;
const ITERS_BATCH: u32 = 400;
const ITERS_JSON: u32 = 400;
const ITERS_FANOUT: u32 = 200;

fn main() {
    let report = run_suite();
    println!("{report}");
}

pub fn run_suite() -> String {
    let mut out = String::new();
    out.push_str("# G1a raw measurements (this host)\n\n");
    out.push_str(&format!(
        "host_iters: latency={ITERS_LAT} batch={ITERS_BATCH} json={ITERS_JSON} fanout={ITERS_FANOUT}\n\n"
    ));

    let s1 = bench_single_event();
    let s2 = bench_small_batch();
    let s3 = bench_json_extract();
    let s4 = bench_fanout();

    out.push_str("| scenario | RowBatch ns/op | Arrow ns/op | ratio Arrow/RowBatch |\n");
    out.push_str("|---|---:|---:|---:|\n");
    for (name, rb, ar) in [s1, s2, s3, s4] {
        let ratio = ar / rb;
        out.push_str(&format!(
            "| {name} | {rb:.1} | {ar:.1} | {ratio:.2} |\n"
        ));
    }
    out.push_str(
        "\nDataFusion snippet: documented in src/datafusion_snippet.rs; not a Cargo dep.\n",
    );
    out
}

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Int64, false),
            Field::new(FieldId::new(2), "temp", DataType::Float64, false),
            Field::new(FieldId::new(3), "name", DataType::Utf8, false),
            Field::new(FieldId::new(4), "payload", DataType::Dynamic, true),
        ],
    )
    .unwrap()
}

fn make_rows(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let temp = 15.0 + (i as f64) * 0.25;
            Row {
                values: vec![
                    Scalar::Int64(i as i64),
                    Scalar::Float64(temp),
                    Scalar::utf8(format!("dev-{i}")),
                    Scalar::Dynamic(DynamicValue::object(vec![
                        ("temp", DynamicValue::Float64(temp)),
                        ("ok", DynamicValue::Bool(temp > 25.0)),
                    ])),
                ],
            }
        })
        .collect()
}

fn build_batch(rows: Vec<Row>) -> (Arc<MemoryOwner>, RowBatch) {
    let owner = MemoryOwner::new(ResourceBudget::performance());
    let mut b = RowBatchBuilder::new(
        Arc::new(schema()),
        Arc::clone(&owner),
        CreditKind::Reservation,
        rows.len().max(1),
        8 * 1024 * 1024,
    )
    .unwrap();
    for r in rows {
        b.push(r).unwrap();
    }
    (owner, b.finish().unwrap())
}

fn rowbatch_filter_project(batch: &RowBatch) -> usize {
    let pred = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column {
            name: "temp".into(),
        }),
        right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
    };
    let proj = Expr::Column {
        name: "temp".into(),
    };
    let schema = batch.schema();
    let mut kept = 0usize;
    for row in batch.rows() {
        match eval(&pred, schema, &row.values).unwrap() {
            Scalar::Bool(true) => {
                let _ = eval(&proj, schema, &row.values).unwrap();
                kept += 1;
            }
            _ => {}
        }
    }
    kept
}

fn arrow_arrays(n: usize) -> (Float64Array, StringArray) {
    let temps: Vec<f64> = (0..n).map(|i| 15.0 + (i as f64) * 0.25).collect();
    let names: Vec<String> = (0..n).map(|i| format!("dev-{i}")).collect();
    (
        Float64Array::from(temps),
        StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
    )
}

fn arrow_filter_project(temps: &Float64Array) -> usize {
    let threshold = Float64Array::from(vec![25.0; temps.len()]);
    let mask = gt(temps, &threshold).unwrap();
    let filtered = filter(temps, &mask).unwrap();
    filtered.len()
}

fn ns_per_op(iters: u32, mut f: impl FnMut()) -> f64 {
    for _ in 0..WARMUP {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_nanos() as f64 / f64::from(iters)
}

fn bench_single_event() -> (&'static str, f64, f64) {
    let (_o, batch) = build_batch(make_rows(1));
    let (temps, _names) = arrow_arrays(1);
    let rb = ns_per_op(ITERS_LAT, || {
        let _ = rowbatch_filter_project(&batch);
    });
    let ar = ns_per_op(ITERS_LAT, || {
        let _ = arrow_filter_project(&temps);
    });
    ("single-event filter/project", rb, ar)
}

fn bench_small_batch() -> (&'static str, f64, f64) {
    let (_o, batch) = build_batch(make_rows(128));
    let (temps, _names) = arrow_arrays(128);
    let rb = ns_per_op(ITERS_BATCH, || {
        let _ = rowbatch_filter_project(&batch);
    });
    let ar = ns_per_op(ITERS_BATCH, || {
        let _ = arrow_filter_project(&temps);
    });
    ("small-batch 128 numeric filter/project", rb, ar)
}

fn bench_json_extract() -> (&'static str, f64, f64) {
    let (_o, batch) = build_batch(make_rows(64));
    let extract = Expr::DynamicGet {
        expr: Box::new(Expr::Column {
            name: "payload".into(),
        }),
        key: "temp".into(),
    };
    let schema = batch.schema().clone();
    let json_lines: Vec<String> = (0..64)
        .map(|i| format!("{{\"temp\":{:.2},\"ok\":true}}", 15.0 + i as f64 * 0.25))
        .collect();
    let json_arr = StringArray::from(json_lines.iter().map(|s| s.as_str()).collect::<Vec<_>>());

    let rb = ns_per_op(ITERS_JSON, || {
        for row in batch.rows() {
            let _ = eval(&extract, &schema, &row.values).unwrap();
        }
    });
    let ar = ns_per_op(ITERS_JSON, || {
        let mut sum = 0.0f64;
        for i in 0..json_arr.len() {
            let s = json_arr.value(i);
            if let Some(v) = tiny_json_temp(s) {
                sum += v;
            }
        }
        std::hint::black_box(sum);
    });
    ("json/dynamic extract (64 rows)", rb, ar)
}

/// Minimal extract of `"temp":<number>` — experiments only, not a codec.
fn tiny_json_temp(s: &str) -> Option<f64> {
    let key = "\"temp\":";
    let idx = s.find(key)?;
    let rest = &s[idx + key.len()..];
    let end = rest
        .find(|c: char| c == ',' || c == '}')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

fn bench_fanout() -> (&'static str, f64, f64) {
    let (_o, batch) = build_batch(make_rows(32));
    let rules: Vec<Expr> = (0..32)
        .map(|i| Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temp".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(10.0 + i as f64))),
        })
        .collect();
    let schema = batch.schema().clone();
    let (temps, _) = arrow_arrays(32);
    let thresholds: Vec<Float64Array> = (0..32)
        .map(|i| Float64Array::from(vec![10.0 + i as f64; 32]))
        .collect();

    let rb = ns_per_op(ITERS_FANOUT, || {
        let shared = batch.share();
        let mut hits = 0usize;
        for rule in &rules {
            for row in shared.rows() {
                if matches!(eval(rule, &schema, &row.values).unwrap(), Scalar::Bool(true)) {
                    hits += 1;
                }
            }
        }
        std::hint::black_box(hits);
    });
    let ar = ns_per_op(ITERS_FANOUT, || {
        let mut hits = 0usize;
        for thr in &thresholds {
            let mask = gt(&temps, thr).unwrap();
            hits += mask.true_count();
        }
        std::hint::black_box(hits);
    });
    ("fan-out 32 rules x 32 rows (share)", rb, ar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn g1a_suite_runs() {
        let report = run_suite();
        assert!(report.contains("RowBatch"));
        assert!(report.contains("single-event"));
    }
}

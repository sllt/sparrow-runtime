//! RowBatch-only G1a candidate timings (provisional layout).

use std::sync::Arc;
use std::time::Instant;

use sparrow_expr::{eval, BinaryOp, Expr};
use sparrow_model::{
    CreditKind, DataType, Field, FieldId, MemoryOwner, ResourceBudget, Row, RowBatchBuilder,
    Scalar, Schema, SchemaId,
};

fn main() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let schema = Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "x", DataType::Int64, false),
            Field::new(FieldId::new(2), "y", DataType::Float64, false),
        ],
    )
    .unwrap();
    let mut b = RowBatchBuilder::new(
        Arc::new(schema.clone()),
        Arc::clone(&owner),
        CreditKind::Reservation,
        64,
        64 * 1024,
    )
    .unwrap();
    for i in 0..64 {
        b.push(Row {
            values: vec![Scalar::Int64(i), Scalar::Float64(i as f64 * 0.5)],
        })
        .unwrap();
    }
    let batch = b.finish().unwrap();
    let pred = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column { name: "x".into() }),
        right: Box::new(Expr::Literal(Scalar::Int64(10))),
    };

    let start = Instant::now();
    let mut kept = 0usize;
    for _ in 0..1_000 {
        for row in batch.rows() {
            if matches!(eval(&pred, &schema, &row.values).unwrap(), Scalar::Bool(true)) {
                kept += 1;
            }
        }
    }
    println!(
        "layout-rowbatch: 1000 x 64-row filter kept={kept} elapsed_ns={} peak_builder={} physical={}",
        start.elapsed().as_nanos(),
        owner.peak_builder_bytes(),
        owner.usage().physical_bytes
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_links() {
        assert!(true);
    }
}

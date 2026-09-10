//! Tight numeric loops for the common `col ▷ lit` filter. Layout stays
//! RowBatch (ADR-003); this is a kernel, not a second engine.

use crate::{bind, eval_bound, BinaryOp, Expr};
use sparrow_model::{Result, Row, Scalar, Schema};

/// Predicate that can run without walking the full expression tree per cell.
#[derive(Clone, Debug, PartialEq)]
pub enum SimplePred {
    CmpF64 {
        column: String,
        op: BinaryOp,
        thr: f64,
    },
}

impl SimplePred {
    pub fn op(&self) -> BinaryOp {
        match self {
            Self::CmpF64 { op, .. } => *op,
        }
    }

    pub fn from_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Binary {
                op,
                left,
                right,
            } if matches!(
                op,
                BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Eq | BinaryOp::NotEq
            ) =>
            {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column { name }, Expr::Literal(lit)) => Some(Self::CmpF64 {
                        column: name.clone(),
                        op: *op,
                        thr: lit.as_f64()?,
                    }),
                    (Expr::Literal(lit), Expr::Column { name }) => {
                        let flipped = match op {
                            BinaryOp::Gt => BinaryOp::Lt,
                            BinaryOp::Gte => BinaryOp::Lte,
                            BinaryOp::Lt => BinaryOp::Gt,
                            BinaryOp::Lte => BinaryOp::Gte,
                            other => *other,
                        };
                        Some(Self::CmpF64 {
                            column: name.clone(),
                            op: flipped,
                            thr: lit.as_f64()?,
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// Keep-mask for a simple numeric compare. Falls back to `eval_bound` if the
/// column is missing or non-numeric (NULL → drop, matching filter semantics).
/// Column names are resolved once (P2-30), not per row.
pub fn filter_mask(pred: &Expr, schema: &Schema, rows: &[Row]) -> Result<Vec<bool>> {
    let bound = bind(pred, schema)?;
    if let Some(simple) = SimplePred::from_expr(pred) {
        if matches!(simple.op(), BinaryOp::Eq | BinaryOp::NotEq) {
            // Integer equality must match evaluator semantics, not f64.
            return rows
                .iter()
                .map(|row| match eval_bound(&bound, &row.values)? {
                    Scalar::Bool(v) => Ok(v),
                    Scalar::Null => Ok(false),
                    other => Err(sparrow_model::SparrowError::new(
                        sparrow_model::ErrorCode::TypeMismatch,
                        format!("filter must be bool, got {}", other.data_type()),
                    )),
                })
                .collect();
        }
        let SimplePred::CmpF64 { column, op, thr } = &simple;
        if let Some(idx) = schema.index_of_name(column) {
            let numeric = schema
                .fields
                .get(idx)
                .map(|f| {
                    matches!(
                        f.data_type,
                        sparrow_model::DataType::Float64
                            | sparrow_model::DataType::Int64
                            | sparrow_model::DataType::UInt64
                    )
                })
                .unwrap_or(false);
            if numeric {
                return rows
                    .iter()
                    .map(|row| match row.values.get(idx) {
                        Some(Scalar::Float64(v)) => Ok(cmp_f64(*op, *v, *thr)),
                        Some(Scalar::Int64(_)) | Some(Scalar::UInt64(_)) => {
                            match eval_bound(&bound, &row.values)? {
                                Scalar::Bool(v) => Ok(v),
                                Scalar::Null => Ok(false),
                                other => Err(sparrow_model::SparrowError::new(
                                    sparrow_model::ErrorCode::TypeMismatch,
                                    format!("filter must be bool, got {}", other.data_type()),
                                )),
                            }
                        }
                        Some(Scalar::Null) | None => Ok(false),
                        Some(_) => Err(sparrow_model::SparrowError::new(
                            sparrow_model::ErrorCode::TypeMismatch,
                            "filter compare fast-path refuses non-numeric cells (must match eval)",
                        )),
                    })
                    .collect();
            }
        }
    }
    rows.iter()
        .map(|row| match eval_bound(&bound, &row.values)? {
            Scalar::Bool(v) => Ok(v),
            Scalar::Null => Ok(false),
            other => Err(sparrow_model::SparrowError::new(
                sparrow_model::ErrorCode::TypeMismatch,
                format!("filter must be bool, got {}", other.data_type()),
            )),
        })
        .collect()
}

fn cmp_f64(op: BinaryOp, left: f64, right: f64) -> bool {
    match op {
        BinaryOp::Gt => left > right,
        BinaryOp::Gte => left >= right,
        BinaryOp::Lt => left < right,
        BinaryOp::Lte => left <= right,
        BinaryOp::Eq => left == right,
        BinaryOp::NotEq => left != right,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval;
    use sparrow_model::{DataType, Field, FieldId, SchemaId};

    #[test]
    fn stride_matches_eval_for_gt() {
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "temp", DataType::Float64, true)],
        )
        .unwrap();
        let rows = vec![
            Row {
                values: vec![Scalar::Float64(10.0)],
            },
            Row {
                values: vec![Scalar::Float64(30.0)],
            },
            Row {
                values: vec![Scalar::Null],
            },
        ];
        let pred = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temp".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
        };
        assert_eq!(
            filter_mask(&pred, &schema, &rows).unwrap(),
            vec![false, true, false]
        );
        assert!(SimplePred::from_expr(&pred).is_some());
    }

    #[test]
    fn r06_fast_filter_eq_matches_evaluator_for_large_ints() {
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "id", DataType::Int64, false)],
        )
        .unwrap();
        let big = 9_007_199_254_740_993i64;
        let rows = vec![
            Row {
                values: vec![Scalar::Int64(big)],
            },
            Row {
                values: vec![Scalar::Int64(big + 1)],
            },
        ];
        let pred = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column { name: "id".into() }),
            right: Box::new(Expr::Literal(Scalar::Int64(big))),
        };
        let mask = filter_mask(&pred, &schema, &rows).unwrap();
        let evaled: Vec<bool> = rows
            .iter()
            .map(|r| match eval(&pred, &schema, &r.values).unwrap() {
                Scalar::Bool(v) => v,
                _ => false,
            })
            .collect();
        assert_eq!(mask, evaled);
        assert_eq!(mask, vec![true, false]);
    }

    #[test]
    fn p0_6_filter_mask_matches_eval_on_utf8() {
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "name", DataType::Utf8, false)],
        )
        .unwrap();
        let rows = vec![
            Row {
                values: vec![Scalar::utf8("ok")],
            },
            Row {
                values: vec![Scalar::utf8("no")],
            },
        ];
        let pred = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column {
                name: "name".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::utf8("ok"))),
        };
        let mask = filter_mask(&pred, &schema, &rows).unwrap();
        let evaled: Vec<bool> = rows
            .iter()
            .map(|r| match eval(&pred, &schema, &r.values).unwrap() {
                Scalar::Bool(v) => v,
                _ => false,
            })
            .collect();
        assert_eq!(mask, evaled);
        assert_eq!(mask, vec![true, false]);

        let bad = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "name".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(1.0))),
        };
        let mask_err = filter_mask(&bad, &schema, &rows).unwrap_err();
        let eval_err = eval(&bad, &schema, &rows[0].values).unwrap_err();
        assert_eq!(mask_err.code, eval_err.code);
        assert_eq!(mask_err.code, sparrow_model::ErrorCode::TypeMismatch);
    }
}

//! Bind-time column resolution (P2-30).
//!
//! `Expr::Column` carries a name. Resolving that name against the schema on
//! every row is a linear scan. [`bind`] does the lookup once; [`eval_bound`]
//! indexes the row and never touches field names.

use sparrow_model::error::ErrorCode;
use sparrow_model::{DataType, DynamicValue, Result, Scalar, Schema, SparrowError};

use crate::{cast, check_call_arity, eval_binary, eval_call_values, BinaryOp, Expr};

/// Expression tree with column names replaced by schema indices.
#[derive(Clone, Debug, PartialEq)]
pub enum BoundExpr {
    Column { index: usize },
    Literal(Scalar),
    Cast {
        expr: Box<BoundExpr>,
        target: DataType,
    },
    TryCast {
        expr: Box<BoundExpr>,
        target: DataType,
    },
    Binary {
        op: BinaryOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    IsNull(Box<BoundExpr>),
    IsNotNull(Box<BoundExpr>),
    Not(Box<BoundExpr>),
    Call {
        name: String,
        args: Vec<BoundExpr>,
    },
    DynamicGet {
        expr: Box<BoundExpr>,
        key: String,
    },
}

/// Resolve every [`Expr::Column`] against `schema`. Unknown names fail closed.
pub fn bind(expr: &Expr, schema: &Schema) -> Result<BoundExpr> {
    match expr {
        Expr::Column { name } => {
            let index = schema.index_of_name(name).ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidArgument, format!("unknown column '{name}'"))
            })?;
            Ok(BoundExpr::Column { index })
        }
        Expr::Literal(s) => Ok(BoundExpr::Literal(s.clone())),
        Expr::Cast { expr, target } => Ok(BoundExpr::Cast {
            expr: Box::new(bind(expr, schema)?),
            target: target.clone(),
        }),
        Expr::TryCast { expr, target } => Ok(BoundExpr::TryCast {
            expr: Box::new(bind(expr, schema)?),
            target: target.clone(),
        }),
        Expr::Binary { op, left, right } => Ok(BoundExpr::Binary {
            op: *op,
            left: Box::new(bind(left, schema)?),
            right: Box::new(bind(right, schema)?),
        }),
        Expr::IsNull(inner) => Ok(BoundExpr::IsNull(Box::new(bind(inner, schema)?))),
        Expr::IsNotNull(inner) => Ok(BoundExpr::IsNotNull(Box::new(bind(inner, schema)?))),
        Expr::Not(inner) => Ok(BoundExpr::Not(Box::new(bind(inner, schema)?))),
        Expr::Call { name, args } => {
            let args = args
                .iter()
                .map(|a| bind(a, schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(BoundExpr::Call {
                name: name.clone(),
                args,
            })
        }
        Expr::DynamicGet { expr, key } => Ok(BoundExpr::DynamicGet {
            expr: Box::new(bind(expr, schema)?),
            key: key.clone(),
        }),
    }
}

/// Evaluate a bind-time tree. Column access is an index; no schema scan.
pub fn eval_bound(expr: &BoundExpr, row: &[Scalar]) -> Result<Scalar> {
    match expr {
        BoundExpr::Column { index } => Ok(row.get(*index).cloned().unwrap_or(Scalar::Null)),
        BoundExpr::Literal(s) => Ok(s.clone()),
        BoundExpr::Cast { expr, target } => cast(&eval_bound(expr, row)?, target, false),
        BoundExpr::TryCast { expr, target } => cast(&eval_bound(expr, row)?, target, true),
        BoundExpr::Binary { op, left, right } => {
            let l = eval_bound(left, row)?;
            let r = eval_bound(right, row)?;
            eval_binary(*op, &l, &r)
        }
        BoundExpr::IsNull(inner) => Ok(Scalar::Bool(eval_bound(inner, row)?.is_null())),
        BoundExpr::IsNotNull(inner) => Ok(Scalar::Bool(!eval_bound(inner, row)?.is_null())),
        BoundExpr::Not(inner) => match eval_bound(inner, row)? {
            Scalar::Bool(v) => Ok(Scalar::Bool(!v)),
            Scalar::Null => Ok(Scalar::Null),
            other => Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!("NOT expects bool, got {}", other.data_type()),
            )),
        },
        BoundExpr::Call { name, args } => {
            check_call_arity(name, args.len())?;
            let vals: Result<Vec<Scalar>> = args.iter().map(|a| eval_bound(a, row)).collect();
            eval_call_values(name, vals?)
        }
        BoundExpr::DynamicGet { expr, key } => match eval_bound(expr, row)? {
            Scalar::Dynamic(dynv) => match dynv.get(key) {
                Some(DynamicValue::Null) | None => Ok(Scalar::Null),
                Some(DynamicValue::Bool(v)) => Ok(Scalar::Bool(*v)),
                Some(DynamicValue::Int64(v)) => Ok(Scalar::Int64(*v)),
                Some(DynamicValue::UInt64(v)) => Ok(Scalar::UInt64(*v)),
                Some(DynamicValue::Float64(v)) => Ok(Scalar::Float64(*v)),
                Some(DynamicValue::Utf8(v)) => Ok(Scalar::Utf8(v.clone())),
                Some(DynamicValue::Bytes(v)) => Ok(Scalar::Bytes(v.clone())),
                Some(other) => Ok(Scalar::Dynamic(other.clone())),
            },
            Scalar::Null => Ok(Scalar::Null),
            other => Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!("dynamic extract requires Dynamic, got {}", other.data_type()),
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{eval, BinaryOp, Expr};
    use sparrow_model::{Field, FieldId, SchemaId};

    fn wide_schema() -> Schema {
        let fields = (0..32)
            .map(|i| {
                Field::new(
                    FieldId::new(i as u16 + 1),
                    format!("c{i}"),
                    DataType::Int64,
                    false,
                )
            })
            .collect();
        Schema::new(SchemaId::new(1), fields).unwrap()
    }

    #[test]
    fn p2_30_bind_resolves_column_index() {
        let schema = wide_schema();
        let expr = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "c31".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Int64(10))),
        };
        let bound = bind(&expr, &schema).unwrap();
        match &bound {
            BoundExpr::Binary { left, right, .. } => {
                assert_eq!(left.as_ref(), &BoundExpr::Column { index: 31 });
                assert_eq!(right.as_ref(), &BoundExpr::Literal(Scalar::Int64(10)));
            }
            other => panic!("expected bound binary, got {other:?}"),
        }
    }

    #[test]
    fn p2_30_eval_bound_matches_eval_without_schema() {
        let schema = wide_schema();
        let mut row = vec![Scalar::Int64(0); 32];
        row[31] = Scalar::Int64(40);
        let expr = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "c31".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Int64(10))),
        };
        let bound = bind(&expr, &schema).unwrap();
        let via_bound = eval_bound(&bound, &row).unwrap();
        let via_eval = eval(&expr, &schema, &row).unwrap();
        assert_eq!(via_bound, via_eval);
        assert_eq!(via_bound, Scalar::Bool(true));
        // Bound column is an index: a renamed schema cannot be consulted.
        let bound_col = BoundExpr::Column { index: 31 };
        assert_eq!(eval_bound(&bound_col, &row).unwrap(), Scalar::Int64(40));
    }

    #[test]
    fn p2_30_bind_unknown_column_fails() {
        let schema = wide_schema();
        let err = bind(
            &Expr::Column {
                name: "missing".into(),
            },
            &schema,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.to_string().contains("missing"));
    }
}

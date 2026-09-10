//! Expression IR shared by SQL and Graph.
//!
//! M0 ships a small scalar evaluator so filter/project stubs and G1a
//! kernels have one semantics. Nested-type kernels are out of scope.

use sparrow_model::{DataType, DynamicValue, Result, Scalar, Schema, SparrowError};
use sparrow_model::error::ErrorCode;

pub mod infer;
pub mod kernels;
pub use infer::infer_type;
pub use kernels::{filter_mask, SimplePred};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

impl BinaryOp {
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Sub => "sub",
            Self::Mul => "mul",
            Self::Div => "div",
            Self::Eq => "eq",
            Self::NotEq => "neq",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::And => "and",
            Self::Or => "or",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column { name: String },
    Literal(Scalar),
    Cast {
        expr: Box<Expr>,
        target: DataType,
    },
    TryCast {
        expr: Box<Expr>,
        target: DataType,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Not(Box<Expr>),
    /// Limited builtin placeholder: abs / lower / upper / length / coalesce.
    Call {
        name: String,
        args: Vec<Expr>,
    },
    /// Extract a key from a Dynamic object (`payload.temp`).
    DynamicGet {
        expr: Box<Expr>,
        key: String,
    },
}

pub fn eval(expr: &Expr, schema: &Schema, row: &[Scalar]) -> Result<Scalar> {
    match expr {
        Expr::Column { name } => {
            let idx = schema.index_of_name(name).ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidArgument, format!("unknown column '{name}'"))
            })?;
            Ok(row.get(idx).cloned().unwrap_or(Scalar::Null))
        }
        Expr::Literal(s) => Ok(s.clone()),
        Expr::Cast { expr, target } => cast(&eval(expr, schema, row)?, target, false),
        Expr::TryCast { expr, target } => cast(&eval(expr, schema, row)?, target, true),
        Expr::Binary { op, left, right } => {
            let l = eval(left, schema, row)?;
            let r = eval(right, schema, row)?;
            eval_binary(*op, &l, &r)
        }
        Expr::IsNull(inner) => Ok(Scalar::Bool(eval(inner, schema, row)?.is_null())),
        Expr::IsNotNull(inner) => Ok(Scalar::Bool(!eval(inner, schema, row)?.is_null())),
        Expr::Not(inner) => match eval(inner, schema, row)? {
            Scalar::Bool(v) => Ok(Scalar::Bool(!v)),
            Scalar::Null => Ok(Scalar::Null),
            other => Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!("NOT expects bool, got {}", other.data_type()),
            )),
        },
        Expr::Call { name, args } => eval_call(name, args, schema, row),
        Expr::DynamicGet { expr, key } => match eval(expr, schema, row)? {
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

fn eval_call(name: &str, args: &[Expr], schema: &Schema, row: &[Scalar]) -> Result<Scalar> {
    check_call_arity(name, args.len())?;
    let vals: Result<Vec<Scalar>> = args.iter().map(|a| eval(a, schema, row)).collect();
    let vals = vals?;
    match name.to_ascii_lowercase().as_str() {
        "abs" => match vals.first() {
            Some(Scalar::Int64(v)) => {
                let abs = v.checked_abs().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::IntegerOverflow,
                        "abs(Int64::MIN) overflows",
                    )
                })?;
                Ok(Scalar::Int64(abs))
            }
            Some(Scalar::Float64(v)) => Ok(Scalar::Float64(v.abs())),
            Some(Scalar::Null) => Ok(Scalar::Null),
            None => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "abs requires 1 argument",
            )),
            _ => Err(SparrowError::new(ErrorCode::TypeMismatch, "abs expects numeric")),
        },
        "lower" => match vals.first() {
            Some(Scalar::Utf8(s)) => Ok(Scalar::utf8(s.to_ascii_lowercase())),
            Some(Scalar::Null) => Ok(Scalar::Null),
            _ => Err(SparrowError::new(ErrorCode::TypeMismatch, "lower expects utf8")),
        },
        "upper" => match vals.first() {
            Some(Scalar::Utf8(s)) => Ok(Scalar::utf8(s.to_ascii_uppercase())),
            Some(Scalar::Null) => Ok(Scalar::Null),
            _ => Err(SparrowError::new(ErrorCode::TypeMismatch, "upper expects utf8")),
        },
        "length" | "char_length" => match vals.first() {
            Some(Scalar::Utf8(s)) => Ok(Scalar::Int64(s.chars().count() as i64)),
            Some(Scalar::Null) => Ok(Scalar::Null),
            _ => Err(SparrowError::new(ErrorCode::TypeMismatch, "length expects utf8")),
        },
        "coalesce" => {
            if vals.is_empty() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "coalesce requires at least 1 argument",
                ));
            }
            Ok(vals.into_iter().find(|v| !v.is_null()).unwrap_or(Scalar::Null))
        }
        "nullif" => {
            if vals.len() != 2 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "nullif requires 2 arguments",
                ));
            }
            if scalars_eq(&vals[0], &vals[1]) {
                Ok(Scalar::Null)
            } else {
                Ok(vals[0].clone())
            }
        }
        "greatest" | "least" => {
            if vals.is_empty() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("{name} requires at least 1 argument"),
                ));
            }
            let mut acc: Option<Scalar> = None;
            for v in vals {
                if v.is_null() {
                    continue;
                }
                acc = Some(match acc {
                    None => v,
                    Some(prev) => {
                        let ord = cmp_scalars(&prev, &v)?;
                        if name.eq_ignore_ascii_case("greatest") {
                            if ord == std::cmp::Ordering::Less {
                                v
                            } else {
                                prev
                            }
                        } else if ord == std::cmp::Ordering::Greater {
                            v
                        } else {
                            prev
                        }
                    }
                });
            }
            Ok(acc.unwrap_or(Scalar::Null))
        }
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("unknown function '{other}'"),
        )),
    }
}

fn check_call_arity(name: &str, argc: usize) -> Result<()> {
    match name.to_ascii_lowercase().as_str() {
        "abs" | "lower" | "upper" | "length" | "char_length" => {
            if argc != 1 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("{name} requires 1 argument, got {argc}"),
                ));
            }
        }
        "coalesce" => {
            if argc == 0 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "coalesce requires at least 1 argument",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn eval_int_arith(op: BinaryOp, left: i64, right: i64) -> Result<Scalar> {
    let v = match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Sub => left.checked_sub(right),
        BinaryOp::Mul => left.checked_mul(right),
        BinaryOp::Div => {
            if right == 0 {
                return Ok(Scalar::Null);
            }
            left.checked_div(right)
        }
        _ => None,
    };
    v.map(Scalar::Int64).ok_or_else(|| {
        SparrowError::new(ErrorCode::IntegerOverflow, "checked integer arithmetic overflow")
    })
}

fn eval_binary(op: BinaryOp, left: &Scalar, right: &Scalar) -> Result<Scalar> {
    if matches!(op, BinaryOp::And | BinaryOp::Or) {
        return eval_logic(op, left, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Scalar::Null);
    }
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if matches!(left, Scalar::Dynamic(_)) || matches!(right, Scalar::Dynamic(_)) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "dynamic arithmetic requires an explicit CAST/TRY_CAST",
                ));
            }
            match (left, right) {
                (Scalar::Int64(l), Scalar::Int64(r)) => eval_int_arith(op, *l, *r),
                (Scalar::UInt64(l), Scalar::UInt64(r)) => {
                    let v = match op {
                        BinaryOp::Add => l.checked_add(*r),
                        BinaryOp::Sub => l.checked_sub(*r),
                        BinaryOp::Mul => l.checked_mul(*r),
                        BinaryOp::Div => {
                            if *r == 0 {
                                return Ok(Scalar::Null);
                            }
                            l.checked_div(*r)
                        }
                        _ => None,
                    };
                    v.map(Scalar::UInt64).ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::IntegerOverflow,
                            "checked unsigned integer arithmetic overflow",
                        )
                    })
                }
                (Scalar::Int64(l), Scalar::UInt64(r)) if *r <= i64::MAX as u64 => {
                    eval_int_arith(op, *l, *r as i64)
                }
                (Scalar::UInt64(l), Scalar::Int64(r)) if *l <= i64::MAX as u64 => {
                    eval_int_arith(op, *l as i64, *r)
                }
                _ => {
                    let l = left.as_f64().ok_or_else(|| {
                        SparrowError::new(ErrorCode::TypeMismatch, "arithmetic expects numeric")
                    })?;
                    let r = right.as_f64().ok_or_else(|| {
                        SparrowError::new(ErrorCode::TypeMismatch, "arithmetic expects numeric")
                    })?;
                    if !matches!(left, Scalar::Float64(_)) && !matches!(right, Scalar::Float64(_)) {
                        return Err(SparrowError::new(
                            ErrorCode::TypeMismatch,
                            "integer arithmetic must not go through f64",
                        ));
                    }
                    let v = match op {
                        BinaryOp::Add => l + r,
                        BinaryOp::Sub => l - r,
                        BinaryOp::Mul => l * r,
                        BinaryOp::Div => {
                            if r == 0.0 {
                                return Ok(Scalar::Null);
                            }
                            l / r
                        }
                        _ => unreachable!(),
                    };
                    Ok(Scalar::Float64(v))
                }
            }
        }
        BinaryOp::Eq => Ok(Scalar::Bool(scalars_eq(left, right))),
        BinaryOp::NotEq => Ok(Scalar::Bool(!scalars_eq(left, right))),
        cmp => Ok(Scalar::Bool(cmp_ord_op(cmp, cmp_scalars(left, right)?)?)),
    }
}

fn eval_logic(op: BinaryOp, left: &Scalar, right: &Scalar) -> Result<Scalar> {
    let lb = match left {
        Scalar::Bool(v) => Some(*v),
        Scalar::Null => None,
        _ => {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "logical op expects bool",
            ))
        }
    };
    let rb = match right {
        Scalar::Bool(v) => Some(*v),
        Scalar::Null => None,
        _ => {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "logical op expects bool",
            ))
        }
    };
    Ok(match (op, lb, rb) {
        (BinaryOp::And, Some(false), _) | (BinaryOp::And, _, Some(false)) => Scalar::Bool(false),
        (BinaryOp::And, Some(true), Some(true)) => Scalar::Bool(true),
        (BinaryOp::Or, Some(true), _) | (BinaryOp::Or, _, Some(true)) => Scalar::Bool(true),
        (BinaryOp::Or, Some(false), Some(false)) => Scalar::Bool(false),
        _ => Scalar::Null,
    })
}

fn cast(value: &Scalar, target: &DataType, try_cast: bool) -> Result<Scalar> {
    if value.is_null() {
        return Ok(Scalar::Null);
    }
    let fail = |msg: String| {
        if try_cast {
            Ok(Scalar::Null)
        } else {
            Err(SparrowError::new(ErrorCode::TypeMismatch, msg))
        }
    };
    match (value, target) {
        (Scalar::Int64(v), DataType::Int64) => Ok(Scalar::Int64(*v)),
        (Scalar::Int64(v), DataType::UInt64) if *v >= 0 => Ok(Scalar::UInt64(*v as u64)),
        (Scalar::Int64(v), DataType::Float64) => Ok(Scalar::Float64(*v as f64)),
        (Scalar::Int64(v), DataType::Utf8) => Ok(Scalar::utf8(v.to_string())),
        (Scalar::UInt64(v), DataType::UInt64) => Ok(Scalar::UInt64(*v)),
        (Scalar::UInt64(v), DataType::Int64) if *v <= i64::MAX as u64 => Ok(Scalar::Int64(*v as i64)),
        (Scalar::UInt64(v), DataType::Float64) => Ok(Scalar::Float64(*v as f64)),
        (Scalar::Float64(v), DataType::Float64) => Ok(Scalar::Float64(*v)),
        (Scalar::Float64(v), DataType::Int64) => Ok(Scalar::Int64(*v as i64)),
        (Scalar::Utf8(s), DataType::Utf8) => Ok(Scalar::Utf8(s.clone())),
        (Scalar::Utf8(s), DataType::Int64) => match s.parse::<i64>() {
            Ok(v) => Ok(Scalar::Int64(v)),
            Err(_) => fail(format!("cannot CAST utf8 '{s}' to int64")),
        },
        (Scalar::Utf8(s), DataType::Float64) => match s.parse::<f64>() {
            Ok(v) => Ok(Scalar::Float64(v)),
            Err(_) => fail(format!("cannot CAST utf8 '{s}' to float64")),
        },
        (Scalar::Bool(v), DataType::Bool) => Ok(Scalar::Bool(*v)),
        (Scalar::Dynamic(DynamicValue::Int64(v)), DataType::Int64) => Ok(Scalar::Int64(*v)),
        (Scalar::Dynamic(DynamicValue::Float64(v)), DataType::Float64) => Ok(Scalar::Float64(*v)),
        (Scalar::Dynamic(DynamicValue::Utf8(s)), DataType::Utf8) => Ok(Scalar::Utf8(s.clone())),
        (Scalar::Dynamic(d), DataType::Float64) => d
            .as_f64()
            .map(Scalar::Float64)
            .map(Ok)
            .unwrap_or_else(|| fail("cannot CAST dynamic to float64".into())),
        (_, DataType::Dynamic) => Ok(Scalar::Dynamic(dynamic_from_scalar(value))),
        _ => fail(format!("cannot CAST {} to {target}", value.data_type())),
    }
}

fn cmp_ord_op(op: BinaryOp, ord: std::cmp::Ordering) -> Result<bool> {
    Ok(match op {
        BinaryOp::Lt => ord.is_lt(),
        BinaryOp::Lte => ord.is_le(),
        BinaryOp::Gt => ord.is_gt(),
        BinaryOp::Gte => ord.is_ge(),
        _ => {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                "cmp_ord_op on non-compare op",
            ))
        }
    })
}

fn cmp_scalars(left: &Scalar, right: &Scalar) -> Result<std::cmp::Ordering> {
    match (left, right) {
        (Scalar::Int64(a), Scalar::Int64(b)) => Ok(a.cmp(b)),
        (Scalar::UInt64(a), Scalar::UInt64(b)) => Ok(a.cmp(b)),
        (Scalar::Int64(a), Scalar::UInt64(b)) if *a >= 0 => Ok((*a as u64).cmp(b)),
        (Scalar::UInt64(a), Scalar::Int64(b)) if *b >= 0 => Ok(a.cmp(&(*b as u64))),
        (Scalar::TimestampMicrosUTC(a), Scalar::TimestampMicrosUTC(b)) => Ok(a.cmp(b)),
        (Scalar::TimestampMicrosUTC(a), Scalar::Int64(b)) => Ok(a.cmp(b)),
        (Scalar::Int64(a), Scalar::TimestampMicrosUTC(b)) => Ok(a.cmp(b)),
        (Scalar::Float64(a), Scalar::Float64(b)) => a.partial_cmp(b).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "NaN comparison is unordered; no implicit Equal",
            )
        }),
        (Scalar::Utf8(a), Scalar::Utf8(b)) => Ok(a.as_ref().cmp(b.as_ref())),
        _ => {
            if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                if matches!(left, Scalar::Float64(_)) || matches!(right, Scalar::Float64(_)) {
                    return a.partial_cmp(&b).ok_or_else(|| {
                        SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "NaN comparison is unordered; no implicit Equal",
                        )
                    });
                }
            }
            Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                format!(
                    "cannot compare {} and {}",
                    left.data_type(),
                    right.data_type()
                ),
            ))
        }
    }
}

/// Evaluator equality: integers compare as integers, not via f64.
fn scalars_eq(left: &Scalar, right: &Scalar) -> bool {
    match (left, right) {
        (Scalar::Int64(a), Scalar::Int64(b)) => a == b,
        (Scalar::UInt64(a), Scalar::UInt64(b)) => a == b,
        (Scalar::Int64(a), Scalar::UInt64(b)) => *a >= 0 && *a as u64 == *b,
        (Scalar::UInt64(a), Scalar::Int64(b)) => *b >= 0 && *a == *b as u64,
        (Scalar::Float64(a), Scalar::Float64(b)) => a == b,
        _ => left == right,
    }
}

fn dynamic_from_scalar(value: &Scalar) -> DynamicValue {
    match value {
        Scalar::Null => DynamicValue::Null,
        Scalar::Bool(v) => DynamicValue::Bool(*v),
        Scalar::Int64(v) => DynamicValue::Int64(*v),
        Scalar::UInt64(v) => DynamicValue::UInt64(*v),
        Scalar::Float64(v) => DynamicValue::Float64(*v),
        Scalar::Utf8(s) => DynamicValue::Utf8(s.clone()),
        Scalar::Bytes(b) => DynamicValue::Bytes(b.clone()),
        Scalar::TimestampMicrosUTC(v) => DynamicValue::Int64(*v),
        Scalar::Dynamic(d) => d.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{Field, FieldId};

    fn schema() -> Schema {
        Schema::new(
            1,
            vec![
                Field::new(FieldId::new(1), "temp", DataType::Float64, true),
                Field::new(FieldId::new(2), "payload", DataType::Dynamic, true),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_and_dynamic_extract() {
        let schema = schema();
        let payload = DynamicValue::object(vec![("humidity", DynamicValue::Float64(40.0))]);
        let row = vec![Scalar::Float64(27.5), Scalar::Dynamic(payload)];
        let pred = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column {
                name: "temp".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Float64(25.0))),
        };
        assert_eq!(eval(&pred, &schema, &row).unwrap(), Scalar::Bool(true));
        let extract = Expr::DynamicGet {
            expr: Box::new(Expr::Column {
                name: "payload".into(),
            }),
            key: "humidity".into(),
        };
        assert_eq!(eval(&extract, &schema, &row).unwrap(), Scalar::Float64(40.0));
    }

    #[test]
    fn rejects_dynamic_arithmetic_without_cast() {
        let schema = schema();
        let row = vec![
            Scalar::Float64(1.0),
            Scalar::Dynamic(DynamicValue::Int64(3)),
        ];
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Column {
                name: "payload".into(),
            }),
            right: Box::new(Expr::Literal(Scalar::Int64(1))),
        };
        assert_eq!(
            eval(&expr, &schema, &row).unwrap_err().code,
            ErrorCode::TypeMismatch
        );
    }

    #[test]
    fn r06_int_add_does_not_go_through_f64() {
        let schema = schema();
        let row = vec![Scalar::Float64(0.0), Scalar::Dynamic(DynamicValue::Null)];
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Scalar::Int64(9_007_199_254_740_993))),
            right: Box::new(Expr::Literal(Scalar::Int64(1))),
        };
        assert_eq!(
            eval(&expr, &schema, &row).unwrap(),
            Scalar::Int64(9_007_199_254_740_994)
        );
    }

    #[test]
    fn r06_checked_int_overflow() {
        let schema = schema();
        let row = vec![Scalar::Float64(0.0), Scalar::Dynamic(DynamicValue::Null)];
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Scalar::Int64(i64::MAX))),
            right: Box::new(Expr::Literal(Scalar::Int64(1))),
        };
        assert_eq!(
            eval(&expr, &schema, &row).unwrap_err().code,
            ErrorCode::IntegerOverflow
        );
    }

    #[test]
    fn r07_empty_abs_and_coalesce_are_errors() {
        let schema = schema();
        let row = vec![Scalar::Float64(0.0), Scalar::Dynamic(DynamicValue::Null)];
        assert_eq!(
            eval(
                &Expr::Call {
                    name: "abs".into(),
                    args: vec![],
                },
                &schema,
                &row
            )
            .unwrap_err()
            .code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            eval(
                &Expr::Call {
                    name: "coalesce".into(),
                    args: vec![],
                },
                &schema,
                &row
            )
            .unwrap_err()
            .code,
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn r07_recursive_not_is_null() {
        let schema = schema();
        let row = vec![Scalar::Null, Scalar::Dynamic(DynamicValue::Null)];
        let expr = Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::Column {
            name: "temp".into(),
        }))));
        assert_eq!(eval(&expr, &schema, &row).unwrap(), Scalar::Bool(false));
        let expr = Expr::IsNotNull(Box::new(Expr::IsNull(Box::new(Expr::Column {
            name: "temp".into(),
        }))));
        assert_eq!(eval(&expr, &schema, &row).unwrap(), Scalar::Bool(true));
    }

    #[test]
    fn p0_7_int_compare_beyond_2_pow_53() {
        let schema = schema();
        let row = vec![Scalar::Float64(0.0), Scalar::Dynamic(DynamicValue::Null)];
        let a = 9_007_199_254_740_993i64;
        let gt = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Literal(Scalar::Int64(a + 1))),
            right: Box::new(Expr::Literal(Scalar::Int64(a))),
        };
        assert_eq!(eval(&gt, &schema, &row).unwrap(), Scalar::Bool(true));
        let eq = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Literal(Scalar::Int64(a))),
            right: Box::new(Expr::Literal(Scalar::Int64(a + 1))),
        };
        assert_eq!(eval(&eq, &schema, &row).unwrap(), Scalar::Bool(false));
        let nan_eq = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Literal(Scalar::Float64(f64::NAN))),
            right: Box::new(Expr::Literal(Scalar::Float64(1.0))),
        };
        assert_eq!(
            eval(&nan_eq, &schema, &row).unwrap(),
            Scalar::Bool(false),
            "Eq uses IEEE: NaN equals nothing (not unwrap_or Equal)"
        );
        let nan_lt = Expr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(Expr::Literal(Scalar::Float64(f64::NAN))),
            right: Box::new(Expr::Literal(Scalar::Float64(1.0))),
        };
        assert_eq!(
            eval(&nan_lt, &schema, &row).unwrap_err().code,
            ErrorCode::InvalidArgument,
            "ordered compare must not treat NaN as Equal"
        );
        let tmin = Expr::Call {
            name: "least".into(),
            args: vec![
                Expr::Literal(Scalar::TimestampMicrosUTC(9)),
                Expr::Literal(Scalar::TimestampMicrosUTC(3)),
            ],
        };
        assert_eq!(
            eval(&tmin, &schema, &row).unwrap(),
            Scalar::TimestampMicrosUTC(3)
        );
        let tmax = Expr::Call {
            name: "greatest".into(),
            args: vec![
                Expr::Literal(Scalar::TimestampMicrosUTC(9)),
                Expr::Literal(Scalar::TimestampMicrosUTC(3)),
            ],
        };
        assert_eq!(
            eval(&tmax, &schema, &row).unwrap(),
            Scalar::TimestampMicrosUTC(9)
        );
    }

    #[test]
    fn p0_9_float_int_arith_and_uint_infer_match_eval() {
        let schema = schema();
        let row = vec![Scalar::Float64(0.0), Scalar::Dynamic(DynamicValue::Null)];
        let add = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Scalar::Float64(1.5))),
            right: Box::new(Expr::Literal(Scalar::Int64(2))),
        };
        assert_eq!(infer_type(&add, &schema).unwrap(), DataType::Float64);
        assert_eq!(
            eval(&add, &schema, &row).unwrap(),
            Scalar::Float64(3.5),
            "Float64+Int64 must stay Float64, not truncate to Int64"
        );
        let uadd = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Scalar::UInt64(7))),
            right: Box::new(Expr::Literal(Scalar::UInt64(9))),
        };
        assert_eq!(infer_type(&uadd, &schema).unwrap(), DataType::UInt64);
        assert_eq!(eval(&uadd, &schema, &row).unwrap(), Scalar::UInt64(16));
        let nf = Expr::Call {
            name: "nullif".into(),
            args: vec![
                Expr::Literal(Scalar::Int64(4)),
                Expr::Literal(Scalar::Int64(4)),
            ],
        };
        assert_eq!(infer_type(&nf, &schema).unwrap(), DataType::Int64);
        assert_eq!(eval(&nf, &schema, &row).unwrap(), Scalar::Null);
        let gr = Expr::Call {
            name: "greatest".into(),
            args: vec![
                Expr::Literal(Scalar::Int64(1)),
                Expr::Literal(Scalar::Int64(8)),
            ],
        };
        assert_eq!(infer_type(&gr, &schema).unwrap(), DataType::Int64);
        assert_eq!(eval(&gr, &schema, &row).unwrap(), Scalar::Int64(8));
    }
}

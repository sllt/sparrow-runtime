//! Expression IR shared by SQL and Graph.
//!
//! M0 ships a small scalar evaluator so filter/project stubs and G1a
//! kernels have one semantics. Nested-type kernels are out of scope.

use sparrow_model::{DataType, DynamicValue, Result, Scalar, Schema, SparrowError};
use sparrow_model::error::ErrorCode;

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
    let vals: Result<Vec<Scalar>> = args.iter().map(|a| eval(a, schema, row)).collect();
    let vals = vals?;
    match name.to_ascii_lowercase().as_str() {
        "abs" => match vals.first() {
            Some(Scalar::Int64(v)) => Ok(Scalar::Int64(v.saturating_abs())),
            Some(Scalar::Float64(v)) => Ok(Scalar::Float64(v.abs())),
            Some(Scalar::Null) => Ok(Scalar::Null),
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
        "coalesce" => Ok(vals.into_iter().find(|v| !v.is_null()).unwrap_or(Scalar::Null)),
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("unknown function '{other}'"),
        )),
    }
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
            let l = left.as_f64().ok_or_else(|| {
                SparrowError::new(ErrorCode::TypeMismatch, "arithmetic expects numeric")
            })?;
            let r = right.as_f64().ok_or_else(|| {
                SparrowError::new(ErrorCode::TypeMismatch, "arithmetic expects numeric")
            })?;
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
            if matches!(left, Scalar::Float64(_)) || matches!(right, Scalar::Float64(_)) {
                Ok(Scalar::Float64(v))
            } else {
                Ok(Scalar::Int64(v as i64))
            }
        }
        BinaryOp::Eq => Ok(Scalar::Bool(left == right)),
        BinaryOp::NotEq => Ok(Scalar::Bool(left != right)),
        cmp => {
            let l = left.as_f64().ok_or_else(|| {
                SparrowError::new(ErrorCode::TypeMismatch, "comparison expects numeric")
            })?;
            let r = right.as_f64().ok_or_else(|| {
                SparrowError::new(ErrorCode::TypeMismatch, "comparison expects numeric")
            })?;
            Ok(Scalar::Bool(match cmp {
                BinaryOp::Lt => l < r,
                BinaryOp::Lte => l <= r,
                BinaryOp::Gt => l > r,
                BinaryOp::Gte => l >= r,
                _ => unreachable!(),
            }))
        }
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
}

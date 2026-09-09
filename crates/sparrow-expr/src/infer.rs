//! Type inference for the shared expression IR. Used by Graph and SQL binders.

use crate::{BinaryOp, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{DataType, Schema};

pub fn infer_type(expr: &Expr, schema: &Schema) -> Result<DataType> {
    match expr {
        Expr::Column { name } => schema
            .field_by_name(name)
            .map(|f| f.data_type.clone())
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidArgument, format!("unknown column '{name}'"))
            }),
        Expr::Literal(s) => Ok(s.data_type()),
        Expr::Cast { target, expr } | Expr::TryCast { target, expr } => {
            infer_type(expr, schema)?;
            Ok(target.clone())
        }
        Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::Not(_) => Ok(DataType::Bool),
        Expr::Binary { op, left, right } => {
            let lt = infer_type(left, schema)?;
            let rt = infer_type(right, schema)?;
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::Lte
                    | BinaryOp::Gt
                    | BinaryOp::Gte
                    | BinaryOp::And
                    | BinaryOp::Or
            ) {
                return Ok(DataType::Bool);
            }
            if matches!(lt, DataType::Dynamic) || matches!(rt, DataType::Dynamic) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "dynamic arithmetic requires an explicit CAST/TRY_CAST",
                ));
            }
            if matches!(lt, DataType::Float64) || matches!(rt, DataType::Float64) {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Int64)
            }
        }
        Expr::Call { name, args } => eval_call_sig(name, args.len(), schema, args),
        Expr::DynamicGet { expr, .. } => {
            let t = infer_type(expr, schema)?;
            if !matches!(t, DataType::Dynamic | DataType::Null) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!("dynamic extract requires Dynamic, got {t}"),
                ));
            }
            Ok(DataType::Dynamic)
        }
    }
}

fn eval_call_sig(
    name: &str,
    argc: usize,
    schema: &Schema,
    args: &[Expr],
) -> Result<DataType> {
    match name.to_ascii_lowercase().as_str() {
        "abs" | "lower" | "upper" | "length" | "char_length" if argc != 1 => {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("{name} requires 1 argument, got {argc}"),
            ));
        }
        "coalesce" if argc == 0 => {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "coalesce requires at least 1 argument",
            ));
        }
        _ => {}
    }
    for a in args {
        infer_type(a, schema)?;
    }
    match name.to_ascii_lowercase().as_str() {
        "abs" => infer_type(&args[0], schema),
        "lower" | "upper" => Ok(DataType::Utf8),
        "length" | "char_length" => Ok(DataType::Int64),
        "coalesce" => infer_type(&args[0], schema),
        "nullif" | "greatest" | "least" => args
            .first()
            .map(|a| infer_type(a, schema))
            .unwrap_or(Ok(DataType::Null)),
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("unknown function '{other}'"),
        )),
    }
}

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
                if is_numeric(&lt) && is_numeric(&rt) {
                    Ok(DataType::Float64)
                } else {
                    Err(SparrowError::new(
                        ErrorCode::TypeMismatch,
                        format!("arithmetic expects numeric, got {lt} and {rt}"),
                    ))
                }
            } else if matches!(lt, DataType::UInt64) && matches!(rt, DataType::UInt64) {
                Ok(DataType::UInt64)
            } else if is_intish(&lt) && is_intish(&rt) {
                Ok(DataType::Int64)
            } else {
                Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!("arithmetic expects numeric, got {lt} and {rt}"),
                ))
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
        "nullif" => {
            if argc != 2 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "nullif requires 2 arguments",
                ));
            }
            infer_type(&args[0], schema)
        }
        "greatest" | "least" => {
            if argc == 0 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("{name} requires at least 1 argument"),
                ));
            }
            infer_type(&args[0], schema)
        }
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("unknown function '{other}'"),
        )),
    }
}

/// Whether evaluating `expr` can produce NULL when the input row is valid.
///
/// Column nullability is copied. Non-null literals stay non-null. `TRY_CAST`
/// and `NULLIF` / dynamic extract can introduce NULL. `IS NULL` / `IS NOT NULL`
/// are never null.
pub fn infer_nullable(expr: &Expr, schema: &Schema) -> Result<bool> {
    match expr {
        Expr::Column { name } => schema
            .field_by_name(name)
            .map(|f| f.nullable)
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidArgument, format!("unknown column '{name}'"))
            }),
        Expr::Literal(s) => Ok(s.is_null()),
        Expr::Cast { expr, .. } => infer_nullable(expr, schema),
        Expr::TryCast { .. } => Ok(true),
        Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(false),
        Expr::Not(inner) => infer_nullable(inner, schema),
        Expr::Binary { left, right, .. } => {
            Ok(infer_nullable(left, schema)? || infer_nullable(right, schema)?)
        }
        Expr::Call { name, args } => match name.to_ascii_lowercase().as_str() {
            "nullif" => Ok(true),
            "coalesce" => {
                if args.is_empty() {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "coalesce requires at least 1 argument",
                    ));
                }
                let mut all_null = true;
                for a in args {
                    if !infer_nullable(a, schema)? {
                        all_null = false;
                        break;
                    }
                }
                Ok(all_null)
            }
            "greatest" | "least" => {
                let mut any = false;
                for a in args {
                    any |= infer_nullable(a, schema)?;
                }
                Ok(any)
            }
            _ => {
                if args.is_empty() {
                    Ok(true)
                } else {
                    infer_nullable(&args[0], schema)
                }
            }
        },
        Expr::DynamicGet { .. } => Ok(true),
    }
}

fn is_numeric(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::TimestampMicrosUTC
    )
}

fn is_intish(t: &DataType) -> bool {
    matches!(t, DataType::Int64 | DataType::UInt64 | DataType::TimestampMicrosUTC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Expr;
    use sparrow_model::{Field, FieldId, Scalar, SchemaId};

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "temp", DataType::Float64, true),
            ],
        )
        .unwrap()
    }

    #[test]
    fn p3_46_project_nullable_follows_input_and_expr() {
        let s = schema();
        let id = Expr::Column { name: "id".into() };
        assert!(!infer_nullable(&id, &s).unwrap(), "non-null column stays non-null");
        let temp = Expr::Column {
            name: "temp".into(),
        };
        assert!(infer_nullable(&temp, &s).unwrap(), "nullable column stays nullable");
        let lit = Expr::Literal(Scalar::Int64(1));
        assert!(!infer_nullable(&lit, &s).unwrap());
        let try_cast = Expr::TryCast {
            expr: Box::new(id.clone()),
            target: DataType::Int64,
        };
        assert!(infer_nullable(&try_cast, &s).unwrap());
        let is_null = Expr::IsNull(Box::new(temp));
        assert!(!infer_nullable(&is_null, &s).unwrap());
    }
}

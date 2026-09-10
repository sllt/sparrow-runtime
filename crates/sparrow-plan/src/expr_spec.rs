//! JSON-friendly expression tree. Converts 1:1 into [`sparrow_expr::Expr`].

use serde::{Deserialize, Serialize};

use sparrow_expr::{BinaryOp, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{DataType, Scalar};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExprSpec {
    Col {
        name: String,
    },
    Lit {
        value: LitSpec,
    },
    Cast {
        expr: Box<ExprSpec>,
        ty: String,
    },
    TryCast {
        expr: Box<ExprSpec>,
        ty: String,
    },
    Bin {
        op: String,
        left: Box<ExprSpec>,
        right: Box<ExprSpec>,
    },
    IsNull {
        expr: Box<ExprSpec>,
    },
    IsNotNull {
        expr: Box<ExprSpec>,
    },
    Not {
        expr: Box<ExprSpec>,
    },
    Call {
        name: String,
        args: Vec<ExprSpec>,
    },
    DynGet {
        expr: Box<ExprSpec>,
        key: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case", deny_unknown_fields)]
pub enum LitSpec {
    Null,
    Bool { v: bool },
    Int64 { v: i64 },
    UInt64 { v: u64 },
    Float64 { v: f64 },
    Utf8 { v: String },
}

impl ExprSpec {
    pub fn into_expr(self) -> Result<Expr> {
        Ok(match self {
            Self::Col { name } => Expr::Column { name },
            Self::Lit { value } => Expr::Literal(value.into_scalar()),
            Self::Cast { expr, ty } => Expr::Cast {
                expr: Box::new(expr.into_expr()?),
                target: parse_type(&ty)?,
            },
            Self::TryCast { expr, ty } => Expr::TryCast {
                expr: Box::new(expr.into_expr()?),
                target: parse_type(&ty)?,
            },
            Self::Bin { op, left, right } => Expr::Binary {
                op: parse_op(&op)?,
                left: Box::new(left.into_expr()?),
                right: Box::new(right.into_expr()?),
            },
            Self::IsNull { expr } => Expr::IsNull(Box::new(expr.into_expr()?)),
            Self::IsNotNull { expr } => Expr::IsNotNull(Box::new(expr.into_expr()?)),
            Self::Not { expr } => Expr::Not(Box::new(expr.into_expr()?)),
            Self::Call { name, args } => Expr::Call {
                name,
                args: args
                    .into_iter()
                    .map(ExprSpec::into_expr)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::DynGet { expr, key } => Expr::DynamicGet {
                expr: Box::new(expr.into_expr()?),
                key,
            },
        })
    }
}

impl LitSpec {
    fn into_scalar(self) -> Scalar {
        match self {
            Self::Null => Scalar::Null,
            Self::Bool { v } => Scalar::Bool(v),
            Self::Int64 { v } => Scalar::Int64(v),
            Self::UInt64 { v } => Scalar::UInt64(v),
            Self::Float64 { v } => Scalar::Float64(v),
            Self::Utf8 { v } => Scalar::utf8(v),
        }
    }
}

pub fn parse_type(name: &str) -> Result<DataType> {
    if name.len() > 4096 {
        return Err(SparrowError::new(ErrorCode::BoundExceeded, "type declaration exceeds 4KiB"));
    }
    parse_type_inner(name.trim(), 0)
}

fn parse_type_inner(name: &str, depth: usize) -> Result<DataType> {
    if depth > 32 {
        return Err(SparrowError::new(ErrorCode::BoundExceeded, "nested type depth exceeds 32"));
    }
    if let Some(open) = name.find('<') {
        let malformed = || SparrowError::new(ErrorCode::InvalidSchema, "malformed nested type");
        let body = name[open + 1..].strip_suffix('>').ok_or_else(malformed)?;
        let mut args = Vec::new();
        let mut level = 0usize;
        let mut start = 0;
        for (i, ch) in body.char_indices() {
            match ch {
                '<' => level += 1,
                '>' => level = level.checked_sub(1).ok_or_else(malformed)?,
                ',' if level == 0 => { args.push(body[start..i].trim()); start = i + 1; }
                _ => {}
            }
        }
        if level != 0 { return Err(malformed()); }
        args.push(body[start..].trim());
        if args.iter().any(|a| a.is_empty()) { return Err(malformed()); }
        let parse = |s: &str| parse_type_inner(s.trim(), depth + 1);
        return match name[..open].trim().to_ascii_lowercase().as_str() {
            "array" if args.len() == 1 => Ok(DataType::Array(Box::new(parse(args[0])?))),
            "map" if args.len() == 2 => {
                let key = parse(args[0])?;
                if !matches!(key, DataType::Utf8 | DataType::Int64 | DataType::UInt64) {
                    return Err(SparrowError::new(ErrorCode::InvalidSchema, "JSON map key must be utf8/int64/uint64"));
                }
                Ok(DataType::Map { key: Box::new(key), value: Box::new(parse(args[1])?) })
            }
            "struct" if args.len() <= 64 => {
                let mut fields = Vec::new();
                for (i, arg) in args.iter().enumerate() {
                    let (field, ty) = arg.split_once(':').ok_or_else(malformed)?;
                    let field = field.trim();
                    let nullable = field.ends_with('?');
                    let field = field.trim_end_matches('?');
                    fields.push(sparrow_model::Field::new(sparrow_model::FieldId::new(i as u16 + 1), field, parse(ty)?, nullable));
                }
                sparrow_model::Schema::new(1, fields.clone())?;
                Ok(DataType::Struct(fields))
            }
            _ => Err(malformed()),
        };
    }
    match name.trim().to_ascii_lowercase().as_str() {
        "null" => Ok(DataType::Null),
        "bool" | "boolean" => Ok(DataType::Bool),
        "int64" | "bigint" | "long" => Ok(DataType::Int64),
        "uint64" => Ok(DataType::UInt64),
        "float64" | "double" => Ok(DataType::Float64),
        "utf8" | "varchar" | "string" | "text" => Ok(DataType::Utf8),
        "bytes" => Ok(DataType::Bytes),
        "timestamp_micros_utc" | "timestamp" => Ok(DataType::TimestampMicrosUTC),
        "dynamic" | "json" => Ok(DataType::Dynamic),
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("unsupported type '{other}'"),
        )),
    }
}

fn parse_op(name: &str) -> Result<BinaryOp> {
    match name.to_ascii_lowercase().as_str() {
        "add" | "+" => Ok(BinaryOp::Add),
        "sub" | "-" => Ok(BinaryOp::Sub),
        "mul" | "*" => Ok(BinaryOp::Mul),
        "div" | "/" => Ok(BinaryOp::Div),
        "eq" | "=" | "==" => Ok(BinaryOp::Eq),
        "neq" | "!=" | "<>" => Ok(BinaryOp::NotEq),
        "lt" | "<" => Ok(BinaryOp::Lt),
        "lte" | "<=" => Ok(BinaryOp::Lte),
        "gt" | ">" => Ok(BinaryOp::Gt),
        "gte" | ">=" => Ok(BinaryOp::Gte),
        "and" => Ok(BinaryOp::And),
        "or" => Ok(BinaryOp::Or),
        other => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("unknown binary op '{other}'"),
        )),
    }
}

#[cfg(test)]
mod r3_type_tests {
    use super::*;

    #[test]
    fn nested_schema_types_are_bounded_and_validated() {
        assert!(matches!(parse_type("array<map<utf8,struct<x:int64, y?:bool>>>>"), Err(_)));
        let parsed = parse_type("array<map<utf8,struct<x:int64, y?:bool>>>").unwrap();
        assert!(parsed.is_nested());
        for bad in ["array<>", "map<utf8>", "map<bool,int64>", "struct<x:int64,x:utf8>"] {
            assert!(parse_type(bad).is_err(), "{bad}");
        }
        assert_eq!(parse_type(&format!("{}int64{}", "array<".repeat(34), ">".repeat(34))).unwrap_err().code, ErrorCode::BoundExceeded);
    }
}

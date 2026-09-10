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
    match name.to_ascii_lowercase().as_str() {
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

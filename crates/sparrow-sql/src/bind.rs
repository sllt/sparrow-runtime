//! Bind G0-accepted SQL onto the shared logical IR.

use sqlparser::ast::{
    BinaryOperator, CastKind, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value,
    ValueWithSpan,
};
use sqlparser::parser::Parser;
use sparrow_expr::{infer_type, BinaryOp, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{DataType, PipelineId, RevisionId, Scalar, Schema, SchemaId};
use sparrow_plan::catalog::project_schema;
use sparrow_plan::{bind_linear, Catalog};
use sparrow_plan::BoundLogicalPlan;

use crate::g0::{check_sql, g0_dialect};

pub fn bind_sql(
    sql: &str,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    if looks_like_v03(sql) {
        return crate::bind_v03::bind_sql_v03(sql, catalog, pipeline, revision);
    }
    if looks_like_v02(sql) {
        return crate::bind_v02::bind_sql_v02(sql, catalog, pipeline, revision);
    }
    let verdict = check_sql(sql)?;
    if !verdict.accepted {
        return Err(SparrowError::new(ErrorCode::FeatureUnavailable, verdict.reason)
            .context("sql", sql.chars().take(80).collect::<String>()));
    }
    let statements = Parser::parse_sql(&g0_dialect(), sql).map_err(|e| {
        SparrowError::new(ErrorCode::InvalidArgument, format!("parse error: {e}"))
    })?;
    let Statement::Query(query) = &statements[0] else {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "only SELECT is bindable in V0.1",
        ));
    };
    bind_query(query, catalog, pipeline, revision)
}

fn looks_like_v03(sql: &str) -> bool {
    let u = sql.to_ascii_uppercase();
    u.contains("HOP(")
        || u.contains("FOR SYSTEM_TIME")
        || u.contains("WATERMARK")
        || (u.contains("TUMBLE(")
            && !u.contains("PROCESSING_TIME")
            && !u.contains("PROCTIME")
            && !u.contains("PROC_TIME"))
}

fn looks_like_v02(sql: &str) -> bool {
    let u = sql.to_ascii_uppercase();
    u.contains("GROUP BY")
        || u.contains("TUMBLE")
        || u.contains("COUNT_WINDOW")
        || u.contains(" JOIN ")
}

fn bind_query(
    query: &Query,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "only SELECT body is bindable",
        ));
    };
    bind_select(select, catalog, pipeline, revision)
}

fn bind_select(
    select: &Select,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let table = table_name(&select.from[0].relation)?;
    let source_schema = catalog.get(&table)?.clone();
    let filter = match &select.selection {
        Some(e) => Some(sql_expr(e)?),
        None => None,
    };
    let (exprs, names) = project_list(&select.projection, &source_schema)?;
    let fields: Result<Vec<(String, DataType, bool)>> = exprs
        .iter()
        .zip(names.iter())
        .map(|(e, n)| {
            let ty = infer_type(e, &source_schema)?;
            Ok((n.clone(), ty, true))
        })
        .collect();
    let output = project_schema(SchemaId::new(2), &fields?)?;
    bind_linear(
        pipeline,
        revision,
        table,
        source_schema,
        filter,
        Some((exprs, output)),
        None,
        "capture".into(),
    )
}

pub(crate) fn table_name_pub(rel: &TableFactor) -> Result<String> {
    table_name(rel)
}

pub(crate) fn sql_expr_pub(expr: &SqlExpr) -> Result<Expr> {
    sql_expr(expr)
}

pub(crate) fn project_list_pub(items: &[SelectItem], schema: &Schema) -> Result<(Vec<Expr>, Vec<String>)> {
    project_list(items, schema)
}

fn table_name(rel: &TableFactor) -> Result<String> {
    match rel {
        TableFactor::Table { name, .. } => Ok(name
            .0
            .last()
            .map(|p| match p {
                sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| name.to_string())),
        _ => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "unsupported FROM clause",
        )),
    }
}

fn project_list(items: &[SelectItem], schema: &Schema) -> Result<(Vec<Expr>, Vec<String>)> {
    let mut exprs = Vec::new();
    let mut names = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                for f in &schema.fields {
                    exprs.push(Expr::Column {
                        name: f.name.clone(),
                    });
                    names.push(f.name.clone());
                }
            }
            SelectItem::UnnamedExpr(e) => {
                let expr = sql_expr(e)?;
                let alias = default_alias(e);
                exprs.push(expr);
                names.push(alias);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push(sql_expr(expr)?);
                names.push(alias.value.clone());
            }
            SelectItem::ExprWithAliases { expr, aliases } => {
                exprs.push(sql_expr(expr)?);
                names.push(
                    aliases
                        .first()
                        .map(|a| a.value.clone())
                        .unwrap_or_else(|| default_alias(expr)),
                );
            }
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    "qualified wildcard is not part of V0.1 bind",
                ));
            }
        }
    }
    Ok((exprs, names))
}

fn default_alias(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Identifier(id) => id.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| "expr".into()),
        _ => "expr".into(),
    }
}

fn sql_expr(expr: &SqlExpr) -> Result<Expr> {
    match expr {
        SqlExpr::Identifier(id) => Ok(Expr::Column {
            name: id.value.clone(),
        }),
        SqlExpr::CompoundIdentifier(parts) => Ok(Expr::Column {
            name: parts
                .last()
                .map(|p| p.value.clone())
                .unwrap_or_default(),
        }),
        SqlExpr::Value(v) => Ok(Expr::Literal(value_to_scalar(v)?)),
        SqlExpr::TypedString(ts) => Ok(Expr::Literal(value_to_scalar(&ts.value)?)),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(sql_expr(inner)?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(sql_expr(inner)?))),
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(Expr::Not(Box::new(sql_expr(expr)?))),
        SqlExpr::Nested(inner) => sql_expr(inner),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let e = sql_expr(expr)?;
            let low = sql_expr(low)?;
            let high = sql_expr(high)?;
            let between = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Gte,
                    left: Box::new(e.clone()),
                    right: Box::new(low),
                }),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Lte,
                    left: Box::new(e),
                    right: Box::new(high),
                }),
            };
            if *negated {
                Ok(Expr::Not(Box::new(between)))
            } else {
                Ok(between)
            }
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let left = sql_expr(expr)?;
            let mut acc: Option<Expr> = None;
            for item in list {
                let eq = Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(left.clone()),
                    right: Box::new(sql_expr(item)?),
                };
                acc = Some(match acc {
                    None => eq,
                    Some(prev) => Expr::Binary {
                        op: BinaryOp::Or,
                        left: Box::new(prev),
                        right: Box::new(eq),
                    },
                });
            }
            let in_list = acc.unwrap_or(Expr::Literal(Scalar::Bool(false)));
            if *negated {
                Ok(Expr::Not(Box::new(in_list)))
            } else {
                Ok(in_list)
            }
        }
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::Binary {
            op: map_op(op)?,
            left: Box::new(sql_expr(left)?),
            right: Box::new(sql_expr(right)?),
        }),
        SqlExpr::Cast {
            kind, expr, data_type, ..
        } => {
            let target = map_sql_type(data_type)?;
            let inner = Box::new(sql_expr(expr)?);
            match kind {
                CastKind::TryCast | CastKind::SafeCast => Ok(Expr::TryCast {
                    expr: inner,
                    target,
                }),
                _ => Ok(Expr::Cast {
                    expr: inner,
                    target,
                }),
            }
        }
        SqlExpr::Function(func) => map_function(func),
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("expression is not bindable: {other}"),
        )),
    }
}

fn map_function(func: &Function) -> Result<Expr> {
    let name = func.name.to_string();
    let mut args = Vec::new();
    match &func.args {
        FunctionArguments::None => {}
        FunctionArguments::List(list) => {
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => args.push(sql_expr(e)?),
                    _ => {}
                }
            }
        }
        FunctionArguments::Subquery(_) => {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "function subquery args",
            ));
        }
    }
    Ok(Expr::Call { name, args })
}

fn map_op(op: &BinaryOperator) -> Result<BinaryOp> {
    Ok(match op {
        BinaryOperator::Plus => BinaryOp::Add,
        BinaryOperator::Minus => BinaryOp::Sub,
        BinaryOperator::Multiply => BinaryOp::Mul,
        BinaryOperator::Divide => BinaryOp::Div,
        BinaryOperator::Eq => BinaryOp::Eq,
        BinaryOperator::NotEq => BinaryOp::NotEq,
        BinaryOperator::Lt => BinaryOp::Lt,
        BinaryOperator::LtEq => BinaryOp::Lte,
        BinaryOperator::Gt => BinaryOp::Gt,
        BinaryOperator::GtEq => BinaryOp::Gte,
        BinaryOperator::And => BinaryOp::And,
        BinaryOperator::Or => BinaryOp::Or,
        other => {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("operator {other} is not part of V0.1"),
            ))
        }
    })
}

fn value_to_scalar(v: &ValueWithSpan) -> Result<Scalar> {
    match &v.value {
        Value::Number(s, _) => {
            if s.contains('.') {
                s.parse::<f64>()
                    .map(Scalar::Float64)
                    .map_err(|e| SparrowError::new(ErrorCode::InvalidArgument, e.to_string()))
            } else {
                s.parse::<i64>()
                    .map(Scalar::Int64)
                    .map_err(|e| SparrowError::new(ErrorCode::InvalidArgument, e.to_string()))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(Scalar::utf8(s)),
        Value::Boolean(b) => Ok(Scalar::Bool(*b)),
        Value::Null => Ok(Scalar::Null),
        other => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("literal {other} is not part of V0.1"),
        )),
    }
}

fn map_sql_type(ty: &sqlparser::ast::DataType) -> Result<DataType> {
    use sqlparser::ast::DataType as T;
    Ok(match ty {
        T::Boolean => DataType::Bool,
        T::TinyInt(_) | T::SmallInt(_) | T::Int(_) | T::Integer(_) | T::BigInt(_) | T::Int64 => {
            DataType::Int64
        }
        T::UInt64 => DataType::UInt64,
        T::Float(_) | T::Double(_) | T::DoublePrecision | T::Real | T::Float64 => DataType::Float64,
        T::Text | T::String(_) | T::Varchar(_) | T::Char(_) | T::Character(_) | T::Nvarchar(_) => {
            DataType::Utf8
        }
        T::Bytea | T::Bytes(_) | T::Binary(_) => DataType::Bytes,
        T::Timestamp(_, _) => DataType::TimestampMicrosUTC,
        other => {
            let s = other.to_string().to_ascii_lowercase();
            if s.contains("double") || s.contains("float") {
                DataType::Float64
            } else if s.contains("int") || s.contains("bigint") {
                DataType::Int64
            } else if s.contains("bool") {
                DataType::Bool
            } else if s.contains("char") || s.contains("text") || s.contains("string") {
                DataType::Utf8
            } else {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    format!("SQL type {other} is not part of V0.1"),
                ));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{Field, FieldId};
    use sparrow_plan::BoundKind;

    fn catalog() -> Catalog {
        let mut c = Catalog::new();
        c.insert(
            "sensor_readings",
            Schema::new(
                SchemaId::new(1),
                vec![
                    Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                    Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
                    Field::new(FieldId::new(3), "payload", DataType::Dynamic, true),
                ],
            )
            .unwrap(),
        );
        c
    }

    #[test]
    fn binds_select_where_cast() {
        let plan = bind_sql(
            "SELECT device_id, CAST(temperature AS DOUBLE) AS temp_c FROM sensor_readings WHERE temperature > 25",
            &catalog(),
            PipelineId::new(1),
            RevisionId::new(1),
        )
        .unwrap();
        assert!(plan
            .nodes
            .iter()
            .any(|n| matches!(n.kind, BoundKind::Filter { .. })));
        assert!(plan
            .nodes
            .iter()
            .any(|n| matches!(n.kind, BoundKind::Project { .. })));
    }

    #[test]
    fn reject_order_by_is_feature_unavailable() {
        let err = bind_sql(
            "SELECT device_id FROM sensor_readings ORDER BY temperature",
            &catalog(),
            PipelineId::new(1),
            RevisionId::new(1),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::FeatureUnavailable);
    }
}

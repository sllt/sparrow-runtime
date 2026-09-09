//! Sparrow SQL v0.3 gate: event-time TUMBLE/HOP, watermark holdback,
//! versioned FOR SYSTEM_TIME AS OF lookup. Session / retract / stream-stream
//! join stay rejected.

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableVersion, TableWithJoins,
};
use sqlparser::parser::Parser;
use sparrow_model::error::{ErrorCode, Result, SparrowError};

use crate::g0::{g0_dialect, G0Verdict};

const SCALAR_FUNCS: &[&str] = &[
    "abs",
    "coalesce",
    "lower",
    "upper",
    "length",
    "char_length",
    "nullif",
    "greatest",
    "least",
];

const AGG_FUNCS: &[&str] = &["count", "sum", "avg", "min", "max"];

const REJECTED: &[&str] = &[
    "session",
    "retract",
    "rank",
    "dense_rank",
    "row_number",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "explode",
    "unnest",
];

pub fn check_sql_v03(sql: &str) -> Result<G0Verdict> {
    let statements = Parser::parse_sql(&g0_dialect(), sql).map_err(|e| {
        SparrowError::new(ErrorCode::InvalidArgument, format!("parse error: {e}"))
    })?;
    if statements.len() != 1 {
        return Ok(G0Verdict {
            accepted: false,
            reason: "exactly one SELECT is required".into(),
        });
    }
    match &statements[0] {
        Statement::Query(q) => check_query(q),
        _ => Ok(G0Verdict {
            accepted: false,
            reason: "only SELECT is part of Sparrow SQL v0.3".into(),
        }),
    }
}

fn check_query(query: &Query) -> Result<G0Verdict> {
    if query.with.is_some() {
        return reject("CTE / WITH is not part of Sparrow SQL v0.3");
    }
    if query.order_by.is_some() {
        return reject("unbounded ORDER BY is not part of Sparrow SQL v0.3");
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => check_select(select),
        SetExpr::SetOperation { .. } => reject("UNION is not part of Sparrow SQL v0.3"),
        other => reject(&format!("set expression {other:?} is not part of v0.3")),
    }
}

fn check_select(select: &Select) -> Result<G0Verdict> {
    if select.distinct.is_some() {
        return reject("SELECT DISTINCT is not part of Sparrow SQL v0.3");
    }
    if select.having.is_some() {
        return reject("HAVING is not part of Sparrow SQL v0.3");
    }
    if !select.named_window.is_empty() {
        return reject("named WINDOW / OVER is not part of Sparrow SQL v0.3");
    }
    if select.from.is_empty() || select.from.len() > 2 {
        return reject("FROM must be one stream, optionally JOIN one reference table");
    }
    if let Some(v) = join_reject(&select.from[0])? {
        return Ok(v);
    }
    let windowed = group_is_windowed(&select.group_by)?;
    if !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty()) && !windowed {
        return reject(
            "unbounded GROUP BY without TUMBLE/HOP/COUNT_WINDOW is rejected",
        );
    }
    if windowed {
        if let Some(v) = group_window_reject(&select.group_by)? {
            return Ok(v);
        }
    }
    for item in &select.projection {
        if let Some(v) = proj_reject(item, windowed)? {
            return Ok(v);
        }
    }
    if let Some(sel) = &select.selection {
        if let Some(v) = expr_reject(sel, windowed)? {
            return Ok(v);
        }
    }
    Ok(G0Verdict {
        accepted: true,
        reason: "accepted".into(),
    })
}

fn join_reject(from: &TableWithJoins) -> Result<Option<G0Verdict>> {
    if from.joins.len() > 1 {
        return Ok(Some(rejected(
            "only one reference-table JOIN is part of V0.3 (stream-stream join is out)",
        )));
    }
    if let Some(j) = from.joins.first() {
        match &j.join_operator {
            JoinOperator::FullOuter(_) | JoinOperator::RightOuter(_) | JoinOperator::CrossJoin(_) => {
                return Ok(Some(rejected(
                    "FULL/RIGHT/CROSS JOIN is not part of V0.3 (stream-stream join is out)",
                )));
            }
            JoinOperator::Inner(JoinConstraint::On(_))
            | JoinOperator::LeftOuter(JoinConstraint::On(_)) => {}
            _ => {
                return Ok(Some(rejected(
                    "only INNER/LEFT JOIN to a reference table is part of V0.3",
                )));
            }
        }
        if !matches!(j.relation, TableFactor::Table { .. }) {
            return Ok(Some(rejected("JOIN must be a catalog reference table")));
        }
    }
    match &from.relation {
        TableFactor::Table { .. } => Ok(None),
        _ => Ok(Some(rejected("unsupported FROM clause"))),
    }
}

fn group_is_windowed(g: &GroupByExpr) -> Result<bool> {
    match g {
        GroupByExpr::Expressions(exprs, _) => Ok(exprs
            .iter()
            .any(|e| matches!(e, Expr::Function(f) if is_window_fn(f)))),
        _ => Ok(false),
    }
}

fn is_window_fn(f: &Function) -> bool {
    let n = f.name.to_string().to_ascii_lowercase();
    n == "tumble" || n == "count_window" || n == "hop"
}

fn group_window_reject(g: &GroupByExpr) -> Result<Option<G0Verdict>> {
    let GroupByExpr::Expressions(exprs, _) = g else {
        return Ok(Some(rejected("GROUP BY form is not part of V0.3")));
    };
    for e in exprs {
        if let Expr::Function(f) = e {
            let n = f.name.to_string().to_ascii_lowercase();
            if n == "session" {
                return Ok(Some(rejected(
                    "SESSION windows and late merge are not part of V0.3",
                )));
            }
            if n == "tumble" || n == "hop" || n == "count_window" {
                continue;
            }
            if AGG_FUNCS.contains(&n.as_str()) {
                return Ok(Some(rejected("aggregates belong in SELECT, not GROUP BY")));
            }
        }
        if let Some(v) = expr_reject(e, true)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn proj_reject(item: &SelectItem, windowed: bool) -> Result<Option<G0Verdict>> {
    match item {
        SelectItem::UnnamedExpr(e)
        | SelectItem::ExprWithAlias { expr: e, .. }
        | SelectItem::ExprWithAliases { expr: e, .. } => expr_reject(e, windowed),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => Ok(None),
    }
}

fn expr_reject(expr: &Expr, windowed: bool) -> Result<Option<G0Verdict>> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) | Expr::TypedString(_)
        | Expr::Interval(_) => Ok(None),
        Expr::IsNull(i) | Expr::IsNotNull(i) | Expr::UnaryOp { expr: i, .. } | Expr::Nested(i) => {
            expr_reject(i, windowed)
        }
        Expr::Cast { expr, .. } => expr_reject(expr, windowed),
        Expr::BinaryOp { left, right, .. } => {
            if let Some(v) = expr_reject(left, windowed)? {
                return Ok(Some(v));
            }
            expr_reject(right, windowed)
        }
        Expr::Function(func) => func_reject(func, windowed),
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => {
            Ok(Some(rejected("subquery is not part of Sparrow SQL v0.3")))
        }
        _ => Ok(None),
    }
}

fn func_reject(func: &Function, windowed: bool) -> Result<Option<G0Verdict>> {
    let name = func.name.to_string().to_ascii_lowercase();
    if REJECTED.contains(&name.as_str()) {
        return Ok(Some(rejected(&format!(
            "function '{name}' is not part of Sparrow SQL v0.3"
        ))));
    }
    if name == "tumble" || name == "hop" {
        return Ok(Some(rejected(
            "TUMBLE/HOP belong in GROUP BY, not the SELECT list",
        )));
    }
    if AGG_FUNCS.contains(&name.as_str()) {
        if !windowed {
            return Ok(Some(rejected(
                "aggregates require GROUP BY TUMBLE/HOP/COUNT_WINDOW",
            )));
        }
        if func.over.is_some() {
            return Ok(Some(rejected("OVER / analytic window is not part of V0.3")));
        }
        return walk_args(func, windowed);
    }
    if name == "watermark" {
        return walk_args(func, windowed);
    }
    if !SCALAR_FUNCS.contains(&name.as_str()) {
        return Ok(Some(rejected(&format!("unknown function '{name}'"))));
    }
    walk_args(func, windowed)
}

fn walk_args(func: &Function, windowed: bool) -> Result<Option<G0Verdict>> {
    match &func.args {
        FunctionArguments::List(list) => {
            for arg in &list.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } = arg
                {
                    if let Some(v) = expr_reject(e, windowed)? {
                        return Ok(Some(v));
                    }
                }
            }
            Ok(None)
        }
        FunctionArguments::None => Ok(None),
        FunctionArguments::Subquery(_) => {
            Ok(Some(rejected("function subquery args are not part of v0.3")))
        }
    }
}

fn reject(reason: &str) -> Result<G0Verdict> {
    Ok(rejected(reason))
}

fn rejected(reason: &str) -> G0Verdict {
    G0Verdict {
        accepted: false,
        reason: reason.to_string(),
    }
}

/// True when the join uses FOR SYSTEM_TIME AS OF (versioned lookup).
pub fn table_is_versioned(factor: &TableFactor) -> bool {
    matches!(
        factor,
        TableFactor::Table {
            version: Some(TableVersion::ForSystemTimeAsOf(_)),
            ..
        }
    )
}

#[allow(dead_code)]
fn _binop() -> BinaryOperator {
    BinaryOperator::Eq
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_event_time_tumble() {
        let v = check_sql_v03(
            "SELECT device_id, AVG(temperature) FROM sensors GROUP BY device_id, TUMBLE(ts, INTERVAL '10' SECOND, INTERVAL '3' SECOND)",
        )
        .unwrap();
        assert!(v.accepted, "{}", v.reason);
    }

    #[test]
    fn accept_hop() {
        let v = check_sql_v03(
            "SELECT AVG(temperature) FROM sensors GROUP BY HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)",
        )
        .unwrap();
        assert!(v.accepted, "{}", v.reason);
    }

    #[test]
    fn reject_session() {
        let v = check_sql_v03(
            "SELECT AVG(temperature) FROM sensors GROUP BY SESSION(ts, INTERVAL '5' SECOND)",
        )
        .unwrap();
        assert!(!v.accepted, "{}", v.reason);
    }
}

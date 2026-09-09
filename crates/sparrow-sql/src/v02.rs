//! Sparrow SQL v0.2 gate: PT windows, count windows, incremental aggs,
//! and static lookup JOIN. Event-time / hop / session / watermark stay rejected.

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
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
    "hop",
    "session",
    "rank",
    "dense_rank",
    "row_number",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "explode",
    "unnest",
    "watermark",
];

pub fn check_sql_v02(sql: &str) -> Result<G0Verdict> {
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
            reason: "only SELECT is part of Sparrow SQL v0.2".into(),
        }),
    }
}

fn check_query(query: &Query) -> Result<G0Verdict> {
    if query.with.is_some() {
        return reject("CTE / WITH is not part of Sparrow SQL v0.2");
    }
    if query.order_by.is_some() {
        return reject("unbounded ORDER BY is not part of Sparrow SQL v0.2");
    }
    if query.limit_clause.is_some() {
        return reject("LIMIT/OFFSET is not part of Sparrow SQL v0.2");
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => check_select(select),
        SetExpr::SetOperation { .. } => reject("UNION is not part of Sparrow SQL v0.2"),
        other => reject(&format!("set expression {other:?} is not part of v0.2")),
    }
}

fn check_select(select: &Select) -> Result<G0Verdict> {
    if select.distinct.is_some() {
        return reject("SELECT DISTINCT is not part of Sparrow SQL v0.2 (use bounded DEDUP in Graph)");
    }
    if select.having.is_some() {
        return reject("HAVING is not part of Sparrow SQL v0.2");
    }
    if !select.named_window.is_empty() {
        return reject("named WINDOW / OVER is not part of Sparrow SQL v0.2");
    }
    if select.from.is_empty() || select.from.len() > 2 {
        return reject("FROM must be one stream, optionally JOIN one reference table");
    }
    if let Some(v) = join_reject(&select.from[0])? {
        return Ok(v);
    }
    let windowed = group_is_windowed(&select.group_by)?;
    if !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty()) && !windowed {
        return reject("unbounded GROUP BY without TUMBLE(PROCESSING_TIME, ...) or COUNT_WINDOW(n) is rejected");
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
        if let Some(v) = expr_reject(sel, false, windowed)? {
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
        return Ok(Some(rejected("only one static lookup JOIN is part of V0.2")));
    }
    if let Some(j) = from.joins.first() {
        match j.join_operator {
            JoinOperator::Inner(_) | JoinOperator::LeftOuter(_) | JoinOperator::LeftAnti(_) => {}
            JoinOperator::Inner(JoinConstraint::None) => {}
            _ => {
                // allow Inner/Left with constraint; reject others below
            }
        }
        match &j.join_operator {
            JoinOperator::FullOuter(_) | JoinOperator::RightOuter(_) | JoinOperator::CrossJoin(_) => {
                return Ok(Some(rejected(
                    "FULL/RIGHT/CROSS JOIN is not part of V0.2 (stream-stream join is out)",
                )));
            }
            JoinOperator::Inner(JoinConstraint::On(_))
            | JoinOperator::LeftOuter(JoinConstraint::On(_)) => {}
            JoinOperator::Inner(JoinConstraint::None)
            | JoinOperator::LeftOuter(JoinConstraint::None) => {
                return Ok(Some(rejected("lookup JOIN requires ON")));
            }
            _ => {
                return Ok(Some(rejected(
                    "only INNER/LEFT JOIN to a static reference table is part of V0.2",
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
        GroupByExpr::Expressions(exprs, _) => {
            Ok(exprs.iter().any(|e| matches!(e, Expr::Function(f) if is_window_fn(f))))
        }
        _ => Ok(false),
    }
}

fn is_window_fn(f: &Function) -> bool {
    let n = f.name.to_string().to_ascii_lowercase();
    n == "tumble" || n == "count_window"
}

fn group_window_reject(g: &GroupByExpr) -> Result<Option<G0Verdict>> {
    let GroupByExpr::Expressions(exprs, _) = g else {
        return Ok(Some(rejected("GROUP BY form is not part of V0.2")));
    };
    for e in exprs {
        if let Expr::Function(f) = e {
            let n = f.name.to_string().to_ascii_lowercase();
            if n == "tumble" {
                if let Some(v) = tumble_reject(f)? {
                    return Ok(Some(v));
                }
            } else if n == "hop" || n == "session" {
                return Ok(Some(rejected(&format!(
                    "function '{n}' is event-time / V0.3, not V0.2"
                ))));
            } else if n == "count_window" {
                continue;
            } else if AGG_FUNCS.contains(&n.as_str()) {
                return Ok(Some(rejected("aggregates belong in SELECT, not GROUP BY")));
            }
        }
        if matches!(e, Expr::Function(f) if is_window_fn(f)) {
            continue;
        }
        if let Some(v) = expr_reject(e, false, true)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn tumble_reject(f: &Function) -> Result<Option<G0Verdict>> {
    let args = match &f.args {
        FunctionArguments::List(list) => &list.args,
        _ => return Ok(Some(rejected("TUMBLE requires (PROCESSING_TIME, INTERVAL)"))),
    };
    if args.is_empty() {
        return Ok(Some(rejected("TUMBLE requires PROCESSING_TIME")));
    }
    let first = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return Ok(Some(rejected("TUMBLE first arg must be PROCESSING_TIME"))),
    };
    match first {
        Expr::Identifier(id) => {
            let n = id.value.to_ascii_lowercase();
            if n == "processing_time"
                || n == "proctime"
                || n == "proc_time"
                || n == "processingtime"
            {
                Ok(None)
            } else {
                Ok(Some(rejected(
                    "event-time TUMBLE(column, ...) is V0.3; V0.2 only allows TUMBLE(PROCESSING_TIME, INTERVAL)",
                )))
            }
        }
        Expr::Function(inner) => {
            let n = inner.name.to_string().to_ascii_lowercase();
            if n == "proctime" || n == "processingtime" || n == "processing_time" {
                Ok(None)
            } else {
                Ok(Some(rejected("event-time TUMBLE is not part of V0.2")))
            }
        }
        _ => Ok(Some(rejected(
            "TUMBLE first argument must be PROCESSING_TIME (not an event-time column)",
        ))),
    }
}

fn proj_reject(item: &SelectItem, windowed: bool) -> Result<Option<G0Verdict>> {
    match item {
        SelectItem::UnnamedExpr(e)
        | SelectItem::ExprWithAlias { expr: e, .. }
        | SelectItem::ExprWithAliases { expr: e, .. } => expr_reject(e, false, windowed),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => Ok(None),
    }
}

fn expr_reject(expr: &Expr, in_arith: bool, windowed: bool) -> Result<Option<G0Verdict>> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) | Expr::TypedString(_)
        | Expr::Interval(_) => Ok(None),
        Expr::IsNull(i) | Expr::IsNotNull(i) | Expr::UnaryOp { expr: i, .. } | Expr::Nested(i) => {
            expr_reject(i, in_arith, windowed)
        }
        Expr::Cast { expr, .. } => expr_reject(expr, false, windowed),
        Expr::BinaryOp { left, op, right } => {
            let arith = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            );
            if let Some(v) = expr_reject(left, arith || in_arith, windowed)? {
                return Ok(Some(v));
            }
            expr_reject(right, arith || in_arith, windowed)
        }
        Expr::Function(func) => func_reject(func, windowed),
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => {
            Ok(Some(rejected("subquery is not part of Sparrow SQL v0.2")))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            for e in [expr.as_ref(), low.as_ref(), high.as_ref()] {
                if let Some(v) = expr_reject(e, false, windowed)? {
                    return Ok(Some(v));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn func_reject(func: &Function, windowed: bool) -> Result<Option<G0Verdict>> {
    let name = func.name.to_string().to_ascii_lowercase();
    if REJECTED.contains(&name.as_str()) || name == "hop" || name == "session" {
        return Ok(Some(rejected(&format!(
            "function '{name}' is not part of Sparrow SQL v0.2"
        ))));
    }
    if name == "tumble" {
        return Ok(Some(rejected(
            "TUMBLE belongs in GROUP BY, not the SELECT list",
        )));
    }
    if AGG_FUNCS.contains(&name.as_str()) {
        if !windowed {
            return Ok(Some(rejected(
                "aggregates require GROUP BY TUMBLE(PROCESSING_TIME, ...) or COUNT_WINDOW(n)",
            )));
        }
        if func.over.is_some() {
            return Ok(Some(rejected("OVER / analytic window is not part of V0.2")));
        }
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
                    if let Some(v) = expr_reject(e, false, windowed)? {
                        return Ok(Some(v));
                    }
                }
            }
            Ok(None)
        }
        FunctionArguments::None => Ok(None),
        FunctionArguments::Subquery(_) => {
            Ok(Some(rejected("function subquery args are not part of v0.2")))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_pt_tumble_avg() {
        let v = check_sql_v02(
            "SELECT device_id, AVG(temperature) AS avg_t FROM sensors GROUP BY device_id, TUMBLE(PROCESSING_TIME, INTERVAL '1' SECOND)",
        )
        .unwrap();
        assert!(v.accepted, "{}", v.reason);
    }

    #[test]
    fn reject_event_time_tumble() {
        let v = check_sql_v02(
            "SELECT AVG(temperature) FROM sensors GROUP BY TUMBLE(ts, INTERVAL '1' SECOND)",
        )
        .unwrap();
        assert!(!v.accepted, "{}", v.reason);
    }

    #[test]
    fn reject_unbounded_group_by() {
        let v = check_sql_v02("SELECT device_id, SUM(temperature) FROM sensors GROUP BY device_id")
            .unwrap();
        assert!(!v.accepted);
    }
}

//! G0 Sparrow SQL v0 accept/reject gate.
//!
//! Parses with `sqlparser` [`GenericDialect`] then walks the AST. This is a
//! *narrow binder*: anything not on the V0.1 allow-list is rejected. The
//! runtime crate does not depend on `sqlparser`.

use std::path::{Path, PathBuf};

use sqlparser::ast::{
    BinaryOperator, CastKind, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sparrow_model::error::{ErrorCode, Result, SparrowError};

/// Dialect pinned for G0. Documented so the corpus is reproducible.
pub fn g0_dialect() -> GenericDialect {
    GenericDialect {}
}

const ALLOWED_FUNCS: &[&str] = &[
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

const REJECTED_FUNCS: &[&str] = &[
    "tumble",
    "hop",
    "session",
    "rank",
    "dense_rank",
    "row_number",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "sum",
    "count",
    "avg",
    "min",
    "max",
    "explode",
    "unnest",
    "unknown_func",
];

/// Columns treated as Dynamic in the G0 stub catalog.
fn is_dynamic_ident(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n == "payload" || n == "dyn" || n.starts_with("dyn_")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct G0Verdict {
    pub accepted: bool,
    pub reason: String,
}

pub fn check_sql(sql: &str) -> Result<G0Verdict> {
    let statements = Parser::parse_sql(&g0_dialect(), sql).map_err(|e| {
        SparrowError::new(ErrorCode::InvalidArgument, format!("parse error: {e}"))
            .context("sql", sql.chars().take(80).collect::<String>())
    })?;
    if statements.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "empty SQL statement",
        ));
    }
    if statements.len() > 1 {
        return Ok(G0Verdict {
            accepted: false,
            reason: "multiple statements are not part of Sparrow SQL v0".into(),
        });
    }
    match &statements[0] {
        Statement::Query(query) => check_query(query),
        other => Ok(G0Verdict {
            accepted: false,
            reason: format!("statement {:?} is not a SELECT", stmt_kind(other)),
        }),
    }
}

fn stmt_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::Merge { .. } => "MERGE",
        Statement::Grant { .. } => "GRANT",
        _ => "unsupported",
    }
}

fn check_query(query: &Query) -> Result<G0Verdict> {
    if query.with.is_some() {
        return reject("CTE / WITH is not part of Sparrow SQL v0");
    }
    if !order_by_is_empty(query) {
        return reject("unbounded ORDER BY is not part of Sparrow SQL v0");
    }
    if query.limit_clause.is_some() {
        return reject("LIMIT/OFFSET is not part of Sparrow SQL v0");
    }
    check_set_expr(&query.body)
}

fn order_by_is_empty(query: &Query) -> bool {
    // sqlparser 0.62 stores ORDER BY on Query; treat any present clause as reject.
    query.order_by.is_none()
}

fn check_set_expr(body: &SetExpr) -> Result<G0Verdict> {
    match body {
        SetExpr::Select(select) => check_select(select),
        SetExpr::Query(inner) => check_query(inner),
        SetExpr::SetOperation { .. } => reject("UNION/EXCEPT/INTERSECT is not part of Sparrow SQL v0"),
        other => reject(&format!("set expression {other:?} is not part of Sparrow SQL v0")),
    }
}

fn check_select(select: &Select) -> Result<G0Verdict> {
    if select.distinct.is_some() {
        return reject("SELECT DISTINCT requires buffering and is not part of Sparrow SQL v0");
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => {}
        _ => return reject("GROUP BY is not part of Sparrow SQL v0"),
    }
    if select.having.is_some() {
        return reject("HAVING is not part of Sparrow SQL v0");
    }
    if !select.named_window.is_empty() {
        return reject("named WINDOW clauses are not part of Sparrow SQL v0");
    }
    if select.from.len() != 1 {
        return reject("unbounded JOIN / multi-FROM is not part of Sparrow SQL v0");
    }
    if let Some(verdict) = table_join_reject(&select.from[0])? {
        return Ok(verdict);
    }
    if let Some(top) = &select.top {
        let _ = top;
        return reject("TOP is not part of Sparrow SQL v0");
    }
    for item in &select.projection {
        if let Some(v) = select_item_reject(item)? {
            return Ok(v);
        }
    }
    if let Some(selection) = &select.selection {
        if let Some(v) = expr_reject(selection, false)? {
            return Ok(v);
        }
    }
    Ok(G0Verdict {
        accepted: true,
        reason: "accepted".into(),
    })
}

fn table_join_reject(from: &TableWithJoins) -> Result<Option<G0Verdict>> {
    if !from.joins.is_empty() {
        return Ok(Some(rejected("unbounded JOIN is not part of Sparrow SQL v0")));
    }
    match &from.relation {
        TableFactor::Table { .. } => Ok(None),
        TableFactor::Derived { subquery, .. } => {
            let v = check_query(subquery)?;
            if v.accepted {
                Ok(Some(rejected("subquery FROM is not part of Sparrow SQL v0")))
            } else {
                Ok(Some(v))
            }
        }
        _ => Ok(Some(rejected("unsupported FROM clause"))),
    }
}

fn select_item_reject(item: &SelectItem) -> Result<Option<G0Verdict>> {
    match item {
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => expr_reject(expr, false),
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => Ok(None),
    }
}

fn expr_reject(expr: &Expr, in_arith: bool) -> Result<Option<G0Verdict>> {
    match expr {
        Expr::Identifier(ident) => {
            if in_arith && is_dynamic_ident(&ident.value) {
                return Ok(Some(rejected(
                    "dynamic arithmetic requires an explicit CAST/TRY_CAST",
                )));
            }
            Ok(None)
        }
        Expr::CompoundIdentifier(parts) => {
            if in_arith && parts.iter().any(|p| is_dynamic_ident(&p.value)) {
                return Ok(Some(rejected(
                    "dynamic arithmetic requires an explicit CAST/TRY_CAST",
                )));
            }
            Ok(None)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_reject(inner, false),
        Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner) => expr_reject(inner, false),
        Expr::InList { expr, list, .. } => {
            if let Some(v) = expr_reject(expr, false)? {
                return Ok(Some(v));
            }
            for e in list {
                if let Some(v) = expr_reject(e, false)? {
                    return Ok(Some(v));
                }
            }
            Ok(None)
        }
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => {
            Ok(Some(rejected("subquery is not part of Sparrow SQL v0")))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            for e in [expr.as_ref(), low.as_ref(), high.as_ref()] {
                if let Some(v) = expr_reject(e, false)? {
                    return Ok(Some(v));
                }
            }
            Ok(None)
        }
        Expr::BinaryOp { left, op, right } => {
            let arith = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            );
            if let Some(v) = expr_reject(left, arith || in_arith)? {
                return Ok(Some(v));
            }
            expr_reject(right, arith || in_arith)
        }
        Expr::UnaryOp { expr, .. } => expr_reject(expr, in_arith),
        Expr::Cast { kind, expr, .. } => {
            let _ = kind;
            // CAST / TRY_CAST clears the dynamic-arith restriction.
            let _ = CastKind::TryCast;
            expr_reject(expr, false)
        }
        Expr::Nested(inner) => expr_reject(inner, in_arith),
        Expr::Value(_) | Expr::TypedString(_) => Ok(None),
        Expr::Function(func) => function_reject(func),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                if let Some(v) = expr_reject(op, false)? {
                    return Ok(Some(v));
                }
            }
            for when in conditions {
                if let Some(v) = expr_reject(&when.condition, false)? {
                    return Ok(Some(v));
                }
                if let Some(v) = expr_reject(&when.result, false)? {
                    return Ok(Some(v));
                }
            }
            if let Some(els) = else_result {
                return expr_reject(els, false);
            }
            Ok(None)
        }
        Expr::Interval(_) => Ok(None),
        other => Ok(Some(rejected(&format!(
            "expression form {:?} is not part of Sparrow SQL v0",
            expr_tag(other)
        )))),
    }
}

fn expr_tag(expr: &Expr) -> &'static str {
    match expr {
        Expr::Subquery(_) => "subquery",
        Expr::Wildcard(_) => "wildcard-expr",
        Expr::JsonAccess { .. } => "json_access",
        _ => "other",
    }
}

fn function_reject(func: &Function) -> Result<Option<G0Verdict>> {
    let name = func.name.to_string().to_ascii_lowercase();
    if REJECTED_FUNCS.contains(&name.as_str()) {
        return Ok(Some(rejected(&format!(
            "function '{name}' is not part of Sparrow SQL v0"
        ))));
    }
    if !ALLOWED_FUNCS.contains(&name.as_str()) {
        return Ok(Some(rejected(&format!(
            "unknown function '{name}'"
        ))));
    }
    if func.filter.is_some() || func.over.is_some() {
        return Ok(Some(rejected(
            "window / FILTER clause is not part of Sparrow SQL v0",
        )));
    }
    match &func.args {
        FunctionArguments::None => Ok(None),
        FunctionArguments::Subquery(_) => {
            Ok(Some(rejected("function subquery args are not part of Sparrow SQL v0")))
        }
        FunctionArguments::List(list) => {
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => {
                        if let Some(v) = expr_reject(e, false)? {
                            return Ok(Some(v));
                        }
                    }
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {}
                    _ => {}
                }
            }
            Ok(None)
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

#[derive(Clone, Debug)]
pub struct FixtureCase {
    pub path: PathBuf,
    pub sql: String,
    pub expect_accept: bool,
}

pub fn load_g0_fixtures(root: &Path) -> Result<Vec<FixtureCase>> {
    let mut cases = Vec::new();
    collect_dir(&root.join("accept"), true, &mut cases)?;
    collect_dir(&root.join("reject"), false, &mut cases)?;
    cases.sort_by(|a, b| a.path.cmp(&b.path));
    if cases.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("no G0 fixtures under {}", root.display()),
        ));
    }
    Ok(cases)
}

fn collect_dir(dir: &Path, expect_accept: bool, out: &mut Vec<FixtureCase>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("read {}: {e}", dir.display()),
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            SparrowError::new(ErrorCode::Internal, format!("dirent: {e}"))
        })?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sql") {
            continue;
        }
        let sql = std::fs::read_to_string(&path).map_err(|e| {
            SparrowError::new(
                ErrorCode::Internal,
                format!("read {}: {e}", path.display()),
            )
        })?;
        out.push(FixtureCase {
            path,
            sql,
            expect_accept,
        });
    }
    Ok(())
}

/// Locate `tests/fixtures/sql/g0` from the crate or workspace root.
pub fn default_g0_root() -> PathBuf {
    let candidates = [
        PathBuf::from("tests/fixtures/sql/g0"),
        PathBuf::from("../tests/fixtures/sql/g0"),
        PathBuf::from("../../tests/fixtures/sql/g0"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/sql/g0"),
    ];
    for c in candidates {
        if c.join("accept").is_dir() {
            return c;
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/sql/g0")
}

pub fn run_g0_corpus(root: &Path) -> Result<(usize, usize)> {
    let cases = load_g0_fixtures(root)?;
    let mut accept = 0usize;
    let mut reject = 0usize;
    for case in &cases {
        let verdict = match check_sql(&case.sql) {
            Ok(v) => v,
            Err(err) if !case.expect_accept => {
                reject += 1;
                let _ = err;
                continue;
            }
            Err(err) => {
                return Err(SparrowError::new(
                    ErrorCode::Internal,
                    format!("{}: unexpected parse/check error: {err}", case.path.display()),
                ));
            }
        };
        if verdict.accepted != case.expect_accept {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                format!(
                    "{}: expected accept={} got accept={} ({})",
                    case.path.display(),
                    case.expect_accept,
                    verdict.accepted,
                    verdict.reason
                ),
            ));
        }
        if case.expect_accept {
            accept += 1;
        } else {
            reject += 1;
        }
    }
    Ok((accept, reject))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_simple_select() {
        let v = check_sql("SELECT temperature FROM sensor_readings WHERE temperature > 20").unwrap();
        assert!(v.accepted, "{}", v.reason);
    }

    #[test]
    fn reject_order_by_and_unknown_func() {
        let v = check_sql("SELECT a FROM t ORDER BY a").unwrap();
        assert!(!v.accepted);
        let v = check_sql("SELECT mystery(a) FROM t").unwrap();
        assert!(!v.accepted);
    }

    #[test]
    fn reject_dynamic_arith() {
        let v = check_sql("SELECT payload + 1 FROM t").unwrap();
        assert!(!v.accepted, "{}", v.reason);
        let v = check_sql("SELECT CAST(payload AS DOUBLE) + 1 FROM t").unwrap();
        assert!(v.accepted, "{}", v.reason);
    }

    #[test]
    fn g0_corpus_meets_minimums() {
        let root = default_g0_root();
        let (accept, reject) = run_g0_corpus(&root).expect("g0 corpus");
        assert!(accept >= 20, "accept={accept}");
        assert!(reject >= 20, "reject={reject}");
    }
}

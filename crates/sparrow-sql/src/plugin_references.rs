//! Metadata-only extraction, including disabled packages and safe-mode startup.
//! Walk the parsed AST, never regex-match SQL text or execute/bind a plugin.
use sparrow_expr::plugins::PackageReference;
use sparrow_model::{ErrorCode, Result, SparrowError};
use sqlparser::{
    ast::{self, Visit, Visitor},
    parser::Parser,
};
use std::ops::ControlFlow;
struct References(Vec<PackageReference>);
fn invalid() -> SparrowError {
    SparrowError::new(
        ErrorCode::InvalidArgument,
        "invalid immutable plugin_call dependency",
    )
}
impl Visitor for References {
    type Break = SparrowError;
    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> ControlFlow<Self::Break> {
        let ast::Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        if !function
            .name
            .to_string()
            .eq_ignore_ascii_case("plugin_call")
        {
            return ControlFlow::Continue(());
        }
        let ast::FunctionArguments::List(args) = &function.args else {
            return ControlFlow::Break(invalid());
        };
        if !(4..=12).contains(&args.args.len()) {
            return ControlFlow::Break(invalid());
        }
        let strings: Option<Vec<_>> = args.args[..4]
            .iter()
            .map(|arg| match arg {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::ValueWithSpan {
                        value: ast::Value::SingleQuotedString(value),
                        ..
                    },
                ))) => Some(value.clone()),
                _ => None,
            })
            .collect();
        let Some(strings) = strings else {
            return ControlFlow::Break(invalid());
        };
        let reference = PackageReference {
            name: strings[0].clone(),
            version: strings[1].clone(),
            manifest_sha256: strings[2].clone(),
        };
        if reference.validate().is_err() || !sparrow_expr::plugins::identifier(&strings[3]) {
            return ControlFlow::Break(invalid());
        }
        if !self.0.contains(&reference) {
            self.0.push(reference);
        }
        if self.0.len() > 64 {
            return ControlFlow::Break(invalid());
        }
        ControlFlow::Continue(())
    }
}
pub fn extract(sql: &str) -> Result<Vec<PackageReference>> {
    if sql.len() > 8192 {
        return Err(invalid());
    }
    let statements = Parser::parse_sql(&crate::g0::g0_dialect(), sql).map_err(|_| invalid())?;
    let mut refs = References(vec![]);
    if let ControlFlow::Break(error) = statements.visit(&mut refs) {
        return Err(error);
    }
    Ok(refs.0)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packages_sql_dependency_walk_ignores_comments_strings_and_visits_nested_calls() {
        let hash = "a".repeat(64);
        assert!(
            extract("SELECT 'plugin_call(hello)' FROM s -- plugin_call('x')")
                .unwrap()
                .is_empty()
        );
        let sql=format!("SELECT abs(PLUGIN_CALL('p','v1','{hash}','f',plugin_call('q','v1','{hash}','f',v))) FROM s");
        assert_eq!(extract(&sql).unwrap().len(), 2);
        assert!(extract("SELECT plugin_call(name,'v1','bad','f',v) FROM s").is_err());
    }
}

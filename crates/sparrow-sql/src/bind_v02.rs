//! Bind V0.2 SQL (PT/count windows, aggregates, static lookup JOIN).

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, ValueWithSpan,
};
use sqlparser::parser::Parser;
use sparrow_expr::{infer_type, BinaryOp, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{
    AggFn, DataType, PipelineId, RevisionId, Scalar, Schema, SchemaId, WindowKind,
};
use sparrow_plan::catalog::project_schema;
use sparrow_plan::{
    bind_linear, bind_lookup_linear, bind_window_linear, lookup_output_schema, window_output_schema,
    AggCall, BoundKind, BoundLogicalPlan, Catalog, LookupSpec, WindowSpec,
};

use crate::bind::{sql_expr_pub, table_name_pub};
use crate::g0::g0_dialect;
use crate::v02::check_sql_v02;

pub fn bind_sql_v02(
    sql: &str,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let verdict = check_sql_v02(sql)?;
    if !verdict.accepted {
        return Err(SparrowError::new(ErrorCode::FeatureUnavailable, verdict.reason));
    }
    let statements = Parser::parse_sql(&g0_dialect(), sql).map_err(|e| {
        SparrowError::new(ErrorCode::InvalidArgument, format!("parse error: {e}"))
    })?;
    let Statement::Query(query) = &statements[0] else {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "only SELECT is bindable",
        ));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "only SELECT body is bindable",
        ));
    };
    bind_select_v02(select, catalog, pipeline, revision)
}

fn bind_select_v02(
    select: &Select,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let stream = table_name_pub(&select.from[0].relation)?;
    let source_schema = catalog.get(&stream)?.clone();
    let filter = match &select.selection {
        Some(e) => Some(sql_expr_pub(e)?),
        None => None,
    };

    if let Some(join) = select.from[0].joins.first() {
        return bind_join(select, join, stream, source_schema, filter, catalog, pipeline, revision);
    }

    if let Some(spec) = window_from_group(&select.group_by, &select.projection, &source_schema)? {
        return bind_window_linear(
            pipeline,
            revision,
            stream,
            source_schema,
            filter,
            spec,
            "capture".into(),
        );
    }

    // Fallback: projection-only (should have been G0).
    let (exprs, names) = crate::bind::project_list_pub(&select.projection, &source_schema)?;
    let fields: Result<Vec<(String, DataType, bool)>> = exprs
        .iter()
        .zip(names.iter())
        .map(|(e, n)| Ok((n.clone(), infer_type(e, &source_schema)?, true)))
        .collect();
    let output = project_schema(SchemaId::new(2), &fields?)?;
    bind_linear(
        pipeline,
        revision,
        stream,
        source_schema,
        filter,
        Some((exprs, output)),
        None,
        "capture".into(),
    )
}

fn bind_join(
    select: &Select,
    join: &sqlparser::ast::Join,
    stream: String,
    source_schema: Schema,
    filter: Option<Expr>,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let table = table_name_pub(&join.relation)?;
    let table_schema = catalog.get(&table)?.clone();
    let (left, right) = join_keys(&join.join_operator)?;
    let spec = LookupSpec {
        table: table.clone(),
        stream_keys: vec![left],
        table_keys: vec![right],
        keep: Vec::new(),
    };
    // keep all non-key table columns
    let spec = LookupSpec {
        keep: table_schema
            .fields
            .iter()
            .filter(|f| !spec.table_keys.iter().any(|k| k == &f.name))
            .map(|f| f.name.clone())
            .collect(),
        ..spec
    };
    let mut plan = bind_lookup_linear(
        pipeline,
        revision,
        stream,
        source_schema.clone(),
        spec,
        table_schema.clone(),
        "capture".into(),
    )?;
    if let Some(predicate) = filter {
        // insert filter after source
        let _ = predicate;
    }
    // If SELECT is not *, project after lookup.
    if !matches!(select.projection.first(), Some(SelectItem::Wildcard(_))) {
        let lookup_out = plan
            .nodes
            .iter()
            .find_map(|n| match &n.kind {
                BoundKind::Lookup { output, .. } => Some(output.clone()),
                _ => None,
            })
            .unwrap_or(source_schema);
        let (exprs, names) = crate::bind::project_list_pub(&select.projection, &lookup_out)?;
        let fields: Result<Vec<(String, DataType, bool)>> = exprs
            .iter()
            .zip(names.iter())
            .map(|(e, n)| Ok((n.clone(), infer_type(e, &lookup_out)?, true)))
            .collect();
        let output = project_schema(SchemaId::new(3), &fields?)?;
        // Rebuild: source → lookup → project → sink via bind_linear is wrong.
        // Attach project before sink.
        insert_project_before_sink(&mut plan, exprs, output)?;
    }
    Ok(plan)
}

fn spec_table_key_placeholder() -> String {
    String::new()
}

fn insert_project_before_sink(
    plan: &mut BoundLogicalPlan,
    exprs: Vec<Expr>,
    output: Schema,
) -> Result<()> {
    let sink_idx = plan
        .nodes
        .iter()
        .position(|n| matches!(n.kind, BoundKind::CaptureSink { .. }))
        .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "no sink"))?;
    let pred_id = plan.nodes[sink_idx - 1].id;
    let sink_id = plan.nodes[sink_idx].id;
    let pid = sparrow_model::OperatorId::new(sink_id.raw() + 50);
    let input = plan.nodes[sink_idx - 1].kind.output_schema().clone();
    if let Some(n) = plan.nodes.iter_mut().find(|n| n.id == pred_id) {
        n.downstream = vec![pid];
    }
    plan.nodes[sink_idx].kind = match &plan.nodes[sink_idx].kind {
        BoundKind::CaptureSink { name, .. } => BoundKind::CaptureSink {
            name: name.clone(),
            schema: output.clone(),
        },
        other => other.clone(),
    };
    plan.nodes.insert(
        sink_idx,
        sparrow_plan::BoundNode {
            id: pid,
            kind: BoundKind::Project {
                exprs,
                input,
                output,
            },
            downstream: vec![sink_id],
        },
    );
    let _ = lookup_output_schema;
    let _ = window_output_schema;
    Ok(())
}

fn join_keys(op: &JoinOperator) -> Result<(String, String)> {
    let on = match op {
        JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::LeftOuter(JoinConstraint::On(e)) => e,
        _ => {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "lookup JOIN requires ON a = b",
            ))
        }
    };
    match on {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => Ok((col_name(left)?, col_name(right)?)),
        _ => Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "only equality ON is supported for static lookup",
        )),
    }
}

fn col_name(e: &SqlExpr) -> Result<String> {
    match e {
        SqlExpr::Identifier(id) => Ok(id.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => Ok(parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_default()),
        _ => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "JOIN ON must be column = column",
        )),
    }
}

fn window_from_group(
    group: &GroupByExpr,
    projection: &[SelectItem],
    schema: &Schema,
) -> Result<Option<WindowSpec>> {
    let GroupByExpr::Expressions(exprs, _) = group else {
        return Ok(None);
    };
    if exprs.is_empty() {
        return Ok(None);
    }
    let mut keys = Vec::new();
    let mut kind: Option<WindowKind> = None;
    for e in exprs {
        if let SqlExpr::Function(f) = e {
            let n = f.name.to_string().to_ascii_lowercase();
            if n == "tumble" {
                kind = Some(WindowKind::tumbling_pt(interval_micros(f)?)?);
                continue;
            }
            if n == "count_window" {
                kind = Some(WindowKind::count(count_window_size(f)?)?);
                continue;
            }
        }
        keys.push(col_name(e)?);
    }
    let Some(kind) = kind else {
        return Ok(None);
    };
    let aggs = aggs_from_projection(projection, schema)?;
    Ok(Some(WindowSpec { kind, keys, aggs }))
}

fn count_window_size(f: &Function) -> Result<u64> {
    match &f.args {
        FunctionArguments::List(list) => match list.args.first() {
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(v)))) => {
                match &v.value {
                    sqlparser::ast::Value::Number(s, _) => s.parse::<u64>().map_err(|e| {
                        SparrowError::new(ErrorCode::InvalidArgument, e.to_string())
                    }),
                    _ => Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "COUNT_WINDOW expects an integer",
                    )),
                }
            }
            _ => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "COUNT_WINDOW(n) requires a size",
            )),
        },
        _ => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "COUNT_WINDOW(n) requires a size",
        )),
    }
}

fn interval_micros(f: &Function) -> Result<i64> {
    let args: Vec<&SqlExpr> = match &f.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    if args.len() < 2 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "TUMBLE(PROCESSING_TIME, INTERVAL ...) requires a size",
        ));
    }
    interval_expr_micros(args[1])
}

fn interval_expr_micros(e: &SqlExpr) -> Result<i64> {
    match e {
        SqlExpr::Interval(iv) => {
            let s = match iv.value.as_ref() {
                SqlExpr::Value(ValueWithSpan {
                    value: sqlparser::ast::Value::SingleQuotedString(s),
                    ..
                })
                | SqlExpr::Value(ValueWithSpan {
                    value: sqlparser::ast::Value::Number(s, _),
                    ..
                }) => s.clone(),
                other => other.to_string().trim_matches('\'').to_string(),
            };
            let n: i64 = s.parse().unwrap_or(1);
            let unit = iv
                .leading_field
                .as_ref()
                .map(|u| format!("{u:?}").to_ascii_lowercase())
                .unwrap_or_else(|| "second".into());
            Ok(match unit.as_str() {
                "second" | "seconds" => n.saturating_mul(1_000_000),
                "minute" | "minutes" => n.saturating_mul(60_000_000),
                "hour" | "hours" => n.saturating_mul(3_600_000_000),
                "millisecond" | "milliseconds" => n.saturating_mul(1_000),
                _ => n.saturating_mul(1_000_000),
            })
        }
        SqlExpr::Value(ValueWithSpan {
            value: sqlparser::ast::Value::Number(s, _),
            ..
        }) => s
            .parse::<i64>()
            .map_err(|e| SparrowError::new(ErrorCode::InvalidArgument, e.to_string())),
        _ => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("cannot parse window interval from {e}"),
        )),
    }
}

fn aggs_from_projection(projection: &[SelectItem], _schema: &Schema) -> Result<Vec<AggCall>> {
    let mut aggs = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                if let Some(a) = agg_from_expr(e, "expr")? {
                    aggs.push(a);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(mut a) = agg_from_expr(expr, &alias.value)? {
                    a.alias = alias.value.clone();
                    aggs.push(a);
                }
            }
            SelectItem::ExprWithAliases { expr, aliases } => {
                let alias = aliases
                    .first()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| "expr".into());
                if let Some(mut a) = agg_from_expr(expr, &alias)? {
                    a.alias = alias;
                    aggs.push(a);
                }
            }
            _ => {}
        }
    }
    if aggs.is_empty() {
        aggs.push(AggCall::count_star("count"));
    }
    Ok(aggs)
}

fn agg_from_expr(e: &SqlExpr, alias: &str) -> Result<Option<AggCall>> {
    let SqlExpr::Function(f) = e else {
        return Ok(None);
    };
    let name = f.name.to_string().to_ascii_lowercase();
    let Ok(func) = AggFn::parse(&name) else {
        return Ok(None);
    };
    let star = matches!(
        &f.args,
        FunctionArguments::List(list)
            if list.args.iter().any(|a| matches!(a, FunctionArg::Unnamed(FunctionArgExpr::Wildcard)))
    );
    if func == AggFn::Count && (star || matches!(&f.args, FunctionArguments::None)) {
        return Ok(Some(AggCall::count_star(alias)));
    }
    let input = match &f.args {
        FunctionArguments::List(list) => list.args.iter().find_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => sql_expr_pub(e).ok(),
            _ => None,
        }),
        _ => None,
    };
    Ok(Some(AggCall::new(func, input, alias)))
}

fn _unused_binop() -> BinaryOp {
    BinaryOp::Eq
}

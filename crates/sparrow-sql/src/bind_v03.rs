//! Bind V0.3 SQL: event-time TUMBLE/HOP, holdback, versioned lookup.

use sparrow_expr::{infer_type, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{
    DataType, PipelineId, RevisionId, Schema, SchemaId, WindowKind, DEFAULT_MAX_HOP_OVERLAP,
};
use sparrow_plan::catalog::project_schema;
use sparrow_plan::{
    bind_linear, bind_lookup_linear, bind_window_linear, BoundKind, BoundLogicalPlan, Catalog,
    LookupSpec, WindowSpec,
};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, JoinConstraint, JoinOperator, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableVersion, ValueWithSpan,
};
use sqlparser::parser::Parser;

use crate::bind::{sql_expr_pub, table_name_pub};
use crate::g0::g0_dialect;
use crate::v03::check_sql_v03;

pub fn bind_sql_v03(
    sql: &str,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let verdict = check_sql_v03(sql)?;
    if !verdict.accepted {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            verdict.reason,
        ));
    }
    let statements = Parser::parse_sql(&g0_dialect(), sql)
        .map_err(|e| SparrowError::new(ErrorCode::InvalidArgument, format!("parse error: {e}")))?;
    if statements.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "empty SQL statement",
        ));
    }
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
    bind_select_v03(select, catalog, pipeline, revision)
}

fn bind_select_v03(
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
        return bind_join(
            select,
            join,
            stream,
            source_schema,
            filter,
            catalog,
            pipeline,
            revision,
        );
    }

    if let Some(spec) = window_from_group(&select.group_by, &select.projection, &source_schema)? {
        let mut plan = bind_window_linear(
            pipeline,
            revision,
            stream,
            source_schema,
            filter,
            spec,
            "capture".into(),
        )?;
        crate::bind_v02::apply_window_select_pub(&mut plan, &select.projection)?;
        return Ok(plan);
    }

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
    let (temporal, as_of_field) = versioned_as_of(&join.relation)?;
    let keep: Vec<String> = table_schema
        .fields
        .iter()
        .filter(|f| f.name != right)
        .map(|f| f.name.clone())
        .collect();
    let spec = LookupSpec {
        table: table.clone(),
        stream_keys: vec![left],
        table_keys: vec![right],
        keep,
        temporal,
        as_of_field,
    };
    let mut plan = bind_lookup_linear(
        pipeline,
        revision,
        stream,
        source_schema.clone(),
        spec,
        table_schema,
        "capture".into(),
    )?;
    if let Some(predicate) = filter {
        let lookup_out = plan
            .nodes
            .iter()
            .find_map(|n| match &n.kind {
                BoundKind::Lookup { output, .. } => Some(output.clone()),
                _ => None,
            })
            .unwrap_or_else(|| source_schema.clone());
        sparrow_plan::validate_predicate(&predicate, &lookup_out)?;
        crate::bind_v02::insert_filter_before_sink_pub(&mut plan, predicate)?;
    }
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
        crate::bind_v02::insert_project_before_sink_pub(&mut plan, exprs, output)?;
    }
    Ok(plan)
}

fn versioned_as_of(factor: &TableFactor) -> Result<(bool, Option<String>)> {
    let TableFactor::Table { version, .. } = factor else {
        return Ok((false, None));
    };
    match version {
        Some(TableVersion::ForSystemTimeAsOf(e)) => Ok((true, Some(col_name(e)?))),
        _ => Ok((false, None)),
    }
}

fn join_keys(op: &JoinOperator) -> Result<(String, String)> {
    let on = match op {
        JoinOperator::Join(JoinConstraint::On(e))
        | JoinOperator::Inner(JoinConstraint::On(e))
        | JoinOperator::Left(JoinConstraint::On(e))
        | JoinOperator::LeftOuter(JoinConstraint::On(e)) => e,
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
            "only equality ON is supported for lookup",
        )),
    }
}

fn col_name(e: &SqlExpr) -> Result<String> {
    match e {
        SqlExpr::Identifier(id) => Ok(id.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            Ok(parts.last().map(|p| p.value.clone()).unwrap_or_default())
        }
        _ => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "expected a column name",
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
    let mut event_time_field = None;
    let mut lateness = 0i64;
    let mut max_future_skew_micros = None;
    for e in exprs {
        if let SqlExpr::Function(f) = e {
            let n = f.name.to_string().to_ascii_lowercase();
            if n == "tumble" {
                let parsed = parse_tumble(f)?;
                kind = Some(parsed.kind);
                event_time_field = parsed.event_time_field;
                lateness = parsed.lateness_micros;
                max_future_skew_micros = parsed.max_future_skew_micros;
                continue;
            }
            if n == "hop" {
                let parsed = parse_hop(f)?;
                kind = Some(parsed.kind);
                event_time_field = parsed.event_time_field;
                lateness = parsed.lateness_micros;
                max_future_skew_micros = parsed.max_future_skew_micros;
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
    let aggs = crate::bind_v02::aggs_from_projection_pub(projection, schema)?;
    let mut spec = WindowSpec::new(kind, keys, aggs);
    spec.event_time_field = event_time_field;
    spec.lateness_micros = lateness;
    spec.max_overlap = DEFAULT_MAX_HOP_OVERLAP;
    spec.max_future_skew_micros = max_future_skew_micros;
    spec.validate()?;
    Ok(Some(spec))
}

struct ParsedWindow {
    kind: WindowKind,
    event_time_field: Option<String>,
    lateness_micros: i64,
    max_future_skew_micros: Option<i64>,
}

fn parse_tumble(f: &Function) -> Result<ParsedWindow> {
    let args = fn_args(f);
    if args.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "TUMBLE requires PROCESSING_TIME or an event-time column",
        ));
    }
    if is_processing_time(args[0]) {
        if args.len() < 2 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "TUMBLE(PROCESSING_TIME, INTERVAL) requires a size",
            ));
        }
        return Ok(ParsedWindow {
            kind: WindowKind::tumbling_pt(interval_expr_micros(args[1])?)?,
            event_time_field: None,
            lateness_micros: 0,
            max_future_skew_micros: None,
        });
    }
    let field = col_name(args[0])?;
    if args.len() < 2 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "TUMBLE(ts, INTERVAL) requires a size",
        ));
    }
    let size = interval_expr_micros(args[1])?;
    let lateness = if args.len() >= 3 {
        interval_expr_micros(args[2])?
    } else {
        0
    };
    let max_future_skew_micros = if args.len() >= 4 {
        Some(interval_expr_micros(args[3])?)
    } else {
        None
    };
    Ok(ParsedWindow {
        kind: WindowKind::tumbling_et(size)?,
        event_time_field: Some(field),
        lateness_micros: lateness,
        max_future_skew_micros,
    })
}

fn parse_hop(f: &Function) -> Result<ParsedWindow> {
    let args = fn_args(f);
    if args.len() < 3 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "HOP(ts, slide, size) requires three arguments",
        ));
    }
    let field = col_name(args[0])?;
    let slide = interval_expr_micros(args[1])?;
    let size = interval_expr_micros(args[2])?;
    let lateness = if args.len() >= 4 {
        interval_expr_micros(args[3])?
    } else {
        0
    };
    let max_future_skew_micros = if args.len() >= 5 {
        Some(interval_expr_micros(args[4])?)
    } else {
        None
    };
    Ok(ParsedWindow {
        kind: WindowKind::hopping_et(size, slide)?,
        event_time_field: Some(field),
        lateness_micros: lateness,
        max_future_skew_micros,
    })
}

fn is_processing_time(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Identifier(id) => {
            let n = id.value.to_ascii_lowercase();
            n == "processing_time" || n == "proctime" || n == "proc_time" || n == "processingtime"
        }
        SqlExpr::Function(inner) => {
            let n = inner.name.to_string().to_ascii_lowercase();
            n == "proctime" || n == "processingtime" || n == "processing_time"
        }
        _ => false,
    }
}

fn fn_args(f: &Function) -> Vec<&SqlExpr> {
    match &f.args {
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
    }
}

fn count_window_size(f: &Function) -> Result<u64> {
    match fn_args(f).first() {
        Some(SqlExpr::Value(v)) => match &v.value {
            sqlparser::ast::Value::Number(s, _) => s
                .parse::<u64>()
                .map_err(|e| SparrowError::new(ErrorCode::InvalidArgument, e.to_string())),
            _ => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "COUNT_WINDOW expects an integer",
            )),
        },
        _ => Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "COUNT_WINDOW(n) requires a size",
        )),
    }
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

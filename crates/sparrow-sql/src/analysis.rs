//! Opt-in bounded SQL extensions; ordinary JOIN remains the existing Lookup.
use sparrow_model::{
    ErrorCode, EventTimeBinding, PipelineId, Result, RevisionId, Schema, SparrowError,
};
use sparrow_plan::{
    AnalysisPlan, BoundKind, BoundLogicalPlan, BoundNode, Catalog, JoinMode, StreamJoinSpec,
    UnnestSpec,
};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, JoinConstraint,
    JoinOperator, SelectItem, SetExpr, Statement, TableFactor,
};
use std::collections::BTreeMap;
type Scope = Vec<(String, BTreeMap<String, String>)>;
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}
fn on(op: &JoinOperator) -> Option<&Expr> {
    match op {
        JoinOperator::Join(JoinConstraint::On(e))
        | JoinOperator::Inner(JoinConstraint::On(e))
        | JoinOperator::Left(JoinConstraint::On(e))
        | JoinOperator::LeftOuter(JoinConstraint::On(e)) => Some(e),
        _ => None,
    }
}
fn marker(e: &Expr) -> bool {
    match e {
        Expr::Function(f) => matches!(
            f.name.to_string().to_ascii_lowercase().as_str(),
            "interval_match" | "window_match"
        ),
        Expr::BinaryOp { left, right, .. } => marker(left) || marker(right),
        Expr::Nested(e) => marker(e),
        _ => false,
    }
}
pub(crate) fn accepts(statement: &Statement) -> bool {
    let Statement::Query(q) = statement else {
        return false;
    };
    let SetExpr::Select(s) = q.body.as_ref() else {
        return false;
    };
    s.from.iter().flat_map(|f| &f.joins).any(|j| {
        matches!(j.relation, TableFactor::UNNEST { .. }) || on(&j.join_operator).is_some_and(marker)
    })
}
fn table(t: &TableFactor) -> Result<(String, String)> {
    if let TableFactor::Table {
        name,
        alias,
        args,
        version,
        with_hints,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = t
    {
        if args.is_some()
            || version.is_some()
            || !with_hints.is_empty()
            || *with_ordinality
            || !partitions.is_empty()
            || json_path.is_some()
            || sample.is_some()
            || !index_hints.is_empty()
            || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
        {
            return Err(invalid("bounded analysis requires plain table sources"));
        }
        Ok((
            name.to_string(),
            alias
                .as_ref()
                .map_or_else(|| name.to_string(), |a| a.name.value.clone()),
        ))
    } else {
        Err(invalid("expected stream table"))
    }
}
fn scope(alias: String, schema: &Schema, prefix: &str) -> (String, BTreeMap<String, String>) {
    (
        alias,
        schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), format!("{prefix}{}", f.name)))
            .collect(),
    )
}
fn rewrite(e: &Expr, scope: &Scope) -> Result<Expr> {
    let mut out = e.clone();
    match &mut out {
        Expr::CompoundIdentifier(parts) => {
            if parts.len() != 2 {
                return Err(invalid("expected alias.column"));
            }
            let field = scope
                .iter()
                .find(|(a, _)| a == &parts[0].value)
                .and_then(|(_, s)| s.get(&parts[1].value))
                .ok_or_else(|| invalid("unknown qualified column"))?;
            out = Expr::Identifier(Ident::new(field));
        }
        Expr::Identifier(id) => {
            let mut names = scope.iter().filter_map(|(_, s)| s.get(&id.value));
            if let Some(name) = names.next() {
                if names.next().is_some() {
                    return Err(invalid("ambiguous unqualified column"));
                }
                *id = Ident::new(name);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            **left = rewrite(left, scope)?;
            **right = rewrite(right, scope)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => **expr = rewrite(expr, scope)?,
        Expr::Function(f) => {
            if let FunctionArguments::List(args) = &mut f.args {
                for arg in &mut args.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        *e = rewrite(e, scope)?;
                    } else {
                        return Err(invalid(
                            "analysis scalar calls require positional expressions",
                        ));
                    }
                }
            }
        }
        _ => {}
    }
    Ok(out)
}
fn pair(e: &Expr, left: &str, right: &str) -> Result<(String, String)> {
    let Expr::BinaryOp {
        left: a,
        op: BinaryOperator::Eq,
        right: b,
    } = e
    else {
        return Err(invalid("join keys require alias.key = alias.key"));
    };
    let field = |e: &Expr, alias: &str| match e {
        Expr::CompoundIdentifier(p) if p.len() == 2 && p[0].value == alias => {
            Some(p[1].value.clone())
        }
        _ => None,
    };
    if let (Some(l), Some(r)) = (field(a, left), field(b, right)) {
        Ok((l, r))
    } else if let (Some(l), Some(r)) = (field(b, left), field(a, right)) {
        Ok((l, r))
    } else {
        Err(invalid(
            "join key comparison must connect left and right aliases",
        ))
    }
}
fn terms<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) -> Result<()> {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            terms(left, out)?;
            terms(right, out)?;
        }
        Expr::Nested(e) => terms(e, out)?,
        _ => out.push(e),
    }
    if out.len() > 9 {
        return Err(invalid(
            "join accepts at most eight keys and one time bound",
        ));
    }
    Ok(())
}
pub(crate) fn bind(
    statement: &Statement,
    catalog: &Catalog,
    pipeline: PipelineId,
    revision: RevisionId,
) -> Result<BoundLogicalPlan> {
    let Statement::Query(query) = statement else {
        return Err(invalid("expected SELECT"));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(invalid("expected SELECT body"));
    };
    if select.from.len() != 1
        || select.from[0].joins.len() != 1
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return Err(invalid(
            "bounded analysis supports exactly one join/expansion without FETCH/locks",
        ));
    }
    let mut pure = statement.clone();
    if let Statement::Query(q) = &mut pure {
        if let SetExpr::Select(s) = q.body.as_mut() {
            s.from[0].joins.clear();
        }
    }
    let gate = crate::g0::check_sql(&pure.to_string())?;
    if !gate.accepted {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            gate.reason,
        ));
    }
    let (left_name, left_alias) = table(&select.from[0].relation)?;
    let left = catalog.get(&left_name)?.clone();
    let join = &select.from[0].joins[0];
    let mut nodes = vec![];
    let mut source_times = vec![];
    let (analysis, scope) = if let TableFactor::UNNEST {
        alias,
        array_exprs,
        with_offset,
        with_offset_alias,
        with_ordinality,
    } = &join.relation
    {
        if !matches!(
            join.join_operator,
            JoinOperator::CrossJoin(JoinConstraint::None)
        ) || array_exprs.len() != 1
            || *with_offset
            || with_offset_alias.is_some()
        {
            return Err(invalid(
                "use CROSS JOIN UNNEST(one_array); WITH OFFSET is not supported",
            ));
        }
        let alias = alias
            .as_ref()
            .ok_or_else(|| invalid("UNNEST requires AS alias(item)"))?;
        if alias.name.value == left_alias
            || alias.columns.is_empty()
            || alias.columns.len() > if *with_ordinality { 2 } else { 1 }
            || alias.columns.iter().any(|c| c.data_type.is_some())
        {
            return Err(invalid("invalid UNNEST alias/column list"));
        }
        let left_scope = scope(left_alias, &left, "");
        let expr =
            crate::bind::sql_expr_pub(&rewrite(&array_exprs[0], &vec![left_scope.clone()])?)?;
        let analysis = AnalysisPlan::unnest(
            UnnestSpec {
                expr,
                as_field: alias.columns[0].name.value.clone(),
                max_rows: 1024,
                max_bytes: 1024 * 1024,
            },
            left.clone(),
        )?;
        let mut extra = BTreeMap::new();
        extra.insert(
            alias.columns[0].name.value.clone(),
            alias.columns[0].name.value.clone(),
        );
        for name in ["unnest_source", "unnest_input", "unnest_ordinal"] {
            extra.insert(name.into(), name.into());
        }
        if alias.columns.len() == 2 {
            if extra.contains_key(&alias.columns[1].name.value) {
                return Err(invalid(
                    "UNNEST ordinal alias conflicts with an element/metadata name",
                ));
            }
            extra.insert(alias.columns[1].name.value.clone(), "unnest_ordinal".into());
        }
        nodes.push(BoundNode {
            id: 1.into(),
            kind: BoundKind::MemorySource {
                name: left_name,
                schema: left,
            },
            downstream: vec![3.into()],
        });
        (
            analysis,
            vec![left_scope, (alias.name.value.clone(), extra)],
        )
    } else {
        let (right_name, right_alias) = table(&join.relation)?;
        if right_alias == left_alias {
            return Err(invalid("join aliases must differ"));
        }
        let right = catalog.get(&right_name)?.clone();
        let mode = match &join.join_operator {
            JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => JoinMode::Left,
            JoinOperator::Join(_) | JoinOperator::Inner(_) => JoinMode::Inner,
            _ => return Err(invalid("only INNER/LEFT bounded joins are supported")),
        };
        let mut items = Vec::new();
        terms(
            on(&join.join_operator).ok_or_else(|| invalid("join ON is required"))?,
            &mut items,
        )?;
        let mut keys = Vec::new();
        let mut time = None;
        for item in items {
            if let Expr::Function(f) = item {
                if marker(item) {
                    if time.replace(f).is_some() {
                        return Err(invalid("one join time bound is required"));
                    }
                    continue;
                }
            }
            keys.push(pair(item, &left_alias, &right_alias)?);
        }
        let time =
            time.ok_or_else(|| invalid("INTERVAL_MATCH or WINDOW_MATCH time bound required"))?;
        let args = crate::bind_v03::strict_window_args(time)?;
        let window = time.name.to_string().eq_ignore_ascii_case("window_match");
        let required = if window { 3 } else { 4 };
        if args.len() != required && args.len() != required + 1 {
            return Err(invalid("invalid bounded join time arguments"));
        }
        let column = |e: &Expr, alias: &str| match e {
            Expr::CompoundIdentifier(p) if p.len() == 2 && p[0].value == alias => {
                Ok(p[1].value.clone())
            }
            _ => Err(invalid(
                "time bound arguments must be left.ts then right.ts",
            )),
        };
        let left_time = column(args[0], &left_alias)?;
        let right_time = column(args[1], &right_alias)?;
        let interval = crate::bind_v03::interval_expr_micros;
        let ooo = if args.len() > required {
            interval(args[required])?
        } else {
            0
        };
        if ooo < 0 {
            return Err(invalid("negative out-of-orderness"));
        }
        let spec = StreamJoinSpec {
            left_input: 1,
            right_input: 2,
            left_keys: keys.iter().map(|p| p.0.clone()).collect(),
            right_keys: keys.iter().map(|p| p.1.clone()).collect(),
            left_time: left_time.clone(),
            right_time: right_time.clone(),
            mode,
            before_micros: if window {
                None
            } else {
                Some(interval(args[2])?)
            },
            after_micros: if window {
                None
            } else {
                Some(interval(args[3])?)
            },
            window_size_micros: if window {
                Some(interval(args[2])?)
            } else {
                None
            },
            left_prefix: format!("{left_alias}_"),
            right_prefix: format!("{right_alias}_"),
            max_rows_per_side: 1024,
            max_matches_per_row: 1024,
            max_output_bytes_per_row: 1024 * 1024,
        };
        let scopes = vec![
            scope(left_alias, &left, &spec.left_prefix),
            scope(right_alias, &right, &spec.right_prefix),
        ];
        let analysis = AnalysisPlan::join(spec, left.clone(), right.clone())?;
        for (id, name, schema, field) in [
            (1, left_name, left, left_time),
            (2, right_name, right, right_time),
        ] {
            nodes.push(BoundNode {
                id: id.into(),
                kind: BoundKind::MemorySource { name, schema },
                downstream: vec![3.into()],
            });
            source_times.push((
                id.into(),
                EventTimeBinding {
                    field,
                    out_of_orderness_micros: ooo,
                    max_future_skew_micros: Some(sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS),
                },
            ));
        }
        (analysis, scopes)
    };
    let schema = analysis.output().clone();
    nodes.push(BoundNode {
        id: 3.into(),
        kind: BoundKind::Analysis(Box::new(analysis)),
        downstream: vec![],
    });
    if let Some(filter) = &select.selection {
        let predicate = crate::bind::sql_expr_pub(&rewrite(filter, &scope)?)?;
        sparrow_plan::bound::validate_predicate(&predicate, &schema)?;
        nodes.last_mut().unwrap().downstream = vec![4.into()];
        nodes.push(BoundNode {
            id: 4.into(),
            kind: BoundKind::Filter {
                predicate,
                input: schema.clone(),
            },
            downstream: vec![],
        });
    }
    let projection = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(e) => Ok(SelectItem::UnnamedExpr(rewrite(e, &scope)?)),
            SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::ExprWithAlias {
                expr: rewrite(expr, &scope)?,
                alias: alias.clone(),
            }),
            SelectItem::Wildcard(_) => Ok(item.clone()),
            _ => Err(invalid("unsupported bounded analysis projection")),
        })
        .collect::<Result<Vec<_>>>()?;
    let (exprs, names) = crate::bind::project_list_pub(&projection, &schema)?;
    let fields = exprs
        .iter()
        .zip(&names)
        .map(|(e, n)| crate::bind::typed_project_field(e, n, &schema))
        .collect::<Result<Vec<_>>>()?;
    let output = sparrow_plan::catalog::project_schema(900.into(), &fields)?;
    nodes.last_mut().unwrap().downstream = vec![5.into()];
    nodes.push(BoundNode {
        id: 5.into(),
        kind: BoundKind::Project {
            exprs,
            input: schema,
            output: output.clone(),
        },
        downstream: vec![6.into()],
    });
    nodes.push(BoundNode {
        id: 6.into(),
        kind: BoundKind::CaptureSink {
            name: "capture".into(),
            schema: output,
        },
        downstream: vec![],
    });
    Ok(BoundLogicalPlan {
        pipeline,
        revision,
        nodes,
        source_times,
        side_outputs: vec![],
    })
}

//! Bind a [`GraphSpec`] into [`BoundLogicalPlan`]. SQL uses the same output type.

use std::collections::{HashMap, HashSet};

use crate::bound::{validate_predicate, BoundKind, BoundLogicalPlan, BoundNode};
use crate::catalog::{project_schema, Catalog};
use crate::graph::{GraphSpec, NodeSpec};
use crate::stateful::{
    lookup_output_schema, window_output_schema, AggCall, DedupSpec, LookupSpec, WindowSpec,
};
use sparrow_expr::{infer_nullable, infer_type, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{AggFn, OperatorId, PipelineId, RevisionId, Schema, SchemaId, WindowKind};

pub fn bind_graph(spec: &GraphSpec, catalog: &Catalog) -> Result<BoundLogicalPlan> {
    if spec.version != crate::graph::GRAPH_SPEC_VERSION {
        return Err(SparrowError::new(ErrorCode::FeatureUnavailable, "unsupported GraphSpec version"));
    }
    if spec.nodes.len() > 64 || spec.nodes.iter().map(|n| n.out.len()).sum::<usize>() > 128 {
        return Err(SparrowError::new(ErrorCode::BoundExceeded, "graph exceeds 64 nodes / 128 edges"));
    }
    let mut cat = catalog.clone();
    cat.extend_from_spec(&spec.catalog)?;
    if spec.nodes.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "GraphSpec has no nodes",
        ));
    }

    let mut ids = HashSet::new();
    for n in &spec.nodes {
        if !ids.insert(n.id) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("duplicate node id {}", n.id),
            ));
        }
    }
    for n in &spec.nodes {
        for d in &n.out {
            if !ids.contains(d) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("node {} edges to unknown id {d}", n.id),
                ));
            }
        }
    }
    reject_cycles(spec)?;
    validate_ports(spec)?;

    let order = topo(spec)?;
    let by_id: HashMap<u32, &NodeSpec> = spec.nodes.iter().map(|n| (n.id, n)).collect();
    let mut incoming_schema: HashMap<u32, Schema> = HashMap::new();
    let mut bound = Vec::new();
    let mut next_schema = 100u32;

    for id in order {
        let node = by_id[&id];
        if node.iot.is_some() && !matches!(node.kind.as_str(), "change_detect" | "deadband" | "hysteresis" | "hold_for" | "debounce" | "alarm" | "silence") {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("node {}: iot configuration is only valid for change_detect/deadband/hysteresis", node.id),
            ));
        }
        let kind = match node.kind.as_str() {
            "branch" | "route" | "switch" | "union_all" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, format!("node {id} has no input"))
                })?;
                let best_effort = node.best_effort.iter().copied().map(OperatorId::new).collect();
                match node.kind.as_str() {
                    "branch" => BoundKind::Branch { input, best_effort },
                    "union_all" => BoundKind::UnionAll { input },
                    _ => {
                        let mode = node.route_mode.ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, format!("route {id} requires route_mode")))?;
                        let default = node.default_out.ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, format!("route {id} requires default_out")))?;
                        let routes = node.routes.as_ref().ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, format!("route {id} requires routes")))?;
                        if routes.is_empty() || routes.len() > 16 || !node.out.contains(&default) {
                            return Err(SparrowError::new(ErrorCode::InvalidArgument, format!("route {id}: invalid cases/default port")));
                        }
                        let mut cases = Vec::new();
                        let mut destinations = HashSet::new();
                        for route in routes {
                            if !node.out.contains(&route.to) || !destinations.insert(route.to) || route.to == default {
                                return Err(SparrowError::new(ErrorCode::InvalidArgument, format!("route {id}: duplicate or missing output port")));
                            }
                            let expr = route.predicate.clone().into_expr()?;
                            validate_predicate(&expr, &input).map_err(|e| e.at_operator(OperatorId::new(id)))?;
                            cases.push((expr, OperatorId::new(route.to)));
                        }
                        if destinations.len() + 1 != node.out.len() {
                            return Err(SparrowError::new(ErrorCode::InvalidArgument, format!("route {id}: unused output port")));
                        }
                        BoundKind::Route { input, mode, cases, default: OperatorId::new(default), best_effort }
                    }
                }
            }
            "memory_source" => {
                let table = node
                    .table
                    .as_deref()
                    .or(node.name.as_deref())
                    .ok_or_else(|| {
                        SparrowError::new(ErrorCode::InvalidArgument, "memory_source needs table")
                    })?;
                let schema = cat.get(table)?.clone();
                incoming_schema.insert(id, schema.clone());
                BoundKind::MemorySource {
                    name: table.to_string(),
                    schema,
                }
            }
            "filter" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, format!("filter {id} has no input"))
                })?;
                let pred_spec = node.predicate.as_ref().ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, "filter needs predicate")
                })?;
                let predicate = pred_spec.clone().into_expr()?;
                validate_predicate(&predicate, &input)?;
                incoming_schema.insert(id, input.clone());
                if let Some(side) = &node.side_output {
                    let primary = *node.out.iter().find(|dest| **dest != side.to).expect("validated side port");
                    BoundKind::Route { input, mode: crate::graph::RouteMode::FirstMatch,
                        cases: vec![(predicate, OperatorId::new(primary))], default: OperatorId::new(side.to),
                        best_effort: if side.full == crate::graph::SideOutputFull::Drop {vec![OperatorId::new(side.to)]} else {vec![]} }
                } else { BoundKind::Filter { predicate, input } }
            }
            "project" | "map" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("{} {id} has no input", node.kind),
                    )
                })?;
                let specs = node.exprs.as_ref().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("{} needs exprs", node.kind),
                    )
                })?;
                let mut exprs = Vec::new();
                let mut fields = Vec::new();
                for named in specs {
                    let expr = named.expr.clone().into_expr()?;
                    let ty = infer_type(&expr, &input)?;
                    let nullable = infer_nullable(&expr, &input)?;
                    fields.push((named.alias.clone(), ty, nullable));
                    exprs.push(expr);
                }
                let output = project_schema(SchemaId::new(next_schema), &fields)?;
                next_schema += 1;
                incoming_schema.insert(id, output.clone());
                if node.kind == "map" {
                    BoundKind::Map {
                        exprs,
                        input,
                        output,
                    }
                } else {
                    BoundKind::Project {
                        exprs,
                        input,
                        output,
                    }
                }
            }
            "capture_sink" | "best_effort_sink" => {
                let schema = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, format!("sink {id} has no input"))
                })?;
                let name = node.name.clone().unwrap_or_else(|| "capture".into());
                if node.kind == "best_effort_sink" {
                    BoundKind::BestEffortSink { name, schema }
                } else {
                    BoundKind::CaptureSink { name, schema }
                }
            }
            "window_agg" | "tumble_pt" | "count_window" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("window {id} has no input"),
                    )
                })?;
                let spec = bind_window_node(node, &input)?;
                spec.validate()?;
                let output = window_output_schema(&input, &spec)?;
                incoming_schema.insert(id, output.clone());
                BoundKind::WindowAgg {
                    spec,
                    input,
                    output,
                }
            }
            "dedup" | "deduplicate" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, format!("dedup {id} has no input"))
                })?;
                let spec = bind_dedup_node(node)?;
                spec.validate()?;
                incoming_schema.insert(id, input.clone());
                BoundKind::Deduplicate { spec, input }
            }
            "lookup" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("lookup {id} has no input"),
                    )
                })?;
                let spec = bind_lookup_node(node)?;
                spec.validate()?;
                let table_schema = cat.get(&spec.table)?.clone();
                let keep = if spec.keep.is_empty() {
                    table_schema
                        .fields
                        .iter()
                        .filter(|f| !spec.table_keys.iter().any(|k| k == &f.name))
                        .map(|f| f.name.clone())
                        .collect()
                } else {
                    spec.keep.clone()
                };
                let mut spec = spec;
                spec.keep = keep.clone();
                let output = lookup_output_schema(&input, &table_schema, &keep)?;
                incoming_schema.insert(id, output.clone());
                BoundKind::Lookup {
                    spec,
                    input,
                    output,
                }
            }
            "change_detect" | "deadband" | "hysteresis" | "hold_for" | "debounce" | "alarm" | "silence" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("{} {id} has no input", node.kind),
                    )
                })?;
                let spec = node.iot.clone().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("{} {id} requires iot configuration", node.kind),
                    )
                })?;
                match node.kind.as_str() {
                    "hold_for" | "debounce" | "alarm" | "silence" if spec.timing.as_ref().map(|t| t.kind_name()) != Some(node.kind.as_str()) => {
                        return Err(SparrowError::new(ErrorCode::InvalidArgument, "timed node kind/config mismatch"));
                    }
                    "change_detect" | "deadband" | "hysteresis" if spec.timing.is_some() => {
                        return Err(SparrowError::new(ErrorCode::InvalidArgument, "legacy IoT cannot contain timing configuration"));
                    }
                    "hysteresis" if spec.hysteresis.is_none() || spec.deadband.is_some() => {
                        return Err(SparrowError::new(ErrorCode::InvalidArgument,
                            format!("hysteresis {id} requires only hysteresis configuration")));
                    }
                    "change_detect" | "deadband" if spec.hysteresis.is_some() => {
                        return Err(SparrowError::new(ErrorCode::InvalidArgument,
                            format!("{} {id} must not contain hysteresis configuration", node.kind)));
                    }
                    "change_detect" if spec.deadband.is_some() => {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            format!("change_detect {id} must not contain deadband configuration"),
                        ));
                    }
                    "deadband" if spec.deadband.is_none() => {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            format!("deadband {id} requires deadband configuration"),
                        ));
                    }
                    _ => {}
                }
                spec.validate(&input)
                    .map_err(|e| e.at_operator(OperatorId::new(id)))?;
                let output = spec.output_schema(&input)?;
                incoming_schema.insert(id, output.clone());
                BoundKind::Iot { spec, input, output }
            }
            "hop" | "tumble_et" | "event_time_window" => {
                let input = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("window {id} has no input"),
                    )
                })?;
                let spec = bind_window_node(node, &input)?;
                spec.validate()?;
                let output = window_output_schema(&input, &spec)?;
                incoming_schema.insert(id, output.clone());
                BoundKind::WindowAgg {
                    spec,
                    input,
                    output,
                }
            }
            "session" | "retract" | "late_merge" => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    format!(
                        "node kind '{}' is not part of V0.3 (session late merge / retract are out)",
                        node.kind
                    ),
                ));
            }
            "join" => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    "stream-stream join is not part of V0.3; use kind=lookup for reference / versioned tables",
                ));
            }
            other => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    format!("node kind '{other}' is not part of V0.3"),
                ));
            }
        };

        if node.side_output.as_ref().is_some_and(|s|s.kind==crate::graph::SideOutputKind::Late)
            && !matches!(&kind,BoundKind::WindowAgg {spec,..} if spec.kind.uses_event_time()) {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,format!("node {id}: late side output requires an event-time window")));
        }
        for dest in &node.out {
            let side_schema = node.side_output.as_ref().filter(|s|s.to==*dest).map(|s|match s.kind {
                crate::graph::SideOutputKind::DecodeError => crate::graph::decode_error_schema(),
                crate::graph::SideOutputKind::Late => match &kind { BoundKind::WindowAgg { input, spec, .. } if spec.kind.uses_event_time() => input.clone(), _ => kind.output_schema().clone() },
                _ => kind.output_schema().clone(),
            });
            let schema = side_schema.as_ref().unwrap_or_else(||kind.output_schema());
            if let Some(existing) = incoming_schema.get(dest) {
                if existing.fields != schema.fields {
                    return Err(SparrowError::new(ErrorCode::TypeMismatch, format!("node {dest}: UnionAll inputs require identical ordered fields, types and nullability")));
                }
            }
            if !matches!(kind, BoundKind::CaptureSink { .. } | BoundKind::BestEffortSink { .. }) {
                incoming_schema
                    .entry(*dest)
                    .or_insert_with(|| schema.clone());
            }
        }

        bound.push(BoundNode {
            id: OperatorId::new(id),
            kind,
            downstream: node.out.iter().copied().map(OperatorId::new).collect(),
        });
    }

    let sources = bound
        .iter()
        .filter(|n| matches!(n.kind, BoundKind::MemorySource { .. }))
        .count();
    let sinks = bound
        .iter()
        .filter(|n| matches!(n.kind, BoundKind::CaptureSink { .. } | BoundKind::BestEffortSink { .. }))
        .count();
    if sources == 0 || sinks == 0 {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("graph requires sources and sinks (got {sources}/{sinks})"),
        ));
    }

    let mut source_times = Vec::new();
    for node in &spec.nodes {
        if node.kind == "memory_source" {
            if let Some(field)=&node.event_time_field {
                let schema=bound.iter().find(|n|n.id.raw()==node.id).unwrap().kind.output_schema();
                let index=schema.index_of_name(field).ok_or_else(||SparrowError::new(ErrorCode::InvalidSchema,"source event-time field missing"))?;
                if schema.fields[index].nullable || !matches!(schema.fields[index].data_type,sparrow_model::DataType::Int64|sparrow_model::DataType::TimestampMicrosUTC) {return Err(SparrowError::new(ErrorCode::TypeMismatch,"source event-time requires non-null Int64/TimestampMicrosUTC"));}
                let time=sparrow_model::EventTimeBinding {field:field.clone(),out_of_orderness_micros:node.out_of_orderness_micros.unwrap_or(0),max_future_skew_micros:Some(node.max_future_skew_micros.unwrap_or(sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS))};
                time.validate()?;source_times.push((OperatorId::new(node.id),time));
            }
        }
    }
    let graph = sources > 1 || spec.nodes.iter().any(|n|n.out.len()>1||n.kind=="union_all");
    if graph && bound.iter().any(|n|matches!(&n.kind,BoundKind::WindowAgg {spec,..} if spec.kind.uses_event_time())) && source_times.len()!=sources {
        return Err(SparrowError::new(ErrorCode::InvalidArgument,"event-time DAG requires explicit event_time_field on every source"));
    }
    if !source_times.is_empty() {
        for node in &bound {
            if let BoundKind::WindowAgg {spec,..}=&node.kind {
                if let Some(binding)=spec.binding() {
                    let allowed=binding.max_future_skew_micros.unwrap_or(i64::MAX);
                    if source_times.iter().any(|(_,time)|time.max_future_skew_micros.unwrap_or(i64::MAX)>allowed) {
                        return Err(SparrowError::new(ErrorCode::InvalidArgument,"source future-skew allowance cannot exceed a downstream event-time window allowance"));
                    }
                }
            }
        }
        let mut times:HashMap<OperatorId,Option<String>>=HashMap::new();
        for node in &bound {
            let incoming=times.get(&node.id).cloned().flatten();
            let output=match &node.kind {
                BoundKind::MemorySource {..}=>source_times.iter().find(|(id,_)|*id==node.id).map(|(_,t)|t.field.clone()),
                BoundKind::Project {exprs,output,..}|BoundKind::Map {exprs,output,..}=>incoming.as_ref().and_then(|field|exprs.iter().zip(&output.fields).find_map(|(expr,f)|if matches!(expr,Expr::Column {name} if name==field){Some(f.name.clone())}else{None})),
                BoundKind::WindowAgg {spec,..}=>{
                    if spec.kind.uses_event_time() && incoming.as_ref()!=spec.event_time_field.as_ref(){return Err(SparrowError::new(ErrorCode::InvalidArgument,format!("window {}: event-time lineage lost, transformed or incompatible across inputs",node.id.raw())));}
                    None
                },
                _=>incoming,
            };
            for dest in &node.downstream {
                let side=spec.nodes.iter().find(|n|n.id==node.id.raw()).and_then(|n|n.side_output.as_ref()).is_some_and(|s|s.to==dest.raw());
                let next=if side{None}else{output.clone()};
                if let Some(previous)=times.get(dest){if previous!=&next{return Err(SparrowError::new(ErrorCode::InvalidArgument,format!("node {}: incompatible input time attributes",dest.raw())));}}
                else{times.insert(*dest,next);}
            }
        }
    }

    Ok(BoundLogicalPlan {
        source_times,
        side_outputs: spec.nodes.iter().filter_map(|n|n.side_output.clone().map(|s|(OperatorId::new(n.id),s))).collect(),
        pipeline: PipelineId::new(spec.pipeline_id),
        revision: RevisionId::new(spec.revision_id),
        nodes: bound,
    })
}

/// Build a linear bound plan from already-typed IR pieces (SQL binder).
pub fn bind_linear(
    pipeline: PipelineId,
    revision: RevisionId,
    source_name: String,
    source_schema: Schema,
    filter: Option<Expr>,
    project: Option<(Vec<Expr>, Schema)>,
    map: Option<(Vec<Expr>, Schema)>,
    sink_name: String,
) -> Result<BoundLogicalPlan> {
    let mut nodes = Vec::new();
    let mut cursor_schema = source_schema.clone();

    let source_id = OperatorId::SOURCE;
    let mut pending = source_id;
    nodes.push(BoundNode {
        id: source_id,
        kind: BoundKind::MemorySource {
            name: source_name,
            schema: source_schema,
        },
        downstream: Vec::new(),
    });

    if let Some(predicate) = filter {
        validate_predicate(&predicate, &cursor_schema)?;
        let fid = OperatorId::FILTER;
        link(&mut nodes, pending, fid);
        nodes.push(BoundNode {
            id: fid,
            kind: BoundKind::Filter {
                predicate,
                input: cursor_schema.clone(),
            },
            downstream: Vec::new(),
        });
        pending = fid;
    }
    if let Some((exprs, output)) = project {
        for e in &exprs {
            infer_type(e, &cursor_schema)?;
        }
        let pid = OperatorId::PROJECT;
        link(&mut nodes, pending, pid);
        cursor_schema = output.clone();
        nodes.push(BoundNode {
            id: pid,
            kind: BoundKind::Project {
                exprs,
                input: nodes
                    .last()
                    .map(|n| n.kind.output_schema().clone())
                    .unwrap_or_else(|| cursor_schema.clone()),
                output,
            },
            downstream: Vec::new(),
        });
        pending = pid;
    }
    if let Some((exprs, output)) = map {
        let mid = OperatorId::MAP;
        link(&mut nodes, pending, mid);
        nodes.push(BoundNode {
            id: mid,
            kind: BoundKind::Map {
                exprs,
                input: cursor_schema.clone(),
                output: output.clone(),
            },
            downstream: Vec::new(),
        });
        cursor_schema = output;
        pending = mid;
    }
    let sid = OperatorId::SINK;
    link(&mut nodes, pending, sid);
    nodes.push(BoundNode {
        id: sid,
        kind: BoundKind::CaptureSink {
            name: sink_name,
            schema: cursor_schema,
        },
        downstream: Vec::new(),
    });
    Ok(BoundLogicalPlan {
        side_outputs: vec![],
        source_times: vec![],
        pipeline,
        revision,
        nodes,
    })
}

/// Source → optional filter → V0.2 stateful op → sink.
pub fn bind_window_linear(
    pipeline: PipelineId,
    revision: RevisionId,
    source_name: String,
    source_schema: Schema,
    filter: Option<Expr>,
    spec: WindowSpec,
    sink_name: String,
) -> Result<BoundLogicalPlan> {
    spec.validate()?;
    let output = window_output_schema(&source_schema, &spec)?;
    bind_after_source(
        pipeline,
        revision,
        source_name,
        source_schema.clone(),
        filter,
        BoundKind::WindowAgg {
            spec,
            input: source_schema,
            output,
        },
        sink_name,
    )
}

pub fn bind_dedup_linear(
    pipeline: PipelineId,
    revision: RevisionId,
    source_name: String,
    source_schema: Schema,
    spec: DedupSpec,
    sink_name: String,
) -> Result<BoundLogicalPlan> {
    spec.validate()?;
    bind_after_source(
        pipeline,
        revision,
        source_name,
        source_schema.clone(),
        None,
        BoundKind::Deduplicate {
            spec,
            input: source_schema,
        },
        sink_name,
    )
}

pub fn bind_lookup_linear(
    pipeline: PipelineId,
    revision: RevisionId,
    source_name: String,
    source_schema: Schema,
    spec: LookupSpec,
    table_schema: Schema,
    sink_name: String,
) -> Result<BoundLogicalPlan> {
    spec.validate()?;
    let keep = if spec.keep.is_empty() {
        table_schema
            .fields
            .iter()
            .filter(|f| !spec.table_keys.iter().any(|k| k == &f.name))
            .map(|f| f.name.clone())
            .collect()
    } else {
        spec.keep.clone()
    };
    let mut spec = spec;
    spec.keep = keep.clone();
    let output = lookup_output_schema(&source_schema, &table_schema, &keep)?;
    bind_after_source(
        pipeline,
        revision,
        source_name,
        source_schema.clone(),
        None,
        BoundKind::Lookup {
            spec,
            input: source_schema,
            output,
        },
        sink_name,
    )
}

fn bind_after_source(
    pipeline: PipelineId,
    revision: RevisionId,
    source_name: String,
    source_schema: Schema,
    filter: Option<Expr>,
    mid: BoundKind,
    sink_name: String,
) -> Result<BoundLogicalPlan> {
    let mut nodes = Vec::new();
    let source_id = OperatorId::SOURCE;
    let mut pending = source_id;
    nodes.push(BoundNode {
        id: source_id,
        kind: BoundKind::MemorySource {
            name: source_name,
            schema: source_schema.clone(),
        },
        downstream: Vec::new(),
    });
    if let Some(predicate) = filter {
        validate_predicate(&predicate, &source_schema)?;
        let fid = OperatorId::FILTER;
        link(&mut nodes, pending, fid);
        nodes.push(BoundNode {
            id: fid,
            kind: BoundKind::Filter {
                predicate,
                input: source_schema.clone(),
            },
            downstream: Vec::new(),
        });
        pending = fid;
    }
    let mid_id = match &mid {
        BoundKind::WindowAgg { .. } => OperatorId::WINDOW,
        BoundKind::Deduplicate { .. } => OperatorId::DEDUP,
        BoundKind::Lookup { .. } => OperatorId::LOOKUP,
        _ => OperatorId::WINDOW,
    };
    link(&mut nodes, pending, mid_id);
    let out_schema = mid.output_schema().clone();
    nodes.push(BoundNode {
        id: mid_id,
        kind: mid,
        downstream: Vec::new(),
    });
    let sid = OperatorId::SINK;
    link(&mut nodes, mid_id, sid);
    nodes.push(BoundNode {
        id: sid,
        kind: BoundKind::CaptureSink {
            name: sink_name,
            schema: out_schema,
        },
        downstream: Vec::new(),
    });
    Ok(BoundLogicalPlan {
        side_outputs: vec![],
        source_times: vec![],
        pipeline,
        revision,
        nodes,
    })
}

fn bind_window_node(node: &NodeSpec, _input: &Schema) -> Result<WindowSpec> {
    if node
        .window
        .as_ref()
        .map(|w| w.kind.to_ascii_lowercase() == "session")
        .unwrap_or(false)
        || node.kind == "session"
    {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "SESSION windows and late merge are not part of V0.3",
        ));
    }
    let kind = if let Some(w) = &node.window {
        match w.kind.to_ascii_lowercase().as_str() {
            "tumble_pt" | "tumbling_pt" | "tumbling_processing_time" | "processing_time" => {
                WindowKind::tumbling_pt(w.size_micros.unwrap_or(0))?
            }
            "count" | "count_window" => WindowKind::count(w.size.unwrap_or(0))?,
            "tumble_et" | "tumbling_et" | "tumbling_event_time" | "event_time" | "tumble" => {
                WindowKind::tumbling_et(w.size_micros.unwrap_or(0))?
            }
            "hop" | "hopping" | "hopping_event_time" => WindowKind::hopping_et(
                w.size_micros.unwrap_or(0),
                w.slide_micros.unwrap_or(0),
            )?,
            "session" => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    "SESSION windows are not part of V0.3",
                ));
            }
            other => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown window kind '{other}'"),
                ));
            }
        }
    } else if node.kind == "count_window" {
        let size = node
            .window
            .as_ref()
            .and_then(|w| w.size)
            .or_else(|| node.max_keys.map(|n| n as u64))
            .unwrap_or(0);
        WindowKind::count(size)?
    } else if node.kind == "hop" {
        let w = node.window.as_ref().ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "hop needs window {size_micros, slide_micros}",
            )
        })?;
        WindowKind::hopping_et(w.size_micros.unwrap_or(0), w.slide_micros.unwrap_or(0))?
    } else if node.kind == "tumble_et" || node.kind == "event_time_window" {
        let size = node.window.as_ref().and_then(|w| w.size_micros).unwrap_or(0);
        WindowKind::tumbling_et(size)?
    } else {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "window_agg needs window {kind, size_micros|size}",
        ));
    };
    let keys = node.keys.clone().unwrap_or_default();
    let aggs = node
        .aggs
        .as_ref()
        .ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, "window_agg needs aggs"))?
        .iter()
        .map(|a| {
            let func = AggFn::parse(&a.func)?;
            let input = match &a.expr {
                Some(e) => Some(e.clone().into_expr()?),
                None => None,
            };
            Ok(AggCall::new(func, input, a.alias.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    let event_time_field = node
        .event_time_field
        .clone()
        .or_else(|| node.window.as_ref().and_then(|w| w.event_time_field.clone()));
    let lateness_micros = node
        .lateness_micros
        .or_else(|| node.window.as_ref().and_then(|w| w.lateness_micros))
        .unwrap_or(0);
    let max_overlap = node
        .window
        .as_ref()
        .and_then(|w| w.max_overlap)
        .unwrap_or(sparrow_model::DEFAULT_MAX_HOP_OVERLAP);
    let max_future_skew_micros = node
        .window
        .as_ref()
        .and_then(|w| w.max_future_skew_micros);
    let spec = WindowSpec {
        kind,
        keys,
        aggs,
        event_time_field,
        lateness_micros,
        max_overlap,
        max_future_skew_micros,
    };
    spec.validate()?;
    Ok(spec)
}

fn bind_dedup_node(node: &NodeSpec) -> Result<DedupSpec> {
    let keys = node.keys.clone().ok_or_else(|| {
        SparrowError::new(ErrorCode::InvalidArgument, "dedup needs keys")
    })?;
    let spec = DedupSpec {
        keys,
        ttl_micros: node.ttl_micros.unwrap_or(0),
        max_keys: node.max_keys.unwrap_or(0),
    };
    spec.validate()?;
    Ok(spec)
}

fn bind_lookup_node(node: &NodeSpec) -> Result<LookupSpec> {
    let table = node
        .table
        .clone()
        .ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, "lookup needs table"))?;
    let on = node.on.as_ref().ok_or_else(|| {
        SparrowError::new(ErrorCode::InvalidArgument, "lookup needs on: [{stream, table}]")
    })?;
    Ok(LookupSpec {
        table,
        stream_keys: on.iter().map(|o| o.stream.clone()).collect(),
        table_keys: on.iter().map(|o| o.table.clone()).collect(),
        keep: node.keep.clone().unwrap_or_default(),
        temporal: node.temporal.unwrap_or(false),
        as_of_field: node.as_of_field.clone(),
    })
}

fn link(nodes: &mut [BoundNode], from: OperatorId, to: OperatorId) {
    if let Some(n) = nodes.iter_mut().find(|n| n.id == from) {
        n.downstream.push(to);
    }
}

fn validate_ports(spec: &GraphSpec) -> Result<()> {
    let mut inbound: HashMap<u32, u32> = HashMap::new();
    for n in &spec.nodes {
        for d in &n.out {
            *inbound.entry(*d).or_insert(0) += 1;
        }
        if n.out.iter().collect::<HashSet<_>>().len() != n.out.len() || n.out.len() > 16 {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, format!("node {}: duplicate edges or more than 16 outputs", n.id)));
        }
    }
    let mut lossy = HashSet::new();
    for id in topo(spec)? {
        let n = spec.nodes.iter().find(|n| n.id == id).unwrap();
        let inputs = inbound.get(&id).copied().unwrap_or(0);
        let side = usize::from(n.side_output.is_some());
        if let Some(side) = &n.side_output {
            let valid = match side.kind {
                crate::graph::SideOutputKind::DecodeError => n.kind=="memory_source",
                crate::graph::SideOutputKind::RuleReject => n.kind=="filter",
                crate::graph::SideOutputKind::Late => matches!(n.kind.as_str(),"window_agg"|"tumble_et"|"event_time_window"|"hop"),
            };
            if !valid || !n.out.contains(&side.to) {return Err(SparrowError::new(ErrorCode::InvalidArgument,format!("node {id}: invalid side output kind/port")));}
        }
        let valid = match n.kind.as_str() {
            "memory_source" => inputs == 0 && n.out.len() == 1 + side,
            "capture_sink" | "best_effort_sink" => inputs == 1 && n.out.is_empty(),
            "branch" | "route" | "switch" => inputs == 1 && !n.out.is_empty(),
            "union_all" => (2..=16).contains(&inputs) && n.out.len() == 1,
            _ => inputs == 1 && n.out.len() == 1 + side,
        };
        if !valid {
            return Err(SparrowError::new(ErrorCode::FeatureUnavailable, format!("node {id}: invalid input/output arity; use explicit Branch/Route/UnionAll")));
        }
        let router = matches!(n.kind.as_str(), "branch" | "route" | "switch");
        if (n.out_of_orderness_micros.is_some()||n.max_future_skew_micros.is_some())&&(n.kind!="memory_source"||n.event_time_field.is_none()) {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,format!("node {id}: source watermark options require memory_source.event_time_field")));
        }
        if (!router && !n.best_effort.is_empty()) || n.best_effort.iter().any(|d| !n.out.contains(d))
            || n.best_effort.iter().collect::<HashSet<_>>().len() != n.best_effort.len() {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, format!("node {id}: invalid best_effort edges")));
        }
        if !matches!(n.kind.as_str(), "route" | "switch") && (n.routes.is_some() || n.route_mode.is_some() || n.default_out.is_some()) {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, format!("node {id}: routing options on a non-route node")));
        }
        if lossy.contains(&id) && matches!(n.kind.as_str(), "capture_sink" | "union_all") {
            return Err(SparrowError::new(ErrorCode::PolicyDenied, format!("node {id}: a lossy branch cannot merge or reach a required sink")));
        }
        if n.kind=="best_effort_sink"&&!lossy.contains(&id){return Err(SparrowError::new(ErrorCode::PolicyDenied,format!("node {id}: best_effort_sink requires an explicit lossy branch edge")));}
        for dest in &n.out {
            if lossy.contains(&id) || n.best_effort.contains(dest) || n.side_output.as_ref().is_some_and(|s|s.to==*dest&&s.full==crate::graph::SideOutputFull::Drop) { lossy.insert(*dest); }
        }
    }
    Ok(())
}

fn reject_cycles(spec: &GraphSpec) -> Result<()> {
    let mut visiting = HashSet::new();
    let mut done = HashSet::new();
    let by_id: HashMap<u32, &NodeSpec> = spec.nodes.iter().map(|n| (n.id, n)).collect();
    fn dfs(
        id: u32,
        by_id: &HashMap<u32, &NodeSpec>,
        visiting: &mut HashSet<u32>,
        done: &mut HashSet<u32>,
    ) -> Result<()> {
        if done.contains(&id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "GraphSpec contains a cycle",
            ));
        }
        if let Some(n) = by_id.get(&id) {
            for d in &n.out {
                dfs(*d, by_id, visiting, done)?;
            }
        }
        visiting.remove(&id);
        done.insert(id);
        Ok(())
    }
    for n in &spec.nodes {
        dfs(n.id, &by_id, &mut visiting, &mut done)?;
    }
    Ok(())
}

fn topo(spec: &GraphSpec) -> Result<Vec<u32>> {
    let mut inbound: HashMap<u32, u32> = spec.nodes.iter().map(|n| (n.id, 0)).collect();
    for n in &spec.nodes {
        for d in &n.out {
            *inbound.get_mut(d).unwrap() += 1;
        }
    }
    let mut ready: Vec<u32> = inbound
        .iter()
        .filter(|(_, c)| **c == 0)
        .map(|(id, _)| *id)
        .collect();
    ready.sort_unstable();
    let mut out = Vec::new();
    let by_id: HashMap<u32, &NodeSpec> = spec.nodes.iter().map(|n| (n.id, n)).collect();
    while let Some(id) = ready.pop() {
        out.push(id);
        if let Some(n) = by_id.get(&id) {
            for d in &n.out {
                let e = inbound.get_mut(d).unwrap();
                *e -= 1;
                if *e == 0 {
                    ready.push(*d);
                }
            }
        }
    }
    if out.len() != spec.nodes.len() {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "GraphSpec is not a DAG",
        ));
    }
    Ok(out)
}

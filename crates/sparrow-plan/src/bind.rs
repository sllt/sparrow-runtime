//! Bind a [`GraphSpec`] into [`BoundLogicalPlan`]. SQL uses the same output type.

use std::collections::{HashMap, HashSet};

use crate::bound::{validate_predicate, BoundKind, BoundLogicalPlan, BoundNode};
use crate::catalog::{project_schema, Catalog};
use crate::graph::{GraphSpec, NodeSpec};
use crate::stateful::{
    lookup_output_schema, window_output_schema, AggCall, DedupSpec, LookupSpec, WindowSpec,
};
use sparrow_expr::{infer_type, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{AggFn, OperatorId, PipelineId, RevisionId, Schema, SchemaId, WindowKind};

pub fn bind_graph(spec: &GraphSpec, catalog: &Catalog) -> Result<BoundLogicalPlan> {
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
    reject_fan_in(spec)?;

    let order = topo(spec)?;
    let by_id: HashMap<u32, &NodeSpec> = spec.nodes.iter().map(|n| (n.id, n)).collect();
    let mut incoming_schema: HashMap<u32, Schema> = HashMap::new();
    let mut bound = Vec::new();
    let mut next_schema = 100u32;

    for id in order {
        let node = by_id[&id];
        let kind = match node.kind.as_str() {
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
                BoundKind::Filter { predicate, input }
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
                    fields.push((named.alias.clone(), ty, true));
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
            "capture_sink" => {
                let schema = incoming_schema.get(&id).cloned().ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, format!("sink {id} has no input"))
                })?;
                BoundKind::CaptureSink {
                    name: node.name.clone().unwrap_or_else(|| "capture".into()),
                    schema,
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

        for dest in &node.out {
            if incoming_schema.contains_key(dest) && !matches!(kind, BoundKind::MemorySource { .. } | BoundKind::Filter { .. } | BoundKind::Project { .. } | BoundKind::Map { .. }) {
                // sink has no output schema to propagate
            }
            if !matches!(kind, BoundKind::CaptureSink { .. }) {
                incoming_schema
                    .entry(*dest)
                    .or_insert_with(|| kind.output_schema().clone());
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
        .filter(|n| matches!(n.kind, BoundKind::CaptureSink { .. }))
        .count();
    if sources != 1 || sinks != 1 {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("V0.3 linear plans must have exactly one source and one sink (got {sources}/{sinks})"),
        ));
    }

    Ok(BoundLogicalPlan {
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

fn reject_fan_in(spec: &GraphSpec) -> Result<()> {
    let mut inbound: HashMap<u32, u32> = HashMap::new();
    for n in &spec.nodes {
        for d in &n.out {
            *inbound.entry(*d).or_insert(0) += 1;
            if inbound[d] > 1 {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    format!("fan-in on node {d} is not part of V0.3 (multi-input watermark is an operator API, not Graph fan-in)"),
                ));
            }
        }
        if n.out.len() > 1 {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                    format!("fan-out on node {} is not part of V0.3", n.id),
            ));
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

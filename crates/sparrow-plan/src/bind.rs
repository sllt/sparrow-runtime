//! Bind a [`GraphSpec`] into [`BoundLogicalPlan`]. SQL uses the same output type.

use std::collections::{HashMap, HashSet};

use crate::bound::{validate_predicate, BoundKind, BoundLogicalPlan, BoundNode};
use crate::catalog::{project_schema, Catalog};
use crate::graph::{GraphSpec, NodeSpec};
use sparrow_expr::{infer_type, Expr};
use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::{OperatorId, PipelineId, RevisionId, Schema, SchemaId};

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
            other => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    format!("node kind '{other}' is not part of V0.1"),
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
            format!("V0.1 plans must have exactly one source and one sink (got {sources}/{sinks})"),
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
    let mut id = 1u32;

    let source_id = OperatorId::new(id);
    id += 1;
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
        let fid = OperatorId::new(id);
        id += 1;
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
        let pid = OperatorId::new(id);
        id += 1;
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
        let mid = OperatorId::new(id);
        id += 1;
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
    let sid = OperatorId::new(id);
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
                    format!("fan-in on node {d} is not part of V0.1"),
                ));
            }
        }
        if n.out.len() > 1 {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("fan-out on node {} is not part of V0.1", n.id),
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

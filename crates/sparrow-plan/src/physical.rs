//! Physical plan: linear ExecutionChain stages. Adjacent
//! Filter → Project → Map fuse into one transform stage.

use crate::bound::{BoundKind, BoundLogicalPlan, BoundNode};
use crate::stateful::{DedupSpec, IotSpec, LookupSpec, WindowSpec};
use sparrow_expr::Expr;
use sparrow_model::{DeliveryContract, OperatorId, PipelineId, RecoveryPolicy, RevisionId, Schema};

#[derive(Clone, Debug, PartialEq)]
pub struct PlanOptions {
    pub fuse: bool,
}

impl Default for PlanOptions {
    fn default() -> Self {
        Self { fuse: true }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan {
    pub pipeline: PipelineId,
    pub revision: RevisionId,
    pub stages: Vec<PhysicalStage>,
    /// None retains the original linear ABI/fast path. Explicit graph edges
    /// index physical chains; edge order is stable, never HashMap iteration.
    pub edges: Option<Vec<PhysicalEdge>>,
    pub side_outputs: Vec<(usize, crate::graph::SideOutputSpec)>,
    pub source_times: Vec<(OperatorId, sparrow_model::EventTimeBinding)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalEdge {
    pub from: usize,
    pub to: usize,
    pub port: OperatorId,
    pub best_effort: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalStage {
    Analysis {
        operator: OperatorId,
        plan: Box<crate::AnalysisPlan>,
    },
    Branch {
        operator: OperatorId,
        input: Schema,
    },
    Route {
        operator: OperatorId,
        input: Schema,
        mode: crate::graph::RouteMode,
        cases: Vec<(Expr, OperatorId)>,
        default: OperatorId,
    },
    UnionAll {
        operator: OperatorId,
        input: Schema,
    },
    BestEffortSink {
        operator: OperatorId,
        name: String,
        schema: Schema,
    },
    MemorySource {
        operator: OperatorId,
        name: String,
        schema: Schema,
    },
    Transform {
        steps: Vec<TransformStep>,
    },
    CaptureSink {
        operator: OperatorId,
        name: String,
        schema: Schema,
    },
    WindowAgg {
        operator: OperatorId,
        spec: WindowSpec,
        input: Schema,
        output: Schema,
    },
    Deduplicate {
        operator: OperatorId,
        spec: DedupSpec,
        input: Schema,
    },
    Lookup {
        operator: OperatorId,
        spec: LookupSpec,
        input: Schema,
        output: Schema,
    },
    /// Keyed IoT value state. It is not a WindowAgg and has its own state
    /// codec/participant identity when a recovery profile opts in.
    Iot {
        operator: OperatorId,
        spec: IotSpec,
        input: Schema,
        output: Schema,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransformStep {
    Filter {
        operator: OperatorId,
        predicate: Expr,
        input: Schema,
    },
    Project {
        operator: OperatorId,
        exprs: Vec<Expr>,
        input: Schema,
        output: Schema,
    },
    Map {
        operator: OperatorId,
        exprs: Vec<Expr>,
        input: Schema,
        output: Schema,
    },
}

impl PhysicalPlan {
    pub fn has_analysis(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, PhysicalStage::Analysis { .. }))
    }
    pub fn has_extended_aggs(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s,PhysicalStage::WindowAgg{spec,..} if spec.has_extended_aggs()))
    }
    /// Cold admission only. Reuse the strict linear silence shape validator,
    /// without minting/exposing a checkpoint identity for the live plan.
    pub fn live_silence_gap(&self) -> sparrow_model::Result<i64> {
        use crate::{IotTimingSpec, SilenceClockPolicy};
        let denied = || {
            sparrow_model::SparrowError::new(sparrow_model::ErrorCode::UnsupportedRestore,
            "live silence requires Source->Silence->[pure Transform]->Sink; live clock and 100ms..120s observation gap")
        };
        if self.edges.is_some() {
            return Err(denied());
        }
        let mut shape = self.clone();
        let Some(PhysicalStage::Iot { spec, .. }) = shape.stages.get_mut(1) else {
            return Err(denied());
        };
        let Some(IotTimingSpec::Silence {
            clock,
            max_observation_gap_micros,
            ..
        }) = &mut spec.timing
        else {
            return Err(denied());
        };
        if *clock != SilenceClockPolicy::Live
            || !(100_000..=120_000_000).contains(max_observation_gap_micros)
        {
            return Err(denied());
        }
        let gap = *max_observation_gap_micros;
        *clock = SilenceClockPolicy::Paused;
        crate::CheckpointPlan::from_physical(&shape)?;
        Ok(gap)
    }
    pub fn edge_pairs(&self) -> Vec<(usize, usize)> {
        match &self.edges {
            Some(edges) => edges.iter().map(|e| (e.from, e.to)).collect(),
            None => (0..self.stages.len().saturating_sub(1))
                .map(|i| (i, i + 1))
                .collect(),
        }
    }
    /// The current barrier ACK/restore protocol has one state participant.
    /// Keep this gate shared by API validation, layout construction and Kernel
    /// admission so embedded callers cannot bypass the control-plane check.
    pub fn aligned_window(&self) -> sparrow_model::Result<(OperatorId, &WindowSpec, &Schema)> {
        use sparrow_model::{ErrorCode, SparrowError};
        if self.has_plugins(){return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"native plugins have no aligned window profile"));}
        if self.has_analysis() || self.has_extended_aggs() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "analysis operators/extended aggregates are restart_fresh only",
            ));
        }
        if self.edges.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "DAG requires the graph checkpoint protocol",
            ));
        }
        let mut window = None;
        for stage in &self.stages {
            match stage {
                PhysicalStage::WindowAgg {
                    operator,
                    spec,
                    input,
                    ..
                } => {
                    if spec.kind.uses_processing_time_timer() || spec.kind.is_new_window() {
                        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                            "processing-time windows cannot use recovery=aligned (no PT timer on the aligned path)"));
                    }
                    if window.is_some() {
                        return Err(SparrowError::new(ErrorCode::FeatureUnavailable,
                            "aligned recovery supports exactly one window; multi-participant snapshots are not implemented"));
                    }
                    window = Some((*operator, spec, input));
                }
                PhysicalStage::Deduplicate { .. } | PhysicalStage::Lookup { .. } => {
                    return Err(SparrowError::new(
                        ErrorCode::FeatureUnavailable,
                        "aligned recovery does not snapshot Dedup/Lookup; refuse dishonest strip",
                    ));
                }
                PhysicalStage::Iot { .. } => {
                    return Err(SparrowError::new(ErrorCode::FeatureUnavailable,
                        "legacy PlanLayout cannot represent IoT state; use the IoT checkpoint participant codec"));
                }
                _ => {}
            }
        }
        window.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "aligned recovery requires a window operator in the plan",
            )
        })
    }

    pub fn source_schema(&self) -> Option<&Schema> {
        self.stages.iter().find_map(|s| match s {
            PhysicalStage::MemorySource { schema, .. } => Some(schema),
            _ => None,
        })
    }

    pub fn mailbox_count(&self) -> usize {
        self.edges
            .as_ref()
            .map_or_else(|| self.stages.len().saturating_sub(1), Vec::len)
    }

    pub fn fused(&self) -> bool {
        self.stages.iter().any(|s| match s {
            PhysicalStage::Transform { steps } => steps.len() > 1,
            _ => false,
        })
    }

    pub fn has_processing_time_window(&self) -> bool {
        self.stages.iter().any(|s| matches!(s,PhysicalStage::WindowAgg {spec,..} if spec.kind.uses_processing_time_timer()))
    }

    pub fn has_new_windows(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s,PhysicalStage::WindowAgg {spec,..} if spec.kind.is_new_window()))
    }

    pub fn has_event_time_window(&self) -> bool {
        self.stages.iter().any(
            |s| matches!(s, PhysicalStage::WindowAgg { spec, .. } if spec.kind.uses_event_time()),
        )
    }

    pub fn has_stream_join(&self) -> bool {
        self.stages.iter().any(|s| matches!(s,
            PhysicalStage::Analysis { plan, .. } if plan.is_join()))
    }

    /// Durable analysis graphs reuse the logged, ordered graph decisions,
    /// including graphs with only UNNEST/Union and no time window.
    pub fn is_recovery_time_graph(&self) -> bool {
        self.edges.is_some()
            && (self.has_processing_time_state() || self.has_event_time_window() || self.has_analysis())
    }

    pub fn recovery_event_time(&self) -> bool {
        self.has_event_time_window() || self.has_stream_join()
            || (self.has_analysis() && !self.source_times.is_empty())
    }

    pub fn event_time_binding(&self) -> Option<sparrow_model::EventTimeBinding> {
        self.stages.iter().find_map(|s| match s {
            PhysicalStage::WindowAgg { spec, .. } => spec.binding(),
            _ => None,
        })
    }

    pub fn has_count_window(&self) -> bool {
        self.stages.iter().any(|s| {
            matches!(
                s,
                PhysicalStage::WindowAgg {
                    spec,
                    ..
                } if spec.kind.is_count()
            )
        })
    }

    pub fn has_iot(&self) -> bool {
        self.stages
            .iter()
            .any(|stage| matches!(stage, PhysicalStage::Iot { .. }))
    }

    pub fn has_timed_iot(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, PhysicalStage::Iot { spec, .. } if spec.timing.is_some()))
    }

    /// Silence states are judged from fresh source observations, so the plan
    /// must not hide them behind an upstream transform or another state.
    pub fn has_silence(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, PhysicalStage::Iot { spec, .. } if spec.is_silence()))
    }

    pub fn has_resample(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, PhysicalStage::Iot { spec, .. } if spec.is_resample()))
    }

    /// These states need source-ordered, durable time when recovery is aligned.
    /// Live PT/TTL jobs retain their existing restart-fresh clock contract.
    pub fn has_processing_time_state(&self) -> bool {
        self.has_processing_time_window() || self.stages.iter().any(|s|
            matches!(s, PhysicalStage::Iot { spec, .. } if spec.timing.is_some() || spec.ttl_micros > 0))
    }

    /// Honesty label for the plan under `recovery`.
    ///
    /// Does not cite a milestone contract (V0.3 etc.). Windowed
    /// `restart_fresh` jobs advertise `none`; aligned jobs advertise `aligned`.
    pub fn recovery_label_for(&self, recovery: RecoveryPolicy) -> &'static str {
        if recovery.is_aligned() {
            return RecoveryPolicy::Aligned.as_str();
        }
        if self.has_processing_time_window()
            || self.has_event_time_window()
            || self.has_count_window()
        {
            RecoveryPolicy::RestartFresh.none_label()
        } else {
            RecoveryPolicy::RestartFresh.as_str()
        }
    }

    pub fn recovery_label(&self) -> &'static str {
        self.recovery_label_for(RecoveryPolicy::RestartFresh)
    }

    pub fn honesty(&self) -> &'static str {
        if self.has_event_time_window() {
            DeliveryContract::ET_WINDOW_HONESTY
        } else if self.has_processing_time_window() {
            DeliveryContract::PT_WINDOW_HONESTY
        } else {
            "V1 default is live_best_effort + restart_fresh. Aligned checkpoint is opt-in for ReplayableSource (not exactly-once). MQTT without replay cannot pretend durable restore."
        }
    }
}

pub fn physicalize(plan: &BoundLogicalPlan, opts: &PlanOptions) -> PhysicalPlan {
    if !plan.source_times.is_empty()
        || !plan.side_outputs.is_empty()
        || plan.nodes.iter().any(|n| {
            n.downstream.len() > 1
                || matches!(
                    n.kind,
                    BoundKind::Branch { .. }
                        | BoundKind::Route { .. }
                        | BoundKind::UnionAll { .. }
                        | BoundKind::BestEffortSink { .. }
                )
        })
        || plan
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, BoundKind::MemorySource { .. }))
            .count()
            > 1
    {
        return physicalize_graph(plan, opts);
    }
    physicalize_linear(plan, opts)
}

fn physicalize_linear(plan: &BoundLogicalPlan, opts: &PlanOptions) -> PhysicalPlan {
    let ordered = linearize(&plan.nodes);
    let mut stages = Vec::new();
    let mut pending: Vec<TransformStep> = Vec::new();

    let flush = |pending: &mut Vec<TransformStep>, stages: &mut Vec<PhysicalStage>| {
        if pending.is_empty() {
            return;
        }
        stages.push(PhysicalStage::Transform {
            steps: std::mem::take(pending),
        });
    };

    for node in ordered {
        match &node.kind {
            BoundKind::Analysis(plan) => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::Analysis {
                    operator: node.id,
                    plan: plan.clone(),
                });
            }
            BoundKind::Branch { input, .. } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::Branch {
                    operator: node.id,
                    input: input.clone(),
                });
            }
            BoundKind::Route {
                input,
                mode,
                cases,
                default,
                ..
            } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::Route {
                    operator: node.id,
                    input: input.clone(),
                    mode: *mode,
                    cases: cases.clone(),
                    default: *default,
                });
            }
            BoundKind::UnionAll { input } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::UnionAll {
                    operator: node.id,
                    input: input.clone(),
                });
            }
            BoundKind::BestEffortSink { name, schema } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::BestEffortSink {
                    operator: node.id,
                    name: name.clone(),
                    schema: schema.clone(),
                });
            }
            BoundKind::MemorySource { name, schema } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::MemorySource {
                    operator: node.id,
                    name: name.clone(),
                    schema: schema.clone(),
                });
            }
            BoundKind::CaptureSink { name, schema } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::CaptureSink {
                    operator: node.id,
                    name: name.clone(),
                    schema: schema.clone(),
                });
            }
            BoundKind::Filter { predicate, input } => {
                let step = TransformStep::Filter {
                    operator: node.id,
                    predicate: predicate.clone(),
                    input: input.clone(),
                };
                if opts.fuse {
                    pending.push(step);
                } else {
                    flush(&mut pending, &mut stages);
                    stages.push(PhysicalStage::Transform { steps: vec![step] });
                }
            }
            BoundKind::Project {
                exprs,
                input,
                output,
            } => {
                let step = TransformStep::Project {
                    operator: node.id,
                    exprs: exprs.clone(),
                    input: input.clone(),
                    output: output.clone(),
                };
                if opts.fuse {
                    pending.push(step);
                } else {
                    flush(&mut pending, &mut stages);
                    stages.push(PhysicalStage::Transform { steps: vec![step] });
                }
            }
            BoundKind::Map {
                exprs,
                input,
                output,
            } => {
                let step = TransformStep::Map {
                    operator: node.id,
                    exprs: exprs.clone(),
                    input: input.clone(),
                    output: output.clone(),
                };
                if opts.fuse {
                    pending.push(step);
                } else {
                    flush(&mut pending, &mut stages);
                    stages.push(PhysicalStage::Transform { steps: vec![step] });
                }
            }
            BoundKind::WindowAgg {
                spec,
                input,
                output,
            } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::WindowAgg {
                    operator: node.id,
                    spec: spec.clone(),
                    input: input.clone(),
                    output: output.clone(),
                });
            }
            BoundKind::Deduplicate { spec, input } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::Deduplicate {
                    operator: node.id,
                    spec: spec.clone(),
                    input: input.clone(),
                });
            }
            BoundKind::Lookup {
                spec,
                input,
                output,
            } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::Lookup {
                    operator: node.id,
                    spec: spec.clone(),
                    input: input.clone(),
                    output: output.clone(),
                });
            }
            BoundKind::Iot {
                spec,
                input,
                output,
            } => {
                flush(&mut pending, &mut stages);
                stages.push(PhysicalStage::Iot {
                    operator: node.id,
                    spec: spec.clone(),
                    input: input.clone(),
                    output: output.clone(),
                });
            }
        }
    }
    flush(&mut pending, &mut stages);
    PhysicalPlan {
        pipeline: plan.pipeline,
        revision: plan.revision,
        stages,
        edges: None,
        side_outputs: vec![],
        source_times: vec![],
    }
}

fn physicalize_graph(plan: &BoundLogicalPlan, opts: &PlanOptions) -> PhysicalPlan {
    use std::collections::BTreeMap;
    let mut stages = Vec::new();
    let mut indices = BTreeMap::new();
    for node in &plan.nodes {
        let single = BoundLogicalPlan {
            pipeline: plan.pipeline,
            revision: plan.revision,
            nodes: vec![BoundNode {
                downstream: vec![],
                ..node.clone()
            }],
            side_outputs: vec![],
            source_times: vec![],
        };
        let mut stage = physicalize_linear(&single, &PlanOptions { fuse: false })
            .stages
            .remove(0);
        let parents: Vec<_> = plan
            .nodes
            .iter()
            .filter(|n| n.downstream.contains(&node.id))
            .collect();
        if opts.fuse && parents.len() == 1 && parents[0].downstream.len() == 1 {
            if let Some(&index) = indices.get(&parents[0].id) {
                if let (
                    Some(PhysicalStage::Transform { steps: before }),
                    PhysicalStage::Transform { steps: after },
                ) = (stages.get_mut(index), &mut stage)
                {
                    before.append(after);
                    indices.insert(node.id, index);
                    continue;
                }
            }
        }
        indices.insert(node.id, stages.len());
        stages.push(stage);
    }
    let mut edges = Vec::new();
    for node in &plan.nodes {
        let best_effort = match &node.kind {
            BoundKind::Branch { best_effort, .. } | BoundKind::Route { best_effort, .. } => {
                best_effort.as_slice()
            }
            _ => &[],
        };
        for dest in &node.downstream {
            let from = indices[&node.id];
            let to = indices[dest];
            let side_drop = plan.side_outputs.iter().any(|(id, s)| {
                *id == node.id && s.to == dest.raw() && s.full == crate::graph::SideOutputFull::Drop
            });
            if from != to {
                edges.push(PhysicalEdge {
                    from,
                    to,
                    port: *dest,
                    best_effort: best_effort.contains(dest) || side_drop,
                });
            }
        }
    }
    PhysicalPlan {
        pipeline: plan.pipeline,
        revision: plan.revision,
        stages,
        edges: Some(edges),
        side_outputs: plan
            .side_outputs
            .iter()
            .map(|(id, s)| (indices[id], s.clone()))
            .collect(),
        source_times: plan.source_times.clone(),
    }
}

fn linearize(nodes: &[BoundNode]) -> Vec<&BoundNode> {
    let by_id: std::collections::HashMap<_, _> = nodes.iter().map(|n| (n.id, n)).collect();
    let inbound: std::collections::HashSet<_> = nodes
        .iter()
        .flat_map(|n| n.downstream.iter().copied())
        .collect();
    let mut cur = nodes.iter().find(|n| !inbound.contains(&n.id));
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    while let Some(n) = cur {
        if !seen.insert(n.id) {
            break;
        }
        out.push(n);
        cur = n.downstream.first().and_then(|id| by_id.get(id).copied());
    }
    out
}

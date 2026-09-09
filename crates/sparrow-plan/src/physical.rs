//! Physical plan: linear ExecutionChain stages. Adjacent
//! Filter → Project → Map fuse into one transform stage.

use crate::bound::{BoundKind, BoundLogicalPlan, BoundNode};
use crate::stateful::{DedupSpec, LookupSpec, WindowSpec};
use sparrow_expr::Expr;
use sparrow_model::{DeliveryContract, OperatorId, PipelineId, RevisionId, Schema};

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
}

#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalStage {
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
    pub fn source_schema(&self) -> Option<&Schema> {
        self.stages.iter().find_map(|s| match s {
            PhysicalStage::MemorySource { schema, .. } => Some(schema),
            _ => None,
        })
    }

    pub fn mailbox_count(&self) -> usize {
        self.stages.len().saturating_sub(1)
    }

    pub fn fused(&self) -> bool {
        self.stages.iter().any(|s| match s {
            PhysicalStage::Transform { steps } => steps.len() > 1,
            _ => false,
        })
    }

    pub fn has_processing_time_window(&self) -> bool {
        self.stages.iter().any(|s| {
            matches!(
                s,
                PhysicalStage::WindowAgg {
                    spec: WindowSpec {
                        kind: sparrow_model::WindowKind::TumblingProcessingTime { .. },
                        ..
                    },
                    ..
                }
            )
        })
    }

    pub fn has_event_time_window(&self) -> bool {
        self.stages.iter().any(|s| {
            matches!(s, PhysicalStage::WindowAgg { spec, .. } if spec.kind.uses_event_time())
        })
    }

    pub fn event_time_binding(&self) -> Option<sparrow_model::EventTimeBinding> {
        self.stages.iter().find_map(|s| match s {
            PhysicalStage::WindowAgg { spec, .. } => spec.binding(),
            _ => None,
        })
    }

    pub fn recovery_label(&self) -> &'static str {
        if self.has_processing_time_window() || self.has_event_time_window() {
            DeliveryContract::V0_3.recovery.none_label()
        } else {
            DeliveryContract::V0_3.recovery.as_str()
        }
    }

    pub fn honesty(&self) -> &'static str {
        if self.has_event_time_window() {
            DeliveryContract::ET_WINDOW_HONESTY
        } else if self.has_processing_time_window() {
            DeliveryContract::PT_WINDOW_HONESTY
        } else {
            "V0.4 default is live_best_effort + restart_fresh. Experimental aligned checkpoint is opt-in (not exactly-once). MQTT without replay cannot pretend durable restore."
        }
    }
}

pub fn physicalize(plan: &BoundLogicalPlan, opts: &PlanOptions) -> PhysicalPlan {
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
        }
    }
    flush(&mut pending, &mut stages);
    PhysicalPlan {
        pipeline: plan.pipeline,
        revision: plan.revision,
        stages,
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

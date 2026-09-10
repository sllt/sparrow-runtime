//! Graph / physical explain: fusion, time, state, guarantee.
//!
//! This is the offline Graph authoring surface (not a Web Designer).

use crate::bound::{BoundKind, BoundLogicalPlan};
use crate::graph::GraphSpec;
use crate::physical::{physicalize, PhysicalPlan, PhysicalStage, PlanOptions};
use crate::{bind_graph, Catalog};
use sparrow_model::{DeliveryContract, DeliveryGuarantee, RecoveryPolicy, Result};

#[derive(Clone, Debug, PartialEq)]
pub struct GraphExplain {
    pub accepted: bool,
    pub stages: Vec<String>,
    pub physical: Vec<String>,
    pub fused: bool,
    pub fusion: String,
    pub mailbox_count: usize,
    pub time: String,
    pub state: String,
    pub guarantee: String,
    pub delivery: &'static str,
    pub recovery: &'static str,
    pub replay: &'static str,
    pub honesty: &'static str,
    pub experimental: bool,
}

/// Replay is not on the physical plan. Graph/SQL explain without a
/// pipeline source cannot pretend a connector is bound.
pub const REPLAY_UNBOUND: &str =
    "unbound — physical plan has no connector; MQTT/HTTP=unsupported, file=replayable";

impl GraphExplain {
    pub fn from_plan(plan: &PhysicalPlan) -> Self {
        Self::from_plan_with(plan, RecoveryPolicy::RestartFresh, REPLAY_UNBOUND)
    }

    pub fn from_plan_with(
        plan: &PhysicalPlan,
        recovery: RecoveryPolicy,
        replay: &'static str,
    ) -> Self {
        let stages = plan
            .stages
            .iter()
            .map(stage_label)
            .collect::<Vec<_>>();
        let physical = plan
            .stages
            .iter()
            .enumerate()
            .map(|(i, s)| format!("{i}:{}", stage_label(s)))
            .collect();
        let fused = plan.fused();
        let fusion = if fused {
            "adjacent Filter→Project→Map fused into one transform stage".into()
        } else {
            "no transform fusion".into()
        };
        let time = if plan.has_event_time_window() {
            format!(
                "event-time ({})",
                plan.event_time_binding()
                    .map(|b| b.field)
                    .unwrap_or_else(|| "bound".into())
            )
        } else if plan.has_processing_time_window() {
            "processing-time".into()
        } else if plan.has_count_window() {
            "count / arrival-order (not event-time)".into()
        } else {
            "none".into()
        };
        let state = describe_state(plan);
        let experimental = false;
        let recovery = plan.recovery_label_for(recovery);
        let guarantee = format!(
            "{} + recovery={} (aligned checkpoint is opt-in for ReplayableSource, not exactly-once)",
            DeliveryGuarantee::LiveBestEffort.as_str(),
            recovery
        );
        GraphExplain {
            accepted: true,
            stages,
            physical,
            fused,
            fusion,
            mailbox_count: plan.mailbox_count(),
            time,
            state,
            guarantee,
            delivery: DeliveryGuarantee::LiveBestEffort.as_str(),
            recovery,
            replay,
            honesty: plan.honesty(),
            experimental,
        }
    }
}

fn describe_state(plan: &PhysicalPlan) -> String {
    let mut bits = Vec::new();
    for s in &plan.stages {
        match s {
            PhysicalStage::WindowAgg { spec, operator, .. } => {
                bits.push(format!(
                    "window_agg op={} keys={} kind={:?}",
                    operator.raw(),
                    spec.keys.join(","),
                    spec.kind
                ));
            }
            PhysicalStage::Deduplicate { spec, operator, .. } => {
                bits.push(format!(
                    "dedup op={} keys={} ttl={} max_keys={}",
                    operator.raw(),
                    spec.keys.join(","),
                    spec.ttl_micros,
                    spec.max_keys
                ));
            }
            PhysicalStage::Lookup { spec, operator, .. } => {
                bits.push(format!("lookup op={} table={}", operator.raw(), spec.table));
            }
            _ => {}
        }
    }
    if bits.is_empty() {
        "stateless transforms".into()
    } else {
        bits.join("; ")
    }
}

fn stage_label(s: &PhysicalStage) -> String {
    match s {
        PhysicalStage::MemorySource { name, .. } => format!("source:{name}"),
        PhysicalStage::Transform { steps } => {
            let kinds: Vec<&str> = steps
                .iter()
                .map(|st| match st {
                    crate::physical::TransformStep::Filter { .. } => "filter",
                    crate::physical::TransformStep::Project { .. } => "project",
                    crate::physical::TransformStep::Map { .. } => "map",
                })
                .collect();
            format!("transform:{}", kinds.join("+"))
        }
        PhysicalStage::CaptureSink { name, .. } => format!("sink:{name}"),
        PhysicalStage::WindowAgg { spec, .. } => format!("window:{:?}", spec.kind),
        PhysicalStage::Deduplicate { .. } => "dedup".into(),
        PhysicalStage::Lookup { spec, .. } => format!("lookup:{}", spec.table),
    }
}

/// Bind + physicalize a GraphSpec (catalog may be embedded on the spec).
pub fn validate_graph(spec: &GraphSpec, catalog: &Catalog) -> Result<BoundLogicalPlan> {
    bind_graph(spec, catalog)
}

pub fn explain_graph(spec: &GraphSpec, catalog: &Catalog) -> Result<GraphExplain> {
    let bound = bind_graph(spec, catalog)?;
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    Ok(GraphExplain::from_plan(&plan))
}

pub fn explain_bound(bound: &BoundLogicalPlan) -> GraphExplain {
    let plan = physicalize(bound, &PlanOptions { fuse: true });
    GraphExplain::from_plan(&plan)
}

/// Minimal offline ET tumble template used by the Graph authoring CLI.
pub fn et_tumble_template() -> &'static str {
    r#"{
  "version": 1,
  "pipeline_id": 1,
  "revision_id": 1,
  "catalog": [
    {
      "name": "sensors",
      "fields": [
        {"name": "device_id", "type": "utf8", "nullable": false},
        {"name": "temperature", "type": "float64", "nullable": true},
        {"name": "ts", "type": "int64", "nullable": false}
      ]
    }
  ],
  "nodes": [
    {"id": 1, "kind": "memory_source", "table": "sensors", "out": [2]},
    {
      "id": 2,
      "kind": "window_agg",
      "keys": ["device_id"],
      "event_time_field": "ts",
      "lateness_micros": 3000000,
      "window": {"kind": "tumble_et", "size_micros": 10000000},
      "aggs": [{"fn": "avg", "expr": {"k": "col", "name": "temperature"}, "alias": "avg_t"}],
      "out": [3]
    },
    {"id": 3, "kind": "capture_sink", "name": "capture"}
  ]
}
"#
}

pub fn bound_kinds(bound: &BoundLogicalPlan) -> Vec<String> {
    bound
        .nodes
        .iter()
        .map(|n| match &n.kind {
            BoundKind::MemorySource { name, .. } => format!("source:{name}"),
            BoundKind::Filter { .. } => "filter".into(),
            BoundKind::Project { .. } => "project".into(),
            BoundKind::Map { .. } => "map".into(),
            BoundKind::WindowAgg { spec, .. } => format!("window:{:?}", spec.kind),
            BoundKind::Deduplicate { .. } => "dedup".into(),
            BoundKind::Lookup { spec, .. } => format!("lookup:{}", spec.table),
            BoundKind::CaptureSink { name, .. } => format!("sink:{name}"),
        })
        .collect()
}

pub fn contract_honesty() -> &'static str {
    DeliveryContract::ET_WINDOW_HONESTY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explains_v03_et_window_graph() {
        let spec = GraphSpec::from_json(et_tumble_template()).unwrap();
        let report = explain_graph(&spec, &Catalog::new()).unwrap();
        assert!(report.accepted);
        assert!(report.time.contains("event-time"));
        assert!(report.state.contains("window_agg"));
        assert!(report.guarantee.contains("live_best_effort"));
        assert!(report.honesty.contains("event-time"));
        assert!(report.physical.iter().any(|s| s.contains("window")));
        assert_eq!(report.recovery, "none");
        assert_eq!(report.replay, REPLAY_UNBOUND);
        assert!(
            !report.guarantee.contains("V0_3") && !report.recovery.contains("V0"),
            "recovery_label must not cite a milestone contract: {}",
            report.guarantee
        );
    }

    #[test]
    fn p3_44_recovery_label_follows_policy_not_v0_3() {
        let spec = GraphSpec::from_json(et_tumble_template()).unwrap();
        let bound = crate::bind_graph(&spec, &Catalog::new()).unwrap();
        let plan = crate::physicalize(&bound, &crate::PlanOptions { fuse: true });
        assert_eq!(
            plan.recovery_label_for(RecoveryPolicy::RestartFresh),
            "none"
        );
        assert_eq!(plan.recovery_label_for(RecoveryPolicy::Aligned), "aligned");
        let aligned = GraphExplain::from_plan_with(&plan, RecoveryPolicy::Aligned, "replayable");
        assert_eq!(aligned.recovery, "aligned");
        assert_eq!(aligned.replay, "replayable");
        assert!(!aligned.guarantee.contains("V0_3"));
    }
}

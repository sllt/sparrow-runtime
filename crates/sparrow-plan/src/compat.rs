//! V1 state-reuse white-list.
//!
//! Safe changes may reuse committed operator state. Changing a filter
//! (WHERE) *before* a window defaults to reset + replay. OperatorId
//! remapping without an explicit map is rejected.

use sparrow_expr::Expr;
use sparrow_model::{
    ErrorCode, OperatorId, Result, SparrowError, StateSlotId, StateSlotKey, WindowKind,
};

use crate::stateful::WindowSpec;
use crate::{BoundKind, BoundLogicalPlan};

/// Decision for whether a new plan may load a committed checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateReuse {
    /// Layout is white-listed; restore may proceed after codec checks.
    Reuse,
    /// Caller must discard state and replay from the source origin.
    ResetReplay { reason: String },
    /// Incompatible; restore is a hard reject.
    Reject { reason: String },
}

impl StateReuse {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reuse => "reuse",
            Self::ResetReplay { .. } => "reset_replay",
            Self::Reject { .. } => "reject",
        }
    }

    pub fn into_result(self) -> Result<()> {
        match self {
            Self::Reuse => Ok(()),
            Self::ResetReplay { reason } => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("state reuse refused; default is reset/replay: {reason}"),
            )
            .context("decision", "reset_replay")),
            Self::Reject { reason } => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("state reuse rejected: {reason}"),
            )
            .context("decision", "reject")),
        }
    }
}

/// Frozen plan layout stored beside a checkpoint (versioned).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanLayout {
    pub operator: OperatorId,
    pub slot: StateSlotId,
    pub window_kind: u8,
    pub keys: Vec<String>,
    pub aggs: Vec<String>,
    pub event_time_field: Option<String>,
    pub lateness_micros: i64,
    pub where_fingerprint: u64,
    pub table_name: Option<String>,
    pub table_revision: Option<u64>,
    /// Fingerprint of window size / slide / duration (R12).
    pub window_params_fingerprint: u64,
}

impl PlanLayout {
    pub fn slot_key(&self) -> StateSlotKey {
        StateSlotKey::new(self.operator, self.slot)
    }

    pub fn from_window(operator: OperatorId, slot: StateSlotId, spec: &WindowSpec) -> Self {
        Self {
            operator,
            slot,
            window_kind: window_kind_tag(spec.kind),
            keys: spec.keys.clone(),
            aggs: spec
                .aggs
                .iter()
                .map(|a| {
                    let input = a
                        .input
                        .as_ref()
                        .map(expr_canonical)
                        .unwrap_or_else(|| "*".into());
                    format!("{}:{}:{input}", a.func.as_str(), a.alias)
                })
                .collect(),
            event_time_field: spec.event_time_field.clone(),
            lateness_micros: spec.lateness_micros,
            where_fingerprint: 0,
            table_name: None,
            table_revision: None,
            window_params_fingerprint: window_params_fingerprint(&spec.kind),
        }
    }

    pub fn with_where(mut self, pred: Option<&Expr>) -> Self {
        self.where_fingerprint = pred.map(expr_fingerprint).unwrap_or(0);
        self
    }

    pub fn with_table(mut self, name: impl Into<String>, revision: u64) -> Self {
        self.table_name = Some(name.into());
        self.table_revision = Some(revision);
        self
    }
}

/// White-list: same operator/slot/window/keys/aggs/WHERE/table revision.
///
/// Allowed without reset: changing a *downstream* sink, log level, or
/// adding a trailing map after the window (not encoded here).
///
/// Default reset/replay: WHERE before the window changes.
pub fn decide_state_reuse(saved: &PlanLayout, live: &PlanLayout) -> StateReuse {
    if saved.operator != live.operator {
        return StateReuse::Reject {
            reason: format!(
                "OperatorId mapping mismatch: saved {} live {} (provide an explicit map or reset/replay)",
                saved.operator, live.operator
            ),
        };
    }
    if saved.slot != live.slot {
        return StateReuse::Reject {
            reason: format!(
                "StateSlotKey mismatch: saved slot {} live slot {}",
                saved.slot.raw(),
                live.slot.raw()
            ),
        };
    }
    if saved.window_kind != live.window_kind
        || saved.window_params_fingerprint != live.window_params_fingerprint
    {
        return StateReuse::Reject {
            reason: "window kind or size/slide/duration is not a white-listed compatible change"
                .into(),
        };
    }
    if saved.keys != live.keys || saved.aggs != live.aggs {
        return StateReuse::Reject {
            reason: "window keys or aggregates changed; state is not reusable".into(),
        };
    }
    if saved.event_time_field != live.event_time_field
        || saved.lateness_micros != live.lateness_micros
    {
        return StateReuse::Reject {
            reason: "event-time field or holdback L changed; state is not reusable".into(),
        };
    }
    if saved.where_fingerprint != live.where_fingerprint {
        return StateReuse::ResetReplay {
            reason: "WHERE / filter before the window changed; default is reset and replay".into(),
        };
    }
    if saved.table_name != live.table_name {
        return StateReuse::Reject {
            reason: "lookup table identity changed".into(),
        };
    }
    if saved.table_revision != live.table_revision {
        return StateReuse::Reject {
            reason: format!(
                "lookup table revision mismatch: saved {:?} live {:?}",
                saved.table_revision, live.table_revision
            ),
        };
    }
    StateReuse::Reuse
}

pub fn window_params_fingerprint(kind: &WindowKind) -> u64 {
    let s = match kind {
        WindowKind::TumblingProcessingTime { size_micros } => format!("pt:{size_micros}"),
        WindowKind::Count { size } => format!("count:{size}"),
        WindowKind::TumblingEventTime { size_micros } => format!("et:{size_micros}"),
        WindowKind::HoppingEventTime {
            size_micros,
            slide_micros,
        } => format!("hop:{size_micros}:{slide_micros}"),
    };
    fnv1a64(s.as_bytes())
}

pub fn window_kind_tag(kind: WindowKind) -> u8 {
    match kind {
        WindowKind::TumblingProcessingTime { .. } => 0,
        WindowKind::Count { .. } => 1,
        WindowKind::HoppingEventTime { .. } => 2,
        WindowKind::TumblingEventTime { .. } => 3,
    }
}

/// Structured fingerprint (not `Debug`) so layout hashes stay stable (P3-54).
pub fn expr_fingerprint(expr: &Expr) -> u64 {
    fnv1a64(expr_canonical(expr).as_bytes())
}

fn scalar_canonical(s: &sparrow_model::Scalar) -> String {
    use sparrow_model::Scalar;
    match s {
        Scalar::Null => "null".into(),
        Scalar::Bool(v) => format!("b:{v}"),
        Scalar::Int64(v) => format!("i:{v}"),
        Scalar::UInt64(v) => format!("u:{v}"),
        Scalar::Float64(v) => format!("f:{v}"),
        Scalar::Utf8(v) => format!("s:{v}"),
        Scalar::Bytes(v) => format!("bin:{}", v.len()),
        Scalar::TimestampMicrosUTC(v) => format!("ts:{v}"),
        Scalar::Dynamic(_) => "dyn".into(),
    }
}

fn expr_canonical(expr: &Expr) -> String {
    match expr {
        Expr::Column { name } => format!("col:{name}"),
        Expr::Literal(s) => format!("lit:{}:{}", s.data_type(), scalar_canonical(s)),
        Expr::Cast { expr, target } => format!("cast:{}:{}", expr_canonical(expr), target),
        Expr::TryCast { expr, target } => format!("trycast:{}:{}", expr_canonical(expr), target),
        Expr::Binary { op, left, right } => {
            format!(
                "bin:{}:{}:{}",
                op.as_tag(),
                expr_canonical(left),
                expr_canonical(right)
            )
        }
        Expr::IsNull(inner) => format!("isnull:{}", expr_canonical(inner)),
        Expr::IsNotNull(inner) => format!("isnotnull:{}", expr_canonical(inner)),
        Expr::Not(inner) => format!("not:{}", expr_canonical(inner)),
        Expr::Call { name, args } => {
            let a = args.iter().map(expr_canonical).collect::<Vec<_>>().join(",");
            format!("call:{name}:{a}")
        }
        Expr::DynamicGet { expr, key } => format!("dget:{}:{key}", expr_canonical(expr)),
    }
}

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// First filter that feeds a window in a linear plan, if any.
pub fn where_before_window(plan: &BoundLogicalPlan) -> Option<&Expr> {
    let mut last_filter: Option<&Expr> = None;
    for n in &plan.nodes {
        match &n.kind {
            BoundKind::Filter { predicate, .. } => last_filter = Some(predicate),
            BoundKind::WindowAgg { .. } => return last_filter,
            BoundKind::MemorySource { .. }
            | BoundKind::Project { .. }
            | BoundKind::Map { .. } => {}
            _ => last_filter = None,
        }
    }
    None
}

/// Last Filter in stages that appear *before* the window. A Filter after
/// the window (HAVING-shaped, or a trailing map/filter) must not change
/// the checkpoint WHERE fingerprint (N16).
pub fn where_before_window_physical(plan: &crate::PhysicalPlan) -> Option<&Expr> {
    use crate::{PhysicalStage, TransformStep};
    let mut last_filter = None;
    for s in &plan.stages {
        match s {
            PhysicalStage::Transform { steps } => {
                for step in steps {
                    if let TransformStep::Filter { predicate, .. } = step {
                        last_filter = Some(predicate);
                    }
                }
            }
            PhysicalStage::WindowAgg { .. } => return last_filter,
            _ => {}
        }
    }
    last_filter
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AggCall;
    use sparrow_expr::Expr;
    use sparrow_model::{AggFn, WindowKind};

    fn layout() -> PlanLayout {
        PlanLayout::from_window(
            OperatorId::new(2),
            StateSlotId::new(1),
            &WindowSpec::new(
                WindowKind::Count { size: 3 },
                vec!["device_id".into()],
                vec![AggCall::new(AggFn::Sum, Some(Expr::Column { name: "v".into() }), "s")],
            ),
        )
    }

    #[test]
    fn identical_layout_reuses() {
        let a = layout();
        let b = layout();
        assert_eq!(decide_state_reuse(&a, &b), StateReuse::Reuse);
    }

    #[test]
    fn where_change_is_reset_replay() {
        let saved = layout();
        let live = layout().with_where(Some(&Expr::Column { name: "v".into() }));
        assert!(matches!(
            decide_state_reuse(&saved, &live),
            StateReuse::ResetReplay { .. }
        ));
    }

    #[test]
    fn r12_window_size_change_is_reject() {
        let saved = layout();
        let mut live = layout();
        live.window_params_fingerprint = window_params_fingerprint(&WindowKind::Count { size: 9 });
        assert!(matches!(
            decide_state_reuse(&saved, &live),
            StateReuse::Reject { .. }
        ));
    }

    #[test]
    fn r12_agg_input_change_is_reject() {
        let saved = layout();
        let mut live = PlanLayout::from_window(
            OperatorId::new(2),
            StateSlotId::new(1),
            &WindowSpec::new(
                WindowKind::Count { size: 3 },
                vec!["device_id".into()],
                vec![AggCall::new(
                    AggFn::Sum,
                    Some(Expr::Column { name: "v2".into() }),
                    "s",
                )],
            ),
        );
        live.operator = saved.operator;
        assert!(matches!(
            decide_state_reuse(&saved, &live),
            StateReuse::Reject { .. }
        ));
    }

    #[test]
    fn p3_43_pt_and_et_window_kind_tags_differ() {
        assert_ne!(
            window_kind_tag(WindowKind::TumblingProcessingTime {
                size_micros: 1_000_000
            }),
            window_kind_tag(WindowKind::TumblingEventTime {
                size_micros: 1_000_000
            })
        );
        assert_eq!(
            window_kind_tag(WindowKind::TumblingProcessingTime {
                size_micros: 1_000_000
            }),
            0
        );
        assert_eq!(
            window_kind_tag(WindowKind::TumblingEventTime {
                size_micros: 1_000_000
            }),
            3
        );
    }

    #[test]
    fn p3_42_agg_fingerprint_includes_func_alias_and_input_once() {
        let l = layout();
        assert_eq!(l.aggs.len(), 1);
        assert_eq!(l.aggs[0], "sum:s:col:v");
    }

    #[test]
    fn p3_54_expr_fingerprint_is_structured_not_debug() {
        let e = Expr::Binary {
            op: sparrow_expr::BinaryOp::Gt,
            left: Box::new(Expr::Column { name: "v".into() }),
            right: Box::new(Expr::Literal(sparrow_model::Scalar::Int64(1))),
        };
        let canon = expr_canonical(&e);
        assert!(canon.starts_with("bin:gt:"), "{canon}");
        assert!(!canon.contains("Binary {"), "{canon}");
        assert!(!canon.contains("Gt"), "{canon}");
    }

    #[test]
    fn n16_physical_layout_uses_filter_before_window() {
        use crate::{PhysicalPlan, PhysicalStage, TransformStep};
        use sparrow_model::{DataType, Field, FieldId, PipelineId, RevisionId, Schema, SchemaId};

        let schema = Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap();
        let before = Expr::Column { name: "before".into() };
        let after = Expr::Column { name: "after".into() };
        let spec = WindowSpec::new(
            WindowKind::Count { size: 2 },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        );
        let out = crate::window_output_schema(&schema, &spec).unwrap();
        let plan = PhysicalPlan {
            pipeline: PipelineId::new(1),
            revision: RevisionId::new(1),
            stages: vec![
                PhysicalStage::MemorySource {
                    operator: OperatorId::SOURCE,
                    name: "sensors".into(),
                    schema: schema.clone(),
                },
                PhysicalStage::Transform {
                    steps: vec![TransformStep::Filter {
                        operator: OperatorId::FILTER,
                        predicate: before.clone(),
                        input: schema.clone(),
                    }],
                },
                PhysicalStage::WindowAgg {
                    operator: OperatorId::WINDOW,
                    spec,
                    input: schema.clone(),
                    output: out,
                },
                PhysicalStage::Transform {
                    steps: vec![TransformStep::Filter {
                        operator: OperatorId::new(99),
                        predicate: after.clone(),
                        input: schema.clone(),
                    }],
                },
            ],
        };
        let pred = where_before_window_physical(&plan).expect("filter before window");
        assert_eq!(pred, &before);
        assert_ne!(expr_fingerprint(pred), expr_fingerprint(&after));
        let layout = PlanLayout::from_window(OperatorId::WINDOW, StateSlotId::new(1), &plan_window(&plan))
            .with_where(where_before_window_physical(&plan));
        assert_eq!(layout.where_fingerprint, expr_fingerprint(&before));
        assert_ne!(layout.where_fingerprint, expr_fingerprint(&after));
    }

    fn plan_window(plan: &crate::PhysicalPlan) -> WindowSpec {
        for s in &plan.stages {
            if let crate::PhysicalStage::WindowAgg { spec, .. } = s {
                return spec.clone();
            }
        }
        panic!("window");
    }

    #[test]
    fn operator_remap_is_reject() {
        let saved = layout();
        let mut live = layout();
        live.operator = OperatorId::new(9);
        assert!(matches!(
            decide_state_reuse(&saved, &live),
            StateReuse::Reject { .. }
        ));
    }
}

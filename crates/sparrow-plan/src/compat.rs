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
                        .map(|e| format!("{e:?}"))
                        .unwrap_or_else(|| "*".into());
                    let ty = a
                        .input
                        .as_ref()
                        .map(|e| format!("{e:?}"))
                        .unwrap_or_default();
                    format!("{}:{}:{input}:{ty}", a.func.as_str(), a.alias)
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
        WindowKind::TumblingProcessingTime { .. } | WindowKind::TumblingEventTime { .. } => 0,
        WindowKind::Count { .. } => 1,
        WindowKind::HoppingEventTime { .. } => 2,
    }
}

/// Stable FNV-1a of a debug-printed predicate (V1 white-list, not a crypto hash).
pub fn expr_fingerprint(expr: &Expr) -> u64 {
    fnv1a64(format!("{expr:?}").as_bytes())
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

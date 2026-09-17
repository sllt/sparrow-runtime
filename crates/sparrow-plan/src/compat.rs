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
    /// Full, versioned semantics. Legacy/hash-only checkpoints cannot restore.
    pub semantic_descriptor: Option<crate::canonical::StateSemantics>,
}

impl PlanLayout {
    pub fn slot_key(&self) -> StateSlotKey {
        StateSlotKey::new(self.operator, self.slot)
    }

    pub fn from_window(operator: OperatorId, slot: StateSlotId, spec: &WindowSpec) -> Self {
        Self::window_fields(
            operator,
            slot,
            spec,
            crate::canonical::StateSemantics::window(spec).ok(),
        )
    }

    /// Fallible construction preserves size/depth errors for embedded callers.
    /// The legacy infallible builder still fails closed at state-reuse checks.
    pub fn try_from_window(
        operator: OperatorId,
        slot: StateSlotId,
        spec: &WindowSpec,
    ) -> Result<Self> {
        Ok(Self::window_fields(
            operator,
            slot,
            spec,
            Some(crate::canonical::StateSemantics::window(spec)?),
        ))
    }

    fn window_fields(
        operator: OperatorId,
        slot: StateSlotId,
        spec: &WindowSpec,
        semantic_descriptor: Option<crate::canonical::StateSemantics>,
    ) -> Self {
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
            semantic_descriptor,
        }
    }

    pub fn with_where(mut self, pred: Option<&Expr>) -> Self {
        self.where_fingerprint = pred.map(expr_fingerprint).unwrap_or(0);
        self.semantic_descriptor = self.semantic_descriptor.take().and_then(|mut s| {
            s.predicate(pred).ok()?;
            Some(s)
        });
        self
    }

    pub fn with_input_schema(mut self, input: &sparrow_model::Schema) -> Self {
        self.semantic_descriptor = self.semantic_descriptor.take().and_then(|mut s| {
            s.input(input).ok()?;
            Some(s)
        });
        self
    }

    /// Recoverable semantics include every upstream step, not merely the last
    /// WHERE. Trailing transforms/sinks and physical fusion are deliberately omitted.
    pub fn from_physical(plan: &crate::PhysicalPlan) -> Result<Self> {
        let (operator, spec, input) = plan.aligned_window()?;
        let mut semantics = crate::canonical::StateSemantics::window(spec)?;
        semantics.predicate(where_before_window_physical(plan))?;
        semantics.input(input)?;
        semantics.upstream(plan)?;
        let mut layout = Self::from_window(operator, StateSlotId::new(1), spec)
            .with_where(where_before_window_physical(plan));
        layout.semantic_descriptor = Some(semantics);
        Ok(layout)
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
    let complete = |l: &PlanLayout| {
        l.semantic_descriptor
            .as_ref()
            .is_some_and(|s| s.has_input_schema())
    };
    if !complete(saved) || !complete(live) {
        return StateReuse::Reject {
            reason: "missing complete versioned state semantics (legacy/hash-only layout, missing input schema, or descriptor construction failure); explicit reset/replay required".into(),
        };
    }
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
    if saved.semantic_descriptor != live.semantic_descriptor {
        return StateReuse::ResetReplay {
            reason:
                "canonical window/input/upstream semantics changed; explicit reset/replay required"
                    .into(),
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
    // Diagnostic only: construction failure must never authorize state reuse.
    crate::canonical::expression(expr)
        .map(|bytes| fnv1a64(&bytes))
        .unwrap_or(0)
}

fn expr_canonical(expr: &Expr) -> String {
    use std::fmt::Write;
    match crate::canonical::expression(expr) {
        Ok(bytes) => {
            let mut text = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                write!(&mut text, "{byte:02x}").expect("String write");
            }
            text
        }
        Err(_) => "invalid-state-expression".into(),
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
            BoundKind::MemorySource { .. } | BoundKind::Project { .. } | BoundKind::Map { .. } => {}
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

    fn test_schema() -> sparrow_model::Schema {
        use sparrow_model::{DataType, Field};
        sparrow_model::Schema::new(
            1,
            vec![
                Field::new(1, "device_id", DataType::Utf8, false),
                Field::new(2, "v", DataType::Int64, true),
            ],
        )
        .unwrap()
    }

    fn test_plan() -> crate::PhysicalPlan {
        use crate::{PhysicalStage as S, TransformStep as T};
        let schema = test_schema();
        let spec = WindowSpec::new(
            WindowKind::Count { size: 3 },
            vec!["device_id".into()],
            vec![AggCall::count_star("n")],
        );
        let output = crate::window_output_schema(&schema, &spec).unwrap();
        crate::PhysicalPlan {
            edges: None,
            side_outputs: vec![],
            source_times: vec![],
            pipeline: sparrow_model::PipelineId::new(1),
            revision: sparrow_model::RevisionId::new(1),
            stages: vec![
                S::MemorySource {
                    operator: OperatorId::SOURCE,
                    name: "sensors".into(),
                    schema: schema.clone(),
                },
                S::Transform {
                    steps: vec![
                        T::Filter {
                            operator: OperatorId::new(2),
                            predicate: Expr::Literal(sparrow_model::Scalar::Bool(true)),
                            input: schema.clone(),
                        },
                        T::Map {
                            operator: OperatorId::new(3),
                            exprs: vec![
                                Expr::Column {
                                    name: "device_id".into(),
                                },
                                Expr::Column { name: "v".into() },
                            ],
                            input: schema.clone(),
                            output: schema.clone(),
                        },
                        T::Filter {
                            operator: OperatorId::new(4),
                            predicate: Expr::IsNotNull(Box::new(Expr::Column { name: "v".into() })),
                            input: schema.clone(),
                        },
                    ],
                },
                S::WindowAgg {
                    operator: OperatorId::new(5),
                    spec,
                    input: schema,
                    output: output.clone(),
                },
                S::CaptureSink {
                    operator: OperatorId::SINK,
                    name: "output".into(),
                    schema: output,
                },
            ],
        }
    }

    #[test]
    fn base01_aligned_rejects_zero_or_multiple_windows() {
        let plan = test_plan();
        assert!(plan.aligned_window().is_ok());
        let mut multiple = plan.clone();
        multiple.stages.insert(3, plan.stages[2].clone());
        assert!(multiple
            .aligned_window()
            .unwrap_err()
            .message
            .contains("exactly one"));
        assert!(PlanLayout::from_physical(&multiple).is_err());
        let mut none = plan.clone();
        none.stages.remove(2);
        assert!(none.aligned_window().is_err());
        use crate::PhysicalStage as S;
        for stage in [
            S::Deduplicate {
                operator: OperatorId::new(9),
                spec: crate::DedupSpec {
                    keys: vec!["device_id".into()],
                    ttl_micros: 100,
                    max_keys: 10,
                },
                input: test_schema(),
            },
            S::Lookup {
                operator: OperatorId::new(9),
                spec: crate::LookupSpec::static_table(
                    "ref",
                    vec!["device_id".into()],
                    vec!["device_id".into()],
                    vec![],
                ),
                input: test_schema(),
                output: test_schema(),
            },
        ] {
            let mut unsupported = plan.clone();
            unsupported.stages.insert(1, stage);
            assert!(unsupported.aligned_window().is_err());
        }
    }

    #[test]
    fn base02_all_upstream_steps_are_semantics_but_fusion_and_sink_are_not() {
        use crate::{PhysicalStage as S, TransformStep as T};
        let plan = test_plan();
        let saved = PlanLayout::from_physical(&plan).unwrap();
        for index in [0, 1, 2] {
            let mut changed = plan.clone();
            if let S::Transform { steps } = &mut changed.stages[1] {
                match &mut steps[index] {
                    T::Filter { predicate, .. } => {
                        *predicate = Expr::Literal(sparrow_model::Scalar::Bool(false))
                    }
                    T::Map { exprs, .. } => {
                        exprs[1] = Expr::Literal(sparrow_model::Scalar::Int64(42))
                    }
                    _ => unreachable!(),
                }
            }
            let live = PlanLayout::from_physical(&changed).unwrap();
            assert_ne!(
                decide_state_reuse(&saved, &live),
                StateReuse::Reuse,
                "step {index}"
            );
        }
        let mut project = plan.clone();
        if let S::Transform { steps } = &mut project.stages[1] {
            let T::Map {
                operator,
                exprs,
                input,
                output,
            } = steps[1].clone()
            else {
                unreachable!()
            };
            steps[1] = T::Project {
                operator,
                exprs,
                input,
                output,
            };
        }
        assert_ne!(
            decide_state_reuse(&saved, &PlanLayout::from_physical(&project).unwrap()),
            StateReuse::Reuse
        );
        let mut split = plan.clone();
        let S::Transform { steps } = split.stages.remove(1) else {
            unreachable!()
        };
        split.stages.splice(
            1..1,
            steps
                .into_iter()
                .map(|step| S::Transform { steps: vec![step] }),
        );
        if let S::CaptureSink { name, .. } = split.stages.last_mut().unwrap() {
            *name = "another-sink".into();
        }
        split.revision = sparrow_model::RevisionId::new(99);
        assert_eq!(
            decide_state_reuse(&saved, &PlanLayout::from_physical(&split).unwrap()),
            StateReuse::Reuse
        );
    }

    #[test]
    fn base02_literals_are_typed_framed_and_content_complete() {
        use sparrow_model::{DataType, DynamicValue as D, Scalar as S};
        let pairs = vec![
            (
                Expr::Literal(S::bytes([1, 2])),
                Expr::Literal(S::bytes([1, 3])),
            ),
            (
                Expr::Literal(S::Dynamic(D::Array(vec![D::Int64(1)].into()))),
                Expr::Literal(S::Dynamic(D::Array(vec![D::Int64(2)].into()))),
            ),
            (
                Expr::Literal(S::Dynamic(D::Object(
                    vec![("a".into(), D::Int64(1)), ("a".into(), D::Int64(2))].into(),
                ))),
                Expr::Literal(S::Dynamic(D::Object(
                    vec![("a".into(), D::Int64(2)), ("a".into(), D::Int64(1))].into(),
                ))),
            ),
            (
                Expr::Literal(S::Float64(0.0)),
                Expr::Literal(S::Float64(-0.0)),
            ),
            (
                Expr::Literal(S::Float64(f64::from_bits(0x7ff8_0000_0000_0001))),
                Expr::Literal(S::Float64(f64::from_bits(0x7ff8_0000_0000_0002))),
            ),
            (Expr::Literal(S::Int64(1)), Expr::Literal(S::UInt64(1))),
            (
                Expr::Call {
                    name: "f".into(),
                    args: vec![Expr::Column {
                        name: "x,col:y".into(),
                    }],
                },
                Expr::Call {
                    name: "f".into(),
                    args: vec![
                        Expr::Column { name: "x".into() },
                        Expr::Column { name: "y".into() },
                    ],
                },
            ),
            (
                Expr::Cast {
                    expr: Box::new(Expr::Literal(S::Null)),
                    target: DataType::Int64,
                },
                Expr::TryCast {
                    expr: Box::new(Expr::Literal(S::Null)),
                    target: DataType::Int64,
                },
            ),
        ];
        for (a, b) in pairs {
            assert_ne!(
                crate::canonical::expression(&a).unwrap(),
                crate::canonical::expression(&b).unwrap()
            );
            let saved = layout().with_where(Some(&a));
            let mut live = layout().with_where(Some(&b));
            // Even a diagnostic hash collision must not authorize reuse.
            live.where_fingerprint = saved.where_fingerprint;
            assert_ne!(decide_state_reuse(&saved, &live), StateReuse::Reuse);
        }
    }

    #[test]
    fn base02_schema_and_window_policy_changes_refuse_reuse() {
        use crate::PhysicalStage;
        let plan = test_plan();
        let saved = PlanLayout::from_physical(&plan).unwrap();
        for variant in 0..5 {
            let mut changed = plan.clone();
            if let PhysicalStage::WindowAgg { spec, input, .. } = &mut changed.stages[2] {
                match variant {
                    0 => input.fields[1].nullable = false,
                    1 => input.fields[1].data_type = sparrow_model::DataType::UInt64,
                    2 => spec.max_future_skew_micros = Some(17),
                    3 => spec.max_overlap += 1,
                    _ => spec.aggs[0].count_star = false,
                }
            }
            assert_ne!(
                decide_state_reuse(&saved, &PlanLayout::from_physical(&changed).unwrap()),
                StateReuse::Reuse,
                "variant {variant}"
            );
        }
        let mut explicit = plan.clone();
        if let PhysicalStage::WindowAgg { spec, .. } = &mut explicit.stages[2] {
            spec.max_future_skew_micros = Some(sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS);
        }
        assert_eq!(
            decide_state_reuse(&saved, &PlanLayout::from_physical(&explicit).unwrap()),
            StateReuse::Reuse
        );
    }

    #[test]
    fn base02_legacy_missing_schema_oversize_and_deep_descriptors_fail_closed() {
        let live = layout();
        let mut legacy = live.clone();
        legacy.semantic_descriptor = None;
        assert_ne!(decide_state_reuse(&legacy, &legacy), StateReuse::Reuse);
        assert_ne!(decide_state_reuse(&legacy, &live), StateReuse::Reuse);
        let incomplete = PlanLayout::from_window(
            OperatorId::new(2),
            StateSlotId::new(1),
            &WindowSpec::new(
                WindowKind::Count { size: 3 },
                vec!["device_id".into()],
                vec![AggCall::count_star("n")],
            ),
        );
        assert_ne!(
            decide_state_reuse(&incomplete, &incomplete),
            StateReuse::Reuse
        );
        let large = Expr::Literal(sparrow_model::Scalar::bytes(vec![0; 64 * 1024]));
        assert!(live
            .clone()
            .with_where(Some(&large))
            .semantic_descriptor
            .is_none());
        let mut deep = Expr::Literal(sparrow_model::Scalar::Bool(true));
        for _ in 0..66 {
            deep = Expr::Not(Box::new(deep));
        }
        assert!(crate::canonical::expression(&deep).is_err());
        assert!(live.with_where(Some(&deep)).semantic_descriptor.is_none());
    }

    #[test]
    fn r9_fallible_window_layout_preserves_descriptor_construction_error() {
        let spec = WindowSpec::new(
            WindowKind::Count { size: 3 },
            vec![],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Literal(sparrow_model::Scalar::bytes(vec![
                    0;
                    64 * 1024
                ]))),
                "s",
            )],
        );
        let error = PlanLayout::try_from_window(OperatorId::new(2), StateSlotId::new(1), &spec)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::BoundExceeded);
        let layout = PlanLayout::from_window(OperatorId::new(2), StateSlotId::new(1), &spec);
        let StateReuse::Reject { reason } = decide_state_reuse(&layout, &layout) else {
            panic!("must reject");
        };
        assert!(reason.contains("descriptor construction failure"));
    }

    fn layout() -> PlanLayout {
        PlanLayout::from_window(
            OperatorId::new(2),
            StateSlotId::new(1),
            &WindowSpec::new(
                WindowKind::Count { size: 3 },
                vec!["device_id".into()],
                vec![AggCall::new(
                    AggFn::Sum,
                    Some(Expr::Column { name: "v".into() }),
                    "s",
                )],
            ),
        )
        .with_input_schema(&test_schema())
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
        )
        .with_input_schema(&test_schema());
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
        assert_eq!(
            l.aggs[0],
            format!(
                "sum:s:{}",
                expr_canonical(&Expr::Column { name: "v".into() })
            )
        );
    }

    #[test]
    fn p3_54_expr_fingerprint_is_structured_not_debug() {
        let e = Expr::Binary {
            op: sparrow_expr::BinaryOp::Gt,
            left: Box::new(Expr::Column { name: "v".into() }),
            right: Box::new(Expr::Literal(sparrow_model::Scalar::Int64(1))),
        };
        let canon = expr_canonical(&e);
        assert!(canon.starts_with("04"), "{canon}");
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
        let before = Expr::Column {
            name: "before".into(),
        };
        let after = Expr::Column {
            name: "after".into(),
        };
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
            edges: None,
            side_outputs: vec![],
            source_times: vec![],
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
        let layout =
            PlanLayout::from_window(OperatorId::WINDOW, StateSlotId::new(1), &plan_window(&plan))
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

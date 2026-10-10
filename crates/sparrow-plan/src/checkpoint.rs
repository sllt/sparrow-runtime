//! Bounded checkpoint participants for the supported linear and required File
//! DAG execution paths. This does not replace the legacy single-window layout.
use std::collections::BTreeSet;

use crate::{PhysicalPlan, PhysicalStage, TransformStep};
use sparrow_model::{ErrorCode, OperatorId, Result, SparrowError, StateSlotId, WindowKind};

pub const MAX_CHECKPOINT_STATES: usize = 2;
pub const MAX_GRAPH_CHECKPOINT_STATES: usize = 16;
pub const MAX_CHECKPOINT_OPERATORS: usize = 64;
pub const MAX_CHECKPOINT_STAGES: usize = 64;
/// Keep public plan admission aligned with the runtime graph mailbox bound.
pub const MAX_CHECKPOINT_EDGES: usize = 128;
pub const MAX_CHECKPOINT_REFERENCES: usize = 8;
pub const WINDOW_STATE_CODEC: u16 = 1;
/// Versioned keyed IoT state.  It is intentionally distinct from the
/// WindowFreeze codec; an IoT value is not a window accumulator.
pub const IOT_STATE_CODEC: u16 = 2;
/// WindowFreeze plus the FIRST/LAST (tag 8) and VAR/STDDEV moment (tag 9)
/// accumulator encodings. Codec 1 never carries tags 8/9; a codec 1 reader
/// rejects them. Only the strict linear v29/v30 profiles admit codec 3.
pub const WINDOW_EXT_STATE_CODEC: u16 = 3;
/// Sliding count buffered-window frame (`BWF1`): evaluated, detached aggregate
/// inputs of the last `size` arrivals per key, never accumulators or raw rows.
/// Only the strict single-state linear File v31 / JetStream v32 profiles.
pub const BUFFERED_WINDOW_STATE_CODEC: u16 = 4;
pub const ANALYSIS_STATE_CODEC: u16 = 5;

fn validate_analysis_profile(plan: &PhysicalPlan, references: bool) -> Result<()> {
    if !plan.has_analysis() { return Ok(()); }
    if references || !plan.side_outputs.is_empty() || plan.stages.iter().any(|s| matches!(s,
        PhysicalStage::WindowAgg { .. } | PhysicalStage::Iot { .. }
        | PhysicalStage::Lookup { .. } | PhysicalStage::Deduplicate { .. })) {
        return Err(rejected("analysis recovery excludes window/IoT/Lookup/Dedup combinations and side outputs"));
    }
    let mut joins = 0;
    let mut analyses = 0;
    for stage in &plan.stages {
        if let PhysicalStage::Analysis { plan: analysis, .. } = stage {
            analysis.validate()?;
            analyses += 1;
            match analysis.as_ref() {
                crate::AnalysisPlan::External { .. } => return Err(rejected("external Transform has no recovery codec")),
                crate::AnalysisPlan::Join { left, right, .. } => {
                    joins += 1;
                    validate_time_input_schema(left)?;
                    validate_time_input_schema(right)?;
                }
                crate::AnalysisPlan::Unnest { .. } => {}
            }
        }
    }
    if joins > 1 || (joins > 0 && (plan.edges.is_none() || analyses != 1))
        || (plan.edges.is_none() && (analyses != 1 || !plan.source_times.is_empty())) {
        return Err(rejected("analysis recovery requires one linear UNNEST, or a File graph with UNNEST/Union or one direct two-source Join"));
    }
    Ok(())
}

/// Buffered window kinds with a codec 4 profile: sliding count (v31/v32),
/// ET sliding and ET session (File v33). PT buffered kinds stay restart_fresh.
pub fn checkpointable_buffered(kind: WindowKind) -> bool {
    matches!(
        kind,
        WindowKind::SlidingCount { .. }
            | WindowKind::SlidingEventTime { .. }
            | WindowKind::SessionEventTime { .. }
            | WindowKind::SlidingProcessingTime { .. }
            | WindowKind::SessionProcessingTime { .. }
    )
}

/// Sub-batch 2c (File v34 / JetStream v35): a single PT window driven only by
/// the durable logical clock. PT hopping (codec 1/3, kind 4), PT sliding /
/// PT session (codec 4, kind 6/8) and PT tumbling with new aggregates
/// (codec 3, kind 0). PT tumbling with legacy aggregates stays v16/v17.
pub fn pt_window_profile(kind: WindowKind, extended: bool) -> bool {
    matches!(
        kind,
        WindowKind::HoppingProcessingTime { .. }
            | WindowKind::SlidingProcessingTime { .. }
            | WindowKind::SessionProcessingTime { .. }
    ) || (extended && matches!(kind, WindowKind::TumblingProcessingTime { .. }))
}
// Keep the CPL1 outer grammar readable by old K1 Stores. An old reader must
// reach compatibility rejection, NOT mistake a new manifest for corruption
// and fall back to an older published snapshot. The CP01 prefix intentionally
// remains recognizable; the zero/version marker cannot equal legacy semantics.
const RECOVERY_PREFIX_MAGIC: &[u8] = b"CP01\0RCP2";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParticipantId {
    Source(OperatorId),
    State {
        operator: OperatorId,
        slot: StateSlotId,
        shard: u16,
    },
    Sink(OperatorId),
}

impl ParticipantId {
    pub fn window(operator: OperatorId) -> Self {
        Self::State {
            operator,
            slot: StateSlotId::new(1),
            shard: 0,
        }
    }

    pub fn iot(operator: OperatorId) -> Self {
        Self::State {
            operator,
            slot: StateSlotId::new(3),
            shard: 0,
        }
    }

    pub fn analysis(operator: OperatorId) -> Self {
        Self::State { operator, slot: StateSlotId::new(4), shard: 0 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateParticipant {
    pub id: ParticipantId,
    pub codec: u16,
    pub window_kind: u8,
}

impl StateParticipant {
    pub fn freeze_kind(&self) -> u8 {
        match self.codec {
            WINDOW_STATE_CODEC | WINDOW_EXT_STATE_CODEC => u8::from(self.window_kind == 1),
            BUFFERED_WINDOW_STATE_CODEC if matches!(self.window_kind, 5..=9) => self.window_kind,
            ANALYSIS_STATE_CODEC if matches!(self.window_kind, 10 | 11) => self.window_kind,
            IOT_STATE_CODEC if matches!(self.window_kind, 4..=15) => self.window_kind,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceTableDependency {
    pub name: String,
    pub revision: u64,
    /// Digest of the verified canonical catalog record, not the runtime CRC.
    pub canonical_sha256: [u8; 32],
    /// Independently verified runtime row/schema/key encoding.
    pub runtime_crc32: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointPlan {
    pub source: OperatorId,
    pub sink: OperatorId,
    pub states: Vec<StateParticipant>,
    /// Immutable read dependencies, never mutable state participants.
    pub reference_tables: Vec<ReferenceTableDependency>,
    /// Full ordered computation, including downstream transforms. Fusion groups
    /// and catalog revision numbers are not semantic identities.
    pub semantics: Vec<u8>,
    /// Plain CP01 had no prefix boundary and retains its whole-plan contract.
    pub recovery_prefix_len: Option<usize>,
}

fn rejected(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::UnsupportedRestore, message)
}

/// Time journals fingerprint the complete decoded input with the scalar value
/// codec, before Filter/Project. Reject unsupported columns at admission even
/// when a downstream projection would discard them (or their first value is NULL).
fn validate_time_input_schema(schema: &sparrow_model::Schema) -> Result<()> {
    if schema
        .fields
        .iter()
        .any(|f| f.data_type.is_nested() || f.data_type == sparrow_model::DataType::Dynamic)
    {
        return Err(rejected("durable time input fingerprint requires scalar source fields; nested/Dynamic source columns are not supported"));
    }
    Ok(())
}

impl CheckpointPlan {
    pub fn from_physical(plan: &PhysicalPlan) -> Result<Self> {
        Self::from_physical_inner(plan, Vec::new())
    }

    pub fn from_physical_with_references(
        plan: &PhysicalPlan,
        mut deps: Vec<ReferenceTableDependency>,
    ) -> Result<Self> {
        if deps.is_empty() {
            return Err(rejected(
                "reference checkpoint requires immutable dependencies",
            ));
        }
        deps.sort_by(|a, b| a.name.cmp(&b.name));
        validate_references(&deps)?;
        Self::from_physical_inner(plan, deps)
    }

    fn from_physical_inner(
        plan: &PhysicalPlan,
        reference_tables: Vec<ReferenceTableDependency>,
    ) -> Result<Self> {
        if plan.has_plugins() {return Err(rejected("plugin functions have no checkpoint/restore profile"));}
        validate_analysis_profile(plan, !reference_tables.is_empty())?;
        let extended = plan.has_extended_aggs();
        // v34/v35: exactly one window stage, a PT profile window, and no
        // other state (no TTL/HoldFor/IoT), DAG, references or side inputs.
        let windows = plan.stages.iter().filter(|s| matches!(s, PhysicalStage::WindowAgg { .. })).count();
        let pt_profile = plan.stages.iter().any(|s| {
            matches!(s, PhysicalStage::WindowAgg { spec, .. } if pt_window_profile(spec.kind, spec.has_extended_aggs()))
        });
        if pt_profile
            && (windows != 1
                || plan.edges.is_some()
                || !reference_tables.is_empty()
                || plan.has_iot()
                || !plan.side_outputs.is_empty()
                || !plan.source_times.is_empty())
        {
            return Err(rejected("PT window checkpoint (v34/v35) requires one linear PT window without DAG, references, IoT/TTL/HoldFor state, side outputs or source time"));
        }
        if extended
            && !pt_profile
            && (plan.edges.is_some()
                || !reference_tables.is_empty()
                || plan.has_processing_time_state()
                || plan.has_iot()
                || !plan.side_outputs.is_empty()
                || !plan.source_times.is_empty())
        {
            return Err(rejected("extended aggregate checkpoint requires a linear Count/event-time window plan without DAG, references, processing-time state, IoT, side outputs or source time"));
        }
        // Codec 4 buffered kinds with a published profile: sliding count
        // (v31/v32) and ET sliding / ET session (File v33).
        let sliding_count = plan.stages.iter().any(|s| {
            matches!(s, PhysicalStage::WindowAgg { spec, .. } if checkpointable_buffered(spec.kind))
        });
        if plan.has_new_windows() {
            let windows = plan.stages.iter().filter(|s| matches!(s, PhysicalStage::WindowAgg { .. })).count();
            let published = |k: WindowKind| checkpointable_buffered(k) || pt_window_profile(k, false);
            if !(sliding_count || pt_profile)
                || windows != 1
                || plan.stages.iter().any(|s| matches!(s, PhysicalStage::WindowAgg { spec, .. } if spec.kind.is_new_window() && !published(spec.kind)))
            {
                return Err(rejected("new window kinds have a checkpoint profile only as the single window of a strict plan (sliding count v31/v32, ET sliding/session v33, PT windows v34/v35); others are restart_fresh only"));
            }
            if plan.edges.is_some()
                || !reference_tables.is_empty()
                || (plan.has_processing_time_state() && !pt_profile)
                || plan.has_iot()
                || !plan.side_outputs.is_empty()
                || !plan.source_times.is_empty()
            {
                return Err(rejected("buffered window checkpoint (sliding count v31/v32, ET sliding/session v33) requires a linear single-window plan without DAG, references, processing-time state, IoT, side outputs or source time"));
            }
        }
        if plan.stages.iter().any(|stage| {
            matches!(stage, PhysicalStage::Iot { spec, .. }
            if spec.timing.as_ref().is_some_and(|t| t.is_live()))
        }) {
            return Err(rejected("live observation state is not checkpointable"));
        }
        let has_references = !reference_tables.is_empty();
        if plan.edges.is_none()
            && plan.has_processing_time_state()
            && (!plan.side_outputs.is_empty() || !plan.source_times.is_empty())
        {
            return Err(rejected(
                "paused-time checkpoint excludes source-time and side-output plans",
            ));
        }
        if has_references && (!plan.side_outputs.is_empty() || !plan.source_times.is_empty()) {
            return Err(rejected(
                "reference checkpoint excludes source-time and side-output plans",
            ));
        }
        if plan.edges.is_some() {
            return Self::from_graph(plan, reference_tables);
        }
        let Some(PhysicalStage::MemorySource {
            operator: source,
            schema,
            ..
        }) = plan.stages.first()
        else {
            return Err(rejected("checkpoint requires one leading source"));
        };
        if plan.has_processing_time_state() {
            validate_time_input_schema(schema)?;
        }
        let Some(PhysicalStage::CaptureSink { operator: sink, .. }) = plan.stages.last() else {
            return Err(rejected("checkpoint requires one trailing required sink"));
        };
        if plan.stages.len() < 2 || plan.stages.len() > MAX_CHECKPOINT_STAGES {
            return Err(rejected("checkpoint linear stage count exceeds bound"));
        }
        let mut ids = BTreeSet::new();
        let mut register = |id| {
            if !ids.insert(id) || ids.len() > MAX_CHECKPOINT_OPERATORS {
                return Err(rejected(
                    "duplicate operator instance or checkpoint operator limit",
                ));
            }
            Ok(())
        };
        register(*source)?;
        register(*sink)?;
        let mut current = schema;
        let mut states = Vec::new();
        let mut used_references = BTreeSet::new();
        for stage in &plan.stages[1..plan.stages.len() - 1] {
            match stage {
                PhysicalStage::Analysis { operator, plan: analysis } => {
                    register(*operator)?;
                    if analysis.is_join() || current.fields != analysis.input().fields {
                        return Err(rejected("linear analysis requires UNNEST with matching input schema"));
                    }
                    states.push(StateParticipant { id: ParticipantId::analysis(*operator),
                        codec: ANALYSIS_STATE_CODEC, window_kind: 10 });
                    current = analysis.output();
                }
                PhysicalStage::Transform { steps } => {
                    if steps.is_empty() {
                        return Err(rejected("empty checkpoint transform stage"));
                    }
                    for step in steps {
                        let (operator, input, output) = match step {
                            TransformStep::Filter {
                                operator,
                                input,
                                predicate,
                            } => {
                                sparrow_expr::bind(predicate, input).map_err(|e| {
                                    rejected(&format!("checkpoint expression binding: {e}"))
                                })?;
                                (*operator, input, input)
                            }
                            TransformStep::Project {
                                operator,
                                input,
                                output,
                                exprs,
                            }
                            | TransformStep::Map {
                                operator,
                                input,
                                output,
                                exprs,
                            } => {
                                if exprs.len() != output.fields.len() {
                                    return Err(rejected("checkpoint projection width mismatch"));
                                }
                                for expr in exprs {
                                    sparrow_expr::bind(expr, input).map_err(|e| {
                                        rejected(&format!("checkpoint expression binding: {e}"))
                                    })?;
                                }
                                (*operator, input, output)
                            }
                        };
                        register(operator)?;
                        if current.fields != input.fields {
                            return Err(rejected("checkpoint transform input schema mismatch"));
                        }
                        current = output;
                    }
                }
                PhysicalStage::WindowAgg {
                    operator,
                    spec,
                    input,
                    output,
                } => {
                    if has_references {
                        if !matches!(spec.kind, WindowKind::Count { .. }) {
                            return Err(rejected("reference checkpoint requires Count windows; ET/PT state is not recoverable"));
                        }
                    }
                    register(*operator)?;
                    spec.validate()?;
                    for key in &spec.keys {
                        let index = input
                            .index_of_name(key)
                            .ok_or_else(|| rejected("checkpoint key is missing from schema"))?;
                        let ty = &input.fields[index].data_type;
                        if ty.is_nested() || *ty == sparrow_model::DataType::Dynamic {
                            return Err(rejected(
                                "checkpoint state key codec excludes nested/Dynamic values",
                            ));
                        }
                    }
                    for call in &spec.aggs {
                        if matches!(call.func, sparrow_model::AggFn::First | sparrow_model::AggFn::Last) {
                            let ty = call.input_type(input)?;
                            if ty.is_nested() || ty == sparrow_model::DataType::Dynamic {
                                return Err(rejected(
                                    "checkpoint FIRST/LAST codec excludes nested/Dynamic values",
                                ));
                            }
                        }
                        if matches!(
                            call.func,
                            sparrow_model::AggFn::Min | sparrow_model::AggFn::Max
                        ) {
                            let ty = call.input_type(input)?;
                            if ty.is_nested() || ty == sparrow_model::DataType::Dynamic {
                                return Err(rejected(
                                    "checkpoint MIN/MAX codec excludes nested/Dynamic values",
                                ));
                            }
                        }
                    }
                    if current.fields != input.fields
                        || crate::window_output_schema(input, spec)?.fields != output.fields
                    {
                        return Err(rejected("checkpoint window input/output schema mismatch"));
                    }
                    if checkpointable_buffered(spec.kind) {
                        // BWF1 stores evaluated inputs with the scalar value
                        // codec; nested/Dynamic inputs have no stable encoding.
                        for call in &spec.aggs {
                            if call.input.is_some() {
                                let ty = call.input_type(input)?;
                                if ty.is_nested() || ty == sparrow_model::DataType::Dynamic {
                                    return Err(rejected(
                                        "buffered window checkpoint excludes nested/Dynamic aggregate inputs",
                                    ));
                                }
                            }
                        }
                    }
                    states.push(StateParticipant {
                        id: ParticipantId::window(*operator),
                        codec: if checkpointable_buffered(spec.kind) {
                            BUFFERED_WINDOW_STATE_CODEC
                        } else if spec.has_extended_aggs() {
                            WINDOW_EXT_STATE_CODEC
                        } else {
                            WINDOW_STATE_CODEC
                        },
                        window_kind: crate::compat::window_kind_tag(spec.kind),
                    });
                    if states.len() > MAX_CHECKPOINT_STATES {
                        return Err(rejected(
                            "checkpoint supports at most two state participants",
                        ));
                    }
                    current = output;
                }
                PhysicalStage::Iot {
                    operator,
                    spec,
                    input,
                    output,
                } => {
                    spec.validate(input)?;
                    register(*operator)?;
                    states.push(StateParticipant {
                        id: ParticipantId::iot(*operator),
                        codec: IOT_STATE_CODEC,
                        window_kind: spec.state_kind_tag(),
                    });
                    if states.len() > MAX_CHECKPOINT_STATES {
                        return Err(rejected(
                            "checkpoint supports at most two state participants",
                        ));
                    }
                    if current.fields != input.fields
                        || spec.output_schema(input)?.fields != output.fields
                    {
                        return Err(rejected("checkpoint IoT input schema mismatch"));
                    }
                    current = output;
                }
                PhysicalStage::Lookup {
                    operator,
                    spec,
                    input,
                    output,
                } if has_references => {
                    validate_static_lookup_stage(spec, input, output)?;
                    register(*operator)?;
                    if current.fields != input.fields {
                        return Err(rejected(
                            "reference checkpoint Lookup input schema mismatch",
                        ));
                    }
                    used_references.insert(spec.table.as_str());
                    current = output;
                }
                _ => {
                    return Err(rejected(
                        "checkpoint excludes additional sources/sinks, Dedup and Lookup",
                    ))
                }
            }
        }
        if used_references
            != reference_tables
                .iter()
                .map(|dep| dep.name.as_str())
                .collect()
        {
            return Err(rejected(
                "checkpoint reference dependencies must exactly match static Lookup tables",
            ));
        }
        if let PhysicalStage::CaptureSink { schema, .. } = plan.stages.last().unwrap() {
            if current.fields != schema.fields {
                return Err(rejected("checkpoint sink schema mismatch"));
            }
        }
        // Processing time is shared by the durable ordered profile. Event time
        // remains a separate clock domain and cannot be mixed with IoT state.
        if states.iter().any(|s| matches!(s.window_kind, 2 | 3))
            && states.iter().any(|s| s.codec == IOT_STATE_CODEC)
        {
            return Err(rejected(
                "IoT checkpoint cannot be combined with event-time windows",
            ));
        }
        // First-release silence admission: exactly one silence state directly
        // after the source, followed only by pure transforms. An upstream
        // filter, projection or window could hide an observation, and a second
        // state would make the decision depend on more than the observed
        // prefix. References, event time and side outputs are excluded too.
        if plan.has_silence() {
            let silence_states = plan
                .stages
                .iter()
                .filter(
                    |stage| matches!(stage, PhysicalStage::Iot { spec, .. } if spec.is_silence()),
                )
                .count();
            let first_state = matches!(plan.stages.get(1), Some(PhysicalStage::Iot { spec, .. }) if spec.is_silence());
            let trailing = plan.stages.len().saturating_sub(1);
            let trailing_pure = (2..trailing)
                .all(|index| matches!(plan.stages[index], PhysicalStage::Transform { .. }));
            if !reference_tables.is_empty()
                || !plan.side_outputs.is_empty()
                || !plan.source_times.is_empty()
                || plan.has_event_time_window()
                || silence_states != 1
                || states.len() != 1
                || !first_state
                || !trailing_pure
            {
                return Err(rejected(
                    "silence requires a linear Source->Silence->[Transform]->Sink plan with one state, no references, no event time and no side outputs",
                ));
            }
        }
        if plan.has_resample()
            && (states.len() != 1
                || !reference_tables.is_empty()
                || !plan.side_outputs.is_empty()
                || !plan.source_times.is_empty()
                || plan.has_event_time_window())
        {
            return Err(rejected("resample requires one linear state, pure transforms and no references/event time/side outputs"));
        }
        let (semantics, recovery_prefix_len) = crate::canonical::checkpoint_pipeline(plan)?;
        let has_iot = states.iter().any(|s| s.codec == IOT_STATE_CODEC);
        let result = Self {
            source: *source,
            sink: *sink,
            states,
            reference_tables,
            semantics,
            // IoT state depends on the complete computation descriptor.  Do
            // not apply the linear RCP2 downstream-only relaxation to it.
            // Extended aggregate profiles (v29/v30) are strict: the complete
            // computation, including downstream transforms, is the identity.
            recovery_prefix_len: (!has_iot
                && !has_references
                && !plan.has_processing_time_state()
                && !extended
                && !sliding_count
                && !plan.has_analysis())
                .then_some(recovery_prefix_len),
        };
        result.validate()?;
        Ok(result)
    }

    pub fn participants(&self) -> BTreeSet<ParticipantId> {
        self.source_ids()
            .into_iter()
            .map(ParticipantId::Source)
            .chain(self.states.iter().map(|s| s.id))
            .chain(self.sink_ids().into_iter().map(ParticipantId::Sink))
            .collect()
    }

    pub fn is_graph(&self) -> bool {
        self.semantics.starts_with(b"CP01DAG1") || self.is_time_graph()
    }
    pub fn is_time_graph(&self) -> bool {
        self.semantics.starts_with(b"CP01DAG2")
    }
    pub fn has_event_time_state(&self) -> bool {
        self.states
            .iter()
            .any(|s| matches!(s.codec, WINDOW_STATE_CODEC | WINDOW_EXT_STATE_CODEC) && matches!(s.window_kind, 2 | 3))
    }
    /// A codec 4 participant selects the strict v31/v32/v33 profiles.
    pub fn has_buffered_state(&self) -> bool {
        self.states.iter().any(|s| s.codec == BUFFERED_WINDOW_STATE_CODEC)
    }
    pub fn has_analysis_state(&self) -> bool {
        self.states.iter().any(|s| s.codec == ANALYSIS_STATE_CODEC)
    }
    /// Codec 4 ET sliding (kind 7) / ET session (kind 9): File v33 only.
    pub fn has_buffered_event_time_state(&self) -> bool {
        self.states
            .iter()
            .any(|s| s.codec == BUFFERED_WINDOW_STATE_CODEC && matches!(s.window_kind, 7 | 9))
    }
    /// v34/v35 PT window participant: PT hopping (codec 1/3 kind 4), PT
    /// tumbling with new aggregates (codec 3 kind 0), PT sliding/session
    /// (codec 4 kind 6/8).
    pub fn has_pt_window_state(&self) -> bool {
        self.states.iter().any(|s| {
            (matches!(s.codec, WINDOW_STATE_CODEC | WINDOW_EXT_STATE_CODEC) && s.window_kind == 4)
                || (s.codec == WINDOW_EXT_STATE_CODEC && s.window_kind == 0)
                || (s.codec == BUFFERED_WINDOW_STATE_CODEC && matches!(s.window_kind, 6 | 8))
        })
    }
    /// Codec 3 participants select the strict v29/v30 profiles.
    pub fn has_extended_state(&self) -> bool {
        self.states.iter().any(|s| s.codec == WINDOW_EXT_STATE_CODEC)
    }
    fn graph_ports(&self) -> Result<(Vec<OperatorId>, Vec<OperatorId>)> {
        let mut bytes = self
            .semantics
            .get(8..)
            .ok_or_else(|| rejected("truncated graph identity"))?;
        let mut read = || -> Result<Vec<OperatorId>> {
            if bytes.len() < 2 {
                return Err(rejected("truncated graph port count"));
            }
            let n = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
            bytes = &bytes[2..];
            if n == 0 || n > 16 || bytes.len() < n * 4 {
                return Err(rejected("invalid graph port count"));
            }
            let ids = bytes[..n * 4]
                .chunks_exact(4)
                .map(|raw| OperatorId::new(u32::from_le_bytes(raw.try_into().unwrap())))
                .collect();
            bytes = &bytes[n * 4..];
            Ok(ids)
        };
        Ok((read()?, read()?))
    }
    pub fn source_ids(&self) -> Vec<OperatorId> {
        if self.is_graph() {
            self.graph_ports().map(|p| p.0).unwrap_or_default()
        } else {
            vec![self.source]
        }
    }
    pub fn sink_ids(&self) -> Vec<OperatorId> {
        if self.is_graph() {
            self.graph_ports().map(|p| p.1).unwrap_or_default()
        } else {
            vec![self.sink]
        }
    }
    pub fn has_iot(&self) -> bool {
        self.states.iter().any(|s| s.codec == IOT_STATE_CODEC)
    }
    pub fn has_timed_iot(&self) -> bool {
        self.states
            .iter()
            .any(|s| s.codec == IOT_STATE_CODEC && matches!(s.window_kind, 7 | 8 | 11..=15))
    }
    pub fn has_alarm(&self) -> bool {
        self.states
            .iter()
            .any(|s| s.codec == IOT_STATE_CODEC && s.window_kind == 11)
    }
    pub fn has_silence(&self) -> bool {
        self.states
            .iter()
            .any(|s| s.codec == IOT_STATE_CODEC && s.window_kind == 12)
    }
    pub fn has_resample(&self) -> bool {
        self.states
            .iter()
            .any(|s| s.codec == IOT_STATE_CODEC && matches!(s.window_kind, 13..=15))
    }
    pub fn requires_paused_time(&self) -> bool {
        self.is_time_graph()
            || self.has_pt_window_state()
            || self.states.iter().any(|s| {
                (s.codec == WINDOW_STATE_CODEC && s.window_kind == 0)
                    || (s.codec == IOT_STATE_CODEC && matches!(s.window_kind, 7..=15))
            })
    }
    pub fn is_single_timed_iot(&self) -> bool {
        self.states.len() == 1 && self.has_timed_iot()
    }
    pub fn has_references(&self) -> bool {
        !self.reference_tables.is_empty()
    }
    pub fn has_hysteresis(&self) -> bool {
        self.states
            .iter()
            .any(|state| state.codec == IOT_STATE_CODEC && state.window_kind == 6)
    }

    fn from_graph(
        plan: &PhysicalPlan,
        reference_tables: Vec<ReferenceTableDependency>,
    ) -> Result<Self> {
        if plan.has_resample() {
            return Err(rejected(
                "resample recovery currently requires a single linear state, not a DAG",
            ));
        }
        let time_graph = plan.is_recovery_time_graph();
        if plan.has_silence() {
            return Err(rejected(
                "silence requires a linear Source->Silence->[Transform]->Sink plan, not a required DAG",
            ));
        }
        if !plan.side_outputs.is_empty() || (!time_graph && !plan.source_times.is_empty()) {
            return Err(rejected(
                "side outputs and legacy source-time DAGs require restart_fresh",
            ));
        }
        if time_graph
            && (!reference_tables.is_empty()
                || (plan.recovery_event_time() && plan.has_processing_time_state())
                || (!plan.recovery_event_time() && !plan.source_times.is_empty()))
        {
            return Err(rejected(
                "durable time DAG excludes references and mixed processing/event-time domains",
            ));
        }
        if plan.stages.len() < 2 || plan.stages.len() > MAX_CHECKPOINT_STAGES {
            return Err(rejected("graph checkpoint stage count exceeds bound"));
        }
        let has_references = !reference_tables.is_empty();
        let mut sources = Vec::new();
        let mut sinks = Vec::new();
        let mut states = Vec::new();
        let mut used_references = BTreeSet::new();
        let mut stage_operators = Vec::with_capacity(plan.stages.len());
        let mut operators = BTreeSet::new();
        for stage in &plan.stages {
            let ids = match stage {
                PhysicalStage::Transform { steps } => steps
                    .iter()
                    .map(|step| match step {
                        TransformStep::Filter { operator, .. }
                        | TransformStep::Project { operator, .. }
                        | TransformStep::Map { operator, .. } => *operator,
                    })
                    .collect::<Vec<_>>(),
                PhysicalStage::Analysis { operator, .. }
                | PhysicalStage::Branch { operator, .. }
                | PhysicalStage::Route { operator, .. }
                | PhysicalStage::UnionAll { operator, .. }
                | PhysicalStage::BestEffortSink { operator, .. }
                | PhysicalStage::MemorySource { operator, .. }
                | PhysicalStage::CaptureSink { operator, .. }
                | PhysicalStage::WindowAgg { operator, .. }
                | PhysicalStage::Deduplicate { operator, .. }
                | PhysicalStage::Lookup { operator, .. }
                | PhysicalStage::Iot { operator, .. } => vec![*operator],
            };
            if ids.is_empty() || ids.iter().any(|id| !operators.insert(*id)) {
                return Err(rejected(
                    "graph checkpoint has empty or duplicate operator identity",
                ));
            }
            stage_operators.push(ids);
            match stage {
                PhysicalStage::MemorySource { operator, schema, .. } => {
                    if time_graph && !plan.has_analysis() { validate_time_input_schema(schema)?; }
                    sources.push(*operator);
                },
                PhysicalStage::CaptureSink { operator, .. } => sinks.push(*operator),
                PhysicalStage::Analysis { operator, plan: analysis } => {
                    states.push(StateParticipant { id: ParticipantId::analysis(*operator),
                        codec: ANALYSIS_STATE_CODEC, window_kind: if analysis.is_join() { 11 } else { 10 } });
                }
                PhysicalStage::WindowAgg { operator, spec, input, output } if time_graph || matches!(spec.kind, WindowKind::Count { .. }) => {
                    spec.validate()?;
                    if crate::window_output_schema(input,spec)?.fields!=output.fields {return Err(rejected("graph checkpoint window schema mismatch"));}
                    for key in &spec.keys {
                        let field=input.index_of_name(key).ok_or_else(||rejected("graph checkpoint key missing"))?;
                        let ty=&input.fields[field].data_type;
                        if ty.is_nested()||*ty==sparrow_model::DataType::Dynamic{return Err(rejected("graph checkpoint key codec excludes nested/Dynamic"));}
                    }
                    for agg in &spec.aggs {
                        if matches!(agg.func,sparrow_model::AggFn::Min|sparrow_model::AggFn::Max){let ty=agg.input_type(input)?;if ty.is_nested()||ty==sparrow_model::DataType::Dynamic{return Err(rejected("graph checkpoint MIN/MAX codec excludes nested/Dynamic"));}}
                    }
                    states.push(StateParticipant { id: ParticipantId::window(*operator), codec: WINDOW_STATE_CODEC, window_kind: crate::compat::window_kind_tag(spec.kind) });
                }
                PhysicalStage::Iot { operator, spec, input, output } => {
                    if !time_graph && spec.ttl_micros != 0 {
                        return Err(rejected("graph aligned IoT recovery currently requires ttl_micros=0"));
                    }
                    spec.validate(input)?;
                    if spec.output_schema(input)?.fields != output.fields {return Err(rejected("graph IoT output schema mismatch"));}
                    states.push(StateParticipant { id: ParticipantId::iot(*operator), codec: IOT_STATE_CODEC, window_kind: spec.state_kind_tag() });
                }
                PhysicalStage::Lookup { operator, spec, input, output } if has_references => {
                    validate_static_lookup_stage(spec, input, output)?;
                    used_references.insert(spec.table.as_str());
                    // The Lookup is part of the full graph descriptor; it is
                    // intentionally not a mutable state participant.
                    let _ = operator;
                }
                PhysicalStage::Branch { .. } | PhysicalStage::Route { .. } | PhysicalStage::UnionAll { .. } | PhysicalStage::Transform { .. } => {},
                _ => return Err(rejected("graph aligned currently admits required branches with stateless/Count/IoT kernels; ET/PT/Dedup/lossy outputs require additional codecs")),
            }
        }
        sources.sort();
        sinks.sort();
        let edges = plan
            .edges
            .as_ref()
            .ok_or_else(|| rejected("graph checkpoint requires explicit physical edges"))?;
        if edges.len() > MAX_CHECKPOINT_EDGES {
            return Err(rejected("graph checkpoint edge count exceeds bound"));
        }
        let mut edge_pairs = BTreeSet::new();
        let mut incoming = vec![0usize; plan.stages.len()];
        let mut outgoing = vec![0usize; plan.stages.len()];
        for edge in edges {
            if edge.best_effort
                || edge.from >= plan.stages.len()
                || edge.to >= plan.stages.len()
                || edge.from == edge.to
                || !edge_pairs.insert((edge.from, edge.to))
                || !stage_operators[edge.to].contains(&edge.port)
            {
                return Err(rejected(
                    "graph aligned requires connected required edges without lossy edges",
                ));
            }
            let target = match &plan.stages[edge.to] {
                PhysicalStage::Analysis { plan: analysis, .. } if analysis.is_join() => {
                    let crate::AnalysisPlan::Join { spec, left, right, .. } = analysis.as_ref() else { unreachable!() };
                    let PhysicalStage::MemorySource { operator, .. } = &plan.stages[edge.from] else {
                        return Err(rejected("recoverable Join inputs must be direct Sources"));
                    };
                    if operator.raw() == spec.left_input { left }
                    else if operator.raw() == spec.right_input { right }
                    else { return Err(rejected("Join source identity mismatch")); }
                }
                stage => graph_stage_schema(stage, false)?,
            };
            if graph_stage_schema(&plan.stages[edge.from], true)?.fields != target.fields
            {
                return Err(rejected("graph checkpoint edge schema mismatch"));
            }
            incoming[edge.to] += 1;
            outgoing[edge.from] += 1;
        }
        if sources.is_empty()
            || sinks.is_empty()
            || sources.len() > 16
            || sinks.len() > 16
            || edges.is_empty()
        {
            return Err(rejected(
                "graph aligned requires connected required edges without lossy edges",
            ));
        }
        let sink_indices = plan
            .stages
            .iter()
            .enumerate()
            .filter_map(|(index, stage)| {
                matches!(stage, PhysicalStage::CaptureSink { .. }).then_some(index)
            })
            .collect::<BTreeSet<_>>();
        let invalid_degree = plan
            .stages
            .iter()
            .enumerate()
            .any(|(index, stage)| match stage {
                PhysicalStage::MemorySource { .. } => incoming[index] != 0 || outgoing[index] != 1,
                PhysicalStage::CaptureSink { .. } => incoming[index] != 1 || outgoing[index] != 0,
                PhysicalStage::Branch { .. } | PhysicalStage::Route { .. } => {
                    incoming[index] != 1 || !(1..=16).contains(&outgoing[index])
                }
                PhysicalStage::UnionAll { .. } => {
                    !(2..=16).contains(&incoming[index]) || outgoing[index] != 1
                }
                PhysicalStage::Analysis { plan, .. } if plan.is_join() => incoming[index] != 2 || outgoing[index] != 1,
                PhysicalStage::BestEffortSink { .. } => true,
                _ => incoming[index] != 1 || outgoing[index] != 1,
            });
        if invalid_degree {
            return Err(rejected(
                "graph checkpoint topology has an invalid source/terminal degree",
            ));
        }
        // Kahn's pass rejects cycles independently of source reachability.
        let mut remaining_incoming = incoming.clone();
        let mut frontier = remaining_incoming
            .iter()
            .enumerate()
            .filter_map(|(index, degree)| (*degree == 0).then_some(index))
            .collect::<Vec<_>>();
        let mut topological_count = 0usize;
        while let Some(index) = frontier.pop() {
            topological_count += 1;
            for edge in edges.iter().filter(|edge| edge.from == index) {
                remaining_incoming[edge.to] -= 1;
                if remaining_incoming[edge.to] == 0 {
                    frontier.push(edge.to);
                }
            }
        }
        if topological_count != plan.stages.len() {
            return Err(rejected("graph checkpoint topology contains a cycle"));
        }
        // Every physical chain must terminate at a required sink. This also
        // rejects a detached terminal branch that happens to have a source.
        let mut to_sink = sink_indices.clone();
        let mut frontier = sink_indices.iter().copied().collect::<Vec<_>>();
        while let Some(index) = frontier.pop() {
            for edge in edges.iter().filter(|edge| edge.to == index) {
                if to_sink.insert(edge.from) {
                    frontier.push(edge.from);
                }
            }
        }
        if to_sink.len() != plan.stages.len() {
            return Err(rejected(
                "graph checkpoint topology has a non-terminal branch",
            ));
        }
        if used_references
            != reference_tables
                .iter()
                .map(|dep| dep.name.as_str())
                .collect()
        {
            return Err(rejected(
                "checkpoint reference dependencies must exactly match graph Lookup tables",
            ));
        }
        let result = Self {
            source: sources[0],
            sink: sinks[0],
            states,
            reference_tables,
            semantics: crate::canonical::checkpoint_graph(plan, &sources, &sinks)?,
            recovery_prefix_len: None,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        validate_references(&self.reference_tables)?;
        if self.has_analysis_state() && (self.has_references() || self.recovery_prefix_len.is_some()
            || self.states.iter().any(|s| s.codec != ANALYSIS_STATE_CODEC)
            || (self.is_graph() && !self.is_time_graph())
            || (!self.is_graph() && (self.states.len() != 1 || self.states[0].window_kind != 10))) {
            return Err(rejected("analysis requires strict v36/v37 UNNEST or v38 ordered File graph")
                .context("checkpoint_guard", "analysis_profile_mismatch"));
        }
        if self.has_pt_window_state()
            && (self.states.len() != 1
                || self.is_graph()
                || self.has_references()
                || self.has_iot()
                || self.recovery_prefix_len.is_some())
        {
            return Err(rejected(
                "PT window manifest (v34/v35) requires one strict linear PT window without references, IoT or RCP2 prefix",
            )
            .context("checkpoint_guard", "pt_profile_mismatch"));
        }
        if self.has_extended_state()
            && !self.has_pt_window_state()
            && (self.is_graph()
                || self.has_references()
                || self.has_iot()
                || self.recovery_prefix_len.is_some()
                || self.requires_paused_time())
        {
            return Err(rejected(
                "extended aggregate manifest requires strict linear Count/event-time windows without references or IoT",
            ).context("checkpoint_guard", "extended_profile_mismatch"));
        }
        if self.has_resample()
            && (self.is_graph()
                || self.has_references()
                || self.states.len() != 1
                || self.recovery_prefix_len.is_some())
        {
            return Err(rejected(
                "resample manifest requires one strict linear state without references",
            ));
        }
        if !self.is_time_graph()
            && self.requires_paused_time()
            && (self.is_graph()
                || self.has_references()
                || self.recovery_prefix_len.is_some()
                || self
                    .states
                    .iter()
                    .any(|s| s.codec == WINDOW_STATE_CODEC && matches!(s.window_kind, 2 | 3)))
        {
            return Err(rejected("paused-time recovery requires a linear plan without references, event time or relaxed semantics"));
        }
        if self.is_time_graph() && !self.has_analysis_state()
            && (self.has_references()
                || self.recovery_prefix_len.is_some()
                || (self.has_event_time_state()
                    && self
                        .states
                        .iter()
                        .any(|s| s.window_kind == 0 || matches!(s.window_kind, 7..=12)))
                || !self
                    .states
                    .iter()
                    .any(|s| matches!(s.window_kind, 0 | 2 | 3 | 7..=12)))
        {
            return Err(rejected(
                "invalid durable time graph participant/domain contract",
            ));
        }
        if self.has_references() && self.recovery_prefix_len.is_some() {
            return Err(rejected(
                "reference checkpoint requires full pipeline semantics",
            ));
        }
        if self.states.len()
            > if self.is_graph() {
                MAX_GRAPH_CHECKPOINT_STATES
            } else {
                MAX_CHECKPOINT_STATES
            }
            || self.source == self.sink
            || (self.has_iot() && self.recovery_prefix_len.is_some())
            || self
                .semantics
                .len()
                .saturating_add(if self.recovery_prefix_len.is_some() {
                    RECOVERY_PREFIX_MAGIC.len() + 4
                } else {
                    0
                })
                > crate::canonical::MAX_STATE_SEMANTICS_BYTES
            || !self.semantics.starts_with(b"CP01")
            || self
                .recovery_prefix_len
                .is_some_and(|n| n < 4 || n > self.semantics.len())
        {
            return Err(rejected(
                "invalid checkpoint participant manifest or semantics",
            ));
        }
        let mut ids = BTreeSet::from([self.source, self.sink]);
        if self.is_graph() {
            let (sources, sinks) = self.graph_ports()?;
            ids.clear();
            if sources.first() != Some(&self.source)
                || sinks.first() != Some(&self.sink)
                || self.recovery_prefix_len.is_some()
                || sources.iter().chain(&sinks).any(|id| !ids.insert(*id))
            {
                return Err(rejected("duplicate/inconsistent graph checkpoint ports"));
            }
        }
        for state in &self.states {
            let ParticipantId::State {
                operator,
                slot,
                shard,
            } = state.id
            else {
                return Err(rejected("invalid state participant role"));
            };
            let valid_window = (state.codec == WINDOW_STATE_CODEC
                && slot.raw() == 1
                && matches!(state.window_kind, 0..=4))
                || (state.codec == WINDOW_EXT_STATE_CODEC
                    && slot.raw() == 1
                    && matches!(state.window_kind, 0..=4));
            let valid_buffered = state.codec == BUFFERED_WINDOW_STATE_CODEC
                && slot.raw() == 1
                && matches!(state.window_kind, 5..=9);
            let valid_iot = state.codec == IOT_STATE_CODEC
                && slot.raw() == 3
                && matches!(state.window_kind, 4..=15);
            let valid_analysis = state.codec == ANALYSIS_STATE_CODEC && slot.raw() == 4
                && matches!(state.window_kind, 10 | 11);
            if shard != 0 || !ids.insert(operator) || !(valid_window || valid_iot || valid_buffered || valid_analysis) {
                return Err(rejected(
                    "duplicate/unsupported checkpoint participant, slot, shard or codec",
                ));
            }
        }
        if !self.is_time_graph()
            && self.states.len() > 1
            && self.states.iter().any(|s| matches!(s.window_kind, 2 | 3))
        {
            return Err(rejected("unsupported multi-state time policy"));
        }
        if self.has_references()
            && self
                .states
                .iter()
                .any(|state| state.codec == WINDOW_STATE_CODEC && state.window_kind != 1)
        {
            return Err(rejected(
                "reference checkpoint requires Count windows; ET/PT state is not recoverable",
            ));
        }
        if self.has_buffered_state()
            && (self.states.len() != 1
                || self.is_graph()
                || self.has_references()
                || self.recovery_prefix_len.is_some())
        {
            return Err(rejected(
                "sliding count (codec 4) requires one strict linear state without references or RCP2 prefix",
            )
            .context("checkpoint_guard", "buffered_profile_mismatch"));
        }
        Ok(())
    }

    pub fn check_compatible(&self, live: &Self) -> Result<()> {
        self.validate()?;
        live.validate()?;
        let compatible = self.source == live.source
            && self.states == live.states
            && self.reference_tables == live.reference_tables
            && match (self.recovery_prefix_len, live.recovery_prefix_len) {
                (Some(a), Some(b)) => self.semantics[..a] == live.semantics[..b],
                // Never retroactively loosen a plain CP01 snapshot's contract.
                _ => self.sink == live.sink && self.semantics == live.semantics,
            };
        if !compatible {
            return Err(rejected("checkpoint participant set, schema, codec or pipeline semantics changed; explicit fresh/reset or original backup required"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let extra = if self.recovery_prefix_len.is_some() {
            RECOVERY_PREFIX_MAGIC.len() + 4
        } else {
            0
        };
        let references = if self.has_references() {
            2 + self
                .reference_tables
                .iter()
                .map(|dep| 46 + dep.name.len())
                .sum::<usize>()
        } else {
            0
        };
        let mut out = Vec::with_capacity(
            18 + self.states.len() * 11 + self.semantics.len() + extra + references,
        );
        // CPL2 was an abandoned prototype; never reuse its magic. CPL1 bytes
        // stay unchanged, and v8 Store profile admission guards old readers.
        out.extend_from_slice(if self.has_references() {
            b"CPL3"
        } else {
            b"CPL1"
        });
        out.extend_from_slice(&self.source.raw().to_le_bytes());
        out.extend_from_slice(&self.sink.raw().to_le_bytes());
        out.extend_from_slice(&(self.states.len() as u16).to_le_bytes());
        for state in &self.states {
            let ParticipantId::State {
                operator,
                slot,
                shard,
            } = state.id
            else {
                unreachable!()
            };
            out.extend_from_slice(&operator.raw().to_le_bytes());
            out.extend_from_slice(&slot.raw().to_le_bytes());
            out.extend_from_slice(&shard.to_le_bytes());
            out.extend_from_slice(&state.codec.to_le_bytes());
            out.push(state.window_kind);
        }
        if self.has_references() {
            out.extend_from_slice(&(self.reference_tables.len() as u16).to_le_bytes());
            for dep in &self.reference_tables {
                out.extend_from_slice(&(dep.name.len() as u16).to_le_bytes());
                out.extend_from_slice(dep.name.as_bytes());
                out.extend_from_slice(&dep.revision.to_le_bytes());
                out.extend_from_slice(&dep.canonical_sha256);
                out.extend_from_slice(&dep.runtime_crc32.to_le_bytes());
            }
        }
        if let Some(n) = self.recovery_prefix_len {
            out.extend_from_slice(
                &((self.semantics.len() + RECOVERY_PREFIX_MAGIC.len() + 4) as u32).to_le_bytes(),
            );
            out.extend_from_slice(RECOVERY_PREFIX_MAGIC);
            out.extend_from_slice(&(n as u32).to_le_bytes());
        } else {
            out.extend_from_slice(&(self.semantics.len() as u32).to_le_bytes());
        }
        out.extend_from_slice(&self.semantics);
        Ok(out)
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            if b.len() < n {
                return Err(rejected("truncated checkpoint participant manifest"));
            }
            let (head, tail) = b.split_at(n);
            *b = tail;
            Ok(head)
        }
        let version = take(&mut bytes, 4)?;
        if version != b"CPL1" && version != b"CPL3" {
            return Err(rejected("unsupported checkpoint plan codec"));
        }
        let source = OperatorId::new(u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap()));
        let sink = OperatorId::new(u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap()));
        let n = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()) as usize;
        if n > MAX_GRAPH_CHECKPOINT_STATES {
            return Err(rejected("checkpoint state participant count exceeds bound"));
        }
        let mut states = Vec::with_capacity(n);
        for _ in 0..n {
            let operator =
                OperatorId::new(u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap()));
            let slot =
                StateSlotId::new(u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()));
            let shard = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap());
            let codec = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap());
            let window_kind = take(&mut bytes, 1)?[0];
            states.push(StateParticipant {
                id: ParticipantId::State {
                    operator,
                    slot,
                    shard,
                },
                codec,
                window_kind,
            });
        }
        let mut reference_tables = Vec::new();
        if version == b"CPL3" {
            let count = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()) as usize;
            if count == 0 || count > MAX_CHECKPOINT_REFERENCES {
                return Err(rejected(
                    "checkpoint reference dependency count exceeds bound",
                ));
            }
            for _ in 0..count {
                let n = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()) as usize;
                if n == 0 || n > 64 {
                    return Err(rejected("checkpoint reference name exceeds bound"));
                }
                let name = std::str::from_utf8(take(&mut bytes, n)?)
                    .map_err(|_| rejected("invalid checkpoint reference name encoding"))?
                    .to_owned();
                let revision = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
                let canonical_sha256 = take(&mut bytes, 32)?.try_into().unwrap();
                let runtime_crc32 = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap());
                reference_tables.push(ReferenceTableDependency {
                    name,
                    revision,
                    canonical_sha256,
                    runtime_crc32,
                });
            }
        }
        let n = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap()) as usize;
        if n > crate::canonical::MAX_STATE_SEMANTICS_BYTES {
            return Err(rejected("checkpoint semantics exceeds bound"));
        }
        let mut semantics = take(&mut bytes, n)?;
        let recovery_prefix_len = if semantics.starts_with(RECOVERY_PREFIX_MAGIC) {
            semantics = &semantics[RECOVERY_PREFIX_MAGIC.len()..];
            Some(u32::from_le_bytes(take(&mut semantics, 4)?.try_into().unwrap()) as usize)
        } else {
            None
        };
        if !bytes.is_empty() {
            return Err(rejected("trailing checkpoint participant manifest"));
        }
        let plan = Self {
            source,
            sink,
            states,
            reference_tables,
            semantics: semantics.to_vec(),
            recovery_prefix_len,
        };
        plan.validate()?;
        Ok(plan)
    }
}

fn validate_references(deps: &[ReferenceTableDependency]) -> Result<()> {
    if deps.len() > MAX_CHECKPOINT_REFERENCES {
        return Err(rejected(
            "checkpoint reference dependency count exceeds bound",
        ));
    }
    let mut previous: Option<&str> = None;
    for dep in deps {
        if dep.name.is_empty()
            || dep.name.len() > 64
            || !dep
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            || dep.revision == 0
            || dep.revision > i64::MAX as u64
            || dep.canonical_sha256 == [0; 32]
            || previous.is_some_and(|name| name >= dep.name.as_str())
        {
            return Err(rejected(
                "invalid, duplicate or unsorted checkpoint reference dependency",
            ));
        }
        previous = Some(&dep.name);
    }
    Ok(())
}

/// Validate the part of a static Lookup contract that is present in a
/// physical plan.  The actual table schema, key order, CRC and owner are
/// checked by the runtime against the attached verified snapshot; keeping the
/// input/output and key-shape checks here prevents a caller from enabling the
/// profile by toggling only a reference-present flag.
fn validate_static_lookup_stage(
    spec: &crate::LookupSpec,
    input: &sparrow_model::Schema,
    output: &sparrow_model::Schema,
) -> Result<()> {
    spec.validate()?;
    if spec.temporal || spec.as_of_field.is_some() {
        return Err(rejected(
            "reference checkpoint excludes temporal/as-of Lookup",
        ));
    }
    let stream_keys = spec.stream_keys.iter().collect::<BTreeSet<_>>();
    let table_keys = spec.table_keys.iter().collect::<BTreeSet<_>>();
    let keep = spec.keep.iter().collect::<BTreeSet<_>>();
    if stream_keys.len() != spec.stream_keys.len()
        || table_keys.len() != spec.table_keys.len()
        || keep.len() != spec.keep.len()
        || spec
            .stream_keys
            .iter()
            .any(|name| input.field_by_name(name).is_none())
        || output.fields.len() != input.fields.len().saturating_add(spec.keep.len())
        || output.fields[..input.fields.len()] != input.fields
        || output.fields[input.fields.len()..]
            .iter()
            .zip(&spec.keep)
            .any(|(field, name)| &field.name != name || !field.nullable)
    {
        return Err(rejected(
            "reference checkpoint Lookup schema/key contract mismatch",
        ));
    }
    Ok(())
}

fn graph_stage_schema(stage: &PhysicalStage, output: bool) -> Result<&sparrow_model::Schema> {
    Ok(match stage {
        PhysicalStage::Analysis { plan, .. } => if output { plan.output() } else { plan.input() },
        PhysicalStage::MemorySource { schema, .. }
        | PhysicalStage::CaptureSink { schema, .. }
        | PhysicalStage::BestEffortSink { schema, .. } => schema,
        PhysicalStage::Branch { input, .. }
        | PhysicalStage::Route { input, .. }
        | PhysicalStage::UnionAll { input, .. }
        | PhysicalStage::Deduplicate { input, .. } => input,
        PhysicalStage::Iot {
            input,
            output: schema,
            ..
        } => {
            if output {
                schema
            } else {
                input
            }
        }
        PhysicalStage::WindowAgg {
            input,
            output: schema,
            ..
        }
        | PhysicalStage::Lookup {
            input,
            output: schema,
            ..
        } => {
            if output {
                schema
            } else {
                input
            }
        }
        PhysicalStage::Transform { steps } => {
            let step = if output { steps.last() } else { steps.first() }
                .ok_or_else(|| rejected("empty graph transform stage"))?;
            match step {
                TransformStep::Filter { input, .. } => input,
                TransformStep::Project {
                    input,
                    output: schema,
                    ..
                }
                | TransformStep::Map {
                    input,
                    output: schema,
                    ..
                } => {
                    if output {
                        schema
                    } else {
                        input
                    }
                }
            }
        }
    })
}

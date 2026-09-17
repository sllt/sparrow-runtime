//! Bounded checkpoint participants for the supported linear execution path.
//! This is not DAG admission and does not replace the legacy single-window layout.
use std::collections::BTreeSet;

use crate::{PhysicalPlan, PhysicalStage, TransformStep};
use sparrow_model::{ErrorCode, OperatorId, Result, SparrowError, StateSlotId, WindowKind};

pub const MAX_CHECKPOINT_STATES: usize = 2;
pub const MAX_GRAPH_CHECKPOINT_STATES: usize = 16;
pub const MAX_CHECKPOINT_OPERATORS: usize = 64;
pub const MAX_CHECKPOINT_STAGES: usize = 64;
pub const WINDOW_STATE_CODEC: u16 = 1;
/// Versioned keyed IoT state.  It is intentionally distinct from the
/// WindowFreeze codec; an IoT value is not a window accumulator.
pub const IOT_STATE_CODEC: u16 = 2;
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
            WINDOW_STATE_CODEC => u8::from(self.window_kind == 1),
            IOT_STATE_CODEC if matches!(self.window_kind, 4 | 5) => self.window_kind,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointPlan {
    pub source: OperatorId,
    pub sink: OperatorId,
    pub states: Vec<StateParticipant>,
    /// Full ordered computation, including downstream transforms. Fusion groups
    /// and catalog revision numbers are not semantic identities.
    pub semantics: Vec<u8>,
    /// Plain CP01 had no prefix boundary and retains its whole-plan contract.
    pub recovery_prefix_len: Option<usize>,
}

fn rejected(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::UnsupportedRestore, message)
}

impl CheckpointPlan {
    pub fn from_physical(plan: &PhysicalPlan) -> Result<Self> {
        if plan.edges.is_some() { return Self::from_graph(plan); }
        let Some(PhysicalStage::MemorySource {
            operator: source,
            schema,
            ..
        }) = plan.stages.first()
        else {
            return Err(rejected("checkpoint requires one leading source"));
        };
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
        for stage in &plan.stages[1..plan.stages.len() - 1] {
            match stage {
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
                    register(*operator)?;
                    spec.validate()?;
                    if matches!(spec.kind, WindowKind::TumblingProcessingTime { .. }) {
                        return Err(rejected(
                            "processing-time window checkpoint is not supported",
                        ));
                    }
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
                    states.push(StateParticipant {
                        id: ParticipantId::window(*operator),
                        codec: WINDOW_STATE_CODEC,
                        window_kind: crate::compat::window_kind_tag(spec.kind),
                    });
                    if states.len() > MAX_CHECKPOINT_STATES {
                        return Err(rejected(
                            "checkpoint supports at most two state participants",
                        ));
                    }
                    current = output;
                }
                PhysicalStage::Iot { operator, spec, input } => {
                    // K4 currently supports aligned IoT state only when no
                    // wall-clock eviction has to be reconstructed.  The
                    // runtime v6 codec may then persist the value map using
                    // its own participant identity (slot 3), never as a
                    // WindowFreeze.
                    if spec.ttl_micros != 0 {
                        return Err(rejected(
                            "aligned IoT recovery currently requires ttl_micros=0",
                        ));
                    }
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
                    if current.fields != input.fields {
                        return Err(rejected("checkpoint IoT input schema mismatch"));
                    }
                    current = input;
                }
                _ => {
                    return Err(rejected(
                        "checkpoint excludes additional sources/sinks, Dedup and Lookup",
                    ))
                }
            }
        }
        if let PhysicalStage::CaptureSink { schema, .. } = plan.stages.last().unwrap() {
            if current.fields != schema.fields {
                return Err(rejected("checkpoint sink schema mismatch"));
            }
        }
        // Open only Count/IoT combinations.  ET/PT states remain governed by
        // the existing single-window/two-Count matrix and cannot be mixed with
        // an IoT participant until their timer/time semantics are extended.
        if states.iter().any(|s| matches!(s.window_kind, 2 | 3))
            && states.iter().any(|s| s.codec == IOT_STATE_CODEC)
        {
            return Err(rejected(
                "IoT checkpoint cannot be combined with event/processing-time windows",
            ));
        }
        let (semantics, recovery_prefix_len) = crate::canonical::checkpoint_pipeline(plan)?;
        let has_iot = states.iter().any(|s| s.codec == IOT_STATE_CODEC);
        let result = Self {
            source: *source,
            sink: *sink,
            states,
            semantics,
            // IoT state depends on the complete computation descriptor.  Do
            // not apply the linear RCP2 downstream-only relaxation to it.
            recovery_prefix_len: (!has_iot).then_some(recovery_prefix_len),
        };
        result.validate()?;
        Ok(result)
    }

    pub fn participants(&self) -> BTreeSet<ParticipantId> {
        self.source_ids().into_iter().map(ParticipantId::Source)
            .chain(self.states.iter().map(|s| s.id))
            .chain(self.sink_ids().into_iter().map(ParticipantId::Sink))
            .collect()
    }

    pub fn is_graph(&self) -> bool { self.semantics.starts_with(b"CP01DAG1") }
    fn graph_ports(&self) -> Result<(Vec<OperatorId>, Vec<OperatorId>)> {
        let mut bytes = self.semantics.get(8..).ok_or_else(|| rejected("truncated graph identity"))?;
        let mut read = || -> Result<Vec<OperatorId>> {
            if bytes.len() < 2 { return Err(rejected("truncated graph port count")); }
            let n = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
            bytes = &bytes[2..];
            if n == 0 || n > 16 || bytes.len() < n * 4 { return Err(rejected("invalid graph port count")); }
            let ids = bytes[..n*4].chunks_exact(4).map(|raw| OperatorId::new(u32::from_le_bytes(raw.try_into().unwrap()))).collect();
            bytes = &bytes[n*4..]; Ok(ids)
        };
        Ok((read()?, read()?))
    }
    pub fn source_ids(&self) -> Vec<OperatorId> { if self.is_graph() { self.graph_ports().map(|p| p.0).unwrap_or_default() } else { vec![self.source] } }
    pub fn sink_ids(&self) -> Vec<OperatorId> { if self.is_graph() { self.graph_ports().map(|p| p.1).unwrap_or_default() } else { vec![self.sink] } }
    pub fn has_iot(&self) -> bool { self.states.iter().any(|s| s.codec == IOT_STATE_CODEC) }

    fn from_graph(plan: &PhysicalPlan) -> Result<Self> {
        if !plan.side_outputs.is_empty() || !plan.source_times.is_empty() {return Err(rejected("side outputs and source-time DAGs currently require restart_fresh"));}
        let mut sources = Vec::new(); let mut sinks = Vec::new(); let mut states = Vec::new();
        for stage in &plan.stages {
            match stage {
                PhysicalStage::MemorySource { operator, .. } => sources.push(*operator),
                PhysicalStage::CaptureSink { operator, .. } => sinks.push(*operator),
                PhysicalStage::WindowAgg { operator, spec, input, output } if matches!(spec.kind, WindowKind::Count { .. }) => {
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
                    states.push(StateParticipant { id: ParticipantId::window(*operator), codec: WINDOW_STATE_CODEC, window_kind: 1 });
                }
                PhysicalStage::Iot { operator, spec, input } => {
                    if spec.ttl_micros != 0 {
                        return Err(rejected("graph aligned IoT recovery currently requires ttl_micros=0"));
                    }
                    spec.validate(input)?;
                    states.push(StateParticipant { id: ParticipantId::iot(*operator), codec: IOT_STATE_CODEC, window_kind: spec.state_kind_tag() });
                }
                PhysicalStage::Branch { .. } | PhysicalStage::Route { .. } | PhysicalStage::UnionAll { .. } | PhysicalStage::Transform { .. } => {},
                _ => return Err(rejected("graph aligned currently admits required branches with stateless/Count kernels only; ET/PT/Lookup/Dedup/lossy outputs require additional codecs")),
            }
        }
        sources.sort(); sinks.sort();
        if sources.is_empty() || sinks.is_empty() || sources.len() > 16 || sinks.len() > 16
            || plan.edges.as_ref().unwrap().iter().any(|e| e.best_effort) { return Err(rejected("graph aligned requires 1..16 sources and required sinks without lossy edges")); }
        let result = Self { source: sources[0], sink: sinks[0], states,
            semantics: crate::canonical::checkpoint_graph(plan, &sources, &sinks)?, recovery_prefix_len: None };
        result.validate()?; Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        if self.states.len() > if self.is_graph() { MAX_GRAPH_CHECKPOINT_STATES } else { MAX_CHECKPOINT_STATES }
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
            if sources.first() != Some(&self.source) || sinks.first() != Some(&self.sink) || self.recovery_prefix_len.is_some()
                || sources.iter().chain(&sinks).any(|id| !ids.insert(*id)) {
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
            let valid_window = state.codec == WINDOW_STATE_CODEC
                && slot.raw() == 1
                && matches!(state.window_kind, 1..=3);
            let valid_iot = state.codec == IOT_STATE_CODEC
                && slot.raw() == 3
                && matches!(state.window_kind, 4 | 5);
            if shard != 0 || !ids.insert(operator) || !(valid_window || valid_iot) {
                return Err(rejected(
                    "duplicate/unsupported checkpoint participant, slot, shard or codec",
                ));
            }
        }
        if self.states.len() > 1 && self.states.iter().any(|s| matches!(s.window_kind, 2 | 3)) {
            return Err(rejected("unsupported multi-state time policy"));
        }
        Ok(())
    }

    pub fn check_compatible(&self, live: &Self) -> Result<()> {
        self.validate()?;
        live.validate()?;
        let compatible = self.source == live.source
            && self.states == live.states
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
        let mut out =
            Vec::with_capacity(18 + self.states.len() * 11 + self.semantics.len() + extra);
        out.extend_from_slice(b"CPL1");
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
        if version != b"CPL1" {
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
            semantics: semantics.to_vec(),
            recovery_prefix_len,
        };
        plan.validate()?;
        Ok(plan)
    }
}

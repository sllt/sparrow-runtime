//! Aligned checkpoint barrier coordination on the Kernel path.
//!
//! Barriers travel with the mailbox. Stateful stages freeze; the sink waits
//! for a real outbox ack (not "channel empty") before the supervisor commits.
//! Acks carry `checkpoint_id`. Stale ids from a timed-out barrier must not
//! pair with a later cut.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use sparrow_model::{ErrorCode, InflightCounter, MemoryOwner, Result, SparrowError};

use crate::window::WindowFreeze;
use sparrow_plan::{CheckpointPlan, ParticipantId};

/// Encoded state plus its working-memory lease. Moving an ACK never clones state.
#[derive(Debug)]
pub struct EncodedFreeze {
    pub(crate) bytes: Vec<u8>,
    pub(crate) lease: sparrow_model::MemoryLease,
    /// Frame written with participant codec 3 (accumulator tags 8/9 allowed).
    /// The envelope encoder requires this to equal the manifest codec.
    pub(crate) ext: bool,
    /// Codec 4 (`BWF1`) sliding count frame; must equal the manifest codec.
    pub(crate) buffered: bool,
}

impl EncodedFreeze {
    pub(crate) fn identity(&self) -> Result<(ParticipantId, u8, usize)> {
        let header = crate::checkpoint::FreezeHeader::parse(&self.bytes)?;
        Ok((
            ParticipantId::State {
                operator: header.operator,
                slot: header.slot,
                shard: 0,
            },
            header.kind,
            header.entries,
        ))
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn from_operator(
        op: &crate::window::WindowOperator,
        owner: &Arc<sparrow_model::MemoryOwner>,
        max_keys: usize,
    ) -> Result<Self> {
        op.check_freeze_encode_bound(max_keys)?;
        let capacity = op.try_estimated_freeze_bytes()?.saturating_add(256);
        let lease = owner.acquire(sparrow_model::CreditKind::Reservation, capacity)?;
        let mut bytes = Vec::with_capacity(capacity);
        let codec = op.accumulator_codec();
        op.encode_freeze_into_codec(&mut bytes, max_keys, codec)?;
        if bytes.len() > capacity {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                "freeze size estimate underflow",
            ));
        }
        Ok(Self { bytes, lease, ext: codec == crate::aggregate::AccumulatorCodec::WindowExt, buffered: false })
    }

    pub(crate) fn from_buffered(
        op: &crate::buffered_window::BufferedWindow,
        operator: sparrow_model::OperatorId,
        owner: &Arc<MemoryOwner>,
        max_keys: usize,
    ) -> Result<Self> {
        op.check_freeze_bound(max_keys)?;
        let capacity = op.estimated_freeze_bytes().saturating_add(256);
        if capacity as u64 > crate::checkpoint::MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(ErrorCode::BoundExceeded, "buffered freeze exceeds snapshot bound"));
        }
        let lease = owner.acquire(sparrow_model::CreditKind::Reservation, capacity)?;
        let mut bytes = Vec::with_capacity(capacity);
        op.encode_freeze_into(operator, &mut bytes, max_keys)?;
        if bytes.len() > capacity {
            return Err(SparrowError::new(ErrorCode::Internal, "buffered freeze estimate underflow"));
        }
        Ok(Self { bytes, lease, ext: false, buffered: true })
    }

    pub fn from_iot(op: &crate::iot::IotOperator, owner: &Arc<MemoryOwner>, max_keys: usize) -> Result<Self> {
        let capacity = op.estimated_freeze_bytes().saturating_add(256);
        if capacity as u64 > crate::checkpoint::MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(ErrorCode::BoundExceeded, "IoT freeze exceeds snapshot bound"));
        }
        let lease = owner.acquire(sparrow_model::CreditKind::Reservation, capacity)?;
        let mut bytes = Vec::with_capacity(capacity);
        op.encode_freeze_into(&mut bytes, max_keys)?;
        if bytes.len() > capacity {
            return Err(SparrowError::new(ErrorCode::Internal, "IoT freeze estimate underflow"));
        }
        Ok(Self { bytes, lease, ext: false, buffered: false })
    }
}

#[derive(Debug)]
pub enum AlignedAck {
    Participant {
        attempt: u64,
        checkpoint_id: u64,
        participant: ParticipantId,
        outcome: ParticipantOutcome,
    },
    FreezeFailed {
        checkpoint_id: u64,
        error: SparrowError,
    },
    WindowFrozen {
        checkpoint_id: u64,
        freeze: EncodedFreeze,
    },
    SinkFlushed {
        checkpoint_id: u64,
        ok: bool,
        dropped: u64,
    },
}

impl AlignedAck {
    pub fn checkpoint_id(&self) -> u64 {
        match self {
            Self::Participant { checkpoint_id, .. }
            | Self::WindowFrozen { checkpoint_id, .. }
            | Self::SinkFlushed { checkpoint_id, .. }
            | Self::FreezeFailed { checkpoint_id, .. } => *checkpoint_id,
        }
    }
}

pub struct AlignedJob {
    pub restore: Option<WindowFreeze>,
    /// K1 mode. Legacy embedding callers leave this None; the Server always
    /// uses the participant protocol, including its zero/single-window cases.
    pub pipeline: Option<PipelineRestore>,
    pub acks: AlignedAcks,
    pub outbox: Arc<InflightCounter>,
}

/// Internal per-attempt handoff. Cloning a stage context must never deep-clone
/// a decoded restore snapshot; the eligible Window consumes it exactly once.
pub(crate) struct RuntimeAligned {
    pub participant_mode: bool,
    pub restore: Mutex<Option<RestoreState>>,
    pub windows: Mutex<BTreeMap<sparrow_model::OperatorId, crate::window::WindowOperator>>,
    pub iot: Mutex<BTreeMap<sparrow_model::OperatorId, crate::iot::IotOperator>>,
    /// Prepared v31/v32 sliding count operators (restored or fresh-durable).
    pub(crate) buffered: Mutex<BTreeMap<sparrow_model::OperatorId, crate::buffered_window::BufferedWindow>>,
    pub acks: AlignedAcks,
    pub outbox: Arc<InflightCounter>,
    _sink_binding: Option<SinkRestoreBinding>,
}
pub(crate) struct RestoreState {
    pub freeze: WindowFreeze,
    // Payload drops before its adoption credit.
    _lease: sparrow_model::MemoryLease,
}
impl RuntimeAligned {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn adopt(job: AlignedJob, owner: &Arc<sparrow_model::MemoryOwner>) -> Result<Arc<Self>> {
        Self::adopt_with_credit(job, owner, None)
    }

    fn adopt_with_credit(
        job: AlignedJob,
        owner: &Arc<sparrow_model::MemoryOwner>,
        mut credit: Option<crate::checkpoint::RestoreCredit>,
    ) -> Result<Arc<Self>> {
        let restore = job
            .restore
            .map(|freeze| {
                let lease = restore_lease(
                    &mut credit,
                    owner,
                    ParticipantId::State { operator: freeze.operator, slot: freeze.slot, shard: 0 },
                    freeze.resident_bytes(),
                )?;
                Ok::<_, SparrowError>(RestoreState {
                    freeze,
                    _lease: lease,
                })
            })
            .transpose()?;
        Ok(Arc::new(Self {
            participant_mode: false,
            restore: Mutex::new(restore),
            windows: Mutex::new(BTreeMap::new()),
            buffered: Mutex::new(BTreeMap::new()),
            iot: Mutex::new(BTreeMap::new()),
            acks: job.acks,
            outbox: job.outbox,
            _sink_binding: None,
        }))
    }

    /// Validate and restore every participant before any stage/source is spawned.
    /// All decoded state shares one admission budget and is released per instance.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn prepare(
        job: AlignedJob,
        plan: &sparrow_plan::PhysicalPlan,
        owner: &Arc<sparrow_model::MemoryOwner>,
        attempt: u64,
        max_keys: usize,
        max_timers: usize,
        processing_time: Option<i64>,
    ) -> Result<Arc<Self>> {
        Self::prepare_with_credit(job, plan, owner, attempt, max_keys, max_timers, processing_time, None)
    }

    /// `credit` is Store-reserved restore memory on `owner`; each restored
    /// participant consumes exactly its own lease (no second acquisition).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_with_credit(
        job: AlignedJob,
        plan: &sparrow_plan::PhysicalPlan,
        owner: &Arc<sparrow_model::MemoryOwner>,
        attempt: u64,
        max_keys: usize,
        max_timers: usize,
        processing_time: Option<i64>,
        mut credit: Option<crate::checkpoint::RestoreCredit>,
    ) -> Result<Arc<Self>> {
        if credit.as_ref().is_some_and(|credit| !credit.belongs_to(owner)) {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "restore credit belongs to another Job owner",
            ));
        }
        let Some(pipeline) = job.pipeline else {
            return Self::adopt_with_credit(job, owner, credit);
        };
        if job.restore.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "cannot combine legacy and participant restore",
            ));
        }
        let live_plan = if pipeline.plan.has_references() {
            // Kernel admission has already verified the attached table Arc,
            // including its canonical digest, runtime CRC, owner and concrete
            // Lookup schema. Rebuild the physical manifest here as a second
            // guard, carrying the persisted dependencies rather than sending
            // a CPL3 plan through the legacy from_physical path.
            CheckpointPlan::from_physical_with_references(
                plan,
                pipeline.plan.reference_tables.clone(),
            )?
        } else {
            CheckpointPlan::from_physical(plan)?
        };
        pipeline.check_compatible(&live_plan, owner)?;
        if pipeline.sink.is_some() && job.acks.output_sequence().is_some() {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                "File/JetStream sink checkpoint cannot adopt a source output cursor"));
        }
        let mut restored = BTreeMap::new();
        let mut restored_iot = BTreeMap::new();
        let mut restored_buffered = BTreeMap::new();
        if pipeline.restore.is_none() && (!pipeline.iot.is_empty() || !pipeline.buffered.is_empty()) {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore, "IoT state supplied without restored participant envelope"));
        }
        if let Some(states) = pipeline.restore {
            if states.iter().any(|s| s.entries.len() > max_keys)
                || pipeline.iot.iter().any(|s| s.entries.len() > max_keys)
                || pipeline.buffered.iter().any(|s| s.groups.len() > max_keys)
                || states.len().saturating_add(pipeline.iot.len()).saturating_add(pipeline.buffered.len()) != pipeline.plan.states.len()
            {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "restore participant/per-instance entry limit",
                ));
            }
            for freeze in states {
                let participant = ParticipantId::State {
                    operator: freeze.operator,
                    slot: freeze.slot,
                    shard: 0,
                };
                if !pipeline.plan.states.iter().any(|s| s.id == participant)
                    || restored.contains_key(&freeze.operator)
                {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "unknown or duplicate restored participant",
                    ));
                }
                let lease = restore_lease(&mut credit, owner, participant, freeze.resident_bytes())?;
                restored.insert(
                    freeze.operator,
                    RestoreState {
                        freeze,
                        _lease: lease,
                    },
                );
            }
            for freeze in pipeline.iot {
                let participant = ParticipantId::iot(freeze.operator);
                if !pipeline.plan.states.iter().any(|s| s.id == participant
                    && s.codec == sparrow_plan::checkpoint::IOT_STATE_CODEC
                    && s.freeze_kind() == freeze.kind)
                    || restored_iot.contains_key(&freeze.operator) {
                    return Err(SparrowError::new(ErrorCode::UnsupportedRestore, "unknown or duplicate restored IoT participant"));
                }
                let lease = restore_lease(&mut credit, owner, participant, freeze.resident_bytes())?;
                restored_iot.insert(freeze.operator, (freeze, lease));
            }
            for freeze in pipeline.buffered {
                let participant = ParticipantId::State { operator: freeze.operator, slot: freeze.slot, shard: 0 };
                if !pipeline.plan.states.iter().any(|s| s.id == participant
                    && s.codec == sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC
                    && s.freeze_kind() == freeze.kind)
                    || restored_buffered.contains_key(&freeze.operator) {
                    return Err(SparrowError::new(ErrorCode::UnsupportedRestore, "unknown or duplicate restored buffered participant"));
                }
                let lease = restore_lease(&mut credit, owner, participant, freeze.resident_bytes())?;
                restored_buffered.insert(freeze.operator, (freeze, lease));
            }
        }
        let mut windows = BTreeMap::new();
        let mut iot = BTreeMap::new();
        let mut buffered = BTreeMap::new();
        for stage in &plan.stages {
            if let sparrow_plan::PhysicalStage::WindowAgg { operator, spec, input, .. } = stage {
                if spec.kind.is_buffered() {
                    let participant = ParticipantId::window(*operator);
                    if !pipeline.plan.states.iter().any(|s| s.id == participant
                        && s.codec == sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC) {
                        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                            "buffered window has no checkpoint participant in this profile"));
                    }
                    let mut op = crate::buffered_window::BufferedWindow::new(
                        spec.clone(), input.clone(), owner.clone(), max_keys, max_timers, false)?;
                    op.set_durable()?;
                    if let Some((freeze, lease)) = restored_buffered.remove(operator) {
                        op.restore_freeze(*operator, freeze).map_err(|e| e.at_operator(*operator))?;
                        drop(lease);
                    }
                    buffered.insert(*operator, op);
                    continue;
                }
            }
            if let sparrow_plan::PhysicalStage::WindowAgg {
                operator,
                spec,
                input,
                ..
            } = stage
            {
                let mut window = crate::window::WindowOperator::new(
                    *operator,
                    spec.clone(),
                    input.clone(),
                    owner.clone(),
                    max_keys,
                    max_timers,
                )?;
                if let Some(restored) = restored.remove(operator) {
                    window.validate_participant_restore(&restored.freeze)?;
                    window.restore_participant_freeze(&restored.freeze)?;
                }
                if let Some(now) = processing_time { window.validate_processing_cut(now)?; }
                windows.insert(*operator, window);
            }
            if let sparrow_plan::PhysicalStage::Iot { operator, spec, input, .. } = stage {
                let mut op = crate::iot::IotOperator::new(*operator, spec.clone(), input.clone(), owner.clone())
                    .map_err(|error| error.at_operator(*operator))?;
                op.bind_generation(pipeline.generation)?;
                if let Some((freeze, lease)) = restored_iot.remove(operator) {
                    op.restore(&freeze).map_err(|error| error.at_operator(*operator))?;
                    drop(freeze);
                    drop(lease);
                }
                if let Some(now) = processing_time { op.validate_processing_cut(now)?; }
                iot.insert(*operator, op);
            }
        }
        if !restored.is_empty() || !restored_iot.is_empty() || !restored_buffered.is_empty() {
            return Err(SparrowError::new(ErrorCode::Internal, "unconsumed restored participant"));
        }
        job.acks
            .configure(pipeline.plan, attempt, pipeline.generation)?;
        Ok(Arc::new(Self {
            participant_mode: true,
            restore: Mutex::new(None),
            windows: Mutex::new(windows),
            buffered: Mutex::new(buffered),
            iot: Mutex::new(iot),
            acks: job.acks,
            outbox: job.outbox,
            _sink_binding: pipeline.sink,
        }))
    }
}

/// Use the Store-reserved lease for a participant when present; otherwise
/// (embedders/tests without an owned Store decode) reserve before adoption.
fn restore_lease(
    credit: &mut Option<crate::checkpoint::RestoreCredit>,
    owner: &Arc<MemoryOwner>,
    participant: ParticipantId,
    resident: usize,
) -> Result<sparrow_model::MemoryLease> {
    if let Some(lease) = credit.as_mut().and_then(|credit| credit.take(participant)) {
        if lease.bytes() < resident || !Arc::ptr_eq(lease.owner(), owner) {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                "restored participant exceeds its reserved restore credit",
            ));
        }
        return Ok(lease);
    }
    owner.acquire(sparrow_model::CreditKind::Reservation, resident)
}

pub struct PipelineRestore {
    pub plan: Arc<CheckpointPlan>,
    pub generation: [u8; 16],
    /// None is fresh; Some(empty) is a restored zero-state plan.
    pub restore: Option<Vec<WindowFreeze>>,
    /// IoT frames are separate codecs; Some(empty) windows plus these frames
    /// represents a restored IoT-only plan, never a fresh reset.
    pub iot: Vec<crate::iot::IotFreeze>,
    /// Codec 4 sliding count frames (v31/v32); restore is Some(empty) then.
    pub buffered: Vec<crate::buffered_window::BufferedFreeze>,
    /// v27/v28 carry a Job-owned saved/live output target. `plan` must be
    /// the saved plan on restore, not a substituted live manifest.
    pub sink: Option<SinkRestoreBinding>,
}

#[derive(Clone, Debug)]
pub struct SinkRestoreBinding {
    pub live: Arc<sparrow_io::OwnedSinkIdentity>,
    pub saved: Option<Arc<sparrow_io::OwnedSinkIdentity>>,
}

impl SinkRestoreBinding {
    pub fn fresh(live: Arc<sparrow_io::OwnedSinkIdentity>) -> Self {
        Self { live, saved: None }
    }

    pub fn restored(
        live: Arc<sparrow_io::OwnedSinkIdentity>,
        saved: Arc<sparrow_io::OwnedSinkIdentity>,
    ) -> Self {
        Self { live, saved: Some(saved) }
    }

    fn check(&self, restored: bool, owner: &Arc<MemoryOwner>) -> Result<()> {
        self.live.identity().validate()?;
        if !self.live.belongs_to(owner)
            || restored != self.saved.is_some()
            || self.saved.as_ref().is_some_and(|saved| {
                !saved.belongs_to(owner) || saved.identity() != self.live.identity()
            })
        {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                "JetStream sink restore requires matching saved/live identities on the admitted Job owner"));
        }
        Ok(())
    }
}

impl PipelineRestore {
    /// Independently used by Kernel admission and the pre-spawn participant
    /// restore handoff. Existing profiles retain their old prefix contract.
    pub fn check_compatible(&self, live: &CheckpointPlan, owner: &Arc<MemoryOwner>) -> Result<()> {
        if let Some(binding) = &self.sink {
            binding.check(self.restore.is_some(), owner)?;
            if !self.iot.is_empty() {
                return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                    "File/JetStream sink checkpoint excludes IoT state"));
            }
            crate::pipeline_checkpoint::sink_snapshot_version_for(&self.plan, "file", binding.live.identity())?;
            crate::pipeline_checkpoint::sink_snapshot_version_for(live, "file", binding.live.identity())?;
            crate::pipeline_checkpoint::check_sink_plan_compatible(&self.plan, live)
        } else {
            self.plan.check_compatible(live)
        }
    }
}

#[derive(Debug)]
pub enum ParticipantOutcome {
    SourceCut,
    State(EncodedFreeze),
    Sink(FlushOutcome),
    ReliableSink { flush: FlushOutcome, next_output: sparrow_model::OutputSequence },
    Failed(SparrowError),
}

struct ParticipantAttempt {
    plan: Arc<CheckpointPlan>,
    attempt: u64,
    generation: [u8; 16],
}

#[derive(Debug)]
pub struct ParticipantAcks {
    pub(crate) attempt: u64,
    pub(crate) generation: [u8; 16],
    pub(crate) freezes: Vec<EncodedFreeze>,
    pub(crate) next_output: Option<sparrow_model::OutputSequence>,
}

impl ParticipantAcks {
    pub fn next_output(&self) -> Option<sparrow_model::OutputSequence> { self.next_output }
    pub fn attempt(&self) -> u64 {
        self.attempt
    }
    pub fn state_count(&self) -> usize {
        self.freezes.len()
    }
}

#[cfg(test)]
mod production_restore_tests {
    use super::*;
    #[test]
    fn production_restore_context_shares_one_allocation_and_releases_after_consumption() {
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let freeze = WindowFreeze {
            operator: 1.into(),
            slot: 1.into(),
            kind: 1,
            entries: vec![],
            wm_in: None,
            wm_out: None,
            last_effective: None,
        };
        let expected = freeze.resident_bytes();
        let shared = RuntimeAligned::adopt(
            AlignedJob {
                restore: Some(freeze),
                pipeline: None,
                acks: AlignedAcks::default(),
                outbox: Arc::new(InflightCounter::new()),
            },
            &owner,
        )
        .unwrap();
        let stages: Vec<_> = (0..64).map(|_| shared.clone()).collect();
        assert_eq!(owner.usage().reservation_bytes, expected);
        let restored = stages[0].restore.lock().unwrap().take().unwrap();
        assert!(stages[1].restore.lock().unwrap().take().is_none());
        drop(restored);
        assert_eq!(
            owner.usage().physical_bytes,
            0,
            "empty stage contexts must not retain decoded state"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushOutcome {
    pub ok: bool,
    pub dropped: u64,
    pub timed_out: bool,
}

/// Only the active checkpoint owns an ACK inbox. Dropping its request on
/// timeout/cancellation closes that inbox, including senders already in flight.
/// Late snapshots are dropped immediately, even if no next checkpoint occurs.
#[derive(Clone, Default)]
pub struct AlignedAcks {
    active: Arc<Mutex<Option<ActiveCheckpoint>>>,
    participants: Arc<OnceLock<ParticipantAttempt>>,
    output: Arc<OnceLock<sparrow_model::OutputSequence>>,
    graph: Arc<OnceLock<Arc<crate::graph_cut::GraphRuntime>>>,
}

struct ActiveCheckpoint {
    id: u64,
    sender: tokio::sync::mpsc::Sender<AlignedAck>,
    deadline: Instant,
    cancelled: CancellationToken,
}

pub struct CheckpointAcks {
    registry: AlignedAcks,
    id: u64,
    receiver: tokio::sync::mpsc::Receiver<AlignedAck>,
}

impl AlignedAcks {
    pub fn with_graph_time(self,graph:Arc<crate::graph_cut::GraphRuntime>)->Result<Self> {
        if self.participants.get().is_some() || self.output.get().is_some() || self.graph.set(graph).is_err() {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,"graph time must be initialized before input"));
        }
        Ok(self)
    }
    pub(crate) fn graph_time(&self)->Option<&Arc<crate::graph_cut::GraphRuntime>> {self.graph.get()}
    /// Configure before Kernel admission. The CaptureSink owns advancement;
    /// checkpointing receives the cursor in its actual Sink barrier ACK.
    pub fn with_output_sequence(self, sequence: sparrow_model::OutputSequence) -> Result<Self> {
        if self.participants.get().is_some() || self.graph.get().is_some() || self.output.set(sequence).is_err() {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,"output cursor must be initialized exactly once before input"));
        }
        Ok(self)
    }
    pub(crate) fn output_sequence(&self) -> Option<sparrow_model::OutputSequence> { self.output.get().copied() }
    pub(crate) fn configure(
        &self,
        plan: Arc<CheckpointPlan>,
        attempt: u64,
        generation: [u8; 16],
    ) -> Result<()> {
        plan.validate()?;
        if generation == [0; 16] {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "state generation must be initialized before input",
            ));
        }
        if self.active.lock().expect("checkpoint registry").is_some()
            || self
                .participants
                .set(ParticipantAttempt {
                    plan,
                    attempt,
                    generation,
                })
                .is_err()
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint registry cannot be reused across attempts",
            ));
        }
        Ok(())
    }

    pub(crate) async fn source_cut(&self, checkpoint_id: u64) {
        let source = self.participants.get().map(|p|p.plan.source);
        if let Some(source) = source { self.source_cut_for(checkpoint_id, source).await; }
    }
    pub(crate) async fn source_cut_for(&self, checkpoint_id: u64, source: sparrow_model::OperatorId) {
        if let Some(p) = self.participants.get() {
            self.send(AlignedAck::Participant {
                attempt: p.attempt,
                checkpoint_id,
                participant: ParticipantId::Source(source),
                outcome: ParticipantOutcome::SourceCut,
            })
            .await;
        }
    }

    pub(crate) async fn state_frozen(
        &self,
        checkpoint_id: u64,
        operator: sparrow_model::OperatorId,
        freeze: Result<EncodedFreeze>,
    ) {
        let ack = if let Some(p) = self.participants.get() {
            AlignedAck::Participant {
                attempt: p.attempt,
                checkpoint_id,
                participant: ParticipantId::window(operator),
                outcome: match freeze {
                    Ok(f) => ParticipantOutcome::State(f),
                    Err(e) => ParticipantOutcome::Failed(e),
                },
            }
        } else {
            match freeze {
                Ok(freeze) => AlignedAck::WindowFrozen {
                    checkpoint_id,
                    freeze,
                },
                Err(error) => AlignedAck::FreezeFailed {
                    checkpoint_id,
                    error,
                },
            }
        };
        self.send(ack).await;
    }

    pub(crate) async fn iot_frozen(&self, checkpoint_id: u64, operator: sparrow_model::OperatorId, freeze: Result<EncodedFreeze>) {
        let Some(p) = self.participants.get() else { return; };
        self.send(AlignedAck::Participant {
            attempt: p.attempt,
            checkpoint_id,
            participant: ParticipantId::iot(operator),
            outcome: match freeze { Ok(freeze) => ParticipantOutcome::State(freeze), Err(error) => ParticipantOutcome::Failed(error) },
        }).await;
    }

    #[cfg(test)]
    pub(crate) async fn sink_flushed(&self, checkpoint_id: u64, flush: FlushOutcome) {
        self.sink_flushed_with_output(checkpoint_id,flush,None).await;
    }
    #[cfg(test)]
    pub(crate) async fn sink_flushed_with_output(&self, checkpoint_id: u64, flush: FlushOutcome, output: Option<sparrow_model::OutputSequence>) {
        let sink = self.participants.get().map(|p|p.plan.sink);
        self.sink_flushed_for(checkpoint_id, flush, output, sink).await;
    }
    pub(crate) async fn sink_flushed_for(&self, checkpoint_id: u64, flush: FlushOutcome, output: Option<sparrow_model::OutputSequence>, sink: Option<sparrow_model::OperatorId>) {
        let ack = if let Some(p) = self.participants.get() {
            AlignedAck::Participant {
                attempt: p.attempt,
                checkpoint_id,
                participant: ParticipantId::Sink(sink.unwrap_or(p.plan.sink)),
                outcome: match output {
                    Some(next_output) => ParticipantOutcome::ReliableSink{flush,next_output},
                    None => ParticipantOutcome::Sink(flush),
                },
            }
        } else {
            AlignedAck::SinkFlushed {
                checkpoint_id,
                ok: flush.ok,
                dropped: flush.dropped,
            }
        };
        self.send(ack).await;
    }
    pub fn begin(&self, id: u64) -> Result<CheckpointAcks> {
        self.begin_with_deadline(id, Instant::now() + Duration::from_secs(5))
    }

    pub fn begin_with_deadline(&self, id: u64, deadline: Instant) -> Result<CheckpointAcks> {
        let mut active = self.active.lock().expect("checkpoint ACK registry");
        if active.is_some() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint already active",
            ));
        }
        let capacity = self
            .participants
            .get()
            .map_or(2, |p| p.plan.participants().len());
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        *active = Some(ActiveCheckpoint {
            id,
            sender,
            deadline,
            cancelled: CancellationToken::new(),
        });
        Ok(CheckpointAcks {
            registry: self.clone(),
            id,
            receiver,
        })
    }

    pub fn is_active(&self, id: u64) -> bool {
        self.active
            .lock()
            .expect("checkpoint ACK registry")
            .as_ref()
            .is_some_and(|active| active.id == id)
    }

    pub(crate) fn abandonment(&self, id: u64) -> Option<CancellationToken> {
        self.active.lock().expect("checkpoint ACK registry").as_ref()
            .filter(|request| request.id == id).map(|request| request.cancelled.clone())
    }

    /// Use the attempt's end-to-end deadline, not an independent sink timeout.
    /// An abandoned barrier must not keep the sink blocked behind old work.
    pub(crate) async fn wait_for_flush(
        &self,
        id: u64,
        outbox: &InflightCounter,
    ) -> Option<FlushOutcome> {
        let (deadline, cancelled) = {
            let active = self.active.lock().expect("checkpoint ACK registry");
            let request = active.as_ref().filter(|request| request.id == id)?;
            (request.deadline, request.cancelled.clone())
        };
        tokio::select! {
            biased;
            _ = cancelled.cancelled() => None,
            outcome = wait_outbox(outbox, deadline.saturating_duration_since(Instant::now())) => Some(outcome),
        }
    }

    pub async fn send(&self, ack: AlignedAck) {
        let sender = self
            .active
            .lock()
            .expect("checkpoint ACK registry")
            .as_ref()
            .filter(|request| request.id == ack.checkpoint_id())
            .map(|request| request.sender.clone());
        if let Some(sender) = sender {
            let _ = sender.send(ack).await;
        }
    }
}

impl CheckpointAcks {
    pub async fn wait_participants(mut self, timeout: Duration) -> Result<ParticipantAcks> {
        let config = self.registry.participants.get().ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "participant registry not prepared",
            )
        })?;
        let plan = config.plan.clone();
        let attempt = config.attempt;
        let generation = config.generation;
        let expected = plan.participants();
        let mut seen = std::collections::BTreeSet::new();
        let mut freezes = BTreeMap::new();
        let mut bytes = 0usize;
        let initial_output=self.registry.output_sequence();
        let mut next_output=None;
        let deadline = Instant::now() + timeout;
        while seen.len() < expected.len() {
            let ack = tokio::time::timeout_at(deadline, self.receiver.recv())
                .await
                .map_err(|_| {
                    SparrowError::new(
                        ErrorCode::ResourceExhausted,
                        "participant checkpoint alignment timed out",
                    ).context("checkpoint_outcome", "timeout")
                })?
                .ok_or_else(|| {
                    SparrowError::new(ErrorCode::Cancelled, "participant ACK channel closed")
                })?;
            let AlignedAck::Participant {
                attempt: got_attempt,
                checkpoint_id,
                participant,
                outcome,
            } = ack
            else {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "legacy ACK cannot satisfy participant checkpoint",
                ));
            };
            if got_attempt != attempt || checkpoint_id != self.id {
                continue;
            }
            if !expected.contains(&participant) {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "unknown checkpoint participant ACK",
                ));
            }
            match (participant, outcome) {
                (_, ParticipantOutcome::Failed(e)) => return Err(e),
                (ParticipantId::Source(_), ParticipantOutcome::SourceCut) => {
                    seen.insert(participant);
                }
                (
                    ParticipantId::Sink(_),
                    ParticipantOutcome::Sink(FlushOutcome {
                        ok: true,
                        dropped: 0,
                        timed_out: false,
                    }),
                ) if initial_output.is_none() && self.registry.graph_time().is_none() => {
                    seen.insert(participant);
                }
                (ParticipantId::Sink(sink),ParticipantOutcome::ReliableSink{flush:FlushOutcome{ok:true,dropped:0,timed_out:false},next_output:position}) => {
                    if let Some(graph)=self.registry.graph_time() {
                        graph.record_sink(sink.raw(),self.id,position)?;seen.insert(participant);continue;
                    }
                    if !initial_output.is_some_and(|start|start.epoch()==position.epoch() && start.first()<=position.first())
                        || next_output.is_some_and(|previous|previous!=position) {
                        return Err(SparrowError::new(ErrorCode::CodecViolation,"invalid/conflicting reliable Sink output cursor"));
                    }
                    next_output=Some(position);
                    seen.insert(participant);
                }
                (ParticipantId::State { .. }, ParticipantOutcome::State(freeze)) => {
                    let (id, kind, _) = freeze.identity()?;
                    let state = plan
                        .states
                        .iter()
                        .find(|s| s.id == participant)
                        .expect("expected state");
                    if id != participant || kind != state.freeze_kind() {
                        return Err(SparrowError::new(
                            ErrorCode::CodecViolation,
                            "participant freeze identity/kind mismatch",
                        ));
                    }
                    if let Some(previous) = freezes.get(&participant) {
                        let previous: &EncodedFreeze = previous;
                        if previous.bytes() != freeze.bytes() {
                            return Err(SparrowError::new(
                                ErrorCode::CodecViolation,
                                "conflicting repeated participant freeze",
                            ));
                        }
                        continue;
                    }
                    seen.insert(participant);
                    bytes = bytes.saturating_add(freeze.bytes().len());
                    if bytes as u64 > crate::checkpoint::MAX_SNAPSHOT_BYTES {
                        return Err(SparrowError::new(
                            ErrorCode::BoundExceeded,
                            "total checkpoint state exceeds byte limit",
                        ));
                    }
                    freezes.insert(participant, freeze);
                }
                (ParticipantId::Sink(_), ParticipantOutcome::Sink(FlushOutcome{timed_out:true,dropped:0,..})) |
                (ParticipantId::Sink(_), ParticipantOutcome::ReliableSink{flush:FlushOutcome{timed_out:true,dropped:0,..},..}) => {
                    return Err(SparrowError::new(ErrorCode::ResourceExhausted,"required sink flush timed out").context("checkpoint_outcome","timeout"));
                }
                (ParticipantId::Sink(_), ParticipantOutcome::Sink(_)) |
                (ParticipantId::Sink(_), ParticipantOutcome::ReliableSink{..}) => {
                    return Err(SparrowError::new(ErrorCode::ResourceExhausted,"barrier did not align: required sink flush failed or dropped output; refusing commit"));
                }
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "wrong checkpoint participant ACK role",
                    ))
                }
            }
        }
        // Encode in plan order, not ACK arrival or numeric operator order.
        Ok(ParticipantAcks {
            attempt,
            generation,
            next_output,
            freezes: plan
                .states
                .iter()
                .map(|s| freezes.remove(&s.id).expect("complete state ACK set"))
                .collect(),
        })
    }
    pub async fn recv(&mut self) -> Option<AlignedAck> {
        self.receiver.recv().await
    }

    pub async fn wait(mut self, timeout: Duration) -> Result<BarrierAcks> {
        wait_aligned_acks(&mut self.receiver, self.id, timeout).await
    }
}

impl Drop for CheckpointAcks {
    fn drop(&mut self) {
        let mut active = self
            .registry
            .active
            .lock()
            .expect("checkpoint ACK registry");
        if active.as_ref().is_some_and(|request| request.id == self.id) {
            if let Some(request) = active.take() {
                request.cancelled.cancel();
            }
        }
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
    }
}

/// Acks accepted for one expected barrier id.
#[derive(Debug, Default)]
pub struct BarrierAcks {
    pub freeze: Option<EncodedFreeze>,
    pub freeze_id: Option<u64>,
    pub flushed: bool,
    pub flush_ok: bool,
    pub flush_id: Option<u64>,
    pub dropped: u64,
}

impl BarrierAcks {
    pub fn apply(&mut self, expected: u64, ack: AlignedAck) {
        match ack {
            AlignedAck::WindowFrozen {
                checkpoint_id,
                freeze,
            } if checkpoint_id == expected => {
                self.freeze = Some(freeze);
                self.freeze_id = Some(checkpoint_id);
            }
            AlignedAck::SinkFlushed {
                checkpoint_id,
                ok,
                dropped,
            } if checkpoint_id == expected => {
                self.flushed = true;
                self.flush_ok = ok;
                self.flush_id = Some(checkpoint_id);
                self.dropped = dropped;
            }
            // Stale or future id: drop. Never stitch a lower id onto this cut.
            _ => {}
        }
    }

    pub fn aligned_ok(&self, expected: u64) -> bool {
        self.freeze.is_some()
            && self.flushed
            && self.flush_ok
            && self.dropped == 0
            && self.freeze_id == Some(expected)
            && self.flush_id == Some(expected)
    }
}

pub async fn wait_outbox(outbox: &InflightCounter, timeout: Duration) -> FlushOutcome {
    outbox.request_flush();
    let deadline = Instant::now() + timeout;
    while outbox.pending() > 0 {
        if Instant::now() >= deadline {
            let dropped = outbox.drops_since_mark();
            return FlushOutcome { ok: false, dropped, timed_out: true };
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let dropped = outbox.drops_since_mark();
    FlushOutcome {
        ok: dropped == 0,
        dropped,
        timed_out: false,
    }
}

/// Wait until freeze+successful flush for `expected` arrive. Ignores other ids.
/// Failed freeze/flush returns immediately. Production callers use the
/// request-scoped `CheckpointAcks::wait` to release queued and late ACKs.
pub async fn wait_aligned_acks(
    ack_rx: &mut tokio::sync::mpsc::Receiver<AlignedAck>,
    expected: u64,
    timeout: Duration,
) -> Result<BarrierAcks> {
    let mut got = BarrierAcks::default();
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline && !got.aligned_ok(expected) {
        match tokio::time::timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            ack_rx.recv(),
        )
        .await
        {
            Ok(Some(ack)) => {
                if let AlignedAck::FreezeFailed {
                    checkpoint_id,
                    error,
                } = ack
                {
                    if checkpoint_id == expected {
                        return Err(error);
                    }
                    continue;
                }
                got.apply(expected, ack);
                if got.flushed && (!got.flush_ok || got.dropped > 0) {
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
    if got.aligned_ok(expected) {
        return Ok(got);
    }
    Err(SparrowError::new(
        ErrorCode::ResourceExhausted,
        format!(
            "barrier did not align (freeze={} flush={} ok={} dropped={}); refusing dishonest commit",
            got.freeze.is_some(),
            got.flushed,
            got.flush_ok,
            got.dropped
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{OperatorId, StateSlotId};

    fn k1_zero_registry() -> AlignedAcks {
        let registry = AlignedAcks::default();
        registry
            .configure(
                Arc::new(CheckpointPlan {
                    source: 1.into(),
                    sink: 20.into(),
                    states: vec![],
                    reference_tables: vec![],
                    semantics: b"CP01test".to_vec(),
                    recovery_prefix_len: Some(8),
                }),
                7,
                [7; 16],
            )
            .unwrap();
        registry
    }

    fn k1_source(attempt: u64, participant: ParticipantId) -> AlignedAck {
        AlignedAck::Participant {
            attempt,
            checkpoint_id: 1,
            participant,
            outcome: ParticipantOutcome::SourceCut,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn k1_zero_state_still_requires_source_and_sink_and_ignores_old_attempt() {
        let registry = k1_zero_registry();
        let request = registry.begin(1).unwrap();
        registry
            .send(k1_source(6, ParticipantId::Source(1.into())))
            .await;
        registry
            .sink_flushed(
                1,
                FlushOutcome {
                    ok: true,
                    dropped: 0,
                    timed_out: false,
                },
            )
            .await;
        assert!(request
            .wait_participants(Duration::from_millis(50))
            .await
            .is_err());
        assert!(!registry.is_active(1));
        let request = registry.begin(2).unwrap();
        registry.source_cut(2).await;
        registry
            .sink_flushed(
                2,
                FlushOutcome {
                    ok: true,
                    dropped: 0,
                    timed_out: false,
                },
            )
            .await;
        let result = request
            .wait_participants(Duration::from_millis(50))
            .await
            .unwrap();
        assert!(result.freezes.is_empty());
        assert_eq!(result.attempt, 7);
        let request = registry.begin(3).unwrap();
        let sender = registry.clone();
        let sending = tokio::spawn(async move {
            sender.source_cut(3).await;
            sender.source_cut(3).await;
            sender
                .sink_flushed(
                    3,
                    FlushOutcome {
                        ok: true,
                        dropped: 0,
                        timed_out: false,
                    },
                )
                .await;
        });
        assert_eq!(
            request
                .wait_participants(Duration::from_millis(50))
                .await
                .unwrap()
                .state_count(),
            0
        );
        sending.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn k1_unknown_role_and_failed_sink_fail_without_waiting_for_timeout() {
        for case in 1..4 {
            let registry = k1_zero_registry();
            let request = registry.begin(1).unwrap();
            registry.source_cut(1).await;
            match case {
                1 => {
                    registry
                        .send(k1_source(7, ParticipantId::Source(99.into())))
                        .await
                }
                2 => {
                    registry
                        .send(k1_source(7, ParticipantId::Sink(20.into())))
                        .await
                }
                _ => {
                    registry
                        .sink_flushed(
                            1,
                            FlushOutcome {
                                ok: false,
                                dropped: 1,
                                timed_out: false,
                            },
                        )
                        .await
                }
            }
            let error = request
                .wait_participants(Duration::from_millis(50))
                .await
                .unwrap_err();
            let expected = match case { 1 => "unknown checkpoint participant", 2 => "wrong checkpoint participant", _ => "required sink flush failed" };
            assert!(error.message.contains(expected), "{error}");
            assert!(!registry.is_active(1));
            let plan = registry.participants.get().unwrap().plan.clone();
            assert!(
                registry.configure(plan, 8, [8; 16]).is_err(),
                "registry cannot be shared by a new attempt"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn self_review_flush_uses_attempt_deadline_and_releases_abandoned_barrier() {
        let registry = AlignedAcks::default();
        let outbox = Arc::new(InflightCounter::new());
        outbox.enqueue();
        let request = registry
            .begin_with_deadline(1, Instant::now() + Duration::from_secs(8))
            .unwrap();
        let delayed = outbox.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            delayed.ack();
        });
        let start = Instant::now();
        assert!(registry.wait_for_flush(1, &outbox).await.unwrap().ok);
        assert!(start.elapsed() >= Duration::from_secs(6));
        drop(request);

        outbox.enqueue();
        let request = registry
            .begin_with_deadline(2, Instant::now() + Duration::from_secs(120))
            .unwrap();
        let waiting_registry = registry.clone();
        let waiting_outbox = outbox.clone();
        let waiting =
            tokio::spawn(async move { waiting_registry.wait_for_flush(2, &waiting_outbox).await });
        tokio::task::yield_now().await;
        drop(request);
        assert!(tokio::time::timeout(Duration::from_millis(10), waiting)
            .await
            .unwrap()
            .unwrap()
            .is_none());
        assert_eq!(
            outbox.pending(),
            1,
            "abandoning a barrier does not forge delivery ACKs"
        );

        let request = registry
            .begin_with_deadline(3, Instant::now() + Duration::from_millis(100))
            .unwrap();
        let start = Instant::now();
        assert!(!registry.wait_for_flush(3, &outbox).await.unwrap().ok);
        assert!(start.elapsed() >= Duration::from_millis(100));
        assert!(start.elapsed() < Duration::from_millis(110));
        drop(request);
        outbox.ack();
        assert!(registry.wait_for_flush(3, &outbox).await.is_none());
    }

    fn leased_ack(owner: &Arc<sparrow_model::MemoryOwner>, id: u64) -> AlignedAck {
        AlignedAck::WindowFrozen {
            checkpoint_id: id,
            freeze: EncodedFreeze {
                bytes: vec![0; 128],
                lease: owner
                    .acquire(sparrow_model::CreditKind::Reservation, 128)
                    .unwrap(),
                ext: false,
                buffered: false,
            },
        }
    }

    #[tokio::test]
    async fn r4_timeout_and_late_ack_release_without_another_checkpoint() {
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let registry = AlignedAcks::default();
        let request = registry.begin(1).unwrap();
        // The producer already owns a snapshot, but sends only after timeout.
        let late = leased_ack(&owner, 1);
        assert!(request.wait(Duration::from_millis(1)).await.is_err());
        assert!(!registry.is_active(1));
        registry.send(late).await;
        assert_eq!(owner.usage().physical_bytes, 0);

        // Frozen state already consumed by wait must also be dropped on timeout.
        let request = registry.begin(2).unwrap();
        registry.send(leased_ack(&owner, 2)).await;
        assert!(request.wait(Duration::from_millis(1)).await.is_err());
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn r4_failed_or_cancelled_request_drops_queued_and_inflight_snapshots() {
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let registry = AlignedAcks::default();
        for id in 1..=2 {
            let request = registry.begin(id).unwrap();
            let failure = if id == 1 {
                AlignedAck::FreezeFailed {
                    checkpoint_id: id,
                    error: SparrowError::new(ErrorCode::BoundExceeded, "freeze refused"),
                }
            } else {
                AlignedAck::SinkFlushed {
                    checkpoint_id: id,
                    ok: false,
                    dropped: 1,
                }
            };
            registry.send(failure).await;
            registry.send(leased_ack(&owner, id)).await;
            assert!(request.wait(Duration::from_secs(1)).await.is_err());
            assert_eq!(owner.usage().physical_bytes, 0);
        }
        let request = registry.begin(3).unwrap();
        registry.send(leased_ack(&owner, 3)).await;
        registry.send(leased_ack(&owner, 3)).await;
        let tx = registry.clone();
        let extra = leased_ack(&owner, 3);
        let sending = tokio::spawn(async move { tx.send(extra).await });
        tokio::task::yield_now().await;
        assert!(
            !sending.is_finished(),
            "third ACK should wait on the bounded inbox"
        );
        drop(request);
        sending.await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);

        let request = registry.begin(4).unwrap();
        registry.send(leased_ack(&owner, 4)).await;
        let waiting = tokio::spawn(request.wait(Duration::from_secs(30)));
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert!(!registry.is_active(4));
        assert_eq!(owner.usage().physical_bytes, 0);

        let request = registry.begin(5).unwrap();
        registry.send(leased_ack(&owner, 4)).await; // cannot pair with new flush
        registry.send(leased_ack(&owner, 5)).await;
        registry
            .send(AlignedAck::SinkFlushed {
                checkpoint_id: 5,
                ok: true,
                dropped: 0,
            })
            .await;
        let accepted = request.wait(Duration::from_secs(1)).await.unwrap();
        assert_eq!(owner.usage().reservation_bytes, 128);
        drop(accepted);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn r3_pending_timeout_can_recover_without_forgiving_loss() {
        let outbox = InflightCounter::new();
        outbox.enqueue();
        let timed=wait_outbox(&outbox, Duration::from_millis(1)).await;
        assert!(timed.timed_out && !timed.ok);
        assert_eq!(timed.dropped,0,"timeout is not evidence of dropped output");
        outbox.ack();
        assert!(wait_outbox(&outbox, Duration::from_millis(1)).await.ok);
        outbox.enqueue();
        outbox.fail();
        for _ in 0..3 {
            assert!(!wait_outbox(&outbox, Duration::from_millis(1)).await.ok);
        }
    }

    fn encoded(freeze: WindowFreeze) -> EncodedFreeze {
        let mut bytes = Vec::new();
        crate::checkpoint::encode_freeze(&freeze, &mut bytes, 1024).unwrap();
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let lease = owner
            .acquire(
                sparrow_model::CreditKind::Reservation,
                bytes.capacity().max(1),
            )
            .unwrap();
        EncodedFreeze { bytes, lease, ext: false, buffered: false }
    }

    fn empty_freeze() -> WindowFreeze {
        WindowFreeze {
            operator: OperatorId::WINDOW,
            slot: StateSlotId::new(1),
            kind: 1,
            entries: Vec::new(),
            wm_in: None,
            wm_out: None,
            last_effective: None,
        }
    }

    fn leftover_freeze(count: u64) -> WindowFreeze {
        let mut f = empty_freeze();
        f.entries.push(crate::window::FrozenEntry {
            key: vec![sparrow_model::Scalar::utf8("d1")],
            window_start: 0,
            window_end: 0,
            count,
            accs: Vec::new(),
        });
        f
    }

    #[test]
    fn stale_ack_id_is_ignored() {
        let mut got = BarrierAcks::default();
        got.apply(
            2,
            AlignedAck::WindowFrozen {
                checkpoint_id: 1,
                freeze: encoded(empty_freeze()),
            },
        );
        got.apply(
            2,
            AlignedAck::SinkFlushed {
                checkpoint_id: 1,
                ok: true,
                dropped: 0,
            },
        );
        assert!(
            !got.aligned_ok(2),
            "barrier #1 freeze+flush must not satisfy expected id 2"
        );
        assert!(got.freeze.is_none());
        got.apply(
            2,
            AlignedAck::WindowFrozen {
                checkpoint_id: 2,
                freeze: encoded(leftover_freeze(1)),
            },
        );
        got.apply(
            2,
            AlignedAck::SinkFlushed {
                checkpoint_id: 2,
                ok: true,
                dropped: 0,
            },
        );
        assert!(got.aligned_ok(2));
        assert!(!got.freeze.as_ref().unwrap().bytes.is_empty());
    }

    #[test]
    fn failed_flush_is_not_aligned() {
        let mut got = BarrierAcks::default();
        got.apply(
            1,
            AlignedAck::WindowFrozen {
                checkpoint_id: 1,
                freeze: encoded(empty_freeze()),
            },
        );
        got.apply(
            1,
            AlignedAck::SinkFlushed {
                checkpoint_id: 1,
                ok: false,
                dropped: 2,
            },
        );
        assert!(!got.aligned_ok(1));
        assert!(got.flushed);
        assert!(!got.flush_ok);
    }

    #[tokio::test]
    async fn wait_aligned_acks_rejects_stale_pair() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tx.send(AlignedAck::WindowFrozen {
            checkpoint_id: 1,
            freeze: encoded(empty_freeze()),
        })
        .await
        .unwrap();
        tx.send(AlignedAck::SinkFlushed {
            checkpoint_id: 1,
            ok: true,
            dropped: 0,
        })
        .await
        .unwrap();
        let err = wait_aligned_acks(&mut rx, 2, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
    }
}

//! K1 snapshot envelope: one File cut, complete participant manifest, bounded
//! state frames and attempt/revision provenance. Legacy SPV1 v1/v2 stay separate.
use crate::barrier::ParticipantAcks;
use crate::checkpoint::{EncodedSnapshot, MAX_SNAPSHOT_BYTES};
use crate::window::WindowFreeze;
use sparrow_io::SourcePosition;
use sparrow_model::{CreditKind, ErrorCode, MemoryOwner, Result, SparrowError};
use sparrow_plan::{CheckpointPlan, ParticipantId};
use std::sync::Arc;

pub const PIPELINE_SNAPSHOT_VERSION: u16 = 3;
pub const RELIABLE_SNAPSHOT_VERSION: u16 = 4;
const MAX_SOURCE_METADATA: usize = 64 * 1024;

#[derive(Debug, PartialEq)]
pub struct PipelineSnapshot {
    pub checkpoint_id: u64,
    pub source: SourcePosition,
    pub ingested_rows: u64,
    pub attempt: u64,
    pub revision: u64,
    pub generation: [u8; 16],
    pub plan: CheckpointPlan,
    pub windows: Vec<WindowFreeze>,
    pub next_output: Option<sparrow_model::OutputSequence>,
}

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if bytes.len() < n {
        return Err(invalid("truncated pipeline checkpoint"));
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}
fn u64_value(bytes: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(bytes, 8)?.try_into().unwrap()))
}
fn u32_value(bytes: &mut &[u8]) -> Result<usize> {
    Ok(u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()) as usize)
}
fn string(bytes: &mut &[u8]) -> Result<String> {
    let n = u32_value(bytes)?;
    if n > MAX_SOURCE_METADATA {
        return Err(invalid("source metadata exceeds limit"));
    }
    Ok(std::str::from_utf8(take(bytes, n)?)
        .map_err(|_| invalid("source metadata UTF-8"))?
        .to_owned())
}

impl PipelineSnapshot {
    /// Header-only provenance for diagnostics, never authorization to restore.
    pub(crate) fn provenance(mut bytes: &[u8]) -> Result<(u64, u64, [u8; 16])> {
        take(&mut bytes, 38)?;
        for _ in 0..2 {
            let n = u32_value(&mut bytes)?;
            if n > MAX_SOURCE_METADATA {
                return Err(invalid("source metadata exceeds limit"));
            }
            take(&mut bytes, n)?;
        }
        take(&mut bytes, 16)?;
        let attempt = u64_value(&mut bytes)?;
        let revision = u64_value(&mut bytes)?;
        let generation = take(&mut bytes, 16)?.try_into().unwrap();
        Ok((attempt, revision, generation))
    }
    pub fn check_compatible(&self, live: &CheckpointPlan) -> Result<()> {
        self.plan.check_compatible(live)
    }

    pub fn encode_frozen(
        checkpoint_id: u64,
        source: &SourcePosition,
        ingested_rows: u64,
        revision: u64,
        plan: &CheckpointPlan,
        acks: ParticipantAcks,
        owner: &Arc<MemoryOwner>,
        max_keys: usize,
    ) -> Result<EncodedSnapshot> {
        plan.validate()?;
        if acks.next_output.is_some() != (source.identity.kind == "jetstream-v1") {
            return Err(invalid("reliable output cursor and source profile disagree"));
        }
        if source.identity.path.len() > MAX_SOURCE_METADATA
            || source.identity.kind.len() > MAX_SOURCE_METADATA
            || acks.freezes.len() != plan.states.len()
            || acks.attempt == 0
            || acks.generation == [0; 16]
        {
            return Err(invalid(
                "pipeline checkpoint source/participant/attempt mismatch",
            ));
        }
        let metadata = plan
            .semantics
            .len()
            .saturating_add(source.identity.kind.len())
            .saturating_add(source.identity.path.len())
            .saturating_add(512)
            .saturating_mul(3);
        let _workspace = owner.acquire(CreditKind::Reservation, metadata)?;
        let manifest = plan.encode()?;
        // Include the state count and first frame length now. Appending either
        // after filling a large manifest must not double a whole metadata Vec.
        let mut prefix = Vec::with_capacity(
            104 + usize::from(acks.next_output.is_some())*24 + source.identity.kind.len() + source.identity.path.len() + manifest.len(),
        );
        prefix.extend_from_slice(crate::checkpoint::MAGIC);
        prefix.extend_from_slice(&if acks.next_output.is_some(){RELIABLE_SNAPSHOT_VERSION}else{PIPELINE_SNAPSHOT_VERSION}.to_le_bytes());
        prefix.extend_from_slice(&checkpoint_id.to_le_bytes());
        prefix.extend_from_slice(&ingested_rows.to_le_bytes());
        crate::checkpoint::encode_position(source, &mut prefix)?;
        prefix.extend_from_slice(&acks.attempt.to_le_bytes());
        prefix.extend_from_slice(&revision.to_le_bytes());
        prefix.extend_from_slice(&acks.generation);
        if let Some(position)=acks.next_output {
            prefix.extend_from_slice(&position.epoch());
            prefix.extend_from_slice(&position.first().to_le_bytes());
        }
        prefix.extend_from_slice(&(manifest.len() as u32).to_le_bytes());
        prefix.extend_from_slice(&manifest);
        prefix.extend_from_slice(&(acks.freezes.len() as u16).to_le_bytes());
        let mut total = prefix.len();
        let mut entries = 0usize;
        for (freeze, participant) in acks.freezes.iter().zip(&plan.states) {
            if !Arc::ptr_eq(freeze.lease.owner(), owner) {
                return Err(invalid(
                    "checkpoint freeze belongs to a different Job memory owner",
                ));
            }
            let (id, kind, n) = freeze.identity()?;
            if id != participant.id || kind != participant.freeze_kind() {
                return Err(invalid("pipeline freeze and manifest identity mismatch"));
            }
            // max_state_keys remains per operator. Participant count and total
            // bytes are separately bounded, rather than halving legal capacity.
            entries = entries.max(n);
            total = total.saturating_add(4).saturating_add(freeze.bytes.len());
        }
        if total as u64 > MAX_SNAPSHOT_BYTES || entries > max_keys {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "total pipeline snapshot bytes or per-participant entry bound exceeded",
            ));
        }
        let mut freezes = acks.freezes.into_iter();
        // Reuse the first encoded state allocation. No second complete decoded
        // snapshot and no per-participant independent Job budget.
        let (bytes, lease) = if let Some(mut first) = freezes.next() {
            let state_len = first.bytes.len();
            prefix.extend_from_slice(&(state_len as u32).to_le_bytes());
            first.lease.grow_to(total.max(first.bytes.capacity()))?;
            first.bytes.reserve_exact(total - state_len);
            first.bytes.resize(state_len + prefix.len(), 0);
            first.bytes.copy_within(..state_len, prefix.len());
            first.bytes[..prefix.len()].copy_from_slice(&prefix);
            for freeze in freezes {
                first
                    .bytes
                    .extend_from_slice(&(freeze.bytes.len() as u32).to_le_bytes());
                first.bytes.extend_from_slice(&freeze.bytes);
            }
            (first.bytes, first.lease)
        } else {
            let lease = owner.acquire(CreditKind::Reservation, prefix.capacity().max(1))?;
            (prefix, lease)
        };
        if bytes.len() != total {
            return Err(invalid("pipeline snapshot length estimate mismatch"));
        }
        Ok(EncodedSnapshot::prepared(
            bytes,
            lease,
            checkpoint_id,
            entries,
        ))
    }

    pub fn decode(bytes: &[u8], max_keys: usize) -> Result<Self> {
        Self::decode_mode(bytes, max_keys, true)
    }

    pub(crate) fn decode_mode(
        mut bytes: &[u8],
        max_keys: usize,
        materialize: bool,
    ) -> Result<Self> {
        if bytes.len() as u64 > MAX_SNAPSHOT_BYTES
            || take(&mut bytes, 4)? != crate::checkpoint::MAGIC
        {
            return Err(invalid("unsupported pipeline snapshot magic/version/size"));
        }
        let version=u16::from_le_bytes(take(&mut bytes,2)?.try_into().unwrap());
        if !matches!(version,PIPELINE_SNAPSHOT_VERSION|RELIABLE_SNAPSHOT_VERSION) {
            return Err(invalid("unsupported pipeline snapshot version"));
        }
        let checkpoint_id = u64_value(&mut bytes)?;
        let ingested_rows = u64_value(&mut bytes)?;
        let offset_bytes = u64_value(&mut bytes)?;
        let record_index = u64_value(&mut bytes)?;
        let kind = string(&mut bytes)?;
        let path = string(&mut bytes)?;
        let size = u64_value(&mut bytes)?;
        let fingerprint = u64_value(&mut bytes)?;
        let source = SourcePosition {
            offset_bytes,
            record_index,
            identity: sparrow_io::SourceIdentity {
                kind,
                path,
                size,
                fingerprint,
            },
        };
        let attempt = u64_value(&mut bytes)?;
        let revision = u64_value(&mut bytes)?;
        let generation: [u8; 16] = take(&mut bytes, 16)?.try_into().unwrap();
        if generation == [0; 16] {
            return Err(invalid("pipeline snapshot lacks state generation"));
        }
        if attempt == 0 {
            return Err(invalid("pipeline snapshot lacks attempt identity"));
        }
        let next_output=if version==RELIABLE_SNAPSHOT_VERSION {
            let epoch=take(&mut bytes,16)?.try_into().unwrap();
            Some(sparrow_model::OutputSequence::new(epoch,u64_value(&mut bytes)?)
                .map_err(|_|invalid("invalid reliable output position"))?)
        } else {None};
        if next_output.is_some() != (source.identity.kind=="jetstream-v1") {
            return Err(invalid("reliable snapshot lacks source/output identity"));
        }
        let length = u32_value(&mut bytes)?;
        let plan = CheckpointPlan::decode(take(&mut bytes, length)?)?;
        let n = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()) as usize;
        if n != plan.states.len() {
            return Err(invalid("missing/extra checkpoint state participant"));
        }
        let mut windows = Vec::with_capacity(if materialize { n } else { 0 });
        let per_participant = crate::checkpoint::freeze_entry_cap(max_keys);
        let mut remaining = per_participant
            .checked_mul(plan.states.len())
            .ok_or_else(|| invalid("total participant work bound overflow"))?;
        for participant in &plan.states {
            let length = u32_value(&mut bytes)?;
            let mut frame = take(&mut bytes, length)?;
            let entries = crate::checkpoint::FreezeHeader::parse(frame)?.entries;
            if entries > per_participant || entries > remaining {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "total decoded participant entry limit",
                ));
            }
            let freeze =
                crate::checkpoint::decode_freeze_mode(&mut frame, per_participant, materialize)?;
            remaining -= entries;
            if !frame.is_empty()
                || participant.id
                    != (ParticipantId::State {
                        operator: freeze.operator,
                        slot: freeze.slot,
                        shard: 0,
                    })
                || freeze.kind != participant.freeze_kind()
            {
                return Err(invalid(
                    "duplicate/unknown/misordered checkpoint state identity or codec",
                ));
            }
            if materialize {
                windows.push(freeze);
            }
        }
        if !bytes.is_empty() {
            return Err(invalid("trailing pipeline snapshot bytes"));
        }
        Ok(Self {
            checkpoint_id,
            source,
            ingested_rows,
            attempt,
            revision,
            generation,
            plan,
            windows,
            next_output,
        })
    }
}

/// Store integrity/GC understands both codecs, without converting one into the
/// other. A format-incompatible CURRENT must not silently fall back to old data.
pub(crate) enum StoredSnapshot {
    Legacy(crate::checkpoint::CheckpointSnapshot),
    Pipeline(PipelineSnapshot),
}
impl StoredSnapshot {
    pub(crate) fn id(&self) -> u64 {
        match self {
            Self::Legacy(s) => s.checkpoint_id,
            Self::Pipeline(s) => s.checkpoint_id,
        }
    }
    pub(crate) fn decode(bytes: &[u8], max_keys: usize, materialize: bool) -> Result<Self> {
        if matches!(bytes.get(4..6),Some([3|4,0])) {
            Ok(Self::Pipeline(PipelineSnapshot::decode_mode(
                bytes,
                max_keys,
                materialize,
            )?))
        } else {
            Ok(Self::Legacy(
                crate::checkpoint::CheckpointSnapshot::decode_mode(bytes, max_keys, materialize)?,
            ))
        }
    }
    pub(crate) fn legacy(self) -> Result<crate::checkpoint::CheckpointSnapshot> {
        match self {
            Self::Legacy(s) => Ok(s),
            Self::Pipeline(_) => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "K1 checkpoint requires participant-aware restore; use the matching binary",
            )),
        }
    }
    pub(crate) fn pipeline(self) -> Result<PipelineSnapshot> {
        match self {Self::Pipeline(s)=>Ok(s),Self::Legacy(_)=>Err(SparrowError::new(ErrorCode::UnsupportedRestore,"legacy single-window snapshot is not migrated to K1; retain original backup/binary or explicitly start fresh with a new store"))}
    }
}

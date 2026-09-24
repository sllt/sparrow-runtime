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
pub const GRAPH_SNAPSHOT_VERSION: u16 = 5;
pub const IOT_SNAPSHOT_VERSION: u16 = 6;
/// Reliable JetStream output plus the bounded TTL=0 IoT state codec.  This is
/// intentionally a new envelope/profile: v4 has no IoT participant and v6
/// has no output cursor, so neither can be extended in place without making a
/// committed checkpoint ambiguous to older binaries.
pub const RELIABLE_IOT_SNAPSHOT_VERSION: u16 = 7;
/// Static immutable reference-table enrichment has a separate File-only
/// profile. It uses the CPL3 participant manifest and intentionally has no
/// output cursor or state frames; v3..v7 remain byte-for-byte compatible.
pub const REFERENCE_SNAPSHOT_VERSION: u16 = 8;
/// Static reference enrichment combined with bounded linear Count/IoT state.
pub const REFERENCE_LINEAR_SNAPSHOT_VERSION: u16 = 9;
/// Static reference enrichment on reliable JetStream input. This profile has
/// the output cursor in addition to the CPL3 reference/state manifest.
pub const REFERENCE_RELIABLE_SNAPSHOT_VERSION: u16 = 10;
/// Static reference enrichment on a required File DAG.
pub const REFERENCE_GRAPH_SNAPSHOT_VERSION: u16 = 11;
/// Hysteresis/other new IoT state without references on File (linear or DAG).
pub const HYSTERESIS_SNAPSHOT_VERSION: u16 = 12;
/// Hysteresis/other new IoT state without references on reliable JetStream.
pub const HYSTERESIS_RELIABLE_SNAPSHOT_VERSION: u16 = 13;
pub const PAUSED_FILE_SNAPSHOT_VERSION: u16 = 14;
pub const PAUSED_RELIABLE_SNAPSHOT_VERSION: u16 = 15;
/// PT windows, positive TTL and up to two ordered linear state participants.
/// v14/v15 remain the original single HoldFor/Debounce profile.
pub const PAUSED_COMBINED_FILE_SNAPSHOT_VERSION: u16 = 16;
pub const PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION: u16 = 17;
pub const TIME_GRAPH_PT_SNAPSHOT_VERSION: u16 = 18;
pub const TIME_GRAPH_ET_SNAPSHOT_VERSION: u16 = 19;
pub const ALARM_FILE_SNAPSHOT_VERSION: u16 = 20;
pub const ALARM_RELIABLE_SNAPSHOT_VERSION: u16 = 21;
pub const ALARM_GRAPH_SNAPSHOT_VERSION: u16 = 22;
const MAX_SOURCE_METADATA: usize = 64 * 1024;

fn output_profile(kind: &str) -> bool {
    matches!(kind,"jetstream-v1"|crate::processing_cut::FILE_KIND|crate::processing_cut::JETSTREAM_KIND)
}

/// Select the outer snapshot profile from the fully validated plan and the
/// connector-declared source identity.  Existing v3..v8 plans retain their
/// previous selection; the new profiles are opt-in through references or the
/// new IoT kind and never silently reinterpret an old directory.
pub fn snapshot_version_for(
    plan: &CheckpointPlan,
    source_kind: &str,
) -> Result<u16> {
    plan.validate()?;
    let graph = plan.is_graph();
    let references = plan.has_references();
    let hysteresis = plan.has_hysteresis();

    if plan.has_alarm() {
        if references || !plan.requires_paused_time() || plan.has_event_time_state() {
            return Err(invalid("alarm profile requires paused time without references/event time"));
        }
        return match (graph, source_kind) {
            (false, crate::processing_cut::FILE_KIND) => Ok(ALARM_FILE_SNAPSHOT_VERSION),
            (false, crate::processing_cut::JETSTREAM_KIND) => Ok(ALARM_RELIABLE_SNAPSHOT_VERSION),
            (true, crate::graph_cut::KIND) if plan.is_time_graph() => Ok(ALARM_GRAPH_SNAPSHOT_VERSION),
            _ => Err(invalid("alarm state requires its own durable time source/profile")),
        };
    }

    if plan.is_time_graph() {
        return if source_kind==crate::graph_cut::KIND {Ok(if plan.has_event_time_state() {TIME_GRAPH_ET_SNAPSHOT_VERSION} else {TIME_GRAPH_PT_SNAPSHOT_VERSION})}
            else {Err(invalid("durable time DAG requires a graph time source cut"))};
    }
    if source_kind==crate::graph_cut::KIND {return Err(invalid("graph time source requires durable graph semantics"));}
    if plan.requires_paused_time() {
        return match source_kind {
            crate::processing_cut::FILE_KIND => Ok(if plan.is_single_timed_iot() { PAUSED_FILE_SNAPSHOT_VERSION } else { PAUSED_COMBINED_FILE_SNAPSHOT_VERSION }),
            crate::processing_cut::JETSTREAM_KIND => Ok(if plan.is_single_timed_iot() { PAUSED_RELIABLE_SNAPSHOT_VERSION } else { PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION }),
            _ => Err(invalid("time state requires a durable paused-time source profile")),
        };
    }
    if matches!(source_kind,crate::processing_cut::FILE_KIND|crate::processing_cut::JETSTREAM_KIND) {
        return Err(invalid("paused-time source requires processing-time semantics"));
    }

    if references {
        // Do not let a raw CheckpointPlan literal claim one of the new
        // reference profiles with an event-time/processing-time participant.
        // `validate` enforces the same invariant for decoded CPL3 manifests;
        // keep this selector guard local so callers cannot bypass the profile
        // matrix by constructing a plan directly.
        if plan
            .states
            .iter()
            .any(|state| state.codec == sparrow_plan::checkpoint::WINDOW_STATE_CODEC && state.window_kind != 1)
        {
            return Err(invalid(
                "reference checkpoint profiles exclude event/processing-time windows",
            ));
        }
        let version = if graph {
            if source_kind != "file-dag-v1" {
                return Err(invalid("reference graph checkpoint requires a File DAG source"));
            }
            REFERENCE_GRAPH_SNAPSHOT_VERSION
        } else if source_kind == "jetstream-v1" {
            REFERENCE_RELIABLE_SNAPSHOT_VERSION
        } else if source_kind == "file" {
            if plan.states.is_empty() {
                REFERENCE_SNAPSHOT_VERSION
            } else {
                REFERENCE_LINEAR_SNAPSHOT_VERSION
            }
        } else {
            return Err(invalid("reference checkpoint requires a File or JetStream source"));
        };
        return Ok(version);
    }

    if hysteresis {
        if graph {
            if source_kind != "file-dag-v1" {
                return Err(invalid("hysteresis graph checkpoint requires a File DAG source"));
            }
            return Ok(HYSTERESIS_SNAPSHOT_VERSION);
        }
        return match source_kind {
            "file" => Ok(HYSTERESIS_SNAPSHOT_VERSION),
            "jetstream-v1" => Ok(HYSTERESIS_RELIABLE_SNAPSHOT_VERSION),
            _ => Err(invalid("hysteresis checkpoint requires a File or JetStream source")),
        };
    }

    // Preserve the pre-reference profile matrix exactly for callers that use
    // the old public open_* methods or construct embedding plans directly.
    if graph {
        return if source_kind == "file-dag-v1" {
            Ok(if plan.has_iot() {
                IOT_SNAPSHOT_VERSION
            } else {
                GRAPH_SNAPSHOT_VERSION
            })
        } else {
            Err(invalid("graph checkpoint requires a File DAG source"))
        };
    }
    if source_kind == "jetstream-v1" {
        return Ok(if plan.has_iot() {
            RELIABLE_IOT_SNAPSHOT_VERSION
        } else {
            RELIABLE_SNAPSHOT_VERSION
        });
    }
    Ok(if plan.has_iot() {
        IOT_SNAPSHOT_VERSION
    } else {
        PIPELINE_SNAPSHOT_VERSION
    })
}

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
    pub iot: Vec<crate::iot::IotFreeze>,
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
        // Keep the established topology rejection ahead of profile selection
        // so old v5/v6 callers retain the same decisive diagnostic.
        if (plan.has_iot() || plan.is_graph())
            && matches!(source.identity.kind.as_str(), "file-dag-v1" | crate::graph_cut::KIND) != plan.is_graph()
        {
            return Err(invalid("checkpoint source topology mismatch"));
        }
        let version = snapshot_version_for(plan, &source.identity.kind)?;
        if matches!(
            version,
            REFERENCE_SNAPSHOT_VERSION
                | REFERENCE_LINEAR_SNAPSHOT_VERSION
                | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                | REFERENCE_GRAPH_SNAPSHOT_VERSION
        ) && !plan.has_references()
        {
            return Err(invalid("reference snapshot profile lacks dependency-bearing plan"));
        }
        if version == REFERENCE_SNAPSHOT_VERSION && !plan.states.is_empty() {
            return Err(invalid("v8 reference checkpoint must remain stateless"));
        }
        if version == REFERENCE_LINEAR_SNAPSHOT_VERSION
            && (plan.is_graph() || plan.states.is_empty() || plan.states.len() > 2)
        {
            return Err(invalid("v9 reference checkpoint requires linear Count/IoT state"));
        }
        if version == REFERENCE_RELIABLE_SNAPSHOT_VERSION
            && (plan.is_graph() || plan.states.len() > 2)
        {
            return Err(invalid("v10 reference checkpoint requires a linear plan"));
        }
        if version == REFERENCE_GRAPH_SNAPSHOT_VERSION && !plan.is_graph() {
            return Err(invalid("v11 reference checkpoint requires a DAG plan"));
        }
        if matches!(version, HYSTERESIS_SNAPSHOT_VERSION | HYSTERESIS_RELIABLE_SNAPSHOT_VERSION)
            && !plan.has_hysteresis()
        {
            return Err(invalid("hysteresis snapshot profile lacks hysteresis state"));
        }
        if (plan.has_iot() || plan.is_graph())
            && matches!(source.identity.kind.as_str(), "file-dag-v1" | crate::graph_cut::KIND) != plan.is_graph() {
            return Err(invalid("checkpoint source topology mismatch"));
        }
        if plan.is_time_graph() {
            if crate::graph_cut::GraphCut::unwrap(source)?.ingested!=ingested_rows {return Err(invalid("graph cut ingested count mismatch"));}
        } else if plan.requires_paused_time() { crate::processing_cut::ProcessingCut::unwrap(source)?; }
        if acks.next_output.is_some() != output_profile(&source.identity.kind) {
            return Err(invalid("reliable output cursor and source profile disagree"));
        }
        if matches!(
            version,
            RELIABLE_IOT_SNAPSHOT_VERSION
                | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                | HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_FILE_SNAPSHOT_VERSION | PAUSED_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_COMBINED_FILE_SNAPSHOT_VERSION | PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION
                | ALARM_FILE_SNAPSHOT_VERSION | ALARM_RELIABLE_SNAPSHOT_VERSION
        )
            && acks
                .next_output
                .is_some_and(|position| position.epoch() != acks.generation)
        {
            return Err(invalid(
                "reliable IoT output epoch differs from the state generation",
            ));
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
        // The plan descriptor owns the CPL3 dependency names/digests. Keep
        // their encoded bytes in the temporary workspace reservation as well
        // as the semantic payload; otherwise the first reference checkpoint
        // can grow an uncharged manifest while it is being assembled.
        let reference_metadata = if plan.has_references() {
            plan.reference_tables.iter().fold(2usize, |bytes, dependency| {
                bytes
                    .saturating_add(2)
                    .saturating_add(dependency.name.len())
                    .saturating_add(8)
                    .saturating_add(32)
                    .saturating_add(4)
            })
        } else {
            0
        };
        let metadata = plan
            .semantics
            .len()
            .saturating_add(reference_metadata)
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
        prefix.extend_from_slice(&version.to_le_bytes());
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
        if !matches!(
            version,
            PIPELINE_SNAPSHOT_VERSION
                | RELIABLE_SNAPSHOT_VERSION
                | GRAPH_SNAPSHOT_VERSION
                | IOT_SNAPSHOT_VERSION
                | RELIABLE_IOT_SNAPSHOT_VERSION
                | REFERENCE_SNAPSHOT_VERSION
                | REFERENCE_LINEAR_SNAPSHOT_VERSION
                | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                | REFERENCE_GRAPH_SNAPSHOT_VERSION
                | HYSTERESIS_SNAPSHOT_VERSION
                | HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_FILE_SNAPSHOT_VERSION | PAUSED_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_COMBINED_FILE_SNAPSHOT_VERSION | PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION
                | TIME_GRAPH_PT_SNAPSHOT_VERSION | TIME_GRAPH_ET_SNAPSHOT_VERSION
                | ALARM_FILE_SNAPSHOT_VERSION | ALARM_RELIABLE_SNAPSHOT_VERSION | ALARM_GRAPH_SNAPSHOT_VERSION
        ) {
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
        let next_output=if matches!(
            version,
            RELIABLE_SNAPSHOT_VERSION
                | RELIABLE_IOT_SNAPSHOT_VERSION
                | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                | HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_FILE_SNAPSHOT_VERSION | PAUSED_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_COMBINED_FILE_SNAPSHOT_VERSION | PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION
                | ALARM_FILE_SNAPSHOT_VERSION | ALARM_RELIABLE_SNAPSHOT_VERSION
        ) {
            let epoch=take(&mut bytes,16)?.try_into().unwrap();
            Some(sparrow_model::OutputSequence::new(epoch,u64_value(&mut bytes)?)
                .map_err(|_|invalid("invalid reliable output position"))?)
        } else {None};
        if next_output.is_some() != output_profile(&source.identity.kind) {
            return Err(invalid("reliable snapshot lacks source/output identity"));
        }
        let length = u32_value(&mut bytes)?;
        let plan = CheckpointPlan::decode(take(&mut bytes, length)?)?;
        let mandatory_iot_version = matches!(
            version,
            IOT_SNAPSHOT_VERSION
                | RELIABLE_IOT_SNAPSHOT_VERSION
                | HYSTERESIS_SNAPSHOT_VERSION
                | HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_FILE_SNAPSHOT_VERSION | PAUSED_RELIABLE_SNAPSHOT_VERSION
        );
        let reference_stateful_version = matches!(
            version,
            REFERENCE_LINEAR_SNAPSHOT_VERSION
                | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                | REFERENCE_GRAPH_SNAPSHOT_VERSION
        ) && plan.has_references();
        let combined_time_version = matches!(version, PAUSED_COMBINED_FILE_SNAPSHOT_VERSION | PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION | TIME_GRAPH_PT_SNAPSHOT_VERSION | TIME_GRAPH_ET_SNAPSHOT_VERSION | ALARM_FILE_SNAPSHOT_VERSION | ALARM_RELIABLE_SNAPSHOT_VERSION | ALARM_GRAPH_SNAPSHOT_VERSION);
        if !reference_stateful_version && !combined_time_version && mandatory_iot_version != plan.has_iot() {
            return Err(invalid("IoT checkpoint version/manifest mismatch"));
        }
        if version == REFERENCE_SNAPSHOT_VERSION
            && (source.identity.kind != "file"
                || plan.is_graph()
                || !plan.has_references()
                || !plan.states.is_empty()
                || next_output.is_some())
        {
            return Err(invalid(
                "reference-table checkpoint requires a linear File stateless profile",
            ));
        }
        if version == REFERENCE_LINEAR_SNAPSHOT_VERSION
            && (source.identity.kind != "file"
                || plan.is_graph()
                || plan.states.is_empty()
                || plan.states.len() > 2
                || !plan.has_references()
                || next_output.is_some())
        {
            return Err(invalid(
                "reference-table v9 checkpoint requires a linear File stateful profile",
            ));
        }
        if version == REFERENCE_RELIABLE_SNAPSHOT_VERSION
            && (source.identity.kind != "jetstream-v1"
                || plan.is_graph()
                || plan.states.len() > 2
                || !plan.has_references()
                || next_output.is_none()
                || next_output.is_some_and(|position| position.epoch() != generation))
        {
            return Err(invalid(
                "reference-table v10 checkpoint requires linear JetStream output identity",
            ));
        }
        if version == REFERENCE_GRAPH_SNAPSHOT_VERSION
            && (source.identity.kind != "file-dag-v1"
                || !plan.is_graph()
                || !plan.has_references()
                || next_output.is_some())
        {
            return Err(invalid(
                "reference-table v11 checkpoint requires a File DAG without output cursor",
            ));
        }
        if version == HYSTERESIS_SNAPSHOT_VERSION
            && (source.identity.kind != "file"
                && !(source.identity.kind == "file-dag-v1" && plan.is_graph()))
        {
            return Err(invalid(
                "hysteresis checkpoint requires a File linear or DAG source",
            ));
        }
        if version == HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
            && (source.identity.kind != "jetstream-v1"
                || plan.is_graph()
                || next_output.is_none()
                || next_output.is_some_and(|position| position.epoch() != generation))
        {
            return Err(invalid(
                "hysteresis reliable checkpoint requires linear JetStream output identity",
            ));
        }
        if version != REFERENCE_SNAPSHOT_VERSION
            && !matches!(
                version,
                REFERENCE_LINEAR_SNAPSHOT_VERSION
                    | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                    | REFERENCE_GRAPH_SNAPSHOT_VERSION
            )
            && plan.has_references()
        {
            return Err(invalid(
                "reference-table dependencies require a v8-v11 checkpoint profile",
            ));
        }
        if version == RELIABLE_IOT_SNAPSHOT_VERSION && plan.is_graph() {
            return Err(invalid("reliable IoT checkpoint requires a linear plan"));
        }
        if version == RELIABLE_IOT_SNAPSHOT_VERSION && next_output.is_none() {
            return Err(invalid("reliable IoT checkpoint lacks output identity"));
        }
        if version == RELIABLE_IOT_SNAPSHOT_VERSION
            && next_output.is_some_and(|position| position.epoch() != generation)
        {
            return Err(invalid(
                "reliable IoT output epoch differs from the state generation",
            ));
        }
        if version == RELIABLE_SNAPSHOT_VERSION && next_output.is_none() {
            return Err(invalid("reliable v4 checkpoint lacks output identity"));
        }
        if version == RELIABLE_SNAPSHOT_VERSION && plan.has_iot() {
            return Err(invalid("reliable v4 checkpoint cannot contain IoT state"));
        }
        if !matches!(
            version,
            IOT_SNAPSHOT_VERSION
                | RELIABLE_IOT_SNAPSHOT_VERSION
                | GRAPH_SNAPSHOT_VERSION
                | REFERENCE_GRAPH_SNAPSHOT_VERSION
                | HYSTERESIS_SNAPSHOT_VERSION
                | TIME_GRAPH_PT_SNAPSHOT_VERSION | TIME_GRAPH_ET_SNAPSHOT_VERSION
                | ALARM_GRAPH_SNAPSHOT_VERSION
        ) && plan.is_graph()
        {
            return Err(invalid("graph checkpoint version/manifest mismatch"));
        }
        if matches!(
            version,
            IOT_SNAPSHOT_VERSION
                | RELIABLE_IOT_SNAPSHOT_VERSION
                | GRAPH_SNAPSHOT_VERSION
                | REFERENCE_GRAPH_SNAPSHOT_VERSION
                | HYSTERESIS_SNAPSHOT_VERSION
        )
            && (source.identity.kind == "file-dag-v1") != plan.is_graph() {
            return Err(invalid("checkpoint source topology mismatch"));
        }
        let expected_version = snapshot_version_for(&plan, &source.identity.kind)?;
        if expected_version != version {
            return Err(invalid("checkpoint source/profile and manifest semantics disagree"));
        }
        let processing_time = if plan.is_time_graph() {
            let cut=crate::graph_cut::GraphCut::unwrap(&source)?;
            if cut.ingested!=ingested_rows || next_output.is_some() {return Err(invalid("graph cut count/output mismatch"));}
            (!plan.has_event_time_state()).then_some(cut.micros)
        } else if plan.requires_paused_time() {
            if next_output.is_none_or(|output|output.epoch()!=generation) {
                return Err(invalid("paused-time snapshot lacks stable output generation"));
            }
            Some(crate::processing_cut::ProcessingCut::unwrap(&source)?.micros)
        } else { None };
        let n = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()) as usize;
        if n != plan.states.len() {
            return Err(invalid("missing/extra checkpoint state participant"));
        }
        let mut windows = Vec::with_capacity(if materialize { n } else { 0 });
        let mut iot = Vec::with_capacity(if materialize { n } else { 0 });
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
            if participant.codec == sparrow_plan::checkpoint::IOT_STATE_CODEC {
                let freeze = crate::iot::IotFreeze::decode_at_cut(&mut frame, per_participant, materialize, processing_time)?;
                remaining -= entries;
                if !frame.is_empty() || participant.id != ParticipantId::iot(freeze.operator)
                    || freeze.kind != participant.freeze_kind() {
                    return Err(invalid("IoT checkpoint state identity or codec mismatch"));
                }
                if materialize { iot.push(freeze); }
                continue;
            }
            let freeze =
                crate::checkpoint::decode_freeze_at_cut(&mut frame, per_participant, materialize,
                    processing_time.filter(|_|participant.window_kind == 0))?;
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
            iot,
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
        if matches!(bytes.get(4..6),Some([3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22,0])) {
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

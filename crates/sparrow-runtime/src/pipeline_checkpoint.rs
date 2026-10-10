//! K1 snapshot envelope: one File cut, complete participant manifest, bounded
//! state frames and attempt/revision provenance. Legacy SPV1 v1/v2 stay separate.
use crate::barrier::ParticipantAcks;
use crate::checkpoint::{EncodedSnapshot, MAX_SNAPSHOT_BYTES};
use crate::window::WindowFreeze;
use sparrow_io::{SinkEncoding, SinkIdentity, SourcePosition};
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
pub const OBSERVED_FILE_SNAPSHOT_VERSION: u16 = 23;
pub const OBSERVED_RELIABLE_SNAPSHOT_VERSION: u16 = 24;
pub const RESAMPLE_FILE_SNAPSHOT_VERSION: u16 = 25;
pub const RESAMPLE_RELIABLE_SNAPSHOT_VERSION: u16 = 26;
/// A linear File cut plus an exact PubAck-confirmed JetStream output target.
/// Neither v3's downstream-prefix relaxation nor a JS source/output cursor
/// can authorize this restore. Existing v1..v26 payloads remain unchanged.
pub const FILE_JETSTREAM_SINK_SNAPSHOT_VERSION: u16 = 27;
/// CSV-v1 output uses JSI2 and its own directory/profile. Existing v27/JSI1
/// JSON history is never upgraded or reinterpreted as a CSV output target.
pub const FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION: u16 = 28;
/// Linear File with at least one codec 3 (FIRST/LAST/VAR/STDDEV) window.
/// Strict full-semantics identity; never the v3 downstream-prefix relaxation.
pub const EXT_AGG_FILE_SNAPSHOT_VERSION: u16 = 29;
/// Linear reliable JetStream input plus output cursor and codec 3 Count windows.
pub const EXT_AGG_RELIABLE_SNAPSHOT_VERSION: u16 = 30;
/// Linear File with exactly one codec 4 (`BWF1`) sliding count participant.
/// Strict full-semantics identity (no RCP2 prefix relaxation).
pub const SLIDING_COUNT_FILE_SNAPSHOT_VERSION: u16 = 31;
/// Linear reliable JetStream + output cursor + one codec 4 sliding count.
pub const SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION: u16 = 32;
const MAX_SOURCE_METADATA: usize = 64 * 1024;

pub(crate) const EXTENDED_PROFILE_GUARD: &str = "extended_profile_mismatch";
pub(crate) const RESTORE_CREDIT_GUARD: &str = "restore_credit";
pub(crate) const BUFFERED_PROFILE_GUARD: &str = "buffered_profile_mismatch";

fn buffered_mismatch(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::UnsupportedRestore, message)
        .context("checkpoint_guard", BUFFERED_PROFILE_GUARD)
}

fn extended_mismatch(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::UnsupportedRestore, message)
        .context("checkpoint_guard", EXTENDED_PROFILE_GUARD)
}

/// Restore-time memory accounting shared by every Store decode entry. With an
/// owner, decode scratch is reserved before each frame is scanned/decoded and
/// the materialize pass verifies every participant against the credit that
/// was reserved from the preceding bounded scan.
pub(crate) struct RestoreMeter {
    owner: Option<Arc<MemoryOwner>>,
    scratch: Option<sparrow_model::MemoryLease>,
    /// Materialize pass only: per-participant reserved resident bytes.
    pub(crate) planned: Option<Vec<usize>>,
    /// Per-participant exact resident bytes, in manifest order.
    pub(crate) resident: Vec<usize>,
}

impl RestoreMeter {
    pub(crate) fn unbilled() -> Self {
        Self { owner: None, scratch: None, planned: None, resident: Vec::new() }
    }
    pub(crate) fn billed(owner: Arc<MemoryOwner>) -> Self {
        Self { owner: Some(owner), scratch: None, planned: None, resident: Vec::new() }
    }
    pub(crate) fn charge_scratch(&mut self, bytes: usize) -> Result<()> {
        let Some(owner) = &self.owner else { return Ok(()); };
        let bytes = bytes.max(1);
        match &mut self.scratch {
            Some(lease) if lease.bytes() >= bytes => Ok(()),
            Some(lease) => lease.grow_to(bytes).map_err(restore_credit_error),
            None => {
                self.scratch = Some(owner.acquire(CreditKind::Reservation, bytes).map_err(restore_credit_error)?);
                Ok(())
            }
        }
    }
    pub(crate) fn release_scratch(&mut self) {
        self.scratch = None;
    }
}

pub(crate) fn restore_credit_error(error: SparrowError) -> SparrowError {
    error.context("checkpoint_guard", RESTORE_CREDIT_GUARD)
}

/// Decode scratch per frame: temporary scalar copies for WindowFreeze; IoT
/// additionally keeps a duplicate-key set (key bytes + tree nodes).
fn frame_scratch_bound(codec: u16, frame_len: usize, entries: usize) -> usize {
    if codec == sparrow_plan::checkpoint::IOT_STATE_CODEC {
        frame_len
            .saturating_mul(5)
            .saturating_add(entries.saturating_mul(72))
            .saturating_add(1024)
    } else {
        frame_len.saturating_add(1024)
    }
}

fn sink_profile_version(sink: &SinkIdentity) -> u16 {
    match &sink.encoding {
        SinkEncoding::Json => FILE_JETSTREAM_SINK_SNAPSHOT_VERSION,
        SinkEncoding::Csv(_) => FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION,
    }
}

pub fn sink_snapshot_version_for(
    plan: &CheckpointPlan,
    source_kind: &str,
    sink: &SinkIdentity,
) -> Result<u16> {
    let guard = |error: SparrowError| error.context("checkpoint_guard", "sink_profile_mismatch");
    sink.validate().map_err(guard)?;
    if source_kind != "file"
        || plan.is_graph()
        || snapshot_version_for(plan, source_kind).map_err(guard)? != PIPELINE_SNAPSHOT_VERSION
    {
        return Err(guard(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "JetStream sink checkpoint requires its independent linear File output profile",
        )));
    }
    Ok(sink_profile_version(sink))
}

/// v27/v28 preserve all output computation, including transforms after the last
/// stateful participant. Never change the old CheckpointPlan prefix contract.
pub(crate) fn check_sink_plan_compatible(saved: &CheckpointPlan, live: &CheckpointPlan) -> Result<()> {
    saved.validate()?;
    live.validate()?;
    if saved.source != live.source
        || saved.sink != live.sink
        || saved.states != live.states
        || saved.reference_tables != live.reference_tables
        || saved.semantics != live.semantics
    {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "JetStream sink checkpoint requires unchanged full pipeline semantics and participants",
        ));
    }
    Ok(())
}

fn output_profile(kind: &str) -> bool {
    matches!(kind,"jetstream-v1"|crate::processing_cut::FILE_KIND|crate::processing_cut::JETSTREAM_KIND
        |crate::observed_cut::FILE_KIND|crate::observed_cut::JETSTREAM_KIND)
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

    if plan.has_buffered_state() {
        // validate() already pins a single linear codec 4 participant.
        if graph || references || plan.has_iot() || plan.requires_paused_time() || plan.states.len() != 1 {
            return Err(buffered_mismatch("sliding count state requires a strict single-state linear profile"));
        }
        return match source_kind {
            "file" => Ok(SLIDING_COUNT_FILE_SNAPSHOT_VERSION),
            "jetstream-v1" => Ok(SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION),
            _ => Err(buffered_mismatch(
                "sliding count state requires a linear File or JetStream source",
            )),
        };
    }

    if plan.has_extended_state() {
        // validate() already excludes DAG/references/IoT/processing time.
        if graph || references || plan.has_iot() || plan.requires_paused_time() {
            return Err(extended_mismatch("extended aggregate state requires a strict linear profile"));
        }
        return match source_kind {
            "file" => Ok(EXT_AGG_FILE_SNAPSHOT_VERSION),
            "jetstream-v1"
                if plan.states.iter().all(|state| state.window_kind == 1) =>
            {
                Ok(EXT_AGG_RELIABLE_SNAPSHOT_VERSION)
            }
            "jetstream-v1" => Err(extended_mismatch(
                "JetStream extended aggregate profile requires Count windows",
            )),
            _ => Err(extended_mismatch(
                "extended aggregate state requires a linear File or JetStream source",
            )),
        };
    }

    if plan.has_resample() {
        if graph || references || plan.states.len() != 1 || !plan.requires_paused_time() {
            return Err(invalid("resample requires one linear paused-time state without references"));
        }
        return match source_kind {
            crate::processing_cut::FILE_KIND => Ok(RESAMPLE_FILE_SNAPSHOT_VERSION),
            crate::processing_cut::JETSTREAM_KIND => Ok(RESAMPLE_RELIABLE_SNAPSHOT_VERSION),
            _ => Err(invalid("resample requires a durable paused-time source")),
        };
    }

    if plan.has_silence() {
        if graph || references || plan.states.len() != 1 || !plan.requires_paused_time() {
            return Err(invalid("silence requires one linear source-observed state without references"));
        }
        return match source_kind {
            crate::observed_cut::FILE_KIND => Ok(OBSERVED_FILE_SNAPSHOT_VERSION),
            crate::observed_cut::JETSTREAM_KIND => Ok(OBSERVED_RELIABLE_SNAPSHOT_VERSION),
            _ => Err(invalid("silence requires its independent observed-time source profile")),
        };
    }
    if matches!(source_kind,crate::observed_cut::FILE_KIND|crate::observed_cut::JETSTREAM_KIND) {
        return Err(invalid("observed-time source requires silence semantics"));
    }

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
    pub sink_identity: Option<SinkIdentity>,
    pub plan: CheckpointPlan,
    pub windows: Vec<WindowFreeze>,
    pub iot: Vec<crate::iot::IotFreeze>,
    /// Codec 4 sliding count participants (v31/v32 only).
    pub buffered: Vec<crate::buffered_window::BufferedFreeze>,
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
        if self.sink_identity.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "JetStream sink checkpoint requires explicit sink-aware compatibility validation",
            ));
        }
        self.plan.check_compatible(live)
    }

    pub fn check_compatible_with_sink(&self, live: &CheckpointPlan, sink: &SinkIdentity) -> Result<()> {
        let saved = self.sink_identity.as_ref().ok_or_else(|| SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "legacy/source-only checkpoint cannot authorize JetStream sink restore; use a new directory",
        ))?;
        sink_snapshot_version_for(&self.plan, &self.source.identity.kind, saved)?;
        sink_snapshot_version_for(live, "file", sink)?;
        if saved != sink || self.next_output.is_some() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "JetStream sink endpoint, stream incarnation, subject or msg-id policy changed",
            ));
        }
        check_sink_plan_compatible(&self.plan, live)
    }

    /// Bounded header-only target parse for profile writers. It never decodes
    /// state or treats diagnostic provenance as a restore authorization.
    pub(crate) fn encoded_sink_identity(mut bytes: &[u8]) -> Result<SinkIdentity> {
        if take(&mut bytes, 4)? != crate::checkpoint::MAGIC {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                "JetStream sink writer cannot adopt another snapshot profile"));
        }
        let version = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap());
        if !matches!(version, FILE_JETSTREAM_SINK_SNAPSHOT_VERSION | FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION) {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                "JetStream sink writer cannot adopt another snapshot profile"));
        }
        take(&mut bytes, 32)?;
        for _ in 0..2 {
            let length = u32_value(&mut bytes)?;
            if length > MAX_SOURCE_METADATA { return Err(invalid("source metadata exceeds limit")); }
            take(&mut bytes, length)?;
        }
        take(&mut bytes, 48)?;
        decode_sink_identity(&mut bytes, version)
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
        Self::encode_frozen_mode(checkpoint_id, source, ingested_rows, revision, plan, None, acks, owner, max_keys)
    }

    pub fn encode_frozen_with_sink(
        checkpoint_id: u64,
        source: &SourcePosition,
        ingested_rows: u64,
        revision: u64,
        plan: &CheckpointPlan,
        sink: &SinkIdentity,
        acks: ParticipantAcks,
        owner: &Arc<MemoryOwner>,
        max_keys: usize,
    ) -> Result<EncodedSnapshot> {
        Self::encode_frozen_mode(checkpoint_id, source, ingested_rows, revision, plan, Some(sink), acks, owner, max_keys)
    }

    fn encode_frozen_mode(
        checkpoint_id: u64,
        source: &SourcePosition,
        ingested_rows: u64,
        revision: u64,
        plan: &CheckpointPlan,
        sink: Option<&SinkIdentity>,
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
        let version = if let Some(sink) = sink {
            sink_snapshot_version_for(plan, &source.identity.kind, sink)?
        } else {
            snapshot_version_for(plan, &source.identity.kind)?
        };
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
        } else if plan.has_silence() { crate::observed_cut::ObservedCut::unwrap(source)?;
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
                | OBSERVED_FILE_SNAPSHOT_VERSION | OBSERVED_RELIABLE_SNAPSHOT_VERSION
                | RESAMPLE_FILE_SNAPSHOT_VERSION | RESAMPLE_RELIABLE_SNAPSHOT_VERSION
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
        let sink_metadata = sink.map(SinkIdentity::encoded_len).transpose()?.map_or(0, |n| n + 4);
        let metadata = plan
            .semantics
            .len()
            .saturating_add(reference_metadata)
            .saturating_add(source.identity.kind.len())
            .saturating_add(source.identity.path.len())
            .saturating_add(sink_metadata)
            .saturating_add(512)
            .saturating_mul(3);
        let _workspace = owner.acquire(CreditKind::Reservation, metadata)?;
        let manifest = plan.encode()?;
        // Include the state count and first frame length now. Appending either
        // after filling a large manifest must not double a whole metadata Vec.
        let mut prefix = Vec::with_capacity(
            104 + usize::from(acks.next_output.is_some())*24 + source.identity.kind.len() + source.identity.path.len() + manifest.len() + sink_metadata,
        );
        prefix.extend_from_slice(crate::checkpoint::MAGIC);
        prefix.extend_from_slice(&version.to_le_bytes());
        prefix.extend_from_slice(&checkpoint_id.to_le_bytes());
        prefix.extend_from_slice(&ingested_rows.to_le_bytes());
        crate::checkpoint::encode_position(source, &mut prefix)?;
        prefix.extend_from_slice(&acks.attempt.to_le_bytes());
        prefix.extend_from_slice(&revision.to_le_bytes());
        prefix.extend_from_slice(&acks.generation);
        if let Some(sink) = sink {
            prefix.extend_from_slice(&(sink.encoded_len()? as u32).to_le_bytes());
            sink.encode_into(&mut prefix)?;
        }
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
            // A codec 1 frame never carries tags 8/9, and a codec 3 manifest
            // never wraps a frame written without the extended grammar.
            if freeze.buffered != (participant.codec == sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC) {
                return Err(buffered_mismatch(
                    "participant state codec differs from the encoded buffered freeze grammar",
                ));
            }
            if freeze.ext != (participant.codec == sparrow_plan::checkpoint::WINDOW_EXT_STATE_CODEC) {
                return Err(extended_mismatch(
                    "participant state codec differs from the encoded freeze grammar",
                ));
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
        bytes: &[u8],
        max_keys: usize,
        materialize: bool,
    ) -> Result<Self> {
        Self::decode_metered(bytes, max_keys, materialize, &mut RestoreMeter::unbilled())
    }

    pub(crate) fn decode_metered(
        mut bytes: &[u8],
        max_keys: usize,
        materialize: bool,
        meter: &mut RestoreMeter,
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
                | OBSERVED_FILE_SNAPSHOT_VERSION | OBSERVED_RELIABLE_SNAPSHOT_VERSION
                | RESAMPLE_FILE_SNAPSHOT_VERSION | RESAMPLE_RELIABLE_SNAPSHOT_VERSION
                | FILE_JETSTREAM_SINK_SNAPSHOT_VERSION
                | FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION
                | EXT_AGG_FILE_SNAPSHOT_VERSION
                | EXT_AGG_RELIABLE_SNAPSHOT_VERSION
                | SLIDING_COUNT_FILE_SNAPSHOT_VERSION
                | SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION
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
        let sink_identity = if matches!(version, FILE_JETSTREAM_SINK_SNAPSHOT_VERSION | FILE_JETSTREAM_CSV_SINK_SNAPSHOT_VERSION) {
            Some(decode_sink_identity(&mut bytes, version)?)
        } else { None };
        if sink_identity.is_some() && source.identity.kind != "file" {
            // Do this before the legacy source/output-cursor matrix can
            // classify a foreign source as a damaged output position.
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                "JetStream sink checkpoint cannot adopt a non-File source profile")
                .context("checkpoint_guard", "sink_profile_mismatch"));
        }
        let extended_version = matches!(
            version,
            EXT_AGG_FILE_SNAPSHOT_VERSION | EXT_AGG_RELIABLE_SNAPSHOT_VERSION
        );
        let buffered_version = matches!(
            version,
            SLIDING_COUNT_FILE_SNAPSHOT_VERSION | SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION
        );
        // v29-v32 envelopes are complete, checksummed records: identity and
        // profile disagreements are incompatibilities, never corruption.
        let strict = |message: &str| if extended_version {
            extended_mismatch(message)
        } else if buffered_version {
            buffered_mismatch(message)
        } else { invalid(message) };
        let next_output=if matches!(
            version,
            RELIABLE_SNAPSHOT_VERSION
                | RELIABLE_IOT_SNAPSHOT_VERSION
                | REFERENCE_RELIABLE_SNAPSHOT_VERSION
                | HYSTERESIS_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_FILE_SNAPSHOT_VERSION | PAUSED_RELIABLE_SNAPSHOT_VERSION
                | PAUSED_COMBINED_FILE_SNAPSHOT_VERSION | PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION
                | ALARM_FILE_SNAPSHOT_VERSION | ALARM_RELIABLE_SNAPSHOT_VERSION
                | OBSERVED_FILE_SNAPSHOT_VERSION | OBSERVED_RELIABLE_SNAPSHOT_VERSION
                | RESAMPLE_FILE_SNAPSHOT_VERSION | RESAMPLE_RELIABLE_SNAPSHOT_VERSION
                | EXT_AGG_RELIABLE_SNAPSHOT_VERSION
                | SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION
        ) {
            let epoch=take(&mut bytes,16)?.try_into().unwrap();
            Some(sparrow_model::OutputSequence::new(epoch,u64_value(&mut bytes)?)
                .map_err(|_| strict("invalid reliable output position"))?)
        } else {None};
        if next_output.is_some() != output_profile(&source.identity.kind) {
            return Err(strict("reliable snapshot lacks source/output identity"));
        }
        let length = u32_value(&mut bytes)?;
        let plan = CheckpointPlan::decode(take(&mut bytes, length)?).map_err(|error| {
            if extended_version && error.code == ErrorCode::UnsupportedRestore {
                error.context("checkpoint_guard", EXTENDED_PROFILE_GUARD)
            } else if buffered_version && error.code == ErrorCode::UnsupportedRestore {
                error.context("checkpoint_guard", BUFFERED_PROFILE_GUARD)
            } else { error }
        })?;
        // Codec 3 and the v29/v30 envelopes select each other exactly. A
        // mismatch is a version/profile incompatibility from a complete,
        // checksummed record; never classify it as corruption fallback.
        if extended_version != plan.has_extended_state() {
            return Err(extended_mismatch(
                "checkpoint outer version and extended aggregate state codec disagree",
            ));
        }
        if buffered_version != plan.has_buffered_state() {
            return Err(buffered_mismatch(
                "checkpoint outer version and sliding count state codec disagree",
            ));
        }
        if buffered_version {
            let expected = snapshot_version_for(&plan, &source.identity.kind)?;
            if expected != version {
                return Err(buffered_mismatch(
                    "sliding count checkpoint source/profile disagree",
                ));
            }
            if version == SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION
                && next_output.is_none_or(|position| position.epoch() != generation)
            {
                return Err(buffered_mismatch(
                    "v32 checkpoint lacks a stable output identity for its state generation",
                ));
            }
        }
        if extended_version {
            let expected = snapshot_version_for(&plan, &source.identity.kind)?;
            if expected != version {
                return Err(extended_mismatch(
                    "extended aggregate checkpoint source/profile disagree",
                ));
            }
            if version == EXT_AGG_RELIABLE_SNAPSHOT_VERSION
                && next_output.is_none_or(|position| position.epoch() != generation)
            {
                return Err(extended_mismatch(
                    "v30 checkpoint lacks a stable output identity for its state generation",
                ));
            }
        }
        // New output profiles have a complete, independent eligibility
        // contract. Reject foreign graph/IoT/time manifests before the old
        // version/participant checks can turn them into corruption fallback.
        let expected_sink_version = sink_identity.as_ref()
            .map(|sink| sink_snapshot_version_for(&plan, &source.identity.kind, sink))
            .transpose()?;
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
        let combined_time_version = matches!(version, PAUSED_COMBINED_FILE_SNAPSHOT_VERSION | PAUSED_COMBINED_RELIABLE_SNAPSHOT_VERSION | TIME_GRAPH_PT_SNAPSHOT_VERSION | TIME_GRAPH_ET_SNAPSHOT_VERSION | ALARM_FILE_SNAPSHOT_VERSION | ALARM_RELIABLE_SNAPSHOT_VERSION | ALARM_GRAPH_SNAPSHOT_VERSION | OBSERVED_FILE_SNAPSHOT_VERSION | OBSERVED_RELIABLE_SNAPSHOT_VERSION | RESAMPLE_FILE_SNAPSHOT_VERSION | RESAMPLE_RELIABLE_SNAPSHOT_VERSION);
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
        let expected_version = if let Some(version) = expected_sink_version {
            version
        } else {
            snapshot_version_for(&plan, &source.identity.kind)?
        };
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
            Some(if plan.has_silence() {
                let cut = crate::observed_cut::ObservedCut::unwrap(&source)?;
                if cut.source.record_index < ingested_rows
                    || (cut.source.identity.kind=="jetstream-v1" && cut.source.record_index!=ingested_rows) {
                    return Err(invalid("observed source cut/input count mismatch"));
                }
                cut.micros
            } else { crate::processing_cut::ProcessingCut::unwrap(&source)?.micros })
        } else { None };
        let n = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().unwrap()) as usize;
        if n != plan.states.len() {
            return Err(invalid("missing/extra checkpoint state participant"));
        }
        let mut windows = Vec::with_capacity(if materialize { n } else { 0 });
        let mut iot = Vec::with_capacity(if materialize { n } else { 0 });
        let mut buffered = Vec::with_capacity(if materialize { n } else { 0 });
        let per_participant = crate::checkpoint::freeze_entry_cap(max_keys);
        let mut remaining = per_participant
            .checked_mul(plan.states.len())
            .ok_or_else(|| invalid("total participant work bound overflow"))?;
        meter.resident.clear();
        for (index, participant) in plan.states.iter().enumerate() {
            let length = u32_value(&mut bytes)?;
            let mut frame = take(&mut bytes, length)?;
            let entries = crate::checkpoint::FreezeHeader::parse(frame)?.entries;
            if entries > per_participant || entries > remaining {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "total decoded participant entry limit",
                ));
            }
            meter.charge_scratch(frame_scratch_bound(participant.codec, frame.len(), entries))?;
            let planned = meter.planned.as_ref().map(|planned| planned.get(index).copied().unwrap_or(0));
            let check_planned = |resident: usize| -> Result<()> {
                if materialize && planned.is_some_and(|planned| resident > planned) {
                    return Err(SparrowError::new(
                        ErrorCode::Internal,
                        "restored participant exceeds its reserved restore credit",
                    ));
                }
                Ok(())
            };
            if participant.codec == sparrow_plan::checkpoint::IOT_STATE_CODEC {
                let mut resident = 0usize;
                let freeze = crate::iot::IotFreeze::decode_metered(&mut frame, per_participant, materialize, processing_time, &mut resident)?;
                remaining -= entries;
                if !frame.is_empty() || participant.id != ParticipantId::iot(freeze.operator)
                    || freeze.kind != participant.freeze_kind() {
                    return Err(invalid("IoT checkpoint state identity or codec mismatch"));
                }
                check_planned(resident)?;
                meter.resident.push(resident);
                if materialize { iot.push(freeze); }
                continue;
            }
            if participant.codec == sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC {
                let mut resident = 0usize;
                let freeze = crate::buffered_window::BufferedFreeze::decode_metered(
                    &mut frame, per_participant, materialize, &mut resident)?;
                remaining -= entries;
                if !frame.is_empty()
                    || participant.id != (ParticipantId::State { operator: freeze.operator, slot: freeze.slot, shard: 0 })
                    || freeze.kind != participant.freeze_kind()
                {
                    return Err(invalid("buffered checkpoint state identity or codec mismatch"));
                }
                check_planned(resident)?;
                meter.resident.push(resident);
                if materialize { buffered.push(freeze); }
                continue;
            }
            let codec = match participant.codec {
                sparrow_plan::checkpoint::WINDOW_EXT_STATE_CODEC => crate::aggregate::AccumulatorCodec::WindowExt,
                _ => crate::aggregate::AccumulatorCodec::Window,
            };
            let mut resident = 0usize;
            let freeze =
                crate::checkpoint::decode_freeze_metered(&mut frame, per_participant, materialize,
                    processing_time.filter(|_|participant.window_kind == 0), codec, &mut resident)?;
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
            check_planned(resident)?;
            meter.resident.push(resident);
            if materialize {
                windows.push(freeze);
            }
        }
        meter.release_scratch();
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
            sink_identity,
            plan,
            windows,
            iot,
            buffered,
            next_output,
        })
    }
}

fn decode_sink_identity(bytes: &mut &[u8], version: u16) -> Result<SinkIdentity> {
    let guard = |error: SparrowError| error.context("checkpoint_guard", "sink_profile_mismatch");
    let length = u32_value(bytes).map_err(guard)?;
    if length > sparrow_io::sink_identity::MAX_SINK_IDENTITY_BYTES {
        return Err(guard(invalid("sink identity exceeds byte bound")));
    }
    let sink = SinkIdentity::decode(take(bytes, length).map_err(guard)?)
        .map_err(guard)?;
    if sink_profile_version(&sink) != version {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
            "checkpoint output profile and sink encoding codec disagree")
            .context("checkpoint_guard", "sink_profile_mismatch"));
    }
    Ok(sink)
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
        Self::decode_metered(bytes, max_keys, materialize, &mut RestoreMeter::unbilled())
    }
    pub(crate) fn decode_metered(
        bytes: &[u8],
        max_keys: usize,
        materialize: bool,
        meter: &mut RestoreMeter,
    ) -> Result<Self> {
        if matches!(bytes.get(4..6),Some([3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32,0])) {
            Ok(Self::Pipeline(PipelineSnapshot::decode_metered(
                bytes,
                max_keys,
                materialize,
                meter,
            )?))
        } else {
            Ok(Self::Legacy(
                crate::checkpoint::CheckpointSnapshot::decode_metered(bytes, max_keys, materialize, meter)?,
            ))
        }
    }
    /// Restore-credit participant identities, in decode (`meter.resident`) order.
    pub(crate) fn credit_participants(&self) -> Vec<ParticipantId> {
        match self {
            Self::Legacy(s) => vec![ParticipantId::State { operator: s.window.operator, slot: s.window.slot, shard: 0 }],
            Self::Pipeline(s) => s.plan.states.iter().map(|state| state.id).collect(),
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

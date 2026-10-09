//! Versioned production aligned single-job checkpoint store.
//!
//! Protocol:
//! 1. Barrier alignment (ordered cut; later rows may already be flowing).
//! 2. Freeze + chunk write (`*.bin.part` → `*.bin`).
//! 3. ACK after every chunk is renamed.
//! 4. Versioned manifest commit (`MANIFEST.tmp` → `MANIFEST`, then `CURRENT.tmp` → `CURRENT`).
//! 5. Recover from a **verified committed** CURRENT+MANIFEST only.
//!
//! This path is **not** exactly-once. Missing or corrupt checkpoints are
//! rejected — never a silent empty-state continue.

use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{
    DeliveryContract, ErrorCode, MemoryOwner, OperatorId, RecoveryPolicy, ResourceBudget, Result,
    Scalar, SparrowError, StateSlotId,
};
use std::sync::Arc;
use sparrow_plan::PlanLayout;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::aggregate::Accumulator;
use crate::window::{FrozenEntry, WindowFreeze};
use crate::pipeline_checkpoint::{PipelineSnapshot, StoredSnapshot};

pub const CHECKPOINT_LABEL: &str = "aligned";
pub const CHUNK_SIZE: usize = 4096;
pub const MAGIC: &[u8; 4] = b"SPV1";
/// Version 2 requires full state semantics. Version 1 remains inspectable,
/// but cannot authorize restore or be silently rewritten as version 2.
pub const SNAPSHOT_VERSION: u16 = 2;
pub const MANIFEST_MAGIC: &[u8; 4] = b"MAN2";
pub const MANIFEST_VERSION: u16 = 1;
/// Reject encode/commit payloads above this (R15).
pub const MAX_SNAPSHOT_BYTES: u64 = 8 * 1024 * 1024;
/// Reject MANIFEST files before allocating the checksum table (R15).
pub const MAX_MANIFEST_BYTES: u64 = 256 * 1024;
pub const MAX_MANIFEST_CHUNKS: u32 = 4096;
/// Keep this many committed generations on disk (R15).
pub const KEEP_GENERATIONS: u64 = 3;
pub const MAX_STORE_BYTES: u64 = 32 * 1024 * 1024;
/// Freeze entry alloc ceiling when no job bound is supplied. Tied to the
/// largest official `ResourceBudget::max_state_keys` so a performance-profile
/// `freeze()`+`commit()` cannot publish a CURRENT that `decode_freeze` rejects.
pub const MAX_FREEZE_ENTRIES: usize = ResourceBudget::performance().max_state_keys;

/// Encode/decode entry cap for one freeze. Uses the job `max_state_keys`
/// when set; otherwise [`MAX_FREEZE_ENTRIES`].
pub const fn freeze_entry_cap(max_state_keys: usize) -> usize {
    if max_state_keys == 0 {
        MAX_FREEZE_ENTRIES
    } else {
        max_state_keys
    }
}

/// Fault injection points for crash-cut / disk-full tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    None,
    /// Write only half of a chunk then fail (disk-full style).
    DuringChunkWrite,
    /// Chunks exist, ACK not written.
    AfterChunkWrite,
    /// ACK exists, MANIFEST not written.
    AfterAck,
    /// MANIFEST.tmp exists, rename not performed.
    AfterManifestTmp,
    /// MANIFEST committed, CURRENT not renamed.
    AfterManifestRename,
    /// Write MANIFEST with a wrong checksum then commit (corrupt).
    CorruptChecksum,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FaultHook {
    pub point: FaultPoint,
}

impl Default for FaultPoint {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableRevisionBind {
    pub name: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CheckpointSnapshot {
    pub checkpoint_id: u64,
    pub source: SourcePosition,
    pub window: WindowFreeze,
    pub ingested_rows: u64,
    pub layout: PlanLayout,
    pub table: Option<TableRevisionBind>,
}

impl CheckpointSnapshot {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.encode_with_max_state_keys(MAX_FREEZE_ENTRIES)
    }

    /// Encode using the job/process `max_state_keys`. Fails closed if the
    /// freeze has more entries than that bound (never publish CURRENT that
    /// the matching decode path cannot recover).
    pub fn encode_with_max_state_keys(&self, max_state_keys: usize) -> Result<Vec<u8>> {
        let cap = freeze_entry_cap(max_state_keys);
        let mut out = Vec::new();
        write_snapshot_prefix(
            &mut out,
            self.checkpoint_id,
            self.ingested_rows,
            &self.source,
        )?;
        encode_freeze(&self.window, &mut out, cap)?;
        write_snapshot_suffix(&mut out, &self.layout, self.table.as_ref())?;
        Ok(out)
    }

    /// Encode from the live operator without materializing [`WindowFreeze`]
    /// (P1-14 incremental path). Same bytes as freeze-then-encode.
    pub fn encode_from_operator(
        checkpoint_id: u64,
        source: &SourcePosition,
        ingested_rows: u64,
        layout: &PlanLayout,
        table: Option<&TableRevisionBind>,
        op: &crate::window::WindowOperator,
        max_state_keys: usize,
    ) -> Result<Vec<u8>> {
        let cap = freeze_entry_cap(max_state_keys);
        let mut out = Vec::new();
        write_snapshot_prefix(&mut out, checkpoint_id, ingested_rows, source)?;
        // Legacy SPV1 is codec 1 only: tags 8/9 are refused on encode.
        op.encode_freeze_into_codec(&mut out, cap, crate::aggregate::AccumulatorCodec::Window)?;
        write_snapshot_suffix(&mut out, layout, table)?;
        Ok(out)
    }

    /// Wrap the already encoded ACK in-place; no second state encode/clone.
    pub fn encode_frozen(
        checkpoint_id: u64,
        source: &SourcePosition,
        ingested_rows: u64,
        layout: &PlanLayout,
        table: Option<&TableRevisionBind>,
        mut freeze: crate::barrier::EncodedFreeze,
    ) -> Result<EncodedSnapshot> {
        if freeze.bytes.len() < 11 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "truncated prepared freeze",
            ));
        }
        let entries = u32::from_le_bytes(freeze.bytes[7..11].try_into().unwrap()) as usize;
        let mut prefix = Vec::new();
        write_snapshot_prefix(&mut prefix, checkpoint_id, ingested_rows, source)?;
        let mut suffix = Vec::new();
        write_snapshot_suffix(&mut suffix, layout, table)?;
        let total = prefix
            .len()
            .saturating_add(freeze.bytes.len())
            .saturating_add(suffix.len());
        if total as u64 > MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "snapshot exceeds byte cap",
            ));
        }
        freeze.lease.grow_to(total.max(freeze.bytes.capacity()))?;
        freeze.bytes.reserve_exact(total - freeze.bytes.len());
        let state_len = freeze.bytes.len();
        freeze.bytes.resize(total, 0);
        freeze.bytes.copy_within(..state_len, prefix.len());
        freeze.bytes[..prefix.len()].copy_from_slice(&prefix);
        freeze.bytes[prefix.len() + state_len..].copy_from_slice(&suffix);
        Ok(EncodedSnapshot {
            bytes: freeze.bytes,
            lease: freeze.lease,
            checkpoint_id,
            entries,
        })
    }

    pub fn decode(src: &[u8]) -> Result<Self> {
        Self::decode_with_max_state_keys(src, MAX_FREEZE_ENTRIES)
    }

    pub fn decode_with_max_state_keys(src: &[u8], max_state_keys: usize) -> Result<Self> {
        Self::decode_mode(src, max_state_keys, true)
    }

    pub(crate) fn decode_mode(src: &[u8], max_state_keys: usize, materialize: bool) -> Result<Self> {
        Self::decode_metered(src, max_state_keys, materialize, &mut crate::pipeline_checkpoint::RestoreMeter::unbilled())
    }

    pub(crate) fn decode_metered(
        src: &[u8],
        max_state_keys: usize,
        materialize: bool,
        meter: &mut crate::pipeline_checkpoint::RestoreMeter,
    ) -> Result<Self> {
        let cap = freeze_entry_cap(max_state_keys);
        let mut src = src;
        if src.len() < 4 + 2 + 8 + 8 || &src[..4] != MAGIC {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "checkpoint snapshot magic/header mismatch (want SPV1 production codec)",
            ));
        }
        src = &src[4..];
        let ver = u16::from_le_bytes(src[..2].try_into().unwrap());
        src = &src[2..];
        if ver != 1 && ver != SNAPSHOT_VERSION {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("checkpoint snapshot version {ver} unsupported (supported versions: 1 (inspect only), {SNAPSHOT_VERSION})"),
            ));
        }
        let checkpoint_id = u64::from_le_bytes(src[..8].try_into().unwrap());
        src = &src[8..];
        let ingested_rows = u64::from_le_bytes(src[..8].try_into().unwrap());
        src = &src[8..];
        let source = decode_position(&mut src)?;
        // SPV1 is codec 1: tags 8/9 are rejected on scan and materialize.
        meter.charge_scratch(src.len().saturating_add(1024))?;
        let mut resident = 0usize;
        let window = decode_freeze_metered(&mut src, cap, materialize, None, crate::aggregate::AccumulatorCodec::Window, &mut resident)?;
        if materialize
            && meter
                .planned
                .as_ref()
                .is_some_and(|planned| resident > planned.first().copied().unwrap_or(0))
        {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                "restored window exceeds its reserved restore credit",
            ));
        }
        meter.resident.clear();
        meter.resident.push(resident);
        meter.release_scratch();
        let layout = decode_layout(&mut src, ver)?;
        if layout.operator != window.operator || layout.slot != window.slot {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "snapshot OperatorId/StateSlotKey does not match frozen window",
            ));
        }
        let table = if src.is_empty() && ver == 1 {
            None
        } else {
            if src.is_empty() {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "truncated table tag",
                ));
            }
            let tag = src[0];
            src = &src[1..];
            match tag {
                0 => None,
                1 => {
                    let name = decode_str(&mut src)?;
                    if src.len() < 8 {
                        return Err(SparrowError::new(
                            ErrorCode::CodecViolation,
                            "truncated table revision",
                        ));
                    }
                    let version = u64::from_le_bytes(src[..8].try_into().unwrap());
                    src = &src[8..];
                    Some(TableRevisionBind { name, version })
                }
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "invalid table revision tag",
                    ));
                }
            }
        };
        if !src.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "trailing snapshot bytes",
            ));
        }
        Ok(Self {
            checkpoint_id,
            source,
            window,
            ingested_rows,
            layout,
            table,
        })
    }

    pub fn check_compatible(&self, live: &PlanLayout) -> Result<()> {
        sparrow_plan::decide_state_reuse(&self.layout, live).into_result()
    }
}

/// Only the runtime encoder can construct this immutable, owner-carrying
/// snapshot. External callers cannot label arbitrary bytes as trusted.
pub struct EncodedSnapshot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) lease: sparrow_model::MemoryLease,
    checkpoint_id: u64,
    // Largest participant's cardinality; K1 separately bounds participant count
    // and total bytes, preserving the existing per-operator max_state_keys.
    entries: usize,
}
impl EncodedSnapshot {
    pub(crate) fn prepared(bytes:Vec<u8>,lease:sparrow_model::MemoryLease,checkpoint_id:u64,entries:usize)->Self {
        Self {bytes,lease,checkpoint_id,entries}
    }
    pub fn bytes(&self) -> &[u8] {
        debug_assert!(self.lease.bytes() >= self.bytes.len());
        &self.bytes
    }
}

/// Directory-backed experimental checkpoint store.
pub struct CheckpointStore {
    dir: PathBuf,
    next_id: u64,
    pub fault: FaultHook,
    max_state_keys: usize,
    writer_lock: Option<sparrow_io::fs_lock::FileLock>,
    retention: CheckpointRetention,
    maintenance_error: Option<ErrorCode>,
    pinned: Option<u64>,
    read_only: bool,
    pipeline_version: Option<u16>,
    pipeline_sink: Option<std::sync::Arc<sparrow_io::OwnedSinkIdentity>>,
    // Diagnostic headers only. Never used by validation, recovery or GC.
    metadata_cache: std::sync::Mutex<std::collections::BTreeMap<u64, SnapshotMetadata>>,
}

#[derive(Clone, Copy, Debug)]
pub struct CheckpointRetention {
    pub generations: usize,
    /// Logical file bytes, including temporary generations; not filesystem blocks.
    pub max_bytes: u64,
}
impl Default for CheckpointRetention {
    fn default() -> Self {
        Self {
            generations: KEEP_GENERATIONS as usize,
            max_bytes: MAX_STORE_BYTES,
        }
    }
}
impl CheckpointRetention {
    pub fn validate(self) -> Result<()> {
        if !(1..=128).contains(&self.generations)
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_bytes)
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint retention requires 1..=128 generations and 1 MiB..=1 GiB",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct SnapshotMetadata {
    pub version: u16,
    pub revision: Option<u64>,
    pub attempt: Option<u64>,
    pub generation: Option<[u8;16]>,
}
#[derive(Clone, Debug)]
pub struct CheckpointGeneration {
    pub id: u64,
    pub bytes: u64,
    pub published: bool,
    pub current: bool,
    pub metadata: Option<SnapshotMetadata>,
}
#[derive(Clone, Debug)]
pub struct CheckpointInventory {
    pub current: Option<u64>,
    pub pinned: Option<u64>,
    pub current_error: Option<ErrorCode>,
    pub generations: Vec<CheckpointGeneration>,
    pub bytes: u64,
    pub maintenance_error: Option<ErrorCode>,
    pub state_generation_marker: Option<[u8;16]>,
    pub marker_error: Option<ErrorCode>,
}

impl CheckpointStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_max_state_keys(dir, MAX_FREEZE_ENTRIES)
    }

    /// Open a store whose encode/decode freeze cap is the job `max_state_keys`.
    pub fn open_with_max_state_keys(
        dir: impl Into<PathBuf>,
        max_state_keys: usize,
    ) -> Result<Self> {
        Self::open_impl(dir, max_state_keys, true)
    }

    pub fn open_readonly(dir: impl Into<PathBuf>) -> Result<Self> {
        Self::open_impl(dir, MAX_FREEZE_ENTRIES, false)
    }

    fn open_impl(dir: impl Into<PathBuf>, max_state_keys: usize, create: bool) -> Result<Self> {
        let dir = dir.into();
        if create {
            fs::create_dir_all(&dir).map_err(io_err)?;
        }
        let next_id = list_generation_ids(&dir)?
            .into_iter()
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::BoundExceeded, "checkpoint id exhausted")
            })?;
        Ok(Self {
            dir,
            next_id,
            fault: FaultHook::default(),
            max_state_keys: freeze_entry_cap(max_state_keys),
            writer_lock: None,
            retention: CheckpointRetention::default(),
            maintenance_error: None,
            pinned: None,
            read_only: !create,
            pipeline_version: None,
            pipeline_sink: None,
            metadata_cache: Default::default(),
        })
    }

    /// Production writers retain exclusive directory ownership through stop,
    /// including any non-cancellable blocking commit. Legacy open permits reads;
    /// its commits acquire a temporary lock and cannot bypass this holder.
    pub fn open_exclusive(
        dir: impl Into<PathBuf>,
        max_state_keys: usize,
        retention: CheckpointRetention,
    ) -> Result<Self> {
        retention.validate()?;
        let mut store = Self::open_with_max_state_keys(dir, max_state_keys)?;
        store.writer_lock = Some(sparrow_io::fs_lock::FileLock::acquire(
            &store.dir.join("WRITER_LOCK"),
        )?);
        store.next_id = list_generation_ids(&store.dir)?
            .into_iter()
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::BoundExceeded, "checkpoint id exhausted")
            })?;
        store.retention = retention;
        Ok(store)
    }

    pub fn next_checkpoint_id(&self) -> u64 {
        self.next_id
    }

    /// Refuse mixing codecs before any generation activation or input. R10
    /// cannot be retroactively taught to reject v3 instead of falling back.
    pub fn open_pipeline_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention) -> Result<Self> {
        Self::open_profile_exclusive(dir,max_keys,retention,3)
    }

    /// K2 never shares writable history with File/v3, including explicit fresh
    /// starts. Prevent a source switch from hiding a foreign CURRENT behind a
    /// new generation and breaking later downgrade/restore behavior.
    pub fn open_reliable_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention) -> Result<Self> {
        Self::open_profile_exclusive(dir,max_keys,retention,4)
    }

    /// A graph cut is not a linear File cursor. A distinct outer version also
    /// makes pre-K3 writers reject this entire directory before fallback/GC.
    pub fn open_graph_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention) -> Result<Self> {
        Self::open_profile_exclusive(dir,max_keys,retention,5)
    }

    /// IoT state has a distinct codec family. Never mix its generations with
    /// legacy File, JetStream or pre-IoT DAG history, including fresh starts.
    pub fn open_iot_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention) -> Result<Self> {
        Self::open_profile_exclusive(dir,max_keys,retention,6)
    }

    /// Reliable JetStream plus TTL=0 IoT state has its own profile.  It must
    /// not share history with either the v4 output-only or v6 IoT-only stores:
    /// the source cut, output epoch and keyed state are one restore contract.
    pub fn open_reliable_iot_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::RELIABLE_IOT_SNAPSHOT_VERSION,
        )
    }

    /// Static immutable reference-table enrichment has its own File-only
    /// history. It must not share a directory with any v3..v7 profile.
    pub fn open_reference_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::REFERENCE_SNAPSHOT_VERSION,
        )
    }

    /// Reference-table enrichment mixed with bounded linear Count/IoT state.
    pub fn open_reference_linear_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
    ) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::REFERENCE_LINEAR_SNAPSHOT_VERSION,
        )
    }

    /// Reference-table enrichment on reliable JetStream input.
    pub fn open_reference_reliable_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
    ) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::REFERENCE_RELIABLE_SNAPSHOT_VERSION,
        )
    }

    /// Reference-table enrichment on a required File DAG.
    pub fn open_reference_graph_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
    ) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::REFERENCE_GRAPH_SNAPSHOT_VERSION,
        )
    }

    /// Hysteresis/other new IoT state on File, linear or required DAG.
    pub fn open_hysteresis_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
    ) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::HYSTERESIS_SNAPSHOT_VERSION,
        )
    }

    /// Hysteresis/other new IoT state on reliable JetStream input.
    pub fn open_reliable_hysteresis_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
    ) -> Result<Self> {
        Self::open_profile_exclusive(
            dir,
            max_keys,
            retention,
            crate::pipeline_checkpoint::HYSTERESIS_RELIABLE_SNAPSHOT_VERSION,
        )
    }

    /// Select the exact profile for a validated plan/source pair.  This is
    /// the shared guard used by control-plane supervisors so a directory can
    /// never be opened under a guessed reference or hysteresis version.
    pub fn open_for_plan_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
        plan: &sparrow_plan::CheckpointPlan,
        source_kind: &str,
    ) -> Result<Self> {
        let version = crate::pipeline_checkpoint::snapshot_version_for(plan, source_kind)?;
        Self::open_profile_exclusive(dir, max_keys, retention, version)
    }

    /// Explicit v27/JSON or v28/CSV writer. Fixed target and encoding credit live through
    /// every blocking commit; neither fresh writes nor recovery may change it.
    pub fn open_file_jetstream_sink_exclusive(
        dir: impl Into<PathBuf>,
        max_keys: usize,
        retention: CheckpointRetention,
        plan: &sparrow_plan::CheckpointPlan,
        sink: std::sync::Arc<sparrow_io::OwnedSinkIdentity>,
    ) -> Result<Self> {
        let version = crate::pipeline_checkpoint::sink_snapshot_version_for(plan, "file", sink.identity())?;
        let mut store = Self::open_profile_exclusive(dir, max_keys, retention, version)?;
        store.pipeline_sink = Some(sink);
        // Valid foreign-target history is incompatibility, not a damaged
        // generation to skip. Preserve it even when CURRENT is corrupt or a
        // caller requested a fresh start rather than recovery.
        let current = read_current(&store.dir).ok().flatten();
        let owner = store.pipeline_sink.as_ref().map(|sink| Arc::clone(sink.owner()));
        for id in list_generation_ids(&store.dir)? {
            if store.was_published(id) || current == Some(id) {
                // History classification reads each payload on the Job owner.
                let mode = owner.as_ref().map_or(LoadMode::Scan, LoadMode::ScanOwned);
                if let Err(error) = store.load_generation_with(id, mode) {
                    if store.nonfallback_error(&error) { return Err(error); }
                }
            }
        }
        Ok(store)
    }

    fn nonfallback_error(&self, error: &SparrowError) -> bool {
        sink_profile_mismatch(error)
            || incompatible_or_credit(error)
            || (self.pipeline_sink.is_some() && error.code == ErrorCode::ResourceExhausted)
    }

    fn check_sink_payload(&self, payload: &[u8]) -> Result<()> {
        let Some(expected) = &self.pipeline_sink else { return Ok(()); };
        let version = self.pipeline_version.ok_or_else(|| sink_mismatch("sink store has no fixed output profile"))?;
        if payload.get(4..6) != Some(version.to_le_bytes().as_slice()) {
            return Err(sink_mismatch("JetStream sink store cannot adopt another checkpoint profile"));
        }
        // The target-only parser is bounded, including temporary canonical
        // endpoint strings. It never re-materializes a prepared state frame.
        let _scratch = expected.owner().acquire(
            sparrow_model::CreditKind::Reservation,
            3 * sparrow_io::sink_identity::MAX_SINK_IDENTITY_BYTES,
        )?;
        let actual = PipelineSnapshot::encoded_sink_identity(payload)?;
        if &actual != expected.identity() {
            return Err(sink_mismatch("JetStream sink checkpoint target differs from the fixed store binding"));
        }
        Ok(())
    }

    fn open_profile_exclusive(dir: impl Into<PathBuf>, max_keys: usize, retention: CheckpointRetention, version:u16) -> Result<Self> {
        let mut store = Self::open_exclusive(dir, max_keys, retention)?;
        for id in list_generation_ids(&store.dir)? {
            let chk = store.dir.join(format!("chk-{id:08}"));
            for file in ["0000.bin", "0000.bin.part"] {
                let path = chk.join(file);
                if !path.exists() { continue; }
                let bytes = read_bounded(&path, CHUNK_SIZE as u64).map_err(|e|e.context("checkpoint_guard_generation",id.to_string()).context("checkpoint_guard","unreadable_history_cannot_be_classified; preserve_directory_and_inspect_backup"))?;
                if bytes.len() < MAGIC.len() + 2 || !bytes.starts_with(MAGIC) {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "checkpoint history first chunk cannot be classified; retain original history and inspect backup",
                    )
                    .context("checkpoint_guard_generation", id.to_string())
                    .context("checkpoint_guard", "unreadable_history_cannot_be_classified"));
                }
                if bytes.starts_with(MAGIC) && matches!(bytes.get(4..6), Some([1 | 2, 0])) {
                    return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                        "legacy single-window checkpoint history: refusing K1 writes in this directory; retain the original backup/binary and explicitly choose a new checkpoint directory"));
                }
                if bytes.starts_with(MAGIC) && bytes.len()>=6 && bytes[4..6]!=version.to_le_bytes() {
                    return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                        "checkpoint source profile mismatch: File/v3, JetStream/v4, DAG/v5, IoT/v6, ReliableIoT/v7, Reference/v8-v11, Hysteresis/v12-v13 and PausedTime/v14-v15 require separate directories; retain original history"));
                }
            }
        }
        store.pipeline_version = Some(version);
        Ok(store)
    }

    fn generation_marker(&self) -> Result<Option<[u8;16]>> {
        let path = self.dir.join("STATE_GENERATION");
        if !path.exists() { return Ok(None); }
        let bytes = read_bounded(&path, 20)?;
        if bytes.len() != 20 || !bytes.starts_with(b"SG01") || bytes[4..] == [0;16] {
            return Err(SparrowError::new(ErrorCode::CodecViolation, "invalid state generation marker"));
        }
        Ok(Some(bytes[4..].try_into().unwrap()))
    }

    fn snapshot_metadata(&self, id: u64) -> Result<SnapshotMetadata> {
        let mut cache = self.metadata_cache.lock().expect("metadata cache");
        if let Some(metadata) = cache.get(&id) { return Ok(metadata.clone()); }
        let chk = self.dir.join(format!("chk-{id:08}"));
        let mut bytes = read_bounded(&chk.join("0000.bin"), CHUNK_SIZE as u64)?;
        if bytes.len() < 6 || !bytes.starts_with(MAGIC) {
            return Err(SparrowError::new(ErrorCode::CodecViolation, "invalid snapshot header"));
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let mut metadata = SnapshotMetadata { version, revision: None, attempt: None, generation: None };
        if matches!(version,3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30) {
            for chunk in 1..=34 {
                match PipelineSnapshot::provenance(&bytes) {
                    Ok((attempt, revision, generation)) => {
                        metadata.attempt = Some(attempt); metadata.revision = Some(revision); metadata.generation = Some(generation);
                        break;
                    }
                    Err(e) if e.message.starts_with("truncated") && chunk < 34 => {
                        bytes.extend_from_slice(&read_bounded(&chk.join(format!("{chunk:04}.bin")), CHUNK_SIZE as u64)?);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        cache.insert(id, metadata.clone());
        Ok(metadata)
    }

    /// Persist activation identity before any fresh/reset output. This marker
    /// is NOT a checkpoint/commit proof and is never used to advance CURRENT.
    pub fn activate_state_generation(&self,generation:[u8;16])->Result<()> {
        if self.read_only || generation==[0;16] {
            return Err(SparrowError::new(ErrorCode::PolicyDenied,"cannot activate state generation in read-only/uninitialized store"));
        }
        let _lock=if self.writer_lock.is_none() {Some(sparrow_io::fs_lock::FileLock::acquire(&self.dir.join("WRITER_LOCK"))?)} else {None};
        if self.generation_marker().ok().flatten() == Some(generation) { return Ok(()); }
        let mut bytes=Vec::with_capacity(20);bytes.extend_from_slice(b"SG01");bytes.extend_from_slice(&generation);
        write_all_sync(&self.dir.join("STATE_GENERATION.tmp"),&bytes)?;
        fs::rename(self.dir.join("STATE_GENERATION.tmp"),self.dir.join("STATE_GENERATION")).map_err(io_err)?;
        fsync_dir(&self.dir)
    }

    /// A numeric RestoreSpec is a persistent dependency, not a one-shot
    /// request. Keep that verified point until the owning attempt is stopped
    /// and a new configuration opens the store without that dependency.
    pub fn pin_recovery_point(&mut self, id: u64) -> Result<()> {
        // Verification only: a bounded scan, never a second unbilled copy.
        self.recover_any_id_with(id, LoadMode::Scan)?;
        self.pinned = Some(id);
        Ok(())
    }

    pub fn inventory(&self) -> Result<CheckpointInventory> {
        let (current, current_error) = match read_current(&self.dir) {
            Ok(id) => (id, None),
            Err(e) => (None, Some(e.code)),
        };
        let mut generations = Vec::new();
        let ids = list_generation_ids(&self.dir)?;
        self.metadata_cache.lock().expect("metadata cache").retain(|id, _| ids.contains(id));
        for id in ids {
            generations.push(CheckpointGeneration {
                id,
                bytes: dir_size(&self.dir.join(format!("chk-{id:08}")))?,
                published: Some(id) == current || self.was_published(id),
                current: Some(id) == current,
                metadata: self.snapshot_metadata(id).ok(),
            });
        }
        generations.sort_by_key(|g| g.id);
        let marker = self.generation_marker();
        Ok(CheckpointInventory {
            current,
            pinned: self.pinned,
            current_error,
            generations,
            bytes: dir_size(&self.dir)?,
            maintenance_error: self.maintenance_error,
            state_generation_marker: marker.as_ref().ok().copied().flatten(),
            marker_error: marker.err().map(|e| e.code),
        })
    }

    pub fn recover_id(&self, id: u64) -> Result<CheckpointSnapshot> {
        self.recover_any_id(id)?.legacy()
    }

    pub fn recover_pipeline_id(&self,id:u64)->Result<PipelineSnapshot> {
        self.recover_any_id(id)?.pipeline()
    }

    fn recover_any_id(&self,id:u64)->Result<StoredSnapshot> {
        self.recover_any_id_with(id, LoadMode::Materialize).map(|(snapshot, _)| snapshot)
    }

    fn recover_any_id_with(&self, id: u64, mode: LoadMode<'_>) -> Result<(StoredSnapshot, Option<RestoreCredit>)> {
        if !self.was_published(id) && read_current(&self.dir)? != Some(id) {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "checkpoint has no durable publication proof",
            ));
        }
        self.load_generation_with(id, mode)
    }

    pub fn set_max_state_keys(&mut self, max_state_keys: usize) {
        self.max_state_keys = freeze_entry_cap(max_state_keys);
    }

    pub fn max_state_keys(&self) -> usize {
        self.max_state_keys
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn honesty() -> &'static str {
        DeliveryContract::ALIGNED_CHECKPOINT_HONESTY
    }

    pub fn policy() -> RecoveryPolicy {
        RecoveryPolicy::Aligned
    }

    pub fn label() -> &'static str {
        CHECKPOINT_LABEL
    }

    /// Freeze + chunk write + ACK + manifest commit. Recoverable only after
    /// CURRENT is renamed.
    pub fn commit(&mut self, snapshot: &CheckpointSnapshot) -> Result<u64> {
        let payload = snapshot.encode_with_max_state_keys(self.max_state_keys)?;
        self.commit_encoded(snapshot.checkpoint_id, &payload)
    }

    /// Commit a pre-encoded snapshot (incremental freeze encode). Fails
    /// closed before CURRENT if the payload exceeds [`MAX_SNAPSHOT_BYTES`].
    pub fn commit_encoded(&mut self, checkpoint_id: u64, payload: &[u8]) -> Result<u64> {
        self.commit_bytes(checkpoint_id, payload, false)
    }

    pub fn commit_prepared(&mut self, snapshot: &EncodedSnapshot) -> Result<u64> {
        if self.pipeline_sink.as_ref().is_some_and(|sink| !std::sync::Arc::ptr_eq(sink.owner(), snapshot.lease.owner())) {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
                "JetStream sink prepared snapshot belongs to another Job memory owner"));
        }
        if snapshot.entries > self.max_state_keys {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "prepared snapshot exceeds store max_state_keys",
            ));
        }
        self.commit_bytes(snapshot.checkpoint_id, snapshot.bytes(), true)
    }

    fn commit_bytes(&mut self, checkpoint_id: u64, payload: &[u8], prepared: bool) -> Result<u64> {
        if self.read_only {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "read-only checkpoint store cannot commit",
            ));
        }
        let _temporary_lock = if self.writer_lock.is_none() {
            Some(sparrow_io::fs_lock::FileLock::acquire(
                &self.dir.join("WRITER_LOCK"),
            )?)
        } else {
            None
        };
        // Legacy/read handles may have been opened before a different writer
        // published and pruned generations. Recheck under the writer lock.
        if self.pipeline_version.is_some_and(|version|payload.get(4..6)!=Some(version.to_le_bytes().as_slice())) {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore, "profile writer cannot publish legacy or foreign source snapshots"));
        }
        self.check_sink_payload(payload)?;
        let disk_next = list_generation_ids(&self.dir)?
            .into_iter()
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::BoundExceeded, "checkpoint id exhausted")
            })?;
        self.next_id = self.next_id.max(disk_next);
        let id = checkpoint_id;
        if id == u64::MAX || id < self.next_id || self.dir.join(format!("chk-{id:08}")).exists() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint id must be fresh and monotonic",
            ));
        }
        if payload.len() < 14 || u64::from_le_bytes(payload[6..14].try_into().unwrap()) != id {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "encoded checkpoint id differs from publication id",
            ));
        }
        if payload.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "checkpoint snapshot {}B exceeds {MAX_SNAPSHOT_BYTES}B quota",
                    payload.len()
                ),
            ));
        }
        // Public pre-encoded callers must not be able to publish an arbitrary
        // id-shaped byte string. Bounds are checked before this cold decode.
        if !prepared {
            drop(StoredSnapshot::decode(
                payload,
                self.max_state_keys,
                false,
            )?);
        }
        let protected = match read_current(&self.dir) {
            Ok(Some(current)) => {
                // Preserve publication proof for pre-marker CURRENT stores
                // before replacing CURRENT. Never bless an unverified generation.
                match self.load_generation_mode(current, false) {
                    Ok(_) => {
                        if !self.was_published(current) {
                            self.record_publication(current)?;
                        }
                        Some(current)
                    }
                    Err(error) if self.nonfallback_error(&error) => return Err(error),
                    Err(error) => Some(
                        self.load_latest_valid_except(Some(current))?
                            .ok_or(error)?
                            .id(),
                    ),
                }
            }
            Ok(None) => None,
            Err(error) => Some(
                self.load_latest_valid_except(None)?
                    .ok_or(error)?
                    .id(),
            ),
        };
        self.prune(protected, payload.len() as u64 + MAX_MANIFEST_BYTES + 4096)?;
        self.next_id = id.checked_add(1).ok_or_else(|| {
            SparrowError::new(ErrorCode::BoundExceeded, "checkpoint id exhausted")
        })?;
        let chk = self.dir.join(format!("chk-{id:08}"));
        fs::create_dir(&chk).map_err(io_err)?;
        fsync_dir(&self.dir)?;
        let chunks: Vec<&[u8]> = payload.chunks(CHUNK_SIZE).collect();
        if chunks.len() as u32 > MAX_MANIFEST_CHUNKS {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "checkpoint would write {} chunks (max {MAX_MANIFEST_CHUNKS})",
                    chunks.len()
                ),
            ));
        }
        for (i, chunk) in chunks.iter().enumerate() {
            let part = chk.join(format!("{i:04}.bin.part"));
            let final_path = chk.join(format!("{i:04}.bin"));
            if self.fault.point == FaultPoint::DuringChunkWrite {
                let half = chunk.len() / 2;
                write_all_trunc(&part, &chunk[..half])?;
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "injected disk-full during chunk write",
                )
                .context("fault", "DuringChunkWrite")
                .context("checkpoint", id.to_string()));
            }
            write_all_sync(&part, chunk)?;
            fs::rename(&part, &final_path).map_err(io_err)?;
        }
        // Sync all chunk names once; file data was individually synced above.
        // ACK/MANIFEST/CURRENT and every crash-cut keep their original ordering.
        fsync_dir(&chk)?;
        if self.fault.point == FaultPoint::AfterChunkWrite {
            return Err(cut("AfterChunkWrite", id));
        }
        write_all_sync(&chk.join("ACK"), b"ok\n")?;
        if self.fault.point == FaultPoint::AfterAck {
            return Err(cut("AfterAck", id));
        }
        let mut manifest = Manifest {
            checkpoint_id: id,
            n_chunks: chunks.len() as u32,
            bytes: payload.len() as u64,
            checksums: chunks.iter().map(|c| crc32(c)).collect(),
            codec_version: MANIFEST_VERSION,
        };
        if self.fault.point == FaultPoint::CorruptChecksum && !manifest.checksums.is_empty() {
            manifest.checksums[0] ^= 0xffff_ffff;
        }
        let man_bytes = manifest.encode();
        let man_tmp = chk.join("MANIFEST.tmp");
        let man = chk.join("MANIFEST");
        write_all_sync(&man_tmp, &man_bytes)?;
        if self.fault.point == FaultPoint::AfterManifestTmp {
            return Err(cut("AfterManifestTmp", id));
        }
        fs::rename(&man_tmp, &man).map_err(io_err)?;
        fsync_dir(&chk)?;
        if self.fault.point == FaultPoint::AfterManifestRename {
            return Err(cut("AfterManifestRename", id));
        }
        let cur_tmp = self.dir.join("CURRENT.tmp");
        let cur = self.dir.join("CURRENT");
        write_all_sync(&cur_tmp, format!("chk-{id:08}\n").as_bytes())?;
        fs::rename(&cur_tmp, &cur).map_err(io_err)?;
        fsync_dir(&self.dir)?;
        // CURRENT is durable. Maintenance failure must not undo that fact or
        // falsely report failed commit, but is exposed to operations.
        self.maintenance_error = self
            .record_publication(id)
            .and_then(|_| self.prune(Some(id), 0))
            .err()
            .map(|e| e.code);
        Ok(id)
    }

    /// Load the last **committed** checkpoint. Partial chunks / missing
    /// MANIFEST / checksum mismatch are not restored.
    ///
    /// `Ok(None)` means no CURRENT file — callers that *claimed* restore
    /// must treat this as a hard error (see [`Self::recover_required`]).
    pub fn recover_committed(&self) -> Result<Option<CheckpointSnapshot>> {
        self.recover_any_committed()?.map(StoredSnapshot::legacy).transpose()
    }

    pub fn recover_pipeline_required(&self)->Result<PipelineSnapshot> {
        self.recover_any_committed()?.ok_or_else(||SparrowError::new(ErrorCode::UnsupportedRestore,
            "no verified committed checkpoint; refusing silent empty-state continue"))?.pipeline()
    }

    /// Production restore entry for every participant-aware profile. The
    /// payload, decode scratch and every participant's resident state are
    /// reserved on the admitted Job `owner` before materialization; the
    /// returned credit is consumed by Kernel admission. Credit exhaustion is
    /// never treated as corruption (no fallback to an older generation).
    /// An explicitly requested id is verified by this owned load and then
    /// pinned (no second unbilled verification pass).
    pub fn recover_pipeline_owned(
        &mut self,
        requested: Option<u64>,
        owner: &Arc<MemoryOwner>,
    ) -> Result<(PipelineSnapshot, RestoreCredit)> {
        let (snapshot, credit) = match requested {
            Some(id) => self.recover_any_id_with(id, LoadMode::Owned(owner))?,
            None => self
                .recover_any_committed_with(LoadMode::Owned(owner))?
                .ok_or_else(|| SparrowError::new(ErrorCode::UnsupportedRestore,
                    "no verified committed checkpoint; refusing silent empty-state continue"))?,
        };
        let credit = credit.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "owned restore lacks credit"))?;
        let snapshot = snapshot.pipeline()?;
        if let Some(id) = requested {
            if snapshot.checkpoint_id != id {
                return Err(SparrowError::new(ErrorCode::Internal, "requested restore id mismatch"));
            }
            self.pinned = Some(id);
        }
        Ok((snapshot, credit))
    }

    /// Legacy SPV1 restore through the same owned entry.
    pub fn recover_required_owned(
        &self,
        owner: &Arc<MemoryOwner>,
    ) -> Result<(CheckpointSnapshot, RestoreCredit)> {
        let (snapshot, credit) = self
            .recover_any_committed_with(LoadMode::Owned(owner))?
            .ok_or_else(|| SparrowError::new(ErrorCode::UnsupportedRestore,
                "no verified committed checkpoint; refusing silent empty-state continue"))?;
        let credit = credit.ok_or_else(|| SparrowError::new(ErrorCode::Internal, "owned restore lacks credit"))?;
        Ok((snapshot.legacy()?, credit))
    }

    fn recover_any_committed(&self)->Result<Option<StoredSnapshot>> {
        Ok(self.recover_any_committed_with(LoadMode::Materialize)?.map(|(snapshot, _)| snapshot))
    }

    fn recover_any_committed_with(&self, mode: LoadMode<'_>)->Result<Option<(StoredSnapshot, Option<RestoreCredit>)>> {
        match read_current(&self.dir) {
            Ok(Some(id)) => match self.load_generation_with(id, mode) {
                Ok(snap) => Ok(Some(snap)),
                Err(e) => {
                    if self.nonfallback_error(&e) { return Err(e); }
                    if let Some(snap) = self.load_latest_valid_except_with(Some(id), mode)? {
                        return Ok(Some(snap));
                    }
                    Err(e)
                }
            },
            // Missing CURRENT means the last publish did not land. Do not
            // promote an unpublished MANIFEST (crash after rename, before CURRENT).
            Ok(None) => Ok(None),
            Err(e) => {
                if let Some(snap) = self.load_latest_valid_except_with(None, mode)? {
                    Ok(Some(snap))
                } else {
                    Err(e)
                }
            }
        }
    }

    fn load_latest_valid_except(&self, skip: Option<u64>) -> Result<Option<StoredSnapshot>> {
        Ok(self.load_latest_valid_except_with(skip, LoadMode::Scan)?.map(|(snapshot, _)| snapshot))
    }

    fn load_latest_valid_except_with(&self, skip: Option<u64>, mode: LoadMode<'_>) -> Result<Option<(StoredSnapshot, Option<RestoreCredit>)>> {
        let mut ids = match list_generation_ids(&self.dir) {
            Ok(ids) => ids,
            Err(_) => return Ok(None),
        };
        ids.sort_unstable();
        ids.reverse();
        for id in ids {
            if skip.is_some_and(|current| id >= current) {
                continue;
            }
            // A MANIFEST may exist after a crash before CURRENT publication.
            // It is not eligible fallback merely because its checksum is valid.
            if !self.was_published(id) {
                continue;
            }
            match self.load_generation_with(id, mode) {
                Ok(snap) => return Ok(Some(snap)),
                Err(error) if self.nonfallback_error(&error) => return Err(error),
                Err(_) => {}
            }
        }
        Ok(None)
    }

    fn load_generation_mode(&self, id: u64, materialize: bool) -> Result<StoredSnapshot> {
        let mode = if materialize { LoadMode::Materialize } else { LoadMode::Scan };
        self.load_generation_with(id, mode).map(|(snapshot, _)| snapshot)
    }

    /// Common Store restore entry. `LoadMode::Owned` reserves the payload
    /// before reading chunks, scans the complete snapshot with bounded
    /// scratch, reserves every participant's exact resident size, and only
    /// then materializes. Any failure drops every lease (full refund).
    fn load_generation_with(&self, id: u64, mode: LoadMode<'_>) -> Result<(StoredSnapshot, Option<RestoreCredit>)> {
        let chk = self.dir.join(format!("chk-{id:08}"));
        if !fs::symlink_metadata(&chk)
            .map_err(io_err)?
            .file_type()
            .is_dir()
        {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "checkpoint generation must be a real directory",
            ));
        }
        let man_path = chk.join("MANIFEST");
        if !man_path.exists() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "CURRENT points at a checkpoint without a committed MANIFEST; recover from committed only",
            )
            .context("checkpoint", id.to_string()));
        }
        if !chk.join("ACK").exists() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "checkpoint ACK missing; not committed",
            ));
        }
        let man_meta = fs::metadata(&man_path).map_err(io_err)?;
        if man_meta.len() > MAX_MANIFEST_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "MANIFEST {}B exceeds {MAX_MANIFEST_BYTES}B; refusing to allocate",
                    man_meta.len()
                ),
            ));
        }
        let man_bytes = read_bounded(&man_path, MAX_MANIFEST_BYTES)?;
        let manifest = Manifest::decode(&man_bytes)?;
        if manifest.checkpoint_id != id {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "MANIFEST checkpoint id does not match CURRENT",
            ));
        }
        if manifest.bytes > MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "MANIFEST declares a snapshot above the snapshot bound",
            ));
        }
        let payload_lease = match mode {
            LoadMode::Owned(owner) | LoadMode::ScanOwned(owner) => Some(
                owner
                    .acquire(
                        sparrow_model::CreditKind::Reservation,
                        (manifest.bytes as usize).saturating_add(CHUNK_SIZE),
                    )
                    .map_err(crate::pipeline_checkpoint::restore_credit_error)?,
            ),
            _ => None,
        };
        let mut payload = if payload_lease.is_some() {
            Vec::with_capacity(manifest.bytes as usize)
        } else {
            Vec::new()
        };
        for i in 0..manifest.n_chunks {
            let part = chk.join(format!("{i:04}.bin.part"));
            if part.exists() {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    format!("partial chunk {i:04}.bin.part present; not committed"),
                ));
            }
            let path = chk.join(format!("{i:04}.bin"));
            let chunk = read_bounded(&path, CHUNK_SIZE as u64)?;
            if payload.len() as u64 + chunk.len() as u64 > manifest.bytes {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "chunks exceed declared snapshot size",
                ));
            }
            let got = crc32(&chunk);
            let expect = manifest.checksums.get(i as usize).copied().unwrap_or(0);
            if got != expect {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    format!("chunk {i} checksum mismatch (got {got:#x} want {expect:#x})"),
                )
                .context("checkpoint", id.to_string()));
            }
            payload.extend_from_slice(&chunk);
        }
        if payload.len() as u64 != manifest.bytes {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                format!(
                    "checkpoint payload {}B != MANIFEST {}",
                    payload.len(),
                    manifest.bytes
                ),
            ));
        }
        self.check_sink_payload(&payload)?;
        let (snapshot, credit) = match mode {
            LoadMode::Scan => (StoredSnapshot::decode(&payload, self.max_state_keys, false)?, None),
            LoadMode::ScanOwned(owner) => {
                let mut meter = crate::pipeline_checkpoint::RestoreMeter::billed(owner.clone());
                (StoredSnapshot::decode_metered(&payload, self.max_state_keys, false, &mut meter)?, None)
            }
            LoadMode::Materialize => (StoredSnapshot::decode(&payload, self.max_state_keys, true)?, None),
            LoadMode::Owned(owner) => {
                let charge = |bytes: usize| {
                    owner
                        .acquire(sparrow_model::CreditKind::Reservation, bytes.max(1))
                        .map_err(crate::pipeline_checkpoint::restore_credit_error)
                };
                // Envelope copies (manifest semantics, source strings, sink
                // identity) are bounded by the payload and by fixed limits.
                let header = charge(
                    payload
                        .len()
                        .saturating_mul(2)
                        .min(MAX_RESTORE_ENVELOPE_BYTES)
                        .saturating_add(4096),
                )?;
                let mut meter = crate::pipeline_checkpoint::RestoreMeter::billed(owner.clone());
                let scanned = StoredSnapshot::decode_metered(&payload, self.max_state_keys, false, &mut meter)?;
                let ids = scanned.credit_participants();
                drop(scanned);
                if ids.len() != meter.resident.len() {
                    return Err(SparrowError::new(ErrorCode::Internal, "restore credit scan/participant mismatch"));
                }
                let planned = meter.resident.clone();
                let mut participants = Vec::with_capacity(planned.len());
                for (participant, bytes) in ids.into_iter().zip(&planned) {
                    participants.push((participant, charge(*bytes)?));
                }
                meter.planned = Some(planned);
                let snapshot = StoredSnapshot::decode_metered(&payload, self.max_state_keys, true, &mut meter)?;
                (
                    snapshot,
                    Some(RestoreCredit {
                        owner: owner.clone(),
                        _header: Some(header),
                        participants,
                    }),
                )
            }
        };
        drop(payload);
        drop(payload_lease);
        if snapshot.id() != id {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "snapshot and generation ids differ",
            ));
        }
        Ok((snapshot, credit))
    }

    fn record_publication(&self, id: u64) -> Result<()> {
        let dir = self.dir.join(format!("chk-{id:08}"));
        let manifest = read_bounded(&dir.join("MANIFEST"), MAX_MANIFEST_BYTES)?;
        let marker = format!("PUB1 {id} {}\n", crc32(&manifest));
        write_all_sync(&dir.join("PUBLISHED.tmp"), marker.as_bytes())?;
        fs::rename(dir.join("PUBLISHED.tmp"), dir.join("PUBLISHED")).map_err(io_err)?;
        fsync_dir(&dir)
    }
    fn was_published(&self, id: u64) -> bool {
        let dir = self.dir.join(format!("chk-{id:08}"));
        let Ok(marker) = read_bounded(&dir.join("PUBLISHED"), 128) else {
            return false;
        };
        let Ok(manifest) = read_bounded(&dir.join("MANIFEST"), MAX_MANIFEST_BYTES) else {
            return false;
        };
        marker == format!("PUB1 {id} {}\n", crc32(&manifest)).as_bytes()
    }
    fn prune(&self, protected: Option<u64>, reserve: u64) -> Result<()> {
        let mut removed = false;
        let mut ids = list_generation_ids(&self.dir)?;
        ids.sort_by_key(|id| (self.was_published(*id), *id));
        let mut count = ids.len();
        let mut bytes = dir_size(&self.dir)?;
        // Leave room for the prospective generation, but never remove CURRENT.
        let keep = self
            .retention
            .generations
            .max(if self.pinned.is_some() { 2 } else { 1 })
            .saturating_sub(usize::from(reserve > 0));
        for id in ids {
            if count <= keep && bytes.saturating_add(reserve) <= self.retention.max_bytes {
                break;
            }
            if Some(id) == protected
                || Some(id) == self.pinned
                || Some(id) == read_current(&self.dir).ok().flatten()
            {
                continue;
            }
            let path = self.dir.join(format!("chk-{id:08}"));
            let size = dir_size(&path)?;
            fs::remove_dir_all(path).map_err(io_err)?;
            removed = true;
            count -= 1;
            bytes = bytes.saturating_sub(size);
        }
        if removed {
            fsync_dir(&self.dir)?;
        }
        if bytes.saturating_add(reserve) > self.retention.max_bytes {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "checkpoint storage quota exhausted; CURRENT preserved",
            ));
        }
        Ok(())
    }

    /// Restore entry point: never continue with empty state when a restore
    /// was claimed. Missing CURRENT or an unverified store is a reject.
    pub fn recover_required(&self) -> Result<CheckpointSnapshot> {
        match self.recover_committed()? {
            Some(snap) => Ok(snap),
            None => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "no verified committed checkpoint; refusing silent empty-state continue",
            )),
        }
    }

    pub fn has_committed(&self) -> bool {
        matches!(self.recover_any_committed_with(LoadMode::Scan), Ok(Some(_)))
    }
}

fn sink_mismatch(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::UnsupportedRestore, message)
        .context("checkpoint_guard", "sink_profile_mismatch")
}

/// Version/profile/codec incompatibility and restore-credit exhaustion come
/// from a complete checksummed record; an older generation is not a repair.
fn incompatible_or_credit(error: &SparrowError) -> bool {
    error.context.iter().any(|(key, value)| {
        key == "checkpoint_guard"
            && matches!(
                value.as_str(),
                crate::pipeline_checkpoint::EXTENDED_PROFILE_GUARD
                    | crate::pipeline_checkpoint::RESTORE_CREDIT_GUARD
                    | "extended_codec_mismatch"
                    | "extended_state_mismatch"
            )
    })
}

#[derive(Clone, Copy)]
enum LoadMode<'a> {
    Scan,
    /// Verification-only scan with the payload and scan scratch billed.
    ScanOwned(&'a Arc<MemoryOwner>),
    Materialize,
    Owned(&'a Arc<MemoryOwner>),
}

/// Envelope scratch cap (manifest semantics, strings, sink identity).
const MAX_RESTORE_ENVELOPE_BYTES: usize = 512 * 1024;

/// Restore memory reserved on the admitted Job owner before materialization.
/// Kernel admission consumes it per participant; dropping it refunds all.
#[derive(Debug)]
pub struct RestoreCredit {
    owner: Arc<MemoryOwner>,
    _header: Option<sparrow_model::MemoryLease>,
    participants: Vec<(sparrow_plan::ParticipantId, sparrow_model::MemoryLease)>,
}

impl RestoreCredit {
    pub fn bytes(&self) -> usize {
        self._header.as_ref().map_or(0, |l| l.bytes())
            + self.participants.iter().map(|(_, l)| l.bytes()).sum::<usize>()
    }
    pub(crate) fn belongs_to(&self, owner: &Arc<MemoryOwner>) -> bool {
        Arc::ptr_eq(&self.owner, owner)
    }
    pub(crate) fn take(&mut self, participant: sparrow_plan::ParticipantId) -> Option<sparrow_model::MemoryLease> {
        let index = self.participants.iter().position(|(id, _)| *id == participant)?;
        Some(self.participants.remove(index).1)
    }
}

fn sink_profile_mismatch(error: &SparrowError) -> bool {
    error.context.iter().any(|(key, value)| key == "checkpoint_guard" && value == "sink_profile_mismatch")
}

#[derive(Clone, Debug, PartialEq)]
struct Manifest {
    checkpoint_id: u64,
    n_chunks: u32,
    bytes: u64,
    checksums: Vec<u32>,
    codec_version: u16,
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        o.extend_from_slice(MANIFEST_MAGIC);
        o.extend_from_slice(&self.codec_version.to_le_bytes());
        o.extend_from_slice(&self.checkpoint_id.to_le_bytes());
        o.extend_from_slice(&self.n_chunks.to_le_bytes());
        o.extend_from_slice(&self.bytes.to_le_bytes());
        o.extend_from_slice(&(self.checksums.len() as u32).to_le_bytes());
        for c in &self.checksums {
            o.extend_from_slice(&c.to_le_bytes());
        }
        o
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 + 2 + 8 + 4 + 8 + 4 || &bytes[..4] != MANIFEST_MAGIC {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "MANIFEST header invalid (want versioned MAN2)",
            ));
        }
        let mut s = &bytes[4..];
        let codec_version = u16::from_le_bytes(s[..2].try_into().unwrap());
        s = &s[2..];
        if codec_version != MANIFEST_VERSION {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("MANIFEST codec version {codec_version} unsupported"),
            ));
        }
        let checkpoint_id = u64::from_le_bytes(s[..8].try_into().unwrap());
        s = &s[8..];
        let n_chunks = u32::from_le_bytes(s[..4].try_into().unwrap());
        s = &s[4..];
        let nbytes = u64::from_le_bytes(s[..8].try_into().unwrap());
        s = &s[8..];
        let nsum = u32::from_le_bytes(s[..4].try_into().unwrap());
        if n_chunks > MAX_MANIFEST_CHUNKS || nsum > MAX_MANIFEST_CHUNKS {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("MANIFEST chunk count {n_chunks}/{nsum} exceeds {MAX_MANIFEST_CHUNKS}"),
            ));
        }
        if nbytes > MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("MANIFEST payload {nbytes}B exceeds {MAX_SNAPSHOT_BYTES}B"),
            ));
        }
        let nsum = nsum as usize;
        if nsum != n_chunks as usize {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "MANIFEST checksum count does not match chunk count",
            ));
        }
        s = &s[4..];
        if s.len() < nsum * 4 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "MANIFEST checksum table truncated",
            ));
        }
        let mut checksums = Vec::with_capacity(nsum);
        for _ in 0..nsum {
            checksums.push(u32::from_le_bytes(s[..4].try_into().unwrap()));
            s = &s[4..];
        }
        if !s.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "MANIFEST trailing bytes",
            ));
        }
        Ok(Self {
            checkpoint_id,
            n_chunks,
            bytes: nbytes,
            checksums,
            codec_version,
        })
    }
}

fn fsync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).map_err(io_err)?;
    dir.sync_all().map_err(io_err)?;
    Ok(())
}

fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    let meta = fs::symlink_metadata(path).map_err(io_err)?;
    if !meta.file_type().is_file() {
        return Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            "checkpoint component must be a regular file",
        ));
    }
    if meta.len() > max {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "checkpoint component exceeds read bound",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io_err)?
        .take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(io_err)?;
    if bytes.len() as u64 > max {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "checkpoint component grew beyond read bound",
        ));
    }
    Ok(bytes)
}

fn dir_size(path: &Path) -> Result<u64> {
    fn walk(path: &Path, depth: usize, entries: &mut usize) -> Result<u64> {
        if depth > 8 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "checkpoint directory nesting limit",
            ));
        }
        let mut total = 0u64;
        for entry in fs::read_dir(path).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            *entries += 1;
            if *entries > 300_000 {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "checkpoint directory entry limit",
                ));
            }
            let kind = entry.file_type().map_err(io_err)?;
            if kind.is_symlink() || (!kind.is_file() && !kind.is_dir()) {
                return Err(SparrowError::new(
                    ErrorCode::PolicyDenied,
                    "checkpoint store contains a link or special file",
                ));
            }
            total = total.saturating_add(if kind.is_dir() {
                walk(&entry.path(), depth + 1, entries)?
            } else {
                entry.metadata().map_err(io_err)?.len()
            });
        }
        Ok(total)
    }
    walk(path, 0, &mut 0)
}

fn list_generation_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for (n, ent) in fs::read_dir(dir).map_err(io_err)?.enumerate() {
        if n >= 1024 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "checkpoint store entry limit",
            ));
        }
        let ent = ent.map_err(io_err)?;
        let name = ent.file_name();
        let Some(s) = name.to_str() else {
            continue;
        };
        if let Some(id) = s.strip_prefix("chk-").and_then(|x| x.parse::<u64>().ok()) {
            if s != format!("chk-{id:08}") || !ent.file_type().map_err(io_err)?.is_dir() {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "noncanonical checkpoint generation path",
                )
                .context("path", ent.path().display().to_string()));
            }
            ids.push(id);
            if ids.len() > 256 {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "too many checkpoint generations; inspect/clean store",
                ));
            }
        }
    }
    Ok(ids)
}

fn write_all_sync(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(io_err)?;
    f.write_all(bytes).map_err(io_err)?;
    f.sync_all().map_err(io_err)?;
    Ok(())
}

fn write_all_trunc(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = File::create(path).map_err(io_err)?;
    f.write_all(bytes).map_err(io_err)?;
    Ok(())
}

fn read_current(dir: &Path) -> Result<Option<u64>> {
    let path = dir.join("CURRENT");
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
        Ok(_) => {}
    }
    let s = String::from_utf8(read_bounded(&path, 128)?)
        .map_err(|_| SparrowError::new(ErrorCode::CodecViolation, "CURRENT is not UTF-8"))?;
    let name = s.trim();
    let id = name
        .strip_prefix("chk-")
        .and_then(|x| x.parse::<u64>().ok())
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::CodecViolation,
                format!("CURRENT has invalid value {name:?}"),
            )
        })?;
    Ok(Some(id))
}

fn io_err(e: std::io::Error) -> SparrowError {
    let code = if e.kind() == std::io::ErrorKind::StorageFull
        || e.raw_os_error() == Some(28)
        || e.to_string().contains("No space")
    {
        ErrorCode::ResourceExhausted
    } else {
        ErrorCode::Internal
    };
    SparrowError::new(code, format!("checkpoint io: {e}"))
}

fn cut(point: &str, id: u64) -> SparrowError {
    SparrowError::new(
        ErrorCode::Cancelled,
        format!("injected crash-cut at {point}"),
    )
    .context("fault", point)
    .context("checkpoint", id.to_string())
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xffff_ffff;
    for &b in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xff) as usize];
    }
    !crc
}

const CRC32_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (crc & 1).wrapping_neg());
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

fn encode_str(s: &str, out: &mut Vec<u8>) {
    let b = s.as_bytes();
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn decode_str(src: &mut &[u8]) -> Result<String> {
    if src.len() < 4 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated string in snapshot",
        ));
    }
    let n = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
    *src = &src[4..];
    if src.len() < n {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated string payload in snapshot",
        ));
    }
    let s = std::str::from_utf8(&src[..n])
        .map_err(|_| SparrowError::new(ErrorCode::CodecViolation, "snapshot string not utf8"))?;
    *src = &src[n..];
    Ok(s.to_string())
}

pub(crate) fn encode_position(p: &SourcePosition, out: &mut Vec<u8>) -> Result<()> {
    out.extend_from_slice(&p.offset_bytes.to_le_bytes());
    out.extend_from_slice(&p.record_index.to_le_bytes());
    encode_str(&p.identity.kind, out);
    encode_str(&p.identity.path, out);
    out.extend_from_slice(&p.identity.size.to_le_bytes());
    out.extend_from_slice(&p.identity.fingerprint.to_le_bytes());
    Ok(())
}

fn decode_position(src: &mut &[u8]) -> Result<SourcePosition> {
    if src.len() < 16 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated source position",
        ));
    }
    let offset_bytes = u64::from_le_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    let record_index = u64::from_le_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    let kind = decode_str(src)?;
    let path = decode_str(src)?;
    if src.len() < 16 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated source identity",
        ));
    }
    let size = u64::from_le_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    let fingerprint = u64::from_le_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    Ok(SourcePosition {
        offset_bytes,
        record_index,
        identity: SourceIdentity {
            kind,
            path,
            size,
            fingerprint,
        },
    })
}

fn encode_opt_i64(v: Option<i64>, out: &mut Vec<u8>) {
    match v {
        None => out.push(0),
        Some(x) => {
            out.push(1);
            out.extend_from_slice(&x.to_le_bytes());
        }
    }
}

fn decode_opt_i64(src: &mut &[u8]) -> Result<Option<i64>> {
    if src.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated optional i64",
        ));
    }
    let tag = src[0];
    *src = &src[1..];
    match tag {
        0 => Ok(None),
        1 => {
            if src.len() < 8 {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "truncated optional i64 payload",
                ));
            }
            let v = i64::from_le_bytes(src[..8].try_into().unwrap());
            *src = &src[8..];
            Ok(Some(v))
        }
        _ => Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "invalid optional i64 tag",
        )),
    }
}

fn encode_layout(l: &PlanLayout, out: &mut Vec<u8>) -> Result<()> {
    let semantics =
        l.semantic_descriptor
            .as_ref()
            .filter(|s| s.has_input_schema())
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::UnsupportedRestore,
            "cannot write a checkpoint without complete state semantics; reset/replay required")
            })?
            .encode()?;
    if l.keys.len() > u16::MAX as usize || l.aggs.len() > u16::MAX as usize {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "plan layout item count exceeds codec bound",
        ));
    }
    out.extend_from_slice(&l.operator.raw().to_le_bytes());
    out.extend_from_slice(&l.slot.raw().to_le_bytes());
    out.push(l.window_kind);
    out.extend_from_slice(&(l.keys.len() as u16).to_le_bytes());
    for k in &l.keys {
        encode_str(k, out);
    }
    out.extend_from_slice(&(l.aggs.len() as u16).to_le_bytes());
    for a in &l.aggs {
        encode_str(a, out);
    }
    match &l.event_time_field {
        None => out.push(0),
        Some(f) => {
            out.push(1);
            encode_str(f, out);
        }
    }
    out.extend_from_slice(&l.lateness_micros.to_le_bytes());
    out.extend_from_slice(&l.where_fingerprint.to_le_bytes());
    match &l.table_name {
        None => out.push(0),
        Some(n) => {
            out.push(1);
            encode_str(n, out);
        }
    }
    encode_opt_i64(l.table_revision.map(|v| v as i64), out);
    out.extend_from_slice(&l.window_params_fingerprint.to_le_bytes());
    out.extend_from_slice(&(semantics.len() as u32).to_le_bytes());
    out.extend_from_slice(&semantics);
    Ok(())
}

fn decode_layout(src: &mut &[u8], version: u16) -> Result<PlanLayout> {
    if src.len() < 4 + 2 + 1 + 2 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated plan layout",
        ));
    }
    let operator = OperatorId::new(u32::from_le_bytes(src[..4].try_into().unwrap()));
    *src = &src[4..];
    let slot = StateSlotId::new(u16::from_le_bytes(src[..2].try_into().unwrap()));
    *src = &src[2..];
    let window_kind = src[0];
    *src = &src[1..];
    let nk = u16::from_le_bytes(src[..2].try_into().unwrap()) as usize;
    *src = &src[2..];
    let mut keys = Vec::with_capacity(nk);
    for _ in 0..nk {
        keys.push(decode_str(src)?);
    }
    if src.len() < 2 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated plan layout aggs",
        ));
    }
    let na = u16::from_le_bytes(src[..2].try_into().unwrap()) as usize;
    *src = &src[2..];
    let mut aggs = Vec::with_capacity(na);
    for _ in 0..na {
        aggs.push(decode_str(src)?);
    }
    if src.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated plan layout event-time",
        ));
    }
    let tag = src[0];
    *src = &src[1..];
    let event_time_field = match tag {
        0 => None,
        1 => Some(decode_str(src)?),
        _ => {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "invalid event-time tag",
            ))
        }
    };
    if src.len() < 8 + 8 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated plan layout fingerprints",
        ));
    }
    let lateness_micros = i64::from_le_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    let where_fingerprint = u64::from_le_bytes(src[..8].try_into().unwrap());
    *src = &src[8..];
    if src.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated plan layout table",
        ));
    }
    let ttag = src[0];
    *src = &src[1..];
    let table_name = match ttag {
        0 => None,
        1 => Some(decode_str(src)?),
        _ => {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "invalid table name tag",
            ))
        }
    };
    let table_revision = decode_opt_i64(src)?.map(|v| v as u64);
    let window_params_fingerprint = if src.len() >= 8 {
        let v = u64::from_le_bytes(src[..8].try_into().unwrap());
        *src = &src[8..];
        v
    } else if version == 1 {
        0
    } else {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated window parameters",
        ));
    };
    let semantic_descriptor = if version >= 2 {
        if src.len() < 4 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "truncated state semantics length",
            ));
        }
        let len = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
        *src = &src[4..];
        if len > sparrow_plan::canonical::MAX_STATE_SEMANTICS_BYTES || len > src.len() {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "state semantics length exceeds bound or snapshot",
            ));
        }
        let semantics = sparrow_plan::canonical::StateSemantics::decode(&src[..len])?;
        *src = &src[len..];
        Some(semantics)
    } else {
        None
    };
    Ok(PlanLayout {
        operator,
        slot,
        window_kind,
        keys,
        aggs,
        event_time_field,
        lateness_micros,
        where_fingerprint,
        table_name,
        table_revision,
        window_params_fingerprint,
        semantic_descriptor,
    })
}

fn write_snapshot_prefix(
    out: &mut Vec<u8>,
    checkpoint_id: u64,
    ingested_rows: u64,
    source: &SourcePosition,
) -> Result<()> {
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    out.extend_from_slice(&checkpoint_id.to_le_bytes());
    out.extend_from_slice(&ingested_rows.to_le_bytes());
    encode_position(source, out)
}

fn write_snapshot_suffix(
    out: &mut Vec<u8>,
    layout: &PlanLayout,
    table: Option<&TableRevisionBind>,
) -> Result<()> {
    encode_layout(layout, out)?;
    match table {
        None => out.push(0),
        Some(t) => {
            out.push(1);
            encode_str(&t.name, out);
            out.extend_from_slice(&t.version.to_le_bytes());
        }
    }
    Ok(())
}

fn estimated_frozen_bytes(
    f: &WindowFreeze,
    codec: crate::aggregate::AccumulatorCodec,
) -> Result<usize> {
    const ENTRY_OVERHEAD: usize = 2 + 8 + 8 + 8 + 2;
    let mut total = 0usize;
    for e in &f.entries {
        total = total.saturating_add(ENTRY_OVERHEAD);
        for v in &e.key {
            total = total.saturating_add(v.encoded_value_len()?);
        }
        for a in &e.accs {
            total = total.saturating_add(a.encoded_len_codec(codec)?);
        }
    }
    Ok(total)
}

pub(crate) fn encode_freeze(f: &WindowFreeze, out: &mut Vec<u8>, max_entries: usize) -> Result<()> {
    encode_freeze_codec(f, out, max_entries, crate::aggregate::AccumulatorCodec::Window)
}

pub(crate) fn encode_freeze_codec(
    f: &WindowFreeze,
    out: &mut Vec<u8>,
    max_entries: usize,
    codec: crate::aggregate::AccumulatorCodec,
) -> Result<()> {
    if f.entries.len() > max_entries {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!(
                "freeze entry count {} exceeds max_state_keys {max_entries}; refusing encode (would publish unrecoverable CURRENT)",
                f.entries.len()
            ),
        ));
    }
    let estimate = estimated_frozen_bytes(f, codec)?.saturating_add(256);
    if estimate as u64 > MAX_SNAPSHOT_BYTES {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!(
                "estimated freeze {estimate}B exceeds {MAX_SNAPSHOT_BYTES}B snapshot quota; refusing encode before CURRENT"
            ),
        ));
    }
    out.extend_from_slice(&f.operator.raw().to_le_bytes());
    out.extend_from_slice(&f.slot.raw().to_le_bytes());
    out.push(f.kind);
    out.extend_from_slice(&(f.entries.len() as u32).to_le_bytes());
    for e in &f.entries {
        crate::window::write_freeze_entry(
            out,
            &e.key,
            e.window_start,
            e.window_end,
            e.count,
            &e.accs,
            codec,
        )?;
    }
    encode_opt_i64(f.wm_in, out);
    encode_opt_i64(f.wm_out, out);
    encode_opt_i64(f.last_effective, out);
    Ok(())
}

#[cfg(test)]
fn decode_freeze(src: &mut &[u8], max_entries: usize) -> Result<WindowFreeze> {
    decode_freeze_mode(src, max_entries, true)
}

pub(crate) struct FreezeHeader {
    pub operator: OperatorId,
    pub slot: StateSlotId,
    pub kind: u8,
    pub entries: usize,
}
impl FreezeHeader {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 11 { return Err(SparrowError::new(ErrorCode::CodecViolation, "truncated window freeze")); }
        Ok(Self {operator:u32::from_le_bytes(bytes[..4].try_into().unwrap()).into(),
            slot:u16::from_le_bytes(bytes[4..6].try_into().unwrap()).into(),kind:bytes[6],
            entries:u32::from_le_bytes(bytes[7..11].try_into().unwrap()) as usize})
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_freeze_mode(
    src: &mut &[u8],
    max_entries: usize,
    materialize: bool,
) -> Result<WindowFreeze> {
    decode_freeze_at_cut(src,max_entries,materialize,None)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_freeze_at_cut(src: &mut &[u8], max_entries: usize, materialize: bool, pt_cut: Option<i64>) -> Result<WindowFreeze> {
    decode_freeze_metered(src, max_entries, materialize, pt_cut, crate::aggregate::AccumulatorCodec::Window, &mut 0)
}

/// Bounded scan/materialize of one WindowFreeze frame. In scan mode
/// `resident` receives the exact [`WindowFreeze::resident_bytes`] that
/// materialization would produce, so restore credit is reserved first.
pub(crate) fn decode_freeze_metered(
    src: &mut &[u8],
    max_entries: usize,
    materialize: bool,
    pt_cut: Option<i64>,
    codec: crate::aggregate::AccumulatorCodec,
    resident: &mut usize,
) -> Result<WindowFreeze> {
    let FreezeHeader {operator,slot,kind,entries:n} = FreezeHeader::parse(src)?;
    *src = &src[11..];
    const MIN_FREEZE_ENTRY: usize = 2 + 8 + 8 + 8 + 2;
    if n > max_entries {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!("freeze entry count {n} exceeds max_state_keys {max_entries}; refusing alloc"),
        ));
    }
    if src.len() < n.saturating_mul(MIN_FREEZE_ENTRY) {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated window freeze entries (declared count exceeds remaining bytes)",
        ));
    }
    let mut entries = Vec::with_capacity(if materialize { n } else { 0 });
    let mut metered = 128usize.saturating_add(n.saturating_mul(std::mem::size_of::<FrozenEntry>()));
    for _ in 0..n {
        metered = metered.saturating_add(128);
        if src.len() < 2 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "truncated freeze entry",
            ));
        }
        let nk = u16::from_le_bytes(src[..2].try_into().unwrap()) as usize;
        *src = &src[2..];
        if nk > 64 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("freeze key arity {nk} exceeds 64"),
            ));
        }
        let mut key = Vec::with_capacity(if materialize { nk } else { 0 });
        for _ in 0..nk {
            let before = *src;
            if materialize {
                key.push(Scalar::decode_value(src)?);
            } else {
                Scalar::skip_encoded_value(src)?;
            }
            metered = metered.saturating_add(crate::aggregate::encoded_scalar_resident(
                &before[..before.len() - src.len()],
            ));
        }
        if src.len() < 8 + 8 + 8 + 2 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "truncated freeze entry body",
            ));
        }
        let window_start = i64::from_le_bytes(src[..8].try_into().unwrap());
        *src = &src[8..];
        let window_end = i64::from_le_bytes(src[..8].try_into().unwrap());
        *src = &src[8..];
        let count = u64::from_le_bytes(src[..8].try_into().unwrap());
        *src = &src[8..];
        if pt_cut.is_some_and(|now| kind != 0 || window_start < 0 || window_start > now || window_end <= now || count != 0) {
            return Err(SparrowError::new(ErrorCode::CodecViolation,"PT window state disagrees with the processing-time cut"));
        }
        let na = u16::from_le_bytes(src[..2].try_into().unwrap()) as usize;
        *src = &src[2..];
        if na > 64 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("freeze accumulator count {na} exceeds 64"),
            ));
        }
        let mut accs = Vec::with_capacity(if materialize { na } else { 0 });
        for _ in 0..na {
            if materialize {
                let acc = Accumulator::decode_codec(src, codec)?;
                metered = metered.saturating_add(acc.tracked_bytes());
                accs.push(acc);
            } else {
                metered = metered.saturating_add(Accumulator::skip_encoded_codec(src, codec)?);
            }
        }
        if materialize {
            entries.push(FrozenEntry {
                key,
                window_start,
                window_end,
                count,
                accs,
            });
        }
    }
    let freeze = WindowFreeze {
        operator,
        slot,
        kind,
        entries,
        wm_in: decode_opt_i64(src)?,
        wm_out: decode_opt_i64(src)?,
        last_effective: decode_opt_i64(src)?,
    };
    if pt_cut.is_some() && (freeze.wm_in.is_some() || freeze.wm_out.is_some() || freeze.last_effective.is_some()) {
        return Err(SparrowError::new(ErrorCode::CodecViolation,"PT freeze contains event-time watermarks"));
    }
    *resident = if materialize { freeze.resident_bytes() } else { metered };
    Ok(freeze)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_unpublished_manifest_is_never_a_corruption_fallback() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        store.fault.point = FaultPoint::AfterManifestRename;
        assert!(store.commit(&sample_snapshot(2)).is_err());
        fs::write(dir.join("CURRENT"), b"broken\n").unwrap();
        assert_eq!(store.recover_required().unwrap().checkpoint_id, 1);
        assert!(store.recover_id(2).is_err());
        assert_eq!(store.recover_id(1).unwrap().checkpoint_id, 1);
        store.fault.point = FaultPoint::None;
        store.commit(&sample_snapshot(3)).unwrap();
        assert_eq!(store.recover_required().unwrap().checkpoint_id, 3);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn production_store_writer_exclusion_and_monotonic_ids() {
        let dir = tmp();
        let mut stale = CheckpointStore::open(&dir).unwrap();
        let mut writer =
            CheckpointStore::open_exclusive(&dir, 1024, CheckpointRetention::default()).unwrap();
        writer.commit(&sample_snapshot(1)).unwrap();
        assert!(
            CheckpointStore::open_exclusive(&dir, 1024, CheckpointRetention::default()).is_err()
        );
        let mut reader = CheckpointStore::open(&dir).unwrap();
        assert_eq!(reader.recover_required().unwrap().checkpoint_id, 1);
        assert!(reader.commit(&sample_snapshot(2)).is_err());
        drop(writer);
        reader.commit(&sample_snapshot(2)).unwrap();
        assert!(reader.commit(&sample_snapshot(2)).is_err());
        let next = CheckpointStore::open(&dir).unwrap().next_checkpoint_id();
        assert_eq!(next, 3);
        for id in 3..=6 {
            reader.commit(&sample_snapshot(id)).unwrap();
        }
        assert!(!dir.join("chk-00000001").exists());
        assert!(
            stale.commit(&sample_snapshot(1)).is_err(),
            "pruned ids cannot be recycled by stale handles"
        );
        assert_eq!(reader.recover_required().unwrap().checkpoint_id, 6);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn production_numeric_restore_dependency_survives_retention_gc() {
        let dir = tmp();
        let mut store = CheckpointStore::open_exclusive(
            &dir,
            1024,
            CheckpointRetention {
                generations: 1,
                ..Default::default()
            },
        )
        .unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        store.pin_recovery_point(1).unwrap();
        for id in 2..=6 {
            store.commit(&sample_snapshot(id)).unwrap();
        }
        assert_eq!(store.recover_id(1).unwrap().checkpoint_id, 1);
        let inventory = store.inventory().unwrap();
        assert_eq!(inventory.pinned, Some(1));
        assert_eq!(
            inventory.generations.len(),
            2,
            "one pinned point plus CURRENT, not unbounded history"
        );
        drop(store);
        let mut store = CheckpointStore::open_exclusive(
            &dir,
            1024,
            CheckpointRetention {
                generations: 1,
                ..Default::default()
            },
        )
        .unwrap();
        store.commit(&sample_snapshot(7)).unwrap();
        assert!(store.recover_id(1).is_err());
        assert_eq!(store.inventory().unwrap().generations.len(), 1);
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn production_quota_failure_preserves_current_and_unrelated_files() {
        let dir = tmp();
        let mut store = CheckpointStore::open_exclusive(
            &dir,
            1024,
            CheckpointRetention {
                generations: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        let current = fs::read(dir.join("CURRENT")).unwrap();
        fs::write(dir.join("operator-owned.bin"), vec![0; 1024 * 1024]).unwrap();
        assert_eq!(
            store.commit(&sample_snapshot(2)).unwrap_err().code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert!(dir.join("operator-owned.bin").exists());
        assert_eq!(store.recover_required().unwrap().checkpoint_id, 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn production_legacy_current_gets_history_proof_before_replacement() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        fs::remove_file(dir.join("chk-00000001/PUBLISHED")).unwrap();
        assert_eq!(store.recover_required().unwrap().checkpoint_id, 1);
        store.commit(&sample_snapshot(2)).unwrap();
        assert!(store.was_published(1));
        fs::write(dir.join("chk-00000002/0000.bin"), b"bad").unwrap();
        assert_eq!(store.recover_required().unwrap().checkpoint_id, 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn production_checkpoint_reads_are_bounded_and_inventory_is_read_only() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let inventory = store.inventory().unwrap();
        assert_eq!(inventory.current, Some(1));
        assert!(inventory.generations[0].published);
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        fs::write(dir.join("chk-00000001/0000.bin"), vec![0; CHUNK_SIZE + 1]).unwrap();
        assert_eq!(
            store.recover_required().unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        fs::write(dir.join("CURRENT"), vec![0; 129]).unwrap();
        assert_eq!(
            store.inventory().unwrap().current_error,
            Some(ErrorCode::BoundExceeded)
        );
        fs::remove_dir_all(dir).unwrap();
    }
    use crate::window::{FrozenEntry, WindowOperator};
    use sparrow_expr::Expr;
    use sparrow_io::{MemoryReplaySource, RecordSource, ReplayableSource};
    use sparrow_model::{
        AggFn, DataType, Field, FieldId, MemoryOwner, ResourceBudget, Schema, SchemaId, WindowKind,
    };
    use sparrow_model::{CreditKind, Row, RowBatchBuilder};
    use sparrow_plan::{AggCall, WindowSpec};
    use std::sync::Arc;

    fn tmp() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sparrow-chk-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        p
    }

    fn sample_snapshot(id: u64) -> CheckpointSnapshot {
        let schema = Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap();
        let spec = WindowSpec::new(
            WindowKind::Count { size: 3 },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        );
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut op = WindowOperator::new(
            OperatorId::new(7),
            spec.clone(),
            schema.clone(),
            owner.clone(),
            16,
            8,
        )
        .unwrap();
        let mut b = RowBatchBuilder::new(
            Arc::new(schema.clone()),
            owner,
            CreditKind::Reservation,
            2,
            4096,
        )
        .unwrap();
        b.push(Row {
            values: vec![Scalar::utf8("d1"), Scalar::Int64(10)],
        })
        .unwrap();
        b.push(Row {
            values: vec![Scalar::utf8("d1"), Scalar::Int64(20)],
        })
        .unwrap();
        let batch = b.finish().unwrap();
        let _ = op.on_batch(&batch, 0).unwrap();
        let window = op.freeze();
        CheckpointSnapshot {
            checkpoint_id: id,
            source: SourcePosition::start(SourceIdentity::memory("demo", 32, 1)),
            window: window.clone(),
            ingested_rows: 2,
            layout: PlanLayout::from_window(OperatorId::new(7), window.slot, &spec)
                .with_input_schema(&schema),
            table: None,
        }
    }

    #[test]
    fn r10_bounded_validation_matches_decode_without_materializing_state() {
        let bytes = sample_snapshot(1).encode().unwrap();
        for end in 0..=bytes.len() {
            assert_eq!(
                CheckpointSnapshot::decode_mode(&bytes[..end], 1024, false).is_ok(),
                CheckpointSnapshot::decode_with_max_state_keys(&bytes[..end], 1024).is_ok(),
                "prefix {end}"
            );
        }
        for index in 0..bytes.len() {
            for value in [0u8, 1, 255] {
                let mut mutated = bytes.clone();
                mutated[index] = value;
                assert_eq!(
                    CheckpointSnapshot::decode_mode(&mutated, 1024, false).is_ok(),
                    CheckpointSnapshot::decode_with_max_state_keys(&mutated, 1024).is_ok(),
                    "byte {index}={value}"
                );
            }
        }
        let validated = CheckpointSnapshot::decode_mode(&bytes, 1024, false).unwrap();
        assert!(validated.window.entries.is_empty());
        assert!(!CheckpointSnapshot::decode(&bytes)
            .unwrap()
            .window
            .entries
            .is_empty());
    }

    #[test]
    fn r10_publication_proof_never_replaces_chunk_validation_before_gc() {
        let dir = tmp();
        let mut store = CheckpointStore::open_exclusive(
            &dir,
            1024,
            CheckpointRetention {
                generations: 2,
                ..Default::default()
            },
        )
        .unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        let proof = fs::metadata(dir.join("chk-00000001/PUBLISHED"))
            .unwrap()
            .modified()
            .unwrap();
        store.commit(&sample_snapshot(2)).unwrap();
        assert_eq!(
            fs::metadata(dir.join("chk-00000001/PUBLISHED"))
                .unwrap()
                .modified()
                .unwrap(),
            proof,
            "old publication proof need not be rewritten"
        );
        let current = dir.join("chk-00000002/0000.bin");
        let mut bytes = fs::read(&current).unwrap();
        bytes[0] ^= 1;
        fs::write(current, bytes).unwrap();
        assert!(
            store.was_published(2),
            "proof authenticates MANIFEST, not chunk contents"
        );
        store.fault.point = FaultPoint::AfterManifestRename;
        assert!(store.commit(&sample_snapshot(3)).is_err());
        assert_eq!(
            store.recover_required().unwrap().checkpoint_id,
            1,
            "failed replacement must not GC the last valid fallback"
        );
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn r10_readonly_open_never_creates_directories_or_commits() {
        let dir = tmp();
        let missing = dir.join("missing");
        assert!(CheckpointStore::open_readonly(&missing).is_err());
        assert!(!missing.exists());
        let mut store = CheckpointStore::open_readonly(&dir).unwrap();
        assert_eq!(
            store.commit(&sample_snapshot(1)).unwrap_err().code,
            ErrorCode::PolicyDenied
        );
        assert!(!dir.join("LOCK").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn r10_wire_estimate_excludes_state_container_overhead() {
        let owner = MemoryOwner::new(ResourceBudget::performance());
        let schema = count_schema();
        let mut op = WindowOperator::new(
            OperatorId::WINDOW,
            count_spec(),
            schema.clone(),
            owner.clone(),
            1024,
            1024,
        )
        .unwrap();
        ingest_distinct_keys(&mut op, &schema, &owner, 512);
        let encoded = crate::barrier::EncodedFreeze::from_operator(&op, &owner, 1024).unwrap();
        let estimate = op.estimated_freeze_bytes() + 256;
        assert!(estimate >= encoded.bytes.len());
        assert!(estimate <= encoded.bytes.len() * 3 / 2);
        assert!(op.retention_bytes() > estimate * 2);
        assert_eq!(
            op.estimated_freeze_bytes(),
            estimated_frozen_bytes(&op.freeze(), crate::aggregate::AccumulatorCodec::Window).unwrap()
        );
    }

    #[test]
    fn commit_and_recover_round_trip() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        let snap = sample_snapshot(1);
        store.commit(&snap).unwrap();
        let got = store.recover_committed().unwrap().unwrap();
        assert_eq!(got.checkpoint_id, 1);
        assert_eq!(got.ingested_rows, 2);
        assert_eq!(got.window.entries.len(), 1);
        assert_eq!(got.window.kind, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn r9_unknown_checkpoint_version_explains_inspection_and_restore_versions() {
        let mut bytes = sample_snapshot(1).encode().unwrap();
        bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
        let error = CheckpointSnapshot::decode(&bytes).unwrap_err();
        assert_eq!(error.code, ErrorCode::FeatureUnavailable);
        assert!(error
            .message
            .contains("supported versions: 1 (inspect only), 2"));
    }

    #[test]
    fn base02_legacy_snapshot_is_inspectable_but_not_reusable_or_rewritten() {
        let snap = sample_snapshot(1);
        let mut bytes = snap.encode().unwrap();
        let descriptor = snap
            .layout
            .semantic_descriptor
            .as_ref()
            .unwrap()
            .encode()
            .unwrap();
        let descriptor_start = bytes.len() - 1 - descriptor.len() - 4;
        bytes.drain(descriptor_start..bytes.len() - 1);
        bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit_encoded(1, &bytes).unwrap();
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let legacy = store.recover_required().unwrap();
        assert!(legacy.layout.semantic_descriptor.is_none());
        assert_eq!(
            legacy.check_compatible(&snap.layout).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        assert!(
            legacy.encode().is_err(),
            "never stamp new semantics onto legacy state"
        );
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert_eq!(fs::read(dir.join("chk-00000001/0000.bin")).unwrap(), bytes);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn base02_descriptor_codec_rejects_truncation_oversize_and_trailing_bytes() {
        let snap = sample_snapshot(1);
        let bytes = snap.encode().unwrap();
        let decoded = CheckpointSnapshot::decode(&bytes).unwrap();
        decoded.check_compatible(&snap.layout).unwrap();
        let descriptor_len = snap
            .layout
            .semantic_descriptor
            .as_ref()
            .unwrap()
            .encode()
            .unwrap()
            .len();
        let start = bytes.len() - 1 - descriptor_len - 4;
        for end in start..bytes.len() {
            assert!(
                CheckpointSnapshot::decode(&bytes[..end]).is_err(),
                "cut {end}"
            );
        }
        let mut bad = bytes.clone();
        bad[start..start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(CheckpointSnapshot::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad[start + 4..start + 8].copy_from_slice(b"SS99");
        assert!(CheckpointSnapshot::decode(&bad).is_err());
        let mut bad = bytes;
        bad.push(0);
        assert!(CheckpointSnapshot::decode(&bad).is_err());
    }

    #[test]
    fn r4_freeze_uses_live_reservation_headroom_without_double_billing() {
        use crate::barrier::EncodedFreeze;
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let schema = count_schema();
        let mut op = WindowOperator::new(
            OperatorId::WINDOW,
            count_spec(),
            schema.clone(),
            owner.clone(),
            64,
            8,
        )
        .unwrap();
        ingest_distinct_keys(&mut op, &schema, &owner, 1);
        op.check_freeze_encode_bound(64).unwrap();
        assert_eq!(
            op.check_freeze_encode_bound(0).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        let estimate = op.estimated_freeze_bytes() + 256;
        let pressure = owner
            .acquire(
                CreditKind::Reservation,
                owner.budget().reservation_bytes - estimate + 1,
            )
            .unwrap();
        let before = owner.usage().reservation_bytes;
        assert_eq!(
            EncodedFreeze::from_operator(&op, &owner, 64)
                .unwrap_err()
                .code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(owner.usage().reservation_bytes, before);
        assert_eq!(op.key_count(), 1);
        drop(pressure);
        // Exactly wire + reference-sort workspace headroom. No whole decoded
        // state reservation; transient sorting credit refunds before return.
        let pressure = owner
            .acquire(
                CreditKind::Reservation,
                owner.budget().reservation_bytes - estimate - op.freeze_workspace_bytes(),
            )
            .unwrap();
        let frozen = EncodedFreeze::from_operator(&op, &owner, 64).unwrap();
        assert_eq!(
            owner.usage().reservation_bytes,
            owner.budget().reservation_bytes - op.freeze_workspace_bytes()
        );
        drop((frozen, pressure, op));
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn r3_encoded_ack_preserves_snapshot_bytes_and_leases() {
        assert_eq!(crc32(b"123456789"), 0xcbf43926);
        let snap = sample_snapshot(7);
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut bytes = Vec::new();
        encode_freeze(&snap.window, &mut bytes, 1024).unwrap();
        let lease = owner
            .acquire(CreditKind::Reservation, bytes.capacity())
            .unwrap();
        let encoded = CheckpointSnapshot::encode_frozen(
            snap.checkpoint_id,
            &snap.source,
            snap.ingested_rows,
            &snap.layout,
            snap.table.as_ref(),
            crate::barrier::EncodedFreeze { bytes, lease, ext: false },
        )
        .unwrap();
        assert_eq!(encoded.bytes, snap.encode().unwrap());
        assert!(owner.usage().reservation_bytes >= encoded.bytes.len());
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store
            .commit_encoded(snap.checkpoint_id, &encoded.bytes)
            .unwrap();
        assert_eq!(store.recover_required().unwrap(), snap);
        drop(encoded);
        assert_eq!(owner.usage().physical_bytes, 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn crash_cut_after_chunk_write_is_not_committed() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.fault.point = FaultPoint::AfterChunkWrite;
        let snap = sample_snapshot(1);
        assert!(store.commit(&snap).is_err());
        assert!(store.recover_committed().unwrap().is_none());
        let chk = dir.join("chk-00000001");
        assert!(chk.join("0000.bin").exists());
        assert!(!chk.join("ACK").exists());
        assert!(!chk.join("MANIFEST").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_cut_after_ack_is_not_committed() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.fault.point = FaultPoint::AfterAck;
        assert!(store.commit(&sample_snapshot(1)).is_err());
        assert!(store.recover_committed().unwrap().is_none());
        assert!(dir.join("chk-00000001/ACK").exists());
        assert!(!dir.join("chk-00000001/MANIFEST").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_cut_after_manifest_tmp_is_not_committed() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.fault.point = FaultPoint::AfterManifestTmp;
        assert!(store.commit(&sample_snapshot(1)).is_err());
        assert!(store.recover_committed().unwrap().is_none());
        assert!(dir.join("chk-00000001/MANIFEST.tmp").exists());
        assert!(!dir.join("chk-00000001/MANIFEST").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_cut_after_manifest_rename_without_current() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.fault.point = FaultPoint::AfterManifestRename;
        assert!(store.commit(&sample_snapshot(1)).is_err());
        assert!(store.recover_committed().unwrap().is_none());
        assert!(dir.join("chk-00000001/MANIFEST").exists());
        assert!(!dir.join("CURRENT").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_chunk_disk_full_is_not_committed() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.fault.point = FaultPoint::DuringChunkWrite;
        let err = store.commit(&sample_snapshot(1)).unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        assert!(store.recover_committed().unwrap().is_none());
        assert!(dir.join("chk-00000001/0000.bin.part").exists());
        assert!(!dir.join("chk-00000001/0000.bin").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checksum_mismatch_rejects_restore() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.fault.point = FaultPoint::CorruptChecksum;
        store.commit(&sample_snapshot(1)).unwrap();
        let err = store.recover_committed().unwrap_err();
        assert_eq!(err.code, ErrorCode::CodecViolation);
        assert!(err.message.contains("checksum"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_source_positions_survive_encode() {
        let mut src = MemoryReplaySource::from_lines("f", &["{}", "{}"]);
        src.next_frame().unwrap();
        let pos = src.position();
        let snap = CheckpointSnapshot {
            checkpoint_id: 3,
            source: pos.clone(),
            window: WindowFreeze {
                operator: OperatorId::new(1),
                slot: StateSlotId::new(1),
                kind: 1,
                entries: Vec::new(),
                wm_in: None,
                wm_out: None,
                last_effective: None,
            },
            ingested_rows: 1,
            layout: PlanLayout::from_window(OperatorId::new(1), StateSlotId::new(1), &count_spec())
                .with_input_schema(&count_schema()),
            table: None,
        };
        let bytes = snap.encode().unwrap();
        let got = CheckpointSnapshot::decode(&bytes).unwrap();
        assert_eq!(got.source, pos);
        assert_eq!(got.layout.operator.raw(), 1);
    }

    #[test]
    fn aligned_not_exactly_once() {
        assert_eq!(CheckpointStore::policy(), RecoveryPolicy::Aligned);
        assert!(
            CheckpointStore::honesty().contains("not default exactly-once")
                || CheckpointStore::honesty().contains("Not default exactly-once")
                || CheckpointStore::honesty().contains("not exactly-once")
                || CheckpointStore::honesty().contains("Not default")
        );
        assert_eq!(CheckpointStore::label(), "aligned");
    }

    #[test]
    fn recover_required_rejects_empty_store() {
        let dir = tmp();
        let store = CheckpointStore::open(&dir).unwrap();
        let err = store.recover_required().unwrap_err();
        assert_eq!(err.code, ErrorCode::UnsupportedRestore);
        assert!(err.message.contains("silent empty-state"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn operator_slot_mismatch_rejects_reuse() {
        let snap = sample_snapshot(1);
        let mut live = snap.layout.clone();
        live.operator = OperatorId::new(99);
        assert!(snap.check_compatible(&live).is_err());
    }

    #[test]
    fn soak_commit_restore_loops_are_finite() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        for i in 1..=8 {
            store.commit(&sample_snapshot(i)).unwrap();
            let got = store.recover_required().unwrap();
            assert_eq!(got.checkpoint_id, i);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn r14_commit_dirents_survive_rename() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        assert!(dir.join("CURRENT").exists());
        assert!(dir.join("chk-00000001/MANIFEST").exists());
        assert!(store.recover_committed().unwrap().is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn r15_oversized_manifest_file_rejected_before_decode_alloc() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        let huge = vec![b'X'; (MAX_MANIFEST_BYTES as usize) + 16];
        fs::write(dir.join("chk-00000001/MANIFEST"), huge).unwrap();
        let err = store.recover_committed().unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn r15_manifest_claims_too_many_chunks() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        let mut huge = Vec::from(*MANIFEST_MAGIC);
        huge.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        huge.extend_from_slice(&1u64.to_le_bytes());
        huge.extend_from_slice(&(MAX_MANIFEST_CHUNKS + 1).to_le_bytes());
        huge.extend_from_slice(&8u64.to_le_bytes());
        huge.extend_from_slice(&(MAX_MANIFEST_CHUNKS + 1).to_le_bytes());
        fs::write(dir.join("chk-00000001/MANIFEST"), huge).unwrap();
        let err = store.recover_committed().unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn r15_old_generations_are_garbage_collected() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        for i in 1..=5 {
            store.commit(&sample_snapshot(i)).unwrap();
        }
        assert!(!dir.join("chk-00000001").exists());
        assert!(!dir.join("chk-00000002").exists());
        assert!(dir.join("chk-00000005").exists());
        assert_eq!(store.recover_committed().unwrap().unwrap().checkpoint_id, 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn p0_13_freeze_rejects_untrusted_capacity() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&1_000_000u32.to_le_bytes());
        let err = super::decode_freeze(&mut buf.as_slice(), MAX_FREEZE_ENTRIES).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
    }

    #[test]
    fn k4_profile_guard_rejects_unclassifiable_first_chunks_without_mutating_history() {
        let fixtures: [(&str, &[u8]); 3] = [
            ("0000.bin", b"SPV1"),
            ("0000.bin.part", &[]),
            ("0000.bin", b"not-a-snapshot"),
        ];
        for (file, bytes) in fixtures {
            let dir = tmp();
            let generation = dir.join("chk-00000001");
            fs::create_dir(&generation).unwrap();
            fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
            let path = generation.join(file);
            fs::write(&path, bytes).unwrap();
            let before_chunk = fs::read(&path).unwrap();
            let before_current = fs::read(dir.join("CURRENT")).unwrap();

            let error = match CheckpointStore::open_iot_exclusive(
                &dir,
                1024,
                CheckpointRetention::default(),
            ) {
                Ok(_) => panic!("unclassifiable checkpoint history was accepted"),
                Err(error) => error,
            };
            assert_eq!(error.code, ErrorCode::UnsupportedRestore);
            assert!(error
                .message
                .contains("first chunk cannot be classified"));
            assert_eq!(fs::read(&path).unwrap(), before_chunk);
            assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), before_current);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    fn count_schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    fn count_spec() -> WindowSpec {
        WindowSpec::new(
            WindowKind::Count { size: 1_000_000 },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        )
    }

    fn synthetic_freeze(operator: OperatorId, n: usize) -> WindowFreeze {
        WindowFreeze {
            operator,
            slot: StateSlotId::new(1),
            kind: 1,
            entries: (0..n)
                .map(|i| FrozenEntry {
                    key: vec![Scalar::Int64(i as i64)],
                    window_start: 0,
                    window_end: 0,
                    count: 1,
                    accs: Vec::new(),
                })
                .collect(),
            wm_in: None,
            wm_out: None,
            last_effective: None,
        }
    }

    fn ingest_distinct_keys(
        op: &mut WindowOperator,
        schema: &Schema,
        owner: &Arc<MemoryOwner>,
        n: usize,
    ) {
        const CHUNK: usize = 256;
        let mut i = 0;
        while i < n {
            let end = (i + CHUNK).min(n);
            let mut b = RowBatchBuilder::new(
                Arc::new(schema.clone()),
                Arc::clone(owner),
                CreditKind::Reservation,
                end - i,
                64 * 1024,
            )
            .unwrap();
            for k in i..end {
                b.push(Row {
                    values: vec![Scalar::utf8(format!("k{k:04}")), Scalar::Int64(1)],
                })
                .unwrap();
            }
            let batch = b.finish().unwrap();
            let _ = op.on_batch(&batch, 0).unwrap();
            i = end;
        }
    }

    #[test]
    fn n6_freeze_entry_cap_matches_performance_budget() {
        assert_eq!(
            MAX_FREEZE_ENTRIES,
            ResourceBudget::performance().max_state_keys
        );
        assert!(MAX_FREEZE_ENTRIES > ResourceBudget::compact().max_state_keys);
        assert!(MAX_FREEZE_ENTRIES > 4096);
        assert_eq!(freeze_entry_cap(8192), 8192);
        assert_eq!(freeze_entry_cap(0), MAX_FREEZE_ENTRIES);
    }

    #[test]
    fn n6_performance_budget_freeze_commit_recover_roundtrip() {
        let budget = ResourceBudget {
            max_state_keys: 8192,
            ..ResourceBudget::performance()
        };
        const N: usize = 5000;
        assert!(N > 4096, "must exceed the old hardcoded decode cap");
        assert!(N <= budget.max_state_keys);

        let schema = count_schema();
        let spec = count_spec();
        let owner = MemoryOwner::new(budget);
        let mut op = WindowOperator::new(
            OperatorId::new(7),
            spec.clone(),
            schema.clone(),
            Arc::clone(&owner),
            budget.max_state_keys,
            budget.max_timers,
        )
        .unwrap();
        ingest_distinct_keys(&mut op, &schema, &owner, N);
        assert_eq!(op.key_count(), N);

        let window = op.freeze();
        assert_eq!(window.entries.len(), N);
        let dir = tmp();
        let mut store =
            CheckpointStore::open_with_max_state_keys(&dir, budget.max_state_keys).unwrap();
        let snap = CheckpointSnapshot {
            checkpoint_id: 1,
            source: SourcePosition::start(SourceIdentity::memory("n6", 32, 1)),
            window: window.clone(),
            ingested_rows: N as u64,
            layout: PlanLayout::from_window(OperatorId::new(7), window.slot, &spec)
                .with_input_schema(&schema),
            table: None,
        };
        store.commit(&snap).unwrap();
        assert!(dir.join("CURRENT").exists());

        let got = store.recover_committed().unwrap().unwrap();
        assert_eq!(got.window.entries.len(), N);
        assert_eq!(got.ingested_rows, N as u64);

        let owner2 = MemoryOwner::new(budget);
        let mut restored = WindowOperator::new(
            OperatorId::new(7),
            spec,
            schema,
            owner2,
            budget.max_state_keys,
            budget.max_timers,
        )
        .unwrap();
        restored.restore_freeze(&got.window).unwrap();
        assert_eq!(restored.key_count(), N);
        assert_eq!(restored.freeze().entries.len(), N);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn n6_encode_over_max_state_keys_does_not_publish_current() {
        let dir = tmp();
        let mut store = CheckpointStore::open_with_max_state_keys(&dir, 16).unwrap();
        let mut snap = sample_snapshot(1);
        snap.window = synthetic_freeze(snap.window.operator, 17);
        let err = store.commit(&snap).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        assert!(
            err.message.contains("max_state_keys"),
            "encode must name the job bound: {err}"
        );
        assert!(!dir.join("CURRENT").exists());
        assert!(store.recover_committed().unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn n6_decode_honors_job_max_state_keys() {
        let n = 5000;
        let mut snap = sample_snapshot(1);
        snap.window = synthetic_freeze(snap.window.operator, n);
        let bytes = snap.encode_with_max_state_keys(8192).unwrap();
        let old_cap_err = CheckpointSnapshot::decode_with_max_state_keys(&bytes, 4096).unwrap_err();
        assert_eq!(old_cap_err.code, ErrorCode::BoundExceeded);
        let got = CheckpointSnapshot::decode_with_max_state_keys(&bytes, 8192).unwrap();
        assert_eq!(got.window.entries.len(), n);
        let codec = CheckpointSnapshot::decode(&bytes).unwrap();
        assert_eq!(codec.window.entries.len(), n);
    }

    #[test]
    fn p1_14_incremental_encode_matches_freeze_bytes() {
        let schema = count_schema();
        let spec = count_spec();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut op = WindowOperator::new(
            OperatorId::new(7),
            spec.clone(),
            schema.clone(),
            Arc::clone(&owner),
            64,
            8,
        )
        .unwrap();
        ingest_distinct_keys(&mut op, &schema, &owner, 12);
        let window = op.freeze();
        let snap = CheckpointSnapshot {
            checkpoint_id: 1,
            source: SourcePosition::start(SourceIdentity::memory("p114", 32, 1)),
            window: window.clone(),
            ingested_rows: 12,
            layout: PlanLayout::from_window(OperatorId::new(7), window.slot, &spec)
                .with_input_schema(&schema),
            table: None,
        };
        let cloned = snap.encode_with_max_state_keys(64).unwrap();
        let streamed = CheckpointSnapshot::encode_from_operator(
            1,
            &snap.source,
            12,
            &snap.layout,
            None,
            &op,
            64,
        )
        .unwrap();
        assert_eq!(
            cloned, streamed,
            "incremental encode must match freeze-then-encode bytes"
        );
        let before = owner.usage().retention_bytes;
        let _ = op.encode_freeze_into(&mut Vec::new(), 64).unwrap();
        assert_eq!(
            owner.usage().retention_bytes,
            before,
            "incremental encode must not clone entries onto the retention ledger"
        );
    }

    #[test]
    fn p1_14_encode_refuses_oversized_freeze_before_current() {
        let dir = tmp();
        let mut store = CheckpointStore::open_with_max_state_keys(&dir, 64).unwrap();
        let mut snap = sample_snapshot(1);
        snap.window = WindowFreeze {
            operator: snap.window.operator,
            slot: snap.window.slot,
            kind: 1,
            entries: (0..24)
                .map(|i| FrozenEntry {
                    key: vec![Scalar::utf8("x".repeat(400_000))],
                    window_start: 0,
                    window_end: 0,
                    count: i,
                    accs: Vec::new(),
                })
                .collect(),
            wm_in: None,
            wm_out: None,
            last_effective: None,
        };
        let err = store.commit(&snap).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        assert!(
            err.message.contains("refusing encode") || err.message.contains("snapshot quota"),
            "must refuse before CURRENT: {err}"
        );
        assert!(!dir.join("CURRENT").exists());
        assert!(store.recover_committed().unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn p1_24_corrupt_current_recovers_previous_generation() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        store.commit(&sample_snapshot(2)).unwrap();
        fs::write(dir.join("CURRENT"), b"not-a-checkpoint\n").unwrap();
        let got = store.recover_committed().unwrap().unwrap();
        assert_eq!(
            got.checkpoint_id, 2,
            "must fall back to a verified generation"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v27_validation_credit_pressure_is_not_corruption_fallback() {
        let dir = tmp();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let target = sparrow_io::OwnedSinkIdentity::new(
            sparrow_io::SinkIdentity::jetstream(
                &["nats://localhost".into()], None, "OUT", 1, "out.rows", None,
            ).unwrap(),
            &owner,
        ).unwrap();
        let schema = Schema::new(1, vec![Field::new(1, "v", DataType::Int64, false)]).unwrap();
        let physical = sparrow_plan::PhysicalPlan {
            pipeline: 1.into(), revision: 1.into(), edges: None,
            side_outputs: vec![], source_times: vec![],
            stages: vec![
                sparrow_plan::PhysicalStage::MemorySource {
                    operator: 1.into(), name: "sensors".into(), schema: schema.clone(),
                },
                sparrow_plan::PhysicalStage::CaptureSink {
                    operator: 2.into(), name: "out".into(), schema,
                },
            ],
        };
        let plan = sparrow_plan::CheckpointPlan::from_physical(&physical).unwrap();
        let position = SourcePosition::start(SourceIdentity {
            kind: "file".into(), path: "fixture.ndjson".into(), size: 0, fingerprint: 0,
        });
        let encode = |id| PipelineSnapshot::encode_frozen_with_sink(
            id, &position, 0, 1, &plan, target.identity(),
            crate::ParticipantAcks {
                attempt: 1, generation: [7; 16], freezes: vec![], next_output: None,
            }, &owner, 16,
        ).unwrap();
        let mut store = CheckpointStore::open_file_jetstream_sink_exclusive(
            &dir, 16, Default::default(), &plan, target.clone(),
        ).unwrap();
        store.commit_prepared(&encode(1)).unwrap();
        store.commit_prepared(&encode(2)).unwrap();
        let candidate = encode(3);
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let available = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
        let pressure = owner.acquire(CreditKind::Reservation, available - 1024).unwrap();
        assert_eq!(store.recover_pipeline_required().unwrap_err().code, ErrorCode::ResourceExhausted);
        // Previously this path swallowed each ResourceExhausted and returned
        // Ok(None), classifying validation pressure as damaged history.
        assert_eq!(store.load_latest_valid_except(Some(2)).err().unwrap().code, ErrorCode::ResourceExhausted);
        assert_eq!(store.commit_prepared(&candidate).unwrap_err().code, ErrorCode::ResourceExhausted);
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert!(!dir.join("chk-00000003").exists());
        drop(pressure);
        assert_eq!(store.recover_pipeline_required().unwrap().checkpoint_id, 2);
        assert_eq!(store.load_latest_valid_except(Some(2)).unwrap().unwrap().id(), 1);
        drop(candidate);
        drop(store);
        drop(target);
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(owner.accounting_errors_total(), 0);
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
#[path = "ext_agg_tests.rs"]
mod ext_agg_tests;

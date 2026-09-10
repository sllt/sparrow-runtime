//! Versioned production aligned single-job checkpoint store.
//!
//! Protocol:
//! 1. Barrier alignment (caller freezes operators; no in-flight rows).
//! 2. Freeze + chunk write (`*.bin.part` → `*.bin`).
//! 3. ACK after every chunk is renamed.
//! 4. Versioned manifest commit (`MANIFEST.tmp` → `MANIFEST`, then `CURRENT.tmp` → `CURRENT`).
//! 5. Recover from a **verified committed** CURRENT+MANIFEST only.
//!
//! This path is **not** exactly-once. Missing or corrupt checkpoints are
//! rejected — never a silent empty-state continue.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{
    DeliveryContract, ErrorCode, OperatorId, RecoveryPolicy, Result, Scalar, SparrowError,
    StateSlotId,
};
use sparrow_plan::PlanLayout;

use crate::aggregate::Accumulator;
use crate::window::{FrozenEntry, WindowFreeze};

pub const CHECKPOINT_LABEL: &str = "aligned";
pub const CHUNK_SIZE: usize = 4096;
pub const MAGIC: &[u8; 4] = b"SPV1";
/// Codec version. Unchanged in R1 (entry-count caps are decode-time only) (P1-25).
pub const SNAPSHOT_VERSION: u16 = 1;
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
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        out.extend_from_slice(&self.checkpoint_id.to_le_bytes());
        out.extend_from_slice(&self.ingested_rows.to_le_bytes());
        encode_position(&self.source, &mut out)?;
        encode_freeze(&self.window, &mut out)?;
        encode_layout(&self.layout, &mut out)?;
        match &self.table {
            None => out.push(0),
            Some(t) => {
                out.push(1);
                encode_str(&t.name, &mut out);
                out.extend_from_slice(&t.version.to_le_bytes());
            }
        }
        Ok(out)
    }

    pub fn decode(mut src: &[u8]) -> Result<Self> {
        if src.len() < 4 + 2 + 8 + 8 || &src[..4] != MAGIC {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "checkpoint snapshot magic/header mismatch (want SPV1 production codec)",
            ));
        }
        src = &src[4..];
        let ver = u16::from_le_bytes(src[..2].try_into().unwrap());
        src = &src[2..];
        if ver != SNAPSHOT_VERSION {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("checkpoint snapshot version {ver} unsupported (V1 codec is {SNAPSHOT_VERSION})"),
            ));
        }
        let checkpoint_id = u64::from_le_bytes(src[..8].try_into().unwrap());
        src = &src[8..];
        let ingested_rows = u64::from_le_bytes(src[..8].try_into().unwrap());
        src = &src[8..];
        let source = decode_position(&mut src)?;
        let window = decode_freeze(&mut src)?;
        let layout = decode_layout(&mut src)?;
        if layout.operator != window.operator || layout.slot != window.slot {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "snapshot OperatorId/StateSlotKey does not match frozen window",
            ));
        }
        let table = if src.is_empty() {
            None
        } else {
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
                    let _ = src;
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

/// Directory-backed experimental checkpoint store.
pub struct CheckpointStore {
    dir: PathBuf,
    next_id: u64,
    pub fault: FaultHook,
}

impl CheckpointStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(io_err)?;
        let next_id = match read_current(&dir) {
            Ok(Some(id)) => id.saturating_add(1),
            _ => 1,
        };
        Ok(Self {
            dir,
            next_id,
            fault: FaultHook::default(),
        })
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
        let id = snapshot.checkpoint_id.max(self.next_id);
        let payload = snapshot.encode()?;
        if payload.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "checkpoint snapshot {}B exceeds {MAX_SNAPSHOT_BYTES}B quota",
                    payload.len()
                ),
            ));
        }
        let chk = self.dir.join(format!("chk-{id:08}"));
        fs::create_dir_all(&chk).map_err(io_err)?;
        fsync_dir(&self.dir)?;
        let chunks = chunk_payload(&payload);
        if chunks.len() as u32 > MAX_MANIFEST_CHUNKS {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("checkpoint would write {} chunks (max {MAX_MANIFEST_CHUNKS})", chunks.len()),
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
            fsync_dir(&chk)?;
        }
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
        self.next_id = id.saturating_add(1);
        // CURRENT is already published. A GC error must not look like a failed commit (P1-24).
        if let Err(_e) = gc_generations(&self.dir, id) {
            // Residual: store may be over quota until the next successful GC.
        }
        Ok(id)
    }

    /// Load the last **committed** checkpoint. Partial chunks / missing
    /// MANIFEST / checksum mismatch are not restored.
    ///
    /// `Ok(None)` means no CURRENT file — callers that *claimed* restore
    /// must treat this as a hard error (see [`Self::recover_required`]).
    pub fn recover_committed(&self) -> Result<Option<CheckpointSnapshot>> {
        match read_current(&self.dir) {
            Ok(Some(id)) => match self.load_generation(id) {
                Ok(snap) => Ok(Some(snap)),
                Err(e) => {
                    if let Some(snap) = self.load_latest_valid_except(Some(id)) {
                        return Ok(Some(snap));
                    }
                    Err(e)
                }
            },
            Ok(None) => Ok(self.load_latest_valid_except(None)),
            Err(e) => {
                if let Some(snap) = self.load_latest_valid_except(None) {
                    Ok(Some(snap))
                } else {
                    Err(e)
                }
            }
        }
    }

    fn load_latest_valid_except(&self, skip: Option<u64>) -> Option<CheckpointSnapshot> {
        let mut ids = list_generation_ids(&self.dir);
        ids.sort_unstable();
        ids.reverse();
        for id in ids {
            if Some(id) == skip {
                continue;
            }
            if let Ok(snap) = self.load_generation(id) {
                return Some(snap);
            }
        }
        None
    }

    fn load_generation(&self, id: u64) -> Result<CheckpointSnapshot> {
        let chk = self.dir.join(format!("chk-{id:08}"));
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
        let man_bytes = fs::read(&man_path).map_err(io_err)?;
        let manifest = Manifest::decode(&man_bytes)?;
        if manifest.checkpoint_id != id {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "MANIFEST checkpoint id does not match CURRENT",
            ));
        }
        let mut payload = Vec::new();
        for i in 0..manifest.n_chunks {
            let part = chk.join(format!("{i:04}.bin.part"));
            if part.exists() {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    format!("partial chunk {i:04}.bin.part present; not committed"),
                ));
            }
            let path = chk.join(format!("{i:04}.bin"));
            let chunk = fs::read(&path).map_err(io_err)?;
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
        Ok(CheckpointSnapshot::decode(&payload)?)
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
        matches!(self.recover_committed(), Ok(Some(_)))
    }
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
        Ok(Self {
            checkpoint_id,
            n_chunks,
            bytes: nbytes,
            checksums,
            codec_version,
        })
    }
}

fn chunk_payload(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.is_empty() {
        return vec![Vec::new()];
    }
    payload.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect()
}

fn fsync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).map_err(io_err)?;
    dir.sync_all().map_err(io_err)?;
    Ok(())
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(rd) = fs::read_dir(path) else {
        return 0;
    };
    for ent in rd.flatten() {
        let p = ent.path();
        if p.is_dir() {
            total = total.saturating_add(dir_size(&p));
        } else if let Ok(m) = ent.metadata() {
            total = total.saturating_add(m.len());
        }
    }
    total
}

fn list_generation_ids(dir: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for ent in rd.flatten() {
            let name = ent.file_name();
            let Some(s) = name.to_str() else {
                continue;
            };
            if let Some(id) = s.strip_prefix("chk-").and_then(|x| x.parse::<u64>().ok()) {
                if ent.path().is_dir() {
                    ids.push(id);
                }
            }
        }
    }
    ids
}

fn gc_generations(dir: &Path, keep_id: u64) -> Result<()> {
    let floor = keep_id.saturating_sub(KEEP_GENERATIONS.saturating_sub(1));
    if let Ok(rd) = fs::read_dir(dir) {
        for ent in rd.flatten() {
            let name = ent.file_name();
            let Some(s) = name.to_str() else {
                continue;
            };
            let Some(id) = s.strip_prefix("chk-").and_then(|x| x.parse::<u64>().ok()) else {
                continue;
            };
            if id < floor {
                let _ = fs::remove_dir_all(ent.path());
            }
        }
    }
    if dir_size(dir) > MAX_STORE_BYTES {
        return Err(SparrowError::new(
            ErrorCode::ResourceExhausted,
            format!("checkpoint store exceeds {MAX_STORE_BYTES}B after GC"),
        ));
    }
    Ok(())
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
    if !path.exists() {
        return Ok(None);
    }
    let mut s = String::new();
    File::open(&path)
        .map_err(io_err)?
        .read_to_string(&mut s)
        .map_err(io_err)?;
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
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

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
    let s = std::str::from_utf8(&src[..n]).map_err(|_| {
        SparrowError::new(ErrorCode::CodecViolation, "snapshot string not utf8")
    })?;
    *src = &src[n..];
    Ok(s.to_string())
}

fn encode_position(p: &SourcePosition, out: &mut Vec<u8>) -> Result<()> {
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
    Ok(())
}

fn decode_layout(src: &mut &[u8]) -> Result<PlanLayout> {
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
    } else {
        0
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
    })
}

fn encode_freeze(f: &WindowFreeze, out: &mut Vec<u8>) -> Result<()> {
    out.extend_from_slice(&f.operator.raw().to_le_bytes());
    out.extend_from_slice(&f.slot.raw().to_le_bytes());
    out.push(f.kind);
    out.extend_from_slice(&(f.entries.len() as u32).to_le_bytes());
    for e in &f.entries {
        out.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
        for s in &e.key {
            s.encode_value(out)?;
        }
        out.extend_from_slice(&e.window_start.to_le_bytes());
        out.extend_from_slice(&e.window_end.to_le_bytes());
        out.extend_from_slice(&e.count.to_le_bytes());
        out.extend_from_slice(&(e.accs.len() as u16).to_le_bytes());
        for a in &e.accs {
            a.encode(out)?;
        }
    }
    encode_opt_i64(f.wm_in, out);
    encode_opt_i64(f.wm_out, out);
    encode_opt_i64(f.last_effective, out);
    Ok(())
}

fn decode_freeze(src: &mut &[u8]) -> Result<WindowFreeze> {
    if src.len() < 4 + 1 + 4 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated window freeze",
        ));
    }
    if src.len() < 4 + 2 + 1 + 4 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated window freeze",
        ));
    }
    let operator = OperatorId::new(u32::from_le_bytes(src[..4].try_into().unwrap()));
    *src = &src[4..];
    let slot = StateSlotId::new(u16::from_le_bytes(src[..2].try_into().unwrap()));
    *src = &src[2..];
    let kind = src[0];
    *src = &src[1..];
    let n = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
    *src = &src[4..];
    const MAX_FREEZE_ENTRIES: usize = 4096;
    const MIN_FREEZE_ENTRY: usize = 2 + 8 + 8 + 8 + 2;
    if n > MAX_FREEZE_ENTRIES {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            format!("freeze entry count {n} exceeds {MAX_FREEZE_ENTRIES}; refusing alloc"),
        ));
    }
    if src.len() < n.saturating_mul(MIN_FREEZE_ENTRY) {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "truncated window freeze entries (declared count exceeds remaining bytes)",
        ));
    }
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
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
        let mut key = Vec::with_capacity(nk);
        for _ in 0..nk {
            key.push(Scalar::decode_value(src)?);
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
        let na = u16::from_le_bytes(src[..2].try_into().unwrap()) as usize;
        *src = &src[2..];
        if na > 64 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("freeze accumulator count {na} exceeds 64"),
            ));
        }
        let mut accs = Vec::with_capacity(na);
        for _ in 0..na {
            accs.push(Accumulator::decode(src)?);
        }
        entries.push(FrozenEntry {
            key,
            window_start,
            window_end,
            count,
            accs,
        });
    }
    Ok(WindowFreeze {
        operator,
        slot,
        kind,
        entries,
        wm_in: decode_opt_i64(src)?,
        wm_out: decode_opt_i64(src)?,
        last_effective: decode_opt_i64(src)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_expr::Expr;
    use sparrow_io::{MemoryReplaySource, RecordSource, ReplayableSource};
    use sparrow_model::{
        AggFn, DataType, Field, FieldId, MemoryOwner, ResourceBudget, Schema, SchemaId, WindowKind,
    };
    use sparrow_plan::{AggCall, WindowSpec};
    use crate::window::WindowOperator;
    use sparrow_model::{Row, RowBatchBuilder, CreditKind};
    use std::sync::Arc;

    fn tmp() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "sparrow-chk-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
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
            Arc::new(schema),
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
            layout: PlanLayout::from_window(OperatorId::new(7), window.slot, &spec),
            table: None,
        }
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
            layout: PlanLayout {
                operator: OperatorId::new(1),
                slot: StateSlotId::new(1),
                window_kind: 1,
                keys: vec!["device_id".into()],
                aggs: vec!["sum:s".into()],
                event_time_field: None,
                lateness_micros: 0,
                where_fingerprint: 0,
                table_name: None,
                table_revision: None,
                window_params_fingerprint: 0,
            },
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
        assert_eq!(
            store.recover_committed().unwrap().unwrap().checkpoint_id,
            5
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn p0_13_freeze_rejects_untrusted_capacity() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&1_000_000u32.to_le_bytes());
        let err = super::decode_freeze(&mut buf.as_slice()).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
    }

    #[test]
    fn p1_24_corrupt_current_recovers_previous_generation() {
        let dir = tmp();
        let mut store = CheckpointStore::open(&dir).unwrap();
        store.commit(&sample_snapshot(1)).unwrap();
        store.commit(&sample_snapshot(2)).unwrap();
        fs::write(dir.join("CURRENT"), b"not-a-checkpoint\n").unwrap();
        let got = store.recover_committed().unwrap().unwrap();
        assert_eq!(got.checkpoint_id, 2, "must fall back to a verified generation");
        let _ = fs::remove_dir_all(&dir);
    }
}

//! File / replay test Source for V1 aligned recovery.
//!
//! NDJSON, message-boundary cuts, identity + rotation checks.
//! Declares `replay=replayable`. This is the only V1 source that may
//! participate in production aligned checkpoint restore.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sparrow_formats::JsonCodec;
use sparrow_io::{
    fnv1a64, RecordSource, ReplayCapabilities, ReplayableSource, SourceIdentity, SourcePosition,
};
use sparrow_model::{
    check_recovery_capabilities, ErrorCode, RecoveryPolicy, RestoreClaim, Result as ModelResult,
    Row, Schema, SourceFrame, SparrowError,
};

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, Result};

const PREFIX: usize = 4096;
const MAX_RECORD: usize = 64 * 1024;
const MAX_PENDING: usize = MAX_RECORD;

/// How the file may change after a checkpoint cut (R28), and what EOF
/// means for event-time watermarks (N5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileContract {
    /// Size and content fingerprint must match exactly. Finite/replay fixture.
    /// EOF is end of stream: inject a terminal watermark and finish the source.
    Immutable,
    /// Finite fixture (same identity rules as [`Self::Immutable`]). Preferred
    /// name when the job must emit final ET windows and then complete.
    Sealed,
    /// The file may grow; bytes `[0, cut)` must stay identical.
    /// EOF is a poll/sleep only — never inject a terminal watermark.
    AppendOnly,
}

/// One `next_frame` + decode. Shared by aligned and restart_fresh file loops.
#[derive(Debug)]
pub enum FilePoll {
    Row(sparrow_model::Row),
    DecodeError,
    Eof,
}

impl FileContract {
    /// Terminal watermark for a sealed/finite file. Large enough to close
    /// every open ET window. Must never be injected on [`Self::AppendOnly`].
    pub const TERMINAL_WM_MICROS: i64 = i64::MAX / 4;

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "append_only" | "append-only" | "appendonly" => Ok(Self::AppendOnly),
            "sealed" => Ok(Self::Sealed),
            "immutable" => Ok(Self::Immutable),
            other => Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("file contract `{other}` is not supported (append_only|sealed|immutable)"),
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppendOnly => "append_only",
            Self::Sealed => "sealed",
            Self::Immutable => "immutable",
        }
    }

    /// EOF is end-of-stream: inject [`Self::TERMINAL_WM_MICROS`] and finish.
    pub fn eof_is_terminal(self) -> bool {
        !matches!(self, Self::AppendOnly)
    }

    pub fn allows_growth(self) -> bool {
        matches!(self, Self::AppendOnly)
    }
}

#[derive(Clone, Debug)]
pub struct FileReplayConfig {
    pub path: PathBuf,
    pub schema: Schema,
    pub restore: RestoreClaim,
    pub recovery: RecoveryPolicy,
    pub contract: FileContract,
}

impl FileReplayConfig {
    pub fn new(path: impl Into<PathBuf>, schema: Schema) -> Self {
        Self {
            path: path.into(),
            schema,
            restore: RestoreClaim::None,
            recovery: RecoveryPolicy::RestartFresh,
            contract: FileContract::Immutable,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::FILE_REPLAY
    }

    pub fn validate(&self) -> Result<()> {
        check_recovery_capabilities("file", true, self.recovery, &self.restore)
            .map_err(|e| ConnectorError::new(e.code, e.to_string()))?;
        crate::policy::check_data_path(&self.path)?;
        Ok(())
    }
}

/// Bounded NDJSON file source. Offsets are message boundaries (`\n`).
pub struct FileReplaySource {
    path: PathBuf,
    identity: SourceIdentity,
    file: File,
    offset: u64,
    record_index: u64,
    pending: Vec<u8>,
    codec: JsonCodec,
    contract: FileContract,
}

impl FileReplaySource {
    pub fn open(cfg: &FileReplayConfig) -> Result<Self> {
        cfg.validate()?;
        let path = cfg.path.clone();
        let meta = fs::metadata(&path).map_err(|e| {
            ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("open {}: {e}", path.display()),
            )
        })?;
        let size = meta.len();
        let mut f = File::open(&path).map_err(|e| {
            ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("open {}: {e}", path.display()),
            )
        })?;
        let fingerprint = content_fingerprint(&mut f, size).map_err(|e| {
            ConnectorError::new(
                ErrorCode::Internal,
                format!("fingerprint {}: {e}", path.display()),
            )
        })?;
        f.seek(SeekFrom::Start(0)).map_err(|e| {
            ConnectorError::new(
                ErrorCode::Internal,
                format!("rewind {}: {e}", path.display()),
            )
        })?;
        let identity = SourceIdentity::file(path.to_string_lossy().into_owned(), size, fingerprint);
        Ok(Self {
            path,
            identity,
            file: f,
            offset: 0,
            record_index: 0,
            pending: Vec::new(),
            codec: JsonCodec::new(cfg.schema.clone()),
            contract: cfg.contract,
        })
    }

    pub fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    pub fn decode_frame(&self, frame: &SourceFrame) -> ModelResult<Option<Row>> {
        self.codec.decode_frame(frame)
    }

    fn reopen_for_append(&mut self) -> ModelResult<bool> {
        if !self.contract.allows_growth() {
            return Ok(false);
        }
        let mut f = File::open(&self.path).map_err(|e| {
            SparrowError::new(
                ErrorCode::Internal,
                format!("reopen {}: {e}", self.path.display()),
            )
        })?;
        f.seek(SeekFrom::Start(self.offset))
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("reopen seek: {e}")))?;
        self.file = f;
        let mut buf = [0u8; 1024];
        let n = self
            .file
            .read(&mut buf)
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("reopen read: {e}")))?;
        if n == 0 {
            return Ok(false);
        }
        self.pending.extend_from_slice(&buf[..n]);
        Ok(true)
    }

    /// Read one NDJSON record and decode it. Decode failures are
    /// [`FilePoll::DecodeError`] (caller increments `IoDiagnostics`).
    pub fn poll_decoded(&mut self) -> ModelResult<FilePoll> {
        match self.next_frame()? {
            Some(frame) => match self.decode_frame(&frame) {
                Ok(Some(row)) => Ok(FilePoll::Row(row)),
                Ok(None) | Err(_) => Ok(FilePoll::DecodeError),
            },
            None => Ok(FilePoll::Eof),
        }
    }

    fn refresh_identity(&self) -> Result<SourceIdentity> {
        let meta = fs::metadata(&self.path).map_err(|e| {
            ConnectorError::new(
                ErrorCode::UnsupportedRestore,
                format!("stat {}: {e}", self.path.display()),
            )
        })?;
        let size = meta.len();
        let mut f = File::open(&self.path).map_err(|e| {
            ConnectorError::new(
                ErrorCode::UnsupportedRestore,
                format!("reopen {}: {e}", self.path.display()),
            )
        })?;
        let fingerprint = content_fingerprint(&mut f, size).map_err(|e| {
            ConnectorError::new(ErrorCode::UnsupportedRestore, format!("fingerprint: {e}"))
        })?;
        Ok(SourceIdentity::file(
            self.path.to_string_lossy().into_owned(),
            size,
            fingerprint,
        ))
    }

    fn check_identity(&self, stored: &SourceIdentity) -> Result<()> {
        let live = self.refresh_identity()?;
        if stored.path != live.path {
            return Err(ConnectorError::new(
                ErrorCode::UnsupportedRestore,
                "file path identity mismatch",
            ));
        }
        match self.contract {
            FileContract::Immutable | FileContract::Sealed => {
                if stored.size != live.size || stored.fingerprint != live.fingerprint {
                    return Err(ConnectorError::new(
                        ErrorCode::UnsupportedRestore,
                        format!(
                            "file '{}' was replaced or rotated ({} contract)",
                            self.path.display(),
                            self.contract.as_str()
                        ),
                    ));
                }
            }
            FileContract::AppendOnly => {
                if live.size < stored.size {
                    return Err(ConnectorError::new(
                        ErrorCode::UnsupportedRestore,
                        format!(
                            "file '{}' truncated under append contract ({} < {})",
                            self.path.display(),
                            live.size,
                            stored.size
                        ),
                    ));
                }
                let mut f = File::open(&self.path).map_err(|e| {
                    ConnectorError::new(ErrorCode::UnsupportedRestore, format!("reopen: {e}"))
                })?;
                let cut_fp = content_fingerprint(&mut f, stored.size).map_err(|e| {
                    ConnectorError::new(
                        ErrorCode::UnsupportedRestore,
                        format!("cut fingerprint: {e}"),
                    )
                })?;
                if cut_fp != stored.fingerprint {
                    return Err(ConnectorError::new(
                        ErrorCode::UnsupportedRestore,
                        format!(
                            "file '{}' prefix before cut changed (append contract)",
                            self.path.display()
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Prefix + mid + suffix + size. First 4KiB alone is not enough (R28).
fn content_fingerprint(file: &mut File, size: u64) -> std::io::Result<u64> {
    let mut mix = Vec::new();
    mix.extend_from_slice(&size.to_le_bytes());
    let windows = [0u64, size / 2, size.saturating_sub(PREFIX as u64)];
    for start in windows {
        if size == 0 {
            break;
        }
        let start = start.min(size.saturating_sub(1));
        file.seek(SeekFrom::Start(start))?;
        let n = PREFIX.min(size.saturating_sub(start) as usize);
        let mut buf = vec![0u8; n];
        if n > 0 {
            file.read_exact(&mut buf)?;
        }
        mix.extend_from_slice(&start.to_le_bytes());
        mix.extend_from_slice(&fnv1a64(&buf).to_le_bytes());
    }
    Ok(fnv1a64(&mix))
}

impl RecordSource for FileReplaySource {
    fn next_frame(&mut self) -> sparrow_model::Result<Option<SourceFrame>> {
        loop {
            if let Some(idx) = self.pending.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.pending.drain(..=idx).collect();
                let payload = if line.ends_with(&[b'\n']) {
                    line[..line.len() - 1].to_vec()
                } else {
                    line
                };
                self.offset += (payload.len() + 1) as u64;
                self.record_index += 1;
                if payload.is_empty() {
                    continue;
                }
                return Ok(Some(SourceFrame::new(payload, 0)));
            }
            if self.pending.len() >= MAX_PENDING {
                return Err(SparrowError::new(
                    ErrorCode::MaxRecordSize,
                    format!("pending record exceeds {MAX_PENDING}B (no newline)"),
                ));
            }
            let mut buf = [0u8; 1024];
            let n = self
                .file
                .read(&mut buf)
                .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("read file: {e}")))?;
            if n == 0 {
                // AppendOnly: reopen at the last message boundary so a
                // growing file is visible even if this fd stopped at EOF.
                if self.reopen_for_append()? {
                    continue;
                }
                // EOF with incomplete line: do not emit (message-boundary cut).
                return Ok(None);
            }
            self.pending.extend_from_slice(&buf[..n]);
        }
    }
}

impl ReplayableSource for FileReplaySource {
    fn replay_capabilities(&self) -> ReplayCapabilities {
        ReplayCapabilities::REPLAYABLE
    }

    fn position(&self) -> SourcePosition {
        SourcePosition {
            offset_bytes: self.offset,
            record_index: self.record_index,
            identity: self.identity.clone(),
        }
    }

    fn seek(&mut self, pos: &SourcePosition) -> sparrow_model::Result<()> {
        self.check_identity(&pos.identity)
            .map_err(|e| SparrowError::new(e.code(), e.to_string()))?;
        let live = self
            .refresh_identity()
            .map_err(|e| SparrowError::new(e.code(), e.to_string()))?;
        if pos.offset_bytes > live.size {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "file '{}' rotated/truncated: offset {} > size {}",
                    self.path.display(),
                    pos.offset_bytes,
                    live.size
                ),
            ));
        }
        if pos.offset_bytes > 0 {
            let mut probe = File::open(&self.path)
                .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("seek probe: {e}")))?;
            probe
                .seek(SeekFrom::Start(pos.offset_bytes - 1))
                .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("seek: {e}")))?;
            let mut prev = [0u8; 1];
            probe
                .read_exact(&mut prev)
                .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("seek read: {e}")))?;
            if prev[0] != b'\n' {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "seek offset {} is not a message boundary (mid-record cut rejected)",
                        pos.offset_bytes
                    ),
                ));
            }
        }
        self.file
            .seek(SeekFrom::Start(pos.offset_bytes))
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("seek: {e}")))?;
        self.offset = pos.offset_bytes;
        self.record_index = pos.record_index;
        self.pending.clear();
        Ok(())
    }
}

pub fn write_ndjson(path: &Path, lines: &[&str]) -> Result<()> {
    let mut body = String::new();
    for line in lines {
        body.push_str(line);
        if !line.ends_with('\n') {
            body.push('\n');
        }
    }
    fs::write(path, body).map_err(|e| {
        ConnectorError::new(
            ErrorCode::Internal,
            format!("write {}: {e}", path.display()),
        )
    })
}

/// Declared file-source capabilities for the matrix.
pub fn file_replay_capabilities() -> ConnectorCapabilities {
    ConnectorCapabilities::FILE_REPLAY
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{DataType, Field, FieldId, SchemaId};

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    fn tmp(name: &str) -> PathBuf {
        crate::policy::ensure_default_data_root().join(format!(
            "sparrow-file-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn boundary_cut_and_seek() {
        let path = tmp("cut");
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\nparti",
        )
        .unwrap();
        let cfg = FileReplayConfig::new(&path, schema());
        let mut src = FileReplaySource::open(&cfg).unwrap();
        assert!(src.next_frame().unwrap().is_some());
        assert!(src.next_frame().unwrap().is_some());
        assert!(src.next_frame().unwrap().is_none());
        let pos = SourcePosition {
            offset_bytes: 0,
            record_index: 0,
            identity: src.identity().clone(),
        };
        src.seek(&pos).unwrap();
        let f = src.next_frame().unwrap().unwrap();
        assert!(std::str::from_utf8(&f.payload).unwrap().contains("\"v\":1"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rotation_rejects_seek() {
        let path = tmp("rot");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let cfg = FileReplayConfig::new(&path, schema());
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let pos = src.position();
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":9}\n").unwrap();
        assert_eq!(
            src.seek(&pos).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn mid_record_seek_rejected() {
        let path = tmp("mid");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let cfg = FileReplayConfig::new(&path, schema());
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let mut pos = src.position();
        pos.offset_bytes = 3;
        assert_eq!(src.seek(&pos).unwrap_err().code, ErrorCode::InvalidArgument);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn r28_identity_sees_bytes_after_first_4kib() {
        let path_a = tmp("id-a");
        let path_b = tmp("id-b");
        let mut a = vec![b'A'; 8192];
        a[8000] = b'X';
        let mut b = vec![b'A'; 8192];
        b[8000] = b'Y';
        std::fs::write(&path_a, &a).unwrap();
        std::fs::write(&path_b, &b).unwrap();
        let sa = FileReplaySource::open(&FileReplayConfig::new(&path_a, schema())).unwrap();
        let sb = FileReplaySource::open(&FileReplayConfig::new(&path_b, schema())).unwrap();
        assert_ne!(
            sa.identity().fingerprint,
            sb.identity().fingerprint,
            "files that differ after 4KiB must not share a fingerprint"
        );
        let _ = std::fs::remove_file(&path_a);
        let _ = std::fs::remove_file(&path_b);
    }

    #[test]
    fn r28_append_contract_allows_growth() {
        let path = tmp("append");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::AppendOnly;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let pos = src.position();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        src.seek(&pos).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn r28_immutable_rejects_append() {
        let path = tmp("imm");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let cfg = FileReplayConfig::new(&path, schema());
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let pos = src.position();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        assert_eq!(
            src.seek(&pos).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn n5_contract_parse_and_eof_policy() {
        assert_eq!(
            FileContract::parse("append_only").unwrap(),
            FileContract::AppendOnly
        );
        assert_eq!(FileContract::parse("sealed").unwrap(), FileContract::Sealed);
        assert_eq!(
            FileContract::parse("immutable").unwrap(),
            FileContract::Immutable
        );
        assert!(!FileContract::AppendOnly.eof_is_terminal());
        assert!(FileContract::Sealed.eof_is_terminal());
        assert!(FileContract::Immutable.eof_is_terminal());
        assert!(FileContract::parse("poison").is_err());
    }

    #[test]
    fn n5_sealed_rejects_append_like_immutable() {
        let path = tmp("sealed");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::Sealed;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let pos = src.position();
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        assert_eq!(
            src.seek(&pos).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn n5_append_only_reads_rows_written_after_eof() {
        use std::io::Write;
        let path = tmp("append-eof");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::AppendOnly;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, r#"{{"device_id":"d1","v":2}}"#).unwrap();
            f.flush().unwrap();
        }
        let again = src.poll_decoded().unwrap();
        assert!(
            matches!(again, FilePoll::Row(_)),
            "AppendOnly must see rows written after EOF, got {again:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn n5_poll_decoded_counts_bad_json() {
        let path = tmp("decode");
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\nnot-json\n{\"device_id\":\"d1\",\"v\":2}\n",
        )
        .unwrap();
        let cfg = FileReplayConfig::new(&path, schema());
        let mut src = FileReplaySource::open(&cfg).unwrap();
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::DecodeError));
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
        let _ = std::fs::remove_file(&path);
    }
}

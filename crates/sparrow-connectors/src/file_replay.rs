//! File / replay test Source for V1 aligned recovery.
//!
//! NDJSON, message-boundary cuts, identity + rotation checks.
//! Declares `replay=replayable`. This is the only V1 source that may
//! participate in production aligned checkpoint restore.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
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
    /// Input scan budget exhausted, not terminal EOF. Poll again after yielding.
    Pending,
    Eof,
}

enum FramePoll { Frame(SourceFrame), Pending, Eof }

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
    /// P1-17: decode errors fail the source (and the job) instead of only counting.
    pub fail_on_decode: bool,
}

impl FileReplayConfig {
    pub fn new(path: impl Into<PathBuf>, schema: Schema) -> Self {
        Self {
            path: path.into(),
            schema,
            restore: RestoreClaim::None,
            recovery: RecoveryPolicy::RestartFresh,
            contract: FileContract::Immutable,
            fail_on_decode: false,
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
    file: BufReader<File>,
    offset: u64,
    record_index: u64,
    pending: Vec<u8>,
    record_bytes: u64,
    discarding: bool,
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
            file: BufReader::with_capacity(16 * 1024, f),
            offset: 0,
            record_index: 0,
            pending: Vec::new(),
            record_bytes: 0,
            discarding: false,
            codec: JsonCodec::new(cfg.schema.clone()).with_fail_on_decode(cfg.fail_on_decode),
            contract: cfg.contract,
        })
    }

    pub fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    pub fn decode_frame(&self, frame: &SourceFrame) -> ModelResult<Option<Row>> {
        self.codec.decode_frame(frame)
    }

    /// Read one NDJSON record and decode it. Decode failures are
    /// [`FilePoll::DecodeError`] (caller increments `IoDiagnostics`).
    pub fn poll_decoded(&mut self) -> ModelResult<FilePoll> {
        match self.read_frame(2 * MAX_RECORD) {
            Ok(FramePoll::Frame(frame)) => match self.decode_frame(&frame) {
                Ok(Some(row)) => Ok(FilePoll::Row(row)),
                Ok(None) | Err(_) => Ok(FilePoll::DecodeError),
            },
            Ok(FramePoll::Eof) => Ok(FilePoll::Eof),
            Ok(FramePoll::Pending) => Ok(FilePoll::Pending),
            Err(e) if e.code == ErrorCode::MaxRecordSize => Ok(FilePoll::DecodeError),
            Err(e) => Err(e),
        }
    }

    /// Read up to `max_frames` records or about `max_bytes` of decoded
    /// rows, then return so the caller can leave `spawn_blocking` (N14).
    /// Stops early on EOF. AppendOnly/Sealed identity is unchanged —
    /// [`Self::poll_decoded`] still owns message-boundary cuts.
    pub fn poll_decoded_batch(
        &mut self,
        max_frames: usize,
        max_bytes: usize,
    ) -> ModelResult<Vec<FilePoll>> {
        let max_frames = max_frames.max(1);
        let mut out = Vec::new();
        let mut bytes = 0usize;
        loop {
            if out.len() >= max_frames {
                break;
            }
            let poll = self.poll_decoded()?;
            match &poll {
                FilePoll::Row(row) => {
                    let sz = row.tracked_bytes();
                    if !out.is_empty() && bytes.saturating_add(sz) > max_bytes {
                        out.push(poll);
                        break;
                    }
                    bytes = bytes.saturating_add(sz);
                    out.push(poll);
                }
                FilePoll::DecodeError => out.push(poll),
                FilePoll::Eof | FilePoll::Pending => {
                    out.push(poll);
                    break;
                }
            }
        }
        Ok(out)
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

impl FileReplaySource {
    // Complete a record before reporting an oversize decode error. A cut while
    // discarding stays at the previous boundary; restore safely replays it.
    fn finish_record(&mut self) -> ModelResult<Option<SourceFrame>> {
        self.offset += self.record_bytes;
        self.record_index += 1;
        self.record_bytes = 0;
        if std::mem::take(&mut self.discarding) {
            return Err(SparrowError::new(ErrorCode::MaxRecordSize,
                format!("record exceeds {MAX_RECORD}B; skipped to next boundary")));
        }
        if self.pending.last() == Some(&b'\n') { self.pending.pop(); }
        if self.pending.last() == Some(&b'\r') { self.pending.pop(); }
        if self.pending.is_empty() { return Ok(None); }
        Ok(Some(SourceFrame::new(std::mem::take(&mut self.pending), 0)))
    }

    fn read_frame(&mut self, mut scan_budget: usize) -> ModelResult<FramePoll> {
        loop {
            if scan_budget == 0 { return Ok(FramePoll::Pending); }
            let available = self.file.fill_buf().map_err(|e| {
                SparrowError::new(ErrorCode::Internal, format!("read file: {e}"))
            })?;
            if available.is_empty() {
                // Keep a partial record on the same descriptor. A later read
                // sees append data without rereading bytes already in pending.
                if !self.contract.eof_is_terminal() || self.record_bytes == 0 {
                    return Ok(FramePoll::Eof);
                }
                return Ok(self.finish_record()?.map_or(FramePoll::Eof, FramePoll::Frame));
            }
            let available = &available[..available.len().min(scan_budget)];
            let newline = available.iter().position(|&b| b == b'\n');
            let take = newline.map_or(available.len(), |i| i + 1);
            let payload_len = self.pending.len().saturating_add(take)
                .saturating_sub(usize::from(newline.is_some()));
            if payload_len > MAX_PENDING || self.discarding {
                self.pending.clear();
                self.discarding = true;
            } else {
                self.pending.extend_from_slice(&available[..take]);
            }
            self.file.consume(take);
            scan_budget -= take;
            self.record_bytes += take as u64;
            if newline.is_some() {
                if let Some(frame) = self.finish_record()? { return Ok(FramePoll::Frame(frame)); }
            }
        }
    }
}

impl RecordSource for FileReplaySource {
    fn next_frame(&mut self) -> ModelResult<Option<SourceFrame>> {
        loop {
            match self.read_frame(2 * MAX_RECORD)? {
                FramePoll::Frame(frame) => return Ok(Some(frame)),
                FramePoll::Eof => return Ok(None),
                FramePoll::Pending => continue,
            }
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
            if prev[0] != b'\n' && !(self.contract.eof_is_terminal() && pos.offset_bytes == live.size) {
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
        self.record_bytes = 0;
        self.discarding = false;
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
    fn r4_oversize_skips_once_at_boundary_for_all_contracts() {
        let path = tmp("r4-oversize");
        let bad = "x".repeat(70 * 1024);
        let body = format!("{bad}\n\r\n{{\"device_id\":\"ok\",\"v\":1}}\r\n");
        fs::write(&path, &body).unwrap();
        for contract in [FileContract::AppendOnly, FileContract::Sealed, FileContract::Immutable] {
            let mut cfg = FileReplayConfig::new(&path, schema());
            cfg.contract = contract;
            let mut src = FileReplaySource::open(&cfg).unwrap();
            assert!(matches!(src.poll_decoded().unwrap(), FilePoll::DecodeError));
            let cut = src.position();
            assert_eq!(cut.offset_bytes, bad.len() as u64 + 1);
            assert_eq!(cut.record_index, 1);
            assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
            assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
            assert_eq!(src.position().offset_bytes, body.len() as u64);
            let mut restored = FileReplaySource::open(&cfg).unwrap();
            restored.seek(&cut).unwrap();
            assert!(matches!(restored.poll_decoded().unwrap(), FilePoll::Row(_)));
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn r4_partial_oversize_is_bounded_and_never_false_terminal_eof() {
        use std::io::Write;
        let path = tmp("r4-large-partial");
        fs::write(&path, vec![b'x'; 1024 * 1024]).unwrap();
        for contract in [FileContract::AppendOnly, FileContract::Sealed, FileContract::Immutable] {
            let mut cfg = FileReplayConfig::new(&path, schema());
            cfg.contract = contract;
            let mut src = FileReplaySource::open(&cfg).unwrap();
            for _ in 0..8 {
                assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Pending));
                assert_eq!(src.position().offset_bytes, 0);
                assert!(src.pending.len() <= MAX_RECORD);
            }
            if contract.eof_is_terminal() {
                assert!(matches!(src.poll_decoded().unwrap(), FilePoll::DecodeError));
                assert_eq!(src.position().offset_bytes, 1024 * 1024);
                assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
            } else {
                for _ in 0..3 { assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof)); }
                assert_eq!(src.position().offset_bytes, 0);
            }
        }
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::AppendOnly;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        while matches!(src.poll_decoded().unwrap(), FilePoll::Pending) {}
        let cut = src.position();
        let mut append = fs::OpenOptions::new().append(true).open(&path).unwrap();
        append.write_all(b"x\n{\"device_id\":\"ok\",\"v\":1}\n").unwrap();
        append.flush().unwrap();
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::DecodeError));
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
        src.seek(&cut).unwrap();
        while matches!(src.poll_decoded().unwrap(), FilePoll::Pending) {}
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn r3_partial_append_is_bounded_and_resumes_exactly_once() {
        use std::io::Write;
        let path = tmp("r3-half");
        fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d2\",").unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::AppendOnly;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        let cut = src.position();
        for _ in 0..20 {
            assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
            assert_eq!(src.position(), cut);
            assert!(src.pending.len() < 64);
        }
        let mut writer = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(b"\"v\":2}\n{\"device_id\":\"d3\",\"v\":3}\n").unwrap();
        for expected in [2, 3] {
            let FilePoll::Row(row) = src.poll_decoded().unwrap() else { panic!("missing row") };
            assert_eq!(row.values[1], sparrow_model::Scalar::Int64(expected));
        }
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
        assert_eq!(src.position().record_index, 3);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn r3_terminal_unterminated_record_and_restore_boundary() {
        for contract in [FileContract::Sealed, FileContract::Immutable] {
            let path = tmp("r3-last");
            fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d2\",\"v\":2}").unwrap();
            let mut cfg = FileReplayConfig::new(&path, schema());
            cfg.contract = contract;
            let mut src = FileReplaySource::open(&cfg).unwrap();
            for _ in 0..2 { assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_))); }
            let cut = src.position();
            assert_eq!(cut.offset_bytes, fs::metadata(&path).unwrap().len());
            assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
            src.seek(&cut).unwrap();
            assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn boundary_cut_and_seek() {
        let path = tmp("cut");
        std::fs::write(
            &path,
            b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\nparti",
        )
        .unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::AppendOnly;
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
    fn n14_poll_decoded_batch_reads_frames_then_eof() {
        let path = tmp("n14-batch");
        let mut body = Vec::new();
        for i in 0..20 {
            body.extend(format!("{{\"device_id\":\"d1\",\"v\":{i}}}\n").into_bytes());
        }
        std::fs::write(&path, &body).unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::Sealed;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let polls = src.poll_decoded_batch(32, 64 * 1024).unwrap();
        let rows = polls
            .iter()
            .filter(|p| matches!(p, FilePoll::Row(_)))
            .count();
        assert_eq!(rows, 20, "one blocking batch should decode many frames");
        assert!(
            matches!(polls.last(), Some(FilePoll::Eof)),
            "sealed batch must end with EOF, got {:?}",
            polls.last()
        );
        assert_eq!(src.poll_decoded_batch(8, 1024).unwrap().len(), 1);
        assert!(matches!(
            src.poll_decoded_batch(8, 1024).unwrap()[0],
            FilePoll::Eof
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn n14_append_only_batch_eof_then_growth() {
        use std::io::Write;
        let path = tmp("n14-append");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n")
            .unwrap();
        let mut cfg = FileReplayConfig::new(&path, schema());
        cfg.contract = FileContract::AppendOnly;
        let mut src = FileReplaySource::open(&cfg).unwrap();
        let first = src.poll_decoded_batch(32, 64 * 1024).unwrap();
        assert_eq!(
            first
                .iter()
                .filter(|p| matches!(p, FilePoll::Row(_)))
                .count(),
            2
        );
        assert!(matches!(first.last(), Some(FilePoll::Eof)));
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, r#"{{"device_id":"d1","v":3}}"#).unwrap();
            f.flush().unwrap();
        }
        let again = src.poll_decoded_batch(32, 64 * 1024).unwrap();
        assert!(
            again.iter().any(|p| matches!(p, FilePoll::Row(_))),
            "AppendOnly batch must see growth after EOF, got {again:?}"
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

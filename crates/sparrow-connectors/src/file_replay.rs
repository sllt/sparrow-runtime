//! File / replay test Source for experimental recovery.
//!
//! NDJSON, message-boundary cuts, identity + rotation checks.
//! Declares `replay=replayable`. This is the only V0.4 source that may
//! participate in experimental aligned checkpoint restore.

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

#[derive(Clone, Debug)]
pub struct FileReplayConfig {
    pub path: PathBuf,
    pub schema: Schema,
    pub restore: RestoreClaim,
    pub recovery: RecoveryPolicy,
}

impl FileReplayConfig {
    pub fn new(path: impl Into<PathBuf>, schema: Schema) -> Self {
        Self {
            path: path.into(),
            schema,
            restore: RestoreClaim::None,
            recovery: RecoveryPolicy::RestartFresh,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::FILE_REPLAY
    }

    pub fn validate(&self) -> Result<()> {
        check_recovery_capabilities(
            "file",
            true,
            self.recovery,
            &self.restore,
        )
        .map_err(|e| ConnectorError::new(e.code, e.to_string()))?;
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
}

impl FileReplaySource {
    pub fn open(cfg: &FileReplayConfig) -> Result<Self> {
        cfg.validate()?;
        let path = cfg.path.clone();
        let meta = fs::metadata(&path).map_err(|e| {
            ConnectorError::new(ErrorCode::InvalidArgument, format!("open {}: {e}", path.display()))
        })?;
        let size = meta.len();
        let mut f = File::open(&path).map_err(|e| {
            ConnectorError::new(ErrorCode::InvalidArgument, format!("open {}: {e}", path.display()))
        })?;
        let mut prefix = vec![0u8; PREFIX.min(size as usize)];
        f.read_exact(&mut prefix).map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("fingerprint {}: {e}", path.display()))
        })?;
        f.seek(SeekFrom::Start(0)).map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("rewind {}: {e}", path.display()))
        })?;
        let identity = SourceIdentity::file(path.to_string_lossy().into_owned(), size, fnv1a64(&prefix));
        Ok(Self {
            path,
            identity,
            file: f,
            offset: 0,
            record_index: 0,
            pending: Vec::new(),
            codec: JsonCodec::new(cfg.schema.clone()),
        })
    }

    pub fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    pub fn decode_frame(&self, frame: &SourceFrame) -> ModelResult<Option<Row>> {
        self.codec.decode_frame(frame)
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
        let n = PREFIX.min(size as usize);
        let mut prefix = vec![0u8; n];
        if n > 0 {
            f.read_exact(&mut prefix).map_err(|e| {
                ConnectorError::new(ErrorCode::UnsupportedRestore, format!("fingerprint: {e}"))
            })?;
        }
        Ok(SourceIdentity::file(
            self.path.to_string_lossy().into_owned(),
            size,
            fnv1a64(&prefix),
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
        if stored.fingerprint != live.fingerprint {
            return Err(ConnectorError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "file '{}' was replaced or rotated (fingerprint changed)",
                    self.path.display()
                ),
            ));
        }
        Ok(())
    }
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
            let n = self.file.read(&mut buf).map_err(|e| {
                SparrowError::new(ErrorCode::Internal, format!("read file: {e}"))
            })?;
            if n == 0 {
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
            let mut probe = File::open(&self.path).map_err(|e| {
                SparrowError::new(ErrorCode::Internal, format!("seek probe: {e}"))
            })?;
            probe
                .seek(SeekFrom::Start(pos.offset_bytes - 1))
                .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("seek: {e}")))?;
            let mut prev = [0u8; 1];
            probe.read_exact(&mut prev).map_err(|e| {
                SparrowError::new(ErrorCode::Internal, format!("seek read: {e}"))
            })?;
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
        ConnectorError::new(ErrorCode::Internal, format!("write {}: {e}", path.display()))
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
        let p = std::env::temp_dir().join(format!(
            "sparrow-file-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    #[test]
    fn boundary_cut_and_seek() {
        let path = tmp("cut");
        std::fs::write(&path, b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\nparti").unwrap();
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
}

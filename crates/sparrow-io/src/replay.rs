//! Replayable source contract for V1 aligned recovery.
//!
//! A ReplayableSource can seek to a previously committed *message boundary*.
//! Mid-record cuts are never treated as a valid restore position.

use sparrow_model::error::{ErrorCode, Result, SparrowError};
use sparrow_model::SourceFrame;

use crate::{Decoder, RecordSource};

/// Connector-declared replay capability (honesty label).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplaySupport {
    Unsupported,
    Replayable,
}

impl ReplaySupport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Replayable => "replayable",
        }
    }

    pub const fn is_replayable(self) -> bool {
        matches!(self, Self::Replayable)
    }
}

/// Identity used to detect file replacement / rotation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceIdentity {
    pub kind: String,
    pub path: String,
    pub size: u64,
    pub fingerprint: u64,
}

impl SourceIdentity {
    pub fn memory(name: &str, size: u64, fingerprint: u64) -> Self {
        Self {
            kind: "memory".into(),
            path: name.into(),
            size,
            fingerprint,
        }
    }

    pub fn file(path: impl Into<String>, size: u64, fingerprint: u64) -> Self {
        Self {
            kind: "file".into(),
            path: path.into(),
            size,
            fingerprint,
        }
    }
}

/// Committed source cursor. `offset_bytes` must be a message boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePosition {
    pub offset_bytes: u64,
    pub record_index: u64,
    pub identity: SourceIdentity,
}

impl SourcePosition {
    pub fn start(identity: SourceIdentity) -> Self {
        Self {
            offset_bytes: 0,
            record_index: 0,
            identity,
        }
    }
}

/// Declared replay capabilities for conformance tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayCapabilities {
    pub replay: ReplaySupport,
    pub message_boundary: bool,
    pub identity_check: bool,
}

impl ReplayCapabilities {
    pub const UNSUPPORTED: Self = Self {
        replay: ReplaySupport::Unsupported,
        message_boundary: false,
        identity_check: false,
    };

    pub const REPLAYABLE: Self = Self {
        replay: ReplaySupport::Replayable,
        message_boundary: true,
        identity_check: true,
    };
}

/// Seekable, message-boundary source used by experimental checkpoint restore.
pub trait ReplayableSource: RecordSource {
    fn replay_capabilities(&self) -> ReplayCapabilities;
    fn position(&self) -> SourcePosition;
    fn seek(&mut self, pos: &SourcePosition) -> Result<()>;
}

/// FNV-1a 64 used for identity fingerprints (no extra crate).
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// In-memory NDJSON replay source for recovery experiments.
///
/// Records are whole lines (trailing `\n` included in the offset). A cut
/// that lands mid-line is **not** a valid seek target.
pub struct MemoryReplaySource {
    name: String,
    data: Vec<u8>,
    identity: SourceIdentity,
    offset: u64,
    record_index: u64,
}

impl MemoryReplaySource {
    pub fn new(name: impl Into<String>, data: impl Into<Vec<u8>>) -> Self {
        let name = name.into();
        let data = data.into();
        let identity = SourceIdentity::memory(&name, data.len() as u64, fnv1a64(&data));
        Self {
            name,
            data,
            identity,
            offset: 0,
            record_index: 0,
        }
    }

    pub fn from_lines(name: impl Into<String>, lines: &[&str]) -> Self {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line.as_bytes());
            if !line.ends_with('\n') {
                data.push(b'\n');
            }
        }
        Self::new(name, data)
    }

    pub fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Byte offsets that are legal message boundaries (start + after each `\n`).
    pub fn boundaries(&self) -> Vec<u64> {
        let mut out = vec![0u64];
        for (i, b) in self.data.iter().enumerate() {
            if *b == b'\n' {
                out.push((i + 1) as u64);
            }
        }
        out
    }

    fn at_boundary(data: &[u8], offset: u64) -> bool {
        if offset == 0 {
            return true;
        }
        if offset as usize > data.len() {
            return false;
        }
        data.get(offset as usize - 1) == Some(&b'\n')
    }
}

impl RecordSource for MemoryReplaySource {
    fn next_frame(&mut self) -> Result<Option<SourceFrame>> {
        let start = self.offset as usize;
        if start >= self.data.len() {
            return Ok(None);
        }
        let rest = &self.data[start..];
        match rest.iter().position(|&b| b == b'\n') {
            None => {
                // Incomplete trailing record: do not emit (message-boundary cut).
                Ok(None)
            }
            Some(rel) => {
                let end = start + rel; // exclude newline
                let payload = self.data[start..end].to_vec();
                self.offset = (end + 1) as u64;
                self.record_index += 1;
                Ok(Some(SourceFrame::new(payload, 0)))
            }
        }
    }
}

impl ReplayableSource for MemoryReplaySource {
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

    fn seek(&mut self, pos: &SourcePosition) -> Result<()> {
        if pos.identity.kind != self.identity.kind || pos.identity.path != self.identity.path {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "source identity mismatch: stored {}/{} current {}/{}",
                    pos.identity.kind, pos.identity.path, self.identity.kind, self.identity.path
                ),
            ));
        }
        if pos.identity.fingerprint != self.identity.fingerprint {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "source '{}' was replaced or rotated (fingerprint changed)",
                    self.name
                ),
            ));
        }
        if pos.offset_bytes as usize > self.data.len() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "source '{}' rotated or truncated: offset {} > size {}",
                    self.name,
                    pos.offset_bytes,
                    self.data.len()
                ),
            ));
        }
        if !Self::at_boundary(&self.data, pos.offset_bytes) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "seek offset {} is not a message boundary (mid-record cut rejected)",
                    pos.offset_bytes
                ),
            ));
        }
        self.offset = pos.offset_bytes;
        self.record_index = pos.record_index;
        Ok(())
    }
}

/// Decode helper used by replay sources after a frame is cut at a boundary.
pub fn decode_or_skip<D: Decoder>(
    decoder: &mut D,
    frame: &SourceFrame,
    bounds: &sparrow_model::CodecBounds,
) -> Result<Option<sparrow_model::RowBatch>> {
    match decoder.decode(frame, bounds) {
        Ok(batch) => Ok(Some(batch)),
        Err(e) if e.code == ErrorCode::CodecViolation => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_boundary_cut_does_not_emit_partial() {
        let mut src = MemoryReplaySource::new("cut", b"one\ntwo\nparti".to_vec());
        assert_eq!(src.next_frame().unwrap().unwrap().payload, b"one");
        assert_eq!(src.next_frame().unwrap().unwrap().payload, b"two");
        assert!(src.next_frame().unwrap().is_none());
        assert_eq!(src.position().record_index, 2);
        assert_eq!(src.position().offset_bytes, 8);
    }

    #[test]
    fn mid_record_seek_rejected() {
        let mut src = MemoryReplaySource::from_lines("m", &["aaaa", "bbbb"]);
        let mut pos = src.position();
        pos.offset_bytes = 2;
        assert_eq!(
            src.seek(&pos).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn identity_rotation_rejected() {
        let src = MemoryReplaySource::from_lines("rot", &["a", "b"]);
        let pos = src.position();
        let mut other = MemoryReplaySource::from_lines("rot", &["x", "y"]);
        assert_eq!(
            other.seek(&pos).unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
    }

    #[test]
    fn seek_to_boundary_replays_tail() {
        let mut src = MemoryReplaySource::from_lines("ok", &["a", "b", "c"]);
        src.next_frame().unwrap();
        let pos = src.position();
        src.next_frame().unwrap();
        src.seek(&pos).unwrap();
        assert_eq!(src.next_frame().unwrap().unwrap().payload, b"b");
        assert_eq!(src.next_frame().unwrap().unwrap().payload, b"c");
    }
}

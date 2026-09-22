//! One durable decision, protected by the checkpoint-directory writer lock.
//! This is NOT a general replay WAL. Only CURRENT and its immediate successor
//! are supported; a decision cannot be replaced until its cut is committed.
use serde::{Deserialize, Serialize};
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{ErrorCode, Result, SparrowError};
use sparrow_runtime::processing_cut::ProcessingCut;
use std::{
    io::{Read, Write},
    path::Path,
};

pub(super) const MAX_LOG_BYTES: usize = 128 * 1024;
pub(super) const WORKSPACE_BYTES: usize = MAX_LOG_BYTES * 4;

pub(super) fn fail(message: &str) -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        format!("paused time: {message}"),
    )
}
fn io(e: std::io::Error) -> SparrowError {
    fail(&format!("decision log I/O: {e}"))
}
pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}
pub(super) fn generation() -> Result<[u8; 16]> {
    use ring::rand::SecureRandom;
    let mut bytes = [0; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| fail("generation entropy unavailable"))?;
    if bytes == [0; 16] {
        return Err(fail("generation entropy invalid"));
    }
    Ok(bytes)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    offset: u64,
    records: u64,
    kind: String,
    path: String,
    size: u64,
    fingerprint: u64,
}
impl Position {
    fn from(source: &SourcePosition) -> Self {
        Self {
            offset: source.offset_bytes,
            records: source.record_index,
            kind: source.identity.kind.clone(),
            path: source.identity.path.clone(),
            size: source.identity.size,
            fingerprint: source.identity.fingerprint,
        }
    }
    fn source(&self) -> SourcePosition {
        SourcePosition {
            offset_bytes: self.offset,
            record_index: self.records,
            identity: SourceIdentity {
                kind: self.kind.clone(),
                path: self.path.clone(),
                size: self.size,
                fingerprint: self.fingerprint,
            },
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    pub generation: [u8; 16],
    pub semantics: [u8; 32],
    pub sequence: u64,
    pub micros: i64,
    pub ingested: u64,
    position: Position,
    pub row_hash: Option<[u8; 32]>,
}
impl Decision {
    pub fn new(
        generation: [u8; 16],
        semantics: [u8; 32],
        cut: ProcessingCut,
        ingested: u64,
        row_hash: Option<[u8; 32]>,
    ) -> Self {
        Self {
            generation,
            semantics,
            sequence: cut.sequence,
            micros: cut.micros,
            ingested,
            position: Position::from(&cut.source),
            row_hash,
        }
    }
    pub fn cut(&self) -> ProcessingCut {
        ProcessingCut {
            sequence: self.sequence,
            micros: self.micros,
            source: self.position.source(),
        }
    }
    fn validate(&self) -> Result<()> {
        self.cut().wrap()?;
        if self.generation == [0; 16]
            || (self.sequence == 0 && (self.ingested != 0 || self.row_hash.is_some()))
        {
            return Err(fail("invalid decision identity/bootstrap"));
        }
        Ok(())
    }
    /// Return true only for the sole uncommitted successor. Missing/gapped or
    /// mismatched history is never permission to sample a replacement clock.
    pub fn check(
        &self,
        generation: [u8; 16],
        semantics: [u8; 32],
        current: &ProcessingCut,
        ingested: u64,
    ) -> Result<bool> {
        self.validate()?;
        if self.generation != generation || self.semantics != semantics {
            return Err(fail("decision generation/semantics mismatch"));
        }
        if self.sequence == current.sequence {
            if self.cut() != *current || self.ingested != ingested {
                return Err(fail("decision and CURRENT disagree"));
            }
            return Ok(false);
        }
        if current.sequence.checked_add(1) != Some(self.sequence)
            || self.micros < current.micros
            || ingested.checked_add(u64::from(self.row_hash.is_some())) != Some(self.ingested)
            || (self.row_hash.is_none() && self.position.source() != current.source)
            || (self.row_hash.is_some()
                && (self.position.offset <= current.source.offset_bytes
                    || self.position.records <= current.source.record_index
                    || (self.position.kind == "jetstream-v1"
                        && self.position.records != current.source.record_index.saturating_add(1))))
        {
            return Err(fail("decision is not the immediate successor of CURRENT"));
        }
        Ok(true)
    }
}
pub(super) fn read(dir: &Path) -> Result<Option<Decision>> {
    let path = dir.join("TIME_PENDING");
    let meta = match std::fs::symlink_metadata(&path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    if !meta.file_type().is_file() || meta.len() > MAX_LOG_BYTES as u64 || meta.len() < 36 {
        return Err(fail("invalid decision log file/size"));
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    std::fs::File::open(&path)
        .map_err(io)?
        .take(MAX_LOG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    if bytes.len() > MAX_LOG_BYTES
        || bytes.len() < 36
        || &bytes[..4] != b"TPD1"
        || digest(&bytes[36..]) != bytes[4..36]
    {
        return Err(fail("decision log checksum/version mismatch"));
    }
    let value: Decision =
        serde_json::from_slice(&bytes[36..]).map_err(|_| fail("invalid decision log encoding"))?;
    value.validate()?;
    Ok(Some(value))
}
pub(super) fn write(dir: &Path, decision: &Decision) -> Result<()> {
    decision.validate()?;
    let bytes = serde_json::to_vec(decision).map_err(|_| fail("decision log encoding failed"))?;
    if bytes.len() + 36 > MAX_LOG_BYTES {
        return Err(fail("decision log exceeds bound"));
    }
    let tmp = dir.join("TIME_PENDING.tmp");
    // Previous interrupted temporary writes have no publication authority.
    // Refuse non-regular paths instead of following a symlink or deleting dirs.
    match std::fs::symlink_metadata(&tmp) {
        Ok(meta) if meta.file_type().is_file() => std::fs::remove_file(&tmp).map_err(io)?,
        Ok(_) => return Err(fail("decision temporary path is not regular")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(e)),
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(io)?;
    file.write_all(b"TPD1").map_err(io)?;
    file.write_all(&digest(&bytes)).map_err(io)?;
    file.write_all(&bytes).map_err(io)?;
    file.sync_all().map_err(io)?;
    std::fs::rename(&tmp, dir.join("TIME_PENDING")).map_err(io)?;
    std::fs::File::open(dir)
        .map_err(io)?
        .sync_all()
        .map_err(io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Scratch(std::path::PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn scratch() -> Scratch {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-time-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        Scratch(dir)
    }
    fn cut() -> ProcessingCut {
        ProcessingCut {
            sequence: 0,
            micros: 0,
            source: SourcePosition::start(SourceIdentity::file("fixture", 0, 1)),
        }
    }
    fn decision() -> Decision {
        Decision::new([1; 16], [2; 32], cut(), 0, None)
    }
    #[test]
    fn paused_time_log_roundtrip_checksum_and_truncation() {
        let dir = scratch();
        let decision = decision();
        write(&dir.0, &decision).unwrap();
        assert_eq!(read(&dir.0).unwrap(), Some(decision));
        let path = dir.0.join("TIME_PENDING");
        let original = std::fs::read(&path).unwrap();
        for n in [0, 4, 35, 36, original.len() - 1] {
            std::fs::write(&path, &original[..n]).unwrap();
            assert!(read(&dir.0).is_err());
        }
        let mut corrupt = original;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        assert!(read(&dir.0).is_err());
    }
    #[test]
    fn paused_time_log_successor_generation_source_and_clock_guards() {
        let base = cut();
        let initial = decision();
        assert!(!initial.check([1; 16], [2; 32], &base, 0).unwrap());
        let mut next = base.clone();
        next.sequence = 1;
        next.micros = 100;
        let good = Decision::new([1; 16], [2; 32], next, 0, None);
        assert!(good.check([1; 16], [2; 32], &base, 0).unwrap());
        for mutation in 0..7 {
            let mut bad = good.clone();
            match mutation {
                0 => bad.sequence = 2,
                1 => bad.micros = -1,
                2 => bad.generation = [3; 16],
                3 => bad.semantics = [3; 32],
                4 => bad.ingested = 1,
                5 => bad.position.offset = 1,
                _ => bad.row_hash = Some([0; 32]),
            }
            assert!(
                bad.check([1; 16], [2; 32], &base, 0).is_err(),
                "mutation {mutation}"
            );
        }
        let mut row = good;
        row.row_hash = Some([7; 32]);
        row.ingested = 1;
        row.position.offset = 100;
        row.position.records = 3;
        assert!(row.check([1; 16], [2; 32], &base, 0).unwrap()); // File blank lines count as records.
        assert!(row.check([1; 16], [2; 32], &row.cut(), 0).is_err());
        assert!(!row.check([1; 16], [2; 32], &row.cut(), 1).unwrap());
    }
    #[test]
    fn paused_time_log_failed_temporary_write_preserves_published_decision() {
        let dir = scratch();
        write(&dir.0, &decision()).unwrap();
        let before = std::fs::read(dir.0.join("TIME_PENDING")).unwrap();
        std::fs::create_dir(dir.0.join("TIME_PENDING.tmp")).unwrap();
        let mut next = decision();
        next.sequence = 1;
        next.micros = 100;
        assert!(write(&dir.0, &next).is_err());
        assert_eq!(std::fs::read(dir.0.join("TIME_PENDING")).unwrap(), before);
        std::fs::remove_dir(dir.0.join("TIME_PENDING.tmp")).unwrap();
        std::fs::write(dir.0.join("TIME_PENDING.tmp"), b"partial").unwrap();
        write(&dir.0, &next).unwrap();
        assert_eq!(read(&dir.0).unwrap(), Some(next));
    }
    #[test]
    fn paused_time_log_is_bounded_and_missing_is_distinct_from_corrupt() {
        let dir = scratch();
        assert!(read(&dir.0).unwrap().is_none());
        std::fs::create_dir(dir.0.join("TIME_PENDING")).unwrap();
        assert!(read(&dir.0).is_err());
        std::fs::remove_dir(dir.0.join("TIME_PENDING")).unwrap();
        std::fs::write(dir.0.join("TIME_PENDING"), vec![0; MAX_LOG_BYTES + 1]).unwrap();
        assert!(read(&dir.0).is_err());
        let mut value = decision();
        value.position.path = "x".repeat(MAX_LOG_BYTES);
        assert!(write(&dir.0, &value).is_err());
    }
}

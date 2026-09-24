//! OFD1 is a new decision protocol, never a TPD1 optional-field extension.
//! The profile-exclusive directory reuses the bounded TIME_PENDING filenames.
pub(super) use super::paused_time_log::{digest, generation};
use serde::{Deserialize, Serialize};
use sparrow_io::{
    feed::{FeedObservation, FeedReadiness},
    SourceIdentity, SourcePosition,
};
use sparrow_model::{ErrorCode, Result, SparrowError};
use sparrow_runtime::observed_cut::{Coverage, ObservedCut};
use std::{
    io::{Read, Write},
    path::Path,
};

pub(super) const MAX_LOG_BYTES: usize = 128 * 1024;
pub(super) const WORKSPACE_BYTES: usize = MAX_LOG_BYTES * 4;

pub(super) fn fail(message: &str) -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        format!("observed time: {message}"),
    )
}
fn io(error: std::io::Error) -> SparrowError {
    fail(&format!("decision log I/O: {error}"))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fact {
    tag: u8,
    pub head: u64,
}
impl Fact {
    pub fn from(observation: &FeedObservation) -> Self {
        let tag = match observation.readiness {
            FeedReadiness::CaughtUp => 1,
            FeedReadiness::Backlog => 2,
            FeedReadiness::PartialRecord => 3,
            FeedReadiness::InFlight => 4,
            FeedReadiness::Unverified => 5,
            FeedReadiness::Ended => 6,
        };
        Self {
            tag,
            head: observation.head,
        }
    }
    pub fn readiness(self) -> Result<FeedReadiness> {
        Ok(match self.tag {
            1 => FeedReadiness::CaughtUp,
            2 => FeedReadiness::Backlog,
            3 => FeedReadiness::PartialRecord,
            4 => FeedReadiness::InFlight,
            5 => FeedReadiness::Unverified,
            6 => FeedReadiness::Ended,
            _ => return Err(fail("unknown feed fact")),
        })
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
    since: Option<i64>,
    last_fresh: Option<i64>,
    pub fact: Option<Fact>,
    pub row_hash: Option<[u8; 32]>,
    /// The first new decision after recovery breaks continuity. Replaying a
    /// pending decision uses this saved bit, not the current attempt's status.
    pub restart: bool,
}
impl Decision {
    pub fn new(
        generation: [u8; 16],
        semantics: [u8; 32],
        cut: ObservedCut,
        ingested: u64,
        fact: Option<Fact>,
        row_hash: Option<[u8; 32]>,
        restart: bool,
    ) -> Result<Self> {
        let value = Self {
            generation,
            semantics,
            sequence: cut.sequence,
            micros: cut.micros,
            ingested,
            position: Position::from(&cut.source),
            since: cut.coverage.since,
            last_fresh: cut.coverage.last_fresh,
            fact,
            row_hash,
            restart,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn cut(&self) -> ObservedCut {
        ObservedCut {
            sequence: self.sequence,
            micros: self.micros,
            coverage: Coverage {
                since: self.since,
                last_fresh: self.last_fresh,
            },
            source: self.position.source(),
        }
    }
    /// Only a fresh CaughtUp fact authorizes this decision's timer drain.
    /// Data-only decisions may retain recent coverage but do not manufacture
    /// another observation from it.
    pub fn qualified_since(&self) -> Result<Option<i64>> {
        Ok(
            if self.fact.map(Fact::readiness).transpose()? == Some(FeedReadiness::CaughtUp) {
                self.since
            } else {
                None
            },
        )
    }
    fn validate(&self) -> Result<()> {
        self.cut().wrap()?;
        if self.generation == [0; 16] {
            return Err(fail("uninitialized decision generation"));
        }
        if self.sequence == 0 {
            if self.ingested != 0 || self.fact.is_some() || self.row_hash.is_some() || self.restart
            {
                return Err(fail("invalid observed-time bootstrap"));
            }
            return Ok(());
        }
        if self.fact.is_some() == self.row_hash.is_some() {
            return Err(fail("decision requires either input or a feed probe"));
        }
        if let Some(fact) = self.fact {
            let kind = fact.readiness()?;
            if fact.head < self.position.offset {
                return Err(fail("observed head behind source cut"));
            }
            if kind == FeedReadiness::CaughtUp {
                if fact.head != self.position.offset
                    || self.last_fresh != Some(self.micros)
                    || self.since.is_none()
                {
                    return Err(fail("caught-up fact disagrees with coverage/cut"));
                }
            } else if self.since.is_some() || self.last_fresh.is_some() {
                return Err(fail("non-ready observation cannot preserve coverage"));
            }
        }
        Ok(())
    }
    /// True only for the one uncommitted successor; committed records must
    /// exactly match CURRENT. The new coverage is checked, not trusted as a
    /// free-standing timestamp that could silently join disconnected periods.
    pub fn check(
        &self,
        generation: [u8; 16],
        semantics: [u8; 32],
        current: &ObservedCut,
        ingested: u64,
        max_gap: i64,
    ) -> Result<bool> {
        self.validate()?;
        current.validate()?;
        if current
            .coverage
            .advance(current.micros, max_gap, None, false)?
            != current.coverage
        {
            return Err(fail("CURRENT retains expired observation coverage"));
        }
        if self.generation != generation || self.semantics != semantics {
            return Err(fail("generation/semantics mismatch"));
        }
        if self.sequence == current.sequence {
            if self.cut() != *current || self.ingested != ingested {
                return Err(fail("decision and CURRENT disagree"));
            }
            return Ok(false);
        }
        let source = self.position.source();
        let previous = &current.source;
        if current.sequence.checked_add(1) != Some(self.sequence)
            || self.micros < current.micros
            || ingested.checked_add(u64::from(self.row_hash.is_some())) != Some(self.ingested)
            || source.identity.kind != previous.identity.kind
            || source.identity.path != previous.identity.path
            || source.offset_bytes < previous.offset_bytes
            || source.record_index < previous.record_index
            || (source.offset_bytes == previous.offset_bytes
                && source.identity != previous.identity)
            || (self.row_hash.is_some()
                && (source.offset_bytes <= previous.offset_bytes
                    || source.record_index <= previous.record_index))
            || (source.identity.kind == "jetstream-v1"
                && (source.identity != previous.identity
                    || previous
                        .offset_bytes
                        .checked_add(u64::from(self.row_hash.is_some()))
                        != Some(source.offset_bytes)
                    || previous
                        .record_index
                        .checked_add(u64::from(self.row_hash.is_some()))
                        != Some(source.record_index)))
        {
            return Err(fail("decision is not a valid immediate source successor"));
        }
        let expected = current.coverage.advance(
            self.micros,
            max_gap,
            self.fact.map(Fact::readiness).transpose()?,
            self.restart,
        )?;
        if self.cut().coverage != expected {
            return Err(fail("decision invents source coverage"));
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
    if !meta.file_type().is_file() || !(36..=MAX_LOG_BYTES as u64).contains(&meta.len()) {
        return Err(fail("invalid observed decision file/size"));
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    std::fs::File::open(path)
        .map_err(io)?
        .take(MAX_LOG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    if bytes.len() < 36
        || bytes.len() > MAX_LOG_BYTES
        || &bytes[..4] != b"OFD1"
        || digest(&bytes[36..]) != bytes[4..36]
    {
        return Err(fail("decision checksum/version mismatch"));
    }
    let value: Decision = serde_json::from_slice(&bytes[36..])
        .map_err(|_| fail("invalid observed decision encoding"))?;
    value.validate()?;
    Ok(Some(value))
}

pub(super) fn write(dir: &Path, decision: &Decision) -> Result<()> {
    decision.validate()?;
    let bytes = serde_json::to_vec(decision).map_err(|_| fail("decision encoding failed"))?;
    if bytes.len() + 36 > MAX_LOG_BYTES {
        return Err(fail("decision exceeds bound"));
    }
    let tmp = dir.join("TIME_PENDING.tmp");
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
    file.write_all(b"OFD1").map_err(io)?;
    file.write_all(&digest(&bytes)).map_err(io)?;
    file.write_all(&bytes).map_err(io)?;
    file.sync_all().map_err(io)?;
    std::fs::rename(tmp, dir.join("TIME_PENDING")).map_err(io)?;
    std::fs::File::open(dir)
        .map_err(io)?
        .sync_all()
        .map_err(io)?;
    Ok(())
}

#[cfg(test)]
#[path = "observed_time_log_tests.rs"]
mod tests;

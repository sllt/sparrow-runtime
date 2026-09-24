//! One durable graph decision. Reuses the directory's TIME_PENDING quota and
//! exclusive writer lock, with a distinct magic and no historical replay.
use super::paused_time_log::{digest, fail};
use serde::{Deserialize, Serialize};
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::Result;
use sparrow_runtime::graph_cut::{GraphCut, KIND};
use std::{
    io::{Read, Write},
    path::Path,
};
const MAX_BYTES: usize = 128 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    pub generation: [u8; 16],
    pub semantics: [u8; 32],
    pub selected: Option<u32>,
    pub row_hash: Option<[u8; 32]>,
    pub eof: bool,
    position: String,
    records: u64,
}
impl Decision {
    pub fn new(
        generation: [u8; 16],
        semantics: [u8; 32],
        cut: &GraphCut,
        selected: Option<u32>,
        row_hash: Option<[u8; 32]>,
        eof: bool,
    ) -> Result<Self> {
        let source = cut.wrap()?;
        let result = Self {
            generation,
            semantics,
            selected,
            row_hash,
            eof,
            position: source.identity.path,
            records: source.record_index,
        };
        result.validate()?;
        Ok(result)
    }
    pub fn cut(&self) -> Result<GraphCut> {
        GraphCut::unwrap(&SourcePosition {
            identity: SourceIdentity {
                kind: KIND.into(),
                path: self.position.clone(),
                size: 0,
                fingerprint: 0,
            },
            offset_bytes: 0,
            record_index: self.records,
        })
    }
    fn validate(&self) -> Result<()> {
        let cut = self.cut()?;
        if self.generation == [0; 16]
            || self
                .selected
                .is_some_and(|id| !cut.sources.contains_key(&id))
            || self.selected.is_some() != (self.row_hash.is_some() || self.eof)
            || (self.eof && self.row_hash.is_some())
            || (cut.sequence == 0 && self.selected.is_some())
        {
            return Err(fail("invalid graph decision identity/action"));
        }
        Ok(())
    }
    pub fn check(
        &self,
        generation: [u8; 16],
        semantics: [u8; 32],
        current: &GraphCut,
    ) -> Result<bool> {
        self.validate()?;
        let target = self.cut()?;
        if self.generation != generation
            || self.semantics != semantics
            || target.idle_micros != current.idle_micros
            || target.sources.keys().ne(current.sources.keys())
        {
            return Err(fail("graph decision identity/policy mismatch"));
        }
        if target.sequence == current.sequence {
            if target.micros != current.micros
                || target.observed_micros != current.observed_micros
                || target.ingested != current.ingested
                || target.next_source != current.next_source
                || target.sources != current.sources
            {
                return Err(fail("graph decision differs from CURRENT input cut"));
            }
            return Ok(false);
        }
        if current.sequence.checked_add(1) != Some(target.sequence)
            || target.micros < current.micros
            || current
                .ingested
                .checked_add(u64::from(self.row_hash.is_some()))
                != Some(target.ingested)
            || target.unions != current.unions
            || target.outputs != current.outputs
        {
            return Err(fail("graph decision is not the immediate successor"));
        }
        let mut next_source = current.next_source;
        for (index, (&id, old)) in current.sources.iter().enumerate() {
            let next = &target.sources[&id];
            if next.contract != old.contract
                || next.position.identity.path != old.position.identity.path
            {
                return Err(fail("graph File contract/path changed"));
            }
            if Some(id) == self.selected {
                next_source = (index + 1) % current.sources.len();
                if old.progress.eof
                    || (self.row_hash.is_some()
                        && (next.position.offset_bytes <= old.position.offset_bytes
                            || next.position.record_index <= old.position.record_index))
                    || (self.eof && !next.progress.eof)
                {
                    return Err(fail("invalid selected File graph cut"));
                }
            } else if next.position != old.position
                || next.progress.watermark != old.progress.watermark
                || next.progress.eof != old.progress.eof
            {
                return Err(fail("unchosen graph source changed position/watermark/EOF"));
            }
            let seen = if Some(id) == self.selected && self.row_hash.is_some() {
                target.micros
            } else {
                old.last_input
            };
            let idle = next.progress.eof
                || (Some(id) != self.selected || self.row_hash.is_none())
                    && (old.progress.idle
                        || target
                            .idle_micros
                            .is_some_and(|ttl| target.micros.saturating_sub(seen) >= ttl));
            if next.last_input != seen || next.progress.idle != idle {
                return Err(fail(
                    "graph idle/input observation differs from logged time",
                ));
            }
        }
        if target.next_source != next_source {
            return Err(fail("graph source scheduler cut changed"));
        }
        Ok(true)
    }
}
pub(super) fn read(dir: &Path) -> Result<Option<Decision>> {
    let path = dir.join("TIME_PENDING");
    let meta = match std::fs::symlink_metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(fail("graph log metadata I/O")),
    };
    if !meta.file_type().is_file() || !(36..=MAX_BYTES as u64).contains(&meta.len()) {
        return Err(fail("graph log size/type"));
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    std::fs::File::open(path)
        .and_then(|f| f.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes))
        .map_err(|_| fail("graph log read"))?;
    if bytes.len() > MAX_BYTES
        || bytes.len() < 36
        || &bytes[..4] != b"GTD1"
        || digest(&bytes[36..]) != bytes[4..36]
    {
        return Err(fail("graph log checksum/magic"));
    }
    let value: Decision =
        serde_json::from_slice(&bytes[36..]).map_err(|_| fail("graph log encoding"))?;
    value.validate()?;
    Ok(Some(value))
}
pub(super) fn write(dir: &Path, decision: &Decision) -> Result<()> {
    decision.validate()?;
    let bytes = serde_json::to_vec(decision).map_err(|_| fail("graph log encode"))?;
    if bytes.len() + 36 > MAX_BYTES {
        return Err(fail("graph log bound"));
    }
    let tmp = dir.join("TIME_PENDING.tmp");
    match std::fs::symlink_metadata(&tmp) {
        Ok(m) if m.file_type().is_file() => {
            std::fs::remove_file(&tmp).map_err(|_| fail("graph log temporary cleanup"))?
        }
        Ok(_) => return Err(fail("graph log temporary is not a regular file")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(fail("graph temporary metadata")),
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(|_| fail("graph log create"))?;
    file.write_all(b"GTD1")
        .and_then(|_| file.write_all(&digest(&bytes)))
        .and_then(|_| file.write_all(&bytes))
        .and_then(|_| file.sync_all())
        .map_err(|_| fail("graph log write/fsync"))?;
    std::fs::rename(tmp, dir.join("TIME_PENDING")).map_err(|_| fail("graph log publish"))?;
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|_| fail("graph log directory fsync"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_runtime::graph_cut::{Progress, SourceProgress};
    fn initial() -> GraphCut {
        GraphCut {
            sequence: 0,
            micros: 0,
            observed_micros: 0,
            ingested: 0,
            next_source: 0,
            idle_micros: Some(100),
            sources: [(
                1,
                SourceProgress {
                    position: SourcePosition::start(SourceIdentity {
                        kind: "file".into(),
                        path: "fixture".into(),
                        size: 0,
                        fingerprint: 0,
                    }),
                    progress: Progress::default(),
                    last_input: 0,
                    contract: 1,
                },
            )]
            .into_iter()
            .collect(),
            unions: Default::default(),
            outputs: [(3, 1)].into_iter().collect(),
        }
    }
    #[test]
    fn time_graph_log_successor_checks_input_idle_and_generation() {
        let current = initial();
        let mut target = current.clone();
        target.sequence = 1;
        target.micros = 100;
        target.observed_micros = 5000;
        target.sources.get_mut(&1).unwrap().progress.idle = true;
        let make =
            |cut: &GraphCut| Decision::new([1; 16], [2; 32], cut, None, None, false).unwrap();
        assert!(make(&target).check([1; 16], [2; 32], &current).unwrap());
        let mut committed = target.clone();
        committed.outputs.insert(3, 2);
        assert!(
            !make(&target).check([1; 16], [2; 32], &committed).unwrap(),
            "post-flush output ordinal is not in pre-publication log"
        );
        for case in 0..6 {
            let mut bad = target.clone();
            match case {
                0 => bad.sequence += 1,
                1 => bad.ingested = 1,
                2 => bad.sources.get_mut(&1).unwrap().progress.idle = false,
                3 => bad.outputs.insert(3, 2).map(|_| ()).unwrap(),
                4 => bad.sources.get_mut(&1).unwrap().position.offset_bytes = 1,
                _ => bad.sources.get_mut(&1).unwrap().contract = 2,
            };
            assert!(
                Decision::new([1; 16], [2; 32], &bad, None, None, false)
                    .and_then(|d| d.check([1; 16], [2; 32], &current))
                    .is_err(),
                "case {case}"
            );
        }
        assert!(make(&target).check([3; 16], [2; 32], &current).is_err());
        assert!(make(&target).check([1; 16], [3; 32], &current).is_err());
    }
    #[test]
    fn time_graph_log_checksum_bounds_and_regular_file_guard() {
        let dir = std::env::temp_dir().join(format!(
            "graph-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let decision = Decision::new([1; 16], [2; 32], &initial(), None, None, false).unwrap();
        assert!(read(&dir).unwrap().is_none());
        write(&dir, &decision).unwrap();
        assert_eq!(read(&dir).unwrap(), Some(decision.clone()));
        let path = dir.join("TIME_PENDING");
        let original = std::fs::read(&path).unwrap();
        for case in 0..4 {
            let mut bytes = original.clone();
            match case {
                0 => bytes[0] ^= 1,
                1 => bytes[10] ^= 1,
                2 => bytes.truncate(35),
                _ => bytes.resize(MAX_BYTES + 1, 0),
            };
            std::fs::write(&path, bytes).unwrap();
            assert!(read(&dir).is_err());
        }
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(read(&dir).is_err());
        std::fs::remove_dir(&path).unwrap();
        std::fs::create_dir(dir.join("TIME_PENDING.tmp")).unwrap();
        assert!(write(&dir, &decision).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

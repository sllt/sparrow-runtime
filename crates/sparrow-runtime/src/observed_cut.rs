//! Source-observed time has its own cut. PTC1 and old profiles are unchanged.
use sparrow_io::{feed::FeedReadiness, SourceIdentity, SourcePosition};
use sparrow_model::{ErrorCode, Result, SparrowError};

pub const FILE_KIND: &str = "observed-file-v1";
pub const JETSTREAM_KIND: &str = "observed-jetstream-v1";
const MAX_BYTES: usize = 30 * 1024;

/// A bounded chain of fresh observations, not a hardware-availability proof.
/// A break always requires a complete new grace interval; unknown gaps are
/// never added to previously observed intervals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub since: Option<i64>,
    pub last_fresh: Option<i64>,
}

pub(crate) fn invalid(message: &str) -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        format!("observed time: {message}"),
    )
}

impl Coverage {
    pub fn validate(self, now: i64) -> Result<()> {
        if now < 0
            || !matches!(
                (self.since, self.last_fresh),
                (None, None) | (Some(0..), Some(0..))
            )
            || self
                .since
                .zip(self.last_fresh)
                .is_some_and(|(since, last)| since > last || last > now)
        {
            return Err(invalid("invalid observation coverage"));
        }
        Ok(())
    }

    /// `None` is a data-only decision, not a new observation. It may preserve
    /// recent coverage but cannot authorize a silence emission. An actual
    /// non-CaughtUp probe breaks coverage conservatively. Restart is explicit
    /// in the durable decision and must not be inferred while replaying it.
    pub fn advance(
        self,
        now: i64,
        max_gap: i64,
        observation: Option<FeedReadiness>,
        restart: bool,
    ) -> Result<Self> {
        self.validate(now)?;
        if max_gap <= 0 {
            return Err(invalid("observation gap must be positive"));
        }
        let previous = if restart || self.last_fresh.is_some_and(|last| now - last > max_gap) {
            Self::default()
        } else {
            self
        };
        Ok(match observation {
            None => previous,
            Some(FeedReadiness::CaughtUp) => Self {
                since: Some(previous.since.unwrap_or(now)),
                last_fresh: Some(now),
            },
            Some(_) => Self::default(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedCut {
    pub sequence: u64,
    pub micros: i64,
    pub coverage: Coverage,
    pub source: SourcePosition,
}

fn take<'a>(src: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if src.len() < n {
        return Err(invalid("truncated source cut"));
    }
    let (head, tail) = src.split_at(n);
    *src = tail;
    Ok(head)
}
fn u64_value(src: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
}
fn i64_value(src: &mut &[u8]) -> Result<i64> {
    Ok(i64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
}
fn string(src: &mut &[u8]) -> Result<String> {
    let n = u32::from_le_bytes(take(src, 4)?.try_into().unwrap()) as usize;
    if n > MAX_BYTES {
        return Err(invalid("source metadata exceeds bound"));
    }
    Ok(std::str::from_utf8(take(src, n)?)
        .map_err(|_| invalid("source metadata UTF-8"))?
        .to_owned())
}

impl ObservedCut {
    pub fn validate(&self) -> Result<()> {
        self.coverage.validate(self.micros)?;
        if !matches!(self.source.identity.kind.as_str(), "file" | "jetstream-v1")
            || (self.sequence == 0
                && (self.micros != 0
                    || self.coverage != Coverage::default()
                    || self.source.offset_bytes != 0
                    || self.source.record_index != 0))
            || (self.source.identity.kind == "file"
                && self.source.offset_bytes > self.source.identity.size)
            || self.source.identity.path.is_empty()
            || self.source.identity.path.len() > MAX_BYTES - 128
        {
            return Err(invalid("invalid source cut identity/bootstrap"));
        }
        Ok(())
    }

    pub fn wrap(&self) -> Result<SourcePosition> {
        self.validate()?;
        let kind = if self.source.identity.kind == "file" {
            FILE_KIND
        } else {
            JETSTREAM_KIND
        };
        let mut bytes = Vec::with_capacity(self.source.identity.path.len() + 128);
        bytes.extend_from_slice(b"OFC1");
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&self.micros.to_le_bytes());
        bytes.extend_from_slice(&self.coverage.since.unwrap_or(-1).to_le_bytes());
        bytes.extend_from_slice(&self.coverage.last_fresh.unwrap_or(-1).to_le_bytes());
        crate::checkpoint::encode_position(&self.source, &mut bytes)?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid("source cut exceeds bound"));
        }
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut path = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            path.push(HEX[(byte >> 4) as usize] as char);
            path.push(HEX[(byte & 15) as usize] as char);
        }
        Ok(SourcePosition {
            offset_bytes: self.source.offset_bytes,
            record_index: self.source.record_index,
            identity: SourceIdentity {
                kind: kind.into(),
                path,
                size: self.source.identity.size,
                fingerprint: self.source.identity.fingerprint,
            },
        })
    }

    pub fn unwrap(position: &SourcePosition) -> Result<Self> {
        let source_kind = match position.identity.kind.as_str() {
            FILE_KIND => "file",
            JETSTREAM_KIND => "jetstream-v1",
            _ => return Err(invalid("source is not an observed-time cut")),
        };
        let hex = position.identity.path.as_bytes();
        if hex.len() > MAX_BYTES * 2 || hex.len() % 2 != 0 {
            return Err(invalid("source cut hex size"));
        }
        let nibble = |b: u8| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(invalid("source cut noncanonical hex")),
        };
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        for pair in hex.chunks_exact(2) {
            bytes.push(nibble(pair[0])? * 16 + nibble(pair[1])?);
        }
        let mut src = bytes.as_slice();
        if take(&mut src, 4)? != b"OFC1" {
            return Err(invalid("source cut version"));
        }
        let sequence = u64_value(&mut src)?;
        let micros = i64_value(&mut src)?;
        let since = i64_value(&mut src)?;
        let last = i64_value(&mut src)?;
        if since < -1 || last < -1 {
            return Err(invalid("coverage sentinel"));
        }
        let offset_bytes = u64_value(&mut src)?;
        let record_index = u64_value(&mut src)?;
        let kind = string(&mut src)?;
        let path = string(&mut src)?;
        let size = u64_value(&mut src)?;
        let fingerprint = u64_value(&mut src)?;
        if !src.is_empty()
            || kind != source_kind
            || offset_bytes != position.offset_bytes
            || record_index != position.record_index
            || size != position.identity.size
            || fingerprint != position.identity.fingerprint
        {
            return Err(invalid("inner/outer source cut mismatch"));
        }
        let cut = Self {
            sequence,
            micros,
            coverage: Coverage {
                since: (since != -1).then_some(since),
                last_fresh: (last != -1).then_some(last),
            },
            source: SourcePosition {
                offset_bytes,
                record_index,
                identity: SourceIdentity {
                    kind,
                    path,
                    size,
                    fingerprint,
                },
            },
        };
        cut.validate()?;
        Ok(cut)
    }
}

#[cfg(test)]
#[path = "observed_cut_tests.rs"]
mod tests;

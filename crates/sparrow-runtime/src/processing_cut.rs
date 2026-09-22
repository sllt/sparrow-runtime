//! A paused logical clock is part of the source cut, not a host timestamp.
//! The wrapper leaves connector positions opaque and uses a separate profile.
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{ErrorCode, Result, SparrowError};

pub const FILE_KIND: &str = "paused-file-v1";
pub const JETSTREAM_KIND: &str = "paused-jetstream-v1";
const MAX_BYTES: usize = 30 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessingCut {
    pub sequence: u64,
    pub micros: i64,
    pub source: SourcePosition,
}
fn invalid() -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        "invalid paused processing-time source cut",
    )
}
fn take<'a>(src: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if src.len() < n {
        return Err(invalid());
    }
    let (head, tail) = src.split_at(n);
    *src = tail;
    Ok(head)
}
fn number(src: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
}
fn string(src: &mut &[u8]) -> Result<String> {
    let n = u32::from_le_bytes(take(src, 4)?.try_into().unwrap()) as usize;
    if n > MAX_BYTES {
        return Err(invalid());
    }
    Ok(std::str::from_utf8(take(src, n)?)
        .map_err(|_| invalid())?
        .to_owned())
}
impl ProcessingCut {
    pub fn wrap(&self) -> Result<SourcePosition> {
        let kind = match self.source.identity.kind.as_str() {
            "file" => FILE_KIND,
            "jetstream-v1" => JETSTREAM_KIND,
            _ => return Err(invalid()),
        };
        if self.micros < 0
            || (self.sequence == 0 && self.micros != 0)
            || self.source.identity.path.len() + self.source.identity.kind.len() + 64 > MAX_BYTES
        {
            return Err(invalid());
        }
        let mut bytes = Vec::with_capacity(self.source.identity.path.len() + 128);
        bytes.extend_from_slice(b"PTC1");
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&self.micros.to_le_bytes());
        crate::checkpoint::encode_position(&self.source, &mut bytes)?;
        let mut path = String::with_capacity(bytes.len() * 2);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for b in bytes {
            path.push(HEX[(b >> 4) as usize] as char);
            path.push(HEX[(b & 15) as usize] as char);
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
        let expected = match position.identity.kind.as_str() {
            FILE_KIND => "file",
            JETSTREAM_KIND => "jetstream-v1",
            _ => return Err(invalid()),
        };
        let hex = position.identity.path.as_bytes();
        if hex.len() > MAX_BYTES * 2 || hex.len() % 2 != 0 {
            return Err(invalid());
        }
        let nibble = |b: u8| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(invalid()),
        };
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        for pair in hex.chunks_exact(2) {
            bytes.push(nibble(pair[0])? * 16 + nibble(pair[1])?);
        }
        let mut src = bytes.as_slice();
        if take(&mut src, 4)? != b"PTC1" {
            return Err(invalid());
        }
        let sequence = number(&mut src)?;
        let micros = i64::from_le_bytes(take(&mut src, 8)?.try_into().unwrap());
        let offset_bytes = number(&mut src)?;
        let record_index = number(&mut src)?;
        let kind = string(&mut src)?;
        let path = string(&mut src)?;
        let size = number(&mut src)?;
        let fingerprint = number(&mut src)?;
        if !src.is_empty()
            || micros < 0
            || (sequence == 0 && micros != 0)
            || kind != expected
            || offset_bytes != position.offset_bytes
            || record_index != position.record_index
            || size != position.identity.size
            || fingerprint != position.identity.fingerprint
        {
            return Err(invalid());
        }
        Ok(Self {
            sequence,
            micros,
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
        })
    }
}

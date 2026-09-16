//! Stable final-output identity. Independent of HTTP batch boundaries.
use crate::{ErrorCode, Result, SparrowError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputSequence {
    epoch: [u8; 16],
    first: u64,
}
impl OutputSequence {
    pub fn new(epoch: [u8; 16], first: u64) -> Result<Self> {
        if epoch == [0; 16] || first == 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "output identity requires an initialized epoch and positive ordinal",
            ));
        }
        Ok(Self { epoch, first })
    }
    pub fn epoch(self) -> [u8; 16] {
        self.epoch
    }
    pub fn first(self) -> u64 {
        self.first
    }
    /// Returns the NEXT output position, failing before any row is sent.
    pub fn advance(self, rows: usize) -> Result<Self> {
        let first = self
            .first
            .checked_add(u64::try_from(rows).map_err(|_| Self::overflow())?)
            .ok_or_else(Self::overflow)?;
        Self::new(self.epoch, first)
    }
    pub fn id_ascii(self, offset: usize) -> Result<[u8; 48]> {
        let ordinal = self.advance(offset)?.first;
        let mut bytes = [0u8; 24];
        bytes[..16].copy_from_slice(&self.epoch);
        bytes[16..].copy_from_slice(&ordinal.to_be_bytes());
        let mut text = [0u8; 48];
        let hex = b"0123456789abcdef";
        for (i, b) in bytes.iter().enumerate() {
            text[2 * i] = hex[(b >> 4) as usize];
            text[2 * i + 1] = hex[(b & 15) as usize];
        }
        Ok(text)
    }
    fn overflow() -> SparrowError {
        SparrowError::new(
            ErrorCode::IntegerOverflow,
            "output ordinal exhausted; refuse identity reuse",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn k2_output_ids_do_not_depend_on_batch_boundaries_and_never_wrap() {
        let first = OutputSequence::new([7; 16], 1).unwrap();
        let all = (0..11)
            .map(|n| first.id_ascii(n).unwrap())
            .collect::<Vec<_>>();
        let mut split = Vec::new();
        let mut at = first;
        for n in [3, 1, 7] {
            split.extend((0..n).map(|i| at.id_ascii(i).unwrap()));
            at = at.advance(n).unwrap();
        }
        assert_eq!(all, split);
        assert_eq!(at.first(), 12);
        assert!(OutputSequence::new([0; 16], 1).is_err());
        assert!(OutputSequence::new([1; 16], 0).is_err());
        assert!(OutputSequence::new([1; 16], u64::MAX)
            .unwrap()
            .advance(1)
            .is_err());
        assert_ne!(
            first.id_ascii(0).unwrap(),
            OutputSequence::new([8; 16], 1)
                .unwrap()
                .id_ascii(0)
                .unwrap()
        );
    }
}

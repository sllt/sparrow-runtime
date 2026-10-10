//! Codec 5 (ANF1): bounded analysis state, separate from every legacy codec.
use crate::graph_cut::Progress;
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Row, Scalar, SparrowError,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) const FIXED: usize = 32 * 1024;
pub(crate) fn mismatch(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::UnsupportedRestore, message)
        .context("checkpoint_guard", "analysis_profile_mismatch")
}
fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

pub(crate) fn row_credit(values: &[Scalar]) -> usize {
    resident_credit(values.iter().map(Scalar::resident_bytes).sum())
}
fn resident_credit(values: usize) -> usize {
    values
        .saturating_add(std::mem::size_of::<Row>() + 32)
        .saturating_mul(8)
        .saturating_add(4096)
}

#[derive(Debug, PartialEq)]
pub struct JoinRowFreeze {
    pub ordinal: u64,
    pub matched: bool,
    pub row: Row,
}
#[derive(Debug, PartialEq)]
pub enum AnalysisData {
    Unnest(BTreeMap<Option<OperatorId>, i64>),
    Join {
        sequences: [u64; 2],
        inputs: [Progress; 2],
        emitted: Progress,
        rows: [Vec<JoinRowFreeze>; 2],
    },
}
#[derive(Debug, PartialEq)]
pub struct AnalysisFreeze {
    pub operator: OperatorId,
    pub data: AnalysisData,
    resident: usize,
}
impl AnalysisFreeze {
    pub fn kind(&self) -> u8 {
        if matches!(self.data, AnalysisData::Unnest(_)) {
            10
        } else {
            11
        }
    }
    pub fn entries(&self) -> usize {
        match &self.data {
            AnalysisData::Unnest(s) => s.len(),
            AnalysisData::Join { rows, .. } => rows.iter().map(Vec::len).sum(),
        }
    }
    pub fn resident_bytes(&self) -> usize {
        self.resident
    }
    pub(crate) fn decode(src: &mut &[u8], limit: usize, materialize: bool) -> Result<Self> {
        let header = crate::checkpoint::FreezeHeader::parse(src)?;
        take(src, 11)?;
        if header.slot.raw() != 4 || !matches!(header.kind, 10 | 11) || take(src, 4)? != b"ANF1" {
            return Err(mismatch("analysis frame identity/grammar mismatch"));
        }
        if header.entries > limit {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "analysis state entry bound",
            ));
        }
        let mut resident = FIXED;
        let data = if header.kind == 10 {
            if header.entries > 64 {
                return Err(invalid("UNNEST source count exceeds 64"));
            }
            let mut sequences = BTreeMap::new();
            let mut previous = None;
            for _ in 0..header.entries {
                let source = match take(src, 1)?[0] {
                    0 => None,
                    1 => Some(OperatorId::new(u32::from_le_bytes(
                        take(src, 4)?.try_into().unwrap(),
                    ))),
                    _ => return Err(invalid("invalid UNNEST source tag")),
                };
                let sequence = number(src)?;
                if sequence == 0
                    || sequence > i64::MAX as u64
                    || previous.is_some_and(|p| p >= source)
                {
                    return Err(invalid("invalid UNNEST source sequence/order"));
                }
                previous = Some(source);
                if materialize {
                    sequences.insert(source, sequence as i64);
                }
            }
            AnalysisData::Unnest(sequences)
        } else {
            let sequences = [number(src)?, number(src)?];
            if sequences.iter().any(|n| *n > i64::MAX as u64) {
                return Err(invalid("Join sequence overflow"));
            }
            let inputs = [progress(src)?, progress(src)?];
            let emitted = progress(src)?;
            if emitted.eof != inputs.iter().all(|p| p.eof) || emitted.idle != emitted.eof {
                return Err(invalid("Join emitted activity mismatch"));
            }
            let mut rows: [Vec<JoinRowFreeze>; 2] = std::array::from_fn(|_| Vec::new());
            let mut total = 0usize;
            for side in 0..2 {
                let count = u32::from_le_bytes(take(src, 4)?.try_into().unwrap()) as usize;
                total = total
                    .checked_add(count)
                    .ok_or_else(|| invalid("Join row count overflow"))?;
                if count > 4096
                    || total > header.entries
                    || count as u64 > sequences[side]
                    || src.len() < count.saturating_mul(12)
                {
                    return Err(invalid(
                        "Join retained row count exceeds frame/sequence bound",
                    ));
                }
                if materialize {
                    rows[side].reserve_exact(count);
                }
                let mut previous = 0;
                for _ in 0..count {
                    let ordinal = number(src)?;
                    if ordinal <= previous || ordinal > sequences[side] {
                        return Err(invalid("Join row ordinal out of order/range"));
                    }
                    previous = ordinal;
                    let matched = match take(src, 1)?[0] {
                        0 => false,
                        1 => true,
                        _ => return Err(invalid("invalid Join matched flag")),
                    };
                    let width = u16::from_le_bytes(take(src, 2)?.try_into().unwrap()) as usize;
                    if width == 0 || width > 128 || src.len() < width {
                        return Err(invalid("invalid Join row width"));
                    }
                    let mut values = Vec::with_capacity(if materialize { width } else { 0 });
                    let mut value_bytes = 0usize;
                    for _ in 0..width {
                        let before = *src;
                        if matches!(before.first(), Some(1))
                            && !matches!(before.get(1), Some(0 | 1))
                        {
                            return Err(invalid("noncanonical Join Bool"));
                        }
                        if materialize {
                            values.push(Scalar::decode_value(src)?);
                        } else {
                            Scalar::skip_encoded_value(src)?;
                        }
                        value_bytes =
                            value_bytes.saturating_add(crate::aggregate::encoded_scalar_resident(
                                &before[..before.len() - src.len()],
                            ));
                    }
                    // Key storage is covered by row credit. One marker lease
                    // per distinct key is bounded by one byte per retained row.
                    resident =
                        resident.saturating_add(resident_credit(value_bytes).saturating_add(1));
                    if materialize {
                        rows[side].push(JoinRowFreeze {
                            ordinal,
                            matched,
                            row: Row { values },
                        });
                    }
                }
            }
            if total != header.entries {
                return Err(invalid("Join frame entry count mismatch"));
            }
            AnalysisData::Join {
                sequences,
                inputs,
                emitted,
                rows,
            }
        };
        Ok(Self {
            operator: header.operator,
            data,
            resident,
        })
    }
}
fn take<'a>(src: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if src.len() < n {
        return Err(invalid("truncated analysis frame"));
    }
    let (a, b) = src.split_at(n);
    *src = b;
    Ok(a)
}
fn number(src: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
}
fn progress(src: &mut &[u8]) -> Result<Progress> {
    let watermark = number(src)? as i64;
    let flags = take(src, 1)?[0];
    Progress::from_control(watermark, flags).map_err(|_| invalid("invalid analysis progress"))
}
pub(crate) fn put_progress(out: &mut Vec<u8>, p: &Progress) {
    out.extend_from_slice(&p.watermark.unwrap_or(-1).to_le_bytes());
    out.push(u8::from(p.idle) | (u8::from(p.eof) << 1));
}
pub(crate) fn header(
    out: &mut Vec<u8>,
    operator: OperatorId,
    kind: u8,
    entries: usize,
    limit: usize,
) -> Result<()> {
    if entries > limit {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "analysis freeze exceeds state entry bound",
        ));
    }
    out.extend_from_slice(&operator.raw().to_le_bytes());
    out.extend_from_slice(&4u16.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&(entries as u32).to_le_bytes());
    out.extend_from_slice(b"ANF1");
    Ok(())
}

pub(crate) struct UnnestState {
    pub sequences: BTreeMap<Option<OperatorId>, i64>,
    _credit: MemoryLease,
}
impl UnnestState {
    pub fn new(owner: &Arc<MemoryOwner>) -> Result<Self> {
        Ok(Self {
            sequences: BTreeMap::new(),
            _credit: owner.acquire(CreditKind::Retention, FIXED)?,
        })
    }
    pub fn restore(&mut self, freeze: AnalysisFreeze) -> Result<()> {
        let AnalysisData::Unnest(sequences) = freeze.data else {
            return Err(mismatch("UNNEST received a Join frame"));
        };
        self.sequences = sequences;
        Ok(())
    }
    pub fn encode(&self, operator: OperatorId, out: &mut Vec<u8>, limit: usize) -> Result<()> {
        header(out, operator, 10, self.sequences.len(), limit)?;
        for (source, sequence) in &self.sequences {
            out.push(u8::from(source.is_some()));
            if let Some(source) = source {
                out.extend_from_slice(&source.raw().to_le_bytes());
            }
            out.extend_from_slice(&sequence.to_le_bytes());
        }
        Ok(())
    }
}
pub(crate) enum PreparedAnalysis {
    Unnest(UnnestState),
    Join {
        op: crate::bounded_join::BoundedJoin,
        inputs: [Progress; 2],
        emitted: Progress,
        _credit: MemoryLease,
    },
}

//! Bounded, final-only sliding/session windows.
//!
//! Sliding count state has the codec 4 (`BWF1`) participant frame used only by
//! the strict File v31 / JetStream v32 profiles. Other buffered kinds remain
//! restart_fresh. The frame stores evaluated, detached aggregate inputs, never
//! raw rows or accumulators, and is rebuilt by the same push-path rules.
//!
//! Store detached aggregate inputs, not input batches. Each group and event
//! owns its credit; the deadline index shares the credited group key. Retained
//! inputs permit MIN/MAX and out-of-order session re-segmentation without an
//! invalid inverse/merge approximation. CPU is bounded by max_buffered_rows.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use sparrow_expr::{allocation::AllocationBound, bind, eval_bound, BoundExpr};
use sparrow_model::{
    CreditKind, ErrorCode, InputId, MemoryLease, MemoryOwner, Result, Row, RowBatch, Scalar,
    Schema, SparrowError, WindowKind,
};
use sparrow_plan::WindowSpec;

use crate::{aggregate::Accumulator, watermark::WatermarkHub};

struct Key {
    encoded: Vec<u8>,
    values: Vec<Scalar>,
    // Includes both B-tree nodes, Arc allocation and Group container.
    credit: MemoryLease,
}
impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.encoded == other.encoded
    }
}
impl Eq for Key {}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.encoded.cmp(&other.encoded)
    }
}

struct Event {
    values: Vec<Scalar>,
    pending: bool,
    credit: MemoryLease,
}
#[derive(Default)]
struct Group {
    events: BTreeMap<(i64, u64), Event>,
    sequence: u64,
    deadline: Option<i64>,
}

pub(crate) enum Ingest {
    Accepted(Option<RowBatch>),
    Late,
    Future,
}

pub(crate) struct BufferedWindow {
    schedule: BTreeSet<(i64, Arc<Key>)>,
    groups: BTreeMap<Arc<Key>, Group>,
    spec: WindowSpec,
    input: Schema,
    output: Schema,
    keys: Vec<usize>,
    event_time: Option<usize>,
    expressions: Vec<Option<BoundExpr>>,
    allocation: Vec<Option<AllocationBound>>,
    owner: Arc<MemoryOwner>,
    hub: WatermarkHub,
    external: bool,
    max_keys: usize,
    max_timers: usize,
    bytes: usize,
    last_now: i64,
    max_group_rows_seen: usize,
    /// v31/v32 checkpointed job: event credit is the exact stored-value rule
    /// ([`event_credit`]) shared with restore, not the pre-eval estimate.
    durable: bool,
}

/// Codec 4 frame grammar marker, after the shared 11-byte freeze header.
pub(crate) const BWF_MAGIC: &[u8; 4] = b"BWF1";
/// Freeze header kind for a sliding count frame (= window kind tag 5).
pub(crate) const SLIDING_COUNT_FREEZE_KIND: u8 = 5;
/// Freeze header kind for an ET sliding frame (= window kind tag 7, v33).
pub(crate) const SLIDING_ET_FREEZE_KIND: u8 = 7;
/// Freeze header kind for an ET session frame (= window kind tag 9, v33).
pub(crate) const SESSION_ET_FREEZE_KIND: u8 = 9;
/// Freeze header kind for a PT sliding frame (= window kind tag 6, v34/v35).
pub(crate) const SLIDING_PT_FREEZE_KIND: u8 = 6;
/// Freeze header kind for a PT session frame (= window kind tag 8, v34/v35).
pub(crate) const SESSION_PT_FREEZE_KIND: u8 = 8;
const KEY_FIXED: usize = 4096;
const EVENT_FIXED: usize = 1024;
const MAX_ARITY: usize = 64;

/// Retention credit of one group key: fixed node/Arc/container slack plus the
/// actual detached key values (both B-trees may hold a copy of the bytes).
/// Legacy SPV1 / untagged encoders cannot carry codec 4.
pub(crate) fn codec4_legacy() -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        "legacy checkpoint encoders cannot carry sliding count codec 4 state; v31/v32 is required",
    )
    .context("checkpoint_guard", "buffered_profile_mismatch")
}

pub(crate) fn key_credit(values: &[Scalar]) -> usize {
    values.iter().fold(KEY_FIXED, |n, v| {
        n.saturating_add(v.resident_bytes().saturating_mul(4))
    })
}

/// Retention credit of one retained input (v31+ live and restore): fixed
/// event/B-tree overhead plus the actual stored values.
pub(crate) fn event_credit(values: &[Scalar]) -> usize {
    values.iter().fold(
        EVENT_FIXED.saturating_add(values.len().saturating_mul(std::mem::size_of::<Scalar>())),
        |n, v| n.saturating_add(v.resident_bytes()),
    )
}

fn codec(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

#[derive(Debug, PartialEq)]
pub struct BufferedGroupFreeze {
    pub key: Vec<Scalar>,
    /// Arrival count of this key (the last retained event's sequence).
    pub sequence: u64,
    /// Retained inputs, oldest first: (sequence, evaluated agg inputs).
    pub events: Vec<(u64, Vec<Scalar>)>,
    /// ET kinds only (v33), parallel to `events`: (event time, pending).
    /// Empty for sliding count. Session events always carry pending=false
    /// in the frame (the live executor never reads it for sessions).
    pub times: Vec<(i64, bool)>,
}

/// v33 watermark generator state of the single linear input, restored
/// exactly so late/close decisions after restore equal uninterrupted ones.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct BufferedClockFreeze {
    pub idle: bool,
    pub wm: Option<i64>,
    pub max_event_time: Option<i64>,
    pub last_effective: Option<i64>,
}

/// Decoded codec 4 participant. `resident` is the exact Retention the rebuild
/// acquires (sum of [`key_credit`] + [`event_credit`]), computed identically by
/// the bounded scan pass so the Store can reserve it before materializing.
#[derive(Debug, PartialEq)]
pub struct BufferedFreeze {
    pub operator: sparrow_model::OperatorId,
    pub slot: sparrow_model::StateSlotId,
    pub kind: u8,
    /// Sliding count size (kind 5); 0 for ET kinds.
    pub size: u64,
    /// ET kinds: (size, delay) for sliding (7), (gap, max_duration) for
    /// session (9); (0, 0) for sliding count.
    pub params: (i64, i64),
    pub groups: Vec<BufferedGroupFreeze>,
    /// ET kinds only.
    pub clock: Option<BufferedClockFreeze>,
    /// PT kinds only: the operator's logical clock at the barrier; must equal
    /// the PTC1 cut micros of the same snapshot.
    pub pt_now: Option<i64>,
    resident: usize,
}

impl BufferedFreeze {
    pub fn resident_bytes(&self) -> usize {
        self.resident
    }

    /// Bounded scan (materialize=false) or decode of one codec 4 frame.
    /// Structural invariants are checked in both passes; type/spec checks
    /// happen at restore against the live operator.
    pub(crate) fn decode_metered(
        src: &mut &[u8],
        max_groups: usize,
        materialize: bool,
        resident: &mut usize,
    ) -> Result<Self> {
        let header = crate::checkpoint::FreezeHeader::parse(src)?;
        *src = &src[11..];
        let take = |src: &mut &[u8], n: usize| -> Result<Vec<u8>> {
            if src.len() < n {
                return Err(codec("truncated buffered window freeze"));
            }
            let (head, tail) = src.split_at(n);
            *src = tail;
            Ok(head.to_vec())
        };
        let u16v = |src: &mut &[u8]| -> Result<usize> {
            Ok(u16::from_le_bytes(take(src, 2)?.try_into().unwrap()) as usize)
        };
        let u32v = |src: &mut &[u8]| -> Result<usize> {
            Ok(u32::from_le_bytes(take(src, 4)?.try_into().unwrap()) as usize)
        };
        let u64v = |src: &mut &[u8]| -> Result<u64> {
            Ok(u64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
        };
        let et = matches!(header.kind, SLIDING_ET_FREEZE_KIND | SESSION_ET_FREEZE_KIND);
        let pt = matches!(header.kind, SLIDING_PT_FREEZE_KIND | SESSION_PT_FREEZE_KIND);
        let sliding_kind = matches!(header.kind, SLIDING_ET_FREEZE_KIND | SLIDING_PT_FREEZE_KIND);
        if !(header.kind == SLIDING_COUNT_FREEZE_KIND || et || pt) || header.slot.raw() != 1 {
            return Err(codec("buffered window freeze kind/slot has no codec 4 grammar"));
        }
        let i64v = |src: &mut &[u8]| -> Result<i64> {
            Ok(i64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
        };
        if header.entries > max_groups {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "buffered window freeze group count exceeds max_state_keys",
            ));
        }
        if take(src, 4)? != BWF_MAGIC {
            return Err(codec("buffered window freeze lacks BWF1 grammar"));
        }
        let (size, params) = if et || pt {
            // Same constructors as the live spec: a parameter the live window
            // could never have is a codec violation, not a spec mismatch.
            let (a, b) = (i64v(src)?, i64v(src)?);
            let valid = if sliding_kind {
                WindowKind::sliding(a, b, et).is_ok()
            } else {
                WindowKind::session(a, b, et).is_ok()
            };
            if !valid {
                return Err(codec("invalid ET/PT sliding/session parameters in freeze"));
            }
            (0, (a, b))
        } else {
            let size = u64v(src)?;
            if size == 0 || size > i64::MAX as u64 {
                return Err(codec("invalid sliding count size in freeze"));
            }
            (size, (0, 0))
        };
        let timed = et || pt;
        let event_min = if timed { 8 + 8 + 1 + 2 } else { 8 + 2 };
        // Minimum per group: arity + sequence + event count + one event.
        if src.len() < header.entries.saturating_mul(2 + 8 + 4 + event_min) {
            return Err(codec("declared buffered group count exceeds remaining bytes"));
        }
        let mut groups = Vec::with_capacity(if materialize { header.entries } else { 0 });
        let mut total = 0usize;
        let mut max_time: Option<i64> = None;
        let value = |src: &mut &[u8], out: &mut Vec<Scalar>, n: &mut usize, key: bool| -> Result<()> {
            let before = *src;
            if materialize {
                out.push(Scalar::decode_value(src)?);
            } else {
                Scalar::skip_encoded_value(src)?;
            }
            if before.first().is_some_and(|tag| *tag > 7) {
                return Err(codec("unsupported scalar in buffered window freeze"));
            }
            let r = crate::aggregate::encoded_scalar_resident(&before[..before.len() - src.len()]);
            *n = n.saturating_add(if key { r.saturating_mul(4) } else { r });
            Ok(())
        };
        for _ in 0..header.entries {
            let nk = u16v(src)?;
            if nk > MAX_ARITY {
                return Err(SparrowError::new(ErrorCode::BoundExceeded, "buffered freeze key arity exceeds 64"));
            }
            let mut key = Vec::with_capacity(if materialize { nk } else { 0 });
            let mut key_bytes = KEY_FIXED;
            for _ in 0..nk {
                value(src, &mut key, &mut key_bytes, true)?;
            }
            total = total.saturating_add(key_bytes);
            let sequence = u64v(src)?;
            let ne = u32v(src)?;
            // Sliding count: exactly the last min(sequence, size) arrivals.
            // ET: 1..=sequence retained (empty groups are removed live).
            if sequence == 0
                || sequence > i64::MAX as u64
                || (!timed && ne as u64 != sequence.min(size))
                || (timed && (ne == 0 || ne as u64 > sequence))
            {
                return Err(codec("buffered window retained input count disagrees with its sequence"));
            }
            if src.len() < ne.saturating_mul(event_min) {
                return Err(codec("declared buffered event count exceeds remaining bytes"));
            }
            let mut events = Vec::with_capacity(if materialize { ne } else { 0 });
            let mut times = Vec::with_capacity(if materialize && timed { ne } else { 0 });
            let mut seqs = Vec::with_capacity(if timed { ne } else { 0 });
            let mut previous: Option<(i64, u64)> = None;
            let mut seen_pending = false;
            let first = sequence - ne as u64 + 1;
            for i in 0..ne {
                let (seq, time) = if timed {
                    let t = i64v(src)?;
                    let seq = u64v(src)?;
                    let pending = match take(src, 1)?[0] {
                        0 => false,
                        1 if sliding_kind => true,
                        _ => return Err(codec("invalid pending flag in buffered ET freeze")),
                    };
                    // Event times are nonnegative (push rejects < 0); (t, seq)
                    // strictly ascending; seq within 1..=sequence and unique;
                    // triggered (non-pending) inputs precede every pending one.
                    if t < 0
                        || seq == 0
                        || seq > sequence
                        || previous.is_some_and(|p| p >= (t, seq))
                        || (seen_pending && !pending)
                    {
                        return Err(codec("buffered ET events are out of order or out of range"));
                    }
                    seen_pending |= pending;
                    previous = Some((t, seq));
                    max_time = max_time.max(Some(t));
                    seqs.push(seq);
                    (seq, Some((t, pending)))
                } else {
                    let seq = u64v(src)?;
                    if seq != first + i as u64 {
                        return Err(codec("buffered window event sequences are not the contiguous tail"));
                    }
                    (seq, None)
                };
                if let (true, Some(time)) = (materialize, time) {
                    times.push(time);
                }
                let nv = u16v(src)?;
                if nv > MAX_ARITY {
                    return Err(SparrowError::new(ErrorCode::BoundExceeded, "buffered freeze value arity exceeds 64"));
                }
                let mut values = Vec::with_capacity(if materialize { nv } else { 0 });
                let mut bytes = EVENT_FIXED.saturating_add(nv.saturating_mul(std::mem::size_of::<Scalar>()));
                for _ in 0..nv {
                    value(src, &mut values, &mut bytes, false)?;
                }
                total = total.saturating_add(bytes);
                if materialize {
                    events.push((seq, values));
                }
            }
            if timed {
                seqs.sort_unstable();
                if seqs.windows(2).any(|w| w[0] == w[1]) {
                    return Err(codec("buffered ET event sequences are duplicated"));
                }
            }
            if materialize {
                groups.push(BufferedGroupFreeze { key, sequence, events, times });
            }
        }
        let clock = if et {
            let idle = match take(src, 1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(codec("invalid input activity in buffered ET freeze")),
            };
            let opt = |src: &mut &[u8]| -> Result<Option<i64>> {
                match take(src, 1)?[0] {
                    0 => Ok(None),
                    1 => {
                        let v = i64v(src)?;
                        if v < 0 {
                            return Err(codec("negative watermark/event time in buffered ET freeze"));
                        }
                        Ok(Some(v))
                    }
                    _ => Err(codec("invalid optional tag in buffered ET freeze")),
                }
            };
            let wm = opt(src)?;
            let max_event_time = opt(src)?;
            let last_effective = opt(src)?;
            // Structural: a watermark never exceeds the max observed time,
            // committed progress never exceeds a watermark, and retained
            // state implies at least one observed event.
            if wm.is_some() != max_event_time.is_some()
                || wm.zip(max_event_time).is_some_and(|(w, m)| w > m)
                || last_effective.zip(wm).is_some_and(|(l, w)| l > w)
                || (last_effective.is_some() && wm.is_none())
                || (header.entries > 0 && max_event_time.is_none())
            {
                return Err(codec("inconsistent watermark state in buffered ET freeze"));
            }
            if max_time.is_some_and(|t| max_event_time.is_none_or(|m| t > m)) {
                return Err(codec("buffered ET event time exceeds max observed event time"));
            }
            Some(BufferedClockFreeze { idle, wm, max_event_time, last_effective })
        } else {
            None
        };
        let pt_now = if pt {
            // PT arrival times are logical clock readings <= the barrier clock.
            let now = i64v(src)?;
            if now < 0 || max_time.is_some_and(|t| t > now) {
                return Err(codec("PT buffered event time exceeds the frame's logical clock"));
            }
            Some(now)
        } else {
            None
        };
        *resident = total.saturating_add(128);
        Ok(Self {
            operator: header.operator,
            slot: header.slot,
            kind: header.kind,
            size,
            params,
            groups,
            clock,
            pt_now,
            resident: *resident,
        })
    }
}

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message)
}
fn overflow() -> SparrowError {
    SparrowError::new(
        ErrorCode::IntegerOverflow,
        "window timestamp/sequence overflow",
    )
}

impl BufferedWindow {
    pub(crate) fn new(
        spec: WindowSpec,
        input: Schema,
        owner: Arc<MemoryOwner>,
        max_keys: usize,
        max_timers: usize,
        external: bool,
    ) -> Result<Self> {
        spec.validate()?;
        if !spec.kind.is_buffered() {
            return Err(invalid("expected a buffered window"));
        }
        let keys = spec
            .keys
            .iter()
            .map(|k| {
                input
                    .index_of_name(k)
                    .ok_or_else(|| invalid("unknown window key"))
            })
            .collect::<Result<Vec<_>>>()?;
        let event_time = spec
            .event_time_field
            .as_ref()
            .map(|k| {
                input
                    .index_of_name(k)
                    .ok_or_else(|| invalid("unknown event-time field"))
            })
            .transpose()?;
        let output = sparrow_plan::window_output_schema(&input, &spec)?;
        let expressions = spec
            .aggs
            .iter()
            .map(|a| a.input.as_ref().map(|e| bind(e, &input)).transpose())
            .collect::<Result<Vec<_>>>()?;
        let allocation = expressions
            .iter()
            .map(|e| e.as_ref().map(AllocationBound::for_expr))
            .collect();
        let mut hub = WatermarkHub::new();
        if let Some(binding) = spec.binding() {
            hub = hub.with_binding(binding)?;
        }
        hub.register(InputId::SINGLE)?;
        if max_keys == 0 {
            return Err(invalid("window requires a nonzero key limit"));
        }
        Ok(Self {
            schedule: BTreeSet::new(),
            groups: BTreeMap::new(),
            spec,
            input,
            output,
            keys,
            event_time,
            expressions,
            allocation,
            max_keys: max_keys.min(owner.budget().max_state_keys),
            max_timers: max_timers.min(owner.budget().max_timers),
            owner,
            hub,
            external,
            bytes: 0,
            last_now: 0,
            max_group_rows_seen: 0,
            durable: false,
        })
    }


    /// Checkpointed operation: sliding count (v31/v32), ET sliding / ET
    /// session (v33). PT buffered kinds and graph mode have no codec.
    pub(crate) fn set_durable(&mut self) -> Result<()> {
        if !sparrow_plan::checkpoint::checkpointable_buffered(self.spec.kind) || self.external {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "only linear sliding count / ET sliding / ET session windows have a buffered checkpoint codec",
            ));
        }
        self.durable = true;
        Ok(())
    }

    pub(crate) fn is_durable(&self) -> bool {
        self.durable
    }

    fn freeze_kind(&self) -> u8 {
        sparrow_plan::compat::window_kind_tag(self.spec.kind)
    }

    /// (size, delay) for ET/PT sliding, (gap, max_duration) for sessions.
    fn et_params(&self) -> Option<(i64, i64)> {
        match self.spec.kind {
            WindowKind::SlidingEventTime { size_micros, delay_micros }
            | WindowKind::SlidingProcessingTime { size_micros, delay_micros } => Some((size_micros, delay_micros)),
            WindowKind::SessionEventTime { gap_micros, max_duration_micros }
            | WindowKind::SessionProcessingTime { gap_micros, max_duration_micros } => {
                Some((gap_micros, max_duration_micros))
            }
            _ => None,
        }
    }

    fn sliding_size(&self) -> Result<u64> {
        match self.spec.kind {
            WindowKind::SlidingCount { size, .. } => Ok(size),
            _ => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "buffered window kind has no checkpoint codec",
            )),
        }
    }

    pub(crate) fn check_freeze_bound(&self, max_keys: usize) -> Result<()> {
        if self.groups.len() > max_keys {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "buffered freeze group count exceeds max_state_keys; refusing encode",
            ));
        }
        Ok(())
    }

    /// Upper bound of the encoded frame (resident bytes bound every scalar's
    /// value encoding), so the caller can admit it before allocating.
    pub(crate) fn estimated_freeze_bytes(&self) -> usize {
        // + ET params (16) and clock tail (1 + 3×9); per event t + pending (9).
        self.groups.iter().fold(11 + 4 + 16 + 28 + 64, |n, (key, group)| {
            let n = key
                .values
                .iter()
                .fold(n.saturating_add(2 + 8 + 4), |n, v| n.saturating_add(v.resident_bytes()));
            group.events.values().fold(n, |n, e| {
                e.values
                    .iter()
                    .fold(n.saturating_add(8 + 9 + 2), |n, v| n.saturating_add(v.resident_bytes()))
            })
        })
    }

    /// Encode the codec 4 frame. Header layout matches `FreezeHeader`.
    pub(crate) fn encode_freeze_into(
        &self,
        operator: sparrow_model::OperatorId,
        out: &mut Vec<u8>,
        max_keys: usize,
    ) -> Result<()> {
        // Only a durable (v31/v32) window holds the exact restore credit; a
        // restart_fresh window's accounting differs, so never publish it.
        if !self.durable {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "buffered window is not checkpointable (restart_fresh); codec 4 requires v31/v32/v33",
            )
            .context("checkpoint_guard", "buffered_profile_mismatch"));
        }
        self.check_freeze_bound(max_keys)?;
        let et = self.et_params();
        out.extend_from_slice(&operator.raw().to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(self.freeze_kind());
        out.extend_from_slice(&(self.groups.len() as u32).to_le_bytes());
        out.extend_from_slice(BWF_MAGIC);
        match et {
            Some((a, b)) => {
                out.extend_from_slice(&a.to_le_bytes());
                out.extend_from_slice(&b.to_le_bytes());
            }
            None => out.extend_from_slice(&self.sliding_size()?.to_le_bytes()),
        }
        let sliding_et = matches!(
            self.spec.kind,
            WindowKind::SlidingEventTime { .. } | WindowKind::SlidingProcessingTime { .. }
        );
        for (key, group) in &self.groups {
            out.extend_from_slice(&(key.values.len() as u16).to_le_bytes());
            for value in &key.values {
                value.encode_value(out)?;
            }
            out.extend_from_slice(&group.sequence.to_le_bytes());
            out.extend_from_slice(&(group.events.len() as u32).to_le_bytes());
            for (&(t, seq), event) in &group.events {
                if et.is_some() {
                    out.extend_from_slice(&t.to_le_bytes());
                    out.extend_from_slice(&seq.to_le_bytes());
                    out.push(u8::from(sliding_et && event.pending));
                } else {
                    out.extend_from_slice(&seq.to_le_bytes());
                }
                out.extend_from_slice(&(event.values.len() as u16).to_le_bytes());
                for value in &event.values {
                    value.encode_value(out)?;
                }
            }
        }
        if self.is_pt() {
            // v34/v35 tail: the logical clock this frame was frozen at.
            out.extend_from_slice(&self.last_now.to_le_bytes());
        }
        if self.is_et() {
            let (activity, wm, max_event_time) = self
                .hub
                .input_state(InputId::SINGLE)
                .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "ET window input not registered"))?;
            out.push(u8::from(activity == sparrow_model::InputActivity::Idle));
            for v in [wm, max_event_time, self.hub.last_effective()] {
                match v {
                    Some(v) => {
                        out.push(1);
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                    None => out.push(0),
                }
            }
        }
        Ok(())
    }

    /// Validate a decoded frame against this live operator without mutating.
    pub(crate) fn validate_restore(
        &self,
        operator: sparrow_model::OperatorId,
        freeze: &BufferedFreeze,
    ) -> Result<()> {
        let mismatch = |m: &str| {
            SparrowError::new(ErrorCode::UnsupportedRestore, m.to_owned())
                .context("checkpoint_guard", "buffered_state_mismatch")
        };
        if !self.groups.is_empty() {
            return Err(mismatch("buffered restore requires an empty operator"));
        }
        let params_match = match self.et_params() {
            Some(p) => freeze.params == p && freeze.size == 0,
            None => freeze.size == self.sliding_size()? && freeze.params == (0, 0),
        };
        if !self.durable
            || freeze.operator != operator
            || freeze.kind != self.freeze_kind()
            || !params_match
            || freeze.clock.is_some() != self.is_et()
            || freeze.pt_now.is_some() != self.is_pt()
        {
            return Err(mismatch("buffered freeze operator/kind/parameters differ from the live window"));
        }
        if freeze.groups.len() > self.max_keys {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "restored buffered groups exceed max_state_keys",
            ));
        }
        let key_types: Vec<_> = self.keys.iter().map(|i| &self.input.fields[*i].data_type).collect();
        let value_types = self
            .spec
            .aggs
            .iter()
            .map(|a| a.input.as_ref().map(|_| a.input_type(&self.input)).transpose())
            .collect::<Result<Vec<_>>>()?;
        let mut previous: Option<Vec<u8>> = None;
        for group in &freeze.groups {
            if group.key.len() != key_types.len()
                || group.key.iter().zip(&key_types).any(|(v, t)| !v.is_null() && !v.matches_type(t))
            {
                return Err(mismatch("buffered freeze key arity/type differs from the live window"));
            }
            let mut encoded = Vec::new();
            for value in &group.key {
                value.encode_key(&mut encoded);
                encoded.push(0xff);
            }
            if previous.as_ref().is_some_and(|p| *p >= encoded) {
                return Err(codec("buffered freeze keys are duplicated or out of order"));
            }
            previous = Some(encoded);
            let et = freeze.clock.is_some() || freeze.pt_now.is_some();
            if group.times.len() != if et { group.events.len() } else { 0 }
                || (et && group.events.len() > self.spec.max_buffered_rows)
            {
                return Err(mismatch("buffered freeze event times/retained count differ from the live window"));
            }
            for (_, values) in &group.events {
                if values.len() != value_types.len()
                    || values.iter().zip(&value_types).any(|(v, t)| match t {
                        Some(t) => !v.is_null() && !v.matches_type(t),
                        None => !matches!(v, Scalar::Null),
                    })
                {
                    return Err(mismatch("buffered freeze input arity/type differs from the live aggregates"));
                }
            }
        }
        if let Some(clock) = freeze.clock {
            self.validate_et_cut(freeze, clock).map_err(|e| {
                if e.code == ErrorCode::UnsupportedRestore { e } else { mismatch(&e.message) }
            })?;
        }
        if let Some(now) = freeze.pt_now {
            // Derived v34/v35 invariants: the executor drains every deadline
            // <= the logical clock before it handles a barrier, and each key
            // owns exactly one timer.
            if freeze.groups.len() > self.max_timers {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "restored PT buffered timers exceed max_timers",
                ));
            }
            for group in &freeze.groups {
                if self.frame_deadline(&group.times).is_none_or(|d| d <= now) {
                    return Err(mismatch("PT buffered state has a deadline already due at the cut"));
                }
            }
        }
        Ok(())
    }

    /// The live `reschedule` rule evaluated over a frame's (time, pending)
    /// list (sorted by (time, seq)).
    fn frame_deadline(&self, times: &[(i64, bool)]) -> Option<i64> {
        match self.spec.kind {
            WindowKind::SlidingEventTime { size_micros, delay_micros }
            | WindowKind::SlidingProcessingTime { size_micros, delay_micros } => {
                match times.iter().find(|(_, p)| *p) {
                    Some((t, _)) => t.checked_add(delay_micros).and_then(|t| t.checked_add(1)),
                    None => times.first().and_then(|(t, _)| t.checked_add(size_micros)),
                }
            }
            WindowKind::SessionEventTime { gap_micros, max_duration_micros }
            | WindowKind::SessionProcessingTime { gap_micros, max_duration_micros } => {
                let first = times.first().map(|(t, _)| *t)?;
                let cap = first.saturating_add(max_duration_micros);
                let mut end = first.saturating_add(gap_micros).min(cap);
                for (t, _) in times {
                    if *t >= end {
                        break;
                    }
                    end = t.saturating_add(gap_micros).min(cap);
                }
                Some(end)
            }
            _ => None,
        }
    }

    /// Derived v33 invariants: the generator state is exactly what the live
    /// single linear input produces (no Idle/Watermark controls exist on a
    /// linear File source), and nothing was due at the cut (the executor
    /// drains every deadline <= progress before it handles a barrier).
    fn validate_et_cut(&self, freeze: &BufferedFreeze, clock: BufferedClockFreeze) -> Result<()> {
        let mismatch = |m: &str| {
            Err(SparrowError::new(ErrorCode::UnsupportedRestore, m.to_owned())
                .context("checkpoint_guard", "buffered_state_mismatch"))
        };
        let ooo = self.spec.binding().map(|b| b.out_of_orderness_micros);
        if clock.idle {
            return mismatch("linear File ET input cannot be idle at a cut");
        }
        let expected_wm = clock
            .max_event_time
            .map(|m| ooo.map_or(m, |o| m.saturating_sub(o).max(0)));
        if clock.wm != expected_wm || clock.last_effective != clock.wm {
            return mismatch("ET watermark state differs from its observed event times");
        }
        let Some(progress) = clock.last_effective else {
            return Ok(()); // no event observed: groups are empty (decode check)
        };
        for group in &freeze.groups {
            let deadline = match self.spec.kind {
                WindowKind::SlidingEventTime { size_micros, delay_micros } => {
                    match group.times.iter().find(|(_, p)| *p) {
                        Some((t, _)) => t.checked_add(delay_micros).and_then(|t| t.checked_add(1)),
                        None => group.times.first().and_then(|(t, _)| t.checked_add(size_micros)),
                    }
                }
                WindowKind::SessionEventTime { gap_micros, max_duration_micros } => {
                    let first = group.times.first().map(|(t, _)| *t).unwrap_or(0);
                    let cap = first.saturating_add(max_duration_micros);
                    let mut end = first.saturating_add(gap_micros).min(cap);
                    for (t, _) in &group.times {
                        if *t >= end {
                            break;
                        }
                        end = t.saturating_add(gap_micros).min(cap);
                    }
                    Some(end)
                }
                _ => None,
            };
            if deadline.is_none_or(|d| d <= progress) {
                return mismatch("buffered ET state has a deadline already due at the cut");
            }
        }
        Ok(())
    }

    /// Rebuild state from a validated frame, consuming it (values move, not
    /// copy). Each key/event takes exactly the credit the live v31 path takes.
    pub(crate) fn restore_freeze(
        &mut self,
        operator: sparrow_model::OperatorId,
        freeze: BufferedFreeze,
    ) -> Result<()> {
        self.validate_restore(operator, &freeze)?;
        for group in freeze.groups {
            let mut encoded = Vec::new();
            for value in &group.key {
                value.encode_key(&mut encoded);
                encoded.push(0xff);
            }
            let credit = self.owner.acquire(CreditKind::Retention, key_credit(&group.key))?;
            self.bytes = self.bytes.saturating_add(credit.bytes());
            let key = Arc::new(Key { encoded, values: group.key, credit });
            let mut events = BTreeMap::new();
            let session = matches!(
                self.spec.kind,
                WindowKind::SessionEventTime { .. } | WindowKind::SessionProcessingTime { .. }
            );
            for (i, (seq, values)) in group.events.into_iter().enumerate() {
                let credit = self.owner.acquire(CreditKind::Retention, event_credit(&values))?;
                self.bytes = self.bytes.saturating_add(credit.bytes());
                // Sliding count keys by sequence. ET keys by event time; live
                // session events keep pending=true (never read for sessions).
                let (t, pending) = match group.times.get(i) {
                    Some(&(t, pending)) => (t, pending || session),
                    None => (seq as i64, true),
                };
                events.insert((t, seq), Event { values, pending, credit });
            }
            self.max_group_rows_seen = self.max_group_rows_seen.max(events.len());
            if self
                .groups
                .insert(Arc::clone(&key), Group { events, sequence: group.sequence, deadline: None })
                .is_some()
            {
                return Err(codec("duplicate buffered freeze key"));
            }
            // Deadlines are derived state: rebuild with the live rule.
            self.reschedule(&key)?;
        }
        if let Some(now) = freeze.pt_now {
            self.last_now = now;
        }
        if let Some(clock) = freeze.clock {
            let activity = if clock.idle {
                sparrow_model::InputActivity::Idle
            } else {
                sparrow_model::InputActivity::Active
            };
            self.hub.restore_input(
                InputId::SINGLE,
                activity,
                clock.wm,
                clock.max_event_time,
                clock.last_effective,
            )?;
        }
        Ok(())
    }

    /// v34/v35: bind the durable logical clock. A restored operator must
    /// have been frozen at exactly the snapshot's PTC1 cut; a fresh one starts
    /// at the source's initial cut. Never reads a host clock.
    pub(crate) fn bind_processing_cut(&mut self, now: i64, restored: bool) -> Result<()> {
        if !self.is_pt() || now < 0 {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore, "invalid PT buffered clock policy"));
        }
        if restored {
            if self.last_now != now {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "PT buffered frame clock differs from the processing-time cut",
                )
                .context("checkpoint_guard", "buffered_state_mismatch"));
            }
        } else if !self.groups.is_empty() {
            return Err(SparrowError::new(ErrorCode::Internal, "fresh PT buffered window is not empty"));
        } else {
            self.last_now = now;
        }
        Ok(())
    }

    pub(crate) fn is_et(&self) -> bool {
        self.spec.kind.uses_event_time()
    }
    pub(crate) fn is_pt(&self) -> bool {
        self.spec.kind.uses_processing_time_timer()
    }
    pub(crate) fn key_count(&self) -> usize {
        self.groups.len()
    }
    pub(crate) fn retention_bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) fn timers(&self) -> usize {
        if self.is_pt() {
            self.schedule.len()
        } else {
            0
        }
    }
    pub(crate) fn deadline(&self) -> Option<i64> {
        self.is_pt()
            .then(|| self.schedule.first().map(|(t, _)| *t))
            .flatten()
    }
    pub(crate) fn progress(&self) -> Option<i64> {
        self.is_et().then(|| self.hub.progress()).flatten()
    }
    pub(crate) fn watermark(&mut self, input: InputId, time: i64) -> Result<()> {
        self.hub.set_watermark(input, time)?;
        Ok(())
    }
    pub(crate) fn activity(&mut self, input: InputId, active: bool) -> Result<()> {
        if active {
            self.hub.mark_active(input)?;
        } else {
            self.hub.mark_idle(input)?;
        }
        Ok(())
    }
    /// Conservative bounded CPU charge; callers split it into work quanta.
    pub(crate) fn work_bound(&self) -> u64 {
        (self
            .max_group_rows_seen
            .saturating_add(1)
            .min(self.spec.max_buffered_rows) as u64)
            .saturating_mul(self.spec.aggs.len() as u64 + 8)
            .max(1)
    }
    pub(crate) fn now(&mut self, now: i64) -> Result<i64> {
        if self.spec.kind.is_count() {
            return Ok(now);
        }
        if now < 0 {
            return Err(invalid("processing time must be nonnegative"));
        }
        // Wall-clock corrections never reopen a PT window or reverse arrivals.
        self.last_now = self.last_now.max(now);
        Ok(self.last_now)
    }

    fn key(&self, row: &Row) -> Result<Arc<Key>> {
        // Conservative singleton-node slack for both group/index B-trees.
        let bytes = self.keys.iter().fold(KEY_FIXED, |n, i| {
            n.saturating_add(row.values[*i].resident_bytes().saturating_mul(4))
        });
        let credit = self.owner.acquire(CreditKind::Retention, bytes)?;
        let values: Vec<_> = self
            .keys
            .iter()
            .map(|i| row.values[*i].detach_copy())
            .collect();
        let mut encoded = Vec::new();
        for value in &values {
            value.encode_key(&mut encoded);
            encoded.push(0xff);
        }
        Ok(Arc::new(Key {
            encoded,
            values,
            credit,
        }))
    }

    fn event(&self, row: &Row) -> Result<Event> {
        let _columns = self.owner.acquire(
            CreditKind::Reservation,
            row.values
                .len()
                .saturating_mul(std::mem::size_of::<usize>())
                .saturating_add(64),
        )?;
        let columns: Vec<_> = row.values.iter().map(Scalar::resident_bytes).collect();
        let (resident, transient) =
            self.allocation
                .iter()
                .fold((1024usize, 256usize), |(r, t), a| {
                    let e = a.as_ref().map(|a| a.estimate(&columns)).unwrap_or_default();
                    (
                        r.saturating_add(e.value.max(std::mem::size_of::<Scalar>())),
                        t.saturating_add(e.allocated)
                            .saturating_add(e.value.saturating_mul(2)),
                    )
                });
        // Fallible collect may round capacity up; admit unused Scalar slots too.
        let resident = resident.saturating_add(
            self.expressions
                .len()
                .saturating_mul(std::mem::size_of::<Scalar>()),
        );
        let credit = self.owner.acquire(CreditKind::Retention, resident)?;
        let _scratch = self.owner.acquire(CreditKind::Reservation, transient)?;
        let values = self
            .expressions
            .iter()
            .map(|expr| match expr {
                Some(expr) => eval_bound(expr, &row.values).map(|v| v.detach_copy()),
                None => Ok(Scalar::Null),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut credit = credit;
        if self.durable {
            // Same rule as restore: actual stored values plus fixed overhead.
            let exact = event_credit(&values);
            if exact > credit.bytes() {
                credit.grow_to(exact)?;
            } else {
                credit.shrink_to(exact)?;
            }
        }
        Ok(Event {
            values,
            pending: true,
            credit,
        })
    }

    pub(crate) fn push(&mut self, row: &Row, now: i64) -> Result<Ingest> {
        if row.values.len() != self.input.fields.len() {
            return Err(invalid("window row/schema width mismatch"));
        }
        let now = self.now(now)?;
        let timestamp = if let Some(index) = self.event_time {
            match row.values[index] {
                Scalar::Int64(v) if v >= 0 => v,
                Scalar::TimestampMicrosUTC(v) if v >= 0 => v,
                _ => return Err(invalid("event time must be a nonnegative timestamp/int64")),
            }
        } else {
            now
        };
        if self.is_et() {
            if self
                .spec
                .binding()
                .and_then(|b| b.max_future_skew_micros)
                .is_some_and(|s| timestamp > now.saturating_add(s))
            {
                return Ok(Ingest::Future);
            }
            if self.progress().is_some_and(|wm| timestamp < wm) {
                return Ok(Ingest::Late);
            }
        }
        // Validate all endpoint arithmetic before retaining input.
        match self.spec.kind {
            WindowKind::SlidingProcessingTime {
                size_micros,
                delay_micros,
            }
            | WindowKind::SlidingEventTime {
                size_micros,
                delay_micros,
            } => {
                timestamp
                    .checked_add(delay_micros)
                    .and_then(|t| t.checked_add(1))
                    .ok_or_else(overflow)?;
                timestamp.checked_add(size_micros).ok_or_else(overflow)?;
            }
            WindowKind::SessionProcessingTime {
                gap_micros,
                max_duration_micros,
            }
            | WindowKind::SessionEventTime {
                gap_micros,
                max_duration_micros,
            } => {
                timestamp
                    .checked_add(gap_micros.max(max_duration_micros))
                    .ok_or_else(overflow)?;
            }
            _ => {}
        }
        let candidate = self.key(row)?;
        let key = self
            .groups
            .get_key_value(&candidate)
            .map(|(key, _)| Arc::clone(key))
            .unwrap_or(candidate);
        let existing = self.groups.get(&key);
        if existing.is_none()
            && (self.groups.len() >= self.max_keys
                || (self.is_pt() && self.schedule.len() >= self.max_timers))
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "buffered window key/timer limit exceeded",
            ));
        }
        let sequence = existing
            .map_or(0, |g| g.sequence)
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(overflow)?;
        if !self.spec.kind.is_count()
            && existing.is_some_and(|g| g.events.len() >= self.spec.max_buffered_rows)
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "window max_buffered_rows exceeded; input is not silently dropped",
            ));
        }
        let event = self.event(row)?;
        self.bytes = self.bytes.saturating_add(event.credit.bytes());
        if existing.is_none() {
            self.bytes = self.bytes.saturating_add(key.credit.bytes());
        }
        let group = self.groups.entry(Arc::clone(&key)).or_default();
        if let Some(deadline) = group.deadline.take() {
            self.schedule.remove(&(deadline, Arc::clone(&key)));
        }
        group.sequence = sequence;
        // Count uses arrival order regardless of clocks.
        group.events.insert(
            (
                if self.spec.kind.is_count() {
                    sequence as i64
                } else {
                    timestamp
                },
                sequence,
            ),
            event,
        );
        self.max_group_rows_seen = self.max_group_rows_seen.max(group.events.len());
        if let WindowKind::SlidingCount { size, .. } = self.spec.kind {
            while group.events.len() > size as usize {
                Self::pop_first(group, &mut self.bytes);
            }
        }
        let immediate = match self.spec.kind {
            WindowKind::SlidingCount { size, step } if sequence >= size && sequence % step == 0 => {
                Some((
                    sequence as i64 - size as i64,
                    sequence as i64,
                    i64::MIN,
                    i64::MAX,
                ))
            }
            WindowKind::SlidingProcessingTime {
                size_micros,
                delay_micros: 0,
            } => Some((
                timestamp - size_micros + 1,
                timestamp + 1,
                timestamp - size_micros + 1,
                timestamp + 1,
            )),
            _ => None,
        };
        let out = immediate
            .map(|(start, end, low, high)| self.aggregate(&key, start, end, low, high))
            .transpose()?;
        if matches!(
            self.spec.kind,
            WindowKind::SlidingProcessingTime {
                delay_micros: 0,
                ..
            }
        ) {
            if let Some(event) = self
                .groups
                .get_mut(&key)
                .and_then(|g| g.events.get_mut(&(timestamp, sequence)))
            {
                event.pending = false;
            }
        }
        self.reschedule(&key)?;
        if self.is_et() && !self.external {
            self.hub.observe_event(InputId::SINGLE, timestamp, now)?;
        }
        Ok(Ingest::Accepted(out))
    }

    fn aggregate(
        &self,
        key: &Arc<Key>,
        start: i64,
        end: i64,
        low: i64,
        high: i64,
    ) -> Result<RowBatch> {
        let group = &self.groups[key];
        // Covers detached MIN/MAX candidates, final values and builder overlap.
        // Only this key's bounded inputs are charged, never all operator state.
        let bytes = group.events.values().fold(
            key.credit
                .bytes()
                .saturating_add(self.spec.aggs.len().saturating_mul(256))
                .saturating_add(512),
            |n, e| {
                e.values.iter().fold(n, |n, v| {
                    n.saturating_add(v.resident_bytes().saturating_mul(3))
                })
            },
        );
        let _scratch = self.owner.acquire(CreditKind::Reservation, bytes)?;
        let mut accs = self
            .spec
            .aggs
            .iter()
            .map(|a| Accumulator::new(a.func, a.input_type(&self.input)?, a.count_star))
            .collect::<Result<Vec<_>>>()?;
        for (_, event) in group
            .events
            .iter()
            .filter(|((ts, _), _)| *ts >= low && (self.spec.kind.is_count() || *ts < high))
        {
            for (acc, value) in accs.iter_mut().zip(&event.values) {
                acc.update(value)?;
            }
        }
        let mut values: Vec<_> = key.values.iter().map(Scalar::detach_copy).collect();
        values.push(Scalar::Int64(start));
        values.push(Scalar::Int64(end));
        values.extend(accs.iter().map(Accumulator::finish));
        crate::window::finish_rows_metered(&self.output, vec![Row { values }], &self.owner)?
            .ok_or_else(|| SparrowError::new(ErrorCode::Internal, "window output missing"))
    }

    fn session(group: &Group, gap: i64, max: i64) -> Option<(i64, i64)> {
        let (&(first, _), _) = group.events.first_key_value()?;
        let mut end = (first + gap).min(first + max);
        for (&(ts, _), _) in &group.events {
            if ts >= end {
                break;
            }
            end = (ts + gap).min(first + max);
        }
        Some((first, end))
    }

    fn reschedule(&mut self, key: &Arc<Key>) -> Result<()> {
        let group = self.groups.get_mut(key).expect("existing group");
        if let Some(old) = group.deadline.take() {
            self.schedule.remove(&(old, Arc::clone(key)));
        }
        let deadline = match self.spec.kind {
            WindowKind::SlidingProcessingTime {
                size_micros,
                delay_micros,
            }
            | WindowKind::SlidingEventTime {
                size_micros,
                delay_micros,
            } => {
                let pending = group
                    .events
                    .iter()
                    .find(|(_, e)| e.pending)
                    .map(|(&(t, _), _)| t);
                let expiration = group
                    .events
                    .first_key_value()
                    .map(|(&(t, _), _)| t + size_micros);
                // A delayed trigger may still need history older than now-size.
                match (pending, expiration) {
                    (Some(t), _) => Some(t + delay_micros + 1),
                    (None, exp) => exp,
                }
            }
            WindowKind::SessionProcessingTime {
                gap_micros,
                max_duration_micros,
            }
            | WindowKind::SessionEventTime {
                gap_micros,
                max_duration_micros,
            } => Self::session(group, gap_micros, max_duration_micros).map(|(_, end)| end),
            _ => None,
        };
        group.deadline = deadline;
        if let Some(time) = deadline {
            self.schedule.insert((time, Arc::clone(key)));
        }
        Ok(())
    }

    fn pop_first(group: &mut Group, bytes: &mut usize) {
        if let Some((_, event)) = group.events.pop_first() {
            *bytes = bytes.saturating_sub(event.credit.bytes());
            drop(event);
        }
    }

    /// Harness-only (timer-driven cuts): the earliest deadline that emits a
    /// final, excluding sliding history-eviction deadlines. O(keys).
    #[cfg(feature = "process-fault-pause")]
    pub(crate) fn output_deadline(&self) -> Option<i64> {
        match self.spec.kind {
            WindowKind::SlidingProcessingTime { delay_micros, .. }
            | WindowKind::SlidingEventTime { delay_micros, .. } => self
                .groups
                .values()
                .filter_map(|g| g.events.iter().find(|(_, e)| e.pending).map(|(&(t, _), _)| t + delay_micros + 1))
                .min(),
            _ => self.schedule.first().map(|(t, _)| *t),
        }
    }

    pub(crate) fn due(&self, time: i64) -> bool {
        self.schedule.first().is_some_and(|(t, _)| *t <= time)
    }

    /// One bounded step: either one final output or history eviction. Output
    /// credit is acquired before changing final/pending state.
    pub(crate) fn take_due(&mut self, time: i64) -> Result<Option<RowBatch>> {
        let time = if self.is_pt() { self.now(time)? } else { time };
        let Some((deadline, key)) = self.schedule.first().filter(|(t, _)| *t <= time).cloned()
        else {
            return Ok(None);
        };
        let group = &self.groups[&key];
        let mut consumed = None;
        let output = match self.spec.kind {
            WindowKind::SlidingProcessingTime {
                size_micros,
                delay_micros,
            }
            | WindowKind::SlidingEventTime {
                size_micros,
                delay_micros,
            } => {
                let pending = group
                    .events
                    .iter()
                    .find(|(_, e)| e.pending)
                    .map(|(k, _)| *k);
                if let Some((ts, seq)) = pending.filter(|(t, _)| t + delay_micros + 1 <= time) {
                    let (start, end) = (ts - size_micros + 1, ts + delay_micros + 1);
                    consumed = Some((ts, seq));
                    Some(self.aggregate(&key, start, end, start, end)?)
                } else {
                    None
                }
            }
            WindowKind::SessionProcessingTime {
                gap_micros,
                max_duration_micros,
            }
            | WindowKind::SessionEventTime {
                gap_micros,
                max_duration_micros,
            } => {
                let (start, end) =
                    Self::session(group, gap_micros, max_duration_micros).expect("session");
                consumed = Some((end, 0));
                Some(self.aggregate(&key, start, end, start, end)?)
            }
            _ => None,
        };
        self.schedule.remove(&(deadline, Arc::clone(&key)));
        let group = self.groups.get_mut(&key).expect("existing group");
        group.deadline = None;
        match self.spec.kind {
            WindowKind::SlidingProcessingTime { size_micros, .. }
            | WindowKind::SlidingEventTime { size_micros, .. } => {
                if let Some(k) = consumed {
                    group.events.get_mut(&k).expect("trigger").pending = false;
                }
                let frontier = group
                    .events
                    .iter()
                    .find(|(_, e)| e.pending)
                    .map_or(time, |(&(t, _), _)| t.min(time));
                let cutoff = frontier.saturating_sub(size_micros);
                while group
                    .events
                    .first_key_value()
                    .is_some_and(|(&(t, _), e)| t <= cutoff && !e.pending)
                {
                    Self::pop_first(group, &mut self.bytes);
                }
            }
            _ => {
                if let Some((end, _)) = consumed {
                    while group
                        .events
                        .first_key_value()
                        .is_some_and(|(&(t, _), _)| t < end)
                    {
                        Self::pop_first(group, &mut self.bytes);
                    }
                }
            }
        }
        if group.events.is_empty() {
            self.groups.remove(&key);
            self.bytes = self.bytes.saturating_sub(key.credit.bytes());
        } else {
            self.reschedule(&key)?;
        }
        Ok(output)
    }
}

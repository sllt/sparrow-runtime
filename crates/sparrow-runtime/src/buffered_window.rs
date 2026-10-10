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
const KEY_FIXED: usize = 4096;
const EVENT_FIXED: usize = 1024;
const MAX_ARITY: usize = 64;

/// Retention credit of one group key: fixed node/Arc/container slack plus the
/// actual detached key values (both B-trees may hold a copy of the bytes).
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
}

/// Decoded codec 4 participant. `resident` is the exact Retention the rebuild
/// acquires (sum of [`key_credit`] + [`event_credit`]), computed identically by
/// the bounded scan pass so the Store can reserve it before materializing.
#[derive(Debug, PartialEq)]
pub struct BufferedFreeze {
    pub operator: sparrow_model::OperatorId,
    pub slot: sparrow_model::StateSlotId,
    pub kind: u8,
    pub size: u64,
    pub groups: Vec<BufferedGroupFreeze>,
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
        if header.kind != SLIDING_COUNT_FREEZE_KIND || header.slot.raw() != 1 {
            return Err(codec("buffered window freeze kind/slot is not sliding count"));
        }
        if header.entries > max_groups {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "buffered window freeze group count exceeds max_state_keys",
            ));
        }
        if take(src, 4)? != BWF_MAGIC {
            return Err(codec("buffered window freeze lacks BWF1 grammar"));
        }
        let size = u64v(src)?;
        if size == 0 || size > i64::MAX as u64 {
            return Err(codec("invalid sliding count size in freeze"));
        }
        // Minimum per group: arity + sequence + event count + one event.
        if src.len() < header.entries.saturating_mul(2 + 8 + 4 + 8 + 2) {
            return Err(codec("declared buffered group count exceeds remaining bytes"));
        }
        let mut groups = Vec::with_capacity(if materialize { header.entries } else { 0 });
        let mut total = 0usize;
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
            // Exactly the last min(sequence, size) arrivals are retained.
            if sequence == 0
                || sequence > i64::MAX as u64
                || ne as u64 != sequence.min(size)
            {
                return Err(codec("buffered window retained input count disagrees with its sequence"));
            }
            if src.len() < ne.saturating_mul(8 + 2) {
                return Err(codec("declared buffered event count exceeds remaining bytes"));
            }
            let mut events = Vec::with_capacity(if materialize { ne } else { 0 });
            let first = sequence - ne as u64 + 1;
            for i in 0..ne {
                let seq = u64v(src)?;
                if seq != first + i as u64 {
                    return Err(codec("buffered window event sequences are not the contiguous tail"));
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
            if materialize {
                groups.push(BufferedGroupFreeze { key, sequence, events });
            }
        }
        *resident = total.saturating_add(128);
        Ok(Self {
            operator: header.operator,
            slot: header.slot,
            kind: header.kind,
            size,
            groups,
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

    pub(crate) fn is_sliding_count(&self) -> bool {
        matches!(self.spec.kind, WindowKind::SlidingCount { .. })
    }

    /// Checkpointed (v31/v32) operation; only sliding count has a codec.
    pub(crate) fn set_durable(&mut self) -> Result<()> {
        if !self.is_sliding_count() || self.external {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "only linear sliding count windows have a buffered checkpoint codec",
            ));
        }
        self.durable = true;
        Ok(())
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
        self.groups.iter().fold(11 + 4 + 8 + 64, |n, (key, group)| {
            let n = key
                .values
                .iter()
                .fold(n.saturating_add(2 + 8 + 4), |n, v| n.saturating_add(v.resident_bytes()));
            group.events.values().fold(n, |n, e| {
                e.values
                    .iter()
                    .fold(n.saturating_add(8 + 2), |n, v| n.saturating_add(v.resident_bytes()))
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
        self.check_freeze_bound(max_keys)?;
        let size = self.sliding_size()?;
        out.extend_from_slice(&operator.raw().to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(SLIDING_COUNT_FREEZE_KIND);
        out.extend_from_slice(&(self.groups.len() as u32).to_le_bytes());
        out.extend_from_slice(BWF_MAGIC);
        out.extend_from_slice(&size.to_le_bytes());
        for (key, group) in &self.groups {
            out.extend_from_slice(&(key.values.len() as u16).to_le_bytes());
            for value in &key.values {
                value.encode_value(out)?;
            }
            out.extend_from_slice(&group.sequence.to_le_bytes());
            out.extend_from_slice(&(group.events.len() as u32).to_le_bytes());
            for (&(_, seq), event) in &group.events {
                out.extend_from_slice(&seq.to_le_bytes());
                out.extend_from_slice(&(event.values.len() as u16).to_le_bytes());
                for value in &event.values {
                    value.encode_value(out)?;
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
        if freeze.operator != operator
            || freeze.kind != SLIDING_COUNT_FREEZE_KIND
            || freeze.size != self.sliding_size()?
        {
            return Err(mismatch("buffered freeze operator/kind/size differs from the live window"));
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
            for (seq, values) in group.events {
                let credit = self.owner.acquire(CreditKind::Retention, event_credit(&values))?;
                self.bytes = self.bytes.saturating_add(credit.bytes());
                events.insert((seq as i64, seq), Event { values, pending: true, credit });
            }
            self.max_group_rows_seen = self.max_group_rows_seen.max(events.len());
            if self
                .groups
                .insert(key, Group { events, sequence: group.sequence, deadline: None })
                .is_some()
            {
                return Err(codec("duplicate buffered freeze key"));
            }
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

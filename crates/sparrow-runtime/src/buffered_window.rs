//! Bounded, final-only sliding/session windows. No legacy checkpoint codec.
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
        })
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
        let bytes = self.keys.iter().fold(4096usize, |n, i| {
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

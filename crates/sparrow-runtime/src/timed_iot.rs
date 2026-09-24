//! Bounded HoldFor/Debounce state, driven only by ordered logical time.
//! A time tick is drained before its following input. No host clock is read.
#[cfg(test)]
use crate::iot::IotEntry;
use crate::iot::{IotFreeze, IotStats};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SparrowError,
};
use sparrow_plan::{InvalidValuePolicy, IotTimingSpec};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

struct Entry {
    key: Vec<Scalar>,
    row: Row,
    start: i64,
    deadline: Option<i64>,
    trailing: bool,
    _lease: MemoryLease,
}
pub(crate) struct TimedIot {
    operator: OperatorId,
    timing: IotTimingSpec,
    schema: Arc<Schema>,
    owner: Arc<MemoryOwner>,
    keys: Vec<usize>,
    fields: Vec<usize>,
    invalid: InvalidValuePolicy,
    max: usize,
    now: i64,
    timers: BTreeSet<(i64, Vec<u8>)>,
    entries: BTreeMap<Vec<u8>, Entry>,
    table: Option<MemoryLease>,
    stats: IotStats,
    _metadata: MemoryLease,
}
fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}
fn key_bytes(values: &[Scalar]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        value.encode_key(&mut bytes);
        bytes.push(0xff);
    }
    bytes
}
fn add(now: i64, duration: i64) -> Result<i64> {
    now.checked_add(duration)
        .ok_or_else(|| invalid("timed IoT deadline overflow"))
}
impl TimedIot {
    pub fn new(
        operator: OperatorId,
        spec: &sparrow_plan::IotSpec,
        schema: Arc<Schema>,
        owner: Arc<MemoryOwner>,
        keys: Vec<usize>,
        fields: Vec<usize>,
    ) -> Result<Self> {
        let max = spec
            .max_keys
            .min(owner.budget().max_state_keys)
            .min(owner.budget().max_timers)
            .min(crate::iot::MAX_IOT_FREEZE_ENTRIES);
        if max == 0 {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "timed IoT needs a bounded state/timer budget",
            ));
        }
        let metadata = owner.acquire(
            CreditKind::Reservation,
            2048 + (keys.len() + fields.len()) * 16,
        )?;
        Ok(Self {
            operator,
            timing: spec.timing.clone().expect("timed spec"),
            schema,
            owner,
            keys,
            fields,
            invalid: spec.invalid,
            max,
            now: 0,
            entries: BTreeMap::new(),
            timers: BTreeSet::new(),
            table: None,
            stats: IotStats::default(),
            _metadata: metadata,
        })
    }
    pub fn set_time(&mut self, now: i64) -> Result<()> {
        if now < self.now || now < 0 {
            return Err(invalid("processing time cannot move backwards"));
        }
        self.now = now;
        Ok(())
    }
    fn validate_row(&self, row: &Row) -> Result<bool> {
        if row.values.len() != self.schema.fields.len() {
            return Err(invalid("timed IoT row arity"));
        }
        for (value, field) in row.values.iter().zip(&self.schema.fields) {
            if (value.is_null() && !field.nullable)
                || (!value.is_null() && !value.matches_type(&field.data_type))
            {
                return Err(invalid("timed IoT row/schema mismatch"));
            }
        }
        if self.keys.iter().any(|&i| row.values[i].is_null()) {
            return Err(invalid("timed IoT null key"));
        }
        if self.fields.iter().any(|&i| {
            row.values[i].is_null() || matches!(row.values[i], Scalar::Float64(v) if !v.is_finite())
        }) {
            return match self.invalid {
                InvalidValuePolicy::Ignore => Ok(false),
                InvalidValuePolicy::Error => Err(invalid("timed IoT invalid observed value")),
            };
        }
        if row
            .values
            .iter()
            .any(|v| matches!(v, Scalar::Float64(x) if !x.is_finite()))
        {
            return Err(invalid("timed IoT retained row contains non-finite value"));
        }
        Ok(true)
    }
    fn release_empty(&mut self) {
        if self.entries.is_empty() {
            self.timers = BTreeSet::new();
            self.entries = BTreeMap::new();
            self.table = None;
        }
    }
    fn copy_entry(
        &self,
        key: Vec<Scalar>,
        row: &Row,
        start: i64,
        deadline: Option<i64>,
        trailing: bool,
        encoded_capacity: usize,
    ) -> Result<Entry> {
        let bytes = row
            .resident_bytes()
            .saturating_add(key.iter().map(Scalar::resident_bytes).sum::<usize>())
            .saturating_add(encoded_capacity.saturating_mul(2))
            .saturating_add(512);
        let lease = self.owner.acquire(CreditKind::Retention, bytes)?;
        Ok(Entry {
            key,
            row: row.detach_copy(),
            start,
            deadline,
            trailing,
            _lease: lease,
        })
    }
    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Option<RowBatch>> {
        self.set_time(now)?;
        if self.next_deadline().is_some_and(|at| at <= now) {
            return Err(invalid("due timers must be drained before timed IoT input"));
        }
        let mut out = RowBatchBuilder::new(
            self.schema.clone(),
            self.owner.clone(),
            CreditKind::Reservation,
            batch.num_rows().max(1),
            self.owner.budget().reservation_bytes,
        )?;
        for row in batch.rows() {
            self.stats.input_rows = self.stats.input_rows.saturating_add(1);
            if !self.validate_row(row)? {
                self.stats.invalid_rows = self.stats.invalid_rows.saturating_add(1);
                self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
                continue;
            }
            let _scratch = self.owner.acquire(
                CreditKind::Reservation,
                row.resident_bytes().saturating_mul(8).saturating_add(2048),
            )?;
            let key = self
                .keys
                .iter()
                .map(|&i| row.values[i].detach_copy())
                .collect::<Vec<_>>();
            let encoded = key_bytes(&key);
            let previous = self.entries.get(&encoded);
            let (start, deadline, trailing, emit) = match self.timing {
                IotTimingSpec::Alarm { .. } => return Err(invalid("alarm requires its independent state machine")),
                IotTimingSpec::HoldFor {
                    duration_micros, ..
                } => {
                    if row.values[self.fields[0]] == Scalar::Bool(false) {
                        if let Some(at) = previous.and_then(|e| e.deadline) {
                            self.timers.remove(&(at, encoded.clone()));
                        }
                        self.entries.remove(&encoded);
                        self.release_empty();
                        self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
                        continue;
                    }
                    match previous {
                        Some(e) => (e.start, e.deadline, e.trailing, false),
                        None => (now, Some(add(now, duration_micros)?), true, false),
                    }
                }
                IotTimingSpec::Debounce {
                    quiet_micros,
                    max_wait_micros,
                    leading,
                    trailing,
                    reset_on_repeat,
                    ..
                } => {
                    let start = previous.map_or(now, |e| e.start);
                    let repeated = previous.is_some_and(|e| {
                        self.fields
                            .iter()
                            .all(|&i| e.row.values[i] == row.values[i])
                    });
                    let deadline = if repeated && !reset_on_repeat {
                        previous
                            .and_then(|e| e.deadline)
                            .expect("live debounce timer")
                    } else {
                        add(now, quiet_micros)?.min(add(start, max_wait_micros)?)
                    };
                    (
                        start,
                        Some(deadline),
                        trailing && (!leading || previous.is_some()),
                        leading && previous.is_none(),
                    )
                }
            };
            if previous.is_none() && self.entries.len() >= self.max {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "timed IoT key/timer limit",
                ));
            }
            let fresh_table = if self.table.is_none() {
                Some(self.owner.acquire(CreditKind::Retention, 8192)?)
            } else {
                None
            };
            let entry = self.copy_entry(key, row, start, deadline, trailing, encoded.capacity())?;
            if emit {
                out.push(row.detach_copy())?;
            }
            if let Some(at) = previous.and_then(|e| e.deadline) {
                self.timers.remove(&(at, encoded.clone()));
            }
            if let Some(at) = deadline {
                self.timers.insert((at, encoded.clone()));
            }
            self.entries.insert(encoded, entry);
            if fresh_table.is_some() {
                self.table = fresh_table;
            }
            if emit {
                self.stats.emitted_rows = self.stats.emitted_rows.saturating_add(1);
            } else {
                self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
            }
        }
        if out.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(
            out.finish()?
                .with_origin(batch.origin())
                .with_source_operator(batch.source_operator()),
        ))
    }
    /// Drain one timer at a time: bounded work/output, no Vec of all due rows.
    pub fn take_due(&mut self, now: i64) -> Result<Option<RowBatch>> {
        self.set_time(now)?;
        {
            let Some((at, key)) = self.timers.first().filter(|(at, _)| *at <= now) else {
                return Ok(None);
            };
            let _index_scratch = self.owner.acquire(
                CreditKind::Reservation,
                key.len().saturating_mul(2).saturating_add(128),
            )?;
            let (at, key) = (*at, key.clone());
            let e = self.entries.get(&key).expect("selected timer");
            let output = if e.trailing {
                let _scratch = self.owner.acquire(
                    CreditKind::Reservation,
                    e.row.resident_bytes().saturating_mul(2).saturating_add(128),
                )?;
                let mut out = RowBatchBuilder::new(
                    self.schema.clone(),
                    self.owner.clone(),
                    CreditKind::Reservation,
                    1,
                    self.owner.budget().reservation_bytes,
                )?;
                out.push(e.row.detach_copy())?;
                Some(out.finish()?)
            } else {
                None
            };
            self.timers.remove(&(at, key.clone()));
            if matches!(self.timing, IotTimingSpec::HoldFor { .. }) {
                let e = self.entries.get_mut(&key).unwrap();
                e.deadline = None;
                e.trailing = false;
            } else {
                self.entries.remove(&key);
                self.release_empty();
            }
            if output.is_some() {
                self.stats.emitted_rows = self.stats.emitted_rows.saturating_add(1);
            }
            Ok(output)
        }
    }
    pub fn validate_cut(&mut self, now: i64) -> Result<()> {
        for entry in self.entries.values() {
            if entry.start > now || entry.deadline.is_some_and(|at| at <= now) {
                return Err(invalid(
                    "timed IoT restore cut has future start or overdue timer",
                ));
            }
            if let IotTimingSpec::HoldFor {
                duration_micros, ..
            } = self.timing
            {
                if entry.deadline.is_none() && add(entry.start, duration_micros)? > now {
                    return Err(invalid("hold_for fired latch precedes its deadline"));
                }
            }
        }
        self.set_time(now)
    }
    pub fn cleanup(&mut self) {
        self.timers = BTreeSet::new();
        self.entries = BTreeMap::new();
        self.table = None;
    }
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }
    pub fn max_keys(&self) -> usize {
        self.max
    }
    pub fn stats(&self) -> IotStats {
        self.stats
    }
    pub fn pending_timers(&self) -> usize {
        self.timers.len()
    }
    pub fn next_deadline(&self) -> Option<i64> {
        self.timers.first().map(|(at, _)| *at)
    }
    pub fn retention_bytes(&self) -> usize {
        self.table.as_ref().map_or(0, MemoryLease::bytes)
            + self
                .entries
                .values()
                .map(|e| e._lease.bytes())
                .sum::<usize>()
    }
    pub fn estimated_freeze_bytes(&self) -> usize {
        self.entries.values().fold(11usize, |n, e| {
            n.saturating_add(24).saturating_add(
                e.key
                    .iter()
                    .chain(&e.row.values)
                    .map(|v| v.encoded_value_len().unwrap_or(usize::MAX / 128))
                    .sum::<usize>(),
            )
        })
    }
    #[cfg(test)]
    pub fn freeze(&self) -> Result<IotFreeze> {
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            self.retention_bytes()
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        let entries = self
            .entries
            .values()
            .map(|e| {
                let mut values = vec![
                    Scalar::Int64(e.start),
                    Scalar::Int64(e.deadline.unwrap_or(-1)),
                    Scalar::Bool(e.trailing),
                ];
                values.extend(e.row.values.iter().map(Scalar::detach_copy));
                IotEntry {
                    key: e.key.iter().map(Scalar::detach_copy).collect(),
                    values,
                }
            })
            .collect();
        IotFreeze::new(self.operator, self.timing.state_kind(), entries)
    }
    pub fn encode(&self, out: &mut Vec<u8>, max: usize) -> Result<()> {
        if self.entries.len() > max
            || self.estimated_freeze_bytes() > crate::iot::MAX_IOT_FREEZE_BYTES
        {
            return Err(invalid("timed IoT freeze bound"));
        }
        let _scratch = self.owner.acquire(CreditKind::Reservation, 4096)?;
        let start = out.len();
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&3u16.to_le_bytes());
        out.push(self.timing.state_kind());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in self.entries.values() {
            let mut values = Vec::with_capacity(3 + e.row.values.len());
            values.extend([
                Scalar::Int64(e.start),
                Scalar::Int64(e.deadline.unwrap_or(-1)),
                Scalar::Bool(e.trailing),
            ]);
            values.extend(e.row.values.iter().cloned()); // short-lived Arc handles, no payload copies
            crate::iot::encode_entry_parts(out, &e.key, &values, start)?;
        }
        Ok(())
    }
    pub fn restore(&mut self, freeze: &IotFreeze) -> Result<()> {
        if freeze.operator != self.operator
            || freeze.kind != self.timing.state_kind()
            || freeze.entries.len() > self.max
        {
            return Err(invalid("timed IoT restore identity/bound mismatch"));
        }
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            freeze
                .resident_bytes()
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        let table = if freeze.entries.is_empty() {
            None
        } else {
            Some(self.owner.acquire(CreditKind::Retention, 8192)?)
        };
        let mut entries = BTreeMap::new();
        let mut timers = BTreeSet::new();
        for e in &freeze.entries {
            let (start, deadline, trailing) = match e.values.as_slice() {
                [Scalar::Int64(start), Scalar::Int64(deadline), Scalar::Bool(trailing), ..]
                    if *start >= 0 && (*deadline == -1 || *deadline > *start) =>
                {
                    (*start, (*deadline != -1).then_some(*deadline), *trailing)
                }
                _ => return Err(invalid("timed IoT freeze metadata")),
            };
            let row = Row {
                values: e.values[3..].iter().map(Scalar::detach_copy).collect(),
            };
            if !self.validate_row(&row)?
                || e.key.len() != self.keys.len()
                || e.key
                    .iter()
                    .zip(&self.keys)
                    .any(|(v, &i)| v != &row.values[i])
            {
                return Err(invalid("timed IoT freeze row/key mismatch"));
            }
            match self.timing {
                IotTimingSpec::Alarm { .. } => return Err(invalid("alarm cannot restore a legacy timed state")),
                IotTimingSpec::HoldFor {
                    duration_micros, ..
                } => {
                    if row.values[self.fields[0]] != Scalar::Bool(true)
                        || trailing != deadline.is_some()
                        || deadline.is_some_and(|at| start.checked_add(duration_micros) != Some(at))
                    {
                        return Err(invalid("hold_for freeze condition/deadline mismatch"));
                    }
                }
                IotTimingSpec::Debounce {
                    quiet_micros,
                    max_wait_micros,
                    leading,
                    trailing: enabled,
                    ..
                } => {
                    if deadline.is_none()
                        || deadline.is_some_and(|at| {
                            start
                                .checked_add(max_wait_micros)
                                .is_none_or(|max| at > max)
                        })
                        || (trailing && !enabled)
                        || (!leading && !trailing)
                        || deadline.is_some_and(|at| {
                            start.checked_add(quiet_micros).is_none_or(|min| at < min)
                        })
                    {
                        return Err(invalid("debounce freeze deadline mismatch"));
                    }
                }
            }
            let encoded = key_bytes(&e.key);
            let entry = self.copy_entry(
                e.key.iter().map(Scalar::detach_copy).collect(),
                &row,
                start,
                deadline,
                trailing,
                encoded.capacity(),
            )?;
            if let Some(at) = deadline {
                timers.insert((at, encoded.clone()));
            }
            if entries.insert(encoded, entry).is_some() {
                return Err(invalid("duplicate timed IoT key"));
            }
        }
        self.timers = timers;
        self.entries = entries;
        self.table = table;
        Ok(())
    }
}

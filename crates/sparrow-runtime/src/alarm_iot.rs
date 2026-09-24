//! Charged keyed storage around the pure alarm transition machine.
use crate::alarm::{Effects, Event, Phase, Policy, State};
#[cfg(test)]
use crate::iot::IotEntry;
use crate::iot::{IotFreeze, IotStats, MAX_IOT_FREEZE_BYTES, MAX_IOT_FREEZE_ENTRIES};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SparrowError,
};
use sparrow_plan::{InvalidValuePolicy, IotSpec, IotTimingSpec};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(crate) const KIND: u8 = 11;
pub(crate) const PREFIX: usize = 6;
fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

pub(crate) fn metadata(values: &[Scalar], now: Option<i64>) -> Result<State> {
    let [Scalar::UInt64(phase), Scalar::Int64(since), Scalar::Int64(deadline), Scalar::UInt64(episode), Scalar::Int64(not_before), Scalar::Int64(notification_since), ..] =
        values
    else {
        return Err(invalid("alarm state metadata types"));
    };
    let state = State {
        phase: Phase::from_tag(*phase)?,
        since: *since,
        deadline: (*deadline != -1).then_some(*deadline),
        episode: *episode,
        not_before: *not_before,
        notification_since: (*notification_since != -1).then_some(*notification_since),
    };
    if values.len() <= PREFIX
        || state.since < 0
        || state.not_before < 0
        || state.deadline.is_some_and(|at| at <= state.since)
        || state
            .notification_since
            .is_some_and(|at| at < 0 || at >= state.not_before)
        || matches!(state.phase, Phase::Pending | Phase::Recovering) != state.deadline.is_some()
        || (matches!(state.phase, Phase::Active | Phase::Recovering) && state.episode == 0)
        || (state.notification_since.is_some()
            && !matches!(state.phase, Phase::Active | Phase::Recovering))
        || now.is_some_and(|now| {
            state.since > now
                || state.deadline.is_some_and(|at| at <= now)
                || state
                    .notification_since
                    .is_some_and(|at| at > now || state.not_before <= now)
        })
    {
        return Err(invalid("alarm state metadata/cut mismatch"));
    }
    Ok(state)
}

fn prefix(state: State) -> [Scalar; PREFIX] {
    [
        Scalar::UInt64(state.phase.tag()),
        Scalar::Int64(state.since),
        Scalar::Int64(state.deadline.unwrap_or(-1)),
        Scalar::UInt64(state.episode),
        Scalar::Int64(state.not_before),
        Scalar::Int64(state.notification_since.unwrap_or(-1)),
    ]
}

struct Entry {
    key: Vec<Scalar>,
    row: Row,
    state: State,
    _lease: MemoryLease,
}

pub(crate) struct AlarmIot {
    operator: OperatorId,
    policy: Policy,
    input: Arc<Schema>,
    output: Arc<Schema>,
    owner: Arc<MemoryOwner>,
    keys: Vec<usize>,
    fields: Vec<usize>,
    invalid: InvalidValuePolicy,
    max: usize,
    now: i64,
    generation: Option<[u8; 16]>,
    retired_generation: Option<[u8; 16]>,
    // Entry leases also cover timer-index key copies. Drop the index first,
    // including on an error/unwind that bypasses explicit cleanup.
    timers: BTreeSet<(i64, Vec<u8>)>,
    entries: BTreeMap<Vec<u8>, Entry>,
    stats: IotStats,
    // Container/node slack and schema/indices are retained before allocation.
    _metadata: MemoryLease,
}

impl AlarmIot {
    pub fn new(
        operator: OperatorId,
        spec: &IotSpec,
        input: Arc<Schema>,
        owner: Arc<MemoryOwner>,
        keys: Vec<usize>,
        fields: Vec<usize>,
    ) -> Result<Self> {
        spec.validate(&input)?;
        let Some(IotTimingSpec::Alarm {
            activate_micros,
            resolve_micros,
            cooldown_micros,
            notification_max_age_micros,
            ..
        }) = spec.timing
        else {
            return Err(invalid("alarm operator requires alarm configuration"));
        };
        let policy = Policy {
            activate_micros,
            resolve_micros,
            cooldown_micros,
            notification_max_age_micros,
        };
        policy.validate()?;
        // Two logical deadlines per key, although the index stores their min.
        let max = spec
            .max_keys
            .min(owner.budget().max_state_keys)
            .min(owner.budget().max_timers / 2)
            .min(MAX_IOT_FREEZE_ENTRIES);
        if max == 0 {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "alarm requires key and two timer slots",
            ));
        }
        let metadata = owner.acquire(
            CreditKind::Reservation,
            8192 + input
                .fields
                .iter()
                .map(|f| f.name.len() * 2 + 256)
                .sum::<usize>()
                + (keys.len() + fields.len()) * 16,
        )?;
        let output = Arc::new(spec.output_schema(&input)?);
        Ok(Self {
            operator,
            policy,
            input,
            output,
            owner,
            keys,
            fields,
            invalid: spec.invalid,
            max,
            now: 0,
            generation: None,
            retired_generation: None,
            entries: BTreeMap::new(),
            timers: BTreeSet::new(),
            stats: IotStats::default(),
            _metadata: metadata,
        })
    }

    pub fn bind_generation(&mut self, generation: [u8; 16]) -> Result<()> {
        if generation == [0; 16]
            || self.retired_generation == Some(generation)
            || self
                .generation
                .is_some_and(|previous| previous != generation)
        {
            return Err(invalid(
                "alarm generation must be initialized once before restore/input",
            ));
        }
        self.generation = Some(generation);
        Ok(())
    }
    fn generation(&self) -> Result<[u8; 16]> {
        self.generation
            .ok_or_else(|| invalid("alarm generation is not bound"))
    }

    pub fn set_time(&mut self, now: i64) -> Result<()> {
        if now < 0 || now < self.now {
            return Err(invalid("alarm time moved backwards"));
        }
        self.now = now;
        Ok(())
    }
    fn conditions(&self, row: &Row) -> Result<Option<(bool, bool)>> {
        if row.values.len() != self.input.fields.len() {
            return Err(invalid("alarm row arity"));
        }
        for (value, field) in row.values.iter().zip(&self.input.fields) {
            if (value.is_null() && !field.nullable)
                || (!value.is_null() && !value.matches_type(&field.data_type))
            {
                return Err(invalid("alarm row/schema mismatch"));
            }
            if matches!(value, Scalar::Float64(v) if !v.is_finite()) {
                return Err(invalid("alarm retained row contains nonfinite value"));
            }
        }
        if self.keys.iter().any(|&i| row.values[i].is_null()) {
            return Err(invalid("alarm null key"));
        }
        match (&row.values[self.fields[0]], &row.values[self.fields[1]]) {
            (Scalar::Bool(enter), Scalar::Bool(clear)) if !(*enter && *clear) => {
                Ok(Some((*enter, *clear)))
            }
            _ if self.invalid == InvalidValuePolicy::Ignore => Ok(None),
            _ => Err(invalid(
                "alarm requires valid exclusive enter/clear conditions",
            )),
        }
    }
    fn encoded(&self, key: &[Scalar]) -> Vec<u8> {
        let mut out = Vec::new();
        for value in key {
            value.encode_key(&mut out);
            out.push(0xff);
        }
        out
    }
    fn entry(
        &self,
        row: &Row,
        key: Vec<Scalar>,
        encoded_capacity: usize,
        state: State,
    ) -> Result<Entry> {
        let bytes = row
            .resident_bytes()
            .saturating_add(key.iter().map(Scalar::resident_bytes).sum::<usize>())
            .saturating_add(encoded_capacity.saturating_mul(3))
            .saturating_add(2048);
        let lease = self.owner.acquire(CreditKind::Retention, bytes)?;
        Ok(Entry {
            row: row.detach_copy(),
            key,
            state,
            _lease: lease,
        })
    }
    fn output_row(&self, row: &Row, event: Event) -> Result<Row> {
        use std::fmt::Write;
        let mut generation = String::with_capacity(32);
        for byte in self.generation()? {
            write!(&mut generation, "{byte:02x}").expect("String write");
        }
        let mut out = row.detach_copy();
        out.values.extend([
            Scalar::utf8(event.kind.name()),
            Scalar::utf8(event.phase.name()),
            Scalar::utf8(generation),
            Scalar::UInt64(u64::from(self.operator.raw())),
            Scalar::UInt64(event.episode),
            Scalar::Int64(event.at),
            Scalar::Bool(event.notify),
        ]);
        Ok(out)
    }
    fn record(&mut self, effects: Effects) {
        self.stats.emitted_rows = self
            .stats
            .emitted_rows
            .saturating_add(u64::from(effects.event.is_some()));
        self.stats.notifications_expired = self
            .stats
            .notifications_expired
            .saturating_add(u64::from(effects.notification_expired));
        self.stats.notifications_cancelled = self
            .stats
            .notifications_cancelled
            .saturating_add(u64::from(effects.notification_cancelled));
        self.stats.notifications_deferred = self
            .stats
            .notifications_deferred
            .saturating_add(u64::from(effects.event.is_some_and(|e| !e.notify)));
    }
    fn replace(&mut self, encoded: Vec<u8>, entry: Entry) -> Result<()> {
        let deadline = entry.state.next_deadline(self.policy)?;
        if let Some(old) = self.entries.get(&encoded) {
            if let Some(at) = old.state.next_deadline(self.policy)? {
                self.timers.remove(&(at, encoded.clone()));
            }
        }
        if let Some(at) = deadline {
            self.timers.insert((at, encoded.clone()));
        }
        self.entries.insert(encoded, entry);
        Ok(())
    }

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Option<RowBatch>> {
        self.generation()?;
        self.set_time(now)?;
        if self.next_deadline().is_some_and(|at| at <= now) {
            return Err(invalid("alarm timers must be drained before input"));
        }
        let mut out = RowBatchBuilder::new(
            self.output.clone(),
            self.owner.clone(),
            CreditKind::Reservation,
            batch.num_rows().max(1),
            self.owner.budget().reservation_bytes,
        )?;
        for row in batch.rows() {
            self.stats.input_rows = self.stats.input_rows.saturating_add(1);
            let Some((enter, clear)) = self.conditions(row)? else {
                self.stats.invalid_rows = self.stats.invalid_rows.saturating_add(1);
                self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
                continue;
            };
            let _scratch = self.owner.acquire(
                CreditKind::Reservation,
                row.resident_bytes().saturating_mul(8).saturating_add(4096),
            )?;
            let key = self
                .keys
                .iter()
                .map(|&i| row.values[i].detach_copy())
                .collect::<Vec<_>>();
            let encoded = self.encoded(&key);
            let previous = self.entries.get(&encoded).map(|e| e.state);
            if previous.is_none() && self.entries.len() >= self.max {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "alarm key/timer bound; states cannot silently expire",
                ));
            }
            let (state, effects) =
                previous
                    .unwrap_or(State::new(now)?)
                    .observe(self.policy, now, enter, clear)?;
            let entry = self.entry(row, key, encoded.capacity(), state)?;
            if let Some(event) = effects.event {
                out.push(self.output_row(row, event)?)?;
            }
            self.replace(encoded, entry)?;
            self.record(effects);
            if effects.event.is_none() {
                self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
            }
        }
        if out.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(
                out.finish()?
                    .with_origin(batch.origin())
                    .with_source_operator(batch.source_operator()),
            ))
        }
    }

    pub fn take_due(&mut self, now: i64) -> Result<Option<RowBatch>> {
        self.generation()?;
        self.set_time(now)?;
        let Some((at, encoded)) = self.timers.first().filter(|(at, _)| *at <= now) else {
            return Ok(None);
        };
        let previous = self
            .entries
            .get(encoded)
            .ok_or_else(|| invalid("alarm timer without state"))?;
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            previous
                .row
                .resident_bytes()
                .saturating_mul(4)
                .saturating_add(encoded.len().saturating_mul(4))
                .saturating_add(4096),
        )?;
        let (state, effects) = previous.state.due(self.policy, now)?;
        let next = state.next_deadline(self.policy)?;
        let output = if let Some(event) = effects.event {
            let mut builder = RowBatchBuilder::new(
                self.output.clone(),
                self.owner.clone(),
                CreditKind::Reservation,
                1,
                self.owner.budget().reservation_bytes,
            )?;
            builder.push(self.output_row(&previous.row, event)?)?;
            Some(builder.finish()?)
        } else {
            None
        };
        let (at, encoded) = (*at, encoded.clone());
        self.timers.remove(&(at, encoded.clone()));
        self.entries
            .get_mut(&encoded)
            .expect("selected alarm")
            .state = state;
        if let Some(at) = next {
            self.timers.insert((at, encoded));
        }
        self.record(effects);
        Ok(output)
    }
    pub fn validate_cut(&mut self, now: i64) -> Result<()> {
        self.generation()?;
        for e in self.entries.values() {
            let (enter, clear) = self
                .conditions(&e.row)?
                .ok_or_else(|| invalid("alarm restored invalid conditions"))?;
            e.state.validate(self.policy, now, enter, clear)?;
        }
        self.set_time(now)
    }
    pub fn next_deadline(&self) -> Option<i64> {
        self.timers.first().map(|(at, _)| *at)
    }
    pub fn pending_timers(&self) -> usize {
        self.entries
            .values()
            .map(|e| {
                usize::from(e.state.deadline.is_some())
                    + usize::from(e.state.notification_since.is_some())
            })
            .sum()
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
    pub fn retention_bytes(&self) -> usize {
        self.entries.values().map(|e| e._lease.bytes()).sum()
    }
    pub fn cleanup(&mut self) {
        self.timers = BTreeSet::new();
        self.entries = BTreeMap::new();
        // A reset cannot reuse episode 1 in the old namespace. Kernel creates
        // a fresh participant for restore; embedded reuse must rotate it.
        if let Some(generation) = self.generation.take() {
            self.retired_generation = Some(generation);
        }
    }
    pub fn estimated_freeze_bytes(&self) -> usize {
        self.entries.values().fold(11usize, |n, e| {
            n.saturating_add(4 + PREFIX * 9).saturating_add(
                e.key
                    .iter()
                    .chain(&e.row.values)
                    .map(|v| v.encoded_value_len().unwrap_or(usize::MAX / 128))
                    .sum::<usize>(),
            )
        })
    }
    pub fn encode(&self, out: &mut Vec<u8>, max: usize) -> Result<()> {
        self.generation()?;
        if self.entries.len() > max || self.estimated_freeze_bytes() > MAX_IOT_FREEZE_BYTES {
            return Err(invalid("alarm freeze bound"));
        }
        let _scratch = self.owner.acquire(CreditKind::Reservation, 8192)?;
        let start = out.len();
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&3u16.to_le_bytes());
        out.push(KIND);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in self.entries.values() {
            let mut values = Vec::with_capacity(PREFIX + e.row.values.len());
            values.extend(prefix(e.state));
            values.extend(e.row.values.iter().cloned());
            crate::iot::encode_entry_parts(out, &e.key, &values, start)?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn freeze(&self) -> Result<IotFreeze> {
        self.generation()?;
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
                let mut values = prefix(e.state).to_vec();
                values.extend(e.row.values.iter().map(Scalar::detach_copy));
                IotEntry {
                    key: e.key.iter().map(Scalar::detach_copy).collect(),
                    values,
                }
            })
            .collect();
        IotFreeze::new(self.operator, KIND, entries)
    }
    pub fn restore(&mut self, freeze: &IotFreeze) -> Result<()> {
        self.generation()?;
        if freeze.operator != self.operator
            || freeze.kind != KIND
            || freeze.entries.len() > self.max
        {
            return Err(invalid("alarm restore identity/bound"));
        }
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            freeze
                .resident_bytes()
                .saturating_mul(3)
                .saturating_add(8192),
        )?;
        let mut entries = BTreeMap::new();
        let mut timers = BTreeSet::new();
        for e in &freeze.entries {
            let state = metadata(&e.values, None)?;
            let row = Row {
                values: e.values[PREFIX..].iter().map(Scalar::detach_copy).collect(),
            };
            let (enter, clear) = self
                .conditions(&row)?
                .ok_or_else(|| invalid("alarm restored invalid conditions"))?;
            state.validate_conditions(self.policy, enter, clear)?;
            if e.key.len() != self.keys.len()
                || e.key
                    .iter()
                    .zip(&self.keys)
                    .any(|(v, &i)| v != &row.values[i])
            {
                return Err(invalid("alarm restore row/key mismatch"));
            }
            let encoded = self.encoded(&e.key);
            let entry = self.entry(
                &row,
                e.key.iter().map(Scalar::detach_copy).collect(),
                encoded.capacity(),
                state,
            )?;
            if let Some(at) = state.next_deadline(self.policy)? {
                timers.insert((at, encoded.clone()));
            }
            if entries.insert(encoded, entry).is_some() {
                return Err(invalid("duplicate alarm key"));
            }
        }
        self.timers = timers;
        self.entries = entries;
        Ok(())
    }
}

//! Bounded, source-ordered resampling over a paused logical clock.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SparrowError,
};
use sparrow_plan::{InvalidValuePolicy, IotSpec, IotTimingSpec, ResampleMode, ResampleSpec};

use crate::iot::{IotEntry, IotFreeze, IotStats, MAX_IOT_FREEZE_BYTES, MAX_IOT_FREEZE_ENTRIES};

pub(crate) const PREFIX: usize = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResampleStats {
    /// Invalid ignored vectors and samples superseded before contributing.
    /// Mean's many-to-one reduction is not an input discard.
    pub discarded_inputs: u64,
    pub missing_outputs: u64,
    pub interpolated_outputs: u64,
}

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

fn numeric(value: &Scalar) -> Option<f64> {
    let value = match value {
        Scalar::Int64(value) => *value as f64,
        Scalar::UInt64(value) => *value as f64,
        Scalar::Float64(value) => *value,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

// A difference of opposite-sign extreme numbers may overflow even when the
// convex result is finite. Same-sign subtraction is bounded; opposite signs
// use weighted terms whose sum cannot overflow in that direction.
fn blend(left: f64, right: f64, weight: f64) -> Result<f64> {
    if !left.is_finite() || !right.is_finite() || !(0.0..=1.0).contains(&weight) {
        return Err(invalid("resample numeric operands are not finite/bounded"));
    }
    let value = if weight == 0.0 {
        left
    } else if weight == 1.0 {
        right
    } else if left.is_sign_negative() == right.is_sign_negative() {
        left + (right - left) * weight
    } else {
        left * (1.0 - weight) + right * weight
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(invalid("resample numeric result is not finite"))
    }
}

fn add_time(at: i64, duration: i64) -> Result<i64> {
    at.checked_add(duration)
        .ok_or_else(|| invalid("resample deadline overflow"))
}

/// Schema-independent checks also run in the non-materializing snapshot scan.
/// NULL is only the complete empty last/mean accumulator, never a partial point.
pub(crate) fn metadata(
    values: &[Scalar],
    kind: u8,
    now: Option<i64>,
) -> Result<(i64, Option<i64>, Option<i64>, u64, bool)> {
    let [Scalar::Int64(next), Scalar::Int64(pending), Scalar::Int64(at), Scalar::UInt64(samples), Scalar::Bool(used), data @ ..] =
        values
    else {
        return Err(invalid("resample freeze metadata types"));
    };
    if !(13..=15).contains(&kind)
        || !(1..=16).contains(&data.len())
        || *next <= 0
        || *pending < -1
        || *at < -1
        || now.is_some_and(|cut| cut < 0 || *next <= cut || *at > cut || *pending > cut)
    {
        return Err(invalid("resample metadata exceeds its processing-time cut"));
    }
    if kind == 15 {
        if *samples != 1
            || *at < 0
            || *at >= *next
            || (*pending != -1 && (*pending <= *at || *pending >= *next))
        {
            return Err(invalid("resample interpolation metadata is inconsistent"));
        }
    } else if *pending != -1 || *used || (*samples == 0) != (*at == -1) || *at >= *next {
        return Err(invalid("resample interval metadata is inconsistent"));
    }
    for value in data {
        if *samples == 0 {
            if !value.is_null() {
                return Err(invalid("resample empty interval retains a value"));
            }
        } else if numeric(value).is_none() || (kind != 13 && !matches!(value, Scalar::Float64(_))) {
            return Err(invalid(
                "resample frozen vector is not complete finite numeric data",
            ));
        }
    }
    Ok((
        *next,
        (*pending != -1).then_some(*pending),
        (*at != -1).then_some(*at),
        *samples,
        *used,
    ))
}

/// Only fixed-width tags are accepted before decoding. The header-only path
/// uses bounded stack storage, not an attacker-selected scalar allocation.
pub(crate) fn scan_values(
    src: &mut &[u8],
    len: usize,
    kind: u8,
    now: Option<i64>,
    materialize: bool,
) -> Result<Vec<Scalar>> {
    if !(PREFIX + 1..=PREFIX + 16).contains(&len) {
        return Err(invalid("resample freeze vector width"));
    }
    let mut values = std::array::from_fn::<_, 21, _>(|_| Scalar::Null);
    for (index, value) in values[..len].iter_mut().enumerate() {
        let tag = *src
            .first()
            .ok_or_else(|| invalid("truncated resample scalar"))?;
        let valid = match index {
            0..=2 => tag == 2,
            3 => tag == 3,
            4 => tag == 1,
            _ => matches!(tag, 0 | 2 | 3 | 4),
        };
        if !valid {
            return Err(invalid("resample freeze scalar tag"));
        }
        // The general Scalar decoder allocates temporary byte Vecs and
        // accepts any non-zero Bool. This strict fixed-width profile does
        // neither, including in the allocation-free admission scan.
        *value = match tag {
            0 => {
                *src = &src[1..];
                Scalar::Null
            }
            1 => {
                if src.len() < 2 || src[1] > 1 {
                    return Err(invalid("resample noncanonical Bool"));
                }
                let value = Scalar::Bool(src[1] == 1);
                *src = &src[2..];
                value
            }
            2..=4 => {
                if src.len() < 9 {
                    return Err(invalid("truncated resample number"));
                }
                let bytes: [u8; 8] = src[1..9].try_into().unwrap();
                *src = &src[9..];
                match tag {
                    2 => Scalar::Int64(i64::from_le_bytes(bytes)),
                    3 => Scalar::UInt64(u64::from_le_bytes(bytes)),
                    _ => Scalar::Float64(f64::from_le_bytes(bytes)),
                }
            }
            _ => unreachable!("fixed-width tag already checked"),
        };
    }
    metadata(&values[..len], kind, now)?;
    Ok(if materialize {
        values[..len].to_vec()
    } else {
        Vec::new()
    })
}

struct Entry {
    key: Vec<Scalar>,
    next_grid: i64,
    pending: Option<i64>,
    sample_time: Option<i64>,
    samples: u64,
    used: bool,
    values: Vec<Scalar>,
    // Key, value and timer allocations must drop before their accounting.
    _lease: MemoryLease,
}

impl Entry {
    fn deadline(&self, config: &ResampleSpec) -> Result<i64> {
        self.pending
            .map(|grid| {
                add_time(grid, config.max_wait_micros).map(|expiry| expiry.min(self.next_grid))
            })
            .unwrap_or(Ok(self.next_grid))
    }

    fn prefix(&self) -> [Scalar; PREFIX] {
        [
            Scalar::Int64(self.next_grid),
            Scalar::Int64(self.pending.unwrap_or(-1)),
            Scalar::Int64(self.sample_time.unwrap_or(-1)),
            Scalar::UInt64(self.samples),
            Scalar::Bool(self.used),
        ]
    }
}

fn entry_bytes(key: &[Scalar], fields: usize) -> usize {
    key.iter()
        .map(Scalar::resident_bytes)
        .sum::<usize>()
        .saturating_mul(3)
        .saturating_add(fields.saturating_mul(std::mem::size_of::<Scalar>() + 32))
        .saturating_add(2048)
}

/// Additional state is cold/boxed; legacy timed machines keep their layout.
pub(crate) struct ResampleIot {
    operator: OperatorId,
    input: Arc<Schema>,
    output: Arc<Schema>,
    owner: Arc<MemoryOwner>,
    keys: Vec<usize>,
    fields: Vec<usize>,
    config: ResampleSpec,
    invalid: InvalidValuePolicy,
    max: usize,
    now: i64,
    remaining: usize,
    generation: Option<[u8; 16]>,
    retired_generation: Option<[u8; 16]>,
    // Timer key copies are charged to entries. Drop them before the entries.
    timers: BTreeSet<(i64, Vec<u8>)>,
    entries: BTreeMap<Vec<u8>, Entry>,
    stats: IotStats,
    resample_stats: ResampleStats,
    reported_stats: ResampleStats,
    _metadata: MemoryLease,
}

impl ResampleIot {
    #[cold]
    pub fn new(
        operator: OperatorId,
        spec: &IotSpec,
        input: Arc<Schema>,
        owner: Arc<MemoryOwner>,
        keys: Vec<usize>,
        fields: Vec<usize>,
    ) -> Result<Self> {
        spec.validate(&input)?;
        let Some(IotTimingSpec::Resample(config)) = &spec.timing else {
            return Err(invalid("resample requires its independent configuration"));
        };
        let max = spec
            .max_keys
            .min(owner.budget().max_state_keys)
            .min(owner.budget().max_timers)
            .min(MAX_IOT_FREEZE_ENTRIES);
        if max == 0 {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "resample needs a state and timer slot",
            ));
        }
        let metadata = owner.acquire(
            CreditKind::Reservation,
            std::mem::size_of::<Self>()
                + 8192
                + input
                    .fields
                    .iter()
                    .map(|f| f.name.len().saturating_mul(2) + 256)
                    .sum::<usize>(),
        )?;
        let output = Arc::new(spec.output_schema(&input)?);
        Ok(Self {
            operator,
            input,
            output,
            owner,
            keys,
            fields,
            config: (**config).clone(),
            invalid: spec.invalid,
            max,
            now: 0,
            remaining: config.max_emissions_per_decision,
            generation: None,
            retired_generation: None,
            timers: BTreeSet::new(),
            entries: BTreeMap::new(),
            stats: IotStats::default(),
            resample_stats: ResampleStats::default(),
            reported_stats: ResampleStats::default(),
            _metadata: metadata,
        })
    }

    pub fn bind_generation(&mut self, generation: [u8; 16]) -> Result<()> {
        if generation == [0; 16]
            || self.retired_generation == Some(generation)
            || self.generation.is_some_and(|old| old != generation)
        {
            return Err(invalid(
                "resample requires a new initialized generation before input/restore",
            ));
        }
        self.generation = Some(generation);
        Ok(())
    }

    fn generation(&self) -> Result<[u8; 16]> {
        self.generation
            .ok_or_else(|| invalid("resample generation is not bound"))
    }

    fn output_row(
        &self,
        entry: &Entry,
        values: Vec<Scalar>,
        grid: i64,
        samples: u64,
    ) -> Result<RowBatch> {
        use std::fmt::Write;
        if self.remaining == 0 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "resample decision emission cap",
            ));
        }
        let mut generation = String::with_capacity(32);
        for byte in self.generation()? {
            write!(&mut generation, "{byte:02x}").expect("String write");
        }
        let missing = samples == 0;
        let mut row = Vec::with_capacity(self.keys.len() + self.fields.len() + 7);
        row.extend(entry.key.iter().map(Scalar::detach_copy));
        row.extend(values);
        row.extend([
            Scalar::utf8(self.config.mode.as_str()),
            Scalar::Int64(grid),
            Scalar::Int64(self.now),
            Scalar::Bool(missing),
            Scalar::UInt64(samples),
            Scalar::utf8(generation),
            Scalar::UInt64(u64::from(self.operator.raw())),
        ]);
        let mut out = RowBatchBuilder::new(
            self.output.clone(),
            self.owner.clone(),
            CreditKind::Reservation,
            1,
            self.owner.budget().reservation_bytes,
        )?;
        out.push(Row { values: row })?;
        out.finish()
    }

    fn emitted(&mut self, missing: bool, interpolated: bool) {
        self.remaining -= 1;
        self.stats.emitted_rows = self.stats.emitted_rows.saturating_add(1);
        self.resample_stats.missing_outputs = self
            .resample_stats
            .missing_outputs
            .saturating_add(u64::from(missing));
        self.resample_stats.interpolated_outputs = self
            .resample_stats
            .interpolated_outputs
            .saturating_add(u64::from(interpolated));
    }

    fn discarded(&mut self, invalid: bool) {
        self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
        self.stats.invalid_rows = self.stats.invalid_rows.saturating_add(u64::from(invalid));
        self.resample_stats.discarded_inputs =
            self.resample_stats.discarded_inputs.saturating_add(1);
    }

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Option<RowBatch>> {
        self.generation()?;
        if now != self.now || self.next_deadline().is_some_and(|deadline| deadline <= now) {
            return Err(invalid(
                "resample requires the current decision's timers drained before input",
            ));
        }
        // The durable paused protocol authenticates one input row per decision.
        // Do not silently pretend a wider embedded batch shares that contract.
        if batch.num_rows() != 1 {
            return Err(invalid(
                "resample requires one input row per ordered decision",
            ));
        }
        let row = &batch.rows()[0];
        self.stats.input_rows = self.stats.input_rows.saturating_add(1);
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            row.resident_bytes().saturating_mul(4).saturating_add(8192),
        )?;
        let mut key = Vec::with_capacity(self.keys.len());
        for &index in &self.keys {
            let value = row
                .values
                .get(index)
                .ok_or_else(|| invalid("resample input/key width mismatch"))?;
            if value.is_null() || !value.matches_type(&self.input.fields[index].data_type) {
                return Err(invalid("resample key must be a valid non-null scalar"));
            }
            key.push(value.detach_copy());
        }
        if self
            .fields
            .iter()
            .any(|&index| row.values.get(index).and_then(numeric).is_none())
        {
            if self.invalid == InvalidValuePolicy::Ignore {
                self.discarded(true);
                return Ok(None);
            }
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "resample requires one complete finite numeric value vector",
            ));
        }
        let values = self
            .fields
            .iter()
            .map(|&index| {
                if self.config.mode == ResampleMode::Last {
                    row.values[index].detach_copy()
                } else {
                    Scalar::Float64(numeric(&row.values[index]).expect("validated numeric vector"))
                }
            })
            .collect::<Vec<_>>();
        let encoded = crate::iot::encoded_key(&key)?;
        if !self.entries.contains_key(&encoded) {
            if self.entries.len() >= self.max {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "resample key bound; known keys cannot silently expire",
                ));
            }
            let remainder = now % self.config.period_micros;
            let next_grid = add_time(now, self.config.period_micros - remainder)?;
            let exact = self.config.mode == ResampleMode::Interpolate && remainder == 0;
            let lease = self
                .owner
                .acquire(CreditKind::Retention, entry_bytes(&key, self.fields.len()))?;
            let entry = Entry {
                key,
                next_grid,
                pending: None,
                sample_time: Some(now),
                samples: 1,
                used: exact,
                values,
                _lease: lease,
            };
            let output = if exact {
                Some(self.output_row(&entry, entry.values.clone(), now, 1)?)
            } else {
                None
            };
            self.timers.insert((next_grid, encoded.clone()));
            self.entries.insert(encoded, entry);
            if exact {
                self.emitted(false, false);
            }
            return Ok(output);
        }
        let entry = self.entries.get(&encoded).expect("existing key");
        let old_deadline = entry.deadline(&self.config)?;
        if !self.timers.contains(&(old_deadline, encoded.clone())) {
            return Err(invalid("resample timer index differs from its state"));
        }
        let mut samples = 1;
        let mut used = false;
        let mut discarded = false;
        let mut missing = false;
        let mut interpolated = false;
        let mut values = values;
        let output = match self.config.mode {
            ResampleMode::Last => {
                samples = entry
                    .samples
                    .checked_add(1)
                    .ok_or_else(|| invalid("resample sample count exhausted"))?;
                discarded = entry.samples > 0;
                None
            }
            ResampleMode::Mean => {
                samples = entry
                    .samples
                    .checked_add(1)
                    .ok_or_else(|| invalid("resample sample count exhausted"))?;
                if entry.samples > 0 {
                    for (value, previous) in values.iter_mut().zip(&entry.values) {
                        let left = numeric(previous)
                            .ok_or_else(|| invalid("resample mean state is not numeric"))?;
                        *value = Scalar::Float64(blend(
                            left,
                            numeric(value).expect("new numeric vector"),
                            1.0 / samples as f64,
                        )?);
                    }
                }
                None
            }
            ResampleMode::Interpolate => {
                let mut left_used = false;
                let output = if let Some(grid) = entry.pending {
                    if grid > now || add_time(grid, self.config.max_wait_micros)? <= now {
                        return Err(invalid(
                            "resample interpolation input is outside its live wait",
                        ));
                    }
                    let at = entry
                        .sample_time
                        .ok_or_else(|| invalid("resample interpolation lacks a left point"))?;
                    let (sampled, count) = if now == grid {
                        used = true;
                        (values.clone(), 1)
                    } else if at < grid && now - at <= self.config.max_gap_micros {
                        let weight = (grid - at) as f64 / (now - at) as f64;
                        let sampled = entry
                            .values
                            .iter()
                            .zip(&values)
                            .map(|(left, right)| {
                                blend(
                                    numeric(left).ok_or_else(|| {
                                        invalid("resample left point is not numeric")
                                    })?,
                                    numeric(right).expect("new numeric vector"),
                                    weight,
                                )
                                .map(Scalar::Float64)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        used = true;
                        left_used = true;
                        interpolated = true;
                        (sampled, 2)
                    } else {
                        missing = true;
                        (vec![Scalar::Null; self.fields.len()], 0)
                    };
                    Some(self.output_row(entry, sampled, grid, count)?)
                } else {
                    None
                };
                discarded = !entry.used && !left_used;
                output
            }
        };
        let new_deadline = entry.next_grid;
        // Output is already admitted and all arithmetic checked. Selected
        // values are fixed-width numeric scalars, so the retained lease size
        // does not grow when replacing a complete vector.
        if old_deadline != new_deadline {
            self.timers.remove(&(old_deadline, encoded.clone()));
            self.timers.insert((new_deadline, encoded.clone()));
        }
        let entry = self.entries.get_mut(&encoded).expect("existing key");
        entry.pending = None;
        entry.sample_time = Some(now);
        entry.samples = samples;
        entry.used = used;
        entry.values = values;
        if discarded {
            self.discarded(false);
        }
        if output.is_some() {
            self.emitted(missing, interpolated);
        }
        Ok(output)
    }

    /// Bound timer output before advancing the clock or touching any key.
    pub fn set_time(&mut self, now: i64) -> Result<()> {
        if now < 0 || now < self.now {
            return Err(invalid("resample time moved backwards"));
        }
        if self.next_deadline().is_some_and(|at| at <= self.now) {
            return Err(invalid(
                "resample previous decision still has undrained timers",
            ));
        }
        let mut outputs = 0u64;
        for (deadline, encoded) in &self.timers {
            if *deadline > now {
                break;
            }
            let entry = self
                .entries
                .get(encoded)
                .ok_or_else(|| invalid("resample timer without state"))?;
            let due_grids = if entry.next_grid <= now {
                ((now - entry.next_grid) / self.config.period_micros) as u64 + 1
            } else {
                0
            };
            if due_grids > 0 {
                let shift = i64::try_from(due_grids)
                    .ok()
                    .and_then(|n| n.checked_mul(self.config.period_micros))
                    .ok_or_else(|| invalid("resample grid advance overflow"))?;
                let next = add_time(entry.next_grid, shift)?;
                if self.config.mode == ResampleMode::Interpolate {
                    add_time(
                        next - self.config.period_micros,
                        self.config.max_wait_micros,
                    )?;
                }
            }
            let due = if self.config.mode == ResampleMode::Interpolate {
                let expired = entry
                    .pending
                    .map(|at| add_time(at, self.config.max_wait_micros))
                    .transpose()?
                    .is_some_and(|expiry| expiry <= now);
                let last_expired_grid = now - self.config.max_wait_micros;
                u64::from(expired)
                    + if entry.next_grid <= last_expired_grid {
                        ((last_expired_grid - entry.next_grid) / self.config.period_micros) as u64
                            + 1
                    } else {
                        0
                    }
            } else {
                due_grids
            };
            outputs = outputs
                .checked_add(due)
                .ok_or_else(|| invalid("resample expansion overflow"))?;
            // Reserve before any timer is published: the following input may
            // resolve one pending grid (or introduce an exact new key).
            let timer_cap = self.config.max_emissions_per_decision
                - usize::from(self.config.mode == ResampleMode::Interpolate);
            if outputs > timer_cap as u64 {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "resample catch-up exceeds the per-decision emission cap",
                ));
            }
        }
        self.now = now;
        self.remaining = self.config.max_emissions_per_decision;
        Ok(())
    }

    /// One timer step; the kernel can yield between keys and expired grids.
    /// Output admission always precedes any mutation of the selected entry.
    pub fn take_due(&mut self, now: i64) -> Result<Option<RowBatch>> {
        self.generation()?;
        if now != self.now {
            return Err(invalid("resample drain must use the current decision time"));
        }
        let Some((deadline, encoded)) = self.timers.first() else {
            return Ok(None);
        };
        let deadline = *deadline;
        if deadline > now {
            return Ok(None);
        }
        let entry = self
            .entries
            .get(encoded)
            .ok_or_else(|| invalid("resample timer without state"))?;
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            entry_bytes(&entry.key, self.fields.len())
                .saturating_mul(3)
                .saturating_add(4096),
        )?;
        let encoded = encoded.clone();
        let mut next_grid = entry.next_grid;
        let mut pending = entry.pending;
        let mut clear_values = false;
        let output = if self.config.mode != ResampleMode::Interpolate {
            next_grid = add_time(next_grid, self.config.period_micros)?;
            let values = entry.values.iter().map(Scalar::detach_copy).collect();
            clear_values = true;
            Some(self.output_row(entry, values, entry.next_grid, entry.samples)?)
        } else if pending
            .map(|grid| add_time(grid, self.config.max_wait_micros))
            .transpose()?
            .is_some_and(|expiry| expiry <= now && expiry <= entry.next_grid)
        {
            let grid = pending.take().expect("checked pending expiry");
            Some(self.output_row(entry, vec![Scalar::Null; self.fields.len()], grid, 0)?)
        } else {
            if pending.is_some() {
                return Err(invalid("resample has overlapping interpolation waits"));
            }
            pending = Some(next_grid);
            add_time(next_grid, self.config.max_wait_micros)?;
            next_grid = add_time(next_grid, self.config.period_micros)?;
            None
        };
        let next_deadline = pending
            .map(|grid| {
                add_time(grid, self.config.max_wait_micros).map(|expiry| expiry.min(next_grid))
            })
            .transpose()?
            .unwrap_or(next_grid);
        let missing = output
            .as_ref()
            .is_some_and(|_| self.config.mode == ResampleMode::Interpolate || entry.samples == 0);
        self.timers.remove(&(deadline, encoded.clone()));
        let entry = self.entries.get_mut(&encoded).expect("checked entry");
        entry.next_grid = next_grid;
        entry.pending = pending;
        if clear_values {
            entry.values.fill(Scalar::Null);
            entry.samples = 0;
            entry.sample_time = None;
            entry.used = false;
        }
        self.timers.insert((next_deadline, encoded));
        if output.is_some() {
            self.emitted(missing, false);
        }
        Ok(output)
    }

    pub fn next_deadline(&self) -> Option<i64> {
        self.timers.first().map(|(at, _)| *at)
    }
    pub fn pending_timers(&self) -> usize {
        self.timers.len()
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
    pub fn resample_stats(&self) -> ResampleStats {
        self.resample_stats
    }
    pub fn report_metrics(&mut self, metrics: &crate::metrics::RuntimeMetrics) {
        use std::sync::atomic::Ordering;
        let current = self.resample_stats;
        for (counter, value, previous) in [
            (
                &metrics.resample.discarded_inputs,
                current.discarded_inputs,
                self.reported_stats.discarded_inputs,
            ),
            (
                &metrics.resample.missing_outputs,
                current.missing_outputs,
                self.reported_stats.missing_outputs,
            ),
            (
                &metrics.resample.interpolated_outputs,
                current.interpolated_outputs,
                self.reported_stats.interpolated_outputs,
            ),
        ] {
            counter.fetch_add(value.saturating_sub(previous), Ordering::Relaxed);
        }
        self.reported_stats = current;
    }
    pub fn retention_bytes(&self) -> usize {
        self.entries
            .values()
            .map(|entry| entry._lease.bytes())
            .sum()
    }

    fn validate_entry(&self, entry: &Entry, cut: Option<i64>) -> Result<()> {
        if entry.values.len() > 16 {
            return Err(invalid("resample vector width"));
        }
        let mut frozen = std::array::from_fn::<_, 21, _>(|_| Scalar::Null);
        frozen[..PREFIX].clone_from_slice(&entry.prefix());
        frozen[PREFIX..PREFIX + entry.values.len()].clone_from_slice(&entry.values);
        metadata(
            &frozen[..PREFIX + entry.values.len()],
            self.config.mode.state_kind(),
            cut,
        )?;
        let period = self.config.period_micros;
        if entry.key.len() != self.keys.len()
            || entry.values.len() != self.fields.len()
            || entry.next_grid % period != 0
            || entry
                .pending
                .is_some_and(|grid| grid % period != 0 || grid != entry.next_grid - period)
            || (self.config.mode != ResampleMode::Interpolate
                && entry
                    .sample_time
                    .is_some_and(|at| at < entry.next_grid - period))
            || entry.key.iter().zip(&self.keys).any(|(value, &index)| {
                value.is_null() || !value.matches_type(&self.input.fields[index].data_type)
            })
            || (self.config.mode == ResampleMode::Last
                && entry.samples > 0
                && entry
                    .values
                    .iter()
                    .zip(&self.fields)
                    .any(|(value, &index)| {
                        !value.matches_type(&self.input.fields[index].data_type)
                    }))
        {
            return Err(invalid("resample restored schema/grid mismatch"));
        }
        if cut.is_some_and(|now| entry.deadline(&self.config).is_ok_and(|at| at <= now)) {
            return Err(invalid("resample restored timer is already due"));
        }
        entry.deadline(&self.config)?;
        Ok(())
    }

    pub fn validate_cut(&mut self, now: i64) -> Result<()> {
        self.generation()?;
        if now < 0 || now < self.now {
            return Err(invalid("resample invalid restored clock"));
        }
        for entry in self.entries.values() {
            self.validate_entry(entry, Some(now))?;
        }
        // Validation never invents an elapsed interval or drains a restored timer.
        self.now = now;
        self.remaining = self.config.max_emissions_per_decision;
        Ok(())
    }

    pub fn estimated_freeze_bytes(&self) -> usize {
        self.entries.values().fold(11usize, |n, entry| {
            n.saturating_add(4 + 38).saturating_add(
                entry
                    .key
                    .iter()
                    .chain(&entry.values)
                    .map(|v| v.encoded_value_len().unwrap_or(MAX_IOT_FREEZE_BYTES))
                    .sum::<usize>(),
            )
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>, max: usize) -> Result<()> {
        self.generation()?;
        if self.entries.len() > max || self.estimated_freeze_bytes() > MAX_IOT_FREEZE_BYTES {
            return Err(invalid("resample freeze bound"));
        }
        let _scratch = self.owner.acquire(CreditKind::Reservation, 8192)?;
        for entry in self.entries.values() {
            self.validate_entry(entry, Some(self.now))?;
        }
        let start = out.len();
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&3u16.to_le_bytes());
        out.push(self.config.mode.state_kind());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in self.entries.values() {
            let mut values = entry.prefix().to_vec();
            values.extend(entry.values.iter().cloned());
            crate::iot::encode_entry_parts(out, &entry.key, &values, start)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn freeze(&self) -> Result<IotFreeze> {
        let mut bytes = Vec::new();
        self.encode(&mut bytes, self.max)?;
        IotFreeze::decode(&bytes, self.max)
    }

    pub fn restore(&mut self, freeze: &IotFreeze) -> Result<()> {
        self.generation()?;
        if freeze.operator != self.operator
            || freeze.slot != crate::iot::IOT_STATE_SLOT
            || freeze.kind != self.config.mode.state_kind()
            || freeze.entries.len() > self.max
        {
            return Err(invalid("resample restore identity/bound mismatch"));
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
        for IotEntry { key, values } in &freeze.entries {
            let (next_grid, pending, sample_time, samples, used) =
                metadata(values, freeze.kind, None)?;
            let lease = self
                .owner
                .acquire(CreditKind::Retention, entry_bytes(key, self.fields.len()))?;
            let entry = Entry {
                key: key.iter().map(Scalar::detach_copy).collect(),
                next_grid,
                pending,
                sample_time,
                samples,
                used,
                values: values[PREFIX..].iter().map(Scalar::detach_copy).collect(),
                _lease: lease,
            };
            self.validate_entry(&entry, None)?;
            let encoded = crate::iot::encoded_key(&entry.key)?;
            timers.insert((entry.deadline(&self.config)?, encoded.clone()));
            if entries.insert(encoded, entry).is_some() {
                return Err(invalid("duplicate resample key"));
            }
        }
        // Publish only after the whole replacement and all its credits exist.
        self.timers = timers;
        self.entries = entries;
        Ok(())
    }

    pub fn cleanup(&mut self) {
        self.timers = BTreeSet::new();
        self.entries = BTreeMap::new();
        if let Some(generation) = self.generation.take() {
            self.retired_generation = Some(generation);
        }
        self.now = 0;
        self.remaining = self.config.max_emissions_per_decision;
    }
}

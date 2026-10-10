//! Two-input ET joins with fixed row/key/byte/fan-out limits.
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result, Row, RowBatch, Scalar, Schema,
    SparrowError,
};
use sparrow_plan::{JoinMode, StreamJoinSpec};
use std::collections::BTreeMap;
use std::sync::Arc;

struct Record {
    row: Row,
    key: Vec<u8>,
    time: i64,
    expires: i64,
    ordinal: u64,
    matched: bool,
    matchable: bool,
    credit: MemoryLease,
}
struct KeyCount {
    count: usize,
    credit: MemoryLease,
}
pub(crate) struct Pending {
    record: Record,
    new_key: Option<MemoryLease>,
}
pub(crate) struct BoundedJoin {
    rows: [BTreeMap<u64, Record>; 2],
    keys: BTreeMap<Vec<u8>, KeyCount>,
    pub(crate) spec: StreamJoinSpec,
    schemas: [Schema; 2],
    output: Schema,
    key_indices: [Vec<usize>; 2],
    time_indices: [usize; 2],
    sequence: [u64; 2],
    watermarks: [Option<i64>; 2],
    owner: Arc<MemoryOwner>,
    max_keys: usize,
    bytes: usize,
    durable: bool,
}
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}
fn bound(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::BoundExceeded, s)
}
impl BoundedJoin {
    pub(crate) fn new(
        spec: StreamJoinSpec,
        left: Schema,
        right: Schema,
        owner: Arc<MemoryOwner>,
        max_keys: usize,
    ) -> Result<Self> {
        let plan = sparrow_plan::AnalysisPlan::join(spec.clone(), left.clone(), right.clone())?;
        let key_indices = [
            spec.left_keys
                .iter()
                .map(|k| left.index_of_name(k).unwrap())
                .collect(),
            spec.right_keys
                .iter()
                .map(|k| right.index_of_name(k).unwrap())
                .collect(),
        ];
        let time_indices = [
            left.index_of_name(&spec.left_time).unwrap(),
            right.index_of_name(&spec.right_time).unwrap(),
        ];
        Ok(Self {
            rows: std::array::from_fn(|_| BTreeMap::new()),
            keys: BTreeMap::new(),
            spec,
            schemas: [left, right],
            output: plan.output().clone(),
            key_indices,
            time_indices,
            sequence: [0, 0],
            watermarks: [None, None],
            max_keys: max_keys.min(owner.budget().max_state_keys),
            owner,
            bytes: 0,
            durable: false,
        })
    }
    pub(crate) fn state(&self) -> (usize, usize) {
        (self.keys.len(), self.bytes)
    }
    pub(crate) fn set_durable(&mut self) { self.durable = true; }
    pub(crate) fn encode_state(
        &self, operator: sparrow_model::OperatorId, inputs: &[crate::graph_cut::Progress; 2],
        emitted: &crate::graph_cut::Progress, out: &mut Vec<u8>, limit: usize,
    ) -> Result<()> {
        if !self.durable { return Err(crate::analysis_state::mismatch("fresh Join cannot encode recovery state")); }
        crate::analysis_state::header(out, operator, 11, self.rows.iter().map(BTreeMap::len).sum(), limit)?;
        for n in self.sequence { out.extend_from_slice(&n.to_le_bytes()); }
        for p in inputs { crate::analysis_state::put_progress(out, p); }
        crate::analysis_state::put_progress(out, emitted);
        for rows in &self.rows {
            out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for record in rows.values() {
                out.extend_from_slice(&record.ordinal.to_le_bytes());
                out.push(u8::from(record.matched));
                out.extend_from_slice(&(record.row.values.len() as u16).to_le_bytes());
                for v in &record.row.values { v.encode_value(out)?; }
            }
        }
        Ok(())
    }
    pub(crate) fn restore_state(&mut self, freeze: crate::analysis_state::AnalysisFreeze)
        -> Result<([crate::graph_cut::Progress; 2], crate::graph_cut::Progress)> {
        use crate::analysis_state::{AnalysisData, mismatch};
        if !self.durable || self.rows.iter().any(|rows| !rows.is_empty()) {
            return Err(mismatch("Join restore requires a fresh durable operator"));
        }
        let AnalysisData::Join { sequences, inputs, emitted, rows } = freeze.data else {
            return Err(mismatch("Join received an UNNEST frame"));
        };
        for (side, rows) in rows.into_iter().enumerate() {
            for saved in rows {
                if saved.row.values.len() != self.schemas[side].fields.len()
                    || saved.row.values.iter().zip(&self.schemas[side].fields).any(|(v, f)| {
                        if v.is_null() { !f.nullable } else { !v.matches_type(&f.data_type) }
                    }) {
                    return Err(mismatch("Join restored row schema mismatch"));
                }
                self.sequence[side] = saved.ordinal - 1;
                let mut pending = self.prepare(side, &saved.row, i64::MAX)?
                    .ok_or_else(|| mismatch("Join restored row unexpectedly rejected"))?;
                pending.record.matched = saved.matched;
                if saved.matched && !pending.record.matchable {
                    return Err(mismatch("unmatchable Join key marked matched"));
                }
                self.insert(side, pending);
            }
        }
        self.sequence = sequences;
        for (side, p) in inputs.iter().enumerate() {
            if let Some(wm) = if p.eof { Some(i64::MAX) } else { p.watermark } { self.advance(side, wm)?; }
        }
        if self.expired().is_some() || emitted.watermark != self.progress() {
            return Err(mismatch("Join state/expiry/output watermark disagrees with committed cut"));
        }
        Ok((inputs, emitted))
    }
    pub(crate) fn work(&self) -> u64 {
        ((self.rows[0].len() + self.rows[1].len() + 1) * (self.output.fields.len() + 4)) as u64
    }
    pub(crate) fn advance(&mut self, side: usize, time: i64) -> Result<()> {
        if time < 0 {
            return Err(invalid("negative join watermark"));
        }
        self.watermarks[side] = Some(self.watermarks[side].map_or(time, |old| old.max(time)));
        Ok(())
    }
    pub(crate) fn progress(&self) -> Option<i64> {
        let mut progress = self.watermarks[0]?.min(self.watermarks[1]?);
        if self.spec.mode == JoinMode::Left {
            for record in self.rows[0].values().filter(|r| !r.matched) {
                progress = progress.min(record.time);
            }
        }
        Some(progress)
    }
    pub(crate) fn prepare(&self, side: usize, row: &Row, now: i64) -> Result<Option<Pending>> {
        if row.values.len() != self.schemas[side].fields.len() {
            return Err(invalid("join row/schema mismatch"));
        }
        let time = row.values[self.time_indices[side]]
            .as_event_time_micros()
            .filter(|t| *t >= 0)
            .ok_or_else(|| invalid("invalid join timestamp"))?;
        if time > now.saturating_add(sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS) {
            return Ok(None);
        }
        if self.watermarks[side].is_some_and(|wm| time < wm) {
            return Err(invalid(
                "join input is late relative to its own watermark; v1 late policy is error",
            ));
        }
        if self.rows[side].len() >= self.spec.max_rows_per_side {
            return Err(bound("join side row limit exceeded"));
        }
        if self.durable && self.rows.iter().map(BTreeMap::len).sum::<usize>() >= self.max_keys {
            return Err(bound("durable Join total retained rows exceed max_state_keys"));
        }
        let ordinal = self.sequence[side]
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| bound("join ordinal exhausted"))?;
        let expires = if let Some(size) = self.spec.window_size_micros {
            (time / size * size).checked_add(size)
        } else {
            time.checked_add(if side == 0 {
                self.spec.after_micros.unwrap()
            } else {
                self.spec.before_micros.unwrap()
            })
            .and_then(|t| t.checked_add(1))
        }
        .ok_or_else(|| SparrowError::new(ErrorCode::IntegerOverflow, "join close time overflow"))?;
        // Includes detach/key copies and singleton B-tree slack; acquired BEFORE allocation.
        let credit = self.owner.acquire(
            CreditKind::Retention,
            if self.durable { crate::analysis_state::row_credit(&row.values) }
            else { row.resident_bytes().saturating_mul(4).saturating_add(2048) },
        )?;
        let mut key = Vec::new();
        let mut matchable = true;
        for index in &self.key_indices[side] {
            let v = &row.values[*index];
            matchable &= !v.is_null() && !v.is_nan();
            v.encode_key(&mut key);
            key.push(0xff);
        }
        let new_key = if self.keys.contains_key(&key) {
            None
        } else {
            if self.keys.len() >= self.max_keys {
                return Err(bound("join distinct key limit exceeded"));
            }
            Some(self.owner.acquire(
                CreditKind::Retention,
                if self.durable { 1 } else { key.len().saturating_mul(2).saturating_add(2048) },
            )?)
        };
        Ok(Some(Pending {
            record: Record {
                row: row.detach_copy(),
                key,
                time,
                expires,
                ordinal,
                matched: false,
                matchable,
                credit,
            },
            new_key,
        }))
    }
    fn matches(&self, side: usize, a: &Record, b: &Record) -> bool {
        if !a.matchable || !b.matchable || a.key != b.key {
            return false;
        }
        let (l, r) = if side == 0 {
            (a.time, b.time)
        } else {
            (b.time, a.time)
        };
        if let Some(size) = self.spec.window_size_micros {
            l / size == r / size
        } else {
            let delta = i128::from(r) - i128::from(l);
            delta >= -i128::from(self.spec.before_micros.unwrap())
                && delta <= i128::from(self.spec.after_micros.unwrap())
        }
    }
    pub(crate) fn next_match(&self, side: usize, pending: &Pending, after: u64) -> Option<u64> {
        self.rows[1 - side]
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .find(|(_, other)| self.matches(side, &pending.record, other))
            .map(|(id, _)| *id)
    }
    pub(crate) fn check_fanout(&self, side: usize, pending: &Pending) -> Result<()> {
        let mut count = 0;
        let mut bytes = 0usize;
        for other in self.rows[1 - side]
            .values()
            .filter(|r| self.matches(side, &pending.record, r))
        {
            count += 1;
            bytes = bytes.saturating_add(Self::output_bound(
                &pending.record,
                Some(other),
                self.output.fields.len(),
            ));
            if count > self.spec.max_matches_per_row || bytes > self.spec.max_output_bytes_per_row {
                return Err(bound("join output fan-out/byte limit exceeded"));
            }
        }
        // An unmatched left output must fit too, even if it will wait for WM.
        if side == 0
            && self.spec.mode == JoinMode::Left
            && Self::output_bound(&pending.record, None, self.output.fields.len())
                > self.spec.max_output_bytes_per_row
        {
            return Err(bound("left-unmatched output exceeds byte limit"));
        }
        Ok(())
    }
    fn output_bound(left: &Record, right: Option<&Record>, width: usize) -> usize {
        left.row
            .resident_bytes()
            .saturating_add(right.map_or(0, |r| r.row.resident_bytes()))
            .saturating_add(width.saturating_mul(std::mem::size_of::<Scalar>()))
            .saturating_add(256)
    }
    fn batch(&self, left: &Record, right: Option<&Record>) -> Result<RowBatch> {
        let bytes = Self::output_bound(left, right, self.output.fields.len());
        let _scratch = self
            .owner
            .acquire(CreditKind::Reservation, bytes.saturating_mul(3))?;
        let mut values = Vec::with_capacity(self.output.fields.len());
        values.extend(left.row.values.iter().map(Scalar::detach_copy));
        if let Some(right) = right {
            values.extend(right.row.values.iter().map(Scalar::detach_copy));
        } else {
            values.resize(values.len() + self.schemas[1].fields.len(), Scalar::Null);
        }
        values.extend([
            Scalar::Int64(right.map_or(left.time, |r| r.time.max(left.time))),
            Scalar::Int64(left.ordinal as i64),
            right.map_or(Scalar::Null, |r| Scalar::Int64(r.ordinal as i64)),
        ]);
        crate::window::finish_rows_metered(&self.output, vec![Row { values }], &self.owner)?
            .ok_or_else(|| invalid("join output absent"))
    }
    pub(crate) fn emit_match(
        &mut self,
        side: usize,
        pending: &mut Pending,
        other: u64,
    ) -> Result<RowBatch> {
        let record = &self.rows[1 - side][&other];
        let output = if side == 0 {
            self.batch(&pending.record, Some(record))?
        } else {
            self.batch(record, Some(&pending.record))?
        };
        self.rows[1 - side].get_mut(&other).unwrap().matched = true;
        pending.record.matched = true;
        Ok(output)
    }
    pub(crate) fn insert(&mut self, side: usize, pending: Pending) {
        let Pending { record, new_key } = pending;
        if let Some(credit) = new_key {
            self.bytes += credit.bytes();
            self.keys
                .insert(record.key.clone(), KeyCount { count: 0, credit });
        }
        self.keys.get_mut(&record.key).unwrap().count += 1;
        self.sequence[side] = record.ordinal;
        self.bytes += record.credit.bytes();
        self.rows[side].insert(record.ordinal, record);
    }
    pub(crate) fn expired(&self) -> Option<(usize, u64)> {
        for side in 0..2 {
            if let Some(wm) = self.watermarks[1 - side] {
                if let Some((id, _)) = self.rows[side].iter().find(|(_, r)| r.expires <= wm) {
                    return Some((side, *id));
                }
            }
        }
        None
    }
    pub(crate) fn close(&mut self, side: usize, id: u64) -> Result<Option<RowBatch>> {
        let r = &self.rows[side][&id];
        let output = if side == 0 && self.spec.mode == JoinMode::Left && !r.matched {
            Some(self.batch(r, None)?)
        } else {
            None
        };
        let r = self.rows[side].remove(&id).unwrap();
        self.bytes -= r.credit.bytes();
        let count = self.keys.get_mut(&r.key).unwrap();
        count.count -= 1;
        if count.count == 0 {
            let key = self.keys.remove(&r.key).unwrap();
            self.bytes -= key.credit.bytes();
            drop(key);
        }
        drop(r);
        Ok(output)
    }
}

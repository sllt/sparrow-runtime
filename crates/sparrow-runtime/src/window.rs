//! Processing-time, count, event-time tumble, and hopping windows.

use std::collections::HashSet;
use std::sync::Arc;

use sparrow_expr::eval;
use sparrow_model::{
    CreditKind, DeliveryContract, ErrorCode, InputId, MemoryOwner, OperatorId, Result, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SparrowError, StateSlotId, WindowKind,
};
use sparrow_plan::WindowSpec;

use crate::aggregate::Accumulator;
use crate::clock::RuntimeClock;
use crate::state::{MemoryState, StateKey};
use crate::timer::{BoundedTimers, TimerId};
use crate::watermark::{OutputHoldback, WatermarkHub};

const SLOT: u16 = 1;

#[derive(Clone, Debug, Default)]
pub struct WindowEmission {
    pub finals: Vec<Row>,
    pub lates: Vec<Row>,
}

impl WindowEmission {
    pub fn is_empty(&self) -> bool {
        self.finals.is_empty() && self.lates.is_empty()
    }
}

#[derive(Clone, Debug)]
struct TumbleEntry {
    window_start: i64,
    window_end: i64,
    accs: Vec<Accumulator>,
}

#[derive(Clone, Debug)]
struct CountEntry {
    count: u64,
    accs: Vec<Accumulator>,
}

enum WindowStore {
    Tumble(MemoryState<TumbleEntry>),
    Count(MemoryState<CountEntry>),
}

pub struct WindowOperator {
    operator: OperatorId,
    spec: WindowSpec,
    group_idx: Vec<usize>,
    event_time_idx: Option<usize>,
    input: Schema,
    output: Schema,
    store: WindowStore,
    timers: BoundedTimers,
    owner: Arc<MemoryOwner>,
    hub: WatermarkHub,
    holdback: Option<OutputHoldback>,
    default_input: InputId,
}

impl WindowOperator {
    pub fn new(
        operator: OperatorId,
        spec: WindowSpec,
        input: Schema,
        owner: Arc<MemoryOwner>,
        max_keys: usize,
        max_timers: usize,
    ) -> Result<Self> {
        spec.validate()?;
        let group_idx = resolve_keys(&input, &spec.keys)?;
        let event_time_idx = match &spec.event_time_field {
            Some(name) => Some(input.index_of_name(name).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown event-time field '{name}'"),
                )
            })?),
            None => None,
        };
        let output = sparrow_plan::window_output_schema(&input, &spec)?;
        let store = match spec.kind {
            WindowKind::Count { .. } => WindowStore::Count(MemoryState::new(
                Arc::clone(&owner),
                operator,
                StateSlotId::new(SLOT),
                max_keys,
            )?),
            _ => WindowStore::Tumble(MemoryState::new(
                Arc::clone(&owner),
                operator,
                StateSlotId::new(SLOT),
                max_keys,
            )?),
        };
        let mut hub = WatermarkHub::new();
        if let Some(bind) = spec.binding() {
            hub = hub.with_binding(bind)?;
        }
        hub.register(InputId(0))?;
        let holdback = if spec.kind.uses_event_time() {
            Some(OutputHoldback::new(spec.lateness_micros)?)
        } else {
            None
        };
        Ok(Self {
            operator,
            spec,
            group_idx,
            event_time_idx,
            input,
            output,
            store,
            timers: BoundedTimers::new(operator, max_timers)?,
            owner,
            hub,
            holdback,
            default_input: InputId(0),
        })
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }

    pub fn honesty() -> &'static str {
        DeliveryContract::ET_WINDOW_HONESTY
    }

    pub fn key_count(&self) -> usize {
        match &self.store {
            WindowStore::Tumble(s) => s.len(),
            WindowStore::Count(s) => s.len(),
        }
    }

    pub fn retention_bytes(&self) -> usize {
        match &self.store {
            WindowStore::Tumble(s) => s.retention_bytes(),
            WindowStore::Count(s) => s.retention_bytes(),
        }
    }

    pub fn live_timers(&self) -> usize {
        self.timers.live()
    }

    pub fn cancelled_timers(&self) -> u64 {
        self.timers.cancelled()
    }

    pub fn peek_deadline(&self) -> Option<i64> {
        self.timers.peek_deadline()
    }

    pub fn hub(&self) -> &WatermarkHub {
        &self.hub
    }

    pub fn wm_out(&self) -> Option<i64> {
        self.holdback.as_ref().and_then(|h| h.wm_out())
    }

    pub fn wm_in(&self) -> Option<i64> {
        self.holdback.as_ref().and_then(|h| h.wm_in())
    }

    pub fn register_input(&mut self, id: InputId) -> Result<()> {
        self.hub.register(id)
    }

    pub fn mark_idle(&mut self, id: InputId) -> Result<WindowEmission> {
        self.hub.mark_idle(id)?;
        self.drain_watermark()
    }

    pub fn mark_active(&mut self, id: InputId) -> Result<WindowEmission> {
        self.hub.mark_active(id)?;
        self.drain_watermark()
    }

    pub fn observe_watermark(&mut self, id: InputId, wm: i64) -> Result<WindowEmission> {
        self.hub.set_watermark(id, wm)?;
        self.drain_watermark()
    }

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<WindowEmission> {
        let mut out = WindowEmission::default();
        let due = self.fire_due(now)?;
        out.finals.extend(due);
        for row in batch.rows() {
            let one = self.on_row(row, now)?;
            out.finals.extend(one.finals);
            out.lates.extend(one.lates);
        }
        Ok(out)
    }

    pub fn fire_due(&mut self, now: i64) -> Result<Vec<Row>> {
        if self.spec.kind.uses_event_time() {
            return Ok(Vec::new());
        }
        let due = self.timers.fire_due(now);
        if due.is_empty() {
            return Ok(Vec::new());
        }
        let ends: HashSet<i64> = due.iter().map(|id| id.namespace as i64).collect();
        self.flush_tumble_ends(&ends)
    }

    fn on_row(&mut self, row: &Row, now: i64) -> Result<WindowEmission> {
        match self.spec.kind {
            WindowKind::TumblingProcessingTime { size_micros } => {
                Ok(WindowEmission {
                    finals: self.on_tumble_row(row, now, size_micros)?,
                    lates: Vec::new(),
                })
            }
            WindowKind::Count { size } => Ok(WindowEmission {
                finals: self.on_count_row(row, size)?,
                lates: Vec::new(),
            }),
            WindowKind::TumblingEventTime { size_micros } => {
                self.on_et_row(row, now, size_micros, None)
            }
            WindowKind::HoppingEventTime {
                size_micros,
                slide_micros,
            } => self.on_et_row(row, now, size_micros, Some(slide_micros)),
        }
    }

    fn on_et_row(
        &mut self,
        row: &Row,
        now: i64,
        size: i64,
        slide: Option<i64>,
    ) -> Result<WindowEmission> {
        let ts = self.row_event_time(row)?;
        self.hub
            .observe_event(self.default_input, ts, now)?;
        let assigned = if let Some(slide) = slide {
            WindowKind::assign_hop(ts, size, slide, self.spec.max_overlap)?
        } else {
            vec![WindowKind::assign_tumble(ts, size)?]
        };
        let wm_out = self.wm_out();
        let mut lates = Vec::new();
        let mut any_open = false;
        let group = self.group_key(row);
        for (start, end) in assigned {
            if let Some(out) = wm_out {
                if end <= out {
                    continue;
                }
            }
            any_open = true;
            self.upsert_et_window(&group, start, end, row)?;
        }
        if !any_open {
            lates.push(Row {
                values: row.values.iter().map(Scalar::detach_copy).collect(),
            });
        }
        let mut out = self.drain_watermark()?;
        out.lates.extend(lates);
        Ok(out)
    }

    fn upsert_et_window(
        &mut self,
        group: &[Scalar],
        start: i64,
        end: i64,
        row: &Row,
    ) -> Result<()> {
        let sk = self.et_state_key(group, start);
        let missing = matches!(&self.store, WindowStore::Tumble(s) if s.get(&sk).is_none());
        if missing {
            let accs = empty_accs(&self.spec, &self.input)?;
            let bytes: usize = accs.iter().map(Accumulator::tracked_bytes).sum();
            if let WindowStore::Tumble(store) = &mut self.store {
                store.put(
                    sk.clone(),
                    TumbleEntry {
                        window_start: start,
                        window_end: end,
                        accs,
                    },
                    bytes,
                )?;
            }
        }
        if let WindowStore::Tumble(store) = &mut self.store {
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&self.spec, &self.input, &mut entry.accs, row)?;
            }
        }
        Ok(())
    }

    fn drain_watermark(&mut self) -> Result<WindowEmission> {
        if self.holdback.is_none() {
            return Ok(WindowEmission::default());
        }
        let Some(wm_in) = self.hub.progress() else {
            return Ok(WindowEmission::default());
        };
        let Some(proposed_out) = self
            .holdback
            .as_mut()
            .expect("holdback")
            .on_wm_in(wm_in)?
        else {
            return Ok(WindowEmission::default());
        };
        let finals = self.flush_closed(proposed_out)?;
        self.holdback
            .as_mut()
            .expect("holdback")
            .advance_out(proposed_out)?;
        Ok(WindowEmission {
            finals,
            lates: Vec::new(),
        })
    }

    fn flush_closed(&mut self, wm_out: i64) -> Result<Vec<Row>> {
        let victims: Vec<StateKey> = match &self.store {
            WindowStore::Tumble(store) => store
                .iter()
                .filter(|(_, e)| e.window_end <= wm_out)
                .map(|(k, _)| k.clone())
                .collect(),
            _ => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        if let WindowStore::Tumble(store) = &mut self.store {
            for k in victims {
                if let Some(entry) = store.remove(&k) {
                    out.push(emit_tumble(&group_from_et_key(&k.key), &entry));
                }
            }
        }
        Ok(out)
    }

    fn row_event_time(&self, row: &Row) -> Result<i64> {
        let idx = self.event_time_idx.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "event-time window missing event_time_field binding",
            )
        })?;
        let v = row.values.get(idx).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "event-time column missing on row")
        })?;
        v.as_event_time_micros().ok_or_else(|| {
            SparrowError::new(
                ErrorCode::TypeMismatch,
                format!("event-time field must be Int64 or TimestampMicrosUTC, got {v:?}"),
            )
        })
    }

    fn et_state_key(&self, group: &[Scalar], start: i64) -> StateKey {
        let mut key = group.to_vec();
        key.push(Scalar::Int64(start));
        StateKey::new(self.operator, StateSlotId::new(SLOT), key)
    }

    fn on_tumble_row(&mut self, row: &Row, now: i64, size: i64) -> Result<Vec<Row>> {
        let (start, end) = WindowKind::assign_tumble(now, size)?;
        let key = self.group_key(row);
        let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), key);
        let spec = self.spec.clone();
        let input = self.input.clone();
        let mut late = Vec::new();
        let stale = match &self.store {
            WindowStore::Tumble(store) => store.get(&sk).and_then(|e| {
                if e.window_end <= now && e.window_start != start {
                    Some(())
                } else {
                    None
                }
            }),
            _ => None,
        };
        if stale.is_some() {
            if let WindowStore::Tumble(store) = &mut self.store {
                if let Some(old) = store.remove(&sk) {
                    late.push(emit_tumble(&sk.key, &old));
                }
            }
        }
        let missing = matches!(&self.store, WindowStore::Tumble(s) if s.get(&sk).is_none());
        if missing {
            let accs = empty_accs(&spec, &input)?;
            let bytes: usize = accs.iter().map(Accumulator::tracked_bytes).sum();
            if let WindowStore::Tumble(store) = &mut self.store {
                store.put(
                    sk.clone(),
                    TumbleEntry {
                        window_start: start,
                        window_end: end,
                        accs,
                    },
                    bytes,
                )?;
            }
            self.timers
                .schedule(TimerId::window(self.operator, end), end)?;
        }
        let rotate = match &self.store {
            WindowStore::Tumble(store) => store
                .get(&sk)
                .map(|e| e.window_start != start)
                .unwrap_or(false),
            _ => false,
        };
        if rotate {
            if let WindowStore::Tumble(store) = &mut self.store {
                if let Some(old) = store.remove(&sk) {
                    late.push(emit_tumble(&sk.key, &old));
                }
            }
            let accs = empty_accs(&spec, &input)?;
            let bytes: usize = accs.iter().map(Accumulator::tracked_bytes).sum();
            if let WindowStore::Tumble(store) = &mut self.store {
                store.put(
                    sk.clone(),
                    TumbleEntry {
                        window_start: start,
                        window_end: end,
                        accs,
                    },
                    bytes,
                )?;
            }
            self.timers
                .schedule(TimerId::window(self.operator, end), end)?;
        }
        if let WindowStore::Tumble(store) = &mut self.store {
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&spec, &input, &mut entry.accs, row)?;
            }
        }
        Ok(late)
    }

    fn on_count_row(&mut self, row: &Row, size: u64) -> Result<Vec<Row>> {
        let key = self.group_key(row);
        let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), key);
        let spec = self.spec.clone();
        let input = self.input.clone();
        let missing = matches!(&self.store, WindowStore::Count(s) if s.get(&sk).is_none());
        if missing {
            let accs = empty_accs(&spec, &input)?;
            let bytes: usize = accs.iter().map(Accumulator::tracked_bytes).sum();
            if let WindowStore::Count(store) = &mut self.store {
                store.put(sk.clone(), CountEntry { count: 0, accs }, bytes)?;
            }
        }
        let mut emit_row = None;
        if let WindowStore::Count(store) = &mut self.store {
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&spec, &input, &mut entry.accs, row)?;
                entry.count += 1;
                if entry.count >= size {
                    if let Some(entry) = store.remove(&sk) {
                        emit_row = Some(emit_count(&sk.key, &entry));
                    }
                }
            }
        }
        Ok(emit_row.into_iter().collect())
    }

    fn flush_tumble_ends(&mut self, ends: &HashSet<i64>) -> Result<Vec<Row>> {
        let victims: Vec<StateKey> = match &self.store {
            WindowStore::Tumble(store) => store
                .iter()
                .filter(|(_, e)| ends.contains(&e.window_end))
                .map(|(k, _)| k.clone())
                .collect(),
            _ => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        if let WindowStore::Tumble(store) = &mut self.store {
            for k in victims {
                if let Some(entry) = store.remove(&k) {
                    out.push(emit_tumble(&k.key, &entry));
                }
            }
        }
        Ok(out)
    }

    fn group_key(&self, row: &Row) -> Vec<Scalar> {
        self.group_idx
            .iter()
            .map(|&i| row.values[i].detach_copy())
            .collect()
    }

    pub fn build_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows(&self.output, rows, &self.owner)
    }

    pub fn build_late_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows(&self.input, rows, &self.owner)
    }

    pub fn cleanup(&mut self) {
        match &mut self.store {
            WindowStore::Tumble(s) => s.clear(),
            WindowStore::Count(s) => s.clear(),
        }
        self.timers.cancel_all();
    }
}

fn group_from_et_key(key: &[Scalar]) -> Vec<Scalar> {
    if key.is_empty() {
        return Vec::new();
    }
    key[..key.len() - 1].to_vec()
}

fn empty_accs(spec: &WindowSpec, input: &Schema) -> Result<Vec<Accumulator>> {
    spec.aggs
        .iter()
        .map(|a| {
            let ty = a.input_type(input)?;
            Accumulator::new(a.func, ty, a.count_star)
        })
        .collect()
}

fn update_accs(spec: &WindowSpec, input: &Schema, accs: &mut [Accumulator], row: &Row) -> Result<()> {
    for (acc, call) in accs.iter_mut().zip(spec.aggs.iter()) {
        let v = if call.count_star {
            Scalar::Null
        } else if let Some(expr) = &call.input {
            eval(expr, input, &row.values)?
        } else {
            Scalar::Null
        };
        acc.update(&v)?;
    }
    Ok(())
}

fn emit_tumble(key: &[Scalar], entry: &TumbleEntry) -> Row {
    let mut values = key.to_vec();
    values.push(Scalar::Int64(entry.window_start));
    values.push(Scalar::Int64(entry.window_end));
    for a in &entry.accs {
        values.push(a.finish());
    }
    Row { values }
}

fn emit_count(key: &[Scalar], entry: &CountEntry) -> Row {
    let mut values = key.to_vec();
    values.push(Scalar::Int64(0));
    values.push(Scalar::Int64(entry.count as i64));
    for a in &entry.accs {
        values.push(a.finish());
    }
    Row { values }
}

/// Drive timers while also reading input. Used by the kernel stage.
pub async fn wait_timer_or_input<T>(
    clock: &RuntimeClock,
    deadline: Option<i64>,
    recv: impl std::future::Future<Output = T>,
) -> TimerOrInput<T> {
    tokio::select! {
        biased;
        v = recv => TimerOrInput::Input(v),
        _ = clock.sleep_until(deadline) => TimerOrInput::Timer,
    }
}

pub enum TimerOrInput<T> {
    Timer,
    Input(T),
}

pub fn resolve_keys(schema: &Schema, keys: &[String]) -> Result<Vec<usize>> {
    let mut idx = Vec::new();
    for k in keys {
        let i = schema.index_of_name(k).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, format!("unknown group key '{k}'"))
        })?;
        idx.push(i);
    }
    Ok(idx)
}

pub fn window_output_schema(input: &Schema, spec: &WindowSpec) -> Result<Schema> {
    sparrow_plan::window_output_schema(input, spec)
}

pub fn finish_rows(
    schema: &Schema,
    rows: Vec<Row>,
    owner: &Arc<MemoryOwner>,
) -> Result<Option<RowBatch>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let bytes: usize = rows.iter().map(Row::tracked_bytes).sum();
    let mut b = RowBatchBuilder::new(
        Arc::new(schema.clone()),
        Arc::clone(owner),
        CreditKind::Reservation,
        rows.len().max(1),
        owner
            .budget()
            .cap(CreditKind::Reservation)
            .min(bytes.saturating_mul(2).max(64)),
    )?;
    for row in rows {
        b.push(row)?;
    }
    Ok(Some(b.finish()?))
}

//! Processing-time, count, event-time tumble, and hopping windows.

use std::collections::{BTreeMap, HashSet};
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
    /// Event-time close watermark that still has keyed state to drain in
    /// mailbox-sized chunks (R17). Kernel must take/send before advancing.
    pub pending_close: Option<i64>,
    /// Rows rejected because `event_time > now + max_future_skew`.
    /// Counted for observability; does not poison the watermark (P0-5).
    pub future_dropped: u64,
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
    /// Ordered closable tumble keys: `(window_end, encoded_key) → StateKey`.
    /// `take_closed_one` / `peek_closed_bytes` pop the first closed entry
    /// instead of scanning every key and `to_vec()`-ing candidates (N11).
    closed_index: BTreeMap<(i64, Vec<u8>), StateKey>,
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
            closed_index: BTreeMap::new(),
        })
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }

    pub fn input_schema(&self) -> &Schema {
        &self.input
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

    /// Ingest a batch at processing-time `now`.
    ///
    /// `now` is **processing time** from [`crate::RuntimeClock`] (host wall
    /// clock or an injected virtual clock), in microseconds. It is **not**
    /// event time and does not advance the watermark. Event-time future-skew
    /// compares `event_time > now + max_future_skew`; those rows are counted
    /// on [`WindowEmission::future_dropped`] and dropped without poisoning
    /// `max_et` (P0-5).
    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<WindowEmission> {
        let mut out = WindowEmission::default();
        let due = self.fire_due(now)?;
        out.finals.extend(due);
        for row in batch.rows() {
            let one = self.on_row(row, now)?;
            out.finals.extend(one.finals);
            out.lates.extend(one.lates);
            out.future_dropped = out.future_dropped.saturating_add(one.future_dropped);
            // ET close is signaled via pending_close (N5). Dropping it
            // means later event times never emit finals on the live path.
            if let Some(wm) = one.pending_close {
                out.pending_close = Some(out.pending_close.map(|p| p.max(wm)).unwrap_or(wm));
            }
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
                    ..WindowEmission::default()
                })
            }
            WindowKind::Count { size } => Ok(WindowEmission {
                finals: self.on_count_row(row, size)?,
                ..WindowEmission::default()
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
        if let Some(bind) = self.spec.binding() {
            if let Some(skew) = bind.max_future_skew_micros {
                // `now` is processing-time micros (see on_batch). Do not
                // observe_event — that would poison max_et (P0-5).
                if ts > now.saturating_add(skew) {
                    return Ok(WindowEmission {
                        future_dropped: 1,
                        ..WindowEmission::default()
                    });
                }
            }
        }
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
            self.index_insert(&sk, end);
        }
        if let WindowStore::Tumble(store) = &mut self.store {
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&self.spec, &self.input, &mut entry.accs, row)?;
            }
            if let Some(entry) = store.get(&sk) {
                let bytes: usize = entry.accs.iter().map(Accumulator::tracked_bytes).sum();
                store.recharge(&sk, bytes)?;
            }
        }
        Ok(())
    }

    /// Test/demo helper: take every closed window for `pending_close`.
    /// Production Kernel uses mailbox-sized `take_closed_chunk` instead.
    pub fn materialize_emission(&mut self, mut emission: WindowEmission) -> Result<WindowEmission> {
        if let Some(wm) = emission.pending_close.take() {
            let chunk = self.take_closed_chunk(wm, 4096, 1024 * 1024)?;
            emission.finals.extend(chunk);
            self.advance_holdback(wm)?;
        }
        Ok(emission)
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
        // Do not materialize every closed window here (P1-20 / R17).
        // Caller takes mailbox-sized chunks, then advance_holdback.
        Ok(WindowEmission {
            pending_close: Some(proposed_out),
            ..WindowEmission::default()
        })
    }

    pub fn advance_holdback(&mut self, wm_out: i64) -> Result<()> {
        if let Some(h) = self.holdback.as_mut() {
            h.advance_out(wm_out)?;
        }
        Ok(())
    }

    /// Remove and emit at most one closed window. Used so a giant flush
    /// cannot drop all state and then fail to send.
    ///
    /// Order is deterministic: smallest `(window_end, encoded_key)` first.
    /// The closed index is maintained on put/remove so this is O(log n),
    /// not a full scan plus per-candidate `to_vec` (N11).
    pub fn take_closed_one(&mut self, wm_out: i64) -> Result<Option<Row>> {
        loop {
            let Some(k) = self.next_closed_key(wm_out) else {
                return Ok(None);
            };
            let entry = match &mut self.store {
                WindowStore::Tumble(store) => store.remove(&k),
                _ => None,
            };
            if let Some(entry) = entry {
                self.closed_index
                    .remove(&(entry.window_end, k.encoded_bytes().to_vec()));
                return Ok(Some(emit_tumble(&group_from_et_key(&k.key), &entry)));
            }
            self.closed_index.retain(|_, v| v != &k);
        }
    }

    fn next_closed_key(&self, wm_out: i64) -> Option<StateKey> {
        match self.closed_index.first_key_value() {
            Some(((end, _), key)) if *end <= wm_out => Some(key.clone()),
            _ => None,
        }
    }

    fn index_insert(&mut self, key: &StateKey, window_end: i64) {
        self.closed_index
            .insert((window_end, key.encoded_bytes().to_vec()), key.clone());
    }

    fn index_remove(&mut self, key: &StateKey, window_end: i64) {
        self.closed_index
            .remove(&(window_end, key.encoded_bytes().to_vec()));
    }

    fn rebuild_closed_index(&mut self) {
        self.closed_index.clear();
        if let WindowStore::Tumble(store) = &self.store {
            for (k, e) in store.iter() {
                self.closed_index
                    .insert((e.window_end, k.encoded_bytes().to_vec()), k.clone());
            }
        }
    }

    /// Take a mailbox-sized chunk of closed windows (R17).
    pub fn take_closed_chunk(
        &mut self,
        wm_out: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Row>> {
        let mut out = Vec::new();
        let mut bytes = 0usize;
        while out.len() < max_rows.max(1) {
            let Some(sz) = self.peek_closed_bytes(wm_out) else {
                break;
            };
            if !out.is_empty() && bytes.saturating_add(sz) > max_bytes {
                break;
            }
            let Some(row) = self.take_closed_one(wm_out)? else {
                break;
            };
            bytes = bytes.saturating_add(row.tracked_bytes());
            out.push(row);
        }
        Ok(out)
    }

    fn peek_closed_bytes(&self, wm_out: i64) -> Option<usize> {
        let key = self.next_closed_key(wm_out)?;
        match &self.store {
            WindowStore::Tumble(store) => store.get(&key).map(|e| {
                e.accs.iter().map(Accumulator::tracked_bytes).sum::<usize>() + 32
            }),
            _ => None,
        }
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
            let old = match &mut self.store {
                WindowStore::Tumble(store) => store.remove(&sk),
                _ => None,
            };
            if let Some(old) = old {
                self.index_remove(&sk, old.window_end);
                late.push(emit_tumble(&sk.key, &old));
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
            self.index_insert(&sk, end);
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
            let old = match &mut self.store {
                WindowStore::Tumble(store) => store.remove(&sk),
                _ => None,
            };
            if let Some(old) = old {
                self.index_remove(&sk, old.window_end);
                late.push(emit_tumble(&sk.key, &old));
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
            self.index_insert(&sk, end);
            self.timers
                .schedule(TimerId::window(self.operator, end), end)?;
        }
        if let WindowStore::Tumble(store) = &mut self.store {
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&spec, &input, &mut entry.accs, row)?;
            }
            if let Some(entry) = store.get(&sk) {
                let bytes: usize = entry.accs.iter().map(Accumulator::tracked_bytes).sum();
                store.recharge(&sk, bytes)?;
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
            }
            if let Some(entry) = store.get(&sk) {
                let bytes: usize = entry.accs.iter().map(Accumulator::tracked_bytes).sum();
                let count = entry.count;
                store.recharge(&sk, bytes)?;
                if count >= size {
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
        for k in victims {
            let entry = match &mut self.store {
                WindowStore::Tumble(store) => store.remove(&k),
                _ => None,
            };
            if let Some(entry) = entry {
                self.index_remove(&k, entry.window_end);
                out.push(emit_tumble(&k.key, &entry));
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
        self.closed_index.clear();
        self.timers.cancel_all();
    }

    /// Freeze open keyed state. This clones in-memory keys; the clone is
    /// bounded by the job `max_state_keys` quota (P1-14). Not a streaming
    /// snapshot — production jobs stay within that cap.
    pub fn freeze(&self) -> WindowFreeze {
        let mut entries = Vec::new();
        let kind = match &self.store {
            WindowStore::Tumble(store) => {
                for (k, e) in store.iter() {
                    entries.push(FrozenEntry {
                        key: k.key.clone(),
                        window_start: e.window_start,
                        window_end: e.window_end,
                        count: 0,
                        accs: e.accs.clone(),
                    });
                }
                0u8
            }
            WindowStore::Count(store) => {
                for (k, e) in store.iter() {
                    entries.push(FrozenEntry {
                        key: k.key.clone(),
                        window_start: 0,
                        window_end: 0,
                        count: e.count,
                        accs: e.accs.clone(),
                    });
                }
                1u8
            }
        };
        entries.sort_by(|a, b| a.key_bytes().cmp(&b.key_bytes()));
        WindowFreeze {
            operator: self.operator,
            slot: StateSlotId::new(SLOT),
            kind,
            entries,
            wm_in: self.wm_in(),
            wm_out: self.wm_out(),
            last_effective: self.hub.last_effective(),
        }
    }

    /// Replace in-memory state from a committed freeze (experimental).
    pub fn restore_freeze(&mut self, freeze: &WindowFreeze) -> Result<()> {
        if freeze.operator != self.operator {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint operator id does not match this window",
            ));
        }
        if freeze.slot != StateSlotId::new(SLOT) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "checkpoint StateSlotKey does not match this window",
            ));
        }
        self.cleanup();
        match (&mut self.store, freeze.kind) {
            (WindowStore::Tumble(store), 0) => {
                for e in &freeze.entries {
                    let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), e.key.clone());
                    let bytes: usize = e.accs.iter().map(Accumulator::tracked_bytes).sum();
                    store.put(
                        sk,
                        TumbleEntry {
                            window_start: e.window_start,
                            window_end: e.window_end,
                            accs: e.accs.clone(),
                        },
                        bytes,
                    )?;
                }
            }
            (WindowStore::Count(store), 1) => {
                for e in &freeze.entries {
                    let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), e.key.clone());
                    let bytes: usize = e.accs.iter().map(Accumulator::tracked_bytes).sum();
                    store.put(
                        sk,
                        CountEntry {
                            count: e.count,
                            accs: e.accs.clone(),
                        },
                        bytes,
                    )?;
                }
            }
            _ => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "checkpoint window kind does not match this operator",
                ));
            }
        }
        if let Some(h) = self.holdback.as_mut() {
            h.restore(freeze.wm_in, freeze.wm_out);
        }
        self.hub.restore_effective(freeze.last_effective);
        self.rebuild_closed_index();
        // R16: PT windows must still close after restore without new data.
        if !self.spec.kind.uses_event_time() {
            if let WindowStore::Tumble(store) = &self.store {
                let ends: Vec<i64> = store.iter().map(|(_, e)| e.window_end).collect();
                for end in ends {
                    self.timers
                        .schedule(TimerId::window(self.operator, end), end)?;
                }
            }
        }
        Ok(())
    }

    /// Stable fingerprint of open state (demo / tests).
    pub fn state_fingerprint(&self) -> String {
        let f = self.freeze();
        format!(
            "keys={} kind={} wm_in={:?} wm_out={:?} accs={}",
            f.entries.len(),
            f.kind,
            f.wm_in,
            f.wm_out,
            f.entries
                .iter()
                .map(|e| e.accs.iter().map(|a| format!("{:?}", a.finish())).collect::<Vec<_>>().join("+"))
                .collect::<Vec<_>>()
                .join(";")
        )
    }
}

/// Frozen window operator state (V1 aligned checkpoint payload).
#[derive(Clone, Debug, PartialEq)]
pub struct WindowFreeze {
    pub operator: OperatorId,
    pub slot: StateSlotId,
    pub kind: u8,
    pub entries: Vec<FrozenEntry>,
    pub wm_in: Option<i64>,
    pub wm_out: Option<i64>,
    pub last_effective: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenEntry {
    pub key: Vec<Scalar>,
    pub window_start: i64,
    pub window_end: i64,
    pub count: u64,
    pub accs: Vec<Accumulator>,
}

impl FrozenEntry {
    fn key_bytes(&self) -> Vec<u8> {
        let mut b = Vec::new();
        for s in &self.key {
            s.encode_key(&mut b);
            b.push(0xff);
        }
        b
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

/// Count-window bounds are **in-window arrival ordinals**, not event-time.
///
/// A closed window of `size` events emits the half-open range `[0, count)`
/// (`window_start = 0`, `window_end = count`, and `count == size` at emit).
/// These columns share names with tumble `window_start` / `window_end` but
/// must not be read as timestamps (P3-47).
pub const COUNT_WINDOW_BOUNDS: &str =
    "count-window window_start/window_end are 0-based half-open arrival ordinals [0, count) within the closed window, not event-time micros";

fn emit_count(key: &[Scalar], entry: &CountEntry) -> Row {
    let mut values = key.to_vec();
    // Honest ordinals: [0, count) in this window. Not a timestamp (P3-47).
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

#[cfg(test)]
mod review_tests {
    use super::*;
    use sparrow_expr::Expr;
    use sparrow_model::{
        AggFn, DataType, Field, FieldId, ResourceBudget, Scalar, SchemaId, WindowKind,
    };
    use sparrow_plan::AggCall;

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    fn pt_spec() -> WindowSpec {
        WindowSpec::new(
            WindowKind::TumblingProcessingTime {
                size_micros: 1_000_000,
            },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        )
    }

    fn op() -> WindowOperator {
        WindowOperator::new(
            OperatorId::new(1),
            pt_spec(),
            schema(),
            MemoryOwner::new(ResourceBudget::compact()),
            16,
            16,
        )
        .unwrap()
    }

    #[test]
    fn r16_pt_restore_rebuilds_timers() {
        let mut a = op();
        let row = Row {
            values: vec![Scalar::utf8("d1"), Scalar::Int64(10)],
        };
        let _ = a.on_row(&row, 0).unwrap();
        assert!(a.peek_deadline().is_some());
        let freeze = a.freeze();
        let mut b = op();
        b.restore_freeze(&freeze).unwrap();
        assert!(
            b.peek_deadline().is_some(),
            "PT restore must reschedule close timers"
        );
        let emitted = b.fire_due(1_000_000).unwrap();
        assert!(
            !emitted.is_empty(),
            "restored PT window must close without new data"
        );
    }

    #[test]
    fn r17_take_closed_chunk_leaves_remaining_state() {
        let mut w = op();
        w.restore_freeze(&WindowFreeze {
            operator: OperatorId::new(1),
            slot: StateSlotId::new(1),
            kind: 0,
            entries: vec![
                FrozenEntry {
                    key: vec![Scalar::utf8("a"), Scalar::Int64(0)],
                    window_start: 0,
                    window_end: 10,
                    count: 0,
                    accs: vec![Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap()],
                },
                FrozenEntry {
                    key: vec![Scalar::utf8("b"), Scalar::Int64(0)],
                    window_start: 0,
                    window_end: 10,
                    count: 0,
                    accs: vec![Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap()],
                },
            ],
            wm_in: Some(10),
            wm_out: Some(0),
            last_effective: Some(10),
        })
        .unwrap();
        let chunk = w.take_closed_chunk(10, 1, 1024).unwrap();
        assert_eq!(chunk.len(), 1);
        let rest = w.take_closed_chunk(10, 8, 1024).unwrap();
        assert_eq!(rest.len(), 1);
        assert!(w.take_closed_chunk(10, 8, 1024).unwrap().is_empty());
    }

    #[test]
    fn r17_many_keys_closing_chunk_within_mailbox() {
        let mut entries = Vec::new();
        for i in 0..32 {
            entries.push(FrozenEntry {
                key: vec![Scalar::utf8(&format!("k{i}")), Scalar::Int64(0)],
                window_start: 0,
                window_end: 10,
                count: 0,
                accs: vec![Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap()],
            });
        }
        let mut w = WindowOperator::new(
            OperatorId::new(1),
            pt_spec(),
            schema(),
            MemoryOwner::new(ResourceBudget::compact()),
            64,
            64,
        )
        .unwrap();
        w.restore_freeze(&WindowFreeze {
            operator: OperatorId::new(1),
            slot: StateSlotId::new(1),
            kind: 0,
            entries,
            wm_in: Some(10),
            wm_out: Some(0),
            last_effective: Some(10),
        })
        .unwrap();
        let mailbox_items = 4usize;
        let mailbox_bytes = 256usize;
        let mut total = 0usize;
        let mut rounds = 0usize;
        loop {
            let chunk = w
                .take_closed_chunk(10, mailbox_items, mailbox_bytes)
                .unwrap();
            if chunk.is_empty() {
                break;
            }
            assert!(
                chunk.len() <= mailbox_items,
                "closed-key flush must stay within mailbox item limit"
            );
            total += chunk.len();
            rounds += 1;
            assert!(rounds <= 64, "chunked flush must make progress");
        }
        assert_eq!(total, 32, "every closed key must be emitted");
        assert!(rounds > 1, "32 keys must not flush as a single mailbox burst");
    }

    #[test]
    fn p3_47_count_window_bounds_are_ordinals_not_event_time() {
        let spec = WindowSpec::new(
            WindowKind::Count { size: 2 },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        );
        let mut w = WindowOperator::new(
            OperatorId::new(1),
            spec,
            schema(),
            MemoryOwner::new(ResourceBudget::compact()),
            16,
            16,
        )
        .unwrap();
        let r1 = Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(1)],
        };
        let r2 = Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(2)],
        };
        assert!(w.on_row(&r1, 0).unwrap().finals.is_empty());
        let out = w.on_row(&r2, 0).unwrap();
        assert_eq!(out.finals.len(), 1);
        let row = &out.finals[0];
        assert_eq!(row.values[1], Scalar::Int64(0), "{COUNT_WINDOW_BOUNDS}");
        assert_eq!(row.values[2], Scalar::Int64(2), "window_end is in-window count");
        assert_eq!(row.values[3], Scalar::Int64(3));
    }

    #[test]
    fn n11_many_keys_close_in_deterministic_order() {
        const N: usize = 256;
        let mut entries = Vec::with_capacity(N);
        for i in 0..N {
            let end = 10 + ((i % 7) as i64) * 10;
            entries.push(FrozenEntry {
                key: vec![Scalar::utf8(&format!("k{i:03}")), Scalar::Int64(0)],
                window_start: 0,
                window_end: end,
                count: 0,
                accs: vec![Accumulator::new(AggFn::Sum, DataType::Int64, false).unwrap()],
            });
        }
        let mut expected: Vec<(i64, Vec<u8>, String)> = entries
            .iter()
            .map(|e| {
                let sk = StateKey::new(OperatorId::new(1), StateSlotId::new(1), e.key.clone());
                let name = match &e.key[0] {
                    Scalar::Utf8(s) => s.to_string(),
                    _ => String::new(),
                };
                (e.window_end, sk.encoded_bytes().to_vec(), name)
            })
            .collect();
        expected.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));

        let mut w = WindowOperator::new(
            OperatorId::new(1),
            pt_spec(),
            schema(),
            MemoryOwner::new(ResourceBudget::compact()),
            512,
            512,
        )
        .unwrap();
        w.restore_freeze(&WindowFreeze {
            operator: OperatorId::new(1),
            slot: StateSlotId::new(1),
            kind: 0,
            entries,
            wm_in: Some(100),
            wm_out: Some(0),
            last_effective: Some(100),
        })
        .unwrap();

        let started = std::time::Instant::now();
        let mut got = Vec::new();
        loop {
            let chunk = w.take_closed_chunk(100, 16, 4096).unwrap();
            if chunk.is_empty() {
                break;
            }
            for row in chunk {
                let name = match &row.values[0] {
                    Scalar::Utf8(s) => s.to_string(),
                    _ => String::new(),
                };
                let end = match row.values.get(2) {
                    Some(Scalar::Int64(v)) => *v,
                    _ => -1,
                };
                got.push((end, name));
            }
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "closing {N} keys via the ordered index must not O(n²) scan"
        );
        assert_eq!(got.len(), N, "every closed key must emit once");
        let expected_pairs: Vec<(i64, String)> =
            expected.into_iter().map(|(end, _, name)| (end, name)).collect();
        assert_eq!(
            got, expected_pairs,
            "closed keys must emit in (window_end, encoded_key) order"
        );
        assert!(w.take_closed_chunk(100, 16, 4096).unwrap().is_empty());
        assert_eq!(w.key_count(), 0);
    }
}

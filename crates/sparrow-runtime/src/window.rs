//! Processing-time tumbling windows and count windows with incremental aggs.

use std::sync::Arc;

use sparrow_expr::eval;
use sparrow_model::{
    CreditKind, DeliveryContract, ErrorCode, Field, FieldId, MemoryOwner, OperatorId, Result, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId, SparrowError, StateSlotId, WindowKind,
};
use sparrow_plan::WindowSpec;

use crate::aggregate::Accumulator;
use crate::clock::RuntimeClock;
use crate::state::{MemoryState, StateKey};
use crate::timer::{BoundedTimers, TimerId};

const SLOT: u16 = 1;

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
    input: Schema,
    output: Schema,
    store: WindowStore,
    timers: BoundedTimers,
    owner: Arc<MemoryOwner>,
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
        let output = sparrow_plan::window_output_schema(&input, &spec)?;
        let store = match spec.kind {
            WindowKind::TumblingProcessingTime { .. } => WindowStore::Tumble(MemoryState::new(
                Arc::clone(&owner),
                operator,
                StateSlotId::new(SLOT),
                max_keys,
            )?),
            WindowKind::Count { .. } => WindowStore::Count(MemoryState::new(
                Arc::clone(&owner),
                operator,
                StateSlotId::new(SLOT),
                max_keys,
            )?),
        };
        Ok(Self {
            operator,
            spec,
            group_idx,
            input,
            output,
            store,
            timers: BoundedTimers::new(operator, max_timers)?,
            owner,
        })
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }

    pub fn honesty() -> &'static str {
        DeliveryContract::PT_WINDOW_HONESTY
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

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Vec<Row>> {
        let mut emitted = Vec::new();
        emitted.extend(self.fire_due(now)?);
        for row in batch.rows() {
            emitted.extend(self.on_row(row, now)?);
        }
        Ok(emitted)
    }

    pub fn fire_due(&mut self, now: i64) -> Result<Vec<Row>> {
        let due = self.timers.fire_due(now);
        if due.is_empty() {
            return Ok(Vec::new());
        }
        let ends: std::collections::HashSet<i64> =
            due.iter().map(|id| id.namespace as i64).collect();
        self.flush_tumble_ends(&ends)
    }

    fn on_row(&mut self, row: &Row, now: i64) -> Result<Vec<Row>> {
        match self.spec.kind {
            WindowKind::TumblingProcessingTime { size_micros } => {
                self.on_tumble_row(row, now, size_micros)
            }
            WindowKind::Count { size } => self.on_count_row(row, size),
        }
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

    fn flush_tumble_ends(&mut self, ends: &std::collections::HashSet<i64>) -> Result<Vec<Row>> {
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

    pub fn cleanup(&mut self) {
        match &mut self.store {
            WindowStore::Tumble(s) => s.clear(),
            WindowStore::Count(s) => s.clear(),
        }
        self.timers.cancel_all();
    }
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
    let mut fields = Vec::new();
    let mut id = 1u16;
    for k in &spec.keys {
        let f = input.field_by_name(k).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, format!("unknown key '{k}'"))
        })?;
        fields.push(Field::new(FieldId::new(id), f.name.clone(), f.data_type.clone(), f.nullable));
        id += 1;
    }
    fields.push(Field::new(
        FieldId::new(id),
        "window_start",
        sparrow_model::DataType::Int64,
        false,
    ));
    id += 1;
    fields.push(Field::new(
        FieldId::new(id),
        "window_end",
        sparrow_model::DataType::Int64,
        false,
    ));
    id += 1;
    for agg in &spec.aggs {
        let ty = agg.result_type(input)?;
        fields.push(Field::new(FieldId::new(id), agg.alias.clone(), ty, true));
        id += 1;
    }
    Schema::new(SchemaId::new(input.id.raw().saturating_add(50)), fields)
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
        owner.budget().cap(CreditKind::Reservation).min(bytes.saturating_mul(2).max(64)),
    )?;
    for row in rows {
        b.push(row)?;
    }
    Ok(Some(b.finish()?))
}

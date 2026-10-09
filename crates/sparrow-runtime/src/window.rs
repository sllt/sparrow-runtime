//! Processing-time, count, event-time tumble, and hopping windows.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use sparrow_expr::{bind, eval_bound, BoundExpr};
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
    external_watermarks: bool,
    /// Ordered closable tumble keys: `(window_end, encoded_key) → StateKey`.
    /// `take_closed_one` / `peek_closed_bytes` pop the first closed entry
    /// instead of scanning every key and `to_vec()`-ing candidates (N11).
    closed_index: BTreeMap<(i64, Vec<u8>), StateKey>,
    /// Agg input exprs bound to column indices once (P2-30).
    bound_aggs: Vec<Option<BoundExpr>>,
    allocation: Vec<sparrow_expr::allocation::AllocationBound>,
    variable_accs: bool,
    closed_index_bytes: usize,
    accumulator_scratch: Vec<Accumulator>,
    accumulator_scratch_lease: Option<sparrow_model::MemoryLease>,
    /// Fresh-only PT hopping must not reopen windows after a wall-clock rollback.
    /// Not part of any legacy freeze or checkpoint identity.
    hopping_clock_high: i64,
}

/// Widest input schema the fixed-size-accumulator credit fast path handles on
/// the stack. Above this the generic path keeps its heap `Vec<usize>`.
const WORKING_CREDIT_MAX_FIELDS: usize = 64;

/// Fixed-size accumulator fast path: max resident bytes per column in a stack
/// array, folded once over the aggregate `allocation` bound.
///
/// `#[inline(never)]` keeps the bounded `[usize; 64]` column array (512 bytes on
/// 64-bit targets) out of `working_credit`'s frame (and out of any async state
/// machine inlining the caller). No heap allocation;
/// `AllocationBound::estimate` only reads the prebuilt bound tree.
#[inline(never)]
fn fixed_size_column_estimate(
    allocation: &[sparrow_expr::allocation::AllocationBound],
    batch: &RowBatch,
    fields: usize,
) -> (usize, usize) {
    debug_assert!(fields <= WORKING_CREDIT_MAX_FIELDS);
    let mut columns = [0usize; WORKING_CREDIT_MAX_FIELDS];
    let columns = &mut columns[..fields];
    for row in batch.rows() {
        for (size, value) in columns.iter_mut().zip(&row.values) {
            *size = (*size).max(value.resident_bytes());
        }
    }
    let mut allocated = 0usize;
    let mut values = 0usize;
    for bound in allocation {
        let e = bound.estimate(columns);
        allocated = allocated.saturating_add(e.allocated);
        values = values.saturating_add(e.value);
    }
    (allocated, values)
}

impl WindowOperator {
    fn hopping_now(&mut self, now: i64) -> i64 {
        if now >= 0 && matches!(self.spec.kind, WindowKind::HoppingProcessingTime { .. }) {
            self.hopping_clock_high = self.hopping_clock_high.max(now);
            self.hopping_clock_high
        } else {
            now
        }
    }
    pub(crate) fn is_processing_time(&self) -> bool {
        self.spec.kind.uses_processing_time_timer()
    }
    fn suffixed_key(&self) -> bool {
        self.spec.kind.uses_event_time()
            || matches!(self.spec.kind, WindowKind::HoppingProcessingTime { .. })
    }
    pub(crate) fn is_event_time(&self) -> bool {
        self.spec.kind.uses_event_time()
    }
    /// A logged permanent EOF closes all windows, including positive holdback.
    /// Used only by the durable ET graph, never by an ordinary watermark.
    pub(crate) fn advance_graph_final(&mut self) {
        if let Some(holdback) = &mut self.holdback {
            holdback.restore(Some(i64::MAX), Some(i64::MAX));
        }
    }
    pub(crate) fn finish_input(&mut self) -> Result<WindowEmission> {
        self.hub.mark_active(self.default_input)?;
        self.hub.set_watermark(self.default_input, i64::MAX)?;
        self.drain_watermark()
    }
    pub(crate) fn use_external_watermarks(&mut self) {
        self.external_watermarks = true;
    }
    pub(crate) fn memory_owner(&self) -> Arc<MemoryOwner> {
        Arc::clone(&self.owner)
    }
    pub(crate) fn operator_id(&self) -> OperatorId {
        self.operator
    }

    /// K1 validates every restored instance before input activation. Legacy raw
    /// restore retains its separate embedding contract.
    pub(crate) fn validate_participant_restore(&self, freeze: &WindowFreeze) -> Result<()> {
        let invalid = || {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "restored window state does not match participant schema/accumulators/bounds",
            )
        };
        let count = matches!(self.spec.kind, WindowKind::Count { .. });
        if freeze.operator != self.operator
            || freeze.slot != StateSlotId::new(SLOT)
            || freeze.kind != u8::from(count)
        {
            return Err(invalid());
        }
        let expected = empty_accs(&self.spec, &self.input)?;
        for entry in &freeze.entries {
            if entry.key.len() != self.group_idx.len() + usize::from(self.suffixed_key())
                || entry.accs.len() != expected.len()
            {
                return Err(invalid());
            }
            for (value, idx) in entry.key.iter().zip(&self.group_idx) {
                let field = &self.input.fields[*idx];
                if !value.matches_type(&field.data_type) && !(value.is_null() && field.nullable) {
                    return Err(invalid());
                }
            }
            if let WindowKind::Count { size } = self.spec.kind {
                if entry.count == 0 || entry.count >= size {
                    return Err(invalid());
                }
            } else {
                let (size, slide) = match self.spec.kind {
                    WindowKind::TumblingProcessingTime { size_micros } => {
                        (size_micros, size_micros)
                    }
                    WindowKind::TumblingEventTime { size_micros } => (size_micros, size_micros),
                    WindowKind::HoppingEventTime {
                        size_micros,
                        slide_micros,
                    }
                    | WindowKind::HoppingProcessingTime {
                        size_micros,
                        slide_micros,
                    } => (size_micros, slide_micros),
                    _ => return Err(invalid()),
                };
                if entry.window_start.checked_add(size) != Some(entry.window_end)
                    || entry.window_start.rem_euclid(slide) != 0
                    || (self.suffixed_key()
                        && entry.key.last() != Some(&Scalar::Int64(entry.window_start)))
                {
                    return Err(invalid());
                }
                if self.is_processing_time() && entry.count != 0 {
                    return Err(invalid());
                }
            }
            for ((actual, prototype), call) in entry.accs.iter().zip(&expected).zip(&self.spec.aggs)
            {
                if std::mem::discriminant(actual) != std::mem::discriminant(prototype) {
                    return Err(invalid());
                }
                let n = match actual {
                    Accumulator::Extended(extra) => {
                        extra.check_restore(
                            call.func,
                            &call.input_type(&self.input)?,
                            count.then_some(entry.count),
                        )?;
                        match extra.as_ref() {
                            crate::aggregate::ExtendedAccumulator::Moment { n, .. } => *n,
                            crate::aggregate::ExtendedAccumulator::Value { .. } => 0,
                        }
                    }
                    Accumulator::Count {
                        rows,
                        non_null,
                        star,
                    } => {
                        if *star != call.count_star
                            || non_null > rows
                            || (count && *rows != entry.count)
                        {
                            return Err(invalid());
                        }
                        *rows
                    }
                    Accumulator::SumI64 { n, .. }
                    | Accumulator::SumU64 { n, .. }
                    | Accumulator::SumF64 { n, .. }
                    | Accumulator::Avg { n, .. } => *n,
                    Accumulator::Min { v } | Accumulator::Max { v } => {
                        if let Some(v) = v {
                            if v.is_null()
                                || v.is_nan()
                                || !v.matches_type(&call.input_type(&self.input)?)
                            {
                                return Err(invalid());
                            }
                        }
                        0
                    }
                };
                if count && n > entry.count {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_processing_cut(&self, now: i64) -> Result<()> {
        if now < 0 || self.spec.kind.uses_event_time() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "invalid ordered window time policy",
            ));
        }
        if let WindowStore::Tumble(store) = &self.store {
            if store
                .iter()
                .any(|(_, e)| e.window_start < 0 || e.window_start > now || e.window_end <= now)
            {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "PT restore has a future window or overdue timer",
                ));
            }
        }
        if self.wm_in().is_some() || self.wm_out().is_some() || self.hub.last_effective().is_some()
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "ordered window cannot restore event-time watermarks",
            ));
        }
        Ok(())
    }

    pub fn new(
        operator: OperatorId,
        spec: WindowSpec,
        input: Schema,
        owner: Arc<MemoryOwner>,
        max_keys: usize,
        max_timers: usize,
    ) -> Result<Self> {
        spec.validate()?;
        if spec.kind.is_buffered() {
            return Err(SparrowError::new(ErrorCode::FeatureUnavailable,"buffered sliding/session windows use BufferedWindow through Kernel, not the legacy WindowOperator codec"));
        }
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
        let bound_aggs = spec
            .aggs
            .iter()
            .map(|a| match &a.input {
                Some(e) => bind(e, &input).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        let allocation = bound_aggs
            .iter()
            .flatten()
            .map(sparrow_expr::allocation::AllocationBound::for_expr)
            .collect();
        let variable_accs = spec.aggs.iter().any(|a| {
            matches!(
                a.func,
                sparrow_model::AggFn::Min
                    | sparrow_model::AggFn::Max
                    | sparrow_model::AggFn::First
                    | sparrow_model::AggFn::Last
            )
        });
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
        let accumulator_scratch_lease = if variable_accs {
            Some(
                owner.acquire(
                    CreditKind::Reservation,
                    spec.aggs
                        .len()
                        .saturating_mul(std::mem::size_of::<Accumulator>())
                        .saturating_add(64),
                )?,
            )
        } else {
            None
        };
        let accumulator_scratch =
            Vec::with_capacity(if variable_accs { spec.aggs.len() } else { 0 });
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
            default_input: InputId::SINGLE,
            external_watermarks: false,
            closed_index: BTreeMap::new(),
            bound_aggs,
            allocation,
            variable_accs,
            closed_index_bytes: 0,
            accumulator_scratch,
            accumulator_scratch_lease,
            hopping_clock_high: 0,
        })
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }

    /// Kernel scratch lifetime covers raw state keys, eager aggregate work,
    /// removed state and emitted rows until a billed RowBatch owns the output.
    /// Public raw Vec-returning helpers are not an owner-preserving API.
    pub(crate) fn working_credit(
        &self,
        batch: Option<&RowBatch>,
    ) -> Result<sparrow_model::MemoryLease> {
        let input = batch
            .map(|b| b.rows().iter().map(Row::resident_bytes).sum::<usize>())
            .unwrap_or(0);
        let rows = batch.map(RowBatch::num_rows).unwrap_or(0);
        let base = input
            .saturating_mul(4)
            .saturating_add(
                rows.saturating_mul(
                    self.output
                        .fields
                        .len()
                        .saturating_mul(96)
                        .saturating_add(128),
                ),
            )
            .saturating_add(
                self.input
                    .fields
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add((self.spec.max_overlap as usize).saturating_mul(32))
            .saturating_add(256);
        // Fixed-size accumulators need no retained variable-value scratch
        // (`touched` stays zero; only Min/Max carry variable-value bytes), so
        // the whole reservation is known before touching the owner: scan the
        // columns on the stack, fold the bound once, then take a single
        // reservation. `base` and the final saturating formula are unchanged,
        // including the `columns` term in `base`, even though the scan no
        // longer heap-allocates.
        if !self.variable_accs && self.input.fields.len() <= WORKING_CREDIT_MAX_FIELDS {
            if let Some(batch) = batch {
                let (allocation, values) =
                    fixed_size_column_estimate(&self.allocation, batch, self.input.fields.len());
                let total = base
                    .saturating_add(allocation.saturating_mul(2))
                    .saturating_add(values.saturating_mul(rows).saturating_mul(4));
                return self.owner.acquire(CreditKind::Reservation, total);
            }
        }
        let mut lease = self.owner.acquire(CreditKind::Reservation, base)?;
        if let Some(batch) = batch {
            let mut columns = vec![0usize; self.input.fields.len()];
            for row in batch.rows() {
                for (size, value) in columns.iter_mut().zip(&row.values) {
                    *size = (*size).max(value.resident_bytes());
                }
            }
            let mut allocation = 0usize;
            let mut values = 0usize;
            for bound in &self.allocation {
                let e = bound.estimate(&columns);
                allocation = allocation.saturating_add(e.allocated);
                values = values.saturating_add(e.value);
            }
            let mut touched = 0usize;
            // Only Count/PT may emit retained values while ingesting. ET closes
            // separately in owned, bounded chunks. Numeric state has fixed size.
            if self.variable_accs && !self.spec.kind.uses_event_time() {
                for row in batch.rows() {
                    let key =
                        StateKey::new(self.operator, StateSlotId::new(SLOT), self.group_key(row));
                    let bytes = match &self.store {
                        WindowStore::Count(s) => s
                            .get(&key)
                            .filter(|e| match self.spec.kind {
                                WindowKind::Count { size } => {
                                    e.count.saturating_add(rows as u64) >= size
                                }
                                _ => false,
                            })
                            .map_or(0, |e| e.accs.iter().map(Accumulator::tracked_bytes).sum()),
                        WindowStore::Tumble(s) => s
                            .get(&key)
                            .map_or(0, |e| e.accs.iter().map(Accumulator::tracked_bytes).sum()),
                    };
                    touched = touched.saturating_add(bytes);
                }
            }
            lease.grow_to(
                base.saturating_add(allocation.saturating_mul(2))
                    .saturating_add(values.saturating_mul(rows).saturating_mul(4))
                    .saturating_add(touched.saturating_mul(2)),
            )?;
        }
        Ok(lease)
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
        let state = match &self.store {
            WindowStore::Tumble(s) => s.retention_bytes(),
            WindowStore::Count(s) => s.retention_bytes(),
        };
        state.saturating_add(self.closed_index_bytes)
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
        let next = self.on_batch_without_timers(batch, now)?;
        out.finals.extend(next.finals);
        out.lates = next.lates;
        out.pending_close = next.pending_close;
        out.future_dropped = next.future_dropped;
        Ok(out)
    }

    /// Kernel drains due PT state separately; never materialize the keyspace
    /// into an unowned Vec before processing this input batch.
    pub(crate) fn on_batch_without_timers(
        &mut self,
        batch: &RowBatch,
        now: i64,
    ) -> Result<WindowEmission> {
        let mut out = WindowEmission::default();
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

    pub(crate) fn begin_due(&mut self, now: i64) -> bool {
        let now = self.hopping_now(now);
        if self.spec.kind.uses_event_time() {
            return false;
        }
        let mut due = false;
        while self.timers.pop_due(now).is_some() {
            due = true;
        }
        due
    }

    pub fn fire_due(&mut self, now: i64) -> Result<Vec<Row>> {
        let now = self.hopping_now(now);
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
        let now = self.hopping_now(now);
        if self.variable_accs && self.accumulator_scratch_lease.is_none() {
            self.accumulator_scratch_lease = Some(
                self.owner.acquire(
                    CreditKind::Reservation,
                    self.spec
                        .aggs
                        .len()
                        .saturating_mul(std::mem::size_of::<Accumulator>())
                        .saturating_add(64),
                )?,
            );
            self.accumulator_scratch = Vec::with_capacity(self.spec.aggs.len());
        }
        match self.spec.kind {
            WindowKind::TumblingProcessingTime { size_micros } => Ok(WindowEmission {
                finals: self.on_tumble_row(row, now, size_micros)?,
                ..WindowEmission::default()
            }),
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
            WindowKind::HoppingProcessingTime {
                size_micros,
                slide_micros,
            } => {
                let group = self.group_key(row);
                for (start, end) in
                    WindowKind::assign_hop(now, size_micros, slide_micros, self.spec.max_overlap)?
                {
                    self.upsert_et_window(&group, start, end, row)?;
                }
                Ok(WindowEmission::default())
            }
            _ => Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "buffered window requires its dedicated executor",
            )),
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
        if !self.external_watermarks {
            self.hub.observe_event(self.default_input, ts, now)?;
        }
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
            self.index_insert(&sk, end)?;
            if matches!(self.spec.kind, WindowKind::HoppingProcessingTime { .. }) {
                self.timers
                    .schedule(TimerId::window(self.operator, end), end)?;
            }
        }
        if let WindowStore::Tumble(store) = &mut self.store {
            if self.variable_accs {
                return store.update_accounted(&sk, |entry, lease, key_bytes| {
                    update_accs_metered(
                        &self.bound_aggs,
                        &self.spec,
                        &mut entry.accs,
                        &mut self.accumulator_scratch,
                        row,
                        lease,
                        key_bytes,
                    )
                });
            }
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&self.bound_aggs, &self.spec, &mut entry.accs, row)?;
            }
            // Numeric accumulators never grow; MIN/MAX already use a
            // pre-admitted replacement above. No post-update hash/re-bill.
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
        let Some(proposed_out) = self.holdback.as_mut().expect("holdback").on_wm_in(wm_in)? else {
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
                self.index_remove(&k, entry.window_end);
                let group = if self.suffixed_key() {
                    group_from_et_key(&k.key)
                } else {
                    k.key.clone()
                };
                return Ok(Some(emit_tumble(&group, &entry)));
            }
            self.closed_index.retain(|_, v| {
                let keep = v != &k;
                if !keep {
                    self.closed_index_bytes = self.owner.replace_accounted_bytes(
                        self.closed_index_bytes,
                        v.index_bytes(),
                        0,
                    );
                }
                keep
            });
        }
    }

    fn next_closed_key(&self, wm_out: i64) -> Option<StateKey> {
        match self.closed_index.first_key_value() {
            Some(((end, _), key)) if *end <= wm_out => Some(key.clone()),
            _ => None,
        }
    }

    fn index_insert(&mut self, key: &StateKey, window_end: i64) -> Result<()> {
        let indexed = key.indexed(&self.owner)?;
        let bytes = indexed.index_bytes();
        let old = self
            .closed_index
            .insert((window_end, key.encoded_bytes().to_vec()), indexed);
        self.closed_index_bytes = self.owner.replace_accounted_bytes(
            self.closed_index_bytes,
            old.as_ref().map_or(0, StateKey::index_bytes),
            bytes,
        );
        Ok(())
    }

    fn index_remove(&mut self, key: &StateKey, window_end: i64) {
        if let Some(old) = self
            .closed_index
            .remove(&(window_end, key.encoded_bytes().to_vec()))
        {
            self.closed_index_bytes =
                self.owner
                    .replace_accounted_bytes(self.closed_index_bytes, old.index_bytes(), 0);
        }
    }

    fn rebuild_closed_index(&mut self) -> Result<()> {
        self.closed_index.clear();
        self.closed_index_bytes = 0;
        if let WindowStore::Tumble(store) = &self.store {
            for (k, e) in store.iter() {
                let indexed = k.indexed(&self.owner)?;
                self.closed_index_bytes = self
                    .closed_index_bytes
                    .saturating_add(indexed.index_bytes());
                self.closed_index
                    .insert((e.window_end, k.encoded_bytes().to_vec()), indexed);
            }
        }
        Ok(())
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

    /// Count only the due prefix that fits one output chunk. Admit before key
    /// clones/removal, and convert to an owning batch before dropping scratch.
    pub(crate) fn take_closed_batch(
        &mut self,
        cut: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Option<RowBatch>> {
        let cut = if matches!(self.spec.kind, WindowKind::HoppingProcessingTime { .. }) {
            cut.max(self.hopping_clock_high)
        } else {
            cut
        };
        let mut work = 0usize;
        let mut wire = 0usize;
        let mut count = 0usize;
        if let WindowStore::Tumble(store) = &self.store {
            for ((end, _), key) in &self.closed_index {
                if *end > cut || count >= max_rows.max(1) {
                    break;
                }
                let Some(entry) = store.get(key) else {
                    return Err(SparrowError::new(
                        ErrorCode::Internal,
                        "closed index has no state entry",
                    ));
                };
                let bytes = entry
                    .accs
                    .iter()
                    .map(Accumulator::tracked_bytes)
                    .sum::<usize>()
                    .saturating_add(key.tracked_bytes())
                    .saturating_add(self.output.fields.len().saturating_mul(64))
                    .saturating_add(128);
                if count > 0 && wire.saturating_add(bytes) > max_bytes {
                    break;
                }
                wire = wire.saturating_add(bytes);
                work = work.saturating_add(bytes.saturating_mul(3));
                count += 1;
            }
        }
        if count == 0 {
            return Ok(None);
        }
        let _scratch = self
            .owner
            .acquire(CreditKind::Reservation, work.saturating_add(128))?;
        let rows = self.take_closed_chunk(cut, count, max_bytes)?;
        self.build_metered_batch(rows)
    }

    pub(crate) fn build_metered_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows_metered(&self.output, rows, &self.owner)
    }

    fn peek_closed_bytes(&self, wm_out: i64) -> Option<usize> {
        let key = self.next_closed_key(wm_out)?;
        match &self.store {
            WindowStore::Tumble(store) => store
                .get(&key)
                .map(|e| e.accs.iter().map(Accumulator::tracked_bytes).sum::<usize>() + 32),
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
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "event-time column missing on row",
            )
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
            self.index_insert(&sk, end)?;
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
            self.index_insert(&sk, end)?;
            self.timers
                .schedule(TimerId::window(self.operator, end), end)?;
        }
        if let WindowStore::Tumble(store) = &mut self.store {
            if self.variable_accs {
                store.update_accounted(&sk, |entry, lease, key_bytes| {
                    update_accs_metered(
                        &self.bound_aggs,
                        &self.spec,
                        &mut entry.accs,
                        &mut self.accumulator_scratch,
                        row,
                        lease,
                        key_bytes,
                    )
                })?;
                return Ok(late);
            }
            if let Some(entry) = store.get_mut(&sk) {
                update_accs(&self.bound_aggs, &self.spec, &mut entry.accs, row)?;
            }
        }
        Ok(late)
    }

    fn on_count_row(&mut self, row: &Row, size: u64) -> Result<Vec<Row>> {
        let key = self.group_key(row);
        let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), key);
        let missing = matches!(&self.store, WindowStore::Count(s) if s.get(&sk).is_none());
        if missing {
            let accs = empty_accs(&self.spec, &self.input)?;
            let bytes: usize = accs.iter().map(Accumulator::tracked_bytes).sum();
            if let WindowStore::Count(store) = &mut self.store {
                store.put(sk.clone(), CountEntry { count: 0, accs }, bytes)?;
            }
        }
        let mut emit_row = None;
        if let WindowStore::Count(store) = &mut self.store {
            let count;
            if self.variable_accs {
                store.update_accounted(&sk, |entry, lease, key_bytes| {
                    update_accs_metered(
                        &self.bound_aggs,
                        &self.spec,
                        &mut entry.accs,
                        &mut self.accumulator_scratch,
                        row,
                        lease,
                        key_bytes,
                    )?;
                    entry.count += 1;
                    Ok(())
                })?;
                count = store.get(&sk).map_or(0, |entry| entry.count);
            } else if let Some(entry) = store.get_mut(&sk) {
                update_accs(&self.bound_aggs, &self.spec, &mut entry.accs, row)?;
                entry.count += 1;
                count = entry.count;
            } else {
                count = 0;
            }
            if count >= size {
                if let Some(entry) = store.remove(&sk) {
                    emit_row = Some(emit_count(&sk.key, &entry));
                }
            }
        }
        Ok(emit_row.into_iter().collect())
    }

    fn flush_tumble_ends(&mut self, ends: &HashSet<i64>) -> Result<Vec<Row>> {
        let mut out = Vec::new();
        let mut ends: Vec<_> = ends.iter().copied().collect();
        ends.sort_unstable();
        for end in ends {
            loop {
                let key = self
                    .closed_index
                    .range((end, Vec::new())..)
                    .next()
                    .filter(|((at, _), _)| *at == end)
                    .map(|(_, k)| k.clone());
                let Some(key) = key else {
                    break;
                };
                let entry = match &mut self.store {
                    WindowStore::Tumble(s) => s.remove(&key),
                    _ => None,
                };
                self.index_remove(&key, end);
                if let Some(entry) = entry {
                    let group = if self.suffixed_key() {
                        group_from_et_key(&key.key)
                    } else {
                        key.key.clone()
                    };
                    out.push(emit_tumble(&group, &entry));
                }
            }
        }
        Ok(out)
    }

    fn group_key(&self, row: &Row) -> Vec<Scalar> {
        // This is a short-lived key view while the input batch is borrowed.
        // StateKey::new performs the owning detach; copying here as well
        // allocated every UTF-8 key twice per input row.
        self.group_idx
            .iter()
            .map(|&i| row.values[i].clone())
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
        self.closed_index_bytes = 0;
        self.timers.cancel_all();
        self.accumulator_scratch = Vec::new();
        self.accumulator_scratch_lease = None;
    }

    pub fn max_state_keys(&self) -> usize {
        match &self.store {
            WindowStore::Tumble(s) => s.max_keys(),
            WindowStore::Count(s) => s.max_keys(),
        }
    }

    /// Participant codec selected by the aggregate list: codec 3 only when an
    /// extended aggregate is present, so existing plans keep codec 1 bytes.
    pub(crate) fn accumulator_codec(&self) -> crate::aggregate::AccumulatorCodec {
        if self.spec.has_extended_aggs() {
            crate::aggregate::AccumulatorCodec::WindowExt
        } else {
            crate::aggregate::AccumulatorCodec::Window
        }
    }

    /// Estimated encoded freeze body (keys + accs + per-entry headers).
    /// Walks live state by reference — does not clone entries. A value that
    /// has no durable encoding makes the estimate fail closed (never zero).
    pub fn estimated_freeze_bytes(&self) -> usize {
        self.try_estimated_freeze_bytes()
            .unwrap_or(crate::checkpoint::MAX_SNAPSHOT_BYTES as usize + 1)
    }

    pub(crate) fn try_estimated_freeze_bytes(&self) -> Result<usize> {
        const ENTRY_OVERHEAD: usize = 2 + 8 + 8 + 8 + 2;
        let codec = self.accumulator_codec();
        let entry = |key: &[Scalar], accs: &[Accumulator]| -> Result<usize> {
            let mut total = ENTRY_OVERHEAD;
            for v in key {
                total = total.saturating_add(v.encoded_value_len()?);
            }
            for a in accs {
                total = total.saturating_add(a.encoded_len_codec(codec)?);
            }
            Ok(total)
        };
        let mut total = 0usize;
        match &self.store {
            WindowStore::Tumble(s) => {
                for (k, e) in s.iter() {
                    total = total.saturating_add(entry(&k.key, &e.accs)?);
                }
            }
            WindowStore::Count(s) => {
                for (k, e) in s.iter() {
                    total = total.saturating_add(entry(&k.key, &e.accs)?);
                }
            }
        }
        Ok(total)
    }

    pub(crate) fn freeze_workspace_bytes(&self) -> usize {
        self.key_count()
            .saturating_mul(2 * std::mem::size_of::<usize>())
            .saturating_add(64)
    }

    /// Fail closed before encode / CURRENT publish when the freeze would
    /// exceed entry/snapshot caps or either memory class. This is a structural
    /// bound; EncodedFreeze atomically acquires current reservation headroom
    /// before allocating. Do not re-check headroom after that lease is acquired.
    pub fn check_freeze_encode_bound(&self, max_entries: usize) -> Result<()> {
        let n = self.key_count();
        if n > max_entries {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "freeze entry count {n} exceeds max_state_keys {max_entries}; refusing encode (would publish unrecoverable CURRENT)"
                ),
            ));
        }
        let estimate = self.try_estimated_freeze_bytes()?.saturating_add(256);
        if estimate as u64 > crate::checkpoint::MAX_SNAPSHOT_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "estimated freeze {estimate}B exceeds {}B snapshot quota; refusing encode before CURRENT",
                    crate::checkpoint::MAX_SNAPSHOT_BYTES
                ),
            ));
        }
        let live = self.retention_bytes();
        let budget = self.owner.budget();
        let working = estimate.saturating_add(self.freeze_workspace_bytes());
        if live > budget.retention_bytes || working > budget.reservation_bytes {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "freeze encode requires live {live}B / retention {}B and working {working}B (encoded {estimate}B) / reservation {}B; refusing encode before CURRENT",
                    budget.retention_bytes, budget.reservation_bytes
                ),
            ));
        }
        Ok(())
    }

    /// Incremental freeze encode: sort key refs only, write each live entry
    /// into `out`. Peak is live retention + the output buffer — no
    /// `Vec<FrozenEntry>` clone of all accs/keys (P1-14).
    pub fn encode_freeze_into(&self, out: &mut Vec<u8>, max_entries: usize) -> Result<()> {
        self.encode_freeze_into_codec(out, max_entries, self.accumulator_codec())
    }

    pub(crate) fn encode_freeze_into_codec(
        &self,
        out: &mut Vec<u8>,
        max_entries: usize,
        codec: crate::aggregate::AccumulatorCodec,
    ) -> Result<()> {
        self.check_freeze_encode_bound(max_entries)?;
        let n = self.key_count();
        // Sorted references are temporary workspace, not encoded payload.
        let _sort_credit = self
            .owner
            .acquire(CreditKind::Reservation, self.freeze_workspace_bytes())?;
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&StateSlotId::new(SLOT).raw().to_le_bytes());
        let kind = match &self.store {
            WindowStore::Tumble(_) => 0u8,
            WindowStore::Count(_) => 1u8,
        };
        out.push(kind);
        out.extend_from_slice(&(n as u32).to_le_bytes());
        match &self.store {
            WindowStore::Tumble(store) => {
                let mut keys: Vec<&crate::state::StateKey> = store.keys().collect();
                keys.sort_by(|a, b| a.encoded_bytes().cmp(b.encoded_bytes()));
                for k in keys {
                    let e = store.get(k).expect("freeze key");
                    write_freeze_entry(out, &k.key, e.window_start, e.window_end, 0, &e.accs, codec)?;
                }
            }
            WindowStore::Count(store) => {
                let mut keys: Vec<&crate::state::StateKey> = store.keys().collect();
                keys.sort_by(|a, b| a.encoded_bytes().cmp(b.encoded_bytes()));
                for k in keys {
                    let e = store.get(k).expect("freeze key");
                    write_freeze_entry(out, &k.key, 0, 0, e.count, &e.accs, codec)?;
                }
            }
        }
        write_opt_i64(self.wm_in(), out);
        write_opt_i64(self.wm_out(), out);
        write_opt_i64(self.hub.last_effective(), out);
        Ok(())
    }

    /// Materialize a [`WindowFreeze`] after the encode bound check.
    /// Residual (P1-14): this still clones entries. The aligned session
    /// encode path uses [`Self::encode_freeze_into`] instead.
    pub fn try_freeze(&self) -> Result<WindowFreeze> {
        self.check_freeze_encode_bound(self.max_state_keys())?;
        Ok(self.freeze())
    }

    /// Freeze open keyed state. Prefer [`Self::encode_freeze_into`] /
    /// [`Self::try_freeze`] on the publish path so oversized freezes fail
    /// closed before CURRENT.
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
    /// Public/legacy entry: extended aggregates are restorable only through
    /// the participant (codec 3, v29/v30) path.
    pub fn restore_freeze(&mut self, freeze: &WindowFreeze) -> Result<()> {
        if self.spec.kind.is_new_window() || self.spec.has_extended_aggs() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "new window families have no published restore codec/profile",
            ));
        }
        self.restore_freeze_inner(freeze)
    }

    /// K1 participant restore after `validate_participant_restore`.
    pub(crate) fn restore_participant_freeze(&mut self, freeze: &WindowFreeze) -> Result<()> {
        if self.spec.kind.is_new_window() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "new window families have no published restore codec/profile",
            ));
        }
        self.restore_freeze_inner(freeze)
    }

    fn restore_freeze_inner(&mut self, freeze: &WindowFreeze) -> Result<()> {
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
        // Admit copied keys/encoded indexes/accumulator containers before any
        // state mutation. Decoded input ownership is a separate handoff lease.
        let _scratch = self
            .owner
            .acquire(CreditKind::Reservation, freeze.restore_scratch_bytes())?;
        self.cleanup();
        match (&mut self.store, freeze.kind) {
            (WindowStore::Tumble(store), 0) => {
                for e in &freeze.entries {
                    let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), e.key.clone());
                    if store.get(&sk).is_some() {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "checkpoint contains duplicate canonical keys; reset/replay required",
                        ));
                    }
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
                    if store.get(&sk).is_some() {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "checkpoint contains duplicate canonical keys; reset/replay required",
                        ));
                    }
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
        self.rebuild_closed_index()?;
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
                .map(|e| e
                    .accs
                    .iter()
                    .map(|a| format!("{:?}", a.finish()))
                    .collect::<Vec<_>>()
                    .join("+"))
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

impl WindowFreeze {
    /// Entries are rebuilt sequentially and immediately charged to retention.
    /// Only one candidate key/value (plus bounded timer bookkeeping) needs
    /// scratch; reserving three whole snapshots made valid checkpoints fail
    /// under the very same job budget on restart.
    fn restore_scratch_bytes(&self) -> usize {
        let largest = self
            .entries
            .iter()
            .map(|e| {
                e.key
                    .iter()
                    .map(Scalar::resident_bytes)
                    .sum::<usize>()
                    .saturating_add(e.accs.iter().map(Accumulator::tracked_bytes).sum::<usize>())
                    .saturating_add(256)
            })
            .max()
            .unwrap_or(0);
        largest
            .saturating_mul(3)
            .saturating_add(self.entries.len().saturating_mul(16))
            .saturating_add(1024)
    }
    pub fn resident_bytes(&self) -> usize {
        self.entries.iter().fold(
            128usize.saturating_add(
                self.entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<FrozenEntry>()),
            ),
            |total, e| {
                total
                    .saturating_add(e.key.iter().map(Scalar::resident_bytes).sum::<usize>())
                    .saturating_add(
                        e.key
                            .capacity()
                            .saturating_sub(e.key.len())
                            .saturating_mul(std::mem::size_of::<Scalar>()),
                    )
                    .saturating_add(e.accs.iter().map(Accumulator::tracked_bytes).sum::<usize>())
                    .saturating_add(
                        e.accs
                            .capacity()
                            .saturating_sub(e.accs.len())
                            .saturating_mul(std::mem::size_of::<Accumulator>()),
                    )
                    .saturating_add(128)
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenEntry {
    pub key: Vec<Scalar>,
    pub window_start: i64,
    pub window_end: i64,
    pub count: u64,
    pub accs: Vec<Accumulator>,
}

pub(crate) fn write_freeze_entry(
    out: &mut Vec<u8>,
    key: &[Scalar],
    window_start: i64,
    window_end: i64,
    count: u64,
    accs: &[Accumulator],
    codec: crate::aggregate::AccumulatorCodec,
) -> Result<()> {
    out.extend_from_slice(&(key.len() as u16).to_le_bytes());
    for s in key {
        s.encode_value(out)?;
    }
    out.extend_from_slice(&window_start.to_le_bytes());
    out.extend_from_slice(&window_end.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&(accs.len() as u16).to_le_bytes());
    for a in accs {
        a.encode_codec(out, codec)?;
    }
    Ok(())
}

fn write_opt_i64(v: Option<i64>, out: &mut Vec<u8>) {
    match v {
        None => out.push(0),
        Some(x) => {
            out.push(1);
            out.extend_from_slice(&x.to_le_bytes());
        }
    }
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

fn update_accs(
    bound_aggs: &[Option<BoundExpr>],
    spec: &WindowSpec,
    accs: &mut [Accumulator],
    row: &Row,
) -> Result<()> {
    for (i, (acc, call)) in accs.iter_mut().zip(spec.aggs.iter()).enumerate() {
        let v = if call.count_star {
            Scalar::Null
        } else if let Some(expr) = bound_aggs.get(i).and_then(|b| b.as_ref()) {
            eval_bound(expr, &row.values)?
        } else {
            Scalar::Null
        };
        acc.update(&v)?;
    }
    Ok(())
}

/// Reuse the candidate Vec; unchanged MIN/MAX do not detach payloads, acquire
/// replacement retention or allocate a Vec on every row. Numeric siblings and
/// COUNT still update. Any expression/growth failure preserves the old entry.
fn update_accs_metered(
    bound: &[Option<BoundExpr>],
    spec: &WindowSpec,
    accs: &mut Vec<Accumulator>,
    scratch: &mut Vec<Accumulator>,
    row: &Row,
    lease: &mut sparrow_model::MemoryLease,
    key_bytes: usize,
) -> Result<()> {
    scratch.clear();
    if accs.len() != spec.aggs.len() || scratch.capacity() < accs.len() {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "accumulator shape exceeds admitted scratch",
        ));
    }
    scratch.extend(accs.iter().cloned());
    let result = (|| {
        update_accs(bound, spec, scratch, row)?;
        let bytes = key_bytes
            .saturating_add(
                scratch
                    .iter()
                    .map(Accumulator::tracked_bytes)
                    .sum::<usize>(),
            )
            .max(1);
        lease.grow_to(bytes)?;
        std::mem::swap(accs, scratch);
        scratch.clear(); // release old payload BEFORE refund
        lease.shrink_to(bytes)?;
        Ok(())
    })();
    scratch.clear();
    result
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
/// (`count_start = 0`, `count_end = count`, and `count == size` at emit).
/// They are deliberately named differently from PT/ET time bounds.
pub const COUNT_WINDOW_BOUNDS: &str =
    "count-window count_start/count_end are 0-based half-open arrival ordinals [0, count) within the closed window, not event-time micros";

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
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("unknown group key '{k}'"),
            )
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

pub(crate) fn finish_rows_metered(
    schema: &Schema,
    rows: Vec<Row>,
    owner: &Arc<MemoryOwner>,
) -> Result<Option<RowBatch>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema.clone()),
        owner.clone(),
        CreditKind::Reservation,
        rows.len(),
        owner.budget().reservation_bytes,
    )?;
    for row in rows {
        let bytes = row.resident_bytes().saturating_add(64);
        builder.push_accounted(row, bytes)?;
    }
    Ok(Some(builder.finish()?))
}

#[cfg(test)]
mod review_tests {
    use super::*;
    use sparrow_expr::Expr;
    use sparrow_model::{
        AggFn, DataType, Field, FieldId, ResourceBudget, Scalar, SchemaId, WindowKind,
    };
    use sparrow_plan::AggCall;

    #[test]
    fn r10_live_scratch_does_not_reserve_the_entire_keyspace() {
        let budget = ResourceBudget {
            reservation_bytes: 1024 * 1024,
            retention_bytes: 1024 * 1024,
            ..ResourceBudget::compact()
        };
        let owner = MemoryOwner::new(budget);
        let schema = Schema::new(
            1,
            vec![
                Field::new(1, "k", DataType::Utf8, false),
                Field::new(2, "v", DataType::Int64, false),
            ],
        )
        .unwrap();
        let spec = WindowSpec::new(
            WindowKind::count(100_000).unwrap(),
            vec!["k".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        );
        let mut window =
            WindowOperator::new(1.into(), spec, schema.clone(), owner.clone(), 1024, 2048).unwrap();
        for n in 0..1024 {
            let mut builder = RowBatchBuilder::new(
                Arc::new(schema.clone()),
                owner.clone(),
                CreditKind::Reservation,
                1,
                4096,
            )
            .unwrap();
            builder
                .push(Row {
                    values: vec![
                        Scalar::utf8(format!("{n:04}{}", "k".repeat(300))),
                        Scalar::Int64(1),
                    ],
                })
                .unwrap();
            let batch = builder.finish().unwrap();
            let scratch = window
                .working_credit(Some(&batch))
                .expect("unrelated resident keys must not consume per-batch scratch");
            window.on_batch(&batch, 0).unwrap();
            drop(scratch);
        }
        assert_eq!(window.key_count(), 1024);
        assert!(window.retention_bytes() > budget.retention_bytes / 2);
        assert!(window.retention_bytes() < budget.retention_bytes);
        window.cleanup();
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn r10_minmax_retention_tracks_state_not_unrelated_input_width() {
        let owner = MemoryOwner::new(ResourceBudget {
            reservation_bytes: 1024 * 1024,
            retention_bytes: 1024 * 1024,
            ..ResourceBudget::compact()
        });
        let schema = Schema::new(
            1,
            vec![
                Field::new(1, "k", DataType::Int64, false),
                Field::new(2, "v", DataType::Utf8, false),
                Field::new(3, "ignored", DataType::Utf8, false),
            ],
        )
        .unwrap();
        let spec = WindowSpec::new(
            WindowKind::count(100_000).unwrap(),
            vec!["k".into()],
            vec![AggCall::new(
                AggFn::Min,
                Some(Expr::Column { name: "v".into() }),
                "m",
            )],
        );
        let mut window =
            WindowOperator::new(1.into(), spec, schema.clone(), owner.clone(), 1024, 2048).unwrap();
        for round in 0..3 {
            for key in 0..128 {
                let mut builder = RowBatchBuilder::new(
                    Arc::new(schema.clone()),
                    owner.clone(),
                    CreditKind::Reservation,
                    1,
                    4096,
                )
                .unwrap();
                builder
                    .push(Row {
                        values: vec![
                            Scalar::Int64(key),
                            Scalar::utf8(if round == 0 { "a" } else { "z" }),
                            Scalar::utf8("x".repeat(1024)),
                        ],
                    })
                    .unwrap();
                let batch = builder.finish().unwrap();
                let _scratch = window.working_credit(Some(&batch)).unwrap();
                window.on_batch(&batch, 0).unwrap();
            }
            assert!(
                window.retention_bytes() < 128 * 1024,
                "a one-byte MIN must not retain an input-width worst-case lease"
            );
        }
        assert!(window
            .freeze()
            .entries
            .iter()
            .all(|e| e.accs[0].finish() == Scalar::utf8("a")));
        window.cleanup();
        drop(window);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn self_review_checkpoint_that_fits_live_budget_can_restore_same_budget() {
        let budget = ResourceBudget {
            reservation_bytes: 64 * 1024,
            retention_bytes: 64 * 1024,
            ..ResourceBudget::compact()
        };
        let owner = MemoryOwner::new(budget);
        let schema = Schema::new(1, vec![Field::new(1, "v", DataType::Int64, false)]).unwrap();
        let spec = WindowSpec::new(
            WindowKind::count(128).unwrap(),
            vec!["v".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        );
        let mut live = WindowOperator::new(
            1.into(),
            spec.clone(),
            schema.clone(),
            owner.clone(),
            256,
            256,
        )
        .unwrap();
        let mut found = false;
        for v in 0..256 {
            let mut b = RowBatchBuilder::new(
                Arc::new(schema.clone()),
                owner.clone(),
                CreditKind::Reservation,
                1,
                512,
            )
            .unwrap();
            b.push(Row {
                values: vec![Scalar::Int64(v)],
            })
            .unwrap();
            let batch = b.finish().unwrap();
            {
                let _scratch = live.working_credit(Some(&batch)).unwrap();
                live.on_batch(&batch, 0).unwrap();
            }
            drop(batch);
            // Decode uses exact-length vectors, not live freeze's growth capacity.
            let freeze = live.freeze().clone();
            if freeze.resident_bytes() * 4 + 1024 > budget.reservation_bytes {
                drop(crate::barrier::EncodedFreeze::from_operator(&live, &owner, 256).unwrap());
                let target = MemoryOwner::new(budget);
                let adopted = crate::barrier::RuntimeAligned::adopt(
                    crate::AlignedJob {
                        restore: Some(freeze.clone()),
                        pipeline: None,
                        acks: Default::default(),
                        outbox: Arc::new(sparrow_model::InflightCounter::new()),
                    },
                    &target,
                )
                .unwrap();
                let restored = adopted.restore.lock().unwrap().take().unwrap();
                assert!(
                    target
                        .acquire(
                            CreditKind::Reservation,
                            restored.freeze.resident_bytes() * 3 + 1024
                        )
                        .is_err(),
                    "fixture must reproduce the previous whole-snapshot overcharge"
                );
                let mut window = WindowOperator::new(
                    1.into(),
                    spec.clone(),
                    schema.clone(),
                    target.clone(),
                    256,
                    256,
                )
                .unwrap();
                window.restore_freeze(&restored.freeze).expect(
                    "a successful live checkpoint must not require four whole snapshots to restore",
                );
                assert_eq!(window.freeze(), freeze);
                drop(restored);
                window.cleanup();
                assert_eq!(target.usage().physical_bytes, 0);
                found = true;
                break;
            }
        }
        assert!(
            found,
            "fixture must exercise the previous full-snapshot scratch threshold"
        );
    }

    #[test]
    fn r3_closed_window_index_is_billed_and_released() {
        let mut window = op();
        let owner = Arc::clone(&window.owner);
        window
            .on_row(
                &Row {
                    values: vec![Scalar::utf8("indexed-key"), Scalar::Int64(1)],
                },
                0,
            )
            .unwrap();
        let index_bytes: usize = window
            .closed_index
            .values()
            .map(StateKey::index_bytes)
            .sum();
        assert!(index_bytes > 0);
        assert_eq!(window.retention_bytes(), owner.usage().retention_bytes);
        window.cleanup();
        assert_eq!(owner.usage().retention_bytes, 0);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

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
        assert!(
            rounds > 1,
            "32 keys must not flush as a single mailbox burst"
        );
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
        assert!(w.output_schema().field_by_name("count_start").is_some());
        assert!(w.output_schema().field_by_name("count_end").is_some());
        assert!(w.output_schema().field_by_name("window_start").is_none());
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
        assert_eq!(
            row.values[2],
            Scalar::Int64(2),
            "window_end is in-window count"
        );
        assert_eq!(row.values[3], Scalar::Int64(3));
    }

    #[test]
    fn n11_many_keys_close_in_deterministic_order() {
        const N: usize = 256;
        let mut entries = Vec::with_capacity(N);
        for i in 0..N {
            let end = 10 + ((i % 7) as i64) * 10;
            entries.push(FrozenEntry {
                // PT keys are group values only; ET additionally stores start.
                key: vec![Scalar::utf8(&format!("k{i:03}"))],
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
        let expected_pairs: Vec<(i64, String)> = expected
            .into_iter()
            .map(|(end, _, name)| (end, name))
            .collect();
        assert_eq!(
            got, expected_pairs,
            "closed keys must emit in (window_end, encoded_key) order"
        );
        assert!(w.take_closed_chunk(100, 16, 4096).unwrap().is_empty());
        assert_eq!(w.key_count(), 0);
    }
}

#[cfg(test)]
#[path = "window_credit_tests.rs"]
mod credit_tests;

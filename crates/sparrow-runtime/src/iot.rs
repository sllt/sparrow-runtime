//! Bounded IoT state operators.
//!
//! Change, Deadband and Hysteresis are task-owned keyed operators over
//! first-class scalar values. Hysteresis stores a typed Bool latch rather
//! than reusing a numeric deadband baseline.
//! Values which enter state are detached, and every state/index replacement is
//! admitted before the old entry is changed.  Processing-time TTL is useful
//! for a live job; aligned Change/Deadband TTL uses kind 9/10 in the paused
//! time profile, preserving its last-valid-input cut rather than host time.

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::Arc;

use sparrow_model::{
    CreditKind, DataType, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Row, RowBatch,
    Scalar, Schema, SparrowError, StateSlotId,
};
use sparrow_plan::{DeadbandBaseline, DeadbandMode, InvalidValuePolicy, IotSpec};

use crate::state::{MemoryState, StateKey};

/// Stable state slot reserved for the first IoT operators.
pub const IOT_STATE_SLOT: StateSlotId = StateSlotId::new(3);
/// Freeze kind for [`IotSpec`] without a deadband.
pub const IOT_CHANGE_KIND: u8 = 4;
/// Freeze kind for [`IotSpec`] with a deadband.
pub const IOT_DEADBAND_KIND: u8 = 5;
pub const IOT_HYSTERESIS_KIND: u8 = 6;
/// A freeze is bounded independently of the outer snapshot envelope.
pub const MAX_IOT_FREEZE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_IOT_FREEZE_ENTRIES: usize = 16 * 1024;

const MAX_IOT_ARITY: usize = 64;

fn validate_timed_metadata(kind:u8,start:i64,deadline:i64,trailing:bool,now:Option<i64>) -> Result<()> {
    if start<0 || (deadline != -1 && deadline<=start)
        || (kind==7 && trailing!=(deadline!=-1)) || (kind==8 && deadline==-1)
        || now.is_some_and(|now| start>now || (deadline!=-1 && deadline<=now)) {
        return Err(codec("timed IoT state disagrees with its processing-time cut"));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct StoredIotState {
    /// Deadband stores the comparison baseline.  LastInput advances this on
    /// every valid row; LastOutput advances it only when a row is emitted.
    values: Vec<Scalar>,
    /// Processing-time last valid input.  TTL=0 still stores it so the state
    /// shape is stable if a future non-aligned attempt enables expiry.
    last_seen: i64,
}

/// Runtime counters for one IoT operator.  `filtered_rows` includes invalid
/// rows ignored by policy; `invalid_rows` is the separately attributable
/// subset.  Neither counter means that state or TTL was refreshed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IotStats {
    pub input_rows: u64,
    pub emitted_rows: u64,
    pub filtered_rows: u64,
    pub invalid_rows: u64,
    pub expired_keys: u64,
    pub notifications_expired: u64,
    pub notifications_cancelled: u64,
    pub notifications_deferred: u64,
}

/// A detached, schema-independent state entry used by the IoT freeze codec.
#[derive(Clone, Debug, PartialEq)]
pub struct IotEntry {
    pub key: Vec<Scalar>,
    pub values: Vec<Scalar>,
}

/// IoT state frame.  The frame is embedded in the outer snapshot v6 envelope;
/// it therefore intentionally has no magic prefix of its own.  Its fixed
/// header is the same 11-byte state frame shape used by the existing runtime:
/// `u32 operator | u16 slot(3) | u8 kind(4/5/6) | u32 entry_count`.
/// Kind 6 requires a newer outer profile; it never upgrades v6/v7 in place.
#[derive(Clone, Debug, PartialEq)]
pub struct IotFreeze {
    pub operator: OperatorId,
    pub slot: StateSlotId,
    pub kind: u8,
    pub entries: Vec<IotEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IotFreezeHeader {
    pub operator: OperatorId,
    pub slot: StateSlotId,
    pub kind: u8,
    pub entries: usize,
}

impl IotFreeze {
    pub fn new(operator: OperatorId, kind: u8, entries: Vec<IotEntry>) -> Result<Self> {
        let freeze = Self {
            operator,
            slot: IOT_STATE_SLOT,
            kind,
            entries,
        };
        freeze.validate_shape(MAX_IOT_FREEZE_ENTRIES)?;
        Ok(freeze)
    }

    /// Encode using the largest supported state bound.  The operator path
    /// should pass its actual `max_keys` to [`Self::encode_into`].
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.encode_into(&mut out, MAX_IOT_FREEZE_ENTRIES)?;
        Ok(out)
    }

    /// Append one complete IoT frame to `out`, refusing oversized or
    /// non-canonical state before publishing it to an outer snapshot.
    pub fn encode_into(&self, out: &mut Vec<u8>, max_keys: usize) -> Result<()> {
        self.validate_shape(max_keys)?;
        let start = out.len();
        ensure_frame_bytes(start, out.len(), 11)?;
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&self.slot.raw().to_le_bytes());
        out.push(self.kind);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        let mut keyed = self
            .entries
            .iter()
            .map(|entry| Ok((encoded_key(&entry.key)?, entry)))
            .collect::<Result<Vec<_>>>()?;
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        for pair in keyed.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(codec("duplicate IoT freeze key"));
            }
        }
        for (_, entry) in keyed {
            encode_entry(out, entry, start)?;
            if out.len().saturating_sub(start) > MAX_IOT_FREEZE_BYTES {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "IoT freeze exceeds 8 MiB",
                ));
            }
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8], max_keys: usize) -> Result<Self> {
        let mut src = bytes;
        let freeze = Self::decode_mode(&mut src, max_keys, true)?;
        if !src.is_empty() {
            return Err(codec("trailing bytes after IoT freeze"));
        }
        Ok(freeze)
    }

    /// Decode the frame at the front of `src`.  `materialize=false` still
    /// scans every scalar and detects duplicate keys, but does not allocate
    /// decoded `Scalar` values; the caller may use it for bounded admission.
    pub fn decode_mode(src: &mut &[u8], max_keys: usize, materialize: bool) -> Result<Self> {
        Self::decode_at_cut(src, max_keys, materialize, None)
    }

    pub(crate) fn decode_at_cut(src: &mut &[u8], max_keys: usize, materialize: bool, now: Option<i64>) -> Result<Self> {
        if src.len() > MAX_IOT_FREEZE_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze input exceeds 8 MiB",
            ));
        }
        let header = Self::header(src)?;
        if max_keys == 0 || header.entries > max_keys || header.entries > MAX_IOT_FREEZE_ENTRIES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze entry count exceeds the state bound",
            ));
        }
        let input_len = src.len();
        *src = &src[11..];
        let mut entries = Vec::with_capacity(if materialize { header.entries } else { 0 });
        let mut seen = BTreeSet::new();
        for _ in 0..header.entries {
            let key_len = take_u16(src, "IoT freeze key arity")? as usize;
            if key_len == 0 || key_len > MAX_IOT_ARITY {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "IoT freeze key arity must be between 1 and 64",
                ));
            }
            let mut key = Vec::with_capacity(if materialize { key_len } else { 0 });
            let mut key_bytes = Vec::new();
            for _ in 0..key_len {
                if materialize {
                    let value = Scalar::decode_value(src)?;
                    validate_freeze_scalar(&value, true)?;
                    value.encode_key(&mut key_bytes);
                    key_bytes.push(0xff);
                    key.push(value);
                } else {
                    let before = *src;
                    Scalar::skip_encoded_value(src)?;
                    let consumed = before.len().saturating_sub(src.len());
                    validate_encoded_scalar(&before[..consumed], true)?;
                    key_bytes.extend_from_slice(&before[..consumed]);
                    key_bytes.push(0xff);
                }
                ensure_decoded_size(input_len.saturating_sub(src.len()))?;
            }
            if !seen.insert(key_bytes) {
                return Err(codec("duplicate IoT freeze key"));
            }
            let value_len = take_u16(src, "IoT freeze value arity")? as usize;
            if value_len == 0 || value_len > MAX_IOT_ARITY
                || (header.kind == IOT_HYSTERESIS_KIND && value_len != 1) {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "IoT freeze value arity is invalid",
                ));
            }
            let timed = matches!(header.kind,7|8);
            let ttl = matches!(header.kind,9|10);
            let mut values = Vec::with_capacity(if materialize { value_len } else { 0 });
            let mut prefix = 0;
            if header.kind == crate::alarm_iot::KIND {
                if value_len <= crate::alarm_iot::PREFIX || src.len() < 54 {
                    return Err(codec("alarm state metadata encoding"));
                }
                let mut metadata = std::array::from_fn::<_, 7, _>(|_| Scalar::Null);
                for i in 0..6 {
                    let expected = if i == 0 || i == 3 { 3 } else { 2 };
                    if src[i * 9] != expected { return Err(codec("alarm metadata scalar tag")); }
                    let bytes: [u8; 8] = src[i * 9 + 1..i * 9 + 9].try_into().unwrap();
                    metadata[i] = if expected == 3 { Scalar::UInt64(u64::from_le_bytes(bytes)) } else { Scalar::Int64(i64::from_le_bytes(bytes)) };
                }
                crate::alarm_iot::metadata(&metadata, now)?;
                if materialize { values.extend(metadata.into_iter().take(6)); }
                *src = &src[54..]; prefix = 6;
            }
            if ttl {
                if value_len < 2 || src.len() < 9 || src[0] != 2 {
                    return Err(codec("TTL IoT freeze requires last_seen followed by values"));
                }
                let last_seen = i64::from_le_bytes(src[1..9].try_into().unwrap());
                if last_seen < 0 || now.is_some_and(|now| last_seen > now) {
                    return Err(codec("TTL last_seen exceeds its processing-time cut"));
                }
                if materialize { values.push(Scalar::Int64(last_seen)); }
                *src = &src[9..]; prefix = 1;
            }
            if timed {
                if value_len < 4 || src.len()<20 || src[0]!=2 || src[9]!=2 || src[18]!=1 || src[19]>1 {
                    return Err(codec("timed IoT freeze metadata encoding"));
                }
                let start = i64::from_le_bytes(src[1..9].try_into().unwrap());
                let deadline = i64::from_le_bytes(src[10..18].try_into().unwrap());
                let trailing = src[19]!=0;
                validate_timed_metadata(header.kind,start,deadline,trailing,now)?;
                if materialize { values.extend([Scalar::Int64(start),Scalar::Int64(deadline),Scalar::Bool(trailing)]); }
                *src = &src[20..]; prefix = 3;
            }
            for _ in prefix..value_len {
                if header.kind == IOT_HYSTERESIS_KIND {
                    // Bool tag 1 is a fixed-width scalar. Check before decode
                    // so header-only validation never allocates another type.
                    if src.len() < 2 || src[0] != 1 || src[1] > 1 {
                        return Err(codec("hysteresis freeze requires a Bool latch"));
                    }
                    let value = Scalar::Bool(src[1] != 0);
                    *src = &src[2..];
                    if materialize { values.push(value); }
                    ensure_decoded_size(input_len.saturating_sub(src.len()))?;
                    continue;
                }
                if materialize {
                    let value = Scalar::decode_value(src)?;
                    if !(matches!(header.kind,7|8|11) && value.is_null()) {
                        validate_freeze_scalar(&value, false)?;
                    }
                    values.push(value);
                } else {
                    let before = *src;
                    Scalar::skip_encoded_value(src)?;
                    let consumed = before.len().saturating_sub(src.len());
                    if !(matches!(header.kind,7|8|11) && before[..consumed]==[0]) {
                        validate_encoded_scalar(&before[..consumed], false)?;
                    }
                }
                ensure_decoded_size(input_len.saturating_sub(src.len()))?;
            }
            if materialize {
                entries.push(IotEntry { key, values });
            }
        }
        Ok(Self {
            operator: header.operator,
            slot: header.slot,
            kind: header.kind,
            entries,
        })
    }

    pub fn header(src: &[u8]) -> Result<IotFreezeHeader> {
        if src.len() < 11 {
            return Err(codec("truncated IoT freeze header"));
        }
        let operator = OperatorId::new(u32::from_le_bytes(src[..4].try_into().unwrap()));
        let slot = StateSlotId::new(u16::from_le_bytes(src[4..6].try_into().unwrap()));
        let kind = src[6];
        let entries = u32::from_le_bytes(src[7..11].try_into().unwrap()) as usize;
        if slot != IOT_STATE_SLOT {
            return Err(codec("IoT freeze StateSlotId is not 3"));
        }
        if !matches!(kind, 4..=11) {
            return Err(codec("unknown IoT freeze kind"));
        }
        Ok(IotFreezeHeader {
            operator,
            slot,
            kind,
            entries,
        })
    }

    pub fn resident_bytes(&self) -> usize {
        self.entries.iter().fold(
            128usize.saturating_add(
                self.entries
                    .capacity()
                    .saturating_mul(size_of::<IotEntry>()),
            ),
            |n, entry| {
                n.saturating_add(
                    entry
                        .key
                        .iter()
                        .chain(&entry.values)
                        .map(Scalar::resident_bytes)
                        .sum::<usize>(),
                )
                .saturating_add(
                    entry
                        .key
                        .capacity()
                        .saturating_sub(entry.key.len())
                        .saturating_mul(size_of::<Scalar>()),
                )
                .saturating_add(
                    entry
                        .values
                        .capacity()
                        .saturating_sub(entry.values.len())
                        .saturating_mul(size_of::<Scalar>()),
                )
            },
        )
    }

    fn validate_shape(&self, max_keys: usize) -> Result<()> {
        if self.slot != IOT_STATE_SLOT {
            return Err(codec("IoT freeze StateSlotId is not 3"));
        }
        if !matches!(self.kind, 4..=11) {
            return Err(codec("unknown IoT freeze kind"));
        }
        if max_keys == 0
            || self.entries.len() > max_keys
            || self.entries.len() > MAX_IOT_FREEZE_ENTRIES
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze entry count exceeds the state bound",
            ));
        }
        let mut seen = BTreeSet::new();
        for entry in &self.entries {
            if self.kind == crate::alarm_iot::KIND { crate::alarm_iot::metadata(&entry.values, None)?; }
            if matches!(self.kind,9|10) {
                if !matches!(entry.values.as_slice(), [Scalar::Int64(last_seen),_,..] if *last_seen >= 0) {
                    return Err(codec("TTL IoT freeze metadata or latch is invalid"));
                }
            }
            if matches!(self.kind,7|8) {
                match entry.values.as_slice() {
                    [Scalar::Int64(start),Scalar::Int64(deadline),Scalar::Bool(trailing),_,..] =>
                        validate_timed_metadata(self.kind,*start,*deadline,*trailing,None)?,
                    _ => return Err(codec("timed IoT freeze metadata")),
                }
            }
            if self.kind == IOT_HYSTERESIS_KIND && !matches!(entry.values.as_slice(), [Scalar::Bool(_)]) {
                return Err(codec("hysteresis freeze requires exactly one Bool latch"));
            }
            if entry.key.is_empty()
                || entry.key.len() > MAX_IOT_ARITY
                || entry.values.is_empty()
                || entry.values.len() > MAX_IOT_ARITY
            {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "IoT freeze key/value arity is invalid",
                ));
            }
            for value in &entry.key {
                validate_freeze_scalar(value, true)?;
            }
            if !seen.insert(encoded_key(&entry.key)?) {
                return Err(codec("duplicate IoT freeze key"));
            }
            for value in &entry.values {
                if matches!(self.kind,7|8|11) && value.is_null() { continue; }
                validate_freeze_scalar(value, false)?;
            }
        }
        Ok(())
    }
}

/// A single-task Change, Deadband or Hysteresis state operator.
pub struct IotOperator {
    operator: OperatorId,
    spec: IotSpec,
    key_idx: Vec<usize>,
    field_idx: Vec<usize>,
    input: Arc<Schema>,
    state: MemoryState<StoredIotState>,
    // HashMap buckets survive partial TTL eviction. Keep a separate
    // conservative high-water lease until their allocation is released.
    table_lease: Option<MemoryLease>,
    owner: Arc<MemoryOwner>,
    expiry: BTreeMap<(i64, Vec<u8>), StateKey>,
    expiry_bytes: usize,
    timed: Option<crate::timed_state::TimedState>,
    ordered_now: Option<i64>,
    _metadata_lease: MemoryLease,
    stats: IotStats,
}

impl IotOperator {
    pub fn new(
        operator: OperatorId,
        spec: IotSpec,
        input: Schema,
        owner: Arc<MemoryOwner>,
    ) -> Result<Self> {
        spec.validate(&input)?;
        if spec.ttl_micros > 0 && owner.budget().max_timers == 0 {
            return Err(SparrowError::new(ErrorCode::ResourceExhausted, "IoT TTL requires a bounded timer slot")
                .at_operator(operator));
        }
        let metadata = metadata_bytes(
            &spec,
            &input,
            spec.keys.len().saturating_mul(2).max(4),
            spec.fields.len().saturating_mul(2).max(4),
        );
        let metadata_lease = owner.acquire(CreditKind::Reservation, metadata.max(1))?;
        let key_idx = resolve_names(&input, &spec.keys, "key")?;
        let field_idx = resolve_names(&input, &spec.fields, "value")?;
        validate_iot_types(&spec, &input, &field_idx)?;
        let state = MemoryState::new(Arc::clone(&owner), operator, IOT_STATE_SLOT, spec.max_keys)?;
        let input = Arc::new(input);
        let timed = spec.timing.as_ref().map(|_| crate::timed_state::TimedState::new(operator, &spec,
            input.clone(),owner.clone(),key_idx.clone(),field_idx.clone())).transpose()?;
        Ok(Self {
            operator,
            spec,
            key_idx,
            field_idx,
            input,
            state,
            table_lease: None,
            owner,
            expiry: BTreeMap::new(),
            expiry_bytes: 0,
            _metadata_lease: metadata_lease,
            stats: IotStats::default(),
            timed,
            ordered_now: None,
        })
    }

    fn apply_row(
        &mut self,
        row: &Row,
        now: i64,
        output: &mut sparrow_model::RowBatchBuilder,
    ) -> Result<bool> {
        self.stats.input_rows = self.stats.input_rows.saturating_add(1);
        if !self.validate_row(row)? {
            self.stats.invalid_rows = self.stats.invalid_rows.saturating_add(1);
            self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
            return Ok(false);
        }

        // Reserve the complete candidate before StateKey/Scalar detach.  A
        // full-row multiple covers the detached key, encoded key and value
        // Vec/container peak rather than only the configured value columns.
        let row_bound = row.resident_bytes();
        let candidate_bound = 2048usize.saturating_add(row_bound.saturating_mul(8));
        let reservation = self
            .owner
            .acquire(CreditKind::Reservation, candidate_bound.max(1))?;

        let key_values = self
            .key_idx
            .iter()
            .map(|&index| row.values[index].detach_copy())
            .collect::<Vec<_>>();
        let key = StateKey::new(self.operator, IOT_STATE_SLOT, key_values);
        let previous_bound = self
            .state
            .get(&key)
            .map(|value| {
                value
                    .values
                    .iter()
                    .map(Scalar::resident_bytes)
                    .sum::<usize>()
                    .saturating_add(256)
            })
            .unwrap_or(0);
        // LastOutput may retain the old, wider value vector while the new
        // input is small.  Keep a separate bounded reservation for that
        // candidate container before cloning it.
        let previous_reservation = if previous_bound > 0 {
            Some(
                self.owner
                    .acquire(CreditKind::Reservation, previous_bound.saturating_mul(2))?,
            )
        } else {
            None
        };
        let previous = self.state.get(&key).cloned();
        let current_values = self
            .field_idx
            .iter()
            .map(|&index| row.values[index].detach_copy())
            .collect::<Vec<_>>();
        let (emit, next_values) = self.decide(previous.as_ref(), &current_values)?;
        // The Kernel supplies a monotonic processing clock.  Keep the
        // operator fail-closed for embedders that accidentally pass an older
        // timestamp: a clock step backwards must not extend an idle TTL.
        let seen_at = previous
            .as_ref()
            .map_or(now, |value| now.max(value.last_seen));
        if emit {
            // Output admission precedes baseline mutation. If state admission
            // then fails, on_batch drops this unpublished builder as well.
            let copy_bound = row.resident_bytes().saturating_mul(2).saturating_add(128);
            let copy_lease = self
                .owner
                .acquire(CreditKind::Reservation, copy_bound.max(1))?;
            output.push_accounted(row.detach_copy(), copy_bound)?;
            drop(copy_lease);
        }
        self.commit_state(key, next_values, seen_at)?;
        drop(current_values);
        drop(previous);
        drop(previous_reservation);
        drop(reservation);
        if emit {
            self.stats.emitted_rows = self.stats.emitted_rows.saturating_add(1);
        } else {
            self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
        }
        Ok(emit)
    }

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Option<RowBatch>> {
        if batch.schema().fields != self.input.fields {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "IoT input batch fields differ from the bound schema",
            )
            .at_operator(self.operator));
        }
        if !Arc::ptr_eq(batch.lease().owner(), &self.owner) {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "IoT input batch belongs to another Job memory owner",
            )
            .at_operator(self.operator));
        }
        if let Some(timed) = &mut self.timed { return timed.on_batch(batch, now); }
        if self.ordered_now.is_some_and(|cut| cut != now) {
            return Err(codec("IoT input must use its last ordered time decision"));
        }
        self.expire(now);
        let mut output = sparrow_model::RowBatchBuilder::new(
            self.input.clone(),
            Arc::clone(&self.owner),
            CreditKind::Reservation,
            batch.num_rows().max(1),
            self.owner.budget().reservation_bytes,
        )?;
        for row in batch.rows() {
            self.apply_row(row, now, &mut output)
                .map_err(|error| error.at_operator(self.operator))?;
        }
        if output.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(
            output
                .finish()?
                .with_origin(batch.origin())
                .with_source_operator(batch.source_operator()),
        ))
    }

    pub fn expire(&mut self, now: i64) {
        if self.spec.ttl_micros <= 0 {
            return;
        }
        while self.expire_one(now) {}
    }

    fn expire_one(&mut self, now: i64) -> bool {
        if !self.expiry.first_key_value().is_some_and(|((at,_),_)| *at <= now) { return false; }
        let (_, key) = self.expiry.pop_first().expect("due TTL");
        self.expiry_bytes = self.owner.replace_accounted_bytes(self.expiry_bytes, key.index_bytes(), 0);
        if self.state.remove(&key).is_some() {
            self.stats.expired_keys = self.stats.expired_keys.saturating_add(1);
        }
        if self.state.is_empty() {
            self.state.clear_and_release_capacity();
            self.table_lease = None;
        }
        true
    }

    pub fn next_deadline(&self) -> Option<i64> {
        if let Some(timed) = &self.timed { return timed.next_deadline(); }
        if self.spec.ttl_micros <= 0 {
            return None;
        }
        self.expiry.first_key_value().map(|((at, _), _)| *at)
    }

    pub fn pending_timers(&self) -> usize {
        if let Some(timed) = &self.timed { return timed.pending_timers(); }
        self.expiry.len()
    }

    pub fn cleanup(&mut self) {
        if let Some(timed) = &mut self.timed { timed.cleanup(); }
        self.state.clear_and_release_capacity();
        self.table_lease = None;
        self.expiry.clear();
        self.expiry_bytes = 0;
    }

    pub fn reset(&mut self) {
        self.cleanup();
    }

    pub fn key_count(&self) -> usize {
        if let Some(timed) = &self.timed { return timed.key_count(); }
        self.state.len()
    }

    /// Effective cap after the job budget is applied to the plan's max_keys.
    pub fn max_keys(&self) -> usize {
        if let Some(timed) = &self.timed { return timed.max_keys(); }
        self.state.max_keys()
    }

    pub fn retention_bytes(&self) -> usize {
        if let Some(timed) = &self.timed { return timed.retention_bytes(); }
        self.state
            .retention_bytes()
            .saturating_add(self.expiry_bytes)
            .saturating_add(self.table_lease.as_ref().map_or(0, MemoryLease::bytes))
    }

    pub fn stats(&self) -> IotStats {
        if let Some(timed) = &self.timed { return timed.stats(); }
        self.stats
    }

    pub fn operator_id(&self) -> OperatorId {
        self.operator
    }

    pub fn estimated_freeze_bytes(&self) -> usize {
        if let Some(timed) = &self.timed { return timed.estimated_freeze_bytes(); }
        let mut total = 11usize;
        for (key, value) in self.state.iter() {
            total = total
                .saturating_add(2)
                .saturating_add(
                    key.key
                        .iter()
                        .map(|s| s.encoded_value_len().unwrap_or(usize::MAX / 2))
                        .sum::<usize>(),
                )
                .saturating_add(2)
                .saturating_add(if self.spec.ttl_micros > 0 { 9 } else { 0 })
                .saturating_add(
                    value
                        .values
                        .iter()
                        .map(|s| s.encoded_value_len().unwrap_or(usize::MAX / 2))
                        .sum::<usize>(),
                );
        }
        total
    }

    #[cfg(test)]
    pub fn freeze(&self) -> Result<IotFreeze> {
        if let Some(timed) = &self.timed { return timed.freeze(); }
        let estimate = self.estimated_freeze_bytes();
        if estimate > MAX_IOT_FREEZE_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze exceeds 8 MiB",
            ));
        }
        let _scratch = self
            .owner
            .acquire(CreditKind::Reservation, estimate.max(1))?;
        let mut entries = self
            .state
            .iter()
            .map(|(key, value)| IotEntry {
                key: key.key.iter().map(Scalar::detach_copy).collect(),
                values: (self.spec.ttl_micros > 0).then_some(Scalar::Int64(value.last_seen))
                    .into_iter().chain(value.values.iter().map(Scalar::detach_copy)).collect(),
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| {
            encoded_key(&a.key)
                .expect("validated IoT key")
                .cmp(&encoded_key(&b.key).expect("validated IoT key"))
        });
        IotFreeze::new(self.operator, self.freeze_kind(), entries)
    }

    #[cfg(test)]
    pub fn try_freeze(&self) -> Result<IotFreeze> {
        self.freeze()
    }

    pub fn encode_freeze_into(&self, out: &mut Vec<u8>, max_keys: usize) -> Result<()> {
        if let Some(timed) = &self.timed { return timed.encode(out,max_keys); }
        if max_keys == 0 || self.state.len() > max_keys.min(MAX_IOT_FREEZE_ENTRIES) {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze entry count exceeds the state bound",
            ));
        }
        let start = out.len();
        ensure_frame_bytes(start, out.len(), 11)?;
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&IOT_STATE_SLOT.raw().to_le_bytes());
        out.push(self.freeze_kind());
        out.extend_from_slice(&(self.state.len() as u32).to_le_bytes());
        let workspace_bytes = self
            .state
            .len()
            .saturating_mul(size_of::<(&StateKey, &StoredIotState)>())
            .saturating_add(64);
        let _workspace = self
            .owner
            .acquire(CreditKind::Reservation, workspace_bytes)?;
        let mut keyed = Vec::with_capacity(self.state.len());
        keyed.extend(self.state.iter());
        keyed.sort_unstable_by(|a, b| a.0.encoded_bytes().cmp(b.0.encoded_bytes()));
        for pair in keyed.windows(2) {
            if pair[0].0.encoded_bytes() == pair[1].0.encoded_bytes() {
                return Err(codec("duplicate IoT state key"));
            }
        }
        for (key, value) in keyed {
            encode_entry_with_prefix(out, &key.key, &value.values, start,
                (self.spec.ttl_micros > 0).then_some(value.last_seen))?;
        }
        if out.len().saturating_sub(start) > MAX_IOT_FREEZE_BYTES {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze exceeds 8 MiB",
            ));
        }
        Ok(())
    }

    /// Restore into a separate candidate map and swap only after all entries
    /// pass schema, key, value and budget checks. A malformed/oversized frame
    /// therefore never clears a currently running operator.
    pub fn restore(&mut self, freeze: &IotFreeze) -> Result<()> {
        if let Some(timed) = &mut self.timed {
            freeze.validate_shape(timed.max_keys())?;
            return timed.restore(freeze);
        }
        if freeze.operator != self.operator
            || freeze.slot != IOT_STATE_SLOT
            || freeze.kind != self.freeze_kind()
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "IoT freeze operator, slot or kind does not match",
            )
            .at_operator(self.operator));
        }
        // MemoryState applies the job budget cap to spec.max_keys.  Restore
        // must validate against that effective cap, not the authoring value,
        // so a frame can never partially fill a replacement map before quota
        // failure.
        let _validation = self.owner.acquire(
            CreditKind::Reservation,
            freeze.resident_bytes().saturating_mul(2).max(1),
        )?;
        freeze.validate_shape(self.state.max_keys())?;
        let replacement_table = if freeze.entries.is_empty() {
            None
        } else {
            Some(self.owner.acquire(CreditKind::Retention, table_bytes(freeze.entries.len()))?)
        };
        let mut replacement = MemoryState::new(
            Arc::clone(&self.owner),
            self.operator,
            IOT_STATE_SLOT,
            self.spec.max_keys,
        )?;
        let mut expiry = BTreeMap::new();
        let mut expiry_bytes = 0usize;
        for entry in &freeze.entries {
            self.validate_freeze_entry(entry)?;
            let (last_seen, values) = self.entry_values(entry)?;
            let deadline = if self.spec.ttl_micros > 0 {
                let at = last_seen.checked_add(self.spec.ttl_micros).ok_or_else(||codec("TTL deadline overflow"))?;
                if self.ordered_now.is_some_and(|now| last_seen > now || at <= now) {
                    return Err(codec("TTL state disagrees with its processing-time cut"));
                }
                if expiry.len() >= self.owner.budget().max_timers { return Err(codec("TTL restore timer bound")); }
                Some(at)
            } else { None };
            let candidate_bound = 2048usize.saturating_add(
                entry
                    .key
                    .iter()
                    .chain(&entry.values)
                    .fold(0usize, |n, value| n.saturating_add(value.resident_bytes()))
                    .saturating_mul(8),
            );
            let _scratch = self
                .owner
                .acquire(CreditKind::Reservation, candidate_bound.max(1))?;
            let key = StateKey::new(
                self.operator,
                IOT_STATE_SLOT,
                entry.key.iter().map(Scalar::detach_copy).collect(),
            );
            if replacement.get(&key).is_some() {
                return Err(codec("duplicate IoT restore key"));
            }
            let values = values
                .iter()
                .map(Scalar::detach_copy)
                .collect::<Vec<_>>();
            if let Some(at) = deadline {
                let indexed = key.indexed(&self.owner)?;
                expiry_bytes = expiry_bytes.saturating_add(indexed.index_bytes());
                expiry.insert((at,indexed.encoded_bytes().to_vec()),indexed);
            }
            replacement.put(
                key,
                StoredIotState {
                    values: values.clone(),
                    last_seen,
                },
                state_value_bytes(&values),
            )?;
        }
        self.expiry.clear();
        self.state = replacement;
        self.table_lease = replacement_table;
        self.expiry = expiry;
        self.expiry_bytes = expiry_bytes;
        Ok(())
    }

    fn freeze_kind(&self) -> u8 {
        self.spec.state_kind_tag()
    }

    pub fn is_timed(&self) -> bool { self.timed.is_some() }
    pub fn bind_generation(&mut self, generation: [u8; 16]) -> Result<()> {
        if let Some(timed) = &mut self.timed { timed.bind_generation(generation)?; }
        Ok(())
    }
    pub fn validate_processing_cut(&mut self,now:i64)->Result<()> {
        if let Some(timed) = &mut self.timed { return timed.validate_cut(now); }
        if self.spec.ttl_micros > 0 && self.state.iter().any(|(_,value)| value.last_seen < 0 || value.last_seen > now
            || value.last_seen.checked_add(self.spec.ttl_micros).is_none_or(|at| at <= now)) {
            return Err(codec("TTL restore has future input or overdue expiry"));
        }
        self.set_processing_time(now)
    }
    pub fn set_processing_time(&mut self, now: i64) -> Result<()> {
        if let Some(timed) = &mut self.timed { return timed.set_time(now); }
        if now < 0 || self.ordered_now.is_some_and(|old| now < old) { return Err(codec("processing time cannot move backwards")); }
        self.ordered_now = Some(now);
        Ok(())
    }
    pub fn take_timed_due(&mut self, now: i64) -> Result<Option<RowBatch>> {
        if let Some(timed) = &mut self.timed { return timed.take_due(now); }
        self.expire_one(now);
        Ok(None)
    }

    fn validate_row(&self, row: &Row) -> Result<bool> {
        if row.values.len() != self.input.fields.len() {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "IoT row width differs from the input schema",
            )
            .at_operator(self.operator));
        }
        for &index in &self.key_idx {
            let value = &row.values[index];
            let field = &self.input.fields[index];
            if value.is_null() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("IoT key '{}' must be non-null", field.name),
                )
                .at_operator(self.operator));
            }
            if !value.matches_type(&field.data_type) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!(
                        "IoT key '{}' has type {} instead of {}",
                        field.name,
                        value.data_type(),
                        field.data_type
                    ),
                )
                .at_operator(self.operator));
            }
        }
        for &index in &self.field_idx {
            let value = &row.values[index];
            let invalid = value.is_null() || matches!(value, Scalar::Float64(v) if !v.is_finite());
            if invalid {
                return match self.spec.invalid {
                    InvalidValuePolicy::Ignore => Ok(false),
                    InvalidValuePolicy::Error => Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!(
                            "IoT field '{}' is NULL or non-finite",
                            self.input.fields[index].name
                        ),
                    )
                    .at_operator(self.operator)),
                };
            }
            if !value.matches_type(&self.input.fields[index].data_type) {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!(
                        "IoT field '{}' type differs from schema",
                        self.input.fields[index].name
                    ),
                )
                .at_operator(self.operator));
            }
        }
        Ok(true)
    }

    fn decide(
        &self,
        previous: Option<&StoredIotState>,
        current: &[Scalar],
    ) -> Result<(bool, Vec<Scalar>)> {
        if let Some(hysteresis) = &self.spec.hysteresis {
            let active = match previous {
                None => false,
                Some(previous) => match previous.values.as_slice() {
                    [Scalar::Bool(active)] => *active,
                    _ => return Err(codec("invalid hysteresis latch in live state")),
                },
            };
            let threshold = if active { hysteresis.exit } else { hysteresis.enter };
            let comparison = compare_threshold(&current[0], threshold)?;
            let crossed = match (hysteresis.direction, active) {
                (sparrow_plan::HysteresisDirection::High, false)
                | (sparrow_plan::HysteresisDirection::Low, true) => !comparison.is_lt(),
                _ => !comparison.is_gt(),
            };
            let next = if crossed { !active } else { active };
            let emit = if previous.is_none() { self.spec.emit_first } else { next != active };
            return Ok((emit, vec![Scalar::Bool(next)]));
        }
        let Some(previous) = previous else {
            return Ok((
                self.spec.emit_first,
                current.iter().map(Scalar::detach_copy).collect(),
            ));
        };
        let (emit, next) = if let Some(deadband) = &self.spec.deadband {
            let changed = deadband_exceeds(
                &previous.values[0],
                &current[0],
                deadband.mode,
                deadband.threshold,
            )?;
            let next = match deadband.baseline {
                DeadbandBaseline::LastInput => current.iter().map(Scalar::detach_copy).collect(),
                DeadbandBaseline::LastOutput if changed => {
                    current.iter().map(Scalar::detach_copy).collect()
                }
                DeadbandBaseline::LastOutput => previous.values.clone(),
            };
            (changed, next)
        } else {
            let changed = previous
                .values
                .iter()
                .zip(current)
                .any(|(old, new)| old != new);
            (changed, current.iter().map(Scalar::detach_copy).collect())
        };
        Ok((emit, next))
    }

    fn commit_state(&mut self, key: StateKey, values: Vec<Scalar>, now: i64) -> Result<()> {
        if self.spec.ttl_micros > 0 && (now < 0 || now.checked_add(self.spec.ttl_micros).is_none()) {
            return Err(codec("TTL deadline overflow or negative time"));
        }
        let old_last_seen = self.state.get(&key).map(|value| value.last_seen);
        let previous_table_bytes = self.table_lease.as_ref().map_or(0, MemoryLease::bytes);
        if old_last_seen.is_none() {
            if self.spec.ttl_micros > 0 && self.expiry.len() >= self.owner.budget().max_timers {
                return Err(SparrowError::new(ErrorCode::ResourceExhausted, "IoT pending TTL timer bound exceeded")
                    .at_operator(self.operator));
            }
            if self.state.len() >= self.state.max_keys() {
                return Err(SparrowError::new(ErrorCode::ResourceExhausted, "IoT max_keys exceeded")
                    .at_operator(self.operator));
            }
            let bytes = table_bytes(self.state.len().saturating_add(1));
            if let Some(lease) = &mut self.table_lease {
                lease.grow_to(bytes)?;
            } else {
                self.table_lease = Some(self.owner.acquire(CreditKind::Retention, bytes)?);
            }
        }
        let admitted = (|| {
            let indexed = if self.spec.ttl_micros > 0 {
                Some(key.indexed(&self.owner)?)
            } else {
                None
            };
            let value_bytes = state_value_bytes(&values);
            // `put` acquires the lease before inserting or replacing. Failure
            // has not changed the map's allocation, so its table charge can
            // safely roll back as well as the candidate value/index charge.
            self.state.put(
                key.clone(),
                StoredIotState { values, last_seen: now },
                value_bytes,
            )?;
            Ok::<_, SparrowError>(indexed)
        })();
        let indexed = match admitted {
            Ok(indexed) => indexed,
            Err(error) => {
                if previous_table_bytes == 0 {
                    self.table_lease = None;
                } else if let Some(lease) = &mut self.table_lease {
                    lease.shrink_to(previous_table_bytes)?;
                }
                return Err(error);
            }
        };
        if let Some(indexed) = indexed {
            if let Some(last_seen) = old_last_seen {
                if let Some(old) = self
                    .expiry
                    .remove(&(self.expire_at(last_seen), key.encoded_bytes().to_vec()))
                {
                    self.expiry_bytes =
                        self.owner
                            .replace_accounted_bytes(self.expiry_bytes, old.index_bytes(), 0);
                }
            }
            let expiry = self.expire_at(now);
            let index_bytes = indexed.index_bytes();
            let old = self
                .expiry
                .insert((expiry, indexed.encoded_bytes().to_vec()), indexed);
            if let Some(old) = old {
                self.expiry_bytes =
                    self.owner
                        .replace_accounted_bytes(self.expiry_bytes, old.index_bytes(), 0);
            }
            self.expiry_bytes =
                self.owner
                    .replace_accounted_bytes(self.expiry_bytes, 0, index_bytes);
        }
        Ok(())
    }

    fn expire_at(&self, last_seen: i64) -> i64 {
        last_seen.saturating_add(self.spec.ttl_micros)
    }

    fn validate_freeze_entry(&self, entry: &IotEntry) -> Result<()> {
        let (_, values) = self.entry_values(entry)?;
        if entry.key.len() != self.key_idx.len() || values.len() != self.field_idx.len() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "IoT freeze key/value arity differs from the live operator",
            ));
        }
        for (value, &index) in entry.key.iter().zip(&self.key_idx) {
            let field = &self.input.fields[index];
            if value.is_null() || !value.matches_type(&field.data_type) {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "IoT freeze key does not match the live schema",
                ));
            }
        }
        if self.spec.hysteresis.is_some() {
            return if matches!(values, [Scalar::Bool(_)]) {
                Ok(())
            } else {
                Err(codec("hysteresis freeze requires a Bool latch"))
            };
        }
        for (value, &index) in values.iter().zip(&self.field_idx) {
            let field = &self.input.fields[index];
            if value.is_null()
                || matches!(value, Scalar::Float64(v) if !v.is_finite())
                || !value.matches_type(&field.data_type)
            {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "IoT freeze value does not match the live schema",
                ));
            }
        }
        Ok(())
    }

    fn entry_values<'a>(&self, entry: &'a IotEntry) -> Result<(i64, &'a [Scalar])> {
        if self.spec.ttl_micros == 0 { return Ok((0, &entry.values)); }
        match entry.values.split_first() {
            Some((Scalar::Int64(last_seen), values)) if *last_seen >= 0 => Ok((*last_seen, values)),
            _ => Err(codec("TTL freeze lacks valid last_seen")),
        }
    }
}

fn resolve_names(schema: &Schema, names: &[String], role: &str) -> Result<Vec<usize>> {
    let mut seen = BTreeSet::new();
    names
        .iter()
        .map(|name| {
            let index = schema.index_of_name(name).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown IoT {role} field '{name}'"),
                )
            })?;
            if !seen.insert(index) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("duplicate IoT {role} field '{name}'"),
                ));
            }
            Ok(index)
        })
        .collect()
}

fn validate_iot_types(spec: &IotSpec, input: &Schema, fields: &[usize]) -> Result<()> {
    for name in &spec.keys {
        let field = input
            .field_by_name(name)
            .ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, "unknown IoT key"))?;
        if !matches!(
            field.data_type.clone(),
            DataType::Bool
                | DataType::Int64
                | DataType::UInt64
                | DataType::Utf8
                | DataType::Bytes
                | DataType::TimestampMicrosUTC
        ) {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!(
                    "IoT key '{}' is not a supported scalar key type",
                    field.name
                ),
            ));
        }
    }
    for &index in fields {
        if !matches!(
            input.fields[index].data_type.clone(),
            DataType::Bool
                | DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Bytes
                | DataType::TimestampMicrosUTC
        ) {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!(
                    "IoT field '{}' is not a supported scalar value type",
                    input.fields[index].name
                ),
            ));
        }
    }
    if spec.deadband.is_some() || spec.hysteresis.is_some() {
        if fields.len() != 1 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "IoT Deadband requires exactly one value field",
            ));
        }
        if !matches!(
            input.fields[fields[0]].data_type.clone(),
            DataType::Int64 | DataType::UInt64 | DataType::Float64
        ) {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "IoT Deadband requires one numeric field",
            ));
        }
    }
    Ok(())
}

fn table_bytes(entries: usize) -> usize {
    // Bucket load-factor, power-of-two growth, entry padding/control bytes.
    // This is deliberately conservative and retained across partial eviction.
    entries.max(4).saturating_mul(4).saturating_mul(
        size_of::<StateKey>()
            .saturating_add(size_of::<StoredIotState>())
            .saturating_add(size_of::<MemoryLease>())
            .saturating_add(128),
    )
}

fn metadata_bytes(
    spec: &IotSpec,
    input: &Schema,
    key_capacity: usize,
    field_capacity: usize,
) -> usize {
    fn type_bytes(ty: &DataType) -> usize {
        match ty {
            DataType::Array(value) => size_of::<DataType>().saturating_add(type_bytes(value)),
            DataType::Map { key, value } => size_of::<DataType>()
                .saturating_mul(2)
                .saturating_add(type_bytes(key))
                .saturating_add(type_bytes(value)),
            DataType::Struct(fields) => fields.iter().fold(
                fields
                    .capacity()
                    .saturating_mul(size_of::<sparrow_model::Field>()),
                |n, field| {
                    n.saturating_add(field.name.capacity())
                        .saturating_add(type_bytes(&field.data_type))
                },
            ),
            _ => 0,
        }
    }
    let names = spec
        .keys
        .iter()
        .chain(&spec.fields)
        .map(String::capacity)
        .sum::<usize>();
    1024usize
        .saturating_add(size_of::<IotOperator>())
        .saturating_add(size_of::<Schema>())
        .saturating_add(
            input
                .fields
                .capacity()
                .saturating_mul(size_of::<sparrow_model::Field>()),
        )
        .saturating_add(names)
        .saturating_add(
            spec.keys
                .capacity()
                .saturating_add(spec.fields.capacity())
                .saturating_mul(size_of::<String>()),
        )
        .saturating_add(input.fields.iter().fold(0usize, |n, field| {
            n.saturating_add(field.name.capacity())
                .saturating_add(type_bytes(&field.data_type))
        }))
        .saturating_add(key_capacity.saturating_mul(size_of::<usize>()))
        .saturating_add(field_capacity.saturating_mul(size_of::<usize>()))
        .saturating_add(size_of::<IotSpec>())
}

// Capacity is part of retained memory, so a slice is insufficient here.
#[allow(clippy::ptr_arg)]
fn state_value_bytes(values: &Vec<Scalar>) -> usize {
    size_of::<StoredIotState>()
        .saturating_add(64)
        .saturating_add(values.capacity().saturating_mul(size_of::<Scalar>()))
        .saturating_add(values.iter().map(Scalar::resident_bytes).sum::<usize>())
}

fn encoded_key(values: &[Scalar]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for value in values {
        let encoded_len = value.encoded_value_len()?;
        if bytes
            .len()
            .checked_add(encoded_len)
            .and_then(|size| size.checked_add(1))
            .is_none_or(|size| size > MAX_IOT_FREEZE_BYTES)
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "IoT freeze key exceeds 8 MiB",
            ));
        }
        value.encode_value(&mut bytes)?;
        bytes.push(0xff);
    }
    Ok(bytes)
}

fn encode_entry(out: &mut Vec<u8>, entry: &IotEntry, start: usize) -> Result<()> {
    encode_entry_parts(out, &entry.key, &entry.values, start)
}

pub(crate) fn encode_entry_parts(
    out: &mut Vec<u8>,
    key: &[Scalar],
    values: &[Scalar],
    start: usize,
) -> Result<()> {
    encode_entry_with_prefix(out,key,values,start,None)
}

fn encode_entry_with_prefix(out: &mut Vec<u8>, key: &[Scalar], values: &[Scalar], start: usize, last_seen: Option<i64>) -> Result<()> {
    ensure_frame_bytes(start, out.len(), 2)?;
    out.extend_from_slice(&(key.len() as u16).to_le_bytes());
    for value in key {
        let len = value.encoded_value_len()?;
        ensure_frame_bytes(start, out.len(), len)?;
        value.encode_value(out)?;
    }
    ensure_frame_bytes(start, out.len(), 2)?;
    out.extend_from_slice(&((values.len() + usize::from(last_seen.is_some())) as u16).to_le_bytes());
    if let Some(last_seen) = last_seen {
        ensure_frame_bytes(start,out.len(),9)?;
        Scalar::Int64(last_seen).encode_value(out)?;
    }
    for value in values {
        let len = value.encoded_value_len()?;
        ensure_frame_bytes(start, out.len(), len)?;
        value.encode_value(out)?;
    }
    Ok(())
}

fn validate_freeze_scalar(value: &Scalar, key: bool) -> Result<()> {
    if value.is_null() || matches!(value, Scalar::Dynamic(_)) {
        return Err(codec("NULL or Dynamic value cannot enter IoT state"));
    }
    if key && matches!(value, Scalar::Float64(_)) {
        return Err(codec("Float64 is not a supported IoT key"));
    }
    if matches!(value, Scalar::Float64(v) if !v.is_finite()) {
        return Err(codec("non-finite Float64 cannot enter IoT state"));
    }
    Ok(())
}

fn validate_encoded_scalar(bytes: &[u8], key: bool) -> Result<()> {
    let Some(&tag) = bytes.first() else {
        return Err(codec("empty encoded IoT scalar"));
    };
    match tag {
        0 => Err(codec("NULL value cannot enter IoT state")),
        4 => {
            if key {
                return Err(codec("Float64 is not a supported IoT key"));
            }
            if bytes.len() != 9 {
                return Err(codec("invalid encoded Float64"));
            }
            let bits = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
            if !f64::from_bits(bits).is_finite() {
                return Err(codec("non-finite Float64 cannot enter IoT state"));
            }
            Ok(())
        }
        1 | 2 | 3 | 5 | 6 | 7 => Ok(()),
        8 => Err(codec("Dynamic value cannot enter IoT state")),
        _ => Err(codec("invalid encoded IoT scalar tag")),
    }
}

/// Compare integer inputs without rounding them to f64. The threshold is
/// the declared finite f64 value, including fractional and >2^53 values.
fn compare_threshold(value: &Scalar, threshold: f64) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    let integer = match value {
        Scalar::Int64(value) => i128::from(*value),
        Scalar::UInt64(value) => i128::from(*value),
        Scalar::Float64(value) => return value.partial_cmp(&threshold)
            .ok_or_else(|| codec("non-finite hysteresis comparison")),
        _ => return Err(codec("hysteresis comparison requires a numeric scalar")),
    };
    if threshold >= i128::MAX as f64 { return Ok(Ordering::Less); }
    if threshold <= i128::MIN as f64 { return Ok(Ordering::Greater); }
    let floor = threshold.floor();
    let order = integer.cmp(&(floor as i128));
    Ok(if order.is_eq() && threshold != floor { Ordering::Less } else { order })
}

fn deadband_exceeds(
    previous: &Scalar,
    current: &Scalar,
    mode: DeadbandMode,
    threshold: f64,
) -> Result<bool> {
    if !threshold.is_finite() || threshold < 0.0 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "IoT Deadband threshold must be finite and non-negative",
        ));
    }
    let result = match (previous, current) {
        (Scalar::Int64(a), Scalar::Int64(b)) => match mode {
            DeadbandMode::Absolute => integer_abs_exceeds_i64(*a, *b, threshold),
            DeadbandMode::Relative => {
                integer_relative_exceeds(i64_delta(*a, *b), i64_abs(*a), threshold)
            }
        },
        (Scalar::UInt64(a), Scalar::UInt64(b)) => match mode {
            DeadbandMode::Absolute => integer_threshold_exceeds(a.abs_diff(*b) as u128, threshold),
            DeadbandMode::Relative => {
                integer_relative_exceeds(a.abs_diff(*b) as u128, *a as u128, threshold)
            }
        },
        (Scalar::Float64(a), Scalar::Float64(b)) => {
            let delta = (*a - *b).abs();
            if delta == 0.0 {
                false
            } else {
                match mode {
                    DeadbandMode::Absolute => delta > threshold,
                    DeadbandMode::Relative => {
                        if *a == 0.0 {
                            true
                        } else if delta.is_infinite() {
                            // Finite opposite-sign extremes can overflow the
                            // subtraction, although their relative change is
                            // small (e.g. -MAX -> MAX is 2, not infinity).
                            let scale = a.abs().max(b.abs());
                            (a / scale - b / scale).abs() > threshold * (a.abs() / scale)
                        } else {
                            delta / a.abs() > threshold
                        }
                    }
                }
            }
        }
        _ => {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "IoT Deadband baseline/value types differ",
            ));
        }
    };
    Ok(result)
}

fn integer_abs_exceeds_i64(a: i64, b: i64, threshold: f64) -> bool {
    integer_threshold_exceeds(i64_delta(a, b), threshold)
}

fn i64_abs(value: i64) -> u128 {
    value.unsigned_abs() as u128
}

fn i64_delta(a: i64, b: i64) -> u128 {
    (i128::from(a) - i128::from(b)).unsigned_abs()
}

fn integer_threshold_exceeds(delta: u128, threshold: f64) -> bool {
    if delta == 0 {
        return false;
    }
    if threshold >= u128::MAX as f64 {
        return false;
    }
    delta > threshold.floor() as u128
}

/// Compare `delta / baseline > threshold` without converting a 64-bit
/// integer to an imprecise f64 first.  Threshold is represented as an exact
/// binary mantissa/exponent and both sides are compared with checked u128
/// arithmetic.
fn integer_relative_exceeds(delta: u128, baseline: u128, threshold: f64) -> bool {
    if delta == 0 {
        return false;
    }
    if baseline == 0 {
        return true;
    }
    if threshold == 0.0 {
        return true;
    }
    let bits = threshold.to_bits();
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, exponent) = if exponent_bits == 0 {
        (fraction, -1074)
    } else {
        ((1u64 << 52) | fraction, exponent_bits - 1023 - 52)
    };
    if exponent >= 0 {
        let Some(rhs) = baseline
            .checked_mul(mantissa as u128)
            .and_then(|value| checked_value_shift(value, exponent as u32))
        else {
            return false;
        };
        delta > rhs
    } else {
        let rhs = baseline.saturating_mul(mantissa as u128);
        let shift = (-exponent) as u32;
        match checked_value_shift(delta, shift) {
            Some(lhs) => lhs > rhs,
            None => true, // mathematical lhs exceeds u128; rhs fits in 117 bits
        }
    }
}

// Rust checked_shl checks the shift amount, not loss of high value bits.
fn checked_value_shift(value: u128, shift: u32) -> Option<u128> {
    if value == 0 {
        return Some(0);
    }
    if shift >= 128 || value > (u128::MAX >> shift) {
        return None;
    }
    Some(value << shift)
}

fn ensure_frame_bytes(start: usize, current: usize, added: usize) -> Result<()> {
    if current
        .checked_add(added)
        .and_then(|size| size.checked_sub(start))
        .is_none_or(|size| size > MAX_IOT_FREEZE_BYTES)
    {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "IoT freeze exceeds 8 MiB",
        ));
    }
    Ok(())
}

fn take_u16(src: &mut &[u8], what: &str) -> Result<u16> {
    if src.len() < 2 {
        return Err(codec(format!("truncated {what}")));
    }
    let value = u16::from_le_bytes(src[..2].try_into().unwrap());
    *src = &src[2..];
    Ok(value)
}

fn ensure_decoded_size(consumed: usize) -> Result<()> {
    if consumed > MAX_IOT_FREEZE_BYTES {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "IoT freeze body exceeds 8 MiB",
        ));
    }
    Ok(())
}

fn codec(message: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{Field, FieldId, ResourceBudget, RowBatchBuilder, SchemaId};
    use sparrow_plan::{DeadbandSpec, IotSpec};

    fn owner() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget::compact())
    }

    fn schema(value: DataType, nullable: bool) -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "value", value, nullable),
            ],
        )
        .unwrap()
    }

    fn spec(value: &str, emit_first: bool, ttl_micros: i64) -> IotSpec {
        IotSpec {
            keys: vec!["id".into()],
            fields: vec![value.into()],
            emit_first,
            ttl_micros,
            max_keys: 16,
            invalid: InvalidValuePolicy::Error,
            deadband: None,
            hysteresis: None,
            timing: None,
        }
    }

    fn batch(owner: &Arc<MemoryOwner>, schema: &Schema, rows: Vec<Row>) -> RowBatch {
        let mut builder = RowBatchBuilder::new(
            Arc::new(schema.clone()),
            owner.clone(),
            CreditKind::Reservation,
            rows.len().max(1),
            owner.budget().reservation_bytes,
        )
        .unwrap();
        for row in rows {
            builder.push(row).unwrap();
        }
        builder.finish().unwrap()
    }

    fn row(id: &str, value: Scalar) -> Row {
        Row {
            values: vec![Scalar::utf8(id), value],
        }
    }

    fn values(batch: Option<RowBatch>) -> Vec<Scalar> {
        batch
            .map(|batch| {
                batch
                    .rows()
                    .iter()
                    .map(|row| row.values[1].clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn k4_change_emits_first_and_only_distinct_values() {
        let owner = owner();
        let input = schema(DataType::Int64, false);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 0),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        let got = op
            .on_batch(
                &batch(
                    &owner,
                    &input,
                    vec![
                        row("a", Scalar::Int64(1)),
                        row("a", Scalar::Int64(1)),
                        row("a", Scalar::Int64(2)),
                        row("b", Scalar::Int64(9)),
                    ],
                ),
                10,
            )
            .unwrap();
        assert_eq!(
            values(got),
            vec![Scalar::Int64(1), Scalar::Int64(2), Scalar::Int64(9)]
        );
        assert_eq!(op.stats().filtered_rows, 1);
        assert_eq!(op.key_count(), 2);
    }

    #[test]
    fn k4_deadband_absolute_equal_is_filtered_and_last_input_drift_does_not_accumulate() {
        let owner = owner();
        let input = schema(DataType::Int64, false);
        let mut configured = spec("value", false, 0);
        configured.deadband = Some(DeadbandSpec {
            mode: DeadbandMode::Absolute,
            baseline: DeadbandBaseline::LastInput,
            threshold: 2.0,
        });
        let mut op =
            IotOperator::new(OperatorId::new(5), configured, input.clone(), owner.clone()).unwrap();
        let got = op
            .on_batch(
                &batch(
                    &owner,
                    &input,
                    vec![
                        row("a", Scalar::Int64(0)),
                        row("a", Scalar::Int64(2)),
                        row("a", Scalar::Int64(3)),
                        row("a", Scalar::Int64(6)),
                    ],
                ),
                0,
            )
            .unwrap();
        assert_eq!(values(got), vec![Scalar::Int64(6)]);
    }

    #[test]
    fn k4_deadband_last_output_accumulates_filtered_drift() {
        let owner = owner();
        let input = schema(DataType::Float64, false);
        let mut configured = spec("value", false, 0);
        configured.deadband = Some(DeadbandSpec {
            mode: DeadbandMode::Absolute,
            baseline: DeadbandBaseline::LastOutput,
            threshold: 2.0,
        });
        let mut op =
            IotOperator::new(OperatorId::new(5), configured, input.clone(), owner.clone()).unwrap();
        let got = op
            .on_batch(
                &batch(
                    &owner,
                    &input,
                    vec![
                        row("a", Scalar::Float64(0.0)),
                        row("a", Scalar::Float64(1.0)),
                        row("a", Scalar::Float64(2.0)),
                        row("a", Scalar::Float64(3.0)),
                    ],
                ),
                0,
            )
            .unwrap();
        assert_eq!(values(got), vec![Scalar::Float64(3.0)]);
    }

    #[test]
    fn k4_deadband_integer_extreme_does_not_lose_precision() {
        let owner = owner();
        let input = schema(DataType::Int64, false);
        let mut configured = spec("value", true, 0);
        configured.deadband = Some(DeadbandSpec {
            mode: DeadbandMode::Absolute,
            baseline: DeadbandBaseline::LastInput,
            threshold: 9_223_372_036_854_775_807.0,
        });
        let mut op =
            IotOperator::new(OperatorId::new(5), configured, input.clone(), owner.clone()).unwrap();
        let got = op
            .on_batch(
                &batch(
                    &owner,
                    &input,
                    vec![
                        row("a", Scalar::Int64(i64::MIN)),
                        row("a", Scalar::Int64(i64::MAX)),
                    ],
                ),
                0,
            )
            .unwrap();
        assert_eq!(
            values(got),
            vec![Scalar::Int64(i64::MIN), Scalar::Int64(i64::MAX)]
        );
    }

    #[test]
    fn k4_relative_zero_baseline_and_float_zero_are_changes() {
        let owner = owner();
        let input = schema(DataType::UInt64, false);
        let mut configured = spec("value", false, 0);
        configured.deadband = Some(DeadbandSpec {
            mode: DeadbandMode::Relative,
            baseline: DeadbandBaseline::LastInput,
            threshold: 0.05,
        });
        let mut op =
            IotOperator::new(OperatorId::new(5), configured, input.clone(), owner.clone()).unwrap();
        let got = op
            .on_batch(
                &batch(
                    &owner,
                    &input,
                    vec![
                        row("a", Scalar::UInt64(0)),
                        row("a", Scalar::UInt64(1)),
                        row("a", Scalar::UInt64(1)),
                    ],
                ),
                0,
            )
            .unwrap();
        assert_eq!(values(got), vec![Scalar::UInt64(1)]);
    }

    #[test]
    fn k4_relative_extremes_preserve_value_bits_and_finite_float_ratios() {
        assert!(integer_relative_exceeds(
            1u128 << 63,
            u64::MAX as u128,
            2f64.powi(-60)
        ));
        assert!(!integer_relative_exceeds(1, 1u128 << 63, 2f64.powi(120)));
        assert!(!integer_relative_exceeds(1, 2, 0.5));
        assert!(integer_relative_exceeds(
            1,
            2,
            f64::from_bits(0.5f64.to_bits() - 1)
        ));
        assert!(deadband_exceeds(
            &Scalar::UInt64(1u64 << 53),
            &Scalar::UInt64((1u64 << 53) + 1),
            DeadbandMode::Absolute,
            0.0
        )
        .unwrap());
        for threshold in [2.0, 3.0, f64::MAX] {
            assert!(!deadband_exceeds(
                &Scalar::Float64(-f64::MAX),
                &Scalar::Float64(f64::MAX),
                DeadbandMode::Relative,
                threshold
            )
            .unwrap());
        }
        assert!(deadband_exceeds(
            &Scalar::Float64(-f64::MAX),
            &Scalar::Float64(f64::MAX),
            DeadbandMode::Relative,
            1.0
        )
        .unwrap());
    }

    #[test]
    fn k4_invalid_ignore_does_not_refresh_baseline_or_ttl() {
        let owner = owner();
        let input = schema(DataType::Float64, true);
        let mut configured = spec("value", false, 10);
        configured.invalid = InvalidValuePolicy::Ignore;
        let mut op =
            IotOperator::new(OperatorId::new(4), configured, input.clone(), owner.clone()).unwrap();
        assert!(op
            .on_batch(
                &batch(&owner, &input, vec![row("a", Scalar::Float64(1.0))]),
                0
            )
            .unwrap()
            .is_none());
        assert!(op
            .on_batch(&batch(&owner, &input, vec![row("a", Scalar::Null)]), 9)
            .unwrap()
            .is_none());
        assert_eq!(op.next_deadline(), Some(10));
        // Expiry resets first-value semantics, including emit_first=false.
        assert!(op.on_batch(
            &batch(&owner, &input, vec![row("a", Scalar::Float64(2.0))]),
            10,
        ).unwrap().is_none());
        assert_eq!(op.stats().expired_keys, 1);
        assert_eq!(op.next_deadline(), Some(20));
        assert_eq!(
            op.on_batch(
                &batch(&owner, &input, vec![row("a", Scalar::Float64(3.0))]),
                11
            )
            .unwrap()
            .unwrap()
            .rows()[0]
                .values[1],
            Scalar::Float64(3.0)
        );
        assert_eq!(op.stats().invalid_rows, 1);
    }

    #[test]
    fn k4_invalid_error_is_attributed_and_does_not_change_existing_state() {
        let owner = owner();
        let input = schema(DataType::Float64, true);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 0),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        op.on_batch(
            &batch(&owner, &input, vec![row("a", Scalar::Float64(1.0))]),
            0,
        )
        .unwrap();
        let err = op
            .on_batch(
                &batch(&owner, &input, vec![row("a", Scalar::Float64(f64::NAN))]),
                1,
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err
            .context
            .iter()
            .any(|(key, value)| key == "operator" && value == "OperatorId(4)"));
        assert_eq!(op.key_count(), 1);
        assert_eq!(
            values(
                op.on_batch(
                    &batch(&owner, &input, vec![row("a", Scalar::Float64(1.0))]),
                    2
                )
                .unwrap()
            ),
            Vec::<Scalar>::new()
        );
    }

    #[test]
    fn k4_ttl_reentry_is_a_new_first_value_and_cleanup_releases_expiry() {
        let owner = owner();
        let input = schema(DataType::Int64, false);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 10),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        assert_eq!(
            op.on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 0)
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        assert!(op
            .on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 5)
            .unwrap()
            .is_none());
        assert_eq!(op.next_deadline(), Some(15));
        op.expire(15);
        assert_eq!(op.key_count(), 0);
        assert_eq!(
            op.on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 15)
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        op.cleanup();
        assert_eq!(op.key_count(), 0);
        assert_eq!(op.next_deadline(), None);
    }

    #[test]
    fn k4_retention_failure_leaves_old_state_unchanged() {
        let owner = owner();
        let input = schema(DataType::Int64, false);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 0),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        op.on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 0)
            .unwrap();
        let used = owner.usage().retention_bytes;
        let blocker = owner
            .acquire(
                CreditKind::Retention,
                owner
                    .budget()
                    .retention_bytes
                    .saturating_sub(used)
                    .saturating_sub(1),
            )
            .unwrap();
        let err = op
            .on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(2))]), 1)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        assert_eq!(op.key_count(), 1);
        drop(blocker);
        assert!(op
            .on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 2)
            .unwrap()
            .is_none());
    }

    #[test]
    fn k4_output_admission_failure_keeps_baseline_and_ttl_unchanged() {
        let owner = owner();
        let input = schema(DataType::Float64, false);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 10),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        op.on_batch(
            &batch(&owner, &input, vec![row("a", Scalar::Float64(1.0))]),
            0,
        )
        .unwrap();
        let before = owner.usage().physical_bytes;
        let mut impossible = sparrow_model::RowBatchBuilder::new(
            Arc::new(input),
            owner.clone(),
            CreditKind::Reservation,
            1,
            1,
        )
        .unwrap();
        assert!(op
            .apply_row(&row("a", Scalar::Float64(2.0)), 1, &mut impossible)
            .is_err());
        assert_eq!(op.next_deadline(), Some(10));
        let key = StateKey::new(OperatorId::new(4), IOT_STATE_SLOT, vec![Scalar::utf8("a")]);
        assert_eq!(
            op.state.get(&key).unwrap().values,
            vec![Scalar::Float64(1.0)]
        );
        assert_eq!(owner.usage().physical_bytes, before);
    }

    #[test]
    fn k4_iot_checks_structural_batch_schema_before_state_mutation() {
        let owner = owner();
        let input = schema(DataType::Float64, false);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 0),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        let mut wrong = input.clone();
        wrong.fields[1].name = "different_sensor".into();
        assert!(op
            .on_batch(
                &batch(&owner, &wrong, vec![row("a", Scalar::Float64(1.0))]),
                0
            )
            .is_err());
        assert_eq!(op.key_count(), 0);
        let equivalent = Schema::new(919, input.fields.clone()).unwrap();
        assert!(op
            .on_batch(
                &batch(&owner, &equivalent, vec![row("a", Scalar::Float64(1.0))]),
                0
            )
            .unwrap()
            .is_some());
    }

    #[test]
    fn k4_freeze_golden_roundtrip_and_corruption_are_strict() {
        let freeze = IotFreeze::new(
            OperatorId::new(7),
            IOT_CHANGE_KIND,
            vec![IotEntry {
                key: vec![Scalar::utf8("a")],
                values: vec![Scalar::Int64(42)],
            }],
        )
        .unwrap();
        let encoded = freeze.encode().unwrap();
        let mut golden = Vec::new();
        golden.extend_from_slice(&7u32.to_le_bytes());
        golden.extend_from_slice(&3u16.to_le_bytes());
        golden.push(IOT_CHANGE_KIND);
        golden.extend_from_slice(&1u32.to_le_bytes());
        golden.extend_from_slice(&1u16.to_le_bytes());
        golden.push(5);
        golden.extend_from_slice(&1u32.to_le_bytes());
        golden.push(b'a');
        golden.extend_from_slice(&1u16.to_le_bytes());
        golden.push(2);
        golden.extend_from_slice(&42i64.to_le_bytes());
        assert_eq!(encoded, golden);
        assert_eq!(IotFreeze::decode(&encoded, 4).unwrap(), freeze);
        let mut framed = encoded.as_slice();
        let scanned = IotFreeze::decode_mode(&mut framed, 4, false).unwrap();
        assert!(scanned.entries.is_empty());
        assert!(framed.is_empty());
        assert_eq!(
            IotFreeze::decode(&encoded[..encoded.len() - 1], 4)
                .unwrap_err()
                .code,
            ErrorCode::CodecViolation
        );
        let mut bad = encoded.clone();
        bad[4] = 4;
        assert_eq!(
            IotFreeze::decode(&bad, 4).unwrap_err().code,
            ErrorCode::CodecViolation
        );
    }

    #[test]
    fn k4_freeze_rejects_empty_keys_in_constructor_and_decoder() {
        assert_eq!(
            IotFreeze::new(
                OperatorId::new(7),
                IOT_CHANGE_KIND,
                vec![IotEntry {
                    key: vec![],
                    values: vec![Scalar::Int64(42)],
                }],
            )
            .unwrap_err()
            .code,
            ErrorCode::BoundExceeded
        );

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&7u32.to_le_bytes());
        encoded.extend_from_slice(&3u16.to_le_bytes());
        encoded.push(IOT_CHANGE_KIND);
        encoded.extend_from_slice(&1u32.to_le_bytes());
        encoded.extend_from_slice(&0u16.to_le_bytes());
        encoded.extend_from_slice(&1u16.to_le_bytes());
        encoded.push(2);
        encoded.extend_from_slice(&42i64.to_le_bytes());
        assert_eq!(
            IotFreeze::decode(&encoded, 4).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
    }

    #[test]
    fn k4_restore_is_atomic_and_detached() {
        let owner = owner();
        let input = schema(DataType::Int64, false);
        let mut op = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 0),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        op.on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 0)
            .unwrap();
        let freeze = op.freeze().unwrap();
        let mut restored = IotOperator::new(
            OperatorId::new(4),
            spec("value", true, 0),
            input.clone(),
            owner.clone(),
        )
        .unwrap();
        restored.restore(&freeze).unwrap();
        assert!(restored
            .on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(1))]), 1)
            .unwrap()
            .is_none());
        assert_eq!(
            restored
                .on_batch(&batch(&owner, &input, vec![row("a", Scalar::Int64(2))]), 1)
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        let mut malformed = freeze.clone();
        malformed.entries[0].values[0] = Scalar::Null;
        assert!(restored.restore(&malformed).is_err());
        assert_eq!(restored.key_count(), 1);
    }

    #[test]
    fn k4_ttl_logical_timer_bound_and_bucket_capacity_are_accounted() {
        let mut budget = ResourceBudget::compact();
        budget.max_timers = 2;
        let owner = MemoryOwner::new(budget);
        let input = schema(DataType::Int64, false);
        let mut op = IotOperator::new(4.into(), spec("value", true, 10), input.clone(), owner.clone()).unwrap();
        for (key, at) in [("a", 0), ("b", 1)] {
            op.on_batch(&batch(&owner, &input, vec![row(key, Scalar::Int64(1))]), at).unwrap();
        }
        let table_high_water = op.table_lease.as_ref().unwrap().bytes();
        assert_eq!(op.pending_timers(), 2);
        let error = op.on_batch(&batch(&owner, &input, vec![row("c", Scalar::Int64(1))]), 2).unwrap_err();
        assert!(error.message.contains("timer bound"));
        assert_eq!(op.pending_timers(), 2);
        op.expire(10);
        assert_eq!(op.key_count(), 1);
        assert_eq!(op.table_lease.as_ref().unwrap().bytes(), table_high_water);
        assert!(op.retention_bytes() >= table_high_water);
        assert_eq!(op.retention_bytes(), owner.usage().retention_bytes);
        op.expire(11);
        assert_eq!(op.key_count(), 0);
        assert_eq!(op.pending_timers(), 0);
        assert!(op.table_lease.is_none());
        assert_eq!(op.retention_bytes(), 0);
        assert_eq!(owner.usage().retention_bytes, 0);
        op.on_batch(&batch(&owner, &input, vec![row("c", Scalar::Int64(1))]), 12).unwrap();
        op.cleanup();
        assert_eq!(owner.usage().retention_bytes, 0);
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn k4_table_capacity_admission_rolls_back_on_entry_failure() {
        for existing in [0, 4] {
            let owner = owner();
            let input = schema(DataType::Int64, false);
            let mut op = IotOperator::new(4.into(), spec("value", true, 0), input.clone(), owner.clone()).unwrap();
            for key in 0..existing {
                op.on_batch(&batch(&owner, &input, vec![row(&key.to_string(), Scalar::Int64(1))]), 0).unwrap();
            }
            let old_table = op.table_lease.as_ref().map_or(0, MemoryLease::bytes);
            let extra = table_bytes(existing + 1) - old_table;
            let blocker = owner.acquire(CreditKind::Retention,
                owner.budget().retention_bytes - owner.usage().retention_bytes - extra).unwrap();
            let before = owner.usage().retention_bytes;
            assert!(op.on_batch(&batch(&owner, &input, vec![row("new", Scalar::Int64(1))]), 1).is_err());
            assert_eq!(op.key_count(), existing);
            assert_eq!(op.table_lease.as_ref().map_or(0, MemoryLease::bytes), old_table);
            assert_eq!(owner.usage().retention_bytes, before);
            drop(blocker);
            drop(op);
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    }
}

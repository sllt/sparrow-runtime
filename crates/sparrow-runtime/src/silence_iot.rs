//! Charged silence state over observed keys plus a bounded static registry.
//!
//! Silence is judged from fresh *source* observations, never from telemetry
//! rows: a key is silent when no input arrived within `duration_micros` of the
//! verified coverage, and it leaves silence only when an input actually
//! arrives. The runtime never invents coverage. The owner of the outer profile
//! passes an already-verified `coverage_since` through
//! [`SilenceIot::observe_feed`]; every round starts unauthorized until that
//! decision arrives, and `set_time` alone never triggers an event.
//!
//! Deadlines are stored raw (`last_seen.unwrap_or(0) + duration`) and the
//! verified coverage contributes a global floor, so replacing coverage costs
//! one `max`, not an O(keys) rescan of the timer index.
//!
//! One round spans `set_time` → optional input → the coverage decision. The
//! machine enforces that order itself: input needs an open round, a decision
//! needs an open round and closes it exactly once, and no freeze, restore or
//! cut validation succeeds while a round is open. Input and drain must carry
//! the round's own time decision, so only `set_time` (a new round) or
//! `validate_cut` (a restored cut) may move the clock: a health fact verified
//! at one time can never be applied at another. Bootstrap (before the first
//! `set_time`) and the closed state after `restore`/`validate_cut` may freeze.
//! A caller that loses the health control therefore cannot commit state it
//! never observed.

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::Arc;

use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SparrowError,
};
use sparrow_plan::{InvalidValuePolicy, IotSpec, IotTimingSpec};

#[cfg(test)]
use crate::iot::IotEntry;
use crate::iot::{IotFreeze, IotStats, MAX_IOT_FREEZE_BYTES, MAX_IOT_FREEZE_ENTRIES};

/// Freeze kind for [`IotSpec`] silence state on paused logical time.
pub(crate) const KIND: u8 = 12;
/// Fixed metadata values per key: `silent`, `episode`, `last_seen`.
const PREFIX: usize = 3;
/// Output columns after the keys: event, generation, operator, episode, time,
/// last_seen, never_seen.
const EVENT_FIELDS: usize = 7;
const NEVER_SEEN: i64 = -1;
/// The plan bounds one registry to 1024 rows and 64 KiB of canonical bytes; the
/// materialised helper result is charged against the same bound before it runs.
const MAX_REGISTERED_KEYS: usize = 1024;
const MAX_REGISTRY_BYTES: usize = 64 * 1024;

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

/// Strict metadata rules shared by live state, the shared freeze validator and
/// the codec: `last_seen` is `NEVER_SEEN` or a real cut within the cut, a
/// silent key has started an episode, and a never-seen key has none.
pub(crate) fn validate_state(
    silent: bool,
    episode: u64,
    last_seen: i64,
    now: Option<i64>,
) -> Result<()> {
    if last_seen < NEVER_SEEN
        || now.is_some_and(|now| last_seen > now)
        || (silent && episode == 0)
        || (!silent && last_seen == NEVER_SEEN && episode != 0)
    {
        return Err(invalid("silence state metadata/cut mismatch"));
    }
    Ok(())
}

fn values3(values: &[Scalar], now: Option<i64>) -> Result<(bool, u64, i64)> {
    let [Scalar::Bool(silent), Scalar::UInt64(episode), Scalar::Int64(last_seen)] = values else {
        return Err(invalid("silence state metadata types"));
    };
    validate_state(*silent, *episode, *last_seen, now)?;
    Ok((*silent, *episode, *last_seen))
}

/// Validate one freeze entry from the shared IoT codec.
pub(crate) fn metadata(values: &[Scalar], now: Option<i64>) -> Result<()> {
    values3(values, now).map(|_| ())
}

fn prefix(silent: bool, episode: u64, last_seen: Option<i64>) -> [Scalar; PREFIX] {
    [
        Scalar::Bool(silent),
        Scalar::UInt64(episode),
        Scalar::Int64(last_seen.unwrap_or(NEVER_SEEN)),
    ]
}

/// Exact canonical key length, computed without allocating.
fn encoded_estimate(key: &[Scalar]) -> Result<usize> {
    key.iter()
        .try_fold(0usize, |n, value| {
            value
                .encoded_value_len()
                .ok()
                .and_then(|len| n.checked_add(len.saturating_add(1)))
        })
        .ok_or_else(|| invalid("silence key encoding overflow"))
}

/// Retained estimate for one observed key: the detached key copy plus its
/// encoded copies in the state map and the timer index. The registration
/// map's own copies and node slack are charged by the long-lived configuration
/// lease instead, so they survive `cleanup`. Charged before the copy is made.
fn entry_bytes(key: &[Scalar]) -> Result<usize> {
    let resident = key.iter().map(Scalar::resident_bytes).sum::<usize>();
    Ok(resident
        .saturating_add(encoded_estimate(key)?.saturating_mul(2))
        .saturating_add(2048))
}

/// Bounded workspace for the planner's registry helper: `key_arity` scalars per
/// admitted key plus the canonical byte cap. Charged before that helper
/// allocates its rows (both when an operator validates and when its silence
/// state materialises the registry).
pub(crate) fn registry_workspace(spec: &IotSpec, key_arity: usize) -> usize {
    let Some(IotTimingSpec::Silence {
        registered_keys, ..
    }) = &spec.timing
    else {
        return 0;
    };
    let count = registered_keys
        .len()
        .min(spec.max_keys)
        .min(MAX_REGISTERED_KEYS);
    // Invalid oversize shapes are rejected by the planner before any scalar
    // copy; valid payload totals cannot exceed the canonical registry cap.
    let payload = registered_keys
        .iter()
        .take(count)
        .flat_map(|row| row.iter().take(16))
        .fold(0usize, |bytes, value| {
            bytes.saturating_add(value.as_str().map_or_else(
                || {
                    value
                        .as_array()
                        .map_or(9, |items| items.len().saturating_add(5))
                },
                |text| text.len().saturating_add(5),
            ))
        })
        .min(MAX_REGISTRY_BYTES);
    count
        .saturating_mul(
            key_arity
                .max(1)
                .min(16)
                .saturating_mul(size_of::<Scalar>() + 64)
                .saturating_add(256),
        )
        .saturating_add(payload.saturating_mul(4))
        .saturating_add(8192)
}

/// Retained slack for one registration-map node, on top of its encoded key
/// bytes. The map outlives the entries, so its allocation is covered by the
/// long-lived configuration lease rather than by an entry lease.
const REGISTRY_NODE_SLACK: usize = 128;

/// Shallow, allocation-free estimate of the operator's resident JSON registry.
///
/// Only legal scalar shapes (string, number, bool, bytes array) are measured;
/// any other shape is charged the fixed per-value slack and is then rejected by
/// the planner's own validation. The traversal never recurses, so a hostile
/// nested object cannot make this estimate unbounded. It is charged in addition
/// to the converted Scalar registry, because both collections exist at once.
pub(crate) fn registry_json_bytes(spec: &IotSpec) -> usize {
    spec.silence_registry_resident_bytes()
}

struct Entry {
    key: Vec<Scalar>,
    last_seen: Option<i64>,
    silent: bool,
    episode: u64,
    _lease: MemoryLease,
}

impl Entry {
    /// Coverage-independent deadline from the key's own record.
    fn raw_deadline(&self, duration: i64) -> Result<i64> {
        self.last_seen
            .unwrap_or(0)
            .checked_add(duration)
            .ok_or_else(|| invalid("silence deadline overflow"))
    }
}

pub(crate) struct SilenceIot {
    operator: OperatorId,
    input: Arc<Schema>,
    output: Arc<Schema>,
    owner: Arc<MemoryOwner>,
    keys: Vec<usize>,
    duration: i64,
    max: usize,
    now: i64,
    /// One round spans `set_time` → optional input → the coverage decision.
    /// Nothing may freeze, restore or validate a cut while a round is open, so
    /// a caller that loses the health control cannot publish state.
    round_open: bool,
    /// Coverage authorized by the current round's fresh observation decision.
    coverage: Option<i64>,
    generation: Option<[u8; 16]>,
    retired_generation: Option<[u8; 16]>,
    /// Static registration: canonical encoded key → typed key material. It
    /// proves that a restore carries every configured key and lets a cleaned
    /// machine re-establish its registered keys on the next bind, so it is
    /// retained (and charged) for the whole attempt.
    registered_keys: BTreeMap<Vec<u8>, Vec<Scalar>>,
    /// Set by `cleanup` only: the next successful bind re-creates the
    /// registered never-seen entries instead of leaving a registry-less state.
    registry_rebuild: bool,
    // Entry leases cover the timer-index key copies. Drop the index first,
    // including on an unwind that bypasses explicit cleanup.
    timers: BTreeSet<(i64, Vec<u8>)>,
    entries: BTreeMap<Vec<u8>, Entry>,
    stats: IotStats,
    _metadata: MemoryLease,
}

impl SilenceIot {
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
        let Some(IotTimingSpec::Silence {
            duration_micros, ..
        }) = spec.timing
        else {
            return Err(invalid("silence operator requires silence configuration"));
        };
        if duration_micros <= 0
            || !fields.is_empty()
            || spec.ttl_micros != 0
            || spec.emit_first
            || spec.invalid != InvalidValuePolicy::Error
        {
            return Err(invalid(
                "silence requires a positive duration, no value fields, TTL 0, no first emission and invalid=error",
            ));
        }
        // The planner helper allocates its registry rows, so the bounded
        // workspace is charged before it runs.
        let _workspace = owner.acquire(
            CreditKind::Reservation,
            registry_workspace(spec, keys.len()),
        )?;
        let registered = spec.silence_registered_keys(&input)?;
        // Exactly one state and one timer slot per registered key.
        let max = spec
            .max_keys
            .min(owner.budget().max_state_keys)
            .min(owner.budget().max_timers)
            .min(MAX_IOT_FREEZE_ENTRIES);
        if max == 0 || registered.len() > max {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "silence requires one state and timer slot per registered key",
            ));
        }
        // Charge the retained object, the planner's registry rows, the schema
        // names, the retained registration map and the index slack before
        // materialising any of them. The registration map lives in this lease,
        // not in an entry's, so it survives cleanup and keeps the contract.
        let registry_scalars = registered
            .iter()
            .map(|key| key.iter().map(Scalar::resident_bytes).sum::<usize>())
            .sum::<usize>();
        let registry_encoded = registered
            .iter()
            .try_fold(0usize, |n, key| {
                encoded_estimate(key)
                    .ok()
                    .and_then(|bytes| n.checked_add(bytes))
            })
            .ok_or_else(|| invalid("silence registry encoding overflow"))?;
        let metadata = owner.acquire(
            CreditKind::Reservation,
            size_of::<Self>()
                .saturating_add(8192)
                .saturating_add(registry_scalars)
                .saturating_add(registry_encoded)
                .saturating_add(
                    input
                        .fields
                        .iter()
                        .map(|field| field.name.len() * 2 + 256)
                        .sum::<usize>(),
                )
                .saturating_add(
                    keys.len()
                        .saturating_add(1)
                        .saturating_add(registered.len())
                        .saturating_mul(16),
                )
                // Each registration-map node keeps its encoded bytes charged
                // here so it survives the entries that share that copy.
                .saturating_add(registered.len().saturating_mul(REGISTRY_NODE_SLACK)),
        )?;
        let output = Arc::new(spec.output_schema(&input)?);
        let mut entries = BTreeMap::new();
        let mut timers = BTreeSet::new();
        let mut registered_keys = BTreeMap::new();
        for key in &registered {
            // Charge the retained entry (key copy plus its state-map and timer
            // copies) before materialising any of it.
            let lease = owner.acquire(CreditKind::Retention, entry_bytes(key)?)?;
            let encoded = crate::iot::encoded_key(key)?;
            let entry = Entry {
                key: key.iter().map(Scalar::detach_copy).collect(),
                last_seen: None,
                silent: false,
                episode: 0,
                _lease: lease,
            };
            timers.insert((entry.raw_deadline(duration_micros)?, encoded.clone()));
            let typed = key.iter().map(Scalar::detach_copy).collect::<Vec<_>>();
            if entries.insert(encoded.clone(), entry).is_some()
                || registered_keys.insert(encoded, typed).is_some()
            {
                return Err(invalid("duplicate silence registered key"));
            }
        }
        Ok(Self {
            operator,
            input,
            output,
            owner,
            keys,
            duration: duration_micros,
            max,
            now: 0,
            round_open: false,
            coverage: None,
            generation: None,
            retired_generation: None,
            registered_keys,
            registry_rebuild: false,
            timers,
            entries,
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
                "silence generation must be initialized once before restore/input",
            ));
        }
        // A reset keeps the registration contract: before any input, restore or
        // freeze can succeed in the new namespace, the registered never-seen
        // entries and their timers are re-established. A normal bind (restore
        // path) rebuilds nothing and keeps the state it already has.
        if self.registry_rebuild {
            self.rebuild_registry()?;
        }
        self.generation = Some(generation);
        Ok(())
    }

    /// Re-create the registered never-seen entries after a reset. The whole set
    /// is materialised with its own credit before any of it is committed.
    fn rebuild_registry(&mut self) -> Result<()> {
        let mut rebuilt = Vec::with_capacity(self.registered_keys.len());
        for (encoded, key) in &self.registered_keys {
            let lease = self
                .owner
                .acquire(CreditKind::Retention, entry_bytes(key)?)?;
            let entry = Entry {
                key: key.iter().map(Scalar::detach_copy).collect(),
                last_seen: None,
                silent: false,
                episode: 0,
                _lease: lease,
            };
            rebuilt.push((entry.raw_deadline(self.duration)?, encoded.clone(), entry));
        }
        for (deadline, encoded, entry) in rebuilt {
            self.timers.insert((deadline, encoded.clone()));
            self.entries.insert(encoded, entry);
        }
        self.registry_rebuild = false;
        Ok(())
    }
    fn generation(&self) -> Result<[u8; 16]> {
        self.generation
            .ok_or_else(|| invalid("silence generation is not bound"))
    }

    /// Move the logical clock forward. Only opening a round or loading a
    /// restored cut may do this: input and drain must carry the round's own
    /// decision time, so a stale health fact cannot be applied later.
    fn advance(&mut self, now: i64) -> Result<()> {
        if now < 0 || now < self.now {
            return Err(invalid("silence time moved backwards"));
        }
        self.now = now;
        Ok(())
    }

    /// Begin a round: the clock may only move forward and the previous round's
    /// coverage can no longer authorize a timer. Time itself never emits, and
    /// an unfinished round must be closed by its coverage decision first.
    pub fn set_time(&mut self, now: i64) -> Result<()> {
        if self.round_open {
            return Err(invalid(
                "silence round is not finished by its coverage decision",
            ));
        }
        self.advance(now)?;
        self.coverage = None;
        self.round_open = true;
        Ok(())
    }

    /// Close this round with the outer profile's verified coverage.
    ///
    /// `Some(since)` authorizes silence decisions inside the observed cut;
    /// `None` is a data-only decision and merely removes the authorization
    /// without disturbing the recorded state or episode. A refused decision
    /// leaves the round open for a corrected one.
    pub fn observe_feed(&mut self, coverage_since: Option<i64>) -> Result<()> {
        self.generation()?;
        if !self.round_open {
            return Err(invalid(
                "silence coverage decision requires a round opened by set_time",
            ));
        }
        match coverage_since {
            Some(since) => {
                if since < 0 || since > self.now {
                    return Err(invalid("silence coverage must lie within the observed cut"));
                }
                // The kernel consults next_deadline before draining. Reject
                // here instead of hiding an overflow behind a missing timer.
                since
                    .checked_add(self.duration)
                    .ok_or_else(|| invalid("silence coverage deadline overflow"))?;
                self.coverage = Some(since);
            }
            None => self.coverage = None,
        }
        self.round_open = false;
        Ok(())
    }

    /// The current round's coverage floor: the earliest cut at which coverage
    /// alone can make any key due. `Ok(None)` means unknown coverage and an
    /// unrepresentable floor is an explicit error rather than a wrap.
    fn grace(&self) -> Result<Option<i64>> {
        match self.coverage {
            Some(since) => since
                .checked_add(self.duration)
                .map(Some)
                .ok_or_else(|| invalid("silence coverage deadline overflow")),
            None => Ok(None),
        }
    }

    fn row_key(&self, row: &Row) -> Result<Vec<Scalar>> {
        if row.values.len() != self.input.fields.len() {
            return Err(invalid("silence row width differs from the input schema"));
        }
        for (value, field) in row.values.iter().zip(&self.input.fields) {
            if (value.is_null() && !field.nullable)
                || (!value.is_null() && !value.matches_type(&field.data_type))
            {
                return Err(invalid("silence row/schema mismatch"));
            }
        }
        let mut key = Vec::with_capacity(self.keys.len());
        for &index in &self.keys {
            let value = &row.values[index];
            if value.is_null() || !value.matches_type(&self.input.fields[index].data_type) {
                return Err(invalid("silence key must be a valid non-null key"));
            }
            key.push(value.detach_copy());
        }
        Ok(key)
    }

    fn output_row(
        &self,
        key: &[Scalar],
        event: &str,
        episode: u64,
        at: i64,
        last_seen: Option<i64>,
        never_seen: bool,
    ) -> Result<Row> {
        use std::fmt::Write;
        let mut generation = String::with_capacity(32);
        for byte in self.generation()? {
            write!(&mut generation, "{byte:02x}").expect("String write");
        }
        let mut values = Vec::with_capacity(key.len() + EVENT_FIELDS);
        values.extend(key.iter().map(Scalar::detach_copy));
        values.extend([
            Scalar::utf8(event),
            Scalar::utf8(generation),
            Scalar::UInt64(u64::from(self.operator.raw())),
            Scalar::UInt64(episode),
            Scalar::Int64(at),
            last_seen.map_or(Scalar::Null, Scalar::Int64),
            Scalar::Bool(never_seen),
        ]);
        Ok(Row { values })
    }

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Option<RowBatch>> {
        self.generation()?;
        if !self.round_open {
            return Err(invalid("silence input requires a round opened by set_time"));
        }
        // Only the round's own time decision may be used: an input stamped with
        // another time would borrow a health fact that was never observed then.
        if now != self.now {
            return Err(invalid(
                "silence input must use the round's own time decision",
            ));
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
            // Charge the row's key materialisation before detaching anything.
            let _scratch = self.owner.acquire(
                CreditKind::Reservation,
                row.resident_bytes().saturating_mul(4).saturating_add(2048),
            )?;
            let key = self.row_key(row)?;
            let encoded = crate::iot::encoded_key(&key)?;
            let deadline = now
                .checked_add(self.duration)
                .ok_or_else(|| invalid("silence deadline overflow"))?;
            // The observed key set is the static registry plus every legal key
            // ever seen: a new key is admitted once here, with no event, and is
            // judged from its own record afterwards.
            if !self.entries.contains_key(&encoded) {
                if self.entries.len() >= self.max {
                    return Err(SparrowError::new(
                        ErrorCode::ResourceExhausted,
                        "silence key bound; states cannot silently expire",
                    ));
                }
                let lease = self
                    .owner
                    .acquire(CreditKind::Retention, entry_bytes(&key)?)?;
                self.timers.insert((deadline, encoded.clone()));
                self.entries.insert(
                    encoded,
                    Entry {
                        key,
                        last_seen: Some(now),
                        silent: false,
                        episode: 0,
                        _lease: lease,
                    },
                );
                self.stats.filtered_rows = self.stats.filtered_rows.saturating_add(1);
                continue;
            }
            // Pre-validate the transition and build the row before touching
            // state: a refused emission must not consume the episode.
            let (raw, resumed, episode) = {
                let entry = self
                    .entries
                    .get(&encoded)
                    .ok_or_else(|| invalid("silence input without state"))?;
                let raw = entry.raw_deadline(self.duration)?;
                if entry.silent {
                    // A silent key leaves only on its own input, in the same
                    // episode; a source reconnect is not a resumed record.
                    if self.timers.contains(&(raw, encoded.clone())) {
                        return Err(invalid("silent silence key must not hold a timer"));
                    }
                } else if !self.timers.contains(&(raw, encoded.clone())) {
                    return Err(invalid("silence timer index is out of sync"));
                }
                (raw, entry.silent, entry.episode)
            };
            let resumed_row = if resumed {
                Some(self.output_row(&key, "resumed", episode, now, Some(now), false)?)
            } else {
                None
            };
            if let Some(row) = resumed_row {
                out.push(row)?;
            }
            // Commit. Everything fallible (deadline, index check, row build and
            // admission) already succeeded above.
            if !resumed && !self.timers.remove(&(raw, encoded.clone())) {
                return Err(invalid("silence timer index is out of sync"));
            }
            self.timers.insert((deadline, encoded.clone()));
            let entry = self
                .entries
                .get_mut(&encoded)
                .expect("silence key validated above");
            entry.last_seen = Some(now);
            entry.silent = false;
            if resumed {
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

    /// Emit at most one silence event: the smallest raw deadline first, with
    /// the canonical key breaking ties. It runs only at the round's own time
    /// and without verified coverage nothing is emitted at all.
    pub fn take_due(&mut self, now: i64) -> Result<Option<RowBatch>> {
        self.generation()?;
        // A drain may only run at the time of the decision it is using; the
        // clock moves when a round opens, never here.
        if now != self.now {
            return Err(invalid(
                "silence drain must use the round's own time decision",
            ));
        }
        let Some(grace) = self.grace()? else {
            return Ok(None);
        };
        let Some((raw, encoded)) = self.timers.first() else {
            return Ok(None);
        };
        let raw = *raw;
        if raw.max(grace) > now {
            return Ok(None);
        }
        // Size and charge the retained key copies and the emitted row before
        // cloning or detaching anything, using the selected key's real size.
        let resident = self
            .entries
            .get(encoded)
            .ok_or_else(|| invalid("silence timer without state"))?
            .key
            .iter()
            .map(Scalar::resident_bytes)
            .sum::<usize>();
        let key_bytes = encoded.len();
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            resident
                .saturating_mul(3)
                .saturating_add(key_bytes.saturating_mul(2))
                .saturating_add(4096),
        )?;
        let encoded = encoded.clone();
        // Pre-validate the transition without mutating anything.
        let (episode, key, last_seen) = {
            let entry = self
                .entries
                .get(&encoded)
                .ok_or_else(|| invalid("silence timer without state"))?;
            if entry.silent {
                return Err(invalid("silent silence key must not hold a timer"));
            }
            (
                entry
                    .episode
                    .checked_add(1)
                    .ok_or_else(|| invalid("silence episode overflow"))?,
                entry
                    .key
                    .iter()
                    .map(Scalar::detach_copy)
                    .collect::<Vec<_>>(),
                entry.last_seen,
            )
        };
        // Build and admit the whole row before touching state: a refused
        // emission must not consume the silence episode.
        let mut builder = RowBatchBuilder::new(
            self.output.clone(),
            self.owner.clone(),
            CreditKind::Reservation,
            1,
            self.owner.budget().reservation_bytes,
        )?;
        builder.push(self.output_row(
            &key,
            "silent",
            episode,
            now,
            last_seen,
            last_seen.is_none(),
        )?)?;
        let batch = builder.finish()?;
        // Commit. Only infallible mutations remain.
        let entry = self
            .entries
            .get_mut(&encoded)
            .expect("silence key validated above");
        entry.silent = true;
        entry.episode = episode;
        self.timers.remove(&(raw, encoded));
        self.stats.emitted_rows = self.stats.emitted_rows.saturating_add(1);
        Ok(Some(batch))
    }

    /// Validate the loaded state against a cut without opening a round: the
    /// kernel may run this twice at startup, and the first new `set_time`
    /// afterwards starts the next round. The machine stays closed and
    /// unauthorized, so nothing can be decided from an unobserved cut.
    pub fn validate_cut(&mut self, now: i64) -> Result<()> {
        self.generation()?;
        if self.round_open {
            return Err(invalid("silence cut validation requires a finished round"));
        }
        for entry in self.entries.values() {
            if entry.last_seen.is_some_and(|at| at < 0 || at > now)
                || (entry.silent && entry.episode == 0)
                || (!entry.silent && entry.last_seen.is_none() && entry.episode != 0)
            {
                return Err(invalid("silence state metadata/cut mismatch"));
            }
        }
        self.coverage = None;
        self.advance(now)
    }

    /// Effective next deadline, or `None` while coverage is unknown or the
    /// floor is unrepresentable. A `None` result only postpones decisions; the
    /// drain reports the unrepresentable floor as an explicit error.
    pub fn next_deadline(&self) -> Option<i64> {
        let grace = self.coverage?.checked_add(self.duration)?;
        let raw = self.timers.first()?.0;
        Some(raw.max(grace))
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
    pub fn retention_bytes(&self) -> usize {
        self.entries.values().map(|e| e._lease.bytes()).sum()
    }
    pub fn cleanup(&mut self) {
        // The index's key copies are charged by the entry leases, so drop it
        // first; the registry stays because it is the registration contract,
        // not observed state, and the next bind re-establishes its entries.
        self.timers = BTreeSet::new();
        self.entries = BTreeMap::new();
        self.coverage = None;
        self.round_open = false;
        self.registry_rebuild = !self.registered_keys.is_empty();
        // A reset cannot reuse episodes in the old namespace.
        if let Some(generation) = self.generation.take() {
            self.retired_generation = Some(generation);
        }
    }
    pub fn estimated_freeze_bytes(&self) -> usize {
        self.entries.values().fold(11usize, |n, entry| {
            n.saturating_add(4 + PREFIX * 9).saturating_add(
                entry
                    .key
                    .iter()
                    .map(|value| value.encoded_value_len().unwrap_or(usize::MAX / 128))
                    .sum::<usize>(),
            )
        })
    }
    pub fn encode(&self, out: &mut Vec<u8>, max: usize) -> Result<()> {
        self.generation()?;
        if self.round_open {
            return Err(invalid(
                "silence freeze requires a round finished by its coverage decision",
            ));
        }
        if self.entries.len() > max || self.estimated_freeze_bytes() > MAX_IOT_FREEZE_BYTES {
            return Err(invalid("silence freeze bound"));
        }
        let _scratch = self.owner.acquire(CreditKind::Reservation, 8192)?;
        let start = out.len();
        out.extend_from_slice(&self.operator.raw().to_le_bytes());
        out.extend_from_slice(&crate::iot::IOT_STATE_SLOT.raw().to_le_bytes());
        out.push(KIND);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in self.entries.values() {
            let values = prefix(entry.silent, entry.episode, entry.last_seen);
            crate::iot::encode_entry_parts(out, &entry.key, &values, start)?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn freeze(&self) -> Result<IotFreeze> {
        self.generation()?;
        if self.round_open {
            return Err(invalid(
                "silence freeze requires a round finished by its coverage decision",
            ));
        }
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            self.retention_bytes()
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        let entries = self
            .entries
            .values()
            .map(|entry| IotEntry {
                key: entry.key.iter().map(Scalar::detach_copy).collect(),
                values: prefix(entry.silent, entry.episode, entry.last_seen).to_vec(),
            })
            .collect();
        IotFreeze::new(self.operator, KIND, entries)
    }
    /// Validate the whole frozen registry and index before replacing anything:
    /// a refused restore leaves no half state behind.
    pub fn restore(&mut self, freeze: &IotFreeze) -> Result<()> {
        self.generation()?;
        if self.round_open {
            return Err(invalid(
                "silence restore requires a round finished by its coverage decision",
            ));
        }
        if freeze.operator != self.operator
            || freeze.kind != KIND
            || freeze.entries.len() > self.max
        {
            return Err(invalid("silence restore identity/bound"));
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
        for frozen in &freeze.entries {
            let (silent, episode, last_seen) = values3(&frozen.values, None)?;
            if frozen.key.len() != self.keys.len() {
                return Err(invalid("silence restore key arity"));
            }
            for (value, &index) in frozen.key.iter().zip(&self.keys) {
                if value.is_null() || !value.matches_type(&self.input.fields[index].data_type) {
                    return Err(invalid("silence restore key/schema mismatch"));
                }
            }
            let encoded = crate::iot::encoded_key(&frozen.key)?;
            // Every registered key is required. Keys observed beyond the static
            // registry are admitted, but only with a real cut: an extra key can
            // never be a never-seen one.
            if !self.registered_keys.contains_key(&encoded) && last_seen == NEVER_SEEN {
                return Err(invalid("silence restore admits only observed extra keys"));
            }
            let lease = self
                .owner
                .acquire(CreditKind::Retention, entry_bytes(&frozen.key)?)?;
            let entry = Entry {
                key: frozen.key.iter().map(Scalar::detach_copy).collect(),
                last_seen: (last_seen != NEVER_SEEN).then_some(last_seen),
                silent,
                episode,
                _lease: lease,
            };
            // Only a non-silent key holds a timer.
            if !silent {
                timers.insert((entry.raw_deadline(self.duration)?, encoded.clone()));
            }
            if entries.insert(encoded, entry).is_some() {
                return Err(invalid("duplicate silence key"));
            }
        }
        if self
            .registered_keys
            .keys()
            .any(|registered| !entries.contains_key(registered))
        {
            return Err(invalid("silence restore is missing a registered key"));
        }
        // Drop the old timer index before releasing the old entries' credit:
        // the index's key copies are charged by those entry leases.
        self.timers = timers;
        self.entries = entries;
        self.coverage = None;
        Ok(())
    }
}

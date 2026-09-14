//! Task-owned [`MemoryState`]: keyed entries on the retention ledger.
//!
//! Values are **detached copies**. Input [`RowBatch`] buffers are never pinned
//! into state. Quotas fail the job instead of growing.

use std::collections::HashMap;
use std::sync::Arc;

use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OperatorId, Result, Scalar, SparrowError,
    StateSlotId,
};

/// Stable address for a keyed state entry (operator + slot + detached key).
#[derive(Clone, Debug)]
pub struct StateKey {
    pub operator: OperatorId,
    pub slot: StateSlotId,
    pub key: Vec<Scalar>,
    encoded: Vec<u8>,
    index_lease: Option<Arc<MemoryLease>>,
}

impl PartialEq for StateKey {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.slot == other.slot && self.encoded == other.encoded
    }
}

impl Eq for StateKey {}

impl std::hash::Hash for StateKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.operator.hash(state);
        self.slot.hash(state);
        self.encoded.hash(state);
    }
}

impl StateKey {
    pub fn new(operator: OperatorId, slot: StateSlotId, key: Vec<Scalar>) -> Self {
        let key: Vec<Scalar> = key.into_iter().map(|s| s.detach_copy()).collect();
        let mut encoded = Vec::new();
        for s in &key {
            s.encode_key(&mut encoded);
            encoded.push(0xff);
        }
        Self {
            operator,
            slot,
            key,
            encoded,
            index_lease: None,
        }
    }

    pub fn tracked_bytes(&self) -> usize {
        // Include Vec capacities, detached nested payloads and map-node/slack
        // allowance, rather than using Scalar's serialized/logical estimate.
        self.key.iter().fold(
            std::mem::size_of::<Self>()
                .saturating_add(128)
                .saturating_add(self.encoded.capacity())
                .saturating_add(
                    self.key
                        .capacity()
                        .saturating_sub(self.key.len())
                        .saturating_mul(std::mem::size_of::<Scalar>()),
                ),
            |n, v| n.saturating_add(v.resident_bytes()),
        )
    }

    /// Index owns another key and encoded sort key plus B-tree overhead.
    /// Acquire before cloning; lease follows index removal/cleanup.
    pub fn indexed(&self, owner: &Arc<MemoryOwner>) -> Result<Self> {
        let bytes = self
            .tracked_bytes()
            .saturating_add(self.encoded.len())
            .saturating_add(128);
        let lease = owner.acquire(CreditKind::Retention, bytes)?;
        let mut key = self.clone();
        key.index_lease = Some(Arc::new(lease));
        Ok(key)
    }

    pub fn index_bytes(&self) -> usize {
        self.index_lease.as_ref().map_or(0, |l| l.bytes())
    }

    pub fn encoded_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

#[derive(Debug)]
struct Entry<V> {
    value: V,
    key_bytes: usize,
    // Rust drops fields in declaration order: release payload before credit.
    lease: MemoryLease,
}

/// Per-operator, task-owned map. One task owns one instance (no sharing).
pub struct MemoryState<V> {
    owner: Arc<MemoryOwner>,
    operator: OperatorId,
    slot: StateSlotId,
    max_keys: usize,
    entries: HashMap<StateKey, Entry<V>>,
    retained_bytes: usize,
}

impl<V> MemoryState<V> {
    pub fn new(
        owner: Arc<MemoryOwner>,
        operator: OperatorId,
        slot: StateSlotId,
        max_keys: usize,
    ) -> Result<Self> {
        if max_keys == 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "state max_keys must be > 0 (unbounded state is rejected)",
            ));
        }
        let max_keys = max_keys.min(owner.budget().max_state_keys);
        Ok(Self {
            owner,
            operator,
            slot,
            max_keys,
            entries: HashMap::new(),
            retained_bytes: 0,
        })
    }

    pub fn operator(&self) -> OperatorId {
        self.operator
    }

    pub fn slot(&self) -> StateSlotId {
        self.slot
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    pub fn retention_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn get(&self, key: &StateKey) -> Option<&V> {
        self.entries.get(key).map(|e| &e.value)
    }

    pub fn get_mut(&mut self, key: &StateKey) -> Option<&mut V> {
        self.entries.get_mut(key).map(|e| &mut e.value)
    }

    /// Variable-size production update: admit the complete replacement before
    /// cloning/evaluating, retain old state on every failure. The caller's
    /// bound includes candidate containers and detached payloads.
    pub fn update_bounded<F>(&mut self, key: &StateKey, value_bound: usize, update: F) -> Result<()>
    where
        V: Clone,
        F: FnOnce(&mut V) -> Result<usize>,
    {
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(());
        };
        let lease = self.owner.acquire(
            CreditKind::Retention,
            entry.key_bytes.saturating_add(value_bound),
        )?;
        let mut candidate = entry.value.clone();
        let actual = update(&mut candidate)?;
        if actual > value_bound {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "state replacement exceeds admitted bound",
            ));
        }
        let old_bytes = entry.lease.bytes();
        let key_bytes = entry.key_bytes;
        *entry = Entry {
            lease,
            value: candidate,
            key_bytes,
        };
        entry
            .lease
            .shrink_to(key_bytes.saturating_add(actual).max(1))?;
        self.retained_bytes = self.retained_bytes - old_bytes + entry.lease.bytes();
        Ok(())
    }

    /// Runtime-only update. Caller owns pre-admitted temporary storage, builds
    /// a complete candidate, grows this exclusive lease before replacement and
    /// drops the old payload before any refund. Use the resident key, not the
    /// query key's (possibly different) Vec capacity.
    pub(crate) fn update_accounted<F>(&mut self, key: &StateKey, update: F) -> Result<()>
    where
        F: FnOnce(&mut V, &mut MemoryLease, usize) -> Result<()>,
    {
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(());
        };
        let before = entry.lease.bytes();
        let result = update(&mut entry.value, &mut entry.lease, entry.key_bytes);
        self.retained_bytes = self.retained_bytes - before + entry.lease.bytes();
        result
    }

    /// Insert or replace. `value_bytes` is the detached payload size (not the
    /// input batch). Replacing drops the previous retention lease.
    pub fn put(&mut self, key: StateKey, value: V, value_bytes: usize) -> Result<()> {
        if key.operator != self.operator || key.slot != self.slot {
            return Err(
                SparrowError::new(ErrorCode::Internal, "state key operator/slot mismatch")
                    .at_operator(self.operator),
            );
        }
        if !self.entries.contains_key(&key) && self.entries.len() >= self.max_keys {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "state key quota exceeded: {} keys (max {})",
                    self.entries.len(),
                    self.max_keys
                ),
            )
            .retryable(false)
            .at_operator(self.operator)
            .context("max_keys", self.max_keys.to_string()));
        }
        let key_bytes = self.entries.get_key_value(&key).map_or_else(
            || key.tracked_bytes(),
            |(resident, _)| resident.tracked_bytes(),
        );
        let bytes = key_bytes.saturating_add(value_bytes).max(1);
        let lease = self.owner.acquire(CreditKind::Retention, bytes)?;
        let old = self.entries.insert(
            key,
            Entry {
                lease,
                value,
                key_bytes,
            },
        );
        self.retained_bytes =
            self.retained_bytes.saturating_add(bytes) - old.as_ref().map_or(0, |e| e.lease.bytes());
        Ok(())
    }

    pub fn remove(&mut self, key: &StateKey) -> Option<V> {
        self.entries.remove(key).map(|e| {
            self.retained_bytes -= e.lease.bytes();
            e.value
        })
    }

    /// Re-bill retention after a variable-size in-place update (MIN/MAX strings).
    /// Growing values must acquire a new lease; shrinking keeps the existing one.
    #[cfg(test)]
    pub fn recharge(&mut self, key: &StateKey, value_bytes: usize) -> Result<()> {
        let Some(entry) = self.entries.get(key) else {
            return Ok(());
        };
        let bytes = key.tracked_bytes().saturating_add(value_bytes).max(1);
        if entry.lease.bytes() >= bytes {
            return Ok(());
        }
        let lease = self.owner.acquire(CreditKind::Retention, bytes)?;
        if let Some(entry) = self.entries.get_mut(key) {
            self.retained_bytes = self.retained_bytes - entry.lease.bytes() + lease.bytes();
            entry.lease = lease;
        }
        Ok(())
    }

    pub fn retain<F: FnMut(&StateKey, &V) -> bool>(&mut self, mut pred: F) {
        self.entries.retain(|k, e| {
            let keep = pred(k, &e.value);
            if !keep {
                self.retained_bytes -= e.lease.bytes();
            }
            keep
        });
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StateKey, &V)> {
        self.entries.iter().map(|(k, e)| (k, &e.value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &StateKey> {
        self.entries.keys()
    }

    /// Drop every entry and return retention credits (job stop / window close).
    pub fn clear(&mut self) {
        self.entries.clear();
        self.retained_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{ResourceBudget, Scalar};

    #[test]
    fn r10_cached_retention_matches_entries_through_mutations_and_errors() {
        let owner = owner();
        let op = OperatorId::new(1);
        let slot = StateSlotId::new(1);
        let mut state = MemoryState::new(owner.clone(), op, slot, 4).unwrap();
        let keys: Vec<_> = (0..4)
            .map(|i| StateKey::new(op, slot, vec![Scalar::Int64(i)]))
            .collect();
        let check = |s: &MemoryState<usize>| {
            let exact: usize = s.entries.values().map(|e| e.lease.bytes()).sum();
            assert_eq!(s.retention_bytes(), exact);
            assert_eq!(owner.usage().physical_bytes, exact);
        };
        for round in 0..20 {
            for key in &keys {
                state.put(key.clone(), round, 64).unwrap();
                check(&state);
                state
                    .update_bounded(key, 256, |v| {
                        *v += 1;
                        Ok(8)
                    })
                    .unwrap();
                check(&state);
                assert!(state
                    .update_accounted(key, |_, lease, key_bytes| {
                        lease.grow_to(key_bytes + 128)?;
                        Err(SparrowError::new(
                            ErrorCode::Cancelled,
                            "injected after growth",
                        ))
                    })
                    .is_err());
                check(&state);
                state
                    .update_accounted(key, |_, lease, key_bytes| lease.shrink_to(key_bytes + 8))
                    .unwrap();
                check(&state);
            }
            state.remove(&keys[0]);
            check(&state);
            state.retain(|key, _| key != &keys[1]);
            check(&state);
            state.clear();
            check(&state);
        }
    }

    #[test]
    fn production_variable_update_admits_before_copy_and_preserves_old_value() {
        let owner = MemoryOwner::new(ResourceBudget {
            retention_bytes: 4096,
            ..ResourceBudget::compact()
        });
        let key = StateKey::new(
            OperatorId::new(1),
            StateSlotId::new(1),
            vec![Scalar::utf8("key")],
        );
        let mut state = MemoryState::new(owner.clone(), key.operator, key.slot, 4).unwrap();
        state.put(key.clone(), String::from("old"), 64).unwrap();
        let before = owner.usage().physical_bytes;
        let called = std::cell::Cell::new(false);
        assert!(state
            .update_bounded(&key, 4096, |_| {
                called.set(true);
                Ok(1)
            })
            .is_err());
        assert!(!called.get(), "quota must be checked before callback/clone");
        assert_eq!(state.get(&key).unwrap(), "old");
        assert!(state
            .update_bounded(&key, 128, |v| {
                *v = "candidate".into();
                Err(SparrowError::new(ErrorCode::IntegerOverflow, "fixture"))
            })
            .is_err());
        assert_eq!(state.get(&key).unwrap(), "old");
        assert_eq!(owner.usage().physical_bytes, before);
        state.clear();
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    fn owner() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget {
            reservation_bytes: 4096,
            retention_bytes: 4096,
            queue_bytes: 4096,
            max_rows: 16,
            work_units: 100,
            max_state_keys: 4,
            max_timers: 8,
        })
    }

    #[test]
    fn detach_does_not_share_input_utf8() {
        let input = Scalar::utf8("live-batch");
        let key = StateKey::new(OperatorId::new(1), StateSlotId::new(1), vec![input.clone()]);
        match (&input, &key.key[0]) {
            (Scalar::Utf8(a), Scalar::Utf8(b)) => {
                assert!(
                    !std::sync::Arc::ptr_eq(a, b),
                    "state must not alias input Arc"
                );
            }
            _ => panic!("expected utf8"),
        }
    }

    #[test]
    fn quota_rejects_unbounded_keys() {
        let mut st = MemoryState::new(owner(), OperatorId::new(7), StateSlotId::new(1), 2).unwrap();
        let op = OperatorId::new(7);
        let slot = StateSlotId::new(1);
        st.put(StateKey::new(op, slot, vec![Scalar::Int64(1)]), 1u8, 8)
            .unwrap();
        st.put(StateKey::new(op, slot, vec![Scalar::Int64(2)]), 2u8, 8)
            .unwrap();
        let err = st
            .put(StateKey::new(op, slot, vec![Scalar::Int64(3)]), 3u8, 8)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        st.remove(&StateKey::new(op, slot, vec![Scalar::Int64(1)]));
        assert_eq!(st.len(), 1);
        st.clear();
        assert_eq!(st.len(), 0);
        assert_eq!(st.retention_bytes(), 0);
    }

    #[test]
    fn r05_recharge_grows_retention_for_variable_state() {
        let mut st = MemoryState::new(owner(), OperatorId::new(1), StateSlotId::new(1), 4).unwrap();
        let key = StateKey::new(
            OperatorId::new(1),
            StateSlotId::new(1),
            vec![Scalar::Int64(1)],
        );
        st.put(key.clone(), 1u8, 8).unwrap();
        let before = st.retention_bytes();
        st.recharge(&key, 2048).unwrap();
        assert!(st.retention_bytes() > before);
    }
}

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
        }
    }

    pub fn tracked_bytes(&self) -> usize {
        16 + self.key.iter().map(Scalar::tracked_bytes).sum::<usize>()
    }

    pub fn encoded_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

#[derive(Debug)]
struct Entry<V> {
    lease: MemoryLease,
    value: V,
}

/// Per-operator, task-owned map. One task owns one instance (no sharing).
pub struct MemoryState<V> {
    owner: Arc<MemoryOwner>,
    operator: OperatorId,
    slot: StateSlotId,
    max_keys: usize,
    entries: HashMap<StateKey, Entry<V>>,
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
        Ok(Self {
            owner,
            operator,
            slot,
            max_keys,
            entries: HashMap::new(),
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
        self.entries.values().map(|e| e.lease.bytes()).sum()
    }

    pub fn get(&self, key: &StateKey) -> Option<&V> {
        self.entries.get(key).map(|e| &e.value)
    }

    pub fn get_mut(&mut self, key: &StateKey) -> Option<&mut V> {
        self.entries.get_mut(key).map(|e| &mut e.value)
    }

    /// Insert or replace. `value_bytes` is the detached payload size (not the
    /// input batch). Replacing drops the previous retention lease.
    pub fn put(&mut self, key: StateKey, value: V, value_bytes: usize) -> Result<()> {
        if key.operator != self.operator || key.slot != self.slot {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                "state key operator/slot mismatch",
            )
            .at_operator(self.operator));
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
        let bytes = key.tracked_bytes().saturating_add(value_bytes).max(1);
        let lease = self.owner.acquire(CreditKind::Retention, bytes)?;
        self.entries.insert(key, Entry { lease, value });
        Ok(())
    }

    pub fn remove(&mut self, key: &StateKey) -> Option<V> {
        self.entries.remove(key).map(|e| e.value)
    }

    /// Re-bill retention after a variable-size in-place update (MIN/MAX strings).
    /// Growing values must acquire a new lease; shrinking keeps the existing one.
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
            entry.lease = lease;
        }
        Ok(())
    }

    pub fn retain<F: FnMut(&StateKey, &V) -> bool>(&mut self, mut pred: F) {
        self.entries.retain(|k, e| pred(k, &e.value));
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{ResourceBudget, Scalar};

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
        let key = StateKey::new(
            OperatorId::new(1),
            StateSlotId::new(1),
            vec![input.detach_copy()],
        );
        match (&input, &key.key[0]) {
            (Scalar::Utf8(a), Scalar::Utf8(b)) => {
                assert!(!std::sync::Arc::ptr_eq(a, b), "state must not alias input Arc");
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
        let key = StateKey::new(OperatorId::new(1), StateSlotId::new(1), vec![Scalar::Int64(1)]);
        st.put(key.clone(), 1u8, 8).unwrap();
        let before = st.retention_bytes();
        st.recharge(&key, 2048).unwrap();
        assert!(st.retention_bytes() > before);
    }
}

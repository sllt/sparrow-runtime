//! Bounded processing-time deduplicate. Forever / unbounded configs are rejected.

use std::collections::BTreeMap;
use std::sync::Arc;

use sparrow_model::{
    ErrorCode, MemoryOwner, OperatorId, Result, Row, RowBatch, Scalar, Schema, SparrowError,
    StateSlotId,
};
use sparrow_plan::DedupSpec;

use crate::state::{MemoryState, StateKey};
use crate::window::{finish_rows, resolve_keys};

const SLOT: u16 = 2;

#[derive(Clone, Debug)]
struct Seen {
    last_seen: i64,
}

pub struct DedupOperator {
    operator: OperatorId,
    spec: DedupSpec,
    key_idx: Vec<usize>,
    input: Schema,
    state: MemoryState<Seen>,
    owner: Arc<MemoryOwner>,
    /// `(expire_at, encoded_key) → StateKey`. `expire` pops the due prefix
    /// instead of scanning the whole map (P2-32).
    expiry: BTreeMap<(i64, Vec<u8>), StateKey>,
    last_expire_probes: usize,
}

impl DedupOperator {
    pub fn new(
        operator: OperatorId,
        spec: DedupSpec,
        input: Schema,
        owner: Arc<MemoryOwner>,
    ) -> Result<Self> {
        spec.validate()?;
        let key_idx = resolve_keys(&input, &spec.keys)?;
        let state = MemoryState::new(
            Arc::clone(&owner),
            operator,
            StateSlotId::new(SLOT),
            spec.max_keys,
        )?;
        Ok(Self {
            operator,
            spec,
            key_idx,
            input,
            state,
            owner,
            expiry: BTreeMap::new(),
            last_expire_probes: 0,
        })
    }

    /// How many expiry-heap probes the last [`Self::expire`] performed.
    /// A no-op tick probes once (the live min); a full-map scan would be O(n).
    pub fn last_expire_probes(&self) -> usize {
        self.last_expire_probes
    }

    pub fn expiry_index_len(&self) -> usize {
        self.expiry.len()
    }

    pub fn key_count(&self) -> usize {
        self.state.len()
    }

    pub fn retention_bytes(&self) -> usize {
        self.state.retention_bytes() + self.expiry.values().map(StateKey::index_bytes).sum::<usize>()
    }

    pub fn on_batch(&mut self, batch: &RowBatch, now: i64) -> Result<Vec<Row>> {
        self.expire(now);
        let mut keep = Vec::new();
        for row in batch.rows() {
            if self.admit(row, now)? {
                keep.push(Row {
                    values: row.values.iter().map(Scalar::detach_copy).collect(),
                });
            }
        }
        Ok(keep)
    }

    fn admit(&mut self, row: &Row, now: i64) -> Result<bool> {
        let key: Vec<Scalar> = self
            .key_idx
            .iter()
            .map(|&i| row.values[i].detach_copy())
            .collect();
        let sk = StateKey::new(self.operator, StateSlotId::new(SLOT), key);
        if let Some(seen) = self.state.get(&sk) {
            if now.saturating_sub(seen.last_seen) < self.spec.ttl_micros {
                return Ok(false);
            }
            let last_seen = seen.last_seen;
            self.unindex_expiry(last_seen, &sk);
            self.state.remove(&sk);
        }
        self.state
            .put(sk.clone(), Seen { last_seen: now }, 16)?;
        self.index_expiry(now, sk)?;
        Ok(true)
    }

    fn expire_at(&self, last_seen: i64) -> i64 {
        last_seen.saturating_add(self.spec.ttl_micros)
    }

    fn index_expiry(&mut self, last_seen: i64, sk: StateKey) -> Result<()> {
        let sk = sk.indexed(&self.owner)?;
        let encoded = sk.encoded_bytes().to_vec();
        self.expiry.insert((self.expire_at(last_seen), encoded), sk);
        Ok(())
    }

    fn unindex_expiry(&mut self, last_seen: i64, sk: &StateKey) {
        self.expiry
            .remove(&(self.expire_at(last_seen), sk.encoded_bytes().to_vec()));
    }

    fn expire(&mut self, now: i64) {
        self.last_expire_probes = 0;
        loop {
            let Some((&(exp, _), _)) = self.expiry.first_key_value() else {
                break;
            };
            self.last_expire_probes = self.last_expire_probes.saturating_add(1);
            if exp > now {
                break;
            }
            let Some((_, sk)) = self.expiry.pop_first() else {
                break;
            };
            self.state.remove(&sk);
        }
    }

    pub fn build_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows(&self.input, rows, &self.owner)
    }

    pub fn cleanup(&mut self) {
        self.state.clear();
        self.expiry.clear();
        self.last_expire_probes = 0;
    }
}

/// Reject configs that omit TTL or max_keys (forever-dedup).
pub fn reject_unbounded_dedup(
    ttl_micros: Option<i64>,
    max_keys: Option<usize>,
) -> Result<DedupSpec> {
    let ttl = ttl_micros.ok_or_else(|| {
        SparrowError::new(
            ErrorCode::InvalidArgument,
            "unbounded forever-dedup is rejected: ttl_micros is required",
        )
    })?;
    let max_keys = max_keys.ok_or_else(|| {
        SparrowError::new(
            ErrorCode::BoundExceeded,
            "unbounded forever-dedup is rejected: max_keys is required",
        )
    })?;
    let spec = DedupSpec {
        keys: vec!["id".into()],
        ttl_micros: ttl,
        max_keys,
    };
    spec.validate()?;
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{
        CreditKind, DataType, Field, FieldId, MemoryOwner, ResourceBudget, RowBatchBuilder,
        SchemaId,
    };

    fn owner() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget::compact())
    }

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "id", DataType::Utf8, false)],
        )
        .unwrap()
    }

    fn op(ttl: i64, max_keys: usize) -> DedupOperator {
        DedupOperator::new(
            OperatorId::new(2),
            DedupSpec {
                keys: vec!["id".into()],
                ttl_micros: ttl,
                max_keys,
            },
            schema(),
            owner(),
        )
        .unwrap()
    }

    fn batch(ids: &[&str], owner: &Arc<MemoryOwner>) -> RowBatch {
        let schema = Arc::new(schema());
        let mut b = RowBatchBuilder::new(
            schema,
            Arc::clone(owner),
            CreditKind::Reservation,
            ids.len().max(1),
            64 * 1024,
        )
        .unwrap();
        for id in ids {
            b.push(Row {
                values: vec![Scalar::utf8(*id)],
            })
            .unwrap();
        }
        b.finish().unwrap()
    }

    #[test]
    fn p2_32_expire_is_prefix_not_full_scan() {
        let owner = owner();
        let mut d = op(100, 256);
        let early: Vec<&str> = ["e0", "e1", "e2"].to_vec();
        d.on_batch(&batch(&early, &owner), 0).unwrap();
        let late: Vec<String> = (0..61).map(|i| format!("l{i}")).collect();
        let late_refs: Vec<&str> = late.iter().map(String::as_str).collect();
        d.on_batch(&batch(&late_refs, &owner), 10).unwrap();
        assert_eq!(d.key_count(), 64);
        assert_eq!(d.expiry_index_len(), 64);
        // Nothing due at t=10: one probe of the min (expire_at=100).
        assert_eq!(d.last_expire_probes(), 1);

        let empty = batch(&[], &owner);
        let kept = d.on_batch(&empty, 100).unwrap();
        assert!(kept.is_empty());
        assert_eq!(d.key_count(), 61, "only the t=0 keys expire at 100");
        assert_eq!(
            d.last_expire_probes(),
            4,
            "3 due pops + 1 peek of the next live key, not 64"
        );
        assert_eq!(d.expiry_index_len(), 61);
    }

    #[test]
    fn p2_32_within_ttl_still_dedups() {
        let owner = owner();
        let mut d = op(100, 16);
        let first = d.on_batch(&batch(&["a"], &owner), 0).unwrap();
        let second = d.on_batch(&batch(&["a"], &owner), 50).unwrap();
        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        assert_eq!(d.key_count(), 1);
    }

    #[test]
    fn r3_expiry_index_is_billed_and_released() {
        let owner = owner();
        let mut d = DedupOperator::new(OperatorId::new(2), DedupSpec {
            keys: vec!["id".into()], ttl_micros: 10, max_keys: 16,
        }, schema(), owner.clone()).unwrap();
        d.on_batch(&batch(&["long-key-material"], &owner), 0).unwrap();
        assert!(d.retention_bytes() > d.state.retention_bytes());
        assert_eq!(d.retention_bytes(), owner.usage().retention_bytes);
        d.expire(10);
        assert_eq!(owner.usage().retention_bytes, 0);
        d.cleanup();
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn p2_32_expire_then_admit_same_key() {
        let owner = owner();
        let mut d = op(100, 16);
        d.on_batch(&batch(&["a"], &owner), 0).unwrap();
        let again = d.on_batch(&batch(&["a"], &owner), 100).unwrap();
        assert_eq!(again.len(), 1, "expired key must be admitted again");
        assert_eq!(d.key_count(), 1);
        assert_eq!(d.expiry_index_len(), 1);
    }
}

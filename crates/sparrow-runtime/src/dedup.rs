//! Bounded processing-time deduplicate. Forever / unbounded configs are rejected.

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
        })
    }

    pub fn key_count(&self) -> usize {
        self.state.len()
    }

    pub fn retention_bytes(&self) -> usize {
        self.state.retention_bytes()
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
        }
        self.state.put(sk, Seen { last_seen: now }, 16)?;
        Ok(true)
    }

    fn expire(&mut self, now: i64) {
        let ttl = self.spec.ttl_micros;
        self.state.retain(|_, s| now.saturating_sub(s.last_seen) < ttl);
    }

    pub fn build_batch(&self, rows: Vec<Row>) -> Result<Option<RowBatch>> {
        finish_rows(&self.input, rows, &self.owner)
    }

    pub fn cleanup(&mut self) {
        self.state.clear();
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

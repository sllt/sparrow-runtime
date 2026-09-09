//! Bounded timer heap with generation-based cancel.
//!
//! Rescheduling a key increments its generation; stale heap entries are
//! ignored. The live timer count (not stale tombstones) is capped.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use sparrow_model::{ErrorCode, OperatorId, Result, SparrowError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerId {
    pub operator: OperatorId,
    pub namespace: u64,
}

impl TimerId {
    pub fn window(operator: OperatorId, window_end: i64) -> Self {
        Self {
            operator,
            namespace: window_end as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HeapItem {
    at: i64,
    gen: u64,
    id: u64,
}

/// One pending timer per [`TimerId`]. Old generations are cancelled, not leaked.
pub struct BoundedTimers {
    operator: OperatorId,
    max: usize,
    gens: HashMap<TimerId, u64>,
    deadlines: HashMap<TimerId, i64>,
    heap: BinaryHeap<Reverse<(i64, u64, u64)>>, // (at, gen, namespace)
    cancelled: u64,
}

impl BoundedTimers {
    pub fn new(operator: OperatorId, max: usize) -> Result<Self> {
        if max == 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "max_timers must be > 0 (unbounded timer heaps are rejected)",
            ));
        }
        Ok(Self {
            operator,
            max,
            gens: HashMap::new(),
            deadlines: HashMap::new(),
            heap: BinaryHeap::new(),
            cancelled: 0,
        })
    }

    pub fn live(&self) -> usize {
        self.gens.len()
    }

    pub fn cancelled(&self) -> u64 {
        self.cancelled
    }

    pub fn heap_len(&self) -> usize {
        self.heap.len()
    }

    pub fn peek_deadline(&self) -> Option<i64> {
        self.deadlines.values().copied().min()
    }

    /// Schedule or replace. Replacing cancels the previous generation.
    pub fn schedule(&mut self, id: TimerId, fire_at: i64) -> Result<u64> {
        if id.operator != self.operator {
            return Err(SparrowError::new(
                ErrorCode::Internal,
                "timer operator mismatch",
            )
            .at_operator(self.operator));
        }
        let replacing = self.gens.contains_key(&id);
        if !replacing && self.gens.len() >= self.max {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "timer quota exceeded: {} live timers (max {})",
                    self.gens.len(),
                    self.max
                ),
            )
            .retryable(false)
            .at_operator(self.operator)
            .context("max_timers", self.max.to_string()));
        }
        let gen = self.gens.get(&id).copied().unwrap_or(0).saturating_add(1);
        if replacing {
            self.cancelled += 1;
        }
        self.gens.insert(id, gen);
        self.deadlines.insert(id, fire_at);
        self.heap.push(Reverse((fire_at, gen, id.namespace)));
        self.compact_if_needed();
        Ok(gen)
    }

    pub fn cancel(&mut self, id: TimerId) {
        if self.gens.remove(&id).is_some() {
            self.deadlines.remove(&id);
            self.cancelled += 1;
        }
    }

    pub fn cancel_all(&mut self) {
        self.cancelled += self.gens.len() as u64;
        self.gens.clear();
        self.deadlines.clear();
        self.heap.clear();
    }

    /// Pop timers with `fire_at <= now` whose generation is still current.
    pub fn fire_due(&mut self, now: i64) -> Vec<TimerId> {
        let mut out = Vec::new();
        while let Some(Reverse((at, gen, ns))) = self.heap.peek().copied() {
            if at > now {
                break;
            }
            self.heap.pop();
            let id = TimerId {
                operator: self.operator,
                namespace: ns,
            };
            match self.gens.get(&id) {
                Some(&g) if g == gen && self.deadlines.get(&id) == Some(&at) => {
                    self.gens.remove(&id);
                    self.deadlines.remove(&id);
                    out.push(id);
                }
                _ => {
                    // stale generation — already cancelled / replaced
                }
            }
        }
        out
    }

    fn compact_if_needed(&mut self) {
        // Stale heap entries from cancelled gens must not grow without bound.
        if self.heap.len() <= self.max.saturating_mul(2) {
            return;
        }
        let mut fresh = BinaryHeap::new();
        for (id, &gen) in &self.gens {
            if let Some(&at) = self.deadlines.get(id) {
                fresh.push(Reverse((at, gen, id.namespace)));
            }
        }
        self.heap = fresh;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_old_generation_does_not_fire() {
        let op = OperatorId::new(3);
        let mut t = BoundedTimers::new(op, 8).unwrap();
        let id = TimerId::window(op, 1_000);
        t.schedule(id, 1_000).unwrap();
        t.schedule(id, 2_000).unwrap();
        assert_eq!(t.live(), 1);
        assert_eq!(t.cancelled(), 1);
        assert!(t.fire_due(1_000).is_empty());
        let due = t.fire_due(2_000);
        assert_eq!(due, vec![id]);
        assert_eq!(t.live(), 0);
    }

    #[test]
    fn quota_rejects_unbounded_heap() {
        let op = OperatorId::new(1);
        let mut t = BoundedTimers::new(op, 2).unwrap();
        t.schedule(TimerId::window(op, 1), 10).unwrap();
        t.schedule(TimerId::window(op, 2), 20).unwrap();
        let err = t.schedule(TimerId::window(op, 3), 30).unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        // replacing an existing id must not grow live count
        t.schedule(TimerId::window(op, 1), 40).unwrap();
        assert_eq!(t.live(), 2);
        t.cancel_all();
        assert_eq!(t.live(), 0);
        assert_eq!(t.heap_len(), 0);
    }
}

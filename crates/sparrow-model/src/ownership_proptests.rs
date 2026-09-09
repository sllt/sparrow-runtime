//! Property-style tests for MemoryLease / builder bounds.
//!
//! Kept as a module (not a separate crate) so M0 has no extra test-harness
//! dependency. The generator is a tiny LCG so results are deterministic.

use crate::memory::MemoryOwner;
use crate::resource::{CreditKind, ResourceBudget};

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn random_share_detach_returns_to_zero() {
    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 50_000,
        retention_bytes: 50_000,
        queue_bytes: 50_000,
        max_rows: 64,
        work_units: 10_000,
    });
    for seed in 1u64..=32 {
        let mut rng = Lcg(seed * 17);
        let mut handles = Vec::new();
        for _ in 0..40 {
            match rng.below(4) {
                0 => {
                    let bytes = 16 + rng.below(128) as usize;
                    if let Ok(lease) = owner.acquire(CreditKind::Reservation, bytes) {
                        handles.push(lease);
                    }
                }
                1 if !handles.is_empty() => {
                    let i = rng.below(handles.len() as u64) as usize;
                    let extra = handles[i].share();
                    handles.push(extra);
                }
                2 if !handles.is_empty() => {
                    let i = rng.below(handles.len() as u64) as usize;
                    if let Ok(d) = handles[i].detach(CreditKind::Retention) {
                        handles.push(d);
                    }
                }
                _ if !handles.is_empty() => {
                    let i = rng.below(handles.len() as u64) as usize;
                    handles.swap_remove(i);
                }
                _ => {}
            }
        }
        handles.clear();
        let usage = owner.usage();
        assert_eq!(usage.physical_bytes, 0, "seed={seed}");
        assert_eq!(usage.live_handles, 0, "seed={seed}");
        assert_eq!(usage.reservation_bytes, 0, "seed={seed}");
        assert_eq!(usage.retention_bytes, 0, "seed={seed}");
    }
}

#[test]
fn expansion_peak_never_silently_unbounded() {
    use crate::batch::{Row, RowBatchBuilder};
    use crate::scalar::Scalar;
    use crate::types::{DataType, Field, Schema};
    use std::sync::Arc;

    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 2048,
        retention_bytes: 2048,
        queue_bytes: 2048,
        max_rows: 128,
        work_units: 100,
    });
    let schema = Arc::new(
        Schema::new(1, vec![Field::new(1, "s", DataType::Utf8, true)]).unwrap(),
    );
    const CAP: usize = 400;
    let mut b = RowBatchBuilder::new(
        schema,
        Arc::clone(&owner),
        CreditKind::Reservation,
        128,
        CAP,
    )
    .unwrap();
    let mut rng = Lcg(99);
    let mut accepted = 0usize;
    for _ in 0..200 {
        let len = 8 + rng.below(40) as usize;
        let s = "x".repeat(len);
        match b.push(Row {
            values: vec![Scalar::utf8(s)],
        }) {
            Ok(()) => accepted += 1,
            Err(_) => break,
        }
    }
    assert!(accepted > 0);
    assert!(
        owner.peak_builder_bytes() <= CAP + 64,
        "peak {} exceeded cap {CAP} without failing",
        owner.peak_builder_bytes()
    );
    assert!(b.current_bytes() <= CAP);
}

//! Byte-accounted connector ingress; no transport/runtime dependency.
use crate::{CreditKind, MemoryLease, MemoryOwner, Result, Row, RowBatchBuilder, SparrowError};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Default)]
pub struct QueueOccupancy {
    pub items: AtomicU64,
    pub bytes: AtomicU64,
    pub peak_items: AtomicU64,
    pub peak_bytes: AtomicU64,
}

#[derive(Debug)]
pub struct QueuedRow {
    row: Option<Row>,
    lease: MemoryLease,
    stats: Arc<QueueOccupancy>,
}

impl QueuedRow {
    /// Conservative allowance for all bounded-channel slots/blocks, retained
    /// for the receiver lifetime even after rows drain. Not an RSS measurement.
    pub fn channel_budget(capacity: usize) -> usize {
        capacity.saturating_add(32).saturating_mul(std::mem::size_of::<Self>() + 64).saturating_add(4096)
    }
    pub fn accounted_bytes(row: &Row) -> usize {
        row.resident_bytes()
            .saturating_add(std::mem::size_of::<Self>())
            .saturating_add(64)
    }

    pub fn try_new(
        row: Row,
        owner: &Arc<MemoryOwner>,
        stats: &Arc<QueueOccupancy>,
    ) -> std::result::Result<Self, (Row, SparrowError)> {
        let bytes = Self::accounted_bytes(&row);
        let lease = match owner.acquire(CreditKind::Queue, bytes) {
            Ok(lease) => lease,
            Err(error) => return Err((row, error)),
        };
        let items = stats.items.fetch_add(1, Ordering::Relaxed) + 1;
        let held = stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        stats.peak_items.fetch_max(items, Ordering::Relaxed);
        stats.peak_bytes.fetch_max(held, Ordering::Relaxed);
        Ok(Self {
            row: Some(row),
            lease,
            stats: stats.clone(),
        })
    }

    pub fn bytes(&self) -> usize {
        self.lease.bytes()
    }

    pub fn push_into(mut self, builder: &mut RowBatchBuilder) -> Result<()> {
        // self (and its Queue lease) survives the destination acquisition.
        builder.push_accounted(self.row.take().expect("queued row"), self.lease.bytes())
    }
}

impl Drop for QueuedRow {
    fn drop(&mut self) {
        self.stats.items.fetch_sub(1, Ordering::Relaxed);
        self.stats
            .bytes
            .fetch_sub(self.lease.bytes() as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataType, DynamicValue, Field, FieldId, ResourceBudget, Scalar, Schema};
    fn row() -> Row {
        Row {
            values: vec![Scalar::Int64(1)],
        }
    }
    fn schema() -> Arc<Schema> {
        Arc::new(
            Schema::new(
                1,
                vec![Field::new(FieldId::new(1), "n", DataType::Int64, false)],
            )
            .unwrap(),
        )
    }

    #[test]
    fn charges_decoded_capacity_nested_payloads_and_shared_values_conservatively() {
        let blob: Arc<[u8]> = vec![0; 4096].into();
        let mut values = Vec::with_capacity(64);
        values.push(Scalar::Dynamic(DynamicValue::Array(
            vec![DynamicValue::Bytes(blob.clone()), DynamicValue::Bytes(blob)].into(),
        )));
        let row = Row { values };
        assert!(row.resident_bytes() >= 8192 + 64 * std::mem::size_of::<Scalar>());
    }

    #[test]
    fn queue_and_destination_overlap_credit_and_release_on_success_or_failure() {
        for invalid in [false, true] {
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let stats = Arc::new(QueueOccupancy::default());
            let input = if invalid {
                Row {
                    values: vec![Scalar::Bool(true)],
                }
            } else {
                row()
            };
            let queued = QueuedRow::try_new(input, &owner, &stats).unwrap();
            let bytes = queued.bytes();
            assert_eq!(owner.usage().queue_bytes, bytes);
            let mut builder =
                RowBatchBuilder::new(schema(), owner.clone(), CreditKind::Reservation, 1, 4096)
                    .unwrap();
            assert_eq!(queued.push_into(&mut builder).is_err(), invalid);
            assert_eq!(owner.usage().queue_bytes, 0);
            assert_eq!(stats.items.load(Ordering::Relaxed), 0);
            if !invalid {
                assert_eq!(owner.usage().reservation_bytes, bytes);
            }
            drop(builder);
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    }

    #[test]
    fn process_budget_cannot_be_bypassed_by_sibling_ingress_queues() {
        let bytes = QueuedRow::accounted_bytes(&row());
        let mut budget = ResourceBudget::compact();
        budget.queue_bytes = bytes;
        let root = MemoryOwner::new(budget);
        let a = MemoryOwner::child(root.clone(), budget, "a");
        let b = MemoryOwner::child(root.clone(), budget, "b");
        let stats = Arc::new(QueueOccupancy::default());
        let held = QueuedRow::try_new(row(), &a, &stats).unwrap();
        assert!(QueuedRow::try_new(row(), &b, &stats).is_err());
        assert_eq!(b.usage().physical_bytes, 0);
        drop(held);
        drop(QueuedRow::try_new(row(), &b, &stats).unwrap());
        assert_eq!(root.usage().live_handles, 0);
        assert_eq!(stats.bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn failed_reservation_handoff_releases_the_queue_lease() {
        let mut budget = ResourceBudget::compact();
        budget.reservation_bytes = 1;
        let owner = MemoryOwner::new(budget);
        let stats = Arc::new(QueueOccupancy::default());
        let queued = QueuedRow::try_new(row(), &owner, &stats).unwrap();
        let mut builder =
            RowBatchBuilder::new(schema(), owner.clone(), CreditKind::Reservation, 1, 1).unwrap();
        assert!(queued.push_into(&mut builder).is_err());
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(stats.items.load(Ordering::Relaxed), 0);
    }
}

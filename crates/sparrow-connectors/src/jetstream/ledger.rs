//! Admission/publication/durable-cut are DISTINCT positions. Acker lifetime is
//! bounded by pending count AND original message bytes, not just SDK buffers.
use super::connection::error;
use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryIdentity {
    pub stream_sequence: u64,
    pub consumer_sequence: u64,
    pub payload_digest: [u8; 32],
    pub wire_bytes: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observation {
    New,
    PendingDuplicate,
    CommittedDuplicate,
}

struct Entry<T> {
    identity: DeliveryIdentity,
    published: bool,
    ack: T,
    confirming: bool,
    _lease: MemoryLease,
}

pub struct DeliveryLedger<T> {
    owner: Arc<MemoryOwner>,
    max_pending: usize,
    max_bytes: usize,
    bytes: usize,
    delivered: u64,
    received: u64,
    published: u64,
    committed: u64,
    entries: BTreeMap<u64, Entry<T>>,
}
impl<T> DeliveryLedger<T> {
    pub fn new(
        owner: Arc<MemoryOwner>,
        max_pending: usize,
        max_bytes: usize,
        committed: u64,
    ) -> Result<Self> {
        if !(1..=4096).contains(&max_pending)
            || !(4096..=16 * 1024 * 1024).contains(&max_bytes)
            || committed == u64::MAX
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "JetStream pending ledger bounds invalid",
            ));
        }
        Ok(Self {
            owner,
            max_pending,
            max_bytes,
            bytes: 0,
            delivered: 0,
            received: committed,
            published: committed,
            committed,
            entries: BTreeMap::new(),
        })
    }
    pub fn pending(&self) -> usize {
        self.entries.len()
    }
    pub fn pending_bytes(&self) -> usize {
        self.bytes
    }
    pub fn published(&self) -> u64 {
        self.published
    }
    pub fn committed(&self) -> u64 {
        self.committed
    }
    pub fn remaining_messages(&self) -> usize {
        self.max_pending - self.pending()
    }
    pub fn remaining_bytes(&self) -> usize {
        self.max_bytes - self.bytes
    }

    /// ALL deliveries, including duplicates, consume a consumer sequence. A
    /// dropped SDK frame must fail before a later sequence mutates operators.
    /// This initial profile consumes ALL stream subjects, so new stream cuts
    /// must also be contiguous. Subject filtering is explicitly not supported.
    pub fn observe(&mut self, id: DeliveryIdentity, ack: T) -> Result<(Observation, Option<T>)> {
        if self.delivered.checked_add(1) != Some(id.consumer_sequence)
            || id.stream_sequence == 0
            || id.wire_bytes == 0
        {
            return Err(error(
                ErrorCode::CodecViolation,
                "JetStream delivery sequence gap or invalid identity",
            ));
        }
        if id.stream_sequence <= self.committed {
            self.delivered = id.consumer_sequence;
            return Ok((Observation::CommittedDuplicate, Some(ack)));
        }
        if let Some(entry) = self.entries.get_mut(&id.stream_sequence) {
            if entry.identity.payload_digest != id.payload_digest
                || entry.identity.wire_bytes != id.wire_bytes
            {
                return Err(error(
                    ErrorCode::CodecViolation,
                    "JetStream redelivery content conflicts with pending source identity",
                ));
            }
            self.delivered = id.consumer_sequence;
            // Keep the newest reply token (consumer delivery sequence changes).
            return Ok((
                Observation::PendingDuplicate,
                Some(std::mem::replace(&mut entry.ack, ack)),
            ));
        }
        if self.received.checked_add(1) != Some(id.stream_sequence) {
            return Err(error(
                ErrorCode::UnsupportedRestore,
                "JetStream source sequence gap; refusing silent skip",
            ));
        }
        if self.entries.len() == self.max_pending || id.wire_bytes > self.remaining_bytes() {
            return Err(error(
                ErrorCode::ResourceExhausted,
                "JetStream pending count/byte limit reached",
            ));
        }
        let charge = 1024usize.checked_add(id.wire_bytes).ok_or_else(|| {
            error(
                ErrorCode::BoundExceeded,
                "JetStream pending charge overflow",
            )
        })?;
        // Charge BEFORE tree allocation. This deliberately includes retained
        // wire size although payload is dropped once the decoder hands off.
        let lease = self.owner.acquire(CreditKind::Retention, charge)?;
        self.entries.insert(
            id.stream_sequence,
            Entry {
                identity: id,
                published: false,
                ack,
                confirming: false,
                _lease: lease,
            },
        );
        self.bytes += id.wire_bytes;
        self.received = id.stream_sequence;
        self.delivered = id.consumer_sequence;
        Ok((Observation::New, None))
    }

    /// Only after inbox publication (or a verified durable poison disposition).
    /// Cancellation before send completion leaves this position unchanged.
    pub fn publish(&mut self, sequence: u64) -> Result<()> {
        if self.published.checked_add(1) != Some(sequence) {
            return Err(error(
                ErrorCode::CodecViolation,
                "JetStream publication is not a contiguous prefix",
            ));
        }
        let entry = self
            .entries
            .get_mut(&sequence)
            .ok_or_else(|| error(ErrorCode::Internal, "unadmitted JetStream publication"))?;
        entry.published = true;
        self.published = sequence;
        Ok(())
    }

    /// Call ONLY with a successfully published durable checkpoint's captured
    /// source cut. Does not remove anything: ACK failures retain their budget
    /// and can be retried without applying rows again.
    pub fn committed_checkpoint(&mut self, cut: u64) -> Result<()> {
        if cut < self.committed || cut > self.published {
            return Err(error(
                ErrorCode::CodecViolation,
                "JetStream checkpoint cut exceeds published prefix or regresses",
            ));
        }
        self.committed = cut;
        Ok(())
    }
    pub fn ackable(&self) -> impl Iterator<Item = (u64, &T)> {
        self.entries
            .range(..=self.committed)
            .map(|(seq, e)| (*seq, &e.ack))
    }
    pub fn unresolved(&self) -> impl Iterator<Item = &T> {
        self.entries.values().map(|e| &e.ack)
    }
    pub fn next_confirmation(&mut self) -> Option<(u64, &T)> {
        let (seq, entry) = self.entries.range_mut(..=self.committed).find(|(_, e)| !e.confirming)?;
        entry.confirming = true;
        Some((*seq, &entry.ack))
    }
    /// Called only after double_ack succeeds. The reply/token and its lease
    /// die together, never as an unaccounted Vec of detached pending ACKs.
    pub fn ack_confirmed(&mut self, sequence: u64) -> Result<()> {
        if sequence > self.committed || !self.entries.get(&sequence).is_some_and(|e| e.published) {
            return Err(error(
                ErrorCode::CodecViolation,
                "JetStream ACK is not checkpoint-confirmed",
            ));
        }
        let entry = self
            .entries
            .remove(&sequence)
            .expect("checked pending entry");
        self.bytes -= entry.identity.wire_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(s: u64, c: u64) -> DeliveryIdentity {
        DeliveryIdentity {
            stream_sequence: s,
            consumer_sequence: c,
            payload_digest: [s as u8; 32],
            wire_bytes: 1024,
        }
    }
    #[test]
    fn k2_pending_is_not_published_and_published_is_not_ackable() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let mut l = DeliveryLedger::new(owner.clone(), 4, 4096, 0).unwrap();
        l.observe(id(1, 1), 11).unwrap();
        l.observe(id(2, 2), 22).unwrap();
        assert_eq!(l.published(), 0);
        assert!(l.committed_checkpoint(1).is_err());
        assert!(l.publish(2).is_err());
        l.publish(1).unwrap();
        assert_eq!(l.ackable().count(), 0);
        assert!(l.ack_confirmed(1).is_err());
        l.committed_checkpoint(1).unwrap();
        assert_eq!(l.ackable().map(|(s, _)| s).collect::<Vec<_>>(), vec![1]);
        let bytes = owner.usage().physical_bytes;
        assert!(bytes >= 4096);
        l.ack_confirmed(1).unwrap();
        assert!(owner.usage().physical_bytes < bytes);
        assert_eq!(l.pending_bytes(), 1024);
        assert_eq!(l.committed(), 1);
        drop(l);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
    #[test]
    fn k2_duplicate_does_not_apply_twice_or_escape_budget() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let mut l = DeliveryLedger::new(owner.clone(), 1, 4096, 0).unwrap();
        l.observe(id(1, 1), 11).unwrap();
        l.publish(1).unwrap();
        let bytes = owner.usage().physical_bytes;
        assert_eq!(
            l.observe(id(1, 2), 12).unwrap(),
            (Observation::PendingDuplicate, Some(11))
        );
        assert_eq!(owner.usage().physical_bytes, bytes);
        assert!(l.publish(1).is_err());
        assert!(l.observe(id(2, 3), 23).is_err());
        assert_eq!(l.pending(), 1);
        l.committed_checkpoint(1).unwrap();
        l.ack_confirmed(1).unwrap();
        assert_eq!(
            l.observe(id(1, 3), 13).unwrap(),
            (Observation::CommittedDuplicate, Some(13))
        );
        l.observe(id(2, 4), 24).unwrap();
        assert!(l.observe(id(2, 6), 26).is_err());
        let mut conflict = id(2, 5);
        conflict.payload_digest = [0; 32];
        assert!(l.observe(conflict, 25).is_err());
    }
    #[test]
    fn k2_byte_cap_and_retention_gap_fail_before_cursor_advance() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let mut l = DeliveryLedger::new(owner.clone(), 8, 4096, 10).unwrap();
        assert!(l.observe(id(12, 1), ()).is_err());
        for n in 1..=4 {
            l.observe(id(10 + n, n), ()).unwrap();
            l.publish(10 + n).unwrap();
        }
        assert!(l.observe(id(15, 5), ()).is_err());
        assert_eq!(l.published(), 14);
        l.committed_checkpoint(12).unwrap();
        l.ack_confirmed(11).unwrap();
        l.observe(id(15, 5), ()).unwrap();
        assert!(l.committed_checkpoint(11).is_err());
        assert!(l.committed_checkpoint(15).is_err());
        drop(l);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

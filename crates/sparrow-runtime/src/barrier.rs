//! Aligned checkpoint barrier coordination on the Kernel path.
//!
//! Barriers travel with the mailbox. Stateful stages freeze; the sink waits
//! for a real outbox ack (not "channel empty") before the supervisor commits.
//! Acks carry `checkpoint_id`. Stale ids from a timed-out barrier must not
//! pair with a later cut.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sparrow_model::{ErrorCode, InflightCounter, Result, SparrowError};

use crate::window::WindowFreeze;

/// Encoded state plus its working-memory lease. Moving an ACK never clones state.
#[derive(Debug)]
pub struct EncodedFreeze {
    pub bytes: Vec<u8>,
    pub lease: sparrow_model::MemoryLease,
}

impl EncodedFreeze {
    pub fn from_operator(op: &crate::window::WindowOperator,
        owner: &Arc<sparrow_model::MemoryOwner>, max_keys: usize) -> Result<Self> {
        op.check_freeze_encode_bound(max_keys)?;
        let capacity = op.estimated_freeze_bytes().saturating_add(256);
        let lease = owner.acquire(sparrow_model::CreditKind::Reservation, capacity)?;
        let mut bytes = Vec::with_capacity(capacity);
        op.encode_freeze_into(&mut bytes, max_keys)?;
        if bytes.len() > capacity {
            return Err(SparrowError::new(ErrorCode::Internal, "freeze size estimate underflow"));
        }
        Ok(Self { bytes, lease })
    }
}

#[derive(Debug)]
pub enum AlignedAck {
    FreezeFailed {
        checkpoint_id: u64,
        error: SparrowError,
    },
    WindowFrozen {
        checkpoint_id: u64,
        freeze: EncodedFreeze,
    },
    SinkFlushed {
        checkpoint_id: u64,
        ok: bool,
        dropped: u64,
    },
}

impl AlignedAck {
    pub fn checkpoint_id(&self) -> u64 {
        match self {
            Self::WindowFrozen { checkpoint_id, .. } | Self::SinkFlushed { checkpoint_id, .. }
            | Self::FreezeFailed { checkpoint_id, .. } => {
                *checkpoint_id
            }
        }
    }
}

#[derive(Clone)]
pub struct AlignedJob {
    pub restore: Option<WindowFreeze>,
    pub acks: AlignedAcks,
    pub outbox: Arc<InflightCounter>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushOutcome {
    pub ok: bool,
    pub dropped: u64,
}

/// Only the active checkpoint owns an ACK inbox. Dropping its request on
/// timeout/cancellation closes that inbox, including senders already in flight.
/// Late snapshots are dropped immediately, even if no next checkpoint occurs.
#[derive(Clone, Default)]
pub struct AlignedAcks {
    active: Arc<Mutex<Option<(u64, tokio::sync::mpsc::Sender<AlignedAck>)>>>,
}

pub struct CheckpointAcks {
    registry: AlignedAcks,
    id: u64,
    receiver: tokio::sync::mpsc::Receiver<AlignedAck>,
}

impl AlignedAcks {
    pub fn begin(&self, id: u64) -> Result<CheckpointAcks> {
        let mut active = self.active.lock().expect("checkpoint ACK registry");
        if active.is_some() {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, "checkpoint already active"));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        *active = Some((id, sender));
        Ok(CheckpointAcks { registry: self.clone(), id, receiver })
    }

    pub fn is_active(&self, id: u64) -> bool {
        self.active.lock().expect("checkpoint ACK registry").as_ref()
            .is_some_and(|(active, _)| *active == id)
    }

    pub async fn send(&self, ack: AlignedAck) {
        let sender = self.active.lock().expect("checkpoint ACK registry").as_ref()
            .filter(|(id, _)| *id == ack.checkpoint_id()).map(|(_, tx)| tx.clone());
        if let Some(sender) = sender { let _ = sender.send(ack).await; }
    }
}

impl CheckpointAcks {
    pub async fn recv(&mut self) -> Option<AlignedAck> { self.receiver.recv().await }

    pub async fn wait(mut self, timeout: Duration) -> Result<BarrierAcks> {
        wait_aligned_acks(&mut self.receiver, self.id, timeout).await
    }
}

impl Drop for CheckpointAcks {
    fn drop(&mut self) {
        let mut active = self.registry.active.lock().expect("checkpoint ACK registry");
        if active.as_ref().is_some_and(|(id, _)| *id == self.id) { *active = None; }
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
    }
}

/// Acks accepted for one expected barrier id.
#[derive(Debug, Default)]
pub struct BarrierAcks {
    pub freeze: Option<EncodedFreeze>,
    pub freeze_id: Option<u64>,
    pub flushed: bool,
    pub flush_ok: bool,
    pub flush_id: Option<u64>,
    pub dropped: u64,
}

impl BarrierAcks {
    pub fn apply(&mut self, expected: u64, ack: AlignedAck) {
        match ack {
            AlignedAck::WindowFrozen {
                checkpoint_id,
                freeze,
            } if checkpoint_id == expected => {
                self.freeze = Some(freeze);
                self.freeze_id = Some(checkpoint_id);
            }
            AlignedAck::SinkFlushed {
                checkpoint_id,
                ok,
                dropped,
            } if checkpoint_id == expected => {
                self.flushed = true;
                self.flush_ok = ok;
                self.flush_id = Some(checkpoint_id);
                self.dropped = dropped;
            }
            // Stale or future id: drop. Never stitch a lower id onto this cut.
            _ => {}
        }
    }

    pub fn aligned_ok(&self, expected: u64) -> bool {
        self.freeze.is_some()
            && self.flushed
            && self.flush_ok
            && self.dropped == 0
            && self.freeze_id == Some(expected)
            && self.flush_id == Some(expected)
    }
}

pub async fn wait_outbox(outbox: &InflightCounter, timeout: Duration) -> FlushOutcome {
    outbox.request_flush();
    let deadline = Instant::now() + timeout;
    while outbox.pending() > 0 {
        if Instant::now() >= deadline {
            let dropped = outbox.drops_since_mark().max(outbox.pending());
            return FlushOutcome { ok: false, dropped };
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let dropped = outbox.drops_since_mark();
    FlushOutcome {
        ok: dropped == 0,
        dropped,
    }
}

/// Wait until freeze+successful flush for `expected` arrive. Ignores other ids.
/// Failed freeze/flush returns immediately. Production callers use the
/// request-scoped `CheckpointAcks::wait` to release queued and late ACKs.
pub async fn wait_aligned_acks(
    ack_rx: &mut tokio::sync::mpsc::Receiver<AlignedAck>,
    expected: u64,
    timeout: Duration,
) -> Result<BarrierAcks> {
    let mut got = BarrierAcks::default();
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline && !got.aligned_ok(expected) {
        match tokio::time::timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            ack_rx.recv(),
        )
        .await
        {
            Ok(Some(ack)) => {
                if let AlignedAck::FreezeFailed { checkpoint_id, error } = ack {
                    if checkpoint_id == expected { return Err(error); }
                    continue;
                }
                got.apply(expected, ack);
                if got.flushed && (!got.flush_ok || got.dropped > 0) { break; }
            }
            Ok(None) | Err(_) => break,
        }
    }
    if got.aligned_ok(expected) {
        return Ok(got);
    }
    Err(SparrowError::new(
        ErrorCode::ResourceExhausted,
        format!(
            "barrier did not align (freeze={} flush={} ok={} dropped={}); refusing dishonest commit",
            got.freeze.is_some(),
            got.flushed,
            got.flush_ok,
            got.dropped
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{OperatorId, StateSlotId};

    fn leased_ack(owner: &Arc<sparrow_model::MemoryOwner>, id: u64) -> AlignedAck {
        AlignedAck::WindowFrozen { checkpoint_id: id, freeze: EncodedFreeze {
            bytes: vec![0; 128],
            lease: owner.acquire(sparrow_model::CreditKind::Reservation, 128).unwrap(),
        } }
    }

    #[tokio::test]
    async fn r4_timeout_and_late_ack_release_without_another_checkpoint() {
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let registry = AlignedAcks::default();
        let request = registry.begin(1).unwrap();
        // The producer already owns a snapshot, but sends only after timeout.
        let late = leased_ack(&owner, 1);
        assert!(request.wait(Duration::from_millis(1)).await.is_err());
        assert!(!registry.is_active(1));
        registry.send(late).await;
        assert_eq!(owner.usage().physical_bytes, 0);

        // Frozen state already consumed by wait must also be dropped on timeout.
        let request = registry.begin(2).unwrap();
        registry.send(leased_ack(&owner, 2)).await;
        assert!(request.wait(Duration::from_millis(1)).await.is_err());
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn r4_failed_or_cancelled_request_drops_queued_and_inflight_snapshots() {
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let registry = AlignedAcks::default();
        for id in 1..=2 {
            let request = registry.begin(id).unwrap();
            let failure = if id == 1 {
                AlignedAck::FreezeFailed { checkpoint_id: id,
                    error: SparrowError::new(ErrorCode::BoundExceeded, "freeze refused") }
            } else { AlignedAck::SinkFlushed { checkpoint_id: id, ok: false, dropped: 1 } };
            registry.send(failure).await;
            registry.send(leased_ack(&owner, id)).await;
            assert!(request.wait(Duration::from_secs(1)).await.is_err());
            assert_eq!(owner.usage().physical_bytes, 0);
        }
        let request = registry.begin(3).unwrap();
        registry.send(leased_ack(&owner, 3)).await;
        registry.send(leased_ack(&owner, 3)).await;
        let tx = registry.clone();
        let extra = leased_ack(&owner, 3);
        let sending = tokio::spawn(async move { tx.send(extra).await });
        tokio::task::yield_now().await;
        assert!(!sending.is_finished(), "third ACK should wait on the bounded inbox");
        drop(request);
        sending.await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0);

        let request = registry.begin(4).unwrap();
        registry.send(leased_ack(&owner, 4)).await;
        let waiting = tokio::spawn(request.wait(Duration::from_secs(30)));
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert!(!registry.is_active(4));
        assert_eq!(owner.usage().physical_bytes, 0);

        let request = registry.begin(5).unwrap();
        registry.send(leased_ack(&owner, 4)).await; // cannot pair with new flush
        registry.send(leased_ack(&owner, 5)).await;
        registry.send(AlignedAck::SinkFlushed { checkpoint_id: 5, ok: true, dropped: 0 }).await;
        let accepted = request.wait(Duration::from_secs(1)).await.unwrap();
        assert_eq!(owner.usage().reservation_bytes, 128);
        drop(accepted);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn r3_pending_timeout_can_recover_without_forgiving_loss() {
        let outbox = InflightCounter::new();
        outbox.enqueue();
        assert!(!wait_outbox(&outbox, Duration::from_millis(1)).await.ok);
        outbox.ack();
        assert!(wait_outbox(&outbox, Duration::from_millis(1)).await.ok);
        outbox.enqueue();
        outbox.fail();
        for _ in 0..3 { assert!(!wait_outbox(&outbox, Duration::from_millis(1)).await.ok); }
    }

    fn encoded(freeze: WindowFreeze) -> EncodedFreeze {
        let mut bytes = Vec::new();
        crate::checkpoint::encode_freeze(&freeze, &mut bytes, 1024).unwrap();
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let lease = owner.acquire(sparrow_model::CreditKind::Reservation, bytes.capacity().max(1)).unwrap();
        EncodedFreeze { bytes, lease }
    }

    fn empty_freeze() -> WindowFreeze {
        WindowFreeze {
            operator: OperatorId::WINDOW,
            slot: StateSlotId::new(1),
            kind: 1,
            entries: Vec::new(),
            wm_in: None,
            wm_out: None,
            last_effective: None,
        }
    }

    fn leftover_freeze(count: u64) -> WindowFreeze {
        let mut f = empty_freeze();
        f.entries.push(crate::window::FrozenEntry {
            key: vec![sparrow_model::Scalar::utf8("d1")],
            window_start: 0,
            window_end: 0,
            count,
            accs: Vec::new(),
        });
        f
    }

    #[test]
    fn stale_ack_id_is_ignored() {
        let mut got = BarrierAcks::default();
        got.apply(
            2,
            AlignedAck::WindowFrozen {
                checkpoint_id: 1,
                freeze: encoded(empty_freeze()),
            },
        );
        got.apply(
            2,
            AlignedAck::SinkFlushed {
                checkpoint_id: 1,
                ok: true,
                dropped: 0,
            },
        );
        assert!(
            !got.aligned_ok(2),
            "barrier #1 freeze+flush must not satisfy expected id 2"
        );
        assert!(got.freeze.is_none());
        got.apply(
            2,
            AlignedAck::WindowFrozen {
                checkpoint_id: 2,
                freeze: encoded(leftover_freeze(1)),
            },
        );
        got.apply(
            2,
            AlignedAck::SinkFlushed {
                checkpoint_id: 2,
                ok: true,
                dropped: 0,
            },
        );
        assert!(got.aligned_ok(2));
        assert!(!got.freeze.as_ref().unwrap().bytes.is_empty());
    }

    #[test]
    fn failed_flush_is_not_aligned() {
        let mut got = BarrierAcks::default();
        got.apply(
            1,
            AlignedAck::WindowFrozen {
                checkpoint_id: 1,
                freeze: encoded(empty_freeze()),
            },
        );
        got.apply(
            1,
            AlignedAck::SinkFlushed {
                checkpoint_id: 1,
                ok: false,
                dropped: 2,
            },
        );
        assert!(!got.aligned_ok(1));
        assert!(got.flushed);
        assert!(!got.flush_ok);
    }

    #[tokio::test]
    async fn wait_aligned_acks_rejects_stale_pair() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tx.send(AlignedAck::WindowFrozen {
            checkpoint_id: 1,
            freeze: encoded(empty_freeze()),
        })
        .await
        .unwrap();
        tx.send(AlignedAck::SinkFlushed {
            checkpoint_id: 1,
            ok: true,
            dropped: 0,
        })
        .await
        .unwrap();
        let err = wait_aligned_acks(&mut rx, 2, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
    }
}

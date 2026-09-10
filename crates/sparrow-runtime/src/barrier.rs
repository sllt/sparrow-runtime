//! Aligned checkpoint barrier coordination on the Kernel path.
//!
//! Barriers travel with the mailbox. Stateful stages freeze; the sink waits
//! for a real outbox ack (not "channel empty") before the supervisor commits.
//! Acks carry `checkpoint_id`. Stale ids from a timed-out barrier must not
//! pair with a later cut.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sparrow_model::{ErrorCode, InflightCounter, Result, SparrowError};

use crate::window::WindowFreeze;

#[derive(Debug)]
pub enum AlignedAck {
    WindowFrozen {
        checkpoint_id: u64,
        freeze: WindowFreeze,
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
            Self::WindowFrozen { checkpoint_id, .. } | Self::SinkFlushed { checkpoint_id, .. } => {
                *checkpoint_id
            }
        }
    }
}

#[derive(Clone)]
pub struct AlignedJob {
    pub restore: Option<WindowFreeze>,
    pub acks: tokio::sync::mpsc::Sender<AlignedAck>,
    pub outbox: Arc<InflightCounter>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushOutcome {
    pub ok: bool,
    pub dropped: u64,
}

/// Acks accepted for one expected barrier id.
#[derive(Debug, Default)]
pub struct BarrierAcks {
    pub freeze: Option<WindowFreeze>,
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

/// Drain queued acks whose id is below `expected` (timed-out / abandoned barriers).
pub fn discard_stale_acks(ack_rx: &mut tokio::sync::mpsc::Receiver<AlignedAck>, expected: u64) {
    while let Ok(ack) = ack_rx.try_recv() {
        // Abandoning a barrier: drop queued acks (ids < expected, and any
        // stray current id). Next wait only accepts `ack.id == next expected`.
        let _ = (ack, expected);
    }
}

pub async fn wait_outbox(outbox: &InflightCounter, timeout: Duration) -> FlushOutcome {
    let failed0 = outbox.failed();
    let deadline = Instant::now() + timeout;
    while outbox.pending() > 0 {
        if Instant::now() >= deadline {
            let dropped = outbox
                .failed()
                .saturating_sub(failed0)
                .max(outbox.pending());
            return FlushOutcome { ok: false, dropped };
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let dropped = outbox.failed().saturating_sub(failed0);
    FlushOutcome {
        ok: dropped == 0,
        dropped,
    }
}

/// Wait until freeze+successful flush for `expected` arrive. Ignores other ids.
/// On timeout or failed flush, discards queued stale acks (`id < next`).
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
            Ok(Some(ack)) => got.apply(expected, ack),
            Ok(None) | Err(_) => break,
        }
    }
    if got.aligned_ok(expected) {
        return Ok(got);
    }
    discard_stale_acks(ack_rx, expected.saturating_add(1));
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
                freeze: empty_freeze(),
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
                freeze: leftover_freeze(1),
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
        assert_eq!(got.freeze.as_ref().unwrap().entries[0].count, 1);
    }

    #[test]
    fn failed_flush_is_not_aligned() {
        let mut got = BarrierAcks::default();
        got.apply(
            1,
            AlignedAck::WindowFrozen {
                checkpoint_id: 1,
                freeze: empty_freeze(),
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
            freeze: empty_freeze(),
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

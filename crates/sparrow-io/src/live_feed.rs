//! Non-durable, FIFO live feed facts. A PINGRESP proves only a responsive
//! connection, never broker catch-up, reliable delivery, or device health.
use std::{sync::Arc, time::Instant};

use crate::observed::Payload;
use sparrow_model::{CreditKind, MemoryLease, MemoryOwner, QueuedRow, Result};

#[derive(Debug)]
pub enum LiveFeedKind {
    Row(QueuedRow),
    Probe { started: Instant },
    Unavailable,
}

#[derive(Debug)]
pub struct LiveFeedFact {
    pub at: Instant,
    /// Sticky monotonic discontinuity counter. Even when a full queue loses
    /// an Unavailable marker, the next fact cannot reuse the old coverage.
    pub epoch: u64,
    pub kind: LiveFeedKind,
    _lease: MemoryLease,
}

/// Boxed to leave existing ingress envelope sizes unchanged. Both the box
/// and optional queued row are charged before publication, until consumption.
#[derive(Debug)]
pub struct LiveFeedEvent(Box<LiveFeedFact>);

impl LiveFeedEvent {
    pub const METADATA_BYTES: usize = std::mem::size_of::<LiveFeedFact>() + 64;
    pub fn new(
        owner: &Arc<MemoryOwner>,
        at: Instant,
        epoch: u64,
        kind: LiveFeedKind,
    ) -> Result<Self> {
        let lease = owner.acquire(CreditKind::Queue, Self::METADATA_BYTES)?;
        Ok(Self(Box::new(LiveFeedFact {
            at,
            epoch,
            kind,
            _lease: lease,
        })))
    }
    pub fn into_fact(self) -> LiveFeedFact {
        *self.0
    }
}

impl Payload for LiveFeedEvent {
    fn rows(&self) -> usize {
        usize::from(matches!(self.0.kind, LiveFeedKind::Row(_)))
    }
    fn bytes(&self) -> usize {
        Self::METADATA_BYTES
            + match &self.0.kind {
                LiveFeedKind::Row(row) => row.bytes(),
                _ => 0,
            }
    }
}

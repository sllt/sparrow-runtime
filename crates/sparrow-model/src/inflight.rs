//! Process-visible in-flight counter for sink flush alignment.
//!
//! Enqueue **before** a batch is handed to a sink. Ack **after** the sink
//! has durable-or-transport-acked that batch (HTTP response, MQTT write+flush,
//! log ring write). A checkpoint barrier must wait until `pending() == 0`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared sent/acked counter. Not a channel-empty check.
#[derive(Debug, Default)]
pub struct InflightCounter {
    sent: AtomicU64,
    acked: AtomicU64,
}

impl InflightCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a batch has been handed to the sink path.
    pub fn enqueue(&self) {
        self.sent.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that the sink finished (success or fail-closed drop).
    pub fn ack(&self) {
        self.acked.fetch_add(1, Ordering::SeqCst);
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    pub fn acked(&self) -> u64 {
        self.acked.load(Ordering::SeqCst)
    }

    /// Unacked batches. Barrier commit is dishonest while this is > 0.
    pub fn pending(&self) -> u64 {
        self.sent().saturating_sub(self.acked())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_tracks_unacked() {
        let c = InflightCounter::new();
        c.enqueue();
        c.enqueue();
        assert_eq!(c.pending(), 2);
        c.ack();
        assert_eq!(c.pending(), 1);
        c.ack();
        assert_eq!(c.pending(), 0);
    }
}

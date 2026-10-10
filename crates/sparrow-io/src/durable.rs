//! Durable HTTP output boundary. A receipt means local durable acceptance,
//! not remote success. Runtime barriers may finish only after `enqueue`
//! commits; the source still ACKs only after its checkpoint is published.
use sparrow_model::{MemoryLease, MemoryOwner, Result};
use std::sync::Arc;

pub struct DurableRequest {
    pub id: String,
    pub attempt: u32,
    pub body: Vec<u8>,
    /// Includes body, HTTP working copy and bounded decoder/SQLite scratch.
    pub credit: MemoryLease,
}

pub enum DurableOutcome {
    Delivered,
    Retry {
        reason: &'static str,
        retry_after_ms: Option<u64>,
    },
    Dead {
        reason: &'static str,
    },
}

/// Blocking methods: connectors run them on the blocking pool, never while
/// holding an async Runtime mailbox lock. One bound handle owns the sender
/// lifetime; administrative views cannot become additional sender owners.
pub trait DurableHttpQueue: Send + Sync {
    fn enqueue(&self, body: &[u8], now_ms: u64) -> Result<String>;
    /// Reserves an attempt durably BEFORE returning bytes to the sender.
    fn next(&self, now_ms: u64, owner: &Arc<MemoryOwner>) -> Result<Option<DurableRequest>>;
    fn settle(&self, id: &str, attempt: u32, outcome: DurableOutcome, now_ms: u64) -> Result<()>;
}

pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as u64
}

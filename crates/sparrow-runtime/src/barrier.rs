//! Aligned checkpoint barrier coordination on the Kernel path.
//!
//! Barriers travel with the mailbox. Stateful stages freeze; the sink waits
//! for a real outbox ack (not "channel empty") before the supervisor commits.

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
    },
}

#[derive(Clone)]
pub struct AlignedJob {
    pub restore: Option<WindowFreeze>,
    pub acks: tokio::sync::mpsc::Sender<AlignedAck>,
    pub outbox: Arc<InflightCounter>,
}

pub async fn wait_outbox(outbox: &InflightCounter, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while outbox.pending() > 0 {
        if Instant::now() >= deadline {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "sink flush timeout: {} unacked batches still in flight",
                    outbox.pending()
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(())
}

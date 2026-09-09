//! Bounded mailbox: item cap + byte cap. Senders select on cancel so a
//! full queue cannot deadlock a stop.

use std::sync::Arc;

use sparrow_model::{ErrorCode, Result, RowBatch, SparrowError};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
pub struct MailboxConfig {
    pub max_items: usize,
    pub max_bytes: usize,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            max_items: 8,
            max_bytes: 256 * 1024,
        }
    }
}

pub struct Envelope {
    pub batch: RowBatch,
    bytes: usize,
    permits: Arc<Semaphore>,
}

impl Drop for Envelope {
    fn drop(&mut self) {
        self.permits.add_permits(self.bytes.max(1));
    }
}

#[derive(Clone)]
pub struct MailboxTx {
    tx: mpsc::Sender<Envelope>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
    cancel: CancellationToken,
}

pub struct MailboxRx {
    rx: mpsc::Receiver<Envelope>,
    cancel: CancellationToken,
}

pub fn channel(cfg: MailboxConfig, cancel: CancellationToken) -> Result<(MailboxTx, MailboxRx)> {
    if cfg.max_items == 0 || cfg.max_bytes == 0 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "mailbox bounds must be non-zero",
        ));
    }
    let (tx, rx) = mpsc::channel(cfg.max_items);
    let bytes = Arc::new(Semaphore::new(cfg.max_bytes));
    Ok((
        MailboxTx {
            tx,
            bytes,
            max_bytes: cfg.max_bytes,
            cancel: cancel.clone(),
        },
        MailboxRx { rx, cancel },
    ))
}

impl MailboxTx {
    pub async fn send(&self, batch: RowBatch) -> Result<bool> {
        let n = batch.tracked_bytes().max(1);
        if n > self.max_bytes {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("batch {n}B exceeds mailbox max_bytes {}", self.max_bytes),
            ));
        }
        let permit = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Ok(false),
            p = self.bytes.acquire_many(n as u32) => p,
        };
        let permit = permit.map_err(|_| {
            SparrowError::new(ErrorCode::Cancelled, "mailbox closed")
        })?;
        permit.forget();
        let env = Envelope {
            batch,
            bytes: n,
            permits: Arc::clone(&self.bytes),
        };
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Ok(false),
            r = self.tx.send(env) => {
                r.map_err(|_| SparrowError::new(ErrorCode::Cancelled, "mailbox closed"))?;
                Ok(true)
            }
        }
    }
}

impl MailboxRx {
    pub async fn recv(&mut self) -> Result<Option<Envelope>> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Ok(None),
            msg = self.rx.recv() => Ok(msg),
        }
    }
}

//! Bounded Explicit ACK confirmations. No per-record detached tasks and no
//! SDK handles survive close. The worker runs independently of inbox pressure.
use super::{connection::error, reader::AckToken};
use futures_util::{stream::FuturesUnordered, StreamExt};
use sparrow_model::{CreditKind, ErrorCode, MemoryOwner, Result};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

pub const ACK_CONCURRENCY: usize = 16;
pub const ACK_WORKSPACE: usize = 64 * 1024;
type Confirmation = (u64, Result<()>);
type Pending = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

pub(super) struct AckDriver {
    commands: mpsc::Sender<(u64, AckToken)>,
    results: mpsc::Receiver<Confirmation>,
    pub wake: Arc<Notify>,
    pub retries: Arc<AtomicU64>,
    cancel: CancellationToken,
    worker: Option<tokio::task::JoinHandle<()>>,
    _lease: Arc<sparrow_model::MemoryLease>,
    pub active: usize,
}
impl AckDriver {
    pub fn start(client: async_nats::Client, owner: &Arc<MemoryOwner>) -> Result<Self> {
        let lease = Arc::new(owner.acquire(CreditKind::Reservation, ACK_WORKSPACE)?);
        let worker_lease = lease.clone();
        let (commands, mut rx) = mpsc::channel::<(u64, AckToken)>(ACK_CONCURRENCY);
        let (tx, results) = mpsc::channel(ACK_CONCURRENCY);
        let wake = Arc::new(Notify::new());
        let retries = Arc::new(AtomicU64::new(0));
        let cancel = CancellationToken::new();
        let (child, notify, counter) = (cancel.clone(), wake.clone(), retries.clone());
        let worker = tokio::spawn(async move {
            let _lease = worker_lease;
            let mut pending: FuturesUnordered<Pending> = FuturesUnordered::new();
            loop {
                tokio::select! {
                    biased;
                    _ = child.cancelled() => break,
                    result = pending.next(), if !pending.is_empty() => {
                        let result = result.expect("nonempty confirmations");
                        tokio::select! {biased; _=child.cancelled()=>break, sent=tx.send(result)=>if sent.is_err(){break}}
                        notify.notify_one();
                    }
                    command = rx.recv(), if pending.len() < ACK_CONCURRENCY => {
                        let Some((seq, token)) = command else {break};
                        let client = client.clone();
                        let counter = counter.clone();
                        pending.push(Box::pin(async move {
                            let mut result = token.confirm(&client).await;
                            for retry in 0..2 {
                                if result.is_ok() {break;}
                                counter.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(Duration::from_millis(50 << retry)).await;
                                result = token.confirm(&client).await;
                            }
                            (seq, result)
                        }));
                    }
                }
            }
            // Futures own client clones; dropping them cancels outstanding
            // waiters. Any ACK already sent is for a durable prefix only.
            drop(pending);
            drop(rx);
            drop(tx);
            drop(client);
            drop(_lease);
        });
        Ok(Self {
            commands,
            results,
            wake,
            retries,
            cancel,
            worker: Some(worker),
            _lease: lease,
            active: 0,
        })
    }
    pub fn enqueue(&mut self, sequence: u64, token: AckToken) -> Result<()> {
        self.commands.try_send((sequence, token)).map_err(|_| {
            error(
                ErrorCode::Internal,
                "JetStream ACK worker capacity/lifecycle violated",
            )
        })?;
        self.active += 1;
        Ok(())
    }
    pub fn completed(&mut self) -> Result<Option<Confirmation>> {
        match self.results.try_recv() {
            Ok(result) => {
                self.active -= 1;
                Ok(Some(result))
            }
            Err(mpsc::error::TryRecvError::Empty)
                if self.worker.as_ref().is_some_and(|w| !w.is_finished()) =>
            {
                Ok(None)
            }
            Err(_) => Err(error(
                ErrorCode::JobFailed,
                "JetStream ACK worker stopped unexpectedly",
            )),
        }
    }
    pub async fn close(mut self) -> Result<()> {
        self.cancel.cancel();
        self.worker
            .take()
            .expect("ACK worker")
            .await
            .map_err(|_| error(ErrorCode::Internal, "JetStream ACK worker panicked"))
    }
}
impl Drop for AckDriver {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

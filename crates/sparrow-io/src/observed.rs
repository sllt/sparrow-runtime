//! Optional Tokio boundary instrumentation. Same bounded channel, no relay
//! task. Publish/pop share a short per-channel lock; never held across await.
//! Legacy mpsc handles convert to unobserved handles without changing behavior.
use sparrow_model::observation::{micros, HistogramSnapshot, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, QueuedRow, Result, Row, RowBatch, SparrowError,
};
use std::collections::VecDeque;
use std::future::poll_fn;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;
use std::time::Instant;
use tokio::sync::mpsc;

/// Queue occupancy/oldest age stay exact. Sample residence clocks/histograms
/// deterministically to bound high-rate ingress instrumentation overhead.
pub const RESIDENCE_SAMPLE_EVERY: u64 = 16;
/// Bulk publication amortizes observation without changing channel item units.
pub const MAX_PUBLISH_BATCH: usize = 32;

pub trait Payload {
    fn rows(&self) -> usize;
    fn bytes(&self) -> usize;
    fn origin(&self) -> OriginSpan {
        OriginSpan::missing()
    }
}
impl Payload for Row {
    fn rows(&self) -> usize {
        1
    }
    fn bytes(&self) -> usize {
        self.resident_bytes()
    }
}
impl Payload for QueuedRow {
    fn rows(&self) -> usize {
        1
    }
    fn bytes(&self) -> usize {
        self.bytes()
    }
}
impl Payload for RowBatch {
    fn rows(&self) -> usize {
        self.num_rows()
    }
    fn bytes(&self) -> usize {
        self.tracked_bytes()
    }
    fn origin(&self) -> OriginSpan {
        self.origin()
    }
}
#[derive(Clone, Debug, Default)]
pub struct QueueSnapshot {
    pub capacity: usize,
    pub items: usize,
    pub rows: usize,
    pub bytes: usize,
    pub peak_items: usize,
    pub peak_bytes: usize,
    pub enqueued: u64,
    pub received: u64,
    pub discarded: u64,
    pub waiting: usize,
    pub waits: u64,
    pub aborted_waits: u64,
    pub oldest_age_us: Option<u64>,
    pub receiver_open: bool,
    pub residence: HistogramSnapshot,
    pub capacity_wait: HistogramSnapshot,
}
struct Stamp {
    at: Instant,
    origin: OriginSpan,
    rows: usize,
    bytes: usize,
}
struct State {
    snapshot: QueueSnapshot,
    stamps: VecDeque<Stamp>,
    receiver_dropped: bool,
    _lease: MemoryLease,
}
pub struct QueueObserver {
    capacity: usize,
    item_bytes: usize,
    // First channel use fixes an uninitialized channel in unobserved mode.
    // Initialization can never start midway through traffic or a pending wait.
    state: OnceLock<Option<Arc<Mutex<State>>>>,
}
impl std::fmt::Debug for QueueObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueObserver")
            .field("capacity", &self.capacity)
            .finish()
    }
}
impl QueueObserver {
    pub fn metadata_bytes(&self) -> usize {
        (self.capacity + 32)
            .saturating_mul(self.item_bytes + std::mem::size_of::<Stamp>() + 64)
            .saturating_add(std::mem::size_of::<State>() + 512)
    }
    pub fn initialize(&self, owner: &Arc<MemoryOwner>) -> Result<()> {
        if self.state.get().is_none() {
            let lease = owner.acquire(CreditKind::Queue, self.metadata_bytes())?;
            let _ = self.state.set(Some(Arc::new(Mutex::new(State {
                snapshot: QueueSnapshot {
                    capacity: self.capacity,
                    receiver_open: true,
                    ..QueueSnapshot::default()
                },
                stamps: VecDeque::with_capacity(self.capacity),
                receiver_dropped: false,
                _lease: lease,
            }))));
        }
        match self.state.get().and_then(Option::as_ref) {
            Some(state)
                if Arc::ptr_eq(
                    state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        ._lease
                        .owner(),
                    owner,
                ) =>
            {
                Ok(())
            }
            Some(_) => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "observed channel belongs to a different memory owner/attempt",
            )),
            None => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "channel observation must be initialized before channel use",
            )),
        }
    }
    fn active_state(&self) -> Option<&Arc<Mutex<State>>> {
        self.state.get_or_init(|| None).as_ref()
    }
    pub fn snapshot(&self) -> Option<QueueSnapshot> {
        self.state.get().and_then(Option::as_ref).map(|s| {
            let s = s.lock().unwrap_or_else(|e| e.into_inner());
            let mut q = s.snapshot.clone();
            q.oldest_age_us = s.stamps.front().map(|v| micros(v.at.elapsed()));
            q
        })
    }
}
pub struct Sender<T> {
    tx: mpsc::Sender<T>,
    observer: Option<Arc<QueueObserver>>,
}
pub struct Receiver<T> {
    rx: mpsc::Receiver<T>,
    observer: Option<Arc<QueueObserver>>,
    last: OriginSpan,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            observer: self.observer.clone(),
        }
    }
}
impl<T> From<mpsc::Sender<T>> for Sender<T> {
    fn from(tx: mpsc::Sender<T>) -> Self {
        Self { tx, observer: None }
    }
}
impl<T> From<mpsc::Receiver<T>> for Receiver<T> {
    fn from(rx: mpsc::Receiver<T>) -> Self {
        Self {
            rx,
            observer: None,
            last: OriginSpan::missing(),
        }
    }
}
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    let observer = Arc::new(QueueObserver {
        capacity,
        item_bytes: std::mem::size_of::<T>(),
        state: OnceLock::new(),
    });
    (
        Sender {
            tx,
            observer: Some(observer.clone()),
        },
        Receiver {
            rx,
            observer: Some(observer),
            last: OriginSpan::missing(),
        },
    )
}
impl<T> Sender<T> {
    pub fn observe_wait(&self) -> Wait<'_> {
        Wait::new(self.observer.as_ref())
    }
    pub fn observer(&self) -> Option<Arc<QueueObserver>> {
        self.observer.clone()
    }
    pub fn max_capacity(&self) -> usize {
        self.tx.max_capacity()
    }
    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
    pub async fn reserve(&self) -> std::result::Result<Permit<'_, T>, mpsc::error::SendError<()>> {
        if let Some(o) = &self.observer {
            o.active_state();
        }
        let permit = self.tx.reserve().await?;
        Ok(Permit {
            permit,
            observer: self.observer.as_ref(),
        })
    }
    pub fn try_reserve(&self) -> std::result::Result<Permit<'_, T>, mpsc::error::TrySendError<()>> {
        if let Some(o) = &self.observer {
            o.active_state();
        }
        Ok(Permit {
            permit: self.tx.try_reserve()?,
            observer: self.observer.as_ref(),
        })
    }
}
impl<T: Payload> Sender<T> {
    /// Publish at most min(MAX_PUBLISH_BATCH, channel capacity) items together.
    /// Each value still consumes ONE Tokio slot and ONE FIFO stamp; capacity,
    /// message order and received/discarded counters retain their original units.
    /// Invalid batch sizes or a closed receiver return every unpublished value.
    pub async fn send_batch_with_origin(
        &self,
        values: Vec<T>,
        origin: OriginSpan,
    ) -> std::result::Result<(), mpsc::error::SendError<Vec<T>>> {
        if values.is_empty() {
            return Ok(());
        }
        let n = values.len();
        if n > MAX_PUBLISH_BATCH || n > self.max_capacity() {
            return Err(mpsc::error::SendError(values));
        }
        let state = self.observer.as_ref().and_then(|o| o.active_state());
        let permits = match self.tx.try_reserve_many(n) {
            Ok(p) => p,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(mpsc::error::SendError(values))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                let mut wait = self.observe_wait();
                match self.tx.reserve_many(n).await {
                    Ok(p) => {
                        wait.finish();
                        p
                    }
                    Err(_) => return Err(mpsc::error::SendError(values)),
                }
            }
        };
        if let Some(state) = state {
            let mut sizes = [(0, 0); MAX_PUBLISH_BATCH];
            for (size, value) in sizes.iter_mut().zip(&values) {
                *size = (value.rows(), value.bytes());
            }
            let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
            let at = Instant::now();
            for ((permit, value), (rows, bytes)) in permits.zip(values).zip(sizes) {
                s.snapshot.enqueued += 1;
                if s.receiver_dropped {
                    s.snapshot.discarded += 1;
                } else {
                    s.snapshot.items += 1;
                    s.snapshot.rows += rows;
                    s.snapshot.bytes += bytes;
                    s.stamps.push_back(Stamp {
                        at,
                        origin,
                        rows,
                        bytes,
                    });
                }
                permit.send(value);
            }
            s.snapshot.peak_items = s.snapshot.peak_items.max(s.snapshot.items);
            s.snapshot.peak_bytes = s.snapshot.peak_bytes.max(s.snapshot.bytes);
        } else {
            for (permit, value) in permits.zip(values) {
                permit.send(value);
            }
        }
        Ok(())
    }
    pub async fn send(&self, value: T) -> std::result::Result<(), mpsc::error::SendError<T>> {
        let origin = value.origin();
        self.send_with_origin(value, origin).await
    }
    pub async fn send_with_origin(
        &self,
        value: T,
        origin: OriginSpan,
    ) -> std::result::Result<(), mpsc::error::SendError<T>> {
        match self.try_reserve() {
            Ok(p) => {
                p.send_with_origin(value, origin);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(mpsc::error::SendError(value)),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let mut wait = Wait::new(self.observer.as_ref());
                match self.reserve().await {
                    Ok(p) => {
                        wait.ok = true;
                        p.send_with_origin(value, origin);
                        Ok(())
                    }
                    Err(_) => Err(mpsc::error::SendError(value)),
                }
            }
        }
    }
    pub fn try_send(&self, value: T) -> std::result::Result<(), mpsc::error::TrySendError<T>> {
        let origin = value.origin();
        self.try_send_with_origin(value, origin)
    }
    pub fn try_send_with_origin(
        &self,
        value: T,
        origin: OriginSpan,
    ) -> std::result::Result<(), mpsc::error::TrySendError<T>> {
        match self.try_reserve() {
            Ok(p) => {
                p.send_with_origin(value, origin);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(mpsc::error::TrySendError::Closed(value))
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(mpsc::error::TrySendError::Full(value)),
        }
    }
}
pub struct Permit<'a, T> {
    permit: mpsc::Permit<'a, T>,
    observer: Option<&'a Arc<QueueObserver>>,
}
impl<T: Payload> Permit<'_, T> {
    pub fn send(self, value: T) {
        let origin = value.origin();
        self.send_with_origin(value, origin);
    }
    pub fn send_with_origin(self, value: T, origin: OriginSpan) {
        if let Some(s) = self.observer.and_then(|o| o.active_state()) {
            // Sizing may walk nested values; keep it outside the publication
            // lock so the consumer can pop earlier items concurrently.
            let rows = value.rows();
            let bytes = value.bytes();
            let mut s = s.lock().unwrap_or_else(|e| e.into_inner());
            if s.receiver_dropped {
                s.snapshot.enqueued += 1;
                s.snapshot.discarded += 1;
                self.permit.send(value);
                return;
            }
            // A permit may outlive receiver.close(). The value is still owned
            // by the channel and will be accounted/discarded by Receiver::drop.
            let stamp = Stamp {
                at: Instant::now(),
                origin,
                rows,
                bytes,
            };
            s.snapshot.items += 1;
            s.snapshot.rows += stamp.rows;
            s.snapshot.bytes += stamp.bytes;
            s.snapshot.enqueued += 1;
            s.snapshot.peak_items = s.snapshot.peak_items.max(s.snapshot.items);
            s.snapshot.peak_bytes = s.snapshot.peak_bytes.max(s.snapshot.bytes);
            s.stamps.push_back(stamp);
            self.permit.send(value);
        } else {
            self.permit.send(value);
        }
    }
}
pub struct Wait<'a> {
    state: Option<&'a Arc<Mutex<State>>>,
    at: Instant,
    ok: bool,
}
impl<'a> Wait<'a> {
    pub fn finish(&mut self) {
        self.ok = true;
    }
    fn new(observer: Option<&'a Arc<QueueObserver>>) -> Self {
        let state = observer.and_then(|o| o.active_state());
        if let Some(s) = state {
            let mut s = s.lock().unwrap_or_else(|e| e.into_inner());
            s.snapshot.waiting += 1;
            s.snapshot.waits += 1;
        }
        Self {
            state,
            at: Instant::now(),
            ok: false,
        }
    }
}
impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if let Some(s) = self.state {
            let mut s = s.lock().unwrap_or_else(|e| e.into_inner());
            s.snapshot.waiting -= 1;
            s.snapshot.capacity_wait.record(self.at.elapsed());
            if !self.ok {
                s.snapshot.aborted_waits += 1;
            }
        }
    }
}
fn popped(s: &mut State, discard: bool) -> OriginSpan {
    let v = s.stamps.pop_front().expect("observed publish/pop paired");
    s.snapshot.items -= 1;
    s.snapshot.rows -= v.rows;
    s.snapshot.bytes -= v.bytes;
    if discard {
        s.snapshot.discarded += 1;
    } else {
        s.snapshot.received += 1;
        if s.snapshot.received % RESIDENCE_SAMPLE_EVERY == 0 {
            s.snapshot.residence.record(v.at.elapsed());
        }
    }
    v.origin
}
impl<T> Receiver<T> {
    pub fn observer(&self) -> Option<Arc<QueueObserver>> {
        self.observer.clone()
    }
    pub fn max_capacity(&self) -> usize {
        self.rx.max_capacity()
    }
    pub fn last_origin(&self) -> OriginSpan {
        self.last
    }
    pub fn close(&mut self) {
        self.rx.close();
        if let Some(s) = self.observer.as_ref().and_then(|o| o.active_state()) {
            s.lock()
                .unwrap_or_else(|e| e.into_inner())
                .snapshot
                .receiver_open = false;
        }
    }
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| {
            let mut guard = self
                .observer
                .as_ref()
                .and_then(|o| o.active_state())
                .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()));
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(v)) => {
                    self.last = guard
                        .as_mut()
                        .map(|s| popped(s, false))
                        .unwrap_or_else(OriginSpan::missing);
                    Poll::Ready(Some(v))
                }
                Poll::Ready(None) => {
                    if let Some(s) = guard.as_mut() {
                        s.snapshot.receiver_open = false;
                    }
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
    pub fn try_recv(&mut self) -> std::result::Result<T, mpsc::error::TryRecvError> {
        let mut guard = self
            .observer
            .as_ref()
            .and_then(|o| o.active_state())
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()));
        let v = self.rx.try_recv()?;
        self.last = guard
            .as_mut()
            .map(|s| popped(s, false))
            .unwrap_or_else(OriginSpan::missing);
        Ok(v)
    }
    /// Nonblocking bounded drain, preserving each value's own origin and FIFO.
    pub fn try_recv_many(&mut self, values: &mut Vec<(T, OriginSpan)>, limit: usize) -> usize {
        // Allocate caller scratch before entering the publication/pop lock.
        values.reserve(limit.min(self.rx.max_capacity()));
        let mut state = self
            .observer
            .as_ref()
            .and_then(|o| o.active_state())
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()));
        let mut received = 0;
        while received < limit {
            let Ok(value) = self.rx.try_recv() else {
                break;
            };
            self.last = state
                .as_mut()
                .map(|s| popped(s, false))
                .unwrap_or_else(OriginSpan::missing);
            values.push((value, self.last));
            received += 1;
        }
        received
    }
    /// Shutdown cleanup is discarded, not a consumer delivery/residence sample.
    pub fn discard_next(&mut self) -> std::result::Result<T, mpsc::error::TryRecvError> {
        let mut guard = self
            .observer
            .as_ref()
            .and_then(|o| o.active_state())
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()));
        let v = self.rx.try_recv()?;
        if let Some(s) = guard.as_mut() {
            popped(s, true);
        }
        Ok(v)
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.rx.close();
        if let Some(s) = self.observer.as_ref().and_then(|o| o.active_state()) {
            let mut s = s.lock().unwrap_or_else(|e| e.into_inner());
            s.snapshot.receiver_open = false;
            // Receiver drop destroys the channel, including values published by
            // outstanding permits later. Those late publications are handled below.
            while let Ok(value) = self.rx.try_recv() {
                popped(&mut s, true);
                drop(value);
            }
            s.receiver_dropped = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(n: i64) -> Row {
        Row {
            values: vec![sparrow_model::Scalar::Int64(n)],
        }
    }

    #[test]
    fn r9_late_initialization_is_rejected_without_stamp_mismatch() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = channel(4);
        let q = tx.observer().unwrap();
        tx.try_send(row(1)).unwrap();
        assert_eq!(
            q.initialize(&owner).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        tx.try_send(row(2)).unwrap();
        assert_eq!(rx.try_recv().unwrap().values, row(1).values);
        assert_eq!(rx.try_recv().unwrap().values, row(2).values);
        assert!(q.snapshot().is_none());
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test]
    async fn r9_late_initialization_cannot_underflow_a_pending_wait() {
        use std::future::Future;
        use std::task::{Context, Waker};
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, _rx) = channel(1);
        tx.try_send(row(1)).unwrap();
        let q = tx.observer().unwrap();
        let mut send = Box::pin(tx.send(row(2)));
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(q.initialize(&owner).is_err());
        drop(send);
        assert!(q.snapshot().is_none());
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn r9_initialize_is_race_safe_and_rejects_another_attempt() {
        for _ in 0..100 {
            let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
            let (tx, mut rx) = channel(1);
            let q = tx.observer().unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let b = barrier.clone();
            let send = std::thread::spawn(move || {
                b.wait();
                tx.try_send(row(7)).unwrap();
            });
            barrier.wait();
            let initialized = q.initialize(&owner).is_ok();
            send.join().unwrap();
            assert_eq!(rx.try_recv().unwrap().values, row(7).values);
            assert_eq!(q.snapshot().is_some(), initialized);
            if initialized {
                let other = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
                assert!(q.initialize(&other).is_err());
                assert_eq!(other.usage().physical_bytes, 0);
                assert_eq!(q.snapshot().unwrap().received, 1);
            }
            drop(rx);
            drop(q);
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    }

    #[tokio::test]
    async fn r9_bulk_publish_keeps_row_capacity_and_cancelled_reservations() {
        use std::future::Future;
        use std::task::{Context, Waker};
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = channel(4);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        let at = Instant::now();
        tx.send_batch_with_origin(vec![row(1), row(2)], OriginSpan::at(at))
            .await
            .unwrap();
        let mut send = Box::pin(
            tx.send_batch_with_origin(vec![row(3), row(4), row(5), row(6)], OriginSpan::at(at)),
        );
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert_eq!(q.snapshot().unwrap().items, 2);
        assert_eq!(q.snapshot().unwrap().waiting, 1);
        drop(send);
        assert_eq!(
            tx.capacity(),
            2,
            "cancel must refund partially acquired multi-permits"
        );
        tx.send_batch_with_origin(vec![row(3), row(4)], OriginSpan::at(at))
            .await
            .unwrap();
        assert_eq!(tx.capacity(), 0);
        assert_eq!(
            (q.snapshot().unwrap().items, q.snapshot().unwrap().rows),
            (4, 4)
        );
        let mut values = Vec::new();
        assert_eq!(rx.try_recv_many(&mut values, 3), 3);
        for (i, (v, origin)) in values.iter().enumerate() {
            assert_eq!(v.values, row(i as i64 + 1).values);
            assert_eq!(origin.first, Some(at));
        }
        drop(rx);
        let s = q.snapshot().unwrap();
        assert_eq!(
            (
                s.enqueued,
                s.received,
                s.discarded,
                s.waiting,
                s.aborted_waits
            ),
            (4, 3, 1, 0, 1)
        );
    }

    #[tokio::test]
    async fn r9_bulk_rejects_oversize_and_closed_without_publishing() {
        let (tx, rx) = channel(2);
        let error = tx
            .send_batch_with_origin(vec![row(1), row(2), row(3)], OriginSpan::missing())
            .await
            .unwrap_err();
        assert_eq!(error.0.len(), 3);
        assert_eq!(tx.capacity(), 2);
        drop(rx);
        assert_eq!(
            tx.send_batch_with_origin(vec![row(1)], OriginSpan::missing())
                .await
                .unwrap_err()
                .0
                .len(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn r9_bulk_concurrent_senders_keep_each_origin_and_fifo() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = channel(8);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        let at = Instant::now();
        let tx = Arc::new(tx);
        let mut senders = tokio::task::JoinSet::new();
        for p in 0..4 {
            let tx = tx.clone();
            senders.spawn(async move {
                for batch in 0..100 {
                    let first = p * 1000 + batch * 2;
                    tx.send_batch_with_origin(
                        vec![row(first), row(first + 1)],
                        OriginSpan::at(at + std::time::Duration::from_nanos(p as u64)),
                    )
                    .await
                    .unwrap();
                }
            });
        }
        drop(tx);
        let mut previous = [-1; 4];
        let mut total = 0;
        while let Some(value) = rx.recv().await {
            let sparrow_model::Scalar::Int64(n) = value.values[0] else {
                panic!("row");
            };
            let p = (n / 1000) as usize;
            assert!(n > previous[p]);
            previous[p] = n;
            assert_eq!(
                rx.last_origin().first,
                Some(at + std::time::Duration::from_nanos(p as u64))
            );
            let s = q.snapshot().unwrap();
            assert!(s.items <= 8);
            assert_eq!(s.enqueued, s.received + s.discarded + s.items as u64);
            total += 1;
        }
        while let Some(result) = senders.join_next().await {
            result.unwrap();
        }
        assert_eq!(total, 800);
        assert_eq!(
            q.snapshot().unwrap().residence.count,
            800 / RESIDENCE_SAMPLE_EVERY
        );
    }

    #[test]
    fn obs_queue_only_counts_published_not_reserved_and_drains() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = channel::<Row>(2);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        let p = tx.try_reserve().unwrap();
        assert_eq!(q.snapshot().unwrap().items, 0);
        p.send(Row { values: vec![] });
        assert_eq!(q.snapshot().unwrap().items, 1);
        rx.try_recv().unwrap();
        tx.try_send(Row { values: vec![] }).unwrap();
        drop(rx);
        let s = q.snapshot().unwrap();
        assert_eq!((s.items, s.enqueued, s.received, s.discarded), (0, 2, 1, 1));
        drop(tx);
        drop(q);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
    #[test]
    fn obs_late_permit_after_receiver_drop_is_discarded_without_queue_leak() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, rx) = channel::<Row>(1);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        let p = tx.try_reserve().unwrap();
        drop(rx);
        p.send(Row { values: vec![] });
        let s = q.snapshot().unwrap();
        assert_eq!((s.items, s.enqueued, s.discarded), (0, 1, 1));
        assert!(!s.receiver_open);
    }
    #[tokio::test]
    async fn obs_cancelled_capacity_wait_is_counted_and_releases_sender() {
        use std::future::Future;
        use std::task::{Context, Waker};
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, rx) = channel::<Row>(1);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        tx.try_send(Row { values: vec![] }).unwrap();
        let mut send = Box::pin(tx.send(Row { values: vec![] }));
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert_eq!(q.snapshot().unwrap().waiting, 1);
        drop(send);
        let s = q.snapshot().unwrap();
        assert_eq!(
            (s.waiting, s.waits, s.aborted_waits, s.capacity_wait.count),
            (0, 1, 1, 1)
        );
        drop(rx);
        assert_eq!(q.snapshot().unwrap().discarded, 1);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn obs_concurrent_publication_snapshots_conserve_counts() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = channel::<Row>(8);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        let mut senders = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let tx = tx.clone();
            senders.spawn(async move {
                for _ in 0..500 {
                    tx.send_with_origin(Row { values: vec![] }, OriginSpan::at(Instant::now()))
                        .await
                        .unwrap();
                }
            });
        }
        drop(tx);
        let mut n = 0;
        while rx.recv().await.is_some() {
            n += 1;
            assert!(rx.last_origin().oldest_age(Instant::now()).is_some());
            let s = q.snapshot().unwrap();
            assert!(s.items <= 8);
            assert_eq!(s.enqueued, s.received + s.discarded + s.items as u64);
        }
        while let Some(r) = senders.join_next().await {
            r.unwrap();
        }
        assert_eq!(n, 2000);
        assert_eq!(
            q.snapshot().unwrap().residence.count,
            2000 / RESIDENCE_SAMPLE_EVERY
        );
        drop(rx);
        drop(q);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

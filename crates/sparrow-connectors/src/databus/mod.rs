//! Local DataBus: an in-process, named-topic bus so one pipeline's Sink can
//! feed other pipelines' Sources inside the same runtime (the equivalent of
//! eKuiper's memory source/sink).
//!
//! - Live and at-most-once: no persistence, no replay, no acknowledgement
//!   back to the publisher. Messages published while nobody subscribes are
//!   counted (`databus_sink_no_subscribers`) and discarded; a subscriber only
//!   sees messages published after it attached.
//! - Every subscriber owns a bounded buffer (`capacity` messages and
//!   `max_bytes` payload bytes) whose full size is charged up front to the
//!   subscribing job's memory reservation. Publishers only hold shared
//!   payloads that subscribers have accepted.
//! - Overflow is per subscriber: `drop_oldest` (default) evicts the oldest
//!   buffered message, `drop_newest` rejects the new one, `block` makes the
//!   publisher wait up to `block_timeout` for space and then drops the message
//!   for that subscriber only. Only `block` lets a slow subscriber slow its
//!   publishers; drop policies never stall a publisher or other subscribers.
//! - Topics are `.`-separated tokens of `[A-Za-z0-9_-]`. Publishers use a
//!   literal topic; subscribers may use `*` (one token) and a trailing `>`
//!   (one or more tokens).
//! - Registrations are RAII: dropping a [`Subscriber`] / [`Publisher`]
//!   (pipeline stop, restart or delete) removes it, discards and counts its
//!   buffered messages and releases its memory. A topic exists only while it
//!   has a publisher or subscriber, so nothing leaks; a restarted pipeline
//!   simply attaches again.

mod sink;
pub mod source;
#[cfg(test)]
mod tests;

pub use sink::{DataBusSink, DataBusSinkConfig};
pub use source::{DataBusSource, DataBusSourceConfig};

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result, SparrowError};
use tokio::sync::Notify;

use crate::diag::IoDiagnostics;

pub const MAX_TOPIC_BYTES: usize = 128;
pub const MAX_TOPIC_TOKENS: usize = 16;
pub const MAX_SUBSCRIPTIONS: usize = 256;
pub const MAX_PUBLISHERS: usize = 256;
pub const MAX_BUFFER_MESSAGES: usize = 4096;
pub const MAX_BUFFER_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BLOCK_TIMEOUT: Duration = Duration::from_secs(30);
/// Per buffered message bookkeeping charged on top of the payload bytes.
pub const MESSAGE_OVERHEAD: usize = 64;

pub(crate) fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Evict the oldest buffered message (keep the freshest data).
    #[default]
    DropOldest,
    /// Reject the incoming message.
    DropNewest,
    /// Publisher waits up to `block_timeout`, then drops for this subscriber.
    Block,
}

impl Overflow {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "drop_oldest" => Ok(Self::DropOldest),
            "drop_newest" => Ok(Self::DropNewest),
            "block" => Ok(Self::Block),
            _ => Err(err(
                ErrorCode::InvalidArgument,
                "databus overflow must be drop_oldest, drop_newest or block",
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::DropOldest => "drop_oldest",
            Self::DropNewest => "drop_newest",
            Self::Block => "block",
        }
    }
}

/// `allow_wildcards`: subscriber patterns may use `*` and a final `>`.
pub fn check_topic(topic: &str, allow_wildcards: bool) -> Result<()> {
    let tokens: Vec<&str> = topic.split('.').collect();
    let ok = !topic.is_empty()
        && topic.len() <= MAX_TOPIC_BYTES
        && tokens.len() <= MAX_TOPIC_TOKENS
        && tokens.iter().enumerate().all(|(i, token)| match *token {
            "*" => allow_wildcards,
            ">" => allow_wildcards && i + 1 == tokens.len(),
            t => {
                !t.is_empty()
                    && t.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            }
        });
    if ok {
        Ok(())
    } else {
        Err(err(
            ErrorCode::InvalidArgument,
            if allow_wildcards {
                "databus topic: 1..=16 `.`-separated tokens of [A-Za-z0-9_-] (<=128 bytes); `*` = one token, `>` only last"
            } else {
                "databus publish topic must be literal: 1..=16 `.`-separated tokens of [A-Za-z0-9_-] (<=128 bytes)"
            },
        ))
    }
}

/// Whether the literal `topic` matches a subscription `pattern`.
pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    let mut p = pattern.split('.');
    let mut t = topic.split('.');
    loop {
        match (p.next(), t.next()) {
            (Some(">"), Some(_)) => return true,
            (Some("*"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Whether some literal topic could match both patterns (used to refuse a
/// pipeline that would feed its own Source).
pub fn patterns_overlap(a: &str, b: &str) -> bool {
    let mut a = a.split('.');
    let mut b = b.split('.');
    loop {
        match (a.next(), b.next()) {
            (Some(">"), Some(_)) | (Some(_), Some(">")) => return true,
            (Some("*"), Some(_)) | (Some(_), Some("*")) => {}
            (Some(x), Some(y)) if x == y => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionConfig {
    pub pattern: String,
    pub capacity: usize,
    pub max_bytes: usize,
    pub overflow: Overflow,
    pub block_timeout: Duration,
}

impl SubscriptionConfig {
    pub fn validate(&self) -> Result<()> {
        check_topic(&self.pattern, true)?;
        if !(1..=MAX_BUFFER_MESSAGES).contains(&self.capacity)
            || !(1024..=MAX_BUFFER_BYTES).contains(&self.max_bytes)
            || !(Duration::from_millis(1)..=MAX_BLOCK_TIMEOUT).contains(&self.block_timeout)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "databus buffer bounds: buffer_capacity 1..=4096, buffer_bytes 1KiB..=4MiB, block_timeout_ms 1..=30000",
            ));
        }
        Ok(())
    }

    /// Bytes charged to the subscribing job's reservation for the buffer.
    pub fn reservation(&self) -> usize {
        self.max_bytes
            .saturating_add(self.capacity.saturating_mul(MESSAGE_OVERHEAD))
    }
}

struct Buffer {
    queue: VecDeque<Arc<[u8]>>,
    bytes: usize,
    closed: bool,
}

struct Subscription {
    config: SubscriptionConfig,
    buffer: Mutex<Buffer>,
    readable: Notify,
    writable: Notify,
    diag: Arc<IoDiagnostics>,
}

enum Offer {
    Accepted,
    Dropped,
    Full,
    Closed,
}

impl Subscription {
    fn cost(payload: &[u8]) -> usize {
        payload.len()
    }

    fn gauges(&self, buffer: &Buffer) {
        self.diag
            .databus_source_buffer_items
            .store(buffer.queue.len() as u64, Ordering::Relaxed);
        self.diag
            .databus_source_buffer_bytes
            .store(buffer.bytes as u64, Ordering::Relaxed);
    }

    /// Non-blocking offer applying the drop policies; `Full` only for block.
    fn offer(&self, payload: &Arc<[u8]>) -> Offer {
        let cost = Self::cost(payload);
        let mut buffer = self.buffer.lock().expect("databus buffer");
        if buffer.closed {
            return Offer::Closed;
        }
        let limit = self.config.max_bytes;
        if cost > limit {
            self.diag
                .databus_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Offer::Dropped;
        }
        let fits = |b: &Buffer| b.queue.len() < self.config.capacity && b.bytes + cost <= limit;
        if !fits(&buffer) {
            match self.config.overflow {
                Overflow::Block => return Offer::Full,
                Overflow::DropNewest => {
                    self.diag
                        .databus_source_dropped_newest
                        .fetch_add(1, Ordering::Relaxed);
                    return Offer::Dropped;
                }
                Overflow::DropOldest => {
                    while !fits(&buffer) {
                        let old = buffer.queue.pop_front().expect("non-empty when full");
                        buffer.bytes -= Self::cost(&old);
                        self.diag
                            .databus_source_dropped_oldest
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        buffer.bytes += cost;
        buffer.queue.push_back(payload.clone());
        self.gauges(&buffer);
        drop(buffer);
        self.diag
            .databus_source_received
            .fetch_add(1, Ordering::Relaxed);
        self.readable.notify_one();
        Offer::Accepted
    }

    /// Block policy: wait for space until `deadline`, then drop.
    async fn offer_blocking(&self, payload: &Arc<[u8]>, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.writable.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.offer(payload) {
                Offer::Accepted => return true,
                Offer::Dropped | Offer::Closed => return false,
                Offer::Full => {}
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                self.diag
                    .databus_source_block_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
    }

    fn close(&self) {
        let mut buffer = self.buffer.lock().expect("databus buffer");
        buffer.closed = true;
        let discarded = buffer.queue.len() as u64;
        buffer.queue.clear();
        buffer.bytes = 0;
        self.gauges(&buffer);
        drop(buffer);
        self.diag
            .databus_source_discarded_on_close
            .fetch_add(discarded, Ordering::Relaxed);
        self.writable.notify_waiters();
        self.readable.notify_one();
    }
}

#[derive(Default)]
struct Registry {
    subscriptions: BTreeMap<u64, Arc<Subscription>>,
    publishers: BTreeMap<String, usize>,
}

/// One bus per runtime (owned by the supervisor).
#[derive(Default)]
pub struct DataBus {
    registry: Mutex<Registry>,
    next_id: AtomicU64,
}

impl std::fmt::Debug for DataBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("DataBus")
            .field("topics", &snapshot.topics.len())
            .finish_non_exhaustive()
    }
}

/// Live registrations; a topic is listed only while something uses it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BusSnapshot {
    pub topics: Vec<TopicSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicSnapshot {
    /// Literal publish topic or subscription pattern.
    pub topic: String,
    pub publishers: usize,
    pub subscribers: usize,
    pub buffered: usize,
}

/// Result of offering one message to every matching subscriber.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishOutcome {
    pub matched: usize,
    pub accepted: usize,
    /// At least one block-policy subscriber made the publisher wait.
    pub blocked: bool,
}

impl DataBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> BusSnapshot {
        let registry = self.registry.lock().expect("databus registry");
        let mut topics: BTreeMap<String, TopicSnapshot> = BTreeMap::new();
        for (topic, n) in &registry.publishers {
            topics.insert(
                topic.clone(),
                TopicSnapshot {
                    topic: topic.clone(),
                    publishers: *n,
                    subscribers: 0,
                    buffered: 0,
                },
            );
        }
        for sub in registry.subscriptions.values() {
            let entry = topics
                .entry(sub.config.pattern.clone())
                .or_insert_with(|| TopicSnapshot {
                    topic: sub.config.pattern.clone(),
                    publishers: 0,
                    subscribers: 0,
                    buffered: 0,
                });
            entry.subscribers += 1;
            entry.buffered += sub.buffer.lock().expect("databus buffer").queue.len();
        }
        BusSnapshot {
            topics: topics.into_values().collect(),
        }
    }

    /// Attach a subscriber; its full buffer is charged to `owner` first.
    pub fn subscribe(
        self: &Arc<Self>,
        config: SubscriptionConfig,
        owner: &Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Subscriber> {
        config.validate()?;
        let lease = owner
            .acquire(CreditKind::Reservation, config.reservation())
            .map_err(|_| {
                err(
                    ErrorCode::BoundExceeded,
                    "databus subscriber buffer does not fit the job memory reservation",
                )
            })?;
        let mut registry = self.registry.lock().expect("databus registry");
        if registry.subscriptions.len() >= MAX_SUBSCRIPTIONS {
            return Err(err(
                ErrorCode::ResourceExhausted,
                "databus subscription limit reached (256 per runtime)",
            ));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let subscription = Arc::new(Subscription {
            config,
            buffer: Mutex::new(Buffer {
                queue: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            readable: Notify::new(),
            writable: Notify::new(),
            diag,
        });
        registry.subscriptions.insert(id, subscription.clone());
        subscription
            .diag
            .databus_source_subscriptions
            .fetch_add(1, Ordering::Relaxed);
        Ok(Subscriber {
            bus: self.clone(),
            id,
            subscription,
            _lease: lease,
        })
    }

    /// Register a publisher for the literal `topic`.
    pub fn publisher(self: &Arc<Self>, topic: &str) -> Result<Publisher> {
        check_topic(topic, false)?;
        let mut registry = self.registry.lock().expect("databus registry");
        if registry.publishers.values().sum::<usize>() >= MAX_PUBLISHERS {
            return Err(err(
                ErrorCode::ResourceExhausted,
                "databus publisher limit reached (256 per runtime)",
            ));
        }
        *registry.publishers.entry(topic.to_string()).or_default() += 1;
        Ok(Publisher {
            bus: self.clone(),
            topic: topic.to_string(),
        })
    }

    fn matching(&self, topic: &str) -> Vec<Arc<Subscription>> {
        let registry = self.registry.lock().expect("databus registry");
        registry
            .subscriptions
            .values()
            .filter(|s| topic_matches(&s.config.pattern, topic))
            .cloned()
            .collect()
    }
}

/// A live subscription; dropping it detaches and discards its buffer.
pub struct Subscriber {
    bus: Arc<DataBus>,
    id: u64,
    subscription: Arc<Subscription>,
    _lease: MemoryLease,
}

impl Subscriber {
    /// Next buffered message in publish order; `None` once closed.
    pub async fn recv(&self) -> Option<Arc<[u8]>> {
        let sub = &self.subscription;
        loop {
            let notified = sub.readable.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut buffer = sub.buffer.lock().expect("databus buffer");
                if let Some(message) = buffer.queue.pop_front() {
                    buffer.bytes -= Subscription::cost(&message);
                    sub.gauges(&buffer);
                    drop(buffer);
                    sub.writable.notify_waiters();
                    return Some(message);
                }
                if buffer.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.bus
            .registry
            .lock()
            .expect("databus registry")
            .subscriptions
            .remove(&self.id);
        self.subscription.close();
    }
}

/// A registered publisher for one literal topic; dropping it unregisters.
pub struct Publisher {
    bus: Arc<DataBus>,
    topic: String,
}

impl Publisher {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Offer `payload` to every subscriber whose pattern matches. Drop
    /// policies apply immediately; block-policy subscribers are awaited
    /// (each up to its own `block_timeout`) after all others got the
    /// message, so they cannot delay delivery to the rest.
    pub async fn publish(&self, payload: Arc<[u8]>) -> PublishOutcome {
        let subscribers = self.bus.matching(&self.topic);
        let mut outcome = PublishOutcome {
            matched: subscribers.len(),
            ..Default::default()
        };
        let mut blocked = Vec::new();
        for sub in &subscribers {
            match sub.offer(&payload) {
                Offer::Accepted => outcome.accepted += 1,
                Offer::Full => blocked.push(sub),
                Offer::Dropped | Offer::Closed => {}
            }
        }
        if !blocked.is_empty() {
            outcome.blocked = true;
            let start = tokio::time::Instant::now();
            for sub in blocked {
                if sub
                    .offer_blocking(&payload, start + sub.config.block_timeout)
                    .await
                {
                    outcome.accepted += 1;
                }
            }
        }
        outcome
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let mut registry = self.bus.registry.lock().expect("databus registry");
        if let Some(n) = registry.publishers.get_mut(&self.topic) {
            *n -= 1;
            if *n == 0 {
                registry.publishers.remove(&self.topic);
            }
        }
    }
}

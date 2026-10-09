//! NATS Core client lifecycle: bounded per-outage reconnect, observable SDK
//! events, ledger-charged SDK buffers released only on actual SDK exit.
//!
//! Unlike the JetStream reader (where any discontinuity is sticky-unhealthy),
//! NATS Core is at-most-once: the SDK may reconnect and resubscribe, and
//! messages published while disconnected are simply not delivered.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sparrow_model::observation::HealthState;
use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result};
use tokio::sync::Notify;

use super::common::{self, error, MAX_SUBJECT_BYTES};
use crate::diag::IoDiagnostics;
use crate::{SecretResolver, TargetPolicy};

pub const MIN_PAYLOAD_BYTES: usize = 1024;
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_RECONNECT_ATTEMPTS: usize = 100;
pub const DEFAULT_RECONNECT_ATTEMPTS: usize = 10;
pub const MAX_CAPACITY: usize = 256;
pub const DEFAULT_CAPACITY: usize = 8;
/// Upper bound of the SDK reconnect delay (exponential from 100ms).
pub const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Conservative estimate of SDK read/write buffers and task state. Like the
/// JetStream reservation this is ledger accounting, not an RSS guarantee.
const SDK_FIXED_RESERVATION: usize = 256 * 1024;
const PER_MESSAGE_OVERHEAD: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Source,
    Sink,
    /// JetStream publish Sink (`jetstream_sink_*` counters).
    #[cfg(feature = "jetstream")]
    JetStreamSink,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NatsClientConfig {
    pub servers: Vec<String>,
    /// SecretRef only; requires `tls://` servers.
    pub token_secret: Option<String>,
    /// SDK connection attempts per outage (reset after each success).
    pub reconnect_attempts: usize,
    pub connect_timeout: Duration,
    /// Largest message payload this connector accepts. A Source gates every
    /// server's advertised `max_payload` before subscribing, and also checks
    /// each incoming wire frame before allocating its bounded body.
    pub max_payload_bytes: usize,
    /// Source: bounded wire prefetch (messages). Sink: SDK command buffer.
    pub capacity: usize,
}

impl NatsClientConfig {
    pub fn new(servers: Vec<String>) -> Self {
        Self {
            servers,
            token_secret: None,
            reconnect_attempts: DEFAULT_RECONNECT_ATTEMPTS,
            connect_timeout: Duration::from_secs(2),
            max_payload_bytes: DEFAULT_PAYLOAD_BYTES,
            capacity: DEFAULT_CAPACITY,
        }
    }

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        if !(1..=MAX_RECONNECT_ATTEMPTS).contains(&self.reconnect_attempts)
            || !(Duration::from_millis(100)..=Duration::from_secs(30))
                .contains(&self.connect_timeout)
            || !(MIN_PAYLOAD_BYTES..=MAX_PAYLOAD_BYTES).contains(&self.max_payload_bytes)
            || !(1..=MAX_CAPACITY).contains(&self.capacity)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS bounds: reconnect_attempts 1..=100, connect_timeout_ms 100..=30000, max_payload_bytes 1024..=1048576, capacity 1..=256",
            ));
        }
        common::validate_servers(&self.servers, self.token_secret.is_some(), policy)?;
        if let Some(reference) = &self.token_secret {
            common::resolve_token(secrets, reference)?;
        }
        Ok(())
    }

    /// Ledger charge for SDK-held buffers. The command queue can refill while
    /// the SDK's recv_many batch (at most 16 commands) remains in its writer.
    /// Source wire buffers conservatively use the same public estimate.
    pub fn sdk_reservation(&self) -> usize {
        SDK_FIXED_RESERVATION.saturating_add(
            self.capacity
                .saturating_add(self.capacity.min(16))
                .saturating_mul(
                    self.max_payload_bytes
                        .saturating_add(PER_MESSAGE_OVERHEAD)
                        .saturating_add(MAX_SUBJECT_BYTES),
                ),
        )
    }

    /// One connector may use at most half the job reservation budget.
    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.sdk_reservation() > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS buffers (command/prefetch plus overlapping writer) exceed half the job reservation budget; lower capacity or max_payload_bytes",
            ));
        }
        Ok(())
    }
}

/// A token resolved from its SecretRef at bind. Never printed.
#[derive(Clone)]
pub struct BoundToken(String);

impl BoundToken {
    pub(crate) fn value(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for BoundToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl NatsClientConfig {
    /// Validate policy/bounds and resolve the token SecretRef once.
    pub fn bind(
        &self,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
    ) -> Result<Option<BoundToken>> {
        self.validate(secrets, policy)?;
        self.token_secret
            .as_deref()
            .map(|reference| common::resolve_token(secrets, reference).map(BoundToken))
            .transpose()
    }
}

#[derive(Default)]
struct Completion {
    closed: AtomicBool,
    notify: Notify,
}

/// Owned by the SDK event callback, so it drops only when the SDK
/// connection handler has actually exited (same pattern as JetStream).
struct SdkLifetime {
    lease: Option<Arc<MemoryLease>>,
    completion: Arc<Completion>,
    _shutdown_guard: Option<Arc<dyn Send + Sync>>,
}
impl Drop for SdkLifetime {
    fn drop(&mut self) {
        drop(self.lease.take());
        drop(self._shutdown_guard.take());
        self.completion.closed.store(true, Ordering::Release);
        self.completion.notify.notify_waiters();
    }
}

struct EventState {
    _lifetime: SdkLifetime,
    diag: Arc<IoDiagnostics>,
    role: Role,
    connected_once: AtomicBool,
}

impl EventState {
    fn counter(&self, source: &'static str) -> &AtomicU64 {
        let d = &self.diag;
        match (self.role, source) {
            (Role::Source, "disconnect") => &d.nats_source_disconnects,
            (Role::Source, "reconnect") => &d.nats_source_reconnects,
            (Role::Source, _) => &d.nats_source_client_errors,
            (Role::Sink, "disconnect") => &d.nats_sink_disconnects,
            (Role::Sink, "reconnect") => &d.nats_sink_reconnects,
            (Role::Sink, _) => &d.nats_sink_client_errors,
            #[cfg(feature = "jetstream")]
            (Role::JetStreamSink, "disconnect") => &d.jetstream_sink_disconnects,
            #[cfg(feature = "jetstream")]
            (Role::JetStreamSink, "reconnect") => &d.jetstream_sink_reconnects,
            #[cfg(feature = "jetstream")]
            (Role::JetStreamSink, _) => &d.jetstream_sink_client_errors,
        }
    }

    fn on_event(&self, event: async_nats::Event) {
        let source = self.role == Role::Source;
        let health = |state, reason| self.diag.observation.health(source, state, reason, None);
        match event {
            async_nats::Event::Connected => {
                if self.connected_once.swap(true, Ordering::AcqRel) {
                    self.counter("reconnect").fetch_add(1, Ordering::Relaxed);
                }
                health(HealthState::Ready, "nats_connected");
            }
            async_nats::Event::Disconnected => {
                self.counter("disconnect").fetch_add(1, Ordering::Relaxed);
                health(
                    HealthState::Reconnecting,
                    "nats_disconnected_bounded_reconnect",
                );
            }
            async_nats::Event::SlowConsumer(_) => {
                // The SDK dropped one message: its bounded subscription
                // buffer was full. NATS Core has no redelivery.
                self.diag
                    .nats_source_slow_consumer
                    .fetch_add(1, Ordering::Relaxed);
            }
            async_nats::Event::ServerError(_) | async_nats::Event::ClientError(_) => {
                self.counter("error").fetch_add(1, Ordering::Relaxed);
            }
            async_nats::Event::LameDuckMode
            | async_nats::Event::Draining
            | async_nats::Event::Closed => {}
        }
    }
}

pub(crate) struct CoreClient {
    pub(crate) client: async_nats::Client,
    completion: Arc<Completion>,
    // The SDK task and the caller jointly own the charge. SDK exit alone
    // must not refund buffers still retained by the caller/subscription.
    _reservation: Arc<MemoryLease>,
}

impl CoreClient {
    /// Opens one connection. Initial connect is attempted once (bounded by
    /// `connect_timeout`); afterwards the SDK reconnects at most
    /// `reconnect_attempts` times per outage with 100ms..2s backoff, then
    /// closes. Callers treat a closed client as a retryable failure.
    /// `config` must have been validated (policy + secrets) at bind; the
    /// token is the value resolved there.
    pub(crate) async fn open(
        config: &NatsClientConfig,
        token: Option<&BoundToken>,
        owner: &Arc<MemoryOwner>,
        diag: &Arc<IoDiagnostics>,
        role: Role,
    ) -> Result<Self> {
        Self::open_with_guard(config, token, owner, diag, role, None).await
    }

    /// Prepared probes may retain a SourceAdmission lifecycle guard until the
    /// SDK really exits, including cancellation or a failed bounded close.
    pub(crate) async fn open_with_guard(
        config: &NatsClientConfig,
        token: Option<&BoundToken>,
        owner: &Arc<MemoryOwner>,
        diag: &Arc<IoDiagnostics>,
        role: Role,
        shutdown_guard: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Self> {
        let lease = Arc::new(owner.acquire(CreditKind::Reservation, config.sdk_reservation())?);
        let completion = Arc::new(Completion::default());
        let state = Arc::new(EventState {
            _lifetime: SdkLifetime {
                lease: Some(lease.clone()),
                completion: completion.clone(),
                _shutdown_guard: shutdown_guard,
            },
            diag: diag.clone(),
            role,
            connected_once: AtomicBool::new(false),
        });
        let mut options = async_nats::ConnectOptions::new()
            .subscription_capacity(config.capacity)
            .client_capacity(config.capacity)
            .read_buffer_capacity(u16::MAX)
            .ignore_discovered_servers()
            // async-nats 0.50 maps 0 to unlimited; validate() requires >= 1.
            .max_reconnects(config.reconnect_attempts)
            .reconnect_delay_callback(reconnect_delay)
            .ping_interval(Duration::from_secs(2))
            .connection_timeout(config.connect_timeout)
            .request_timeout(Some(config.connect_timeout))
            .event_callback(move |event| {
                let state = Arc::clone(&state);
                async move {
                    state.on_event(event);
                    drop(state);
                }
            });
        if let Some(token) = token {
            options = options.token(token.0.clone());
        }
        let servers = common::parse_servers(&config.servers)?;
        let client = tokio::time::timeout(
            config.connect_timeout.saturating_mul(2),
            options.connect(servers),
        )
        .await
        .map_err(|_| error(ErrorCode::JobFailed, "NATS connect timed out").retryable(true))?
        .map_err(|_| error(ErrorCode::JobFailed, "NATS connection failed").retryable(true))?;
        let opened = Self {
            client,
            completion,
            _reservation: lease,
        };
        if role == Role::Source {
            if let Err(e) = opened.check_server_payload(config.max_payload_bytes) {
                let _ = opened.close(false).await;
                return Err(e);
            }
        }
        Ok(opened)
    }

    /// The SDK buffers whole messages up to the server's `max_payload`
    /// before a Source sees them; the ledger charge assumes our bound.
    pub(crate) fn check_server_payload(&self, max_payload_bytes: usize) -> Result<()> {
        if self.client.server_info().max_payload > max_payload_bytes {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS server max_payload exceeds the source max_payload_bytes; raise max_payload_bytes (and lower subscription_capacity) or lower the server limit",
            ));
        }
        Ok(())
    }

    pub(crate) fn server_max_payload(&self) -> usize {
        self.client.server_info().max_payload
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.completion.closed.load(Ordering::Acquire)
    }

    /// Optionally flush local socket writes (not a broker receipt), then
    /// drain and wait (bounded) for the SDK to exit and release its credit.
    pub(crate) async fn close(self, flush: bool) -> Result<bool> {
        let Self {
            client,
            completion,
            _reservation,
        } = self;
        let flushed = if flush {
            matches!(
                tokio::time::timeout(CLOSE_TIMEOUT, client.flush()).await,
                Ok(Ok(()))
            )
        } else {
            false
        };
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, client.drain()).await;
        drop(client);
        tokio::time::timeout(CLOSE_TIMEOUT, async {
            loop {
                let notified = completion.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if completion.closed.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| {
            error(
                ErrorCode::ResourceExhausted,
                "NATS client close not joined; SDK credit remains held until it exits",
            )
        })?;
        Ok(flushed)
    }
}

/// 100ms, 200ms, 400ms ... capped at 2s.
pub fn reconnect_delay(attempt: usize) -> Duration {
    let exp = attempt.saturating_sub(1).min(5) as u32;
    Duration::from_millis(100u64.saturating_mul(1 << exp)).min(MAX_RECONNECT_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MapSecretResolver;

    #[test]
    fn nats_client_bounds_budget_and_backoff() {
        let p = TargetPolicy::allow("127.0.0.1", 4222);
        let s = MapSecretResolver::empty();
        let base = NatsClientConfig::new(vec!["nats://127.0.0.1:4222".into()]);
        base.validate(&s, &p).unwrap();
        type M = fn(&mut NatsClientConfig);
        let cases: [(M, ErrorCode); 9] = [
            (|c| c.reconnect_attempts = 0, ErrorCode::BoundExceeded),
            (|c| c.reconnect_attempts = 101, ErrorCode::BoundExceeded),
            (
                |c| c.connect_timeout = Duration::from_millis(99),
                ErrorCode::BoundExceeded,
            ),
            (|c| c.max_payload_bytes = 1023, ErrorCode::BoundExceeded),
            (
                |c| c.max_payload_bytes = MAX_PAYLOAD_BYTES + 1,
                ErrorCode::BoundExceeded,
            ),
            (|c| c.capacity = 0, ErrorCode::BoundExceeded),
            (|c| c.capacity = 257, ErrorCode::BoundExceeded),
            (
                |c| c.token_secret = Some("tok".into()),
                ErrorCode::PolicyDenied,
            ),
            (
                |c| c.servers = vec!["nats://10.0.0.1:4222".into()],
                ErrorCode::PolicyDenied,
            ),
        ];
        for (i, (mutate, code)) in cases.iter().enumerate() {
            let mut c = base.clone();
            mutate(&mut c);
            assert_eq!(c.validate(&s, &p).unwrap_err().code, *code, "case {i}");
        }
        let mut tls = base.clone();
        tls.servers = vec!["tls://127.0.0.1:4222".into()];
        tls.token_secret = Some("missing".into());
        assert_eq!(
            tls.validate(&s, &p).unwrap_err().code,
            ErrorCode::SecretMissing
        );

        // Defaults fit half of the compact job reservation. A full 1 MiB
        // payload plus overlapping writer cannot fit even at capacity one.
        let compact = sparrow_model::ResourceBudget::compact().reservation_bytes;
        base.check_reservation_budget(compact).unwrap();
        let mut big = base.clone();
        big.max_payload_bytes = MAX_PAYLOAD_BYTES;
        assert_eq!(
            big.check_reservation_budget(compact).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        big.capacity = 1;
        assert_eq!(
            big.check_reservation_budget(compact).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        big.max_payload_bytes = 512 * 1024;
        big.check_reservation_budget(compact).unwrap();
        let mut overlapping = base.clone();
        overlapping.capacity = 16;
        assert!(
            overlapping.check_reservation_budget(compact).is_err(),
            "queue plus writer batch must be charged"
        );
        overlapping.max_payload_bytes = usize::MAX;
        assert_eq!(overlapping.sdk_reservation(), usize::MAX);

        let delays: Vec<_> = (1..=8).map(reconnect_delay).collect();
        assert_eq!(delays[0], Duration::from_millis(100));
        assert_eq!(delays[1], Duration::from_millis(200));
        assert!(delays.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(*delays.last().unwrap(), MAX_RECONNECT_DELAY);
    }
}

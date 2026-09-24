use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result, SparrowError};
use tokio::sync::Notify;

use crate::{SecretResolver, TargetPolicy};

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const SDK_FIXED_RESERVATION: usize = 512 * 1024;
pub(super) const SDK_EXPANSION: usize = 32;
pub(super) const MAX_SUBJECT_BYTES: usize = 4096;
const COMMAND_CAPACITY: usize = 32;

#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub servers: Vec<String>,
    /// References only. No credentials in URLs or diagnostic strings.
    pub token_secret: Option<String>,
    pub subscription_capacity: usize,
    /// Upper bound of the ONE outstanding pull, not the whole backlog.
    pub pull_bytes: usize,
}

impl ConnectionConfig {
    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        if !(1..=4).contains(&self.servers.len())
            || !(2..=256).contains(&self.subscription_capacity)
            || !(4096..=1024 * 1024).contains(&self.pull_bytes)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "JetStream connection bounds invalid",
            ));
        }
        for server in &self.servers {
            let u = url::Url::parse(server)
                .map_err(|_| error(ErrorCode::InvalidArgument, "invalid NATS URL"))?;
            if server.len() > 1024
                || !matches!(u.scheme(), "nats" | "tls")
                || !u.username().is_empty()
                || u.password().is_some()
                || !matches!(u.path(), "" | "/")
                || u.query().is_some()
                || u.fragment().is_some()
            {
                return Err(error(
                    ErrorCode::PolicyDenied,
                    "NATS URLs require nats/tls and no credentials, path, query or fragment",
                ));
            }
            if self.token_secret.is_some() && u.scheme() != "tls" {
                return Err(error(
                    ErrorCode::PolicyDenied,
                    "NATS authentication requires TLS",
                ));
            }
            policy
                .check_host_port(
                    u.host_str()
                        .ok_or_else(|| error(ErrorCode::InvalidArgument, "NATS host required"))?,
                    u.port().unwrap_or(4222),
                )
                .map_err(|e| error(e.code(), "NATS endpoint not allowed"))?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Completion {
    closed: AtomicBool,
    unhealthy: AtomicBool,
    notify: Notify,
}

/// Lives inside the SDK's event-callback closure, NOT in the adapter. The
/// closure drops only after ConnectionHandler drops the last events_tx and
/// the event task drains its queue. Event::Closed itself is lossy (try_send).
struct SdkLifetime {
    lease: Option<MemoryLease>,
    shutdown_guard: Option<Arc<dyn Send + Sync>>,
    completion: Arc<Completion>,
}
impl Drop for SdkLifetime {
    fn drop(&mut self) {
        // Credit is refunded before waking the lifecycle waiter.
        drop(self.lease.take());
        drop(self.shutdown_guard.take());
        self.completion.closed.store(true, Ordering::Release);
        self.completion.notify.notify_waiters();
    }
}

pub struct Connection {
    // Never expose cloneable SDK handles to integration callers. Their scope
    // must end before close() waits for the actual SDK tasks.
    pub(super) client: async_nats::Client,
    pub(super) pull_bytes: usize,
    pub(super) subscription_capacity: usize,
    completion: Arc<Completion>,
}

impl Connection {
    pub async fn open(
        config: &ConnectionConfig,
        policy: &TargetPolicy,
        secrets: &dyn SecretResolver,
        owner: &Arc<MemoryOwner>,
    ) -> Result<Self> {
        Self::open_inner(config, policy, secrets, owner, None).await
    }

    /// Keep external ownership (the CheckpointStore FileLock in production)
    /// tied to ACTUAL SDK exit, even when the caller's close waiter times out.
    /// An old background reader can never overlap a new writer of that store.
    pub async fn open_with_guard(
        config: &ConnectionConfig,
        policy: &TargetPolicy,
        secrets: &dyn SecretResolver,
        owner: &Arc<MemoryOwner>,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<Self> {
        Self::open_inner(config, policy, secrets, owner, Some(guard)).await
    }

    async fn open_inner(
        config: &ConnectionConfig,
        policy: &TargetPolicy,
        secrets: &dyn SecretResolver,
        owner: &Arc<MemoryOwner>,
        shutdown_guard: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Self> {
        config.validate(policy)?;
        let reserve = SDK_FIXED_RESERVATION
            // HeaderMap/Vec/String allocation can far exceed raw header bytes.
            .checked_add(
                config
                    .pull_bytes
                    .checked_mul(SDK_EXPANSION)
                    .ok_or_else(|| {
                        error(ErrorCode::BoundExceeded, "NATS SDK expansion overflow")
                    })?,
            )
            .and_then(|n| n.checked_add(config.subscription_capacity * (MAX_SUBJECT_BYTES + 512)))
            .ok_or_else(|| error(ErrorCode::BoundExceeded, "NATS SDK reservation overflow"))?;
        let lease = owner.acquire(CreditKind::Reservation, reserve)?;
        let completion = Arc::new(Completion::default());
        let lifetime = Arc::new(SdkLifetime {
            lease: Some(lease),
            shutdown_guard,
            completion: completion.clone(),
        });
        let mut options = async_nats::ConnectOptions::new()
            .subscription_capacity(config.subscription_capacity)
            .client_capacity(COMMAND_CAPACITY)
            .read_buffer_capacity(u16::MAX)
            .ignore_discovered_servers()
            // async-nats 0.50 maps zero to None (= unlimited), not "off".
            // Bound SDK attempts to one pass over the configured endpoints.
            // Any discontinuity still makes this reader sticky-unhealthy;
            // a successful SDK reconnect never authorizes source continuation.
            .max_reconnects(config.servers.len())
            .ping_interval(Duration::from_secs(1))
            .request_timeout(Some(REQUEST_TIMEOUT))
            .connection_timeout(REQUEST_TIMEOUT)
            .event_callback(move |event| {
                let lifetime = Arc::clone(&lifetime);
                async move {
                    if matches!(
                        event,
                        async_nats::Event::SlowConsumer(_)
                            | async_nats::Event::ServerError(_)
                            | async_nats::Event::ClientError(_)
                            | async_nats::Event::Disconnected
                    ) {
                        lifetime.completion.unhealthy.store(true, Ordering::Release);
                    }
                    // Force capture of the whole lifetime (not just its Arc
                    // completion field through Rust's disjoint-field capture).
                    drop(lifetime);
                }
            });
        if let Some(reference) = &config.token_secret {
            let token = secrets
                .resolve(reference)
                .map_err(|e| error(e.code(), "NATS token SecretRef resolution failed"))?;
            if token.is_empty() || token.len() > 4096 {
                return Err(error(ErrorCode::InvalidArgument, "NATS token size invalid"));
            }
            options = options.token(token);
        }
        let servers = config
            .servers
            .iter()
            .map(|s| s.parse::<async_nats::ServerAddr>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| error(ErrorCode::InvalidArgument, "invalid NATS server address"))?;
        let client = options
            .connect(servers)
            .await
            .map_err(|_| error(ErrorCode::JobFailed, "NATS connection failed").retryable(true))?;
        let connection = Self {
            client,
            completion,
            pull_bytes: config.pull_bytes,
            subscription_capacity: config.subscription_capacity,
        };
        // This profile requires a deliberately bounded broker. SDK read_buffer
        // capacity is initial capacity, NOT a frame limit or a hostile-server
        // RSS guarantee. Never infer a byte bound from subscription count.
        if let Err(error) = check_server_profile(
            &connection.client.server_info().version,
            connection.client.server_info().max_payload,
        ) {
            let _ = connection.close().await;
            return Err(error);
        }
        Ok(connection)
    }

    pub fn check_health(&self) -> Result<()> {
        if self.completion.closed.load(Ordering::Acquire)
            || self.completion.unhealthy.load(Ordering::Acquire)
        {
            return Err(error(
                ErrorCode::JobFailed,
                "NATS connection ended or lost protocol continuity",
            )
            .retryable(true));
        }
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        let Self {
            client, completion, ..
        } = self;
        // drain() queues a command only. Always drop the command sender even
        // on timeout; last-sender closure also terminates ConnectionHandler.
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, client.drain()).await;
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
                "NATS close not joined; SDK credit remains held, replacement must not proceed",
            )
        })
    }
}

pub(super) fn error(code: ErrorCode, message: &str) -> SparrowError {
    SparrowError::new(code, message)
}

fn check_server_profile(version: &str, max_payload: usize) -> Result<()> {
    if version != "2.14.6" {
        return Err(error(
            ErrorCode::FeatureUnavailable,
            "JetStream Preview currently requires conformance-tested NATS Server 2.14.6",
        ));
    }
    if max_payload > MAX_MESSAGE_BYTES {
        return Err(error(
            ErrorCode::BoundExceeded,
            "JetStream broker max_payload must be <= 65536 bytes",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn k2_server_profile_refuses_unverified_version_and_unbounded_payload() {
        assert_eq!(
            check_server_profile("2.14.5", 65536).unwrap_err().code,
            ErrorCode::FeatureUnavailable
        );
        assert_eq!(
            check_server_profile("2.14.6", 65537).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        check_server_profile("2.14.6", 65536).unwrap();
    }
}

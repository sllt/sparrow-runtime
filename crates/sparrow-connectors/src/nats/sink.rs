//! NATS Core publish Sink: one JSON message per row to a static subject.
//! Live, at-most-once, no ack, no replay.
//!
//! `nats_sink_published` counts messages handed to the client, not server
//! receipts (NATS Core has none). A successful SDK `flush` on shutdown means
//! pending writes reached the local socket, not that the broker received them. Rows whose
//! publish fails or times out are counted and dropped, never retried.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{JsonLimits, PayloadFormat};
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{CreditKind, ErrorCode, InflightCounter, RestoreClaim, Result, RowBatch};
use tokio_util::sync::CancellationToken;

use super::client::{BoundToken, CoreClient, NatsClientConfig, Role, MAX_RECONNECT_DELAY};
use super::common::{self, error};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::{SecretResolver, TargetPolicy};

const MAX_OUTBOX: usize = 4096;

#[derive(Clone, Debug)]
pub struct NatsSinkConfig {
    pub client: NatsClientConfig,
    /// Literal publish subject (no wildcards, no templating).
    pub subject: String,
    pub outbox_capacity: usize,
    /// Bound on handing one message to the client (command buffer full,
    /// e.g. while reconnecting). On expiry the row is dropped and counted.
    pub publish_timeout: Duration,
    /// Shutdown budget: publish batches already queued, then flush.
    pub flush_timeout: Duration,
    pub restore: RestoreClaim,
    /// Message payload format: one JSON object (default) or one CSV record.
    pub payload_format: PayloadFormat,
}

impl NatsSinkConfig {
    pub fn new(servers: Vec<String>, subject: impl Into<String>) -> Self {
        Self {
            client: NatsClientConfig::new(servers),
            subject: subject.into(),
            outbox_capacity: 16,
            publish_timeout: Duration::from_secs(2),
            flush_timeout: Duration::from_secs(2),
            restore: RestoreClaim::None,
            payload_format: PayloadFormat::Json,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::NATS_SINK
    }

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        common::check_subject(&self.subject, false)?;
        let window = Duration::from_millis(10)..=Duration::from_secs(30);
        if !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
            || !window.contains(&self.publish_timeout)
            || !window.contains(&self.flush_timeout)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS sink bounds: outbox_capacity 1..=4096, publish/flush timeouts 10..=30000 ms",
            ));
        }
        self.client.validate(secrets, policy)
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        self.client.check_reservation_budget(reservation_budget)
    }
}

pub struct NatsSink {
    pub config: NatsSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    token: Option<BoundToken>,
    owner: Arc<sparrow_model::MemoryOwner>,
}

impl std::fmt::Debug for NatsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSink")
            .field("servers", &self.config.client.servers.len())
            .field("subject", &self.config.subject)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// Outbox receipt: ack only if every row of the batch was handed off.
struct BatchReceipt(Option<Arc<InflightCounter>>);
impl BatchReceipt {
    fn ack(&mut self) {
        if let Some(outbox) = self.0.take() {
            outbox.ack();
        }
    }
}
impl Drop for BatchReceipt {
    fn drop(&mut self) {
        if let Some(outbox) = self.0.take() {
            outbox.fail();
        }
    }
}

enum Session {
    /// Input ended or the job stopped; flushed and closed.
    Finished(Option<tokio::time::Instant>),
    /// The SDK closed after its bounded reconnect attempts; open again.
    Lost,
}

impl NatsSink {
    pub fn bind(
        config: NatsSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        owner: Arc<sparrow_model::MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let token = config.client.bind(secrets, policy)?;
        Ok(Self {
            config,
            diag,
            token,
            owner,
        })
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx = rx.into();
        let _lifecycle = self.diag.observation.lifecycle(false);
        let mut backoff = Duration::from_millis(100);
        loop {
            if cancel.is_cancelled() {
                break;
            }
            self.diag.observation.health(
                false,
                HealthState::Connecting,
                "nats_sink_connecting",
                None,
            );
            let opened = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                c = CoreClient::open(&self.config.client, self.token.as_ref(), &self.owner, &self.diag, Role::Sink) => c,
            };
            let client = match opened {
                Ok(client) => client,
                Err(e) => {
                    self.diag
                        .nats_sink_client_errors
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.health(
                        false,
                        HealthState::Reconnecting,
                        "nats_sink_connect_failed",
                        Some(e.code),
                    );
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
                    continue;
                }
            };
            backoff = Duration::from_millis(100);
            self.diag.nats_sink_sessions.fetch_add(1, Ordering::Relaxed);
            self.diag
                .observation
                .health(false, HealthState::Ready, "nats_sink_connected", None);
            match self
                .session(&client, &mut rx, &cancel, outbox.as_ref())
                .await
            {
                Session::Finished(deadline) => {
                    self.shutdown(client, &mut rx, outbox.as_ref(), deadline)
                        .await;
                    return;
                }
                Session::Lost => {
                    let _ = client.close(false).await;
                    self.diag.observation.health(
                        false,
                        HealthState::Reconnecting,
                        "nats_sink_connection_closed",
                        Some(ErrorCode::JobFailed),
                    );
                }
            }
        }
        // Stopped while not connected: nothing can be flushed.
        self.discard(&mut rx, outbox.as_ref());
    }

    async fn session(
        &self,
        client: &CoreClient,
        rx: &mut ObservedReceiver<RowBatch>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) -> Session {
        let mut deadline = None;
        loop {
            let batch = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Session::Finished(Some(tokio::time::Instant::now() + self.config.flush_timeout)),
                b = rx.recv() => b,
            };
            let Some(batch) = batch else {
                return Session::Finished(None);
            };
            if !self
                .publish_batch(client, &batch, outbox, &mut deadline, Some(cancel))
                .await
            {
                return Session::Lost;
            }
            if deadline.is_some() {
                return Session::Finished(deadline);
            }
        }
    }

    /// Returns false if the client closed (remaining rows counted failed).
    async fn publish_batch(
        &self,
        client: &CoreClient,
        batch: &RowBatch,
        outbox: Option<&Arc<InflightCounter>>,
        deadline: &mut Option<tokio::time::Instant>,
        cancel: Option<&CancellationToken>,
    ) -> bool {
        let mut receipt = BatchReceipt(outbox.cloned());
        let mut delivered = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        let limit = self
            .config
            .client
            .max_payload_bytes
            .min(client.server_max_payload())
            .min(JsonLimits::default().max_bytes);
        let schema = batch.schema();
        let mut all = true;
        for (i, row) in batch.rows().iter().enumerate() {
            if deadline.is_none() && cancel.is_some_and(CancellationToken::is_cancelled) {
                *deadline = Some(tokio::time::Instant::now() + self.config.flush_timeout);
            }
            if deadline.is_some_and(|at| tokio::time::Instant::now() >= at) {
                self.diag
                    .nats_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                return true; // Receipt/delivery guards fail this partial batch.
            }
            let started = std::time::Instant::now();
            // Bound the writer before allocating output (JSON: the single-row
            // array envelope is removed in place; CSV: header + record).
            // Format-specific encoder scratch is also pre-admitted.
            let format = &self.config.payload_format;
            let scratch = format.encode_scratch(schema, row);
            let encoded = (|| {
                let mut lease = self.owner.acquire(CreditKind::Reservation, scratch)?;
                // The SDK reservation already covers the bounded working
                // payload plus command/writer overlap. Synchronous encoder
                // tree scratch must not consume credit while publish waits.
                format.encode_row_bounded_with_capacity(schema, row, limit, |capacity| {
                    lease.grow_to(scratch.saturating_add(capacity))
                })
            })();
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            let body = match encoded {
                Ok(encoded) => encoded,
                Err(e) if e.code == ErrorCode::BoundExceeded => {
                    all = false;
                    self.diag
                        .nats_sink_dropped_oversize
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(e) => {
                    all = false;
                    if e.code == ErrorCode::ResourceExhausted {
                        self.diag.nats_sink_failed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.diag.csv_encode_error(&self.config.payload_format);
                        self.diag
                            .nats_sink_dropped_bad
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };
            let publish = client
                .client
                .publish(self.config.subject.clone(), body.into());
            tokio::pin!(publish);
            let published = loop {
                let budget = deadline.map_or(self.config.publish_timeout, |at| {
                    at.saturating_duration_since(tokio::time::Instant::now())
                        .min(self.config.publish_timeout)
                });
                let wait = tokio::time::timeout(budget, publish.as_mut());
                if let Some(cancel) = cancel.filter(|_| deadline.is_none()) {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            // Start exactly one shutdown deadline, even in the
                            // middle of a publish blocked on the SDK queue.
                            *deadline = Some(tokio::time::Instant::now() + self.config.flush_timeout);
                        }
                        result = wait => break result,
                    }
                } else {
                    break wait.await;
                }
            };
            match published {
                Ok(Ok(())) => {
                    self.diag
                        .nats_sink_published
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.progress(false, 1);
                }
                Ok(Err(_)) | Err(_) => {
                    all = false;
                    self.diag.nats_sink_failed.fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.health(
                        false,
                        HealthState::Reconnecting,
                        "nats_sink_publish_failed",
                        Some(ErrorCode::JobFailed),
                    );
                    if client.is_closed() {
                        let rest = (batch.num_rows() - i - 1) as u64;
                        self.diag
                            .nats_sink_failed
                            .fetch_add(rest, Ordering::Relaxed);
                        return false;
                    }
                }
            }
        }
        if all {
            delivered.complete();
            receipt.ack();
        }
        true
    }

    /// Flush on stop/EOF: publish batches already queued in the outbox
    /// within the shared `flush_timeout`, then a local socket flush and drain.
    async fn shutdown(
        &self,
        client: CoreClient,
        rx: &mut ObservedReceiver<RowBatch>,
        outbox: Option<&Arc<InflightCounter>>,
        deadline: Option<tokio::time::Instant>,
    ) {
        let mut deadline = Some(
            deadline.unwrap_or_else(|| tokio::time::Instant::now() + self.config.flush_timeout),
        );
        rx.close();
        while tokio::time::Instant::now() < deadline.expect("shutdown deadline") {
            let Ok(batch) = rx.try_recv() else { break };
            if !self
                .publish_batch(&client, &batch, outbox, &mut deadline, None)
                .await
            {
                break;
            }
        }
        self.discard(rx, outbox);
        let remaining = deadline
            .expect("shutdown deadline")
            .saturating_duration_since(tokio::time::Instant::now());
        let flushed = matches!(
            tokio::time::timeout(remaining, client.client.flush()).await,
            Ok(Ok(()))
        );
        if flushed {
            self.diag.nats_sink_flushes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.diag
                .nats_sink_flush_failed
                .fetch_add(1, Ordering::Relaxed);
        }
        let _ = client.close(false).await;
        self.diag
            .observation
            .health(false, HealthState::Stopped, "nats_sink_stopped", None);
    }

    fn discard(&self, rx: &mut ObservedReceiver<RowBatch>, outbox: Option<&Arc<InflightCounter>>) {
        rx.close();
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .nats_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
        }
    }
}

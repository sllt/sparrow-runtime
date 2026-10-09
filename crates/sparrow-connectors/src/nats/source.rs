//! NATS Core subscribe Source: live, at-most-once, no ack, no replay.
//!
//! - One JSON object per message (`sparrow-formats` decoder). Headers ignored.
//! - Rows enter the byte-accounted Kernel ingress one at a time; while the
//!   bounded inbox is full the pump waits (cancel-aware) holding one row.
//! - Meanwhile a private wire actor keeps reading into its bounded
//!   prefetch buffer; when that is full it drops messages and increments
//!   `nats_source_slow_consumer`. Nothing is
//!   buffered without bound and nothing is redelivered.
//! - Reconnect is bounded per outage (see [`NatsClientConfig`]); messages
//!   published while disconnected are lost. When the actor gives up the Source
//!   fails retryably and the supervisor restart policy applies.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{decode_json_row, JsonLimits};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryOwner, QueuedRow, RestoreClaim, Result, Row, Schema,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::client::{BoundToken, NatsClientConfig};
use super::common::{self, error};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::{SecretResolver, TargetPolicy};

pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
const MAX_INBOX: usize = 4096;

#[derive(Clone, Debug)]
pub struct NatsSourceConfig {
    pub client: NatsClientConfig,
    /// Subscribe subject; `*` and trailing `>` wildcards allowed.
    pub subject: String,
    /// Optional queue group: each message goes to one member of the group.
    pub queue_group: Option<String>,
    pub inbox_capacity: usize,
    /// Decoded-row Queue credit for the inbox (P1-27).
    pub inbox_bytes: usize,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    pub fail_on_decode: bool,
    pub restore: RestoreClaim,
}

impl NatsSourceConfig {
    pub fn new(servers: Vec<String>, subject: impl Into<String>, schema: Schema) -> Self {
        Self {
            client: NatsClientConfig::new(servers),
            subject: subject.into(),
            queue_group: None,
            inbox_capacity: 16,
            inbox_bytes: DEFAULT_INBOX_BYTES,
            schema,
            json_limits: JsonLimits::default(),
            fail_on_decode: false,
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::NATS_SOURCE
    }

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        common::check_subject(&self.subject, true)?;
        if let Some(group) = &self.queue_group {
            common::check_queue_group(group)?;
        }
        if !(1..=MAX_INBOX).contains(&self.inbox_capacity) || self.json_limits.max_bytes > 64 * 1024
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS inbox_capacity must be 1..=4096; records remain <=64KiB",
            ));
        }
        self.check_inbox_budget(sparrow_model::ResourceBudget::compact().queue_bytes)?;
        self.client.validate(secrets, policy)
    }

    /// P1-27: inbox payload credit plus channel metadata within the job
    /// Queue budget and the static 4 MiB ceiling.
    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        if self.inbox_bytes == 0
            || self
                .inbox_bytes
                .saturating_add(QueuedRow::channel_budget(self.inbox_capacity))
                > queue_budget.min(4 * 1024 * 1024)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
            ));
        }
        Ok(())
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        self.client.check_reservation_budget(reservation_budget)
    }
}

pub struct NatsSource {
    pub config: NatsSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    token: Option<BoundToken>,
}

impl std::fmt::Debug for NatsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSource")
            .field("servers", &self.config.client.servers.len())
            .field("subject", &self.config.subject)
            .field("queue_group", &self.config.queue_group)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

struct Ingress<'a> {
    tx: &'a ObservedSender<QueuedRow>,
    owner: &'a Arc<MemoryOwner>,
    queue: &'a Arc<MemoryOwner>,
    max_row_bytes: usize,
}

impl NatsSource {
    pub fn bind(
        config: NatsSourceConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        let token = config.client.bind(secrets, policy)?;
        Ok(Self {
            config,
            diag,
            token,
        })
    }

    /// Subscribe until cancelled. Requires the byte-accounted ingress
    /// (`JobRequest::with_budgeted_live_io` / graph `budgeted`).
    pub async fn run_budgeted(
        self,
        tx: impl Into<ObservedSender<QueuedRow>>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        let tx = tx.into();
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(error(
                ErrorCode::InvalidArgument,
                "NATS inbox_capacity does not match channel",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes)?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "nats-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let _lifecycle = self.diag.observation.lifecycle(true);
        self.diag
            .observation
            .health(true, HealthState::Connecting, "nats_connecting", None);
        let child = cancel.child_token();
        let _cancel_on_drop = child.clone().drop_guard();
        let mut wire = super::wire::Wire::start(
            self.config.client.clone(),
            self.token.clone(),
            self.config.subject.clone(),
            self.config.queue_group.clone(),
            self.config.json_limits.max_bytes,
            owner.clone(),
            self.diag.clone(),
            child.clone(),
        )?;
        let result = loop {
            let message = tokio::select! {
                biased;
                _ = child.cancelled() => break Ok(()),
                m = wire.messages.recv() => m,
            };
            let Some(message) = message else {
                // Subscription channel closed: the actor exhausted its bounded
                // reconnect attempts (or the server closed us).
                self.diag.observation.health(
                    true,
                    HealthState::Failed,
                    "nats_connection_closed",
                    Some(ErrorCode::JobFailed),
                );
                break Err(error(
                    ErrorCode::JobFailed,
                    "NATS connection closed after bounded reconnect attempts",
                )
                .retryable(true));
            };
            self.diag
                .nats_source_received
                .fetch_add(1, Ordering::Relaxed);
            let Some(payload) = message.payload.as_deref() else {
                self.diag
                    .nats_source_dropped_oversize
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            match self
                .ingest(payload, &ingress, &child, message.received_at)
                .await
            {
                Ok(true) => {}
                Ok(false) => break Ok(()),
                Err(e) => break Err(e),
            }
        };
        let actor = wire.close().await;
        // Actor failure must not be mistaken for external cancellation merely
        // because it interrupted admission using the child token.
        let result = result.and(actor);
        if let Err(e) = &result {
            self.diag.observation.health(
                true,
                HealthState::Failed,
                "nats_wire_failed",
                Some(e.code),
            );
        }
        result
    }

    async fn ingest(
        &self,
        payload: &[u8],
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        received_at: std::time::Instant,
    ) -> Result<bool> {
        if payload.len() > self.config.json_limits.max_bytes {
            self.diag
                .nats_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        let estimate = payload
            .len()
            .saturating_mul(64)
            .saturating_add(
                self.config
                    .schema
                    .fields
                    .len()
                    .saturating_mul(std::mem::size_of::<sparrow_model::Scalar>())
                    .saturating_mul(2),
            )
            .saturating_add(4096);
        let Ok(_scratch) = ingress.owner.acquire(CreditKind::Reservation, estimate) else {
            self.diag
                .nats_source_dropped_budget
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        };
        let started = std::time::Instant::now();
        let decoded = decode_json_row(&self.config.schema, payload, &self.config.json_limits);
        self.diag
            .observation
            .record(Latency::Decode, started.elapsed());
        let row = match decoded {
            Ok(row) => row,
            Err(e) => {
                self.diag
                    .nats_source_dropped_bad
                    .fetch_add(1, Ordering::Relaxed);
                self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                if self.config.fail_on_decode {
                    self.diag.observation.health(
                        true,
                        HealthState::Failed,
                        "nats_decode_failed",
                        Some(e.code),
                    );
                    return Err(error(e.code, "NATS record decode failed (fail_on_decode)"));
                }
                return Ok(true);
            }
        };
        self.diag.observation.progress(true, 1);
        self.admit(row, ingress, cancel, OriginSpan::at(received_at))
            .await
    }

    /// Cancel-aware wait for a channel slot and Queue credit, holding one
    /// Reservation-charged working row (same contract as HTTP Poll / MQTT).
    async fn admit(
        &self,
        row: Row,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        origin: OriginSpan,
    ) -> Result<bool> {
        let _admission = self.diag.observation.timer(Latency::SourceAdmission);
        let bytes = QueuedRow::accounted_bytes(&row);
        if bytes
            > ingress
                .queue
                .budget()
                .queue_bytes
                .min(ingress.max_row_bytes)
        {
            self.diag
                .nats_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        let Ok(_working) = ingress.owner.acquire(CreditKind::Reservation, bytes) else {
            self.diag
                .nats_source_dropped_budget
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        };
        let mut row = Some(row);
        let mut observed_wait: Option<sparrow_io::observed::Wait<'_>> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let budget_full = match ingress.tx.try_reserve() {
                Ok(permit) => match QueuedRow::try_new(
                    row.take().expect("pending row"),
                    ingress.queue,
                    &self.diag.nats_source_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        self.diag.nats_source_rows.fetch_add(1, Ordering::Relaxed);
                        return Ok(true);
                    }
                    Err((r, _)) => {
                        row = Some(r);
                        true
                    }
                },
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => false,
            };
            if observed_wait.is_none() {
                observed_wait = Some(ingress.tx.observe_wait());
                self.diag
                    .nats_source_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(false),
                _ = async {
                    if budget_full {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        let _ = ingress.tx.reserve().await;
                    }
                } => {}
            }
        }
    }
}

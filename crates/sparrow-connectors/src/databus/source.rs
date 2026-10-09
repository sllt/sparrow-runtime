//! DataBus Source: subscribe to a topic pattern on the in-process bus.
//!
//! Each message is one JSON object (the publishing Sink's row encoding)
//! decoded with this pipeline's stream schema. A message over the record
//! limit is rejected by length; otherwise the decoder working set is charged
//! to the job reservation before decoding and held until the row is queued
//! (no credit: counted `dropped_budget`, not decoded). Rows enter the byte-accounted
//! Kernel ingress one at a time; while that inbox is full the pump waits and
//! the subscriber buffer fills, where the overflow policy applies.

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

use super::{err, DataBus, Overflow, SubscriptionConfig};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;

pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
pub const DEFAULT_BUFFER_MESSAGES: usize = 1024;
pub const DEFAULT_BUFFER_BYTES: usize = 256 * 1024;
pub const DEFAULT_BLOCK_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_INBOX: usize = 4096;

#[derive(Clone, Debug)]
pub struct DataBusSourceConfig {
    pub subscription: SubscriptionConfig,
    pub inbox_capacity: usize,
    /// Decoded-row Queue credit for the Kernel inbox.
    pub inbox_bytes: usize,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    pub fail_on_decode: bool,
    pub restore: RestoreClaim,
}

impl DataBusSourceConfig {
    pub fn new(topic: impl Into<String>, schema: Schema) -> Self {
        Self {
            subscription: SubscriptionConfig {
                pattern: topic.into(),
                capacity: DEFAULT_BUFFER_MESSAGES,
                max_bytes: DEFAULT_BUFFER_BYTES,
                overflow: Overflow::default(),
                block_timeout: DEFAULT_BLOCK_TIMEOUT,
            },
            inbox_capacity: 16,
            inbox_bytes: DEFAULT_INBOX_BYTES,
            schema,
            json_limits: JsonLimits::default(),
            fail_on_decode: false,
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::DATABUS_SOURCE
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        self.subscription.validate()?;
        if !(1..=MAX_INBOX).contains(&self.inbox_capacity) || self.json_limits.max_bytes > 64 * 1024
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "databus inbox_capacity must be 1..=4096; records remain <=64KiB",
            ));
        }
        self.check_inbox_budget(sparrow_model::ResourceBudget::compact().queue_bytes)?;
        self.check_reservation_budget(sparrow_model::ResourceBudget::compact().reservation_bytes)
    }

    /// Inbox payload credit plus channel metadata within the job Queue
    /// budget and the static 4 MiB ceiling.
    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        if self.inbox_bytes == 0
            || self
                .inbox_bytes
                .saturating_add(QueuedRow::channel_budget(self.inbox_capacity))
                > queue_budget.min(4 * 1024 * 1024)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "databus inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
            ));
        }
        Ok(())
    }

    /// The subscriber buffer is charged to the job reservation (<= half).
    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.subscription.reservation() > reservation_budget / 2 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "databus buffer_bytes (+64 B per buffer_capacity slot) exceeds half the job reservation budget",
            ));
        }
        Ok(())
    }
}

pub struct DataBusSource {
    pub config: DataBusSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    bus: Arc<DataBus>,
}

impl std::fmt::Debug for DataBusSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataBusSource")
            .field("topic", &self.config.subscription.pattern)
            .field("overflow", &self.config.subscription.overflow)
            .finish_non_exhaustive()
    }
}

struct Ingress<'a> {
    tx: &'a ObservedSender<QueuedRow>,
    owner: &'a Arc<MemoryOwner>,
    queue: &'a Arc<MemoryOwner>,
    max_row_bytes: usize,
}

impl DataBusSource {
    pub fn bind(
        config: DataBusSourceConfig,
        bus: Arc<DataBus>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self { config, diag, bus })
    }

    /// Subscribe until cancelled. Requires the byte-accounted ingress.
    pub async fn run_budgeted(
        self,
        tx: impl Into<ObservedSender<QueuedRow>>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        let tx = tx.into();
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(err(
                ErrorCode::InvalidArgument,
                "databus inbox_capacity does not match channel",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes)?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "databus-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let _lifecycle = self.diag.observation.lifecycle(true);
        let subscriber =
            self.bus
                .subscribe(self.config.subscription.clone(), &owner, self.diag.clone())?;
        self.diag
            .observation
            .health(true, HealthState::Ready, "databus_subscribed", None);
        let result = loop {
            let message = tokio::select! {
                biased;
                _ = cancel.cancelled() => break Ok(()),
                m = subscriber.recv() => m,
            };
            let Some(message) = message else {
                break Ok(());
            };
            match self
                .ingest(&message, &ingress, &cancel, std::time::Instant::now())
                .await
            {
                Ok(true) => {}
                Ok(false) => break Ok(()),
                Err(e) => break Err(e),
            }
        };
        // Detach (discarding and counting anything still buffered) before
        // reporting stopped, so a restart re-attaches to a clean slot.
        drop(subscriber);
        if result.is_ok() {
            self.diag
                .observation
                .health(true, HealthState::Stopped, "databus_detached", None);
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
                .databus_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        // Parser tree + Row expansion, held until the row is queued.
        let scratch = crate::scratch::json_decode_scratch(payload.len(), &self.config.schema);
        let Ok(_scratch) = ingress.owner.acquire(CreditKind::Reservation, scratch) else {
            self.diag
                .databus_source_dropped_budget
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
                    .databus_source_dropped_bad
                    .fetch_add(1, Ordering::Relaxed);
                self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                if self.config.fail_on_decode {
                    self.diag.observation.health(
                        true,
                        HealthState::Failed,
                        "databus_decode_failed",
                        Some(e.code),
                    );
                    return Err(err(e.code, "databus record decode failed (fail_on_decode)"));
                }
                return Ok(true);
            }
        };
        self.diag.observation.progress(true, 1);
        self.admit(row, ingress, cancel, OriginSpan::at(received_at))
            .await
    }

    /// Cancel-aware wait for a channel slot and Queue credit, holding one
    /// Reservation-charged working row (same contract as NATS / HTTP Poll).
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
                .databus_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        let Ok(_working) = ingress.owner.acquire(CreditKind::Reservation, bytes) else {
            self.diag
                .databus_source_dropped_budget
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
                    &self.diag.databus_source_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        self.diag
                            .databus_source_rows
                            .fetch_add(1, Ordering::Relaxed);
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
                    .databus_source_backpressure_waits
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

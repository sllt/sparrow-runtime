//! DataBus Sink: publish each output row as one JSON object to a literal
//! topic on the in-process bus.
//!
//! A batch is reported done (outbox receipt) only after the bus has offered
//! every row to every matching subscriber: accepted into its buffer, or
//! dropped by that subscriber's overflow policy (counted on the subscriber),
//! or – for `block` subscribers – after waiting up to their timeout. With no
//! subscriber the row is counted in `databus_sink_no_subscribers` and
//! discarded. None of this is a downstream processing receipt.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{encode_json_row, JsonLimits};
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{ErrorCode, InflightCounter, RestoreClaim, Result, RowBatch, SparrowError};
use tokio_util::sync::CancellationToken;

use super::{check_topic, err, DataBus, Publisher};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;

const MAX_OUTBOX: usize = 4096;

#[derive(Clone, Debug)]
pub struct DataBusSinkConfig {
    /// Literal topic (no wildcards).
    pub topic: String,
    pub outbox_capacity: usize,
    /// Stop/EOF budget for publishing batches already queued.
    pub flush_timeout: Duration,
    pub restore: RestoreClaim,
}

impl DataBusSinkConfig {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            outbox_capacity: 16,
            flush_timeout: Duration::from_secs(1),
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::DATABUS_SINK
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        check_topic(&self.topic, false)?;
        if !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
            || !(Duration::from_millis(10)..=Duration::from_secs(30)).contains(&self.flush_timeout)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "databus sink bounds: outbox_capacity 1..=4096, flush_timeout_ms 10..=30000",
            ));
        }
        Ok(())
    }
}

pub struct DataBusSink {
    pub config: DataBusSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    bus: Arc<DataBus>,
}

impl std::fmt::Debug for DataBusSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataBusSink")
            .field("topic", &self.config.topic)
            .finish_non_exhaustive()
    }
}

/// Outbox receipt: ack only if every row of the batch was offered.
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

impl DataBusSink {
    pub fn bind(
        config: DataBusSinkConfig,
        bus: Arc<DataBus>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self { config, diag, bus })
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx = rx.into();
        let _lifecycle = self.diag.observation.lifecycle(false);
        let publisher = match self.bus.publisher(&self.config.topic) {
            Ok(p) => p,
            Err(e) => {
                self.fatal(&e, &cancel);
                self.discard(&mut rx, outbox.as_ref());
                return;
            }
        };
        self.diag
            .observation
            .health(false, HealthState::Ready, "databus_publisher_ready", None);
        loop {
            let batch = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                b = rx.recv() => b,
            };
            let Some(batch) = batch else { break };
            self.publish_batch(&publisher, &batch, outbox.as_ref(), None)
                .await;
        }
        // Stop/EOF: publish what is already queued within flush_timeout.
        let deadline = tokio::time::Instant::now() + self.config.flush_timeout;
        rx.close();
        while tokio::time::Instant::now() < deadline {
            let Ok(batch) = rx.try_recv() else { break };
            self.publish_batch(&publisher, &batch, outbox.as_ref(), Some(deadline))
                .await;
        }
        self.discard(&mut rx, outbox.as_ref());
        drop(publisher);
        self.diag
            .observation
            .health(false, HealthState::Stopped, "databus_sink_stopped", None);
    }

    async fn publish_batch(
        &self,
        publisher: &Publisher,
        batch: &RowBatch,
        outbox: Option<&Arc<InflightCounter>>,
        deadline: Option<tokio::time::Instant>,
    ) {
        let mut receipt = BatchReceipt(outbox.cloned());
        let mut delivered = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        let limit = JsonLimits::default().max_bytes;
        let schema = batch.schema();
        let mut all = true;
        for row in batch.rows() {
            let started = std::time::Instant::now();
            let encoded = encode_json_row(schema, row);
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            let body = match encoded {
                Ok(body) if body.len() <= limit => body,
                Ok(_) => {
                    all = false;
                    self.diag
                        .databus_sink_dropped_oversize
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(_) => {
                    all = false;
                    self.diag
                        .databus_sink_dropped_bad
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            let publish = publisher.publish(Arc::from(body));
            let outcome = match deadline {
                // On stop a block-policy subscriber may not hold us past the
                // flush budget.
                Some(at) => match tokio::time::timeout_at(at, publish).await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        all = false;
                        self.diag
                            .databus_sink_discarded_on_close
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                },
                None => publish.await,
            };
            self.diag
                .databus_sink_published
                .fetch_add(1, Ordering::Relaxed);
            self.diag
                .databus_sink_deliveries
                .fetch_add(outcome.accepted as u64, Ordering::Relaxed);
            if outcome.matched == 0 {
                self.diag
                    .databus_sink_no_subscribers
                    .fetch_add(1, Ordering::Relaxed);
            }
            if outcome.blocked {
                self.diag
                    .databus_sink_blocked_publishes
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.diag.observation.progress(false, 1);
        }
        self.diag
            .databus_sink_batches
            .fetch_add(1, Ordering::Relaxed);
        if all {
            delivered.complete();
            receipt.ack();
        }
    }

    fn fatal(&self, e: &SparrowError, cancel: &CancellationToken) {
        self.diag.databus_sink_fatal.fetch_add(1, Ordering::Relaxed);
        self.diag.observation.health(
            false,
            HealthState::Failed,
            "databus_sink_failed",
            Some(e.code),
        );
        cancel.cancel();
    }

    fn discard(&self, rx: &mut ObservedReceiver<RowBatch>, outbox: Option<&Arc<InflightCounter>>) {
        rx.close();
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .databus_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
        }
    }
}

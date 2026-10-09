//! DataBus Sink: publish each output row as one JSON object to a literal
//! topic on the in-process bus.
//!
//! A batch is reported done (outbox receipt) only after the bus has offered
//! every row to every matching subscriber: accepted into its buffer, or
//! dropped by that subscriber's overflow policy (counted on the subscriber),
//! or – for `block` subscribers – after waiting up to their timeout. With no
//! subscriber the row is counted in `databus_sink_no_subscribers` and
//! discarded. None of this is a downstream processing receipt.
//!
//! Each row is encoded under a reservation lease (encoder scratch plus the
//! bounded output, stopped at the 64 KiB record limit before growing past
//! it); the lease is held while the publisher still owns the payload. A row
//! without credit is counted `databus_sink_dropped_budget`.
//!
//! Stop: the first cancellation starts one `flush_timeout` deadline shared by
//! the batch in flight (including a publish waiting on a `block` subscriber)
//! and the batches still queued. Rows not fully offered by then are counted
//! `discarded_on_close` and their batch receipt fails.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::JsonLimits;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{
    ErrorCode, InflightCounter, MemoryOwner, RestoreClaim, Result, RowBatch, SparrowError,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{check_topic, err, DataBus, Publisher};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::scratch::{encode_json_row_charged, EncodeRejected};

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
    owner: Arc<MemoryOwner>,
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
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            diag,
            bus,
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
        // Set once, by the first cancellation (or EOF), and shared by the
        // batch in flight and the queued ones.
        let mut deadline: Option<Instant> = None;
        loop {
            let batch = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                b = rx.recv() => b,
            };
            let Some(batch) = batch else { break };
            self.publish_batch(&publisher, &batch, outbox.as_ref(), &mut deadline, &cancel)
                .await;
            if deadline.is_some() {
                break;
            }
        }
        // Stop/EOF: publish what is already queued within the same deadline.
        let deadline = *deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
        rx.close();
        while Instant::now() < deadline {
            let Ok(batch) = rx.try_recv() else { break };
            self.publish_batch(
                &publisher,
                &batch,
                outbox.as_ref(),
                &mut Some(deadline),
                &cancel,
            )
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
        deadline: &mut Option<Instant>,
        cancel: &CancellationToken,
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
        for (i, row) in batch.rows().iter().enumerate() {
            if deadline.is_none() && cancel.is_cancelled() {
                *deadline = Some(Instant::now() + self.config.flush_timeout);
            }
            if deadline.is_some_and(|at| Instant::now() >= at) {
                // The receipt guard fails this partial batch.
                self.diag
                    .databus_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                return;
            }
            let started = std::time::Instant::now();
            let encoded = encode_json_row_charged(&self.owner, schema, row, limit);
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
            // Dropped at the end of this iteration, after the publish future.
            let (body, mut lease) = match encoded {
                Ok(encoded) => encoded,
                Err(rejected) => {
                    all = false;
                    let counter = match rejected {
                        EncodeRejected::Oversize => &self.diag.databus_sink_dropped_oversize,
                        EncodeRejected::Budget => &self.diag.databus_sink_dropped_budget,
                        EncodeRejected::Bad => &self.diag.databus_sink_dropped_bad,
                    };
                    counter.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            // Copy into the shared payload while the scratch lease still
            // covers both buffers, then keep only the payload charged while
            // the publisher owns it.
            let payload: Arc<[u8]> = Arc::from(body);
            let _ = lease.shrink_to(payload.len());
            let publish = publisher.publish(payload);
            tokio::pin!(publish);
            let outcome = loop {
                match *deadline {
                    // On stop a block-policy subscriber may not hold us past
                    // the shared flush deadline.
                    Some(at) => break tokio::time::timeout_at(at, publish.as_mut()).await.ok(),
                    None => tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            *deadline = Some(Instant::now() + self.config.flush_timeout);
                        }
                        outcome = publish.as_mut() => break Some(outcome),
                    },
                }
            };
            let Some(outcome) = outcome else {
                // Interrupted at the deadline; the rest of the batch is
                // discarded and the receipt fails.
                self.diag
                    .databus_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                return;
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

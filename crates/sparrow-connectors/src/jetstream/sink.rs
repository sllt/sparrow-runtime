//! JetStream publish Sink: one JSON message per row to a literal subject of
//! an existing stream, confirmed by the server's PubAck.
//!
//! Contract (see `docs/adr/005-jetstream-sink.md`):
//! - A batch is acknowledged to the outbox (and so to an aligned checkpoint
//!   barrier) only after every row of it received a PubAck.
//! - At most `max_inflight_acks` messages await a PubAck at once; each waits
//!   at most `ack_timeout` per attempt and is retried at most `max_retries`
//!   times with 100ms..2s backoff. A retry after a lost PubAck may store a
//!   duplicate unless a `Nats-Msg-Id` is set and the stream's duplicate
//!   window covers the retry.
//! - Anything that cannot be confirmed (retries exhausted, non-retriable
//!   publish error, encode/oversize, stream missing at start) fails the job
//!   instead of silently dropping: `jetstream_sink_fatal`.
//! - The stream is verified at start (exists, not sealed or a mirror, binds
//!   the subject). The sink never creates or edits streams.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::context::{GetStreamErrorKind, PublishErrorKind};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::{Context, ContextBuilder, ErrorCode as JsErrorCode};
use futures_util::stream::{self, StreamExt};
use sparrow_formats::encode_json_row;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState, Latency};
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, Result, Row, RowBatch,
    Scalar, Schema, SparrowError,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::capabilities::ConnectorCapabilities;
use crate::diag::IoDiagnostics;
use crate::nats::client::{BoundToken, CoreClient, NatsClientConfig, Role, MAX_RECONNECT_DELAY};
use crate::nats::common;
use crate::{SecretResolver, TargetPolicy};

pub const MAX_OUTBOX: usize = 4096;
pub const MAX_INFLIGHT_ACKS: usize = 256;
pub const DEFAULT_INFLIGHT_ACKS: usize = 8;
pub const MAX_RETRIES: u32 = 20;
pub const DEFAULT_RETRIES: u32 = 5;
pub const MAX_MSG_ID_BYTES: usize = 128;
/// Header block (`NATS/1.0`, `Nats-Msg-Id:`, CRLFs) beyond the id itself.
const HEADER_OVERHEAD: usize = 64;
/// Retained per in-flight message besides its payload (subject, headers,
/// SDK request state).
const RETAINED_OVERHEAD: usize = 8 * 1024;
const FIRST_RETRY_DELAY: Duration = Duration::from_millis(100);

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JetStreamSinkConfig {
    pub client: NatsClientConfig,
    /// Existing stream that must bind `subject`. Never auto-created.
    pub stream: String,
    /// Literal publish subject.
    pub subject: String,
    pub outbox_capacity: usize,
    /// Per attempt: send plus PubAck wait.
    pub ack_timeout: Duration,
    pub max_inflight_acks: usize,
    /// Extra attempts per message (0 = no retry).
    pub max_retries: u32,
    /// Stop/EOF budget for publishing and confirming already queued batches.
    pub flush_timeout: Duration,
    /// Output column (utf8 or integer) used as `Nats-Msg-Id`.
    pub msg_id_column: Option<String>,
}

impl JetStreamSinkConfig {
    pub fn new(
        servers: Vec<String>,
        stream: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        let mut client = NatsClientConfig::new(servers);
        client.capacity = DEFAULT_INFLIGHT_ACKS;
        Self {
            client,
            stream: stream.into(),
            subject: subject.into(),
            outbox_capacity: 16,
            ack_timeout: Duration::from_secs(2),
            max_inflight_acks: DEFAULT_INFLIGHT_ACKS,
            max_retries: DEFAULT_RETRIES,
            flush_timeout: Duration::from_secs(5),
            msg_id_column: None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::JETSTREAM_SINK
    }

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        common::check_stream_name(&self.stream)?;
        common::check_subject(&self.subject, false)?;
        if !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
            || !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&self.ack_timeout)
            || !(1..=MAX_INFLIGHT_ACKS).contains(&self.max_inflight_acks)
            || self.max_retries > MAX_RETRIES
            || !(Duration::from_millis(10)..=Duration::from_secs(60)).contains(&self.flush_timeout)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "JetStream sink bounds: outbox_capacity 1..=4096, ack_timeout_ms 100..=30000, max_inflight_acks 1..=256, max_retries 0..=20, flush_timeout_ms 10..=60000",
            ));
        }
        if let Some(column) = &self.msg_id_column {
            if column.is_empty() || column.len() > 256 {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "msg_id_column must name an output column",
                ));
            }
        }
        self.client.validate(secrets, policy)
    }

    /// SDK buffers plus payloads retained for retry while awaiting PubAcks.
    pub fn sdk_reservation(&self) -> usize {
        self.client.sdk_reservation().saturating_add(
            self.max_inflight_acks
                .saturating_mul(self.client.max_payload_bytes + RETAINED_OVERHEAD),
        )
    }

    fn retained_reservation(&self) -> usize {
        self.sdk_reservation() - self.client.sdk_reservation()
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.sdk_reservation() > reservation_budget / 2 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "JetStream sink buffers ((client_capacity + max_inflight_acks) x max_payload_bytes) exceed half the job reservation budget",
            ));
        }
        Ok(())
    }

    /// Output-schema check for `msg_id_column` (utf8 or integer).
    pub fn check_schema(&self, schema: &Schema) -> Result<()> {
        let Some(column) = &self.msg_id_column else {
            return Ok(());
        };
        let index = schema.index_of_name(column).ok_or_else(|| {
            err(
                ErrorCode::InvalidSchema,
                format!("msg_id_column `{column}` is not an output column"),
            )
        })?;
        use sparrow_model::DataType;
        match schema.fields[index].data_type {
            DataType::Utf8 | DataType::Int64 | DataType::UInt64 => Ok(()),
            _ => Err(err(
                ErrorCode::InvalidSchema,
                "msg_id_column must be utf8, int64 or uint64",
            )),
        }
    }
}

pub struct JetStreamSink {
    pub config: JetStreamSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    token: Option<BoundToken>,
    owner: Arc<MemoryOwner>,
}

impl std::fmt::Debug for JetStreamSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamSink")
            .field("servers", &self.config.client.servers.len())
            .field("stream", &self.config.stream)
            .field("subject", &self.config.subject)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// Outbox receipt: ack only when every row of the batch has a PubAck.
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

struct Session {
    core: CoreClient,
    context: Context,
    _retained: MemoryLease,
}

struct InflightGuard<'a>(&'a IoDiagnostics);
impl<'a> InflightGuard<'a> {
    fn new(diag: &'a IoDiagnostics) -> Self {
        diag.jetstream_sink_inflight.fetch_add(1, Ordering::Relaxed);
        Self(diag)
    }
}
impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0
            .jetstream_sink_inflight
            .fetch_sub(1, Ordering::Relaxed);
    }
}

fn next_delay(delay: Duration) -> Duration {
    (delay * 2).min(MAX_RECONNECT_DELAY)
}

impl JetStreamSink {
    pub fn bind(
        config: JetStreamSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
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
        let session = match self.connect(&cancel).await {
            Ok(Some(session)) => session,
            Ok(None) => {
                self.discard(&mut rx, outbox.as_ref(), false);
                return;
            }
            Err(e) => {
                self.fatal(&e, &cancel);
                self.discard(&mut rx, outbox.as_ref(), true);
                return;
            }
        };
        loop {
            let batch = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                batch = rx.recv() => batch,
            };
            let Some(batch) = batch else { break };
            if let Err(e) = self
                .publish_batch(&session, &batch, outbox.as_ref(), None)
                .await
            {
                self.fatal(&e, &cancel);
                self.discard(&mut rx, outbox.as_ref(), true);
                let _ = session.core.close(false).await;
                return;
            }
        }
        self.shutdown(session, &mut rx, outbox.as_ref(), &cancel)
            .await;
    }

    /// Connect and verify the stream, retrying transient failures within
    /// the same bounded budget as publishes. `Ok(None)` = cancelled.
    async fn connect(&self, cancel: &CancellationToken) -> Result<Option<Session>> {
        let mut delay = FIRST_RETRY_DELAY;
        let mut last = err(ErrorCode::JobFailed, "JetStream sink not connected");
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(None),
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = next_delay(delay);
            }
            self.diag.observation.health(
                false,
                HealthState::Connecting,
                "jetstream_sink_connecting",
                None,
            );
            let opened = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(None),
                c = CoreClient::open(&self.config.client, self.token.as_ref(), &self.owner, &self.diag, Role::JetStreamSink) => c,
            };
            let core = match opened {
                Ok(core) => core,
                Err(e) => {
                    self.diag
                        .jetstream_sink_client_errors
                        .fetch_add(1, Ordering::Relaxed);
                    last = e;
                    continue;
                }
            };
            let context = ContextBuilder::new()
                .timeout(self.config.ack_timeout)
                .max_ack_inflight(self.config.max_inflight_acks)
                .backpressure_on_inflight(true)
                .build(core.client.clone());
            let verified = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = core.close(false).await;
                    return Ok(None);
                }
                v = self.verify_stream(&context) => v,
            };
            match verified {
                Ok(()) => {
                    let retained = match self
                        .owner
                        .acquire(CreditKind::Reservation, self.config.retained_reservation())
                    {
                        Ok(lease) => lease,
                        Err(e) => {
                            let _ = core.close(false).await;
                            return Err(e);
                        }
                    };
                    self.diag
                        .jetstream_sink_sessions
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.health(
                        false,
                        HealthState::Ready,
                        "jetstream_sink_stream_verified",
                        None,
                    );
                    return Ok(Some(Session {
                        core,
                        context,
                        _retained: retained,
                    }));
                }
                Err(e) => {
                    let _ = core.close(false).await;
                    if !e.retryable {
                        return Err(e);
                    }
                    last = e;
                }
            }
        }
        Err(err(
            ErrorCode::JobFailed,
            format!(
                "JetStream sink could not connect and verify stream after {} attempts: {}",
                self.config.max_retries + 1,
                last.message
            ),
        ))
    }

    /// The stream must already exist, accept publishes and bind the subject.
    async fn verify_stream(&self, context: &Context) -> Result<()> {
        let stream = context
            .get_stream(&self.config.stream)
            .await
            .map_err(|e| match e.kind() {
                GetStreamErrorKind::JetStream(js)
                    if js.error_code() == JsErrorCode::STREAM_NOT_FOUND =>
                {
                    err(
                        ErrorCode::InvalidArgument,
                        format!(
                            "JetStream stream `{}` does not exist; this sink never creates streams",
                            self.config.stream
                        ),
                    )
                }
                GetStreamErrorKind::JetStream(_) => err(
                    ErrorCode::JobFailed,
                    "JetStream stream info request was rejected",
                ),
                _ => err(ErrorCode::JobFailed, "JetStream stream info request failed")
                    .retryable(true),
            })?;
        let config = &stream.cached_info().config;
        if config.sealed || config.mirror.is_some() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "JetStream stream is sealed or a mirror and does not accept publishes",
            ));
        }
        if !config
            .subjects
            .iter()
            .any(|filter| common::subject_matches(filter, &self.config.subject))
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                format!(
                    "subject `{}` is not bound to stream `{}`",
                    self.config.subject, self.config.stream
                ),
            ));
        }
        if self.config.msg_id_column.is_some() && config.duplicate_window.is_zero() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "msg_id_column requires a stream duplicate_window > 0",
            ));
        }
        Ok(())
    }

    fn msg_id_index(&self, schema: &Schema) -> Result<Option<usize>> {
        match &self.config.msg_id_column {
            None => Ok(None),
            Some(column) => {
                self.config.check_schema(schema)?;
                Ok(schema.index_of_name(column))
            }
        }
    }

    fn msg_id(&self, row: &Row, index: Option<usize>) -> Result<Option<String>> {
        let Some(index) = index else { return Ok(None) };
        let id = match row.values.get(index) {
            Some(Scalar::Utf8(s)) => s.to_string(),
            Some(Scalar::Int64(v)) => v.to_string(),
            Some(Scalar::UInt64(v)) => v.to_string(),
            Some(Scalar::Null) | None => {
                self.diag
                    .jetstream_sink_msg_id_missing
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            Some(_) => {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    "msg_id_column must be utf8, int64 or uint64",
                ))
            }
        };
        if id.is_empty() || id.len() > MAX_MSG_ID_BYTES || !id.bytes().all(|b| b.is_ascii_graphic())
        {
            self.diag
                .jetstream_sink_dropped_bad
                .fetch_add(1, Ordering::Relaxed);
            return Err(err(
                ErrorCode::InvalidArgument,
                "Nats-Msg-Id must be 1..=128 printable ASCII bytes",
            ));
        }
        Ok(Some(id))
    }

    fn encode(
        &self,
        schema: &Schema,
        row: &Row,
        index: Option<usize>,
        limit: usize,
    ) -> Result<(bytes::Bytes, Option<String>)> {
        let started = std::time::Instant::now();
        let body = encode_json_row(schema, row).inspect_err(|_| {
            self.diag
                .jetstream_sink_dropped_bad
                .fetch_add(1, Ordering::Relaxed);
        })?;
        self.diag
            .observation
            .record(Latency::Encode, started.elapsed());
        let id = self.msg_id(row, index)?;
        let header = id.as_ref().map_or(0, |id| id.len() + HEADER_OVERHEAD);
        if body.len() + header > limit {
            self.diag
                .jetstream_sink_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Err(err(
                ErrorCode::MaxRecordSize,
                "encoded row exceeds max_payload_bytes / server max_payload",
            ));
        }
        Ok((body.into(), id))
    }

    /// Publish every row of `batch` with bounded in-flight PubAcks. The
    /// outbox receipt is acked only if all rows were confirmed.
    async fn publish_batch(
        &self,
        session: &Session,
        batch: &RowBatch,
        outbox: Option<&Arc<InflightCounter>>,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let mut receipt = BatchReceipt(outbox.cloned());
        let mut delivered = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        let schema = batch.schema();
        let index = self.msg_id_index(schema)?;
        let limit = self
            .config
            .client
            .max_payload_bytes
            .min(session.core.server_max_payload());
        // Encoding happens inside each bounded future: only in-flight
        // messages hold a payload (charged as `retained_reservation`).
        let rows = batch.rows();
        let mut acks = stream::iter(0..rows.len())
            .map(|i| async move {
                let (body, id) = self.encode(schema, &rows[i], index, limit)?;
                self.publish_with_retry(&session.context, body, id, deadline)
                    .await
            })
            .buffer_unordered(self.config.max_inflight_acks);
        while let Some(result) = acks.next().await {
            result?;
        }
        self.diag
            .jetstream_sink_batches
            .fetch_add(1, Ordering::Relaxed);
        self.diag.observation.progress(false, batch.num_rows());
        delivered.complete();
        receipt.ack();
        Ok(())
    }

    async fn publish_with_retry(
        &self,
        context: &Context,
        body: bytes::Bytes,
        id: Option<String>,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let _inflight = InflightGuard::new(&self.diag);
        let mut delay = FIRST_RETRY_DELAY;
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                self.diag
                    .jetstream_sink_retries
                    .fetch_add(1, Ordering::Relaxed);
                self.diag.observation.health(
                    false,
                    HealthState::Reconnecting,
                    "jetstream_sink_retry_backoff",
                    None,
                );
                let pause = deadline.map_or(delay, |at| {
                    delay.min(at.saturating_duration_since(Instant::now()))
                });
                tokio::time::sleep(pause).await;
                delay = next_delay(delay);
            }
            let budget = deadline.map_or(self.config.ack_timeout * 2, |at| {
                at.saturating_duration_since(Instant::now())
            });
            if budget.is_zero() {
                break;
            }
            let mut message = PublishMessage::build().payload(body.clone());
            if let Some(id) = &id {
                message = message.message_id(id);
            }
            let attempt = async {
                context
                    .send_publish(self.config.subject.clone(), message)
                    .await?
                    .await
            };
            match tokio::time::timeout(budget, attempt).await {
                Ok(Ok(ack)) => {
                    self.diag
                        .jetstream_sink_acked
                        .fetch_add(1, Ordering::Relaxed);
                    if ack.duplicate {
                        self.diag
                            .jetstream_sink_duplicates
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                Ok(Err(e)) => match e.kind() {
                    PublishErrorKind::MaxPayloadExceeded => {
                        self.diag
                            .jetstream_sink_dropped_oversize
                            .fetch_add(1, Ordering::Relaxed);
                        self.diag
                            .jetstream_sink_failed
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(err(
                            ErrorCode::MaxRecordSize,
                            "message exceeds the server max_payload",
                        ));
                    }
                    PublishErrorKind::WrongLastMessageId | PublishErrorKind::WrongLastSequence => {
                        self.diag
                            .jetstream_sink_failed
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(err(
                            ErrorCode::JobFailed,
                            "JetStream rejected the publish (non-retriable)",
                        ));
                    }
                    PublishErrorKind::TimedOut => {
                        self.diag
                            .jetstream_sink_ack_timeouts
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                },
                Err(_) => {
                    self.diag
                        .jetstream_sink_ack_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.diag
            .jetstream_sink_failed
            .fetch_add(1, Ordering::Relaxed);
        Err(err(
            ErrorCode::JobFailed,
            format!(
                "JetStream PubAck not received after {} attempts",
                self.config.max_retries + 1
            ),
        )
        .retryable(true))
    }

    /// Stop/EOF: confirm batches already queued within `flush_timeout`.
    /// Anything left unconfirmed is a delivery failure, not a quiet drop.
    async fn shutdown(
        &self,
        session: Session,
        rx: &mut ObservedReceiver<RowBatch>,
        outbox: Option<&Arc<InflightCounter>>,
        cancel: &CancellationToken,
    ) {
        let deadline = Instant::now() + self.config.flush_timeout;
        rx.close();
        let mut failed = None;
        while let Ok(batch) = rx.try_recv() {
            if let Err(e) = self
                .publish_batch(&session, &batch, outbox, Some(deadline))
                .await
            {
                failed = Some(e);
                break;
            }
        }
        let left = self.discard(rx, outbox, false);
        let _ = session.core.close(false).await;
        match failed {
            Some(e) => self.fatal(&e, cancel),
            None if left > 0 => self.fatal(
                &err(
                    ErrorCode::JobFailed,
                    "queued rows were not confirmed within flush_timeout",
                ),
                cancel,
            ),
            None => self.diag.observation.health(
                false,
                HealthState::Stopped,
                "jetstream_sink_stopped",
                None,
            ),
        }
    }

    fn fatal(&self, e: &SparrowError, cancel: &CancellationToken) {
        self.diag
            .jetstream_sink_fatal
            .fetch_add(1, Ordering::Relaxed);
        self.diag.observation.health(
            false,
            HealthState::Failed,
            "jetstream_sink_failed",
            Some(e.code),
        );
        cancel.cancel();
    }

    /// Returns the number of discarded rows.
    fn discard(
        &self,
        rx: &mut ObservedReceiver<RowBatch>,
        outbox: Option<&Arc<InflightCounter>>,
        failed: bool,
    ) -> u64 {
        rx.close();
        let mut rows = 0u64;
        while let Ok(batch) = rx.discard_next() {
            rows += batch.num_rows() as u64;
            if let Some(outbox) = outbox {
                outbox.fail();
            }
        }
        self.diag
            .jetstream_sink_discarded_on_close
            .fetch_add(rows, Ordering::Relaxed);
        if failed && rows > 0 {
            self.diag.observation.health(
                false,
                HealthState::Failed,
                "jetstream_sink_discarded_after_failure",
                Some(ErrorCode::JobFailed),
            );
        }
        rows
    }
}

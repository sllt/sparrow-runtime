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
//! - The stream is verified at start (exists, not sealed or a mirror, enables
//!   PubAcks and binds the subject). Every publish names the expected stream.
//!   The sink never creates or edits streams.

use std::borrow::Cow;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::context::{PublishError, PublishErrorKind};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{
    Config as StreamConfig, Info as StreamInfo, RetentionPolicy, StorageType,
};
use async_nats::jetstream::{Context, ContextBuilder, ErrorCode as JsErrorCode};
use futures_util::stream::{self, StreamExt};
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_io::{CsvEncodeIdentity, OwnedSinkIdentity, SinkIdentity};
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
/// Retained per in-flight message besides its payload (subject, headers,
/// SDK request state).
const RETAINED_OVERHEAD: usize = 8 * 1024;
/// JSON fallback objects, escaped/base64 values and bounded header state.
const ENCODE_SCRATCH_OVERHEAD: usize = 8 * 1024;
const MAX_PREPARED_CONFIG_BYTES: usize = 64 * 1024;
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
    /// Stop budget shared by the active batch and already queued batches;
    /// EOF uses the same budget for the remaining queue.
    pub flush_timeout: Duration,
    /// Output column (utf8 or integer) used as `Nats-Msg-Id`.
    pub msg_id_column: Option<String>,
    /// Message payload format: one JSON object (default) or one CSV record.
    pub payload_format: sparrow_formats::PayloadFormat,
}

impl JetStreamSinkConfig {
    /// Command queue plus the SDK writer batch share the job reservation.
    /// Keep this separate from the default number of pending PubAcks.
    pub const DEFAULT_CLIENT_CAPACITY: usize = 4;

    pub fn new(
        servers: Vec<String>,
        stream: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        let mut client = NatsClientConfig::new(servers);
        client.capacity = Self::DEFAULT_CLIENT_CAPACITY;
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
            payload_format: sparrow_formats::PayloadFormat::Json,
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
        self.client
            .sdk_reservation()
            .saturating_add(self.retained_reservation())
    }

    fn retained_reservation(&self) -> usize {
        self.max_inflight_acks.saturating_mul(
            self.client
                .max_payload_bytes
                .saturating_add(RETAINED_OVERHEAD),
        )
    }

    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.sdk_reservation() > reservation_budget / 2 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "JetStream sink SDK command/writer buffers and retained PubAck payloads exceed half the job reservation budget",
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
    target: Option<PreparedTarget>,
    _retained: MemoryLease,
}

struct PreparedTarget {
    identity: Arc<OwnedSinkIdentity>,
    config: StreamConfig,
    _config_credit: MemoryLease,
}

/// The v27/v28 sink has already connected and captured its exact target before
/// the controller opens/seeks/activates the replay source. No second session
/// or independently owned budget is created by `run`.
pub struct PreparedJetStreamSink {
    sink: JetStreamSink,
    session: Session,
}

impl std::fmt::Debug for PreparedJetStreamSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedJetStreamSink")
            .field("sink", &self.sink)
            .field("identity", self.identity().identity())
            .finish_non_exhaustive()
    }
}

impl PreparedJetStreamSink {
    pub fn identity(&self) -> Arc<OwnedSinkIdentity> {
        self.session
            .target
            .as_ref()
            .expect("prepared target")
            .identity
            .clone()
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let Self { sink, session } = self;
        let _lifecycle = sink.diag.observation.lifecycle(false);
        sink.run_session(session, rx.into(), cancel, outbox).await;
    }

    /// Explicit cleanup for failed restore/admission. SDK credit and the
    /// controller's lifecycle guard remain owned until actual SDK exit.
    pub async fn close(self) -> Result<()> {
        self.session.core.close(false).await.map(|_| ())
    }
}

enum AttemptFailure {
    Target(SparrowError),
    Publish(PublishError),
}

/// Count borrowed config serialization before retaining it. Unlike a Vec,
/// this never grows an uncharged intermediate metadata allocation.
struct ConfigSize(usize);
impl std::io::Write for ConfigSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= MAX_PREPARED_CONFIG_BYTES)
            .ok_or_else(|| std::io::Error::other("prepared stream config exceeds byte bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
        let session = match self.connect(&cancel, None, false).await {
            Ok(Some(session)) => session,
            Ok(None) => {
                if self.discard(&mut rx, outbox.as_ref(), false) > 0 {
                    self.fatal(
                        &err(
                            ErrorCode::JobFailed,
                            "JetStream stopped before queued rows could be confirmed",
                        ),
                        &cancel,
                    );
                }
                return;
            }
            Err(e) => {
                self.fatal(&e, &cancel);
                self.discard(&mut rx, outbox.as_ref(), true);
                return;
            }
        };
        self.run_session(session, rx, cancel, outbox).await;
    }

    pub async fn prepare_aligned(
        self,
        cancel: CancellationToken,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<Option<PreparedJetStreamSink>> {
        match self.connect(&cancel, Some(guard), true).await {
            Ok(Some(session)) => Ok(Some(PreparedJetStreamSink {
                sink: self,
                session,
            })),
            Ok(None) => Ok(None),
            Err(error) => {
                self.fatal(&error, &cancel);
                Err(error)
            }
        }
    }

    async fn run_session(
        &self,
        session: Session,
        mut rx: ObservedReceiver<RowBatch>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut flush_deadline = None;
        loop {
            let batch = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    flush_deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
                    break;
                },
                batch = rx.recv() => batch,
            };
            let Some(batch) = batch else { break };
            let result = {
                let publishing = self.publish_batch(&session, &batch, outbox.as_ref(), None);
                tokio::pin!(publishing);
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        // Keep the current receipt/futures alive for a bounded
                        // flush, then fail them on timeout. The queue uses this
                        // same deadline, not a fresh budget after this batch.
                        let deadline = *flush_deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
                        rx.close();
                        tokio::time::timeout_at(deadline, &mut publishing)
                            .await
                            .unwrap_or_else(|_| {
                                self.diag.jetstream_sink_failed.fetch_add(1, Ordering::Relaxed);
                                Err(err(ErrorCode::JobFailed, "JetStream in-flight batch was not confirmed within flush_timeout"))
                            })
                    },
                    result = &mut publishing => result,
                }
            };
            // The batch future (and its receipt) is dropped before closing
            // the SDK; unresolved batches must never be reported as acked.
            if let Err(e) = result {
                self.fatal(&e, &cancel);
                self.discard(&mut rx, outbox.as_ref(), true);
                let _ = session.core.close(false).await;
                return;
            }
        }
        let deadline = flush_deadline.unwrap_or_else(|| Instant::now() + self.config.flush_timeout);
        self.shutdown(session, &mut rx, outbox.as_ref(), &cancel, deadline)
            .await;
    }

    /// Connect and verify the stream, retrying transient failures within
    /// the same bounded budget as publishes. `Ok(None)` = cancelled.
    async fn connect(
        &self,
        cancel: &CancellationToken,
        guard: Option<Arc<dyn Send + Sync>>,
        aligned: bool,
    ) -> Result<Option<Session>> {
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
                c = CoreClient::open_with_guard(&self.config.client, self.token.as_ref(), &self.owner, &self.diag, Role::JetStreamSink, guard.clone()) => c,
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
                Ok(info) => {
                    let target = if aligned {
                        match self.prepare_target(info) {
                            Ok(target) => Some(target),
                            Err(error) => {
                                let _ = core.close(false).await;
                                return Err(error);
                            }
                        }
                    } else {
                        None
                    };
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
                    if cancel.is_cancelled() {
                        let _ = core.close(false).await;
                        return Ok(None);
                    }
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
                        target,
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
    async fn verify_stream(&self, context: &Context) -> Result<StreamInfo> {
        // No-info constructs a local handle; get_info returns an owned fresh
        // response. Do not overwrite or clone the prepared baseline cache.
        let stream = context
            .get_stream_no_info(&self.config.stream)
            .await
            .map_err(|_| err(ErrorCode::InvalidArgument, "invalid JetStream stream name"))?;
        let info = stream.get_info().await.map_err(|e| {
            match std::error::Error::source(&e)
                .and_then(|cause| cause.downcast_ref::<async_nats::jetstream::Error>())
            {
                Some(js) if js.error_code() == JsErrorCode::STREAM_NOT_FOUND => err(
                    ErrorCode::InvalidArgument,
                    format!(
                        "JetStream stream `{}` does not exist; this sink never creates streams",
                        self.config.stream
                    ),
                ),
                Some(_) => err(
                    ErrorCode::JobFailed,
                    "JetStream stream info request was rejected",
                ),
                None => err(ErrorCode::JobFailed, "JetStream stream info request failed")
                    .retryable(true),
            }
        })?;
        let config = &info.config;
        if config.name != self.config.stream {
            return Err(err(
                ErrorCode::InvalidArgument,
                "JetStream stream info returned a different name",
            ));
        }
        if config.sealed || config.mirror.is_some() || config.no_ack {
            return Err(err(
                ErrorCode::InvalidArgument,
                "JetStream stream is sealed, a mirror or has no_ack; confirmed publishes require PubAcks",
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
        Ok(info)
    }

    fn check_aligned_storage(config: &StreamConfig) -> Result<()> {
        if config.storage != StorageType::File
            || config.retention != RetentionPolicy::Limits
            || config.no_ack
            || config.sealed
            || config.mirror.is_some()
        {
            return Err(err(ErrorCode::UnsupportedRestore, "aligned JetStream output requires an existing writable File/Limits stream with PubAcks"));
        }
        Ok(())
    }

    fn prepare_target(&self, info: StreamInfo) -> Result<PreparedTarget> {
        Self::check_aligned_storage(&info.config)?;
        let mut size = ConfigSize(0);
        serde_json::to_writer(&mut size, &info.config).map_err(|_| {
            err(
                ErrorCode::BoundExceeded,
                "prepared stream configuration exceeds its bounded metadata contract",
            )
        })?;
        // Config strings/vectors/maps are now retained as one owned baseline.
        // Include metadata overhead and the bounded temporary identity copy.
        let config_credit = self.owner.acquire(
            CreditKind::Reservation,
            size.0
                .saturating_mul(16)
                .saturating_add(sparrow_io::sink_identity::MAX_SINK_IDENTITY_BYTES),
        )?;
        let identity = SinkIdentity::jetstream(
            &self.config.client.servers,
            self.config.client.token_secret.as_deref(),
            &self.config.stream,
            info.created.unix_timestamp_nanos(),
            &self.config.subject,
            self.config.msg_id_column.as_deref(),
        )?;
        // The compiled format has already validated single-byte delimiter /
        // quote and encode-only options. Bind effective values, not whether a
        // default happened to be written explicitly in the stored spec.
        if self.config.payload_format.as_protobuf().is_some() {
            // The durable sink identity records CSV encode options only, so a
            // protobuf restore could not be bound to its descriptor/mapping.
            return Err(err(
                ErrorCode::UnsupportedRestore,
                "JetStream aligned output does not support protobuf payloads (no protobuf sink identity)",
            ));
        }
        let identity = if let Some(csv) = self.config.payload_format.as_csv() {
            let options = csv.options();
            identity.with_csv(CsvEncodeIdentity::new(
                options.delimiter.as_bytes()[0],
                options.quote.as_bytes()[0],
                csv.header(),
                &options.null_value,
            )?)?
        } else {
            identity
        };
        let identity = OwnedSinkIdentity::new(identity, &self.owner)?;
        Ok(PreparedTarget {
            identity,
            config: info.config,
            _config_credit: config_credit,
        })
    }

    async fn verify_prepared(&self, context: &Context, target: &PreparedTarget) -> Result<()> {
        let info = self.verify_stream(context).await?;
        Self::check_aligned_storage(&info.config)?;
        if info.created.unix_timestamp_nanos() != target.identity.identity().created_nanos
            || info.config != target.config
        {
            return Err(err(
                ErrorCode::UnsupportedRestore,
                "prepared JetStream stream incarnation or configuration changed",
            ));
        }
        Ok(())
    }

    async fn verify_prepared_at(
        &self,
        context: &Context,
        target: &PreparedTarget,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let budget = deadline.map_or(self.config.ack_timeout, |at| {
            at.saturating_duration_since(Instant::now())
                .min(self.config.ack_timeout)
        });
        let result = if budget.is_zero() {
            Err(err(
                ErrorCode::JobFailed,
                "prepared JetStream target verification exceeded the flush deadline",
            ))
        } else {
            tokio::time::timeout(budget, self.verify_prepared(context, target))
                .await
                .unwrap_or_else(|_| {
                    Err(err(
                        ErrorCode::JobFailed,
                        "prepared JetStream target verification timed out",
                    )
                    .retryable(true))
                })
        };
        result.inspect_err(|_| {
            self.diag
                .jetstream_sink_failed
                .fetch_add(1, Ordering::Relaxed);
        })
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
            Some(Scalar::Utf8(s)) => Cow::Borrowed(s.as_ref()),
            Some(Scalar::Int64(v)) => Cow::Owned(v.to_string()),
            Some(Scalar::UInt64(v)) => Cow::Owned(v.to_string()),
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
        Ok(Some(id.into_owned()))
    }

    /// Use exactly the same bounded header block for size admission and send.
    fn publish_headers(&self, id: Option<&str>) -> async_nats::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            async_nats::header::NATS_EXPECTED_STREAM,
            self.config.stream.as_str(),
        );
        if let Some(id) = id {
            headers.insert(async_nats::header::NATS_MESSAGE_ID, id);
        }
        headers
    }

    pub(super) fn encode(
        &self,
        schema: &Schema,
        row: &Row,
        index: Option<usize>,
        limit: usize,
    ) -> Result<(bytes::Bytes, Option<String>)> {
        let started = std::time::Instant::now();
        // The bounded writer limits the final Vec, but wide/Bytes/Dynamic
        // rows can also build temporary serde Values. Admit conservative
        // scratch before cloning even the id/header or entering the codec.
        let names = schema.fields.iter().fold(0usize, |bytes, field| {
            bytes.saturating_add(
                field
                    .name
                    .capacity()
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(32),
            )
        });
        let format = &self.config.payload_format;
        let scratch_bytes = match format {
            // CSV keeps no serde tree for flat cells, protobuf borrows the
            // row's strings (see their estimators).
            sparrow_formats::PayloadFormat::Csv(_) | sparrow_formats::PayloadFormat::Protobuf(_) => {
                format.encode_scratch(schema, row)
            }
            sparrow_formats::PayloadFormat::Json => row
                .resident_bytes()
                .saturating_mul(8)
                .saturating_add(names.saturating_mul(4))
                .saturating_add(ENCODE_SCRATCH_OVERHEAD),
        };
        let _scratch = self.owner.acquire(CreditKind::Reservation, scratch_bytes)?;
        let id = self.msg_id(row, index)?;
        let headers = self.publish_headers(id.as_deref());
        // Mirrors async-nats 0.50 HeaderMap serialization. Its wire_len helper
        // is private, so count the actual public entries without allocating.
        let header_bytes =
            headers
                .iter()
                .fold(b"NATS/1.0\r\n\r\n".len(), |total, (name, values)| {
                    let name: &str = name.as_ref();
                    values.iter().fold(total, |total, value| {
                        total
                            .saturating_add(name.len())
                            .saturating_add(b": ".len())
                            .saturating_add(value.as_str().len())
                            .saturating_add(b"\r\n".len())
                    })
                });
        let body_limit = limit.saturating_sub(header_bytes);
        // The writer checks the bound before buffer growth, including JSON
        // escaping / CSV quoting. JSON's one-row array envelope is removed in
        // place; its two spare bytes remain covered by RETAINED_OVERHEAD.
        let encoded = format.encode_row_bounded_with_capacity(schema, row, body_limit, |_| Ok(()));
        self.diag
            .observation
            .record(Latency::Encode, started.elapsed());
        let body = encoded.map_err(|e| {
            if e.code == ErrorCode::BoundExceeded {
                self.diag
                    .jetstream_sink_dropped_oversize
                    .fetch_add(1, Ordering::Relaxed);
                err(
                    ErrorCode::MaxRecordSize,
                    "encoded row and headers exceed max_payload_bytes / server max_payload",
                )
            } else {
                self.diag.format_encode_error(format);
                self.diag
                    .jetstream_sink_dropped_bad
                    .fetch_add(1, Ordering::Relaxed);
                e
            }
        })?;
        Ok((bytes::Bytes::from(body), id))
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
        if let Some(target) = &session.target {
            if !target.identity.belongs_to(&self.owner)
                || !Arc::ptr_eq(batch.lease().owner(), &self.owner)
            {
                return Err(err(
                    ErrorCode::PolicyDenied,
                    "prepared sink and output batch must share the admitted Job memory owner",
                ));
            }
            self.verify_prepared_at(&session.context, target, deadline)
                .await?;
        }
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
                self.publish_with_retry(
                    &session.context,
                    body,
                    id,
                    deadline,
                    session.target.as_ref(),
                )
                .await
            })
            .buffer_unordered(self.config.max_inflight_acks);
        while let Some(result) = acks.next().await {
            result?;
        }
        if let Some(target) = &session.target {
            // A stream recreated or reconfigured while PubAcks were pending
            // must not authorize this batch's aligned outbox receipt.
            self.verify_prepared_at(&session.context, target, deadline)
                .await?;
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
        target: Option<&PreparedTarget>,
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
            let attempt_budget = self.config.ack_timeout.saturating_mul(2);
            let budget = deadline.map_or(attempt_budget, |at| {
                at.saturating_duration_since(Instant::now())
                    .min(attempt_budget)
            });
            if budget.is_zero() {
                break;
            }
            let message = PublishMessage::build()
                .payload(body.clone())
                .headers(self.publish_headers(id.as_deref()));
            let attempt = async {
                if attempt > 0 {
                    if let Some(target) = target {
                        // First attempts are covered by the batch's before /
                        // after INFO checks. Revalidate before every retry: a
                        // reconnect may reach a different stream incarnation.
                        self.verify_prepared_at(context, target, deadline)
                            .await
                            .map_err(AttemptFailure::Target)?;
                    }
                }
                let ack = context
                    .send_publish(self.config.subject.clone(), message)
                    .await
                    .map_err(AttemptFailure::Publish)?
                    .await
                    .map_err(AttemptFailure::Publish)?;
                Ok::<_, AttemptFailure>(ack)
            };
            match tokio::time::timeout(budget, attempt).await {
                Ok(Ok(ack)) => {
                    if ack.stream != self.config.stream {
                        self.diag
                            .jetstream_sink_failed
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(err(
                            ErrorCode::JobFailed,
                            "JetStream PubAck came from a different stream",
                        ));
                    }
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
                Ok(Err(AttemptFailure::Target(error))) => return Err(error),
                Ok(Err(AttemptFailure::Publish(e))) => match e.kind() {
                    // async-nats 0.50 maps this server rejection to Other.
                    // It is a target mismatch, never a transient retry.
                    PublishErrorKind::Other
                        if std::error::Error::source(&e)
                            .and_then(|cause| cause.downcast_ref::<async_nats::jetstream::Error>())
                            .is_some_and(|js| js.error_code() == JsErrorCode::STREAM_NOT_MATCH) =>
                    {
                        self.diag
                            .jetstream_sink_failed
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(err(
                            ErrorCode::JobFailed,
                            "JetStream rejected the publish: expected stream does not match",
                        ));
                    }
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

    /// Stop/EOF: confirm queued batches within the original flush deadline.
    /// Anything left unconfirmed is a delivery failure, not a quiet drop.
    async fn shutdown(
        &self,
        session: Session,
        rx: &mut ObservedReceiver<RowBatch>,
        outbox: Option<&Arc<InflightCounter>>,
        cancel: &CancellationToken,
        deadline: Instant,
    ) {
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

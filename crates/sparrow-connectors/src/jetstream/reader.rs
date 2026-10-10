use super::ack::{AckDriver, ACK_CONCURRENCY};
use super::connection::{
    error, MAX_MESSAGE_BYTES, MAX_SUBJECT_BYTES, REQUEST_TIMEOUT, SDK_EXPANSION,
};
use super::{
    check_stream, Connection, DeliveryIdentity, DeliveryLedger, Observation, StreamIdentity,
};
use async_nats::jetstream::{
    self,
    consumer::{pull, AckPolicy, DeliverPolicy, PullConsumer},
};
use futures_util::{FutureExt, StreamExt};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result, RowBatch, RowBatchBuilder, Schema,
};
use std::{sync::Arc, time::Duration};

#[path = "feed.rs"]
mod feed;

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub namespace: String,
    pub stream: String,
    pub consumer: String,
    pub ownership_bucket: String,
    pub max_pending: usize,
    pub pending_bytes: usize,
    pub pull_messages: usize,
    pub pull_bytes: usize,
    /// Message payload format: one JSON object (default) or one CSV record.
    pub payload_format: sparrow_formats::PayloadFormat,
}
fn check_ownership_policy(kv: &jetstream::stream::Config) -> Result<()> {
    if kv.storage != jetstream::stream::StorageType::File
        || kv.num_replicas != 1
        || !kv.max_age.is_zero()
        || kv.max_bytes <= 0
        || kv.max_messages > 0
        || kv.max_messages_per_subject != 1
        || kv.discard != jetstream::stream::DiscardPolicy::New
        || kv.allow_message_ttl
        || kv.subject_delete_marker_ttl.is_some()
        || kv.mirror.is_some()
        || kv.sources.is_some()
    {
        return Err(error(ErrorCode::UnsupportedRestore,"JetStream ownership bucket must use bounded non-expiring File history=1 discard-new storage"));
    }
    Ok(())
}
impl ReaderConfig {
    pub fn validate(&self) -> Result<()> {
        for name in [
            &self.namespace,
            &self.stream,
            &self.consumer,
            &self.ownership_bucket,
        ] {
            super::policy::check_name(name)?;
        }
        if self.consumer.len() > 80
            || !(1..=4096).contains(&self.max_pending)
            || !(1..=128).contains(&self.pull_messages)
            || self.pull_messages > self.max_pending
            || !(MAX_MESSAGE_BYTES + 8192..=1024 * 1024).contains(&self.pull_bytes)
            || !(self.pull_bytes..=16 * 1024 * 1024).contains(&self.pending_bytes)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "JetStream reader/pull bounds invalid",
            ));
        }
        Ok(())
    }
}

/// Holds message storage and its OWN lease, even if the reader stops first.
/// No unaccounted payload or Acker escapes into the control/runtime layer.
pub struct InputRecord {
    message: async_nats::Message,
    _lease: MemoryLease,
    sequence: u64,
    format: sparrow_formats::PayloadFormat,
}
impl InputRecord {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }
    pub fn subject(&self) -> &str {
        self.message.subject.as_str()
    }
    pub fn payload_format(&self) -> &sparrow_formats::PayloadFormat {
        &self.format
    }
    pub fn decode(
        &self,
        schema: &Arc<Schema>,
        owner: &Arc<MemoryOwner>,
        row_limit: usize,
    ) -> Result<RowBatch> {
        let mut builder = RowBatchBuilder::new(
            schema.clone(),
            owner.clone(),
            CreditKind::Reservation,
            1,
            row_limit,
        )?;
        self.decode_into(schema, owner, &mut builder, row_limit)?;
        builder.finish()
    }
    /// Returns false only when an existing batch must be published first.
    /// The caller retains this InputRecord and retries it, never skips it.
    pub fn decode_into(
        &self,
        schema: &Arc<Schema>,
        owner: &Arc<MemoryOwner>,
        builder: &mut RowBatchBuilder,
        byte_limit: usize,
    ) -> Result<bool> {
        // Strict decode scratch is acquired before the JSON tree/row is built.
        // Adversarial tiny scalars can expand far beyond serialized bytes.
        let estimate = self
            .payload()
            .len()
            .checked_mul(64)
            // Sparse objects still materialize one Scalar for every nullable
            // schema field. Wire length alone does not bound that allocation.
            .and_then(|n| {
                schema
                    .fields
                    .len()
                    .checked_mul(std::mem::size_of::<sparrow_model::Scalar>() * 2)
                    .and_then(|fields| n.checked_add(fields))
            })
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| {
                error(
                    ErrorCode::BoundExceeded,
                    "JetStream decode scratch overflow",
                )
            })?;
        // CSV and protobuf have their own working-set estimators.
        let estimate = if self.format.is_json() {
            estimate
        } else {
            self.format.decode_scratch(schema, self.payload().len())
        };
        let _scratch = owner.acquire(CreditKind::Reservation, estimate)?;
        let row = self.format.decode_row(
            schema,
            self.payload(),
            &sparrow_formats::JsonLimits::default(),
            None,
        )?;
        let resident = row.resident_bytes();
        if builder.num_rows() > 0
            && builder
                .current_bytes()
                .saturating_add(resident.max(row.tracked_bytes()))
                > byte_limit
        {
            return Ok(false);
        }
        builder.push_accounted(row, resident)?;
        Ok(true)
    }
}

pub enum ReaderPoll {
    Record(InputRecord),
    Duplicate,
    Empty,
    /// A nonempty fetch ended. Immediately pull again, do NOT idle-backoff.
    BatchEnd,
    Full,
}

/// Retain only the bounded reply subject, not a Context clone per input.
#[derive(Clone)]
pub(super) struct AckToken(pub(super) async_nats::Subject);
impl AckToken {
    pub(super) async fn confirm(&self, client: &async_nats::Client) -> Result<()> {
        let response = client
            .request(self.0.clone(), "+ACK".into())
            .await
            .map_err(|_| {
                error(ErrorCode::JobFailed, "JetStream ACK request failed").retryable(true)
            })?;
        // SDK Acker::double_ack accepts Some(_) including a negative status.
        // Do not call a 503/no-responders or error payload a confirmed ACK.
        if response
            .status
            .is_some_and(|status| status != async_nats::StatusCode::OK)
            || !response.payload.is_empty()
        {
            return Err(error(
                ErrorCode::JobFailed,
                "JetStream ACK confirmation was not successful",
            )
            .retryable(true));
        }
        Ok(())
    }
}

pub struct Reader {
    connection: Connection,
    stream: jetstream::stream::Stream,
    consumer: PullConsumer,
    context: jetstream::Context,
    ownership: jetstream::kv::Store,
    ownership_key: String,
    ownership_revision: u64,
    identity: StreamIdentity,
    /// Payload format bound into every cut (0 = JSON).
    format_fingerprint: u64,
    reader_name: String,
    config: ReaderConfig,
    owner: Arc<MemoryOwner>,
    ledger: DeliveryLedger<AckToken>,
    replay_end: Option<u64>,
    pull: Option<pull::Batch>,
    pull_started: Option<tokio::time::Instant>,
    pull_received: bool,
    acknowledgements: AckDriver,
    redeliveries: u64,
    pulls: u64,
}

impl Reader {
    /// Caller MUST hold the local CheckpointStore exclusive lock from before
    /// this call through close(). The durable binding rejects another store
    /// owner; copied owner files on another node are NOT distributed fencing.
    /// A fresh reader is always created from the durable source cut, never from
    /// a previous consumer's ACK floor (which may include post-crash replies).
    pub async fn open(
        connection: Connection,
        config: ReaderConfig,
        owner: Arc<MemoryOwner>,
        binding_owner: [u8; 32],
        attempt_nonce: [u8; 16],
        restore: Option<&sparrow_io::SourcePosition>,
    ) -> Result<Self> {
        let format_fingerprint = super::policy::format_fingerprint(&config.payload_format);
        let setup=async {
            config.validate()?;
            if config.pull_bytes>connection.pull_bytes || config.pull_messages+2>connection.subscription_capacity {
                return Err(error(ErrorCode::BoundExceeded,"JetStream reader exceeds connection's pre-admitted pull/subscription budget"));
            }
            if binding_owner==[0;32] || attempt_nonce==[0;16] {return Err(error(ErrorCode::InvalidArgument,"JetStream ownership identity is uninitialized"));}
            let context=jetstream::context::ContextBuilder::new().timeout(REQUEST_TIMEOUT).ack_timeout(REQUEST_TIMEOUT)
                .max_ack_inflight(4).concurrency_limit(Some(1)).backpressure_on_inflight(false).build(connection.client.clone());
            let mut stream=context.get_stream(&config.stream).await.map_err(|_|error(ErrorCode::JobFailed,"JetStream input stream unavailable").retryable(true))?;
            let info=stream.info().await.map_err(|_|error(ErrorCode::JobFailed,"JetStream stream info unavailable").retryable(true))?;
            let identity=StreamIdentity::from_info(&config.namespace,info)?.with_reader(&config.ownership_bucket,&config.consumer);
            let cut=if let Some(position)=restore {identity.check_position_bound(position,format_fingerprint)?;position.offset_bytes}else{0};
            let next=cut.checked_add(1).ok_or_else(||error(ErrorCode::BoundExceeded,"JetStream source sequence exhausted"))?;
            check_stream(info,next)?;
            let mut ownership=context.get_key_value(&config.ownership_bucket).await.map_err(|_|error(ErrorCode::PolicyDenied,"JetStream ownership KV bucket must be provisioned"))?;
            let kv_info=ownership.stream.info().await.map_err(|_|error(ErrorCode::JobFailed,"JetStream ownership policy unavailable").retryable(true))?;
            let kv=&kv_info.config;
            // Binding eviction/TTL would permit a second active local owner.
            // Bounded storage must refuse new writes, not evict other keys.
            check_ownership_policy(kv)?;
            let key=format!("{}.{}",config.stream,config.consumer);
            let entry=ownership.entry(key.clone()).await.map_err(|_|error(ErrorCode::JobFailed,"JetStream ownership read failed").retryable(true))?;
            let mut binding=Vec::with_capacity(52);binding.extend_from_slice(b"BND1");binding.extend_from_slice(&binding_owner);binding.extend_from_slice(&attempt_nonce);
            let revision=if let Some(entry)=entry {
                if entry.operation!=jetstream::kv::Operation::Put || entry.value.len()!=52
                    || &entry.value[..4]!=b"BND1" || entry.value[4..36]!=binding_owner {
                    return Err(error(ErrorCode::PolicyDenied,"JetStream logical consumer is bound to another checkpoint owner or requires explicit operator repair"));
                }
                let old:[u8;16]=entry.value[36..].try_into().unwrap();
                if old==attempt_nonce {return Err(error(ErrorCode::PolicyDenied,"JetStream reader attempt nonce reused"));}
                // Keep the old reader name durably discoverable UNTIL its
                // deletion succeeds. Updating the binding first would leak an
                // untracked consumer on crash/timeout between CAS and delete.
                let old_name=reader_name(&config.consumer,old);
                match stream.delete_consumer(&old_name).await {
                    Ok(_)=>{},
                    Err(e) if matches!(e.kind(),jetstream::stream::ConsumerErrorKind::JetStream(ref e) if e.error_code()==jetstream::ErrorCode::CONSUMER_NOT_FOUND)=>{},
                    Err(_)=>return Err(error(ErrorCode::JobFailed,"JetStream previous reader cleanup failed").retryable(true)),
                }
                ownership.update(&key,binding.into(),entry.revision).await.map_err(|_|error(ErrorCode::PolicyDenied,"JetStream ownership CAS failed; another owner changed binding"))?
            } else {
                ownership.create(&key,binding.into()).await.map_err(|_|error(ErrorCode::PolicyDenied,"JetStream ownership create failed; bucket full or consumer already bound"))?
            };
            let name=reader_name(&config.consumer,attempt_nonce);
            let consumer=stream.create_consumer_strict(pull::Config {
                name:Some(name.clone()),durable_name:Some(name.clone()),
                deliver_policy:DeliverPolicy::ByStartSequence{start_sequence:next},ack_policy:AckPolicy::Explicit,
                ack_wait:Duration::from_secs(30),max_deliver:-1,max_ack_pending:config.max_pending as i64,
                inactive_threshold:Duration::from_secs(24*60*60),
                max_waiting:1,max_batch:config.pull_messages as i64,max_bytes:config.pull_bytes as i64,
                // fetch() uses no_wait without a client-side timer. Broker
                // completes the pull; our health/watchdog fails, never skips.
                num_replicas:1,memory_storage:false,..Default::default()
            }).await.map_err(|_|error(ErrorCode::JobFailed,"JetStream attempt reader creation failed").retryable(true))?;
            let ledger=DeliveryLedger::new(owner.clone(),config.max_pending,config.pending_bytes,cut)?;
            let acknowledgements=AckDriver::start(connection.client.clone(),&owner)?;
            Ok((context,stream,consumer,ownership,key,revision,identity,name,ledger,acknowledgements))
        }.await;
        match setup {
            Ok((
                context,
                stream,
                consumer,
                ownership,
                ownership_key,
                ownership_revision,
                identity,
                reader_name,
                ledger,
                acknowledgements,
            )) => Ok(Self {
                replay_end: None,
                connection,
                stream,
                consumer,
                context,
                ownership,
                ownership_key,
                ownership_revision,
                identity,
                format_fingerprint,
                reader_name,
                config,
                owner,
                ledger,
                pull: None,
                pull_started: None,
                pull_received: false,
                acknowledgements,
                redeliveries: 0,
                pulls: 0,
            }),
            Err(e) => {
                let _ = connection.close().await;
                Err(e)
            }
        }
    }

    pub fn position(&self, records: u64) -> sparrow_io::SourcePosition {
        self.identity
            .position_bound(self.ledger.published(), records, self.format_fingerprint)
    }
    pub fn set_replay_end(&mut self,end:Option<u64>)->Result<()> {
        if end.is_some_and(|n|n<self.ledger.committed()) {return Err(error(ErrorCode::UnsupportedRestore,"replay end precedes committed cut"));}
        self.replay_end=end;Ok(())
    }
    pub fn pending(&self) -> usize {
        self.ledger.pending()
    }
    pub fn pending_bytes(&self) -> usize {
        self.ledger.pending_bytes()
    }
    pub fn committed(&self) -> u64 {
        self.ledger.committed()
    }
    pub fn redeliveries(&self) -> u64 {
        self.redeliveries
    }
    pub fn pulls(&self) -> u64 {
        self.pulls
    }
    pub fn ack_retries(&self) -> u64 {
        self.acknowledgements
            .retries
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn ack_signal(&self) -> Arc<tokio::sync::Notify> {
        self.acknowledgements.wake.clone()
    }
    pub fn retention_available(&self) -> usize {
        self.owner
            .budget()
            .retention_bytes
            .saturating_sub(self.owner.usage().retention_bytes)
    }
    pub fn service_acknowledgements(&mut self) -> Result<()> {
        while let Some((sequence, result)) = self.acknowledgements.completed()? {
            result?;
            self.ledger.ack_confirmed(sequence)?;
        }
        while self.acknowledgements.active < ACK_CONCURRENCY {
            let Some((sequence, token)) = self.ledger.next_confirmation() else {
                break;
            };
            self.acknowledgements.enqueue(sequence, token.clone())?;
        }
        Ok(())
    }
    pub fn mark_committed(&mut self, cut: u64) -> Result<()> {
        self.ledger.committed_checkpoint(cut)?;
        self.service_acknowledgements()
    }
    pub fn try_next(&mut self) -> Option<Result<ReaderPoll>> {
        self.next().now_or_never()
    }
    pub fn published(&mut self, sequence: u64) -> Result<()> {
        self.ledger.publish(sequence)
    }

    pub async fn verify(&mut self) -> Result<()> {
        self.connection.check_health()?;
        let kv = self.ownership.stream.info().await.map_err(|_| {
            error(
                ErrorCode::JobFailed,
                "JetStream ownership policy verification failed",
            )
            .retryable(true)
        })?;
        check_ownership_policy(&kv.config)?;
        let info = self.stream.info().await.map_err(|_| {
            error(
                ErrorCode::JobFailed,
                "JetStream input policy/range verification failed",
            )
            .retryable(true)
        })?;
        if StreamIdentity::from_info(&self.config.namespace, info)?
            .with_reader(&self.config.ownership_bucket, &self.config.consumer)
            != self.identity
        {
            return Err(error(
                ErrorCode::UnsupportedRestore,
                "JetStream stream recreated during attempt",
            ));
        }
        check_stream(
            info,
            self.ledger.committed().checked_add(1).ok_or_else(|| {
                error(
                    ErrorCode::BoundExceeded,
                    "JetStream source sequence exhausted",
                )
            })?,
        )?;
        let binding = self
            .ownership
            .entry(self.ownership_key.clone())
            .await
            .map_err(|_| {
                error(
                    ErrorCode::JobFailed,
                    "JetStream ownership verification failed",
                )
                .retryable(true)
            })?
            .ok_or_else(|| {
                error(
                    ErrorCode::PolicyDenied,
                    "JetStream ownership binding disappeared",
                )
            })?;
        if binding.revision != self.ownership_revision
            || binding.operation != jetstream::kv::Operation::Put
        {
            return Err(error(
                ErrorCode::PolicyDenied,
                "JetStream reader no longer owns logical consumer",
            ));
        }
        Ok(())
    }

    /// Creation is awaited to completion by the source actor. Never drop this
    /// future in a control select after the SDK subscribes/sends the request.
    pub async fn prepare_pull(&mut self) -> Result<bool> {
        if self.pull.is_some() {
            return Ok(true);
        }
        let mut count = self
            .ledger
            .remaining_messages()
            .min(self.config.pull_messages);
        if let Some(end)=self.replay_end {
            let remaining=end.saturating_sub(self.ledger.published());
            if remaining==0 {return Ok(false);}
            count=count.min(remaining.min(usize::MAX as u64) as usize);
        }
        let retention = self
            .owner
            .budget()
            .retention_bytes
            .saturating_sub(self.owner.usage().retention_bytes);
        let bytes = self
            .ledger
            .remaining_bytes()
            .min(self.config.pull_bytes)
            .min(retention.saturating_sub(count * 1024));
        if count == 0 || bytes < MAX_MESSAGE_BYTES + 8192 {
            if self.ledger.pending() == 0 {
                return Err(error(ErrorCode::ResourceExhausted,"JetStream state leaves no bounded retention space for a pull; cannot make progress with an empty checkpoint"));
            }
            return Ok(false);
        }
        // Stream/KV verification runs at open, before every checkpoint, and on
        // the actor's 5s health tick. Do not add two control-plane RTTs to each
        // tiny pull; delivery/source continuity is checked on EVERY message.
        self.connection.check_health()?;
        self.pull = Some(
            self.consumer
                .fetch()
                .max_messages(count)
                .max_bytes(bytes)
                .messages()
                .await
                .map_err(|_| {
                    error(ErrorCode::JobFailed, "JetStream bounded fetch failed").retryable(true)
                })?,
        );
        self.pull_started = Some(tokio::time::Instant::now());
        self.pull_received = false;
        self.pulls = self.pulls.saturating_add(1);
        Ok(true)
    }

    /// Cancel-safe waiting: Batch is stored on the reader, never local to the
    /// select future. No expiry is passed to SDK Batch, avoiding its timer-first
    /// discard of buffered frames after a blocked decoder/mailbox.
    pub async fn next(&mut self) -> Result<ReaderPoll> {
        self.connection.check_health()?;
        let Some(pull) = self.pull.as_mut() else {
            return Ok(ReaderPoll::Full);
        };
        // Tokio timeout polls the inner future first: already-buffered frames
        // survive a long mailbox stall. The fixed request deadline also cannot
        // be reset forever by cancelling this wait for periodic controls.
        let deadline = self.pull_started.expect("active pull start") + Duration::from_secs(5);
        let message = tokio::time::timeout_at(deadline, pull.next())
            .await
            .map_err(|_| {
                error(
                    ErrorCode::JobFailed,
                    "JetStream fetch reply deadline exceeded; refusing to skip uncertain delivery",
                )
                .retryable(true)
            })?;
        let Some(message) = message else {
            self.pull = None;
            self.pull_started = None;
            return Ok(if self.pull_received {
                ReaderPoll::BatchEnd
            } else {
                ReaderPoll::Empty
            });
        };
        let message = message.map_err(|_| {
            error(ErrorCode::JobFailed, "JetStream fetch protocol failed").retryable(true)
        })?;
        self.pull_received = true;
        // Deadline measures lack of actual frames, not time spent processing
        // earlier buffered records; cancelling a wait never extends it.
        self.pull_started = Some(tokio::time::Instant::now());
        let info = message
            .info()
            .map_err(|_| error(ErrorCode::CodecViolation, "JetStream ACK metadata invalid"))?;
        if info.stream != self.identity.stream || info.consumer != self.reader_name {
            return Err(error(
                ErrorCode::CodecViolation,
                "JetStream frame belongs to a different stream/attempt",
            ));
        }
        let sequence = info.stream_sequence;
        if self.replay_end.is_some_and(|end|sequence>end) {return Err(error(ErrorCode::UnsupportedRestore,"broker delivered outside approved replay range"));}
        if message.payload.len() > MAX_MESSAGE_BYTES
            || message.length > MAX_MESSAGE_BYTES + 8192
            || message.subject.len() > MAX_SUBJECT_BYTES
        {
            return Err(error(
                ErrorCode::MaxRecordSize,
                "JetStream frame exceeds verified payload/metadata bound",
            ));
        }
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        digest.update(&(message.subject.len() as u64).to_le_bytes());
        digest.update(message.subject.as_bytes());
        digest.update(&message.payload);
        // SDK length includes the changing ACK reply and subject. Original
        // headers+payload bytes remain stable across redelivery.
        let wire_bytes = message
            .length
            .checked_sub(message.subject.len())
            .and_then(|n| n.checked_sub(message.reply.as_ref().map_or(0, |r| r.len())))
            .filter(|n| *n <= MAX_MESSAGE_BYTES)
            .ok_or_else(|| {
                error(
                    ErrorCode::CodecViolation,
                    "JetStream wire length metadata invalid",
                )
            })?
            .max(1);
        let retained = message
            .length
            .checked_mul(SDK_EXPANSION)
            .and_then(|n| n.checked_add(256 * 1024))
            .ok_or_else(|| {
                error(
                    ErrorCode::BoundExceeded,
                    "JetStream retained frame estimate overflow",
                )
            })?;
        let id = DeliveryIdentity {
            stream_sequence: sequence,
            consumer_sequence: info.consumer_sequence,
            payload_digest: digest.finish().as_ref().try_into().unwrap(),
            wire_bytes,
        };
        let lease = self
            .owner
            // Tiny Bytes views may pin a complete SDK read allocation; decoded
            // headers also survive reader close while this record is retained.
            .acquire(CreditKind::Reservation, retained)?;
        let reply = message
            .reply
            .clone()
            .ok_or_else(|| error(ErrorCode::CodecViolation, "JetStream ACK reply missing"))?;
        if reply.len() > 1024 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "JetStream ACK reply metadata too large",
            ));
        }
        let (message, sdk_ack) = message.split();
        drop(sdk_ack);
        let (observation, retired) = self.ledger.observe(id, AckToken(reply))?;
        match observation {
            Observation::New => Ok(ReaderPoll::Record(InputRecord {
                message,
                _lease: lease,
                sequence,
                format: self.config.payload_format.clone(),
            })),
            Observation::PendingDuplicate => {
                self.redeliveries = self.redeliveries.saturating_add(1);
                drop(retired);
                Ok(ReaderPoll::Duplicate)
            }
            Observation::CommittedDuplicate => {
                self.redeliveries = self.redeliveries.saturating_add(1);
                // Either its Explicit ACK is still tracked/in flight, or it
                // was confirmed already and this is a buffered stale delivery.
                // Never create an unbounded second ACK queue for duplicates.
                drop(retired);
                Ok(ReaderPoll::Duplicate)
            }
        }
    }

    /// Called after CURRENT publication, even if the HTTP waiter disappeared.
    /// Newer prefetched/applied rows above cut never become eligible for ACK.
    pub async fn checkpoint_committed(&mut self, cut: u64) -> Result<()> {
        self.mark_committed(cut)?;
        while self.acknowledgements.active > 0 {
            self.acknowledgements.wake.notified().await;
            self.service_acknowledgements()?;
        }
        Ok(())
    }
    pub async fn progress(&self) -> Result<()> {
        self.connection.check_health()?;
        for ack in self.ledger.unresolved() {
            self.connection
                .client
                .publish(ack.0.clone(), "+WPI".into())
                .await
                .map_err(|_| {
                    error(ErrorCode::JobFailed, "JetStream ACK progress failed").retryable(true)
                })?;
        }
        Ok(())
    }
    pub async fn close(self) -> Result<()> {
        let Self {
            connection,
            stream,
            consumer,
            context,
            ownership,
            ledger,
            pull,
            acknowledgements,
            ..
        } = self;
        drop(pull);
        drop(ledger);
        drop(consumer);
        drop(stream);
        drop(ownership);
        let ack_closed = acknowledgements.close().await;
        let drained = tokio::time::timeout(REQUEST_TIMEOUT * 2, context.wait_for_acks()).await;
        drop(context);
        connection.close().await?;
        ack_closed?;
        drained.map_err(|_| {
            error(
                ErrorCode::ResourceExhausted,
                "JetStream pending publish acknowledgments did not drain",
            )
        })
    }
}

fn reader_name(base: &str, nonce: [u8; 16]) -> String {
    use std::fmt::Write;
    let mut name = String::with_capacity(base.len() + 33);
    name.push_str(base);
    name.push('_');
    for b in nonce {
        write!(name, "{b:02x}").expect("String write");
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn k2_sparse_json_schema_materialization_is_precharged_before_decode() {
        let mut budget = sparrow_model::ResourceBudget::compact();
        budget.reservation_bytes = 5000;
        let owner = MemoryOwner::new(budget);
        let lease = owner.acquire(CreditKind::Reservation, 512).unwrap();
        let record = InputRecord {
            sequence: 1,
            _lease: lease,
            message: async_nats::Message {
                subject: "test".into(),
                payload: "{}".into(),
                reply: None,
                headers: None,
                status: None,
                description: None,
                length: 2,
            },
            format: Default::default(),
        };
        let schema = Arc::new(
            Schema::new(
                1,
                (1..=1000)
                    .map(|i| {
                        sparrow_model::Field::new(
                            i,
                            format!("f{i}"),
                            sparrow_model::DataType::Int64,
                            true,
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        );
        assert_eq!(
            record.decode(&schema, &owner, 4096).unwrap_err().code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(
            owner.usage().peak_physical_bytes,
            512,
            "sparse schema must be rejected at scratch admission, not after allocating the row"
        );
        drop(record);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn k2_csv_payload_format_decodes_one_record_per_message() {
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let format = sparrow_formats::PayloadFormat::csv(
            sparrow_formats::CsvOptions::default()
                .compile(sparrow_formats::CsvRole::Decode)
                .unwrap(),
        );
        let record = |payload: &'static str| InputRecord {
            sequence: 1,
            _lease: owner.acquire(CreditKind::Reservation, 64).unwrap(),
            message: async_nats::Message {
                subject: "test".into(),
                payload: payload.into(),
                reply: None,
                headers: None,
                status: None,
                description: None,
                length: payload.len(),
            },
            format: format.clone(),
        };
        let schema = Arc::new(
            Schema::new(
                1,
                vec![
                    sparrow_model::Field::new(1, "id", sparrow_model::DataType::Utf8, false),
                    sparrow_model::Field::new(2, "v", sparrow_model::DataType::Int64, true),
                ],
            )
            .unwrap(),
        );
        let batch = record("v,id\n,\"a\"\n")
            .decode(&schema, &owner, 4096)
            .unwrap();
        assert_eq!(
            batch.rows()[0].values,
            vec![
                sparrow_model::Scalar::utf8("a"),
                sparrow_model::Scalar::Null
            ]
        );
        drop(batch);
        for bad in ["id,v\na,1\nb,2\n", "id,v\na,x\n", "{\"id\":\"a\"}"] {
            assert!(record(bad).decode(&schema, &owner, 4096).is_err(), "{bad}");
        }
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

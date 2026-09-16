//! Ordered JetStream source + independent checkpoint worker. The reader keeps
//! all post-cut pending deliveries; only successful durable worker completions
//! authorize ACKs. Both workers and the SDK are joined before dropping the lock.
use super::*;
use sparrow_connectors::jetstream::{Connection, InputRecord, Reader, ReaderPoll};
use sparrow_model::observation::OriginSpan;
use sparrow_model::{ErrorCode, MemoryOwner, OutputSequence, Schema};
use sparrow_plan::CheckpointPlan;
use std::path::Path;

fn random<const N: usize>() -> Result<[u8; N]> {
    use ring::rand::SecureRandom;
    let mut value = [0; N];
    ring::rand::SystemRandom::new()
        .fill(&mut value)
        .map_err(|_| {
            SparrowError::new(
                ErrorCode::Internal,
                "JetStream identity entropy unavailable",
            )
        })?;
    if value == [0; N] {
        return Err(SparrowError::new(
            ErrorCode::Internal,
            "JetStream identity entropy invalid",
        ));
    }
    Ok(value)
}

/// Stable local owner, tied to the canonical checkpoint directory. Creation is
/// private and durable before any network reader. A partial creation fails
/// closed rather than regenerating an identity around existing broker state.
fn binding_owner(dir: &Path) -> Result<[u8; 32]> {
    use std::io::{Read, Write};
    let fail = |_: std::io::Error| {
        SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "JetStream checkpoint owner file unavailable/invalid",
        )
    };
    let path = dir.join("JETSTREAM_OWNER");
    let identity = match std::fs::symlink_metadata(&path) {
        Ok(meta) => {
            if !meta.file_type().is_file() || meta.len() != 20 {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "JetStream owner marker must be a regular 20-byte file",
                ));
            }
            let mut bytes = [0u8; 20];
            std::fs::File::open(&path)
                .map_err(fail)?
                .read_exact(&mut bytes)
                .map_err(fail)?;
            if &bytes[..4] != b"JOW1" || bytes[4..] == [0; 16] {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "JetStream owner marker corrupt",
                ));
            }
            bytes[4..].try_into().unwrap()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let identity = random::<16>()?;
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&path).map_err(fail)?;
            file.write_all(b"JOW1").map_err(fail)?;
            file.write_all(&identity).map_err(fail)?;
            file.sync_all().map_err(fail)?;
            std::fs::File::open(dir)
                .map_err(fail)?
                .sync_all()
                .map_err(fail)?;
            identity
        }
        Err(e) => return Err(fail(e)),
    };
    let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
    hash.update(b"Sparrow-JetStream-owner-v1\0");
    hash.update(&identity);
    hash.update(
        dir.canonicalize()
            .map_err(fail)?
            .as_os_str()
            .as_encoded_bytes(),
    );
    Ok(hash.finish().as_ref().try_into().unwrap())
}

impl Supervisor {
    pub(super) async fn start_jetstream(
        &self,
        spec: &crate::PipelineSpec,
        schema: Schema,
        plan: PhysicalPlan,
        target_policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<RunningJob> {
        validate_aligned_plan(spec, &plan)?;
        let config = spec
            .source
            .jetstream
            .as_ref()
            .expect("validated JetStream config");
        let policy = spec
            .checkpoint
            .clone()
            .expect("validated checkpoint policy");
        let dir = spec
            .checkpoint_dir
            .clone()
            .expect("validated checkpoint directory");
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan)?);
        let layout = manifest.clone();
        let retention = policy.retention();
        let max_keys = self.kernel.job_budget().max_state_keys;
        // Reserve capacity before filesystem/network bootstrap. A waiting
        // ninth source must not steal the unused quotas of eight live jobs.
        let admission = self.kernel.prepare_source_admission(plan.pipeline)?;
        let owner = admission.owner();
        let (store,snapshot,generation,output,binding)=self.store.run_blocking(move || {
            sparrow_connectors::check_data_path(Path::new(&dir))?;
            let store=CheckpointStore::open_reliable_exclusive(&dir,max_keys,retention)?;
            let inventory=store.inventory()?;
            let restore=inventory.current.is_some() || inventory.current_error.is_some();
            if !restore && !inventory.generations.is_empty() {return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"JetStream checkpoint history exists without CURRENT"));}
            let snapshot=if restore {Some(store.recover_pipeline_required()?)}else{None};
            let (generation,output)=if let Some(s)=&snapshot {
                s.check_compatible(&layout)?;
                if s.source.identity.kind!="jetstream-v1" {return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"JetStream cannot adopt another source's checkpoint history"));}
                let position=s.next_output.ok_or_else(||SparrowError::new(ErrorCode::UnsupportedRestore,"JetStream snapshot lacks output identity"))?;
                // Initial K2 deliberately refuses live semantic forks. A
                // durable fork/replay operation needs its own lineage commit;
                // deriving a new epoch only in RAM would be unsafe on crash.
                if s.plan.semantics!=layout.semantics {return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"JetStream downstream semantic changes require an explicit lineage fork (not yet enabled)"));}
                (s.generation,position)
            } else {let generation=random::<16>()?;(generation,OutputSequence::new(generation,1)?)};
            let binding=binding_owner(store.dir())?;
            store.activate_state_generation(generation)?;
            Ok((store,snapshot,generation,output,binding))
        }).await?;
        let inventory = store.inventory()?;
        let nonce = random::<16>()?;
        let next_checkpoint = store.next_checkpoint_id();
        let store = Arc::new(std::sync::Mutex::new(store));
        let shutdown_guard = Arc::new((store.clone(), admission.lifecycle_guard()));
        let connection = Connection::open_with_guard(
            &config.connection(),
            target_policy,
            &self.secrets,
            &owner,
            shutdown_guard,
        )
        .await?;
        let reader = Reader::open(
            connection,
            config.reader(),
            owner.clone(),
            binding,
            nonce,
            snapshot.as_ref().map(|s| &s.source),
        )
        .await?;
        let ingested = snapshot.as_ref().map_or(0, |s| s.ingested_rows);
        let restored_from = snapshot.as_ref().map(|s| s.checkpoint_id);
        let restore = snapshot.map(|s| s.windows);
        let revision = plan.revision.raw();
        let (tx, rx) = observed::channel(spec.source.inbox_capacity);
        let (tx_out, rx_out) = observed::channel(spec.sink.outbox_capacity);
        let outbox = Arc::new(InflightCounter::new());
        let acks = AlignedAcks::default().with_output_sequence(output)?;
        let diag = IoDiagnostics::new();
        diag.observe_source(&tx);
        diag.observe_sink(&tx_out);
        let submitted = self.kernel.submit(
            JobRequest::new(plan, vec![], SharedCapture::disabled())
                .with_source_admission(admission)
                .with_live_events(rx)
                .with_live_out(tx_out)
                .with_observation(diag.observation.clone())
                .with_aligned(AlignedJob {
                    restore: None,
                    pipeline: Some(PipelineRestore {
                        plan: manifest.clone(),
                        generation,
                        restore,
                    }),
                    acks: acks.clone(),
                    outbox: outbox.clone(),
                }),
        );
        let job = match submitted {
            Ok(job) => job,
            Err(e) => {
                let _ = reader.close().await;
                return Err(e);
            }
        };
        let cancel = job.cancellation();
        let checkpoint = CheckpointControl::new(policy.clone(), job.attempt.raw(), inventory);
        checkpoint.restored_from(restored_from);
        checkpoint.state_generation(generation);
        let (cmd, commands) = tokio::sync::mpsc::channel(1);
        let sink = match self.spawn_sink(
            spec,
            rx_out,
            cancel.clone(),
            diag.clone(),
            None,
            target_policy,
            Some(outbox.clone()),
        ) {
            Ok(sink) => sink,
            Err(e) => {
                cancel.cancel();
                let _ = job.stop().await;
                let _ = reader.close().await;
                return Err(e);
            }
        };
        let restored_cut = reader.committed();
        let source = self.kernel.handle().spawn(
            Actor {
                reader,
                tx,
                commands,
                schema: Arc::new(schema),
                acks,
                outbox,
                store,
                manifest,
                owner,
                metrics: self.kernel.metrics.clone(),
                control: checkpoint.clone(),
                diag: diag.clone(),
                cancel: cancel.clone(),
                ingested,
                next_checkpoint,
                revision,
                max_keys,
                row_limit: self.kernel.ingress_row_limit(),
                batch_rows: self.kernel.ingress_batch_rows().min(config.pull_messages),
                deferred: None,
                bootstrap: restored_from.is_none(),
                restored_cut,
                max_pending: config.max_pending,
                max_pending_bytes: config.pending_bytes,
            }
            .run(),
        );
        let scheduler = Some(self.kernel.handle().spawn(periodic_checkpoints(
            cmd.clone(),
            checkpoint.clone(),
            cancel.clone(),
        )));
        Ok(RunningJob {
            source_kind: "jetstream",
            sink_kind: "http",
            started_at: Instant::now(),
            stable: false,
            revision,
            diag,
            kind: RunningKind::Aligned {
                cancel,
                cmd,
                handle: job,
                source,
                sink,
                scheduler,
                checkpoint,
            },
        })
    }
}

struct Actor {
    reader: Reader,
    tx: observed::Sender<IngressEvent>,
    commands: tokio::sync::mpsc::Receiver<AlignedCmd>,
    schema: Arc<Schema>,
    acks: AlignedAcks,
    outbox: Arc<InflightCounter>,
    store: Arc<std::sync::Mutex<CheckpointStore>>,
    manifest: Arc<CheckpointPlan>,
    owner: Arc<MemoryOwner>,
    metrics: Arc<sparrow_runtime::RuntimeMetrics>,
    control: Arc<CheckpointControl>,
    diag: Arc<IoDiagnostics>,
    cancel: CancellationToken,
    ingested: u64,
    next_checkpoint: u64,
    revision: u64,
    max_keys: usize,
    row_limit: usize,
    batch_rows: usize,
    deferred: Option<InputRecord>,
    bootstrap: bool,
    restored_cut: u64,
    max_pending: usize,
    max_pending_bytes: usize,
}
impl Actor {
    fn observe(&self) {
        self.control
            .observe_reliable_source(crate::checkpoint::ReliableSourceStatus {
                restored_cut: self.restored_cut,
                published_cut: self.reader.position(self.ingested).offset_bytes,
                committed_cut: self.reader.committed(),
                pending_messages: self.reader.pending(),
                pending_bytes: self.reader.pending_bytes(),
                max_pending_messages: self.max_pending,
                max_pending_bytes: self.max_pending_bytes,
                sampled_at: tokio::time::Instant::now(),
                redeliveries: self.reader.redeliveries(),
                pull_requests: self.reader.pulls(),
                ack_retries: self.reader.ack_retries(),
                retention_available: self.reader.retention_available(),
            });
    }
    async fn checkpoint(
        &mut self,
        mut admission: CheckpointAdmission,
        reply: Option<tokio::sync::oneshot::Sender<Result<u64>>>,
        workers: &mut tokio::task::JoinSet<Result<Option<u64>>>,
    ) -> Result<()> {
        self.reader.verify().await?;
        let cut = self.reader.position(self.ingested);
        let ingested = self.ingested;
        let id = self.next_checkpoint;
        self.next_checkpoint = self.next_checkpoint.checked_add(1).ok_or_else(|| {
            SparrowError::new(ErrorCode::BoundExceeded, "checkpoint id exhausted")
        })?;
        admission.phase("aligning");
        let request = self.acks.begin_with_deadline(id, admission.deadline)?;
        let injected = tokio::select! {
            _=self.cancel.cancelled()=>return Err(SparrowError::new(ErrorCode::Cancelled,"JetStream checkpoint cancelled")),
            result=tokio::time::timeout_at(admission.deadline,self.tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier{checkpoint_id:id})))=>result,
        };
        if !matches!(injected, Ok(Ok(()))) {
            if injected.is_err() {
                let error = checkpoint_timeout("JetStream barrier publication timed out");
                self.metrics.record_checkpoint_abort();
                admission.finish(&Err(error.clone()), None);
                if let Some(reply) = reply {
                    let _ = reply.send(Err(error));
                }
                // Dropping request abandons this barrier id; no cut is ACKable.
                workers.spawn(async { Ok(None) });
                return Ok(());
            }
            return Err(SparrowError::new(
                ErrorCode::JobFailed,
                "JetStream barrier publication failed",
            )
            .retryable(true));
        }
        let store = self.store.clone();
        let plan = self.manifest.clone();
        let owner = self.owner.clone();
        let max_keys = self.max_keys;
        let revision = self.revision;
        let timeout = self.control.timeout();
        let cancel = self.cancel.clone();
        let metrics = self.metrics.clone();
        workers.spawn(async move {
            let source_sequence=cut.offset_bytes;
            let result=async {
                let aligned=tokio::select! {
                    _=cancel.cancelled()=>return Err(SparrowError::new(ErrorCode::Cancelled,"JetStream checkpoint cancelled")),
                    result=tokio::time::timeout_at(admission.deadline,request.wait_participants(timeout))=>result
                        .map_err(|_|checkpoint_timeout("JetStream required output/checkpoint timed out"))??,
                };
                admission.phase("committing");
                // Never cancel/abort this blocking worker after durable I/O has
                // begun. Source stop joins it before releasing the writer lock.
                tokio::task::spawn_blocking(move || {
                    let started=std::time::Instant::now();let mut store=store.lock().expect("checkpoint store");
                    let encoded=PipelineSnapshot::encode_frozen(id,&cut,ingested,revision,&plan,aligned,&owner,max_keys)?;
                    let bytes=encoded.bytes().len() as u64;let committed=store.commit_prepared(&encoded)?;
                    Ok::<_,SparrowError>((committed,bytes,started.elapsed(),store.inventory().ok()))
                }).await.map_err(|_|SparrowError::new(ErrorCode::Internal,"JetStream checkpoint worker panicked"))?
            }.await;
            match result {
                Ok((id,bytes,elapsed,inventory))=>{
                    metrics.record_checkpoint(elapsed,bytes);admission.finish(&Ok(id),inventory);
                    if let Some(reply)=reply {let _=reply.send(Ok(id));}
                    Ok(Some(source_sequence))
                }
                Err(e)=>{
                    metrics.record_checkpoint_abort();admission.finish(&Err(e.clone()),None);
                    if let Some(reply)=reply {let _=reply.send(Err(e.clone()));}
                    if e.context.iter().any(|(key,value)|key=="checkpoint_outcome" && value=="timeout") {Ok(None)} else {Err(e)}
                }
            }
        });
        Ok(())
    }

    /// Drain only already-ready records, bounded by the kernel row/byte caps.
    /// Control/barrier handling resumes only after this whole batch is published.
    async fn publish_input(&mut self, first: InputRecord) -> Result<()> {
        let started = std::time::Instant::now();
        let first_sequence = first.sequence();
        let mut last_sequence = first_sequence;
        let mut builder = sparrow_model::RowBatchBuilder::new(
            self.schema.clone(),
            self.owner.clone(),
            sparrow_model::CreditKind::Reservation,
            self.batch_rows,
            self.row_limit,
        )?;
        let mut record = Some(first);
        for _ in 0..self.batch_rows {
            if let Some(next) = record.take() {
                let sequence = next.sequence();
                let added = next
                    .decode_into(&self.schema, &self.owner, &mut builder, self.row_limit)
                    .map_err(|e| {
                        self.diag
                            .decode_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        SparrowError::new(
                            e.code,
                            format!(
                                "JetStream decode failed at source_sequence={sequence} ({})",
                                e.code
                            ),
                        )
                        .context("source_sequence", sequence.to_string())
                    })?;
                if !added {
                    self.deferred = Some(next);
                    break;
                }
                last_sequence = sequence;
            }
            if builder.num_rows() >= self.batch_rows {
                break;
            }
            match self.reader.try_next().transpose()? {
                Some(ReaderPoll::Record(next)) => record = Some(next),
                Some(ReaderPoll::Duplicate) => {}
                _ => break,
            }
        }
        // The last nonblocking poll may have consumed a record on a duplicate
        // iteration at the quantum boundary. Keep its credit and position.
        if let Some(record) = record {
            self.deferred = Some(record);
        }
        let batch = builder.finish()?;
        let rows = batch.num_rows();
        self.diag.observation.progress(true, rows);
        let event = IngressEvent::admitted_batch(batch)?;
        tokio::select! {
            _=self.cancel.cancelled()=>return Ok(()),
            result=self.tx.send_with_origin(event,OriginSpan::at(started))=>result
                .map_err(|_|SparrowError::new(ErrorCode::Cancelled,"JetStream Kernel ingress closed"))?,
        }
        for sequence in first_sequence..=last_sequence {
            self.reader.published(sequence)?;
        }
        self.ingested = self.ingested.checked_add(rows as u64).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::IntegerOverflow,
                "JetStream row counter exhausted",
            )
        })?;
        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        self.observe();
        let _lifecycle = self.diag.observation.lifecycle(true);
        let mut workers = tokio::task::JoinSet::new();
        let mut progress = tokio::time::interval(Duration::from_secs(5));
        progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let ack_signal = self.reader.ack_signal();
        let mut idle = IdlePull::default();
        let mut retry_after = tokio::time::Instant::now();
        let result:Result<()>=async {
            loop {
                if self.cancel.is_cancelled() {return Ok(());}
                if self.outbox.failed()>0 {
                    return Err(SparrowError::new(ErrorCode::JobFailed,"JetStream required HTTP output failed; input remains uncommitted").retryable(true));
                }
                self.reader.service_acknowledgements()?;
                let pull_due=tokio::time::Instant::now()>=idle.until;
                let ready=if self.bootstrap || !pull_due {false}else if self.deferred.is_some(){true}else{tokio::select! {
                    _=self.cancel.cancelled()=>return Ok(()),
                    result=tokio::time::timeout(Duration::from_secs(5),self.reader.prepare_pull())=>result
                        .map_err(|_|SparrowError::new(ErrorCode::JobFailed,"JetStream pull setup timed out; aborting attempt without ACK").retryable(true))??,
                }};
                let needs_cut=self.bootstrap || (!ready && pull_due && self.reader.position(self.ingested).offset_bytes>self.reader.committed());
                if needs_cut && workers.is_empty() && tokio::time::Instant::now()>=retry_after {
                    if let Some(admission)=self.control.try_begin(if self.bootstrap {"bootstrap"}else{"source_full"})? {
                        self.checkpoint(admission,None,&mut workers).await?;
                    }
                }
                tokio::select! {
                    biased;
                    _=self.cancel.cancelled()=>return Ok(()),
                    _=ack_signal.notified()=>{self.reader.service_acknowledgements()?;self.observe();},
                    _=tokio::time::sleep_until(idle.until),if !pull_due=>{},
                    _=tokio::time::sleep_until(retry_after),if needs_cut && tokio::time::Instant::now()<retry_after=>{},
                    joined=workers.join_next(),if !workers.is_empty()=>{
                        let cut=joined.expect("active worker").map_err(|_|SparrowError::new(ErrorCode::Internal,"JetStream checkpoint task panicked"))??;
                        let Some(cut)=cut else {retry_after=tokio::time::Instant::now()+Duration::from_millis(250);continue;};
                        self.reader.mark_committed(cut)?;
                        self.observe();
                        self.bootstrap=false;
                        self.diag.observation.health(true,HealthState::Ready,"jetstream_checkpoint_confirmed",None);
                    }
                    command=self.commands.recv()=>{
                        let Some(AlignedCmd::Checkpoint{reply,admission})=command else {return Ok(());};
                        if reply.is_closed() || tokio::time::Instant::now()>=admission.deadline {continue;}
                        self.checkpoint(admission,Some(reply),&mut workers).await?;
                    }
                    _=progress.tick()=>{
                        tokio::select! {
                            _=self.cancel.cancelled()=>return Ok(()),
                            result=tokio::time::timeout(Duration::from_secs(10),async {self.reader.progress().await?;self.reader.verify().await})=>result
                                .map_err(|_|SparrowError::new(ErrorCode::JobFailed,"JetStream progress/ownership check timed out").retryable(true))??,
                        }
                        self.observe();
                    }
                    input=async {if let Some(record)=self.deferred.take(){Ok(ReaderPoll::Record(record))}else{self.reader.next().await}},if ready=>{
                        match input? {
                            ReaderPoll::Record(record)=>{
                                idle.reset();self.publish_input(record).await?;
                            }
                            ReaderPoll::Empty=>idle.empty(),
                            ReaderPoll::BatchEnd=>idle.reset(),
                            ReaderPoll::Duplicate|ReaderPoll::Full=>{},
                        }
                    }
                }
            }
        }.await;
        if result.is_err() {
            self.cancel.cancel();
        }
        // Complete non-cancellable durable commits, but no need to ACK on stop:
        // restart creates a reader from that CURRENT even if all ACKs were lost.
        let mut join_error = None;
        while let Some(joined) = workers.join_next().await {
            if let Err(e) = joined {
                join_error = Some(SparrowError::new(
                    ErrorCode::Internal,
                    format!("JetStream checkpoint join: {e}"),
                ));
            }
        }
        let closed = self.reader.close().await;
        drop(self.store);
        if result.is_err() {
            return result;
        }
        closed?;
        if let Some(e) = join_error {
            return Err(e);
        }
        result
    }
}

fn checkpoint_timeout(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::ResourceExhausted, message)
        .context("checkpoint_outcome", "timeout")
}
struct IdlePull {
    delay_ms: u64,
    until: tokio::time::Instant,
}
impl Default for IdlePull {
    fn default() -> Self {
        Self {
            delay_ms: 5,
            until: tokio::time::Instant::now(),
        }
    }
}
impl IdlePull {
    fn reset(&mut self) {
        *self = Self::default();
    }
    fn empty(&mut self) {
        self.until = tokio::time::Instant::now() + Duration::from_millis(self.delay_ms);
        self.delay_ms = (self.delay_ms * 2).min(250);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn k2_idle_backoff_is_bounded_and_nonempty_fetch_resets_it() {
        let mut idle = IdlePull::default();
        for expected in [10, 20, 40, 80, 160, 250, 250] {
            idle.empty();
            assert_eq!(idle.delay_ms, expected);
        }
        assert!(idle.until > tokio::time::Instant::now());
        idle.reset();
        assert_eq!(idle.delay_ms, 5);
        assert!(idle.until <= tokio::time::Instant::now());
    }
}

//! Independent source-observed decisions for the bounded silence profile.
//! Publication order: persist decision -> time -> optional input -> feed fact -> barrier ->
//! required output flush -> CURRENT -> broker ACK. No next decision before it.
use super::observed_time_log::{self as log, fail, Decision, Fact};
use super::*;
#[cfg(feature = "jetstream")]
use sparrow_connectors::jetstream::{Connection, Reader, ReaderPoll};
use sparrow_io::{
    feed::{FeedObservation, FeedReadiness},
    SourcePosition,
};
use sparrow_model::{
    CreditKind, MemoryLease, MemoryOwner, OutputSequence, RowBatch, Schema, SharedVirtualClock,
};
use sparrow_runtime::{
    clock::RuntimeClock,
    observed_cut::{Coverage, ObservedCut},
};
use std::path::Path;

enum Input {
    File(Option<FileReplaySource>),
    #[cfg(feature = "jetstream")]
    JetStream(Reader),
}
impl Input {
    async fn close(self) -> Result<()> {
        match self {
            Self::File(_) => Ok(()),
            #[cfg(feature = "jetstream")]
            Self::JetStream(reader) => reader.close().await,
        }
    }
    async fn maintain(&mut self) -> Result<()> {
        #[cfg(feature = "jetstream")]
        if let Self::JetStream(reader) = self {
            reader.service_acknowledgements()?;
            tokio::time::timeout(Duration::from_secs(5), reader.progress())
                .await
                .map_err(|_| fail("JetStream progress timed out"))??;
        }
        Ok(())
    }
    fn committed(&mut self, cut: &ObservedCut) -> Result<()> {
        #[cfg(feature = "jetstream")]
        if let Self::JetStream(reader) = self {
            reader.mark_committed(cut.source.offset_bytes)?;
        }
        let _ = cut;
        Ok(())
    }
}
impl Supervisor {
    pub(super) async fn start_observed_time(
        &self,
        spec: &crate::PipelineSpec,
        schema: Schema,
        plan: PhysicalPlan,
        target_policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<RunningJob> {
        validate_aligned_plan(spec, &plan)?;
        let max_gap = plan
            .stages
            .iter()
            .find_map(|stage| match stage {
                sparrow_plan::PhysicalStage::Iot { spec, .. } => match &spec.timing {
                    Some(sparrow_plan::IotTimingSpec::Silence {
                        max_observation_gap_micros,
                        ..
                    }) => Some(*max_observation_gap_micros),
                    _ => None,
                },
                _ => None,
            })
            .ok_or_else(|| fail("source-observed profile requires silence"))?;
        let admission = self.kernel.prepare_source_admission(plan.pipeline)?;
        let owner = admission.owner();
        // Covers the bounded decision record, JSON/hex encodings, cold read
        // buffers and hashes throughout startup and the actor lifetime.
        let workspace = owner.acquire(CreditKind::Reservation, log::WORKSPACE_BYTES)?;
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan)?);
        let semantics = log::digest(&manifest.semantics);
        let policy = spec.checkpoint.clone().expect("paused admission");
        let dir = spec.checkpoint_dir.clone().expect("paused admission");
        let profile = if spec.source.kind == "jetstream" {
            sparrow_runtime::observed_cut::JETSTREAM_KIND
        } else {
            sparrow_runtime::observed_cut::FILE_KIND
        };
        let max_keys = self.kernel.job_budget().max_state_keys;
        let layout = manifest.clone();
        let retention = policy.retention();
        let (store, snapshot, generation, pending) = self
            .store
            .run_blocking(move || {
                sparrow_connectors::check_data_path(Path::new(&dir))?;
                let store = CheckpointStore::open_for_plan_exclusive(
                    &dir, max_keys, retention, &layout, profile,
                )?;
                let inventory = store.inventory()?;
                if inventory.marker_error.is_some() {
                    return Err(fail(
                        "state generation marker is corrupt; refusing fresh activation",
                    ));
                }
                let has_current = inventory.current.is_some() || inventory.current_error.is_some();
                if !has_current && !inventory.generations.is_empty() {
                    return Err(fail(
                        "history without CURRENT; preserve it and inspect the original directory",
                    ));
                }
                let snapshot = if has_current {
                    Some(store.recover_pipeline_required()?)
                } else {
                    None
                };
                let pending = log::read(store.dir())?;
                let generation = if let Some(snapshot) = &snapshot {
                    snapshot.check_compatible(&layout)?;
                    if snapshot.plan.semantics != layout.semantics {
                        return Err(fail("semantic changes require a new directory"));
                    }
                    if inventory.state_generation_marker != Some(snapshot.generation) {
                        return Err(fail("state generation marker differs from CURRENT"));
                    }
                    let cut = ObservedCut::unwrap(&snapshot.source)?;
                    if cut.source.identity.kind
                        != if profile == sparrow_runtime::observed_cut::FILE_KIND {
                            "file"
                        } else {
                            "jetstream-v1"
                        }
                    {
                        return Err(fail("source kind differs from profile"));
                    }
                    let saved = pending
                        .as_ref()
                        .ok_or_else(|| fail("TIME_PENDING is missing for CURRENT"))?;
                    saved.check(
                        snapshot.generation,
                        semantics,
                        &cut,
                        snapshot.ingested_rows,
                        max_gap,
                    )?;
                    snapshot.generation
                } else {
                    let generation = inventory
                        .state_generation_marker
                        .map(Ok)
                        .unwrap_or_else(log::generation)?;
                    if pending.as_ref().is_some_and(|p| {
                        p.sequence != 0 || p.generation != generation || p.semantics != semantics
                    }) {
                        return Err(fail("non-bootstrap decision exists without CURRENT"));
                    }
                    generation
                };
                store.activate_state_generation(generation)?;
                Ok((store, snapshot, generation, pending))
            })
            .await?;
        let restored_from = snapshot.as_ref().map(|s| s.checkpoint_id);
        let restored_cut = snapshot
            .as_ref()
            .map(|s| ObservedCut::unwrap(&s.source))
            .transpose()?;
        let ingested = snapshot.as_ref().map_or(0, |s| s.ingested_rows);
        let output = snapshot
            .as_ref()
            .and_then(|s| s.next_output)
            .unwrap_or(OutputSequence::new(generation, 1)?);
        let inventory = store.inventory()?;
        let next_checkpoint = store.next_checkpoint_id();
        let store = Arc::new(std::sync::Mutex::new(store));
        let guard: Arc<dyn Send + Sync> = Arc::new((store.clone(), admission.lifecycle_guard()));
        let (input, cut) = if spec.source.kind != "jetstream" {
            let mut cfg = FileReplayConfig::new(
                spec.source.path.as_ref().expect("file path"),
                schema.clone(),
            );
            cfg.recovery = RecoveryPolicy::Aligned;
            cfg.fail_on_decode = true;
            cfg.contract = sparrow_connectors::FileContract::AppendOnly;
            let restored = restored_cut.clone();
            let (source, cut) = self
                .store
                .run_blocking(move || {
                    sparrow_connectors::check_data_path(&cfg.path)?;
                    let mut source = FileReplaySource::open(&cfg).map_err(SparrowError::from)?;
                    let cut = if let Some(cut) = restored {
                        source.seek(&cut.source)?;
                        cut
                    } else {
                        ObservedCut {
                            sequence: 0,
                            micros: 0,
                            coverage: Coverage::default(),
                            source: source.checkpoint_position()?,
                        }
                    };
                    Ok((source, cut))
                })
                .await?;
            (Input::File(Some(source)), cut)
        } else {
            #[cfg(feature = "jetstream")]
            {
                let directory = store.lock().expect("checkpoint store").dir().to_owned();
                let binding = self
                    .store
                    .run_blocking(move || super::jetstream_source::binding_owner(&directory))
                    .await?;
                let config = spec.source.jetstream.as_ref().expect("JetStream config");
                let connection = Connection::open_with_guard(
                    &config.connection(),
                    target_policy,
                    &self.secrets,
                    &owner,
                    guard.clone(),
                )
                .await?;
                let mut reader_config = config.reader();
                reader_config.pull_messages = 1;
                let reader = Reader::open(
                    connection,
                    reader_config,
                    owner.clone(),
                    binding,
                    super::jetstream_source::random::<16>()?,
                    restored_cut.as_ref().map(|c| &c.source),
                )
                .await?;
                let cut = restored_cut.unwrap_or_else(|| ObservedCut {
                    sequence: 0,
                    micros: 0,
                    coverage: Coverage::default(),
                    source: reader.position(0),
                });
                (Input::JetStream(reader), cut)
            }
            #[cfg(not(feature = "jetstream"))]
            {
                return Err(fail("JetStream is not enabled by this build"));
            }
        };
        if let Some(pending) = &pending {
            if let Err(e) = pending.check(generation, semantics, &cut, ingested, max_gap) {
                let _ = input.close().await;
                return Err(e);
            }
        }
        let (restore, iot) = snapshot
            .map(|s| (Some(s.windows), s.iot))
            .unwrap_or((None, Vec::new()));
        let (tx, rx) = observed::channel(spec.source.inbox_capacity.max(1));
        let (tx_out, rx_out) = observed::channel(spec.sink.outbox_capacity.max(1));
        let outbox = Arc::new(InflightCounter::new());
        let acks = AlignedAcks::default().with_output_sequence(output)?;
        let diag = IoDiagnostics::new();
        diag.observe_source(&tx);
        diag.observe_sink(&tx_out);
        let revision = plan.revision.raw();
        let request = JobRequest::new(plan, vec![], SharedCapture::disabled())
            .with_source_admission(admission)
            .with_live_events(rx)
            .with_live_out(tx_out)
            .with_observation(diag.observation.clone())
            .with_clock(RuntimeClock::virtual_clock(SharedVirtualClock::new(
                cut.micros,
            )))
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    sink: None,
                    plan: manifest.clone(),
                    generation,
                    restore,
                    iot,
                }),
                acks: acks.clone(),
                outbox: outbox.clone(),
            });
        let job = match self.kernel.submit(request) {
            Ok(job) => job,
            Err(e) => {
                let _ = input.close().await;
                return Err(e);
            }
        };
        let cancel = job.cancellation();
        let control = CheckpointControl::new(policy.clone(), job.attempt.raw(), inventory);
        control.restored_from(restored_from);
        control.state_generation(generation);
        if restored_from.is_some() {
            control.observe_committed_source(&cut);
        }
        let (cmd, commands) = tokio::sync::mpsc::channel(1);
        let sink = match self.spawn_sink(
            job.memory_owner(),
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
                let _ = input.close().await;
                return Err(e);
            }
        };
        let actor = Actor {
            input,
            tx,
            commands,
            schema: Arc::new(schema),
            acks,
            outbox,
            store,
            manifest,
            owner,
            control: control.clone(),
            diag: diag.clone(),
            cancel: cancel.clone(),
            metrics: self.kernel.metrics.clone(),
            cut,
            ingested,
            generation,
            semantics,
            pending,
            bootstrap: restored_from.is_none(),
            next_checkpoint,
            revision,
            max_keys,
            row_limit: self.kernel.ingress_row_limit(),
            max_gap,
            restart: true,
            tick: Duration::from_millis(policy.interval_ms.unwrap()),
            _workspace: workspace,
            _guard: guard,
        };
        let source = self.kernel.handle().spawn(actor.run());
        Ok(RunningJob {
            graph_ports: None,
            source_kind: if spec.source.kind == "jetstream" {
                "jetstream"
            } else {
                "file"
            },
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
                scheduler: None,
                checkpoint: control,
            },
        })
    }
}

struct Actor {
    input: Input,
    tx: observed::Sender<IngressEvent>,
    commands: tokio::sync::mpsc::Receiver<AlignedCmd>,
    schema: Arc<Schema>,
    acks: AlignedAcks,
    outbox: Arc<InflightCounter>,
    store: Arc<std::sync::Mutex<CheckpointStore>>,
    manifest: Arc<CheckpointPlan>,
    owner: Arc<MemoryOwner>,
    control: Arc<CheckpointControl>,
    diag: Arc<IoDiagnostics>,
    cancel: CancellationToken,
    metrics: Arc<sparrow_runtime::RuntimeMetrics>,
    cut: ObservedCut,
    ingested: u64,
    generation: [u8; 16],
    semantics: [u8; 32],
    pending: Option<Decision>,
    bootstrap: bool,
    next_checkpoint: u64,
    revision: u64,
    max_keys: usize,
    row_limit: usize,
    max_gap: i64,
    restart: bool,
    tick: Duration,
    _workspace: MemoryLease,
    _guard: Arc<dyn Send + Sync>,
}
type Reply = Option<tokio::sync::oneshot::Sender<Result<u64>>>;
impl Actor {
    async fn maintenance(&mut self) -> Result<()> {
        self.input.maintain().await
    }
    /// Once cold durable I/O starts, always join it, even on cancellation or
    /// connection errors. Keep broker ownership/ACK progress alive meanwhile.
    async fn worker<T: Send + 'static>(&mut self, mut worker: JoinHandle<Result<T>>) -> Result<T> {
        let mut error = None;
        loop {
            tokio::select! {
                result=&mut worker=>{let value=result.map_err(|_|fail("durable worker panicked"))?;return if let Some(e)=error {Err(e)}else{value};},
                _=tokio::time::sleep(Duration::from_secs(1))=>{if let Err(e)=self.maintenance().await {error=Some(e);}},
            }
        }
    }
    async fn save(&mut self, decision: Decision) -> Result<()> {
        let store = self.store.clone();
        self.worker(tokio::task::spawn_blocking(move || {
            let store = store.lock().expect("checkpoint store");
            log::write(store.dir(), &decision)
        }))
        .await
    }
    async fn send(&self, event: IngressEvent) -> Result<()> {
        tokio::select! { _=self.cancel.cancelled()=>Err(fail("attempt cancelled before publication")),
        result=self.tx.send(event)=>result.map_err(|_|fail("Kernel ingress closed")) }
    }
    async fn acquire(&mut self) -> Result<(CheckpointAdmission, Reply)> {
        loop {
            if let Ok(AlignedCmd::Checkpoint { admission, reply }) = self.commands.try_recv() {
                return Ok((admission, Some(reply)));
            }
            if let Some(admission) = self.control.try_begin("time_decision")? {
                return Ok((admission, None));
            }
            tokio::select! {
                _=self.cancel.cancelled()=>return Err(fail("attempt cancelled while admitting decision")),
                command=self.commands.recv()=>match command {Some(AlignedCmd::Checkpoint {admission,reply})=>return Ok((admission,Some(reply))),None=>return Err(fail("checkpoint control closed"))},
                _=tokio::time::sleep(Duration::from_millis(1))=>{},
            }
        }
    }
    async fn checkpoint(&mut self, mut admission: CheckpointAdmission, reply: Reply) -> Result<()> {
        let result = self.commit(&admission).await;
        match &result {
            Ok((id, bytes, elapsed, inventory)) => {
                self.metrics.record_checkpoint(*elapsed, *bytes);
                admission.finish(&Ok(*id), inventory.clone());
            }
            Err(e) => {
                self.metrics.record_checkpoint_abort();
                admission.finish(&Err(e.clone()), None);
            }
        }
        if let Some(reply) = reply {
            let _ = reply.send(result.as_ref().map(|(id, ..)| *id).map_err(Clone::clone));
        }
        result?;
        self.control.observe_committed_source(&self.cut);
        self.input.committed(&self.cut)?;
        Ok(())
    }
    async fn commit(
        &mut self,
        admission: &CheckpointAdmission,
    ) -> Result<(
        u64,
        u64,
        Duration,
        Option<sparrow_runtime::checkpoint::CheckpointInventory>,
    )> {
        #[cfg(feature = "jetstream")]
        if let Input::JetStream(reader) = &mut self.input {
            tokio::time::timeout(Duration::from_secs(5), reader.verify())
                .await
                .map_err(|_| fail("JetStream verification timed out"))??;
        }
        let id = self.next_checkpoint;
        self.next_checkpoint = id
            .checked_add(1)
            .ok_or_else(|| fail("checkpoint id exhausted"))?;
        admission.phase("aligning");
        let request = self.acks.begin_with_deadline(id, admission.deadline)?;
        tokio::time::timeout_at(
            admission.deadline,
            self.send(IngressEvent::Control(StreamControl::CheckpointBarrier {
                checkpoint_id: id,
            })),
        )
        .await
        .map_err(|_| fail("barrier publication timed out"))??;
        let timeout = self.control.timeout();
        let wait = request.wait_participants(timeout);
        tokio::pin!(wait);
        let aligned = loop {
            tokio::select! {
                _=self.cancel.cancelled()=>return Err(fail("attempt cancelled before commit")),
                _=tokio::time::sleep_until(admission.deadline)=>return Err(fail("required output/checkpoint timed out; decision retained")),
                result=&mut wait=>break result?,
                _=tokio::time::sleep(Duration::from_secs(1))=>self.maintenance().await?,
            }
        };
        admission.phase("committing");
        let store = self.store.clone();
        let manifest = self.manifest.clone();
        let owner = self.owner.clone();
        let max_keys = self.max_keys;
        let revision = self.revision;
        let ingested = self.ingested;
        let position = self.cut.wrap()?;
        self.worker(tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let mut store = store.lock().expect("checkpoint store");
            let encoded = PipelineSnapshot::encode_frozen(
                id, &position, ingested, revision, &manifest, aligned, &owner, max_keys,
            )?;
            let bytes = encoded.bytes().len() as u64;
            let id = store.commit_prepared(&encoded)?;
            Ok((id, bytes, started.elapsed(), store.inventory().ok()))
        }))
        .await
    }
    async fn poll(
        &mut self,
        _until: tokio::time::Instant,
    ) -> Result<Option<(RowBatch, SourcePosition)>> {
        match &mut self.input {
            Input::File(source) => {
                let mut file = source.take().expect("file poll ownership");
                let schema = self.schema.clone();
                let owner = self.owner.clone();
                let row_limit = self.row_limit;
                let (file, result) = tokio::task::spawn_blocking(move || {
                    let result = (|| match file.poll_admitted(schema, owner, row_limit)? {
                        Some(batch) => {
                            let position = file.checkpoint_position()?;
                            Ok(Some((batch, position)))
                        }
                        None => Ok(None),
                    })();
                    (file, result)
                })
                .await
                .map_err(|_| fail("File poll worker panicked"))?;
                *source = Some(file);
                result
            }
            #[cfg(feature = "jetstream")]
            Input::JetStream(reader) => {
                reader.service_acknowledgements()?;
                tokio::time::timeout(Duration::from_secs(5), reader.prepare_pull())
                    .await
                    .map_err(|_| fail("JetStream pull setup timed out"))??;
                let event = tokio::select! {
                    _=self.cancel.cancelled()=>return Ok(None),
                    _=tokio::time::sleep_until(_until)=>return Ok(None),
                    event=reader.next()=>event?,
                };
                if let ReaderPoll::Record(record) = event {
                    let batch = record.decode(&self.schema, &self.owner, self.row_limit)?;
                    let mut position = reader.position(
                        self.ingested
                            .checked_add(1)
                            .ok_or_else(|| fail("input count exhausted"))?,
                    );
                    position.offset_bytes = record.sequence();
                    Ok(Some((batch, position)))
                } else {
                    Ok(None)
                }
            }
        }
    }
    async fn observe(&mut self) -> Result<FeedObservation> {
        let started = tokio::time::Instant::now();
        let mut observation = match &mut self.input {
            Input::File(source) => {
                let file = source.take().expect("File probe ownership");
                let (file, result) = tokio::task::spawn_blocking(move || {
                    let result = file.observe_feed();
                    (file, result)
                })
                .await
                .map_err(|_| fail("File probe worker panicked"))?;
                *source = Some(file);
                result?
            }
            #[cfg(feature = "jetstream")]
            Input::JetStream(reader) => {
                tokio::time::timeout(Duration::from_secs(5), reader.observe_feed(self.ingested))
                    .await
                    .map_err(|_| fail("JetStream feed probe timed out"))??
            }
        };
        if started.elapsed().as_micros() > self.max_gap as u128 {
            // Even an apparently drained source cannot authorize a new device
            // silence event using a sample that took longer than its bound.
            observation.readiness = FeedReadiness::Unverified;
        }
        Ok(observation)
    }

    async fn restore_probed_position(&mut self, target: &SourcePosition) -> Result<()> {
        if *target == self.cut.source {
            return Ok(());
        }
        match &mut self.input {
            Input::File(source) => {
                // A bounded scan can consume blank NDJSON frames without a
                // telemetry row. The saved sampled prefix authenticates that
                // decision; seek it without resampling its readiness/time.
                let mut file = source.take().expect("File replay ownership");
                let target = target.clone();
                let (file, result) = tokio::task::spawn_blocking(move || {
                    let result = file.seek(&target);
                    (file, result)
                })
                .await
                .map_err(|_| fail("File probe replay worker panicked"))?;
                *source = Some(file);
                result
            }
            #[cfg(feature = "jetstream")]
            Input::JetStream(_) => Err(fail("a feed probe cannot advance JetStream input")),
        }
    }

    fn hash(&self, batch: &RowBatch) -> Result<[u8; 32]> {
        if batch.num_rows() != 1 {
            return Err(fail("decision must contain exactly one input row"));
        }
        let row = &batch.rows()[0];
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            row.resident_bytes().saturating_mul(2).saturating_add(128),
        )?;
        let mut bytes = Vec::new();
        for value in &row.values {
            value.encode_value(&mut bytes)?;
        }
        Ok(log::digest(&bytes))
    }
    async fn publish(&mut self, decision: &Decision, batch: Option<RowBatch>) -> Result<()> {
        if decision.row_hash.is_some() != batch.is_some() {
            return Err(fail("decision/input presence mismatch"));
        }
        self.send(IngressEvent::Control(StreamControl::ProcessingTime {
            micros: decision.micros,
        }))
        .await?;
        if let Some(batch) = batch {
            self.send(IngressEvent::admitted_batch(batch)?).await?;
            #[cfg(feature = "jetstream")]
            if let Input::JetStream(reader) = &mut self.input {
                reader.published(decision.cut().source.offset_bytes)?;
            }
            self.diag.observation.progress(true, 1);
        }
        self.send(IngressEvent::Control(StreamControl::FeedObservation {
            coverage_since: decision.qualified_since()?.unwrap_or(-1),
        }))
        .await?;
        self.cut = decision.cut();
        self.ingested = decision.ingested;
        Ok(())
    }
    async fn recover_pending(&mut self) -> Result<()> {
        if self.bootstrap {
            let decision = Decision::new(
                self.generation,
                self.semantics,
                self.cut.clone(),
                0,
                None,
                None,
                false,
            )?;
            if self.pending.as_ref().is_some_and(|old| old != &decision) {
                return Err(fail("bootstrap source cut changed"));
            }
            self.save(decision).await?;
            let (admission, reply) = self.acquire().await?;
            self.checkpoint(admission, reply).await?;
            self.pending = None;
            self.bootstrap = false;
            return Ok(());
        }
        let pending = self
            .pending
            .take()
            .ok_or_else(|| fail("CURRENT lacks a decision log"))?;
        if !pending.check(
            self.generation,
            self.semantics,
            &self.cut,
            self.ingested,
            self.max_gap,
        )? {
            return Ok(());
        }
        let batch = if let Some(hash) = pending.row_hash {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                if self.cancel.is_cancelled() || tokio::time::Instant::now() >= deadline {
                    return Err(fail(
                        "pending input unavailable; refusing replacement decision",
                    ));
                }
                if let Some((batch, position)) = self.poll(deadline).await? {
                    if position != pending.cut().source || self.hash(&batch)? != hash {
                        return Err(fail("replayed input differs from durable decision"));
                    }
                    break Some(batch);
                }
                self.maintenance().await?;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } else {
            self.restore_probed_position(&pending.cut().source).await?;
            None
        };
        let (admission, reply) = self.acquire().await?;
        self.publish(&pending, batch).await?;
        self.checkpoint(admission, reply).await
    }
    async fn drive(&mut self) -> Result<()> {
        self.recover_pending().await?;
        let started = tokio::time::Instant::now();
        let base = self.cut.micros;
        let mut next_tick = started + self.tick;
        let mut next_maintenance = started + Duration::from_secs(1);
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            if self.outbox.failed() > 0 {
                return Err(fail(
                    "required HTTP output failed; decision remains uncommitted",
                ));
            }
            if tokio::time::Instant::now() >= next_maintenance {
                self.maintenance().await?;
                next_maintenance = tokio::time::Instant::now() + Duration::from_secs(1);
            }
            let command = self.commands.try_recv().ok();
            // Data must not indefinitely postpone source observations. When a
            // probe is due it runs at the already published prefix; any queued
            // heartbeat makes the probe non-ready rather than a false silence.
            let input = if command.is_none() && tokio::time::Instant::now() < next_tick {
                self.poll(next_tick.min(next_maintenance)).await?
            } else {
                None
            };
            if input.is_none() && command.is_none() && tokio::time::Instant::now() < next_tick {
                tokio::time::sleep_until(
                    (tokio::time::Instant::now() + Duration::from_millis(10)).min(next_tick),
                )
                .await;
                continue;
            }
            let (admission, reply) =
                if let Some(AlignedCmd::Checkpoint { admission, reply }) = command {
                    (admission, Some(reply))
                } else {
                    self.acquire().await?
                };
            let elapsed = i64::try_from(started.elapsed().as_micros())
                .map_err(|_| fail("logical clock overflow"))?;
            let now = base
                .checked_add(elapsed)
                .ok_or_else(|| fail("logical clock overflow"))?;
            let (batch, source, hash, fact) = if let Some((batch, source)) = input {
                let hash = self.hash(&batch)?;
                (Some(batch), source, Some(hash), None)
            } else {
                let observation = self.observe().await?;
                let fact = Fact::from(&observation);
                (None, observation.position, None, Some(fact))
            };
            if self.cancel.is_cancelled() || tokio::time::Instant::now() >= admission.deadline {
                return Err(fail(
                    "decision cancelled or expired before journal publication",
                ));
            }
            let coverage = self.cut.coverage.advance(
                now,
                self.max_gap,
                fact.map(Fact::readiness).transpose()?,
                self.restart,
            )?;
            let cut = ObservedCut {
                sequence: self
                    .cut
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| fail("decision sequence exhausted"))?,
                micros: now,
                coverage,
                source,
            };
            let ingested = self
                .ingested
                .checked_add(u64::from(batch.is_some()))
                .ok_or_else(|| fail("input count exhausted"))?;
            let decision = Decision::new(
                self.generation,
                self.semantics,
                cut,
                ingested,
                fact,
                hash,
                self.restart,
            )?;
            decision.check(
                self.generation,
                self.semantics,
                &self.cut,
                self.ingested,
                self.max_gap,
            )?;
            self.save(decision.clone()).await?;
            self.publish(&decision, batch).await?;
            self.checkpoint(admission, reply).await?;
            self.restart = false;
            if fact.is_some() {
                next_tick = tokio::time::Instant::now() + self.tick;
            }
            let (state, reason) = match fact.map(Fact::readiness).transpose()? {
                None => (HealthState::Ready, "observed_input_committed"),
                Some(FeedReadiness::CaughtUp) => {
                    (HealthState::WaitingForAppend, "feed_prefix_caught_up")
                }
                Some(FeedReadiness::Backlog) => (HealthState::Ready, "feed_backlog"),
                Some(FeedReadiness::PartialRecord) => {
                    (HealthState::WaitingForAppend, "feed_incomplete_record")
                }
                Some(FeedReadiness::InFlight) => (HealthState::Ready, "feed_pull_inflight"),
                Some(FeedReadiness::Unverified) => (HealthState::Initializing, "feed_unverified"),
                Some(FeedReadiness::Ended) => (HealthState::Eof, "feed_ended"),
            };
            self.diag.observation.health(true, state, reason, None);
        }
    }
    async fn run(mut self) -> Result<()> {
        let _lifecycle = self.diag.observation.lifecycle(true);
        let result = self.drive().await;
        if let Err(error) = &result {
            if !self.cancel.is_cancelled() {
                self.diag.observation.health(
                    true,
                    HealthState::Failed,
                    "observed_attempt_failed",
                    Some(error.code),
                );
            }
        }
        if result.is_err() {
            self.cancel.cancel();
        }
        let closed = self.input.close().await;
        result.and(closed)
    }
}

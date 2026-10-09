//! Serialized File graph decisions: log -> ordered round -> full barrier ->
//! every required HTTP flush -> CURRENT. Only one decision may be uncommitted.
use super::graph_time_log::{self as log, Decision};
use super::paused_time_log::{digest, fail, generation};
use super::*;
use sparrow_connectors::{FileContract, TargetPolicy};
use sparrow_io::SourcePosition;
use sparrow_model::{
    CreditKind, EventTimeBinding, MemoryLease, MemoryOwner, RowBatch, Schema, SharedVirtualClock,
};
use sparrow_runtime::{
    graph_cut::{GraphCut, GraphRuntime, Progress, SourceProgress, UnionProgress, KIND},
    GraphInput, GraphOutput,
};
use std::{collections::BTreeMap, path::Path};

fn contract_tag(value: FileContract) -> u8 {
    match value {
        FileContract::AppendOnly => 0,
        FileContract::Sealed => 1,
        FileContract::Immutable => 2,
    }
}
struct Input {
    id: u32,
    file: Option<FileReplaySource>,
    schema: Arc<Schema>,
    contract: FileContract,
    tx: observed::Sender<IngressEvent>,
    diag: Arc<IoDiagnostics>,
    time: Option<EventTimeBinding>,
}
impl Supervisor {
    pub(super) async fn start_time_graph(
        &self,
        spec: &crate::PipelineSpec,
        plan: PhysicalPlan,
        policy: &TargetPolicy,
    ) -> Result<RunningJob> {
        crate::validate::validate_time_graph_profile(spec, &plan)?;
        let io = spec.graph_io.as_ref().expect("validated graph I/O");
        let admission = self.kernel.prepare_source_admission(plan.pipeline)?;
        let owner = admission.owner();
        // Cold File readers, log/checksum buffers, bounded graph cuts and their
        // temporary clones stay admitted for the actor's complete lifetime.
        let workspace = Arc::new(owner.acquire(
            CreditKind::Reservation,
            512 * 1024 + io.sources.len() * 128 * 1024,
        )?);
        let startup_guard = Arc::new((admission.lifecycle_guard(), workspace.clone()));
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan)?);
        let semantics = digest(&manifest.semantics);
        let checkpoint_policy = spec.checkpoint.clone().expect("time graph checkpoint");
        let directory = spec.checkpoint_dir.clone().expect("time graph directory");
        let max_keys = self.kernel.job_budget().max_state_keys;
        let layout = manifest.clone();
        let retention = checkpoint_policy.retention();
        let opening_guard = startup_guard.clone();
        let (opened, snapshot, generation, pending) = self
            .store
            .run_blocking(move || {
                let _startup = opening_guard;
                sparrow_connectors::check_data_path(Path::new(&directory))?;
                let store = CheckpointStore::open_for_plan_exclusive(
                    &directory, max_keys, retention, &layout, KIND,
                )?;
                let inventory = store.inventory()?;
                if inventory.marker_error.is_some() {
                    return Err(fail("graph generation marker is corrupt"));
                }
                let has_current = inventory.current.is_some() || inventory.current_error.is_some();
                if !has_current && !inventory.generations.is_empty() {
                    return Err(fail("graph history without CURRENT"));
                }
                let snapshot = if has_current {
                    Some(store.recover_pipeline_required()?)
                } else {
                    None
                };
                let pending = log::read(store.dir())?;
                let generation = if let Some(snapshot) = &snapshot {
                    snapshot.check_compatible(&layout)?;
                    if snapshot.plan.semantics != layout.semantics
                        || inventory.state_generation_marker != Some(snapshot.generation)
                    {
                        return Err(fail("graph generation/semantics differs from CURRENT"));
                    }
                    let cut = GraphCut::unwrap(&snapshot.source)?;
                    pending
                        .as_ref()
                        .ok_or_else(|| fail("TIME_PENDING is missing for graph CURRENT"))?
                        .check(snapshot.generation, semantics, &cut)?;
                    snapshot.generation
                } else {
                    let value = inventory
                        .state_generation_marker
                        .map(Ok)
                        .unwrap_or_else(generation)?;
                    if let Some(p) = &pending {
                        if p.cut()?.sequence != 0
                            || p.generation != value
                            || p.semantics != semantics
                        {
                            return Err(fail("graph successor exists without CURRENT"));
                        }
                    }
                    value
                };
                store.activate_state_generation(generation)?;
                Ok((store, snapshot, generation, pending))
            })
            .await?;
        let restored_from = snapshot.as_ref().map(|s| s.checkpoint_id);
        let restored = snapshot
            .as_ref()
            .map(|s| GraphCut::unwrap(&s.source))
            .transpose()?;
        let idle_micros = io.idle_after_ms.map(|n| (n * 1000) as i64);
        if restored
            .as_ref()
            .is_some_and(|c| c.idle_micros != idle_micros)
        {
            return Err(fail("graph idle policy changed; use a new directory"));
        }
        let mut cut = restored.clone().unwrap_or(GraphCut {
            sequence: 0,
            micros: 0,
            observed_micros: 0,
            ingested: 0,
            next_source: 0,
            idle_micros,
            sources: BTreeMap::new(),
            unions: BTreeMap::new(),
            outputs: io.sinks.keys().map(|id| (*id, 1)).collect(),
        });
        if restored.is_none() {
            for (index, stage) in plan.stages.iter().enumerate() {
                if let sparrow_plan::PhysicalStage::UnionAll { operator, .. } = stage {
                    let n = plan
                        .edges
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|e| e.to == index)
                        .count();
                    cut.unions.insert(
                        operator.raw(),
                        UnionProgress {
                            inputs: vec![Progress::default(); n],
                            emitted: Progress::default(),
                        },
                    );
                }
            }
        }
        let diag = IoDiagnostics::new();
        let mut ports = GraphPortDiagnostics {
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
        };
        let mut request = JobRequest::new(plan.clone(), vec![], SharedCapture::disabled())
            .with_observation(diag.observation.clone());
        let mut inputs = Vec::new();
        for (&id, source_spec) in &io.sources {
            let schema = Arc::new(
                plan.stages
                    .iter()
                    .find_map(|stage| match stage {
                        sparrow_plan::PhysicalStage::MemorySource {
                            operator, schema, ..
                        } if operator.raw() == id => Some(schema.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| fail("graph source schema missing"))?,
            );
            let mut single = spec.clone();
            single.graph_io = None;
            single.source = source_spec.clone();
            let contract =
                crate::validate::resolve_file_contract(&single, RecoveryPolicy::Aligned)?;
            let prior = cut.sources.get(&id).cloned();
            if restored.is_some()
                && prior
                    .as_ref()
                    .is_none_or(|p| p.contract != contract_tag(contract))
            {
                return Err(fail("graph File contract/source set changed"));
            }
            let mut config = FileReplayConfig::new(
                source_spec.path.as_ref().expect("File path"),
                schema.as_ref().clone(),
            );
            config.contract = contract;
            config.recovery = RecoveryPolicy::Aligned;
            config.fail_on_decode = true;
            config.format = source_spec.payload_format()?;
            let (schema_copy, owner_copy, row_limit) = (
                schema.clone(),
                owner.clone(),
                self.kernel.ingress_row_limit(),
            );
            let opening_guard = startup_guard.clone();
            let (file, position) = self
                .store
                .run_blocking(move || {
                    let _startup = opening_guard;
                    sparrow_connectors::check_data_path(&config.path)?;
                    let mut file = FileReplaySource::open(&config).map_err(SparrowError::from)?;
                    if let Some(prior) = &prior {
                        file.seek(&prior.position)?;
                        if prior.progress.eof {
                            if !contract.eof_is_terminal() {
                                return Err(fail(
                                    "append-only source cannot restore permanent EOF",
                                ));
                            }
                            let (row, eof) =
                                file.poll_admitted_with_eof(schema_copy, owner_copy, row_limit)?;
                            if row.is_some() || !eof {
                                return Err(fail("persisted source EOF differs from File"));
                            }
                        }
                    }
                    let position = match prior {
                        Some(p) => p.position,
                        None => file.checkpoint_position()?,
                    };
                    Ok((file, position))
                })
                .await?;
            if restored.is_none() {
                cut.sources.insert(
                    id,
                    SourceProgress {
                        position,
                        progress: Progress::default(),
                        last_input: 0,
                        contract: contract_tag(contract),
                    },
                );
            }
            let source_diag = IoDiagnostics::new();
            source_diag.observation.initialize(&owner)?;
            let (tx, rx) = observed::channel(source_spec.inbox_capacity.max(1));
            source_diag.observe_source(&tx);
            ports.sources.insert(id, source_diag.clone());
            request.graph_inputs.insert(
                id.into(),
                GraphInput {
                    events: Some(rx),
                    ..Default::default()
                },
            );
            inputs.push(Input {
                id,
                file: Some(file),
                schema,
                contract,
                tx,
                diag: source_diag,
                time: plan
                    .source_times
                    .iter()
                    .find(|(source, _)| source.raw() == id)
                    .map(|(_, binding)| binding.clone()),
            });
        }
        cut.check_plan(&plan)?;
        if let Some(p) = &pending {
            p.check(generation, semantics, &cut)?;
        }
        let graph = GraphRuntime::new(cut.clone(), &plan, generation, &owner)?;
        let acks = AlignedAcks::default().with_graph_time(graph.clone())?;
        let mut outputs = Vec::new();
        let mut outboxes = Vec::new();
        for (&id, sink_spec) in &io.sinks {
            let sink_diag = IoDiagnostics::new();
            sink_diag.observation.initialize(&owner)?;
            let (tx, rx) = observed::channel(sink_spec.outbox_capacity.max(1));
            sink_diag.observe_sink(&tx);
            ports.sinks.insert(id, sink_diag.clone());
            let counter = Arc::new(InflightCounter::new());
            outboxes.push(counter.clone());
            request.graph_outputs.insert(
                id.into(),
                GraphOutput {
                    capture: SharedCapture::disabled(),
                    live: Some(tx),
                    outbox: Some(counter.clone()),
                },
            );
            outputs.push((sink_spec.clone(), rx, counter, sink_diag));
        }
        let inventory = opened.inventory()?;
        let next_checkpoint = opened.next_checkpoint_id();
        let store = Arc::new(std::sync::Mutex::new(opened));
        let guard: Arc<dyn Send + Sync> = Arc::new((store.clone(), startup_guard));
        let (restore, iot) = snapshot
            .map(|s| (Some(s.windows), s.iot))
            .unwrap_or((None, Vec::new()));
        let request = request
            .with_source_admission(admission)
            .with_clock(sparrow_runtime::RuntimeClock::virtual_clock(
                SharedVirtualClock::new(cut.micros),
            ))
            .with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    plan: manifest.clone(),
                    generation,
                    restore,
                    iot,
                }),
                acks: acks.clone(),
                outbox: Arc::new(InflightCounter::new()),
            });
        let job = self.kernel.submit(request)?;
        let cancel = job.cancellation();
        let mut sinks = Vec::new();
        for (sink_spec, rx, outbox, sink_diag) in outputs {
            let mut single = spec.clone();
            single.graph_io = None;
            single.sink = sink_spec;
            match self.spawn_sink(
                job.memory_owner(),
                &single,
                rx,
                cancel.clone(),
                sink_diag,
                None,
                policy,
                Some(outbox),
            ) {
                Ok(sink) => sinks.push(sink),
                Err(error) => {
                    cancel.cancel();
                    let _ = job.stop().await;
                    for sink in sinks {
                        let _ = sink.await;
                    }
                    return Err(error);
                }
            }
        }
        let sink = self.kernel.handle().spawn(async move {
            for sink in sinks {
                let _ = sink.await;
            }
        });
        let control =
            CheckpointControl::new(checkpoint_policy.clone(), job.attempt.raw(), inventory);
        control.restored_from(restored_from);
        control.state_generation(generation);
        let (cmd, commands) = tokio::sync::mpsc::channel(1);
        let actor = Actor {
            inputs,
            outboxes,
            commands,
            acks,
            graph,
            store,
            manifest,
            owner,
            control: control.clone(),
            diag: diag.clone(),
            cancel: cancel.clone(),
            metrics: self.kernel.metrics.clone(),
            cut,
            generation,
            semantics,
            pending,
            bootstrap: restored_from.is_none(),
            next_checkpoint,
            revision: plan.revision.raw(),
            max_keys,
            row_limit: self.kernel.ingress_row_limit(),
            tick: Duration::from_millis(checkpoint_policy.interval_ms.unwrap()),
            _workspace: workspace,
            _guard: guard,
        };
        let source = self.kernel.handle().spawn(actor.run());
        Ok(RunningJob {
            graph_ports: Some(Arc::new(ports)),
            source_kind: "graph",
            sink_kind: "graph",
            started_at: Instant::now(),
            stable: false,
            revision: plan.revision.raw(),
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
    inputs: Vec<Input>,
    outboxes: Vec<Arc<InflightCounter>>,
    commands: tokio::sync::mpsc::Receiver<AlignedCmd>,
    acks: AlignedAcks,
    graph: Arc<GraphRuntime>,
    store: Arc<std::sync::Mutex<CheckpointStore>>,
    manifest: Arc<CheckpointPlan>,
    owner: Arc<MemoryOwner>,
    control: Arc<CheckpointControl>,
    diag: Arc<IoDiagnostics>,
    cancel: CancellationToken,
    metrics: Arc<sparrow_runtime::RuntimeMetrics>,
    cut: GraphCut,
    generation: [u8; 16],
    semantics: [u8; 32],
    pending: Option<Decision>,
    bootstrap: bool,
    next_checkpoint: u64,
    revision: u64,
    max_keys: usize,
    row_limit: usize,
    tick: Duration,
    _workspace: Arc<MemoryLease>,
    _guard: Arc<dyn Send + Sync>,
}
type Reply = Option<tokio::sync::oneshot::Sender<Result<u64>>>;
struct Read {
    index: usize,
    batch: Option<RowBatch>,
    position: SourcePosition,
    eof: bool,
}
impl Actor {
    async fn poll_one(&mut self, index: usize) -> Result<Option<Read>> {
        if self.cut.sources[&self.inputs[index].id].progress.eof {
            return Ok(None);
        }
        let input = &mut self.inputs[index];
        let mut file = input.file.take().expect("File actor ownership");
        let schema = input.schema.clone();
        let owner = self.owner.clone();
        let limit = self.row_limit;
        let guard = self._guard.clone();
        let (file, result) = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let result = (|| {
                let (batch, eof) = file.poll_admitted_with_eof(schema, owner, limit)?;
                let position = if batch.is_some() || eof {
                    Some(file.checkpoint_position()?)
                } else {
                    None
                };
                Ok::<_, SparrowError>((batch, eof, position))
            })();
            (file, result)
        })
        .await
        .map_err(|_| fail("graph File worker panicked"))?;
        input.file = Some(file);
        let (batch, eof, position) = result?;
        if batch.is_some() || (eof && input.contract.eof_is_terminal()) {
            Ok(Some(Read {
                index,
                batch,
                position: position.unwrap(),
                eof: eof && input.contract.eof_is_terminal(),
            }))
        } else {
            Ok(None)
        }
    }
    async fn poll(&mut self) -> Result<Option<Read>> {
        for offset in 0..self.inputs.len() {
            let index = (self.cut.next_source + offset) % self.inputs.len();
            if let Some(row) = self.poll_one(index).await? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
    fn hash(&self, batch: &RowBatch) -> Result<[u8; 32]> {
        if batch.num_rows() != 1 {
            return Err(fail("graph decision requires one input row"));
        }
        let row = &batch.rows()[0];
        let _credit = self.owner.acquire(
            CreditKind::Reservation,
            row.resident_bytes().saturating_mul(2).saturating_add(128),
        )?;
        let mut bytes = Vec::new();
        for value in &row.values {
            value.encode_value(&mut bytes)?;
        }
        Ok(digest(&bytes))
    }
    fn next_cut(
        &self,
        micros: i64,
        observed_micros: i64,
        input: Option<&Read>,
    ) -> Result<GraphCut> {
        let mut next = self.cut.clone();
        next.sequence = next
            .sequence
            .checked_add(1)
            .ok_or_else(|| fail("graph decision sequence exhausted"))?;
        next.micros = micros;
        next.observed_micros = observed_micros;
        if let Some(read) = input {
            let input = &self.inputs[read.index];
            let source = next.sources.get_mut(&input.id).unwrap();
            source.position = read.position.clone();
            next.next_source = (read.index + 1) % self.inputs.len();
            if let Some(batch) = &read.batch {
                next.ingested = next
                    .ingested
                    .checked_add(1)
                    .ok_or_else(|| fail("graph input count overflow"))?;
                source.last_input = micros;
                source.progress.idle = false;
                if let Some(binding) = &input.time {
                    let i = input
                        .schema
                        .index_of_name(&binding.field)
                        .ok_or_else(|| fail("ET source field missing"))?;
                    let timestamp = batch.rows()[0].values[i]
                        .as_event_time_micros()
                        .filter(|t| *t >= 0)
                        .ok_or_else(|| fail("invalid graph event time"))?;
                    if !binding
                        .max_future_skew_micros
                        .is_some_and(|skew| timestamp > observed_micros.saturating_add(skew))
                    {
                        let wm = timestamp
                            .saturating_sub(binding.out_of_orderness_micros)
                            .max(0);
                        source.progress.watermark =
                            Some(source.progress.watermark.map_or(wm, |old| old.max(wm)));
                    }
                }
            }
            if read.eof {
                source.progress.eof = true;
                source.progress.idle = true;
            }
        }
        for source in next.sources.values_mut() {
            if next
                .idle_micros
                .is_some_and(|idle| micros.saturating_sub(source.last_input) >= idle)
            {
                source.progress.idle = true;
            }
        }
        next.validate()?;
        Ok(next)
    }
    async fn send(&self, index: usize, event: IngressEvent) -> Result<()> {
        tokio::select! {biased;_=self.cancel.cancelled()=>Err(fail("graph attempt cancelled before publication")),
        result=self.inputs[index].tx.send(event)=>result.map_err(|_|fail("graph ingress closed"))}
    }
    async fn save(&self, decision: Decision) -> Result<()> {
        let store = self.store.clone();
        let guard = self._guard.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let store = store.lock().expect("time graph store");
            log::write(store.dir(), &decision)
        })
        .await
        .map_err(|_| fail("graph log worker panicked"))?
    }
    async fn publish(&mut self, decision: &Decision, mut batch: Option<RowBatch>) -> Result<()> {
        let target = decision.cut()?;
        self.graph.begin_round(&target)?;
        if decision.row_hash.is_some() != batch.is_some() {
            return Err(fail("graph decision row presence mismatch"));
        }
        // No per-source source-time generator runs in this profile. Controls
        // are reconstructed from this logged observation, even after a crash.
        for index in 0..self.inputs.len() {
            let id = self.inputs[index].id;
            let mut before = self.cut.sources[&id].progress.clone();
            if Some(id) == decision.selected && batch.is_some() {
                before.idle = false;
            }
            self.send(
                index,
                IngressEvent::Control(StreamControl::ProcessingTime {
                    micros: target.micros,
                }),
            )
            .await?;
            self.send(index, IngressEvent::Control(before.control()))
                .await?;
            if Some(id) == decision.selected {
                if let Some(batch) = batch.take() {
                    self.send(index, IngressEvent::admitted_batch(batch)?)
                        .await?;
                    self.inputs[index].diag.observation.progress(true, 1);
                }
            }
            self.send(
                index,
                IngressEvent::Control(target.sources[&id].progress.control()),
            )
            .await?;
            self.send(
                index,
                IngressEvent::Control(StreamControl::GraphRoundEnd {
                    sequence: target.sequence,
                }),
            )
            .await?;
        }
        self.cut = target;
        Ok(())
    }
    async fn acquire(&mut self) -> Result<(CheckpointAdmission, Reply)> {
        loop {
            if let Ok(AlignedCmd::Checkpoint { admission, reply }) = self.commands.try_recv() {
                return Ok((admission, Some(reply)));
            }
            if let Some(admission) = self.control.try_begin("graph_time_decision")? {
                return Ok((admission, None));
            }
            tokio::select! {biased;_=self.cancel.cancelled()=>return Err(fail("graph decision cancelled")),
            command=self.commands.recv()=>match command {Some(AlignedCmd::Checkpoint {admission,reply})=>return Ok((admission,Some(reply))),None=>return Err(fail("graph commands closed"))},
            _=tokio::time::sleep(Duration::from_millis(1))=>{}}
        }
    }
    async fn checkpoint(&mut self, mut admission: CheckpointAdmission, reply: Reply) -> Result<()> {
        let result = self.commit(&admission).await;
        match &result {
            Ok((id, bytes, elapsed, inventory)) => {
                self.metrics.record_checkpoint(*elapsed, *bytes);
                admission.finish(&Ok(*id), inventory.clone());
            }
            Err(error) => {
                self.metrics.record_checkpoint_abort();
                admission.finish(&Err(error.clone()), None);
            }
        }
        if let Some(reply) = reply {
            let _ = reply.send(result.as_ref().map(|(id, ..)| *id).map_err(Clone::clone));
        }
        result.map(|_| ())
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
        let id = self.next_checkpoint;
        self.next_checkpoint = id
            .checked_add(1)
            .ok_or_else(|| fail("graph checkpoint sequence exhausted"))?;
        admission.phase("aligning");
        let request = self.acks.begin_with_deadline(id, admission.deadline)?;
        let align = async {
            for index in 0..self.inputs.len() {
                self.send(
                    index,
                    IngressEvent::Control(StreamControl::CheckpointBarrier { checkpoint_id: id }),
                )
                .await?;
            }
            request
                .wait_participants(
                    admission
                        .deadline
                        .saturating_duration_since(tokio::time::Instant::now()),
                )
                .await
        };
        let frozen = tokio::select! {biased;_=self.cancel.cancelled()=>return Err(fail("graph cancelled before checkpoint")),
        result=tokio::time::timeout_at(admission.deadline,align)=>result.map_err(|_|fail("graph checkpoint timed out; decision retained"))??};
        self.graph.complete(id, &mut self.cut)?;
        let position = self.cut.wrap()?;
        let ingested = self.cut.ingested;
        let (store, owner, manifest, revision, max_keys) = (
            self.store.clone(),
            self.owner.clone(),
            self.manifest.clone(),
            self.revision,
            self.max_keys,
        );
        admission.phase("committing");
        // A started fsync/rename worker must be joined, including on shutdown.
        let guard = self._guard.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let started = Instant::now();
            let mut store = store.lock().expect("graph store");
            let encoded = PipelineSnapshot::encode_frozen(
                id, &position, ingested, revision, &manifest, frozen, &owner, max_keys,
            )?;
            let bytes = encoded.bytes().len() as u64;
            let id = store.commit_prepared(&encoded)?;
            Ok((id, bytes, started.elapsed(), store.inventory().ok()))
        })
        .await
        .map_err(|_| fail("graph checkpoint worker panicked"))?
    }
    async fn recover(&mut self) -> Result<()> {
        if self.bootstrap {
            let decision = Decision::new(
                self.generation,
                self.semantics,
                &self.cut,
                None,
                None,
                false,
            )?;
            if self.pending.as_ref().is_some_and(|old| old != &decision) {
                return Err(fail("graph bootstrap source changed"));
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
            .ok_or_else(|| fail("graph CURRENT lacks TIME_PENDING"))?;
        if !pending.check(self.generation, self.semantics, &self.cut)? {
            return Ok(());
        }
        let target = pending.cut()?;
        let read = if let Some(id) = pending.selected {
            let index = self
                .inputs
                .iter()
                .position(|s| s.id == id)
                .ok_or_else(|| fail("pending graph source missing"))?;
            let read = self
                .poll_one(index)
                .await?
                .ok_or_else(|| fail("pending graph input/EOF unavailable"))?;
            if read.eof != pending.eof
                || read.position != target.sources[&id].position
                || read.batch.as_ref().map(|b| self.hash(b)).transpose()? != pending.row_hash
            {
                return Err(fail("graph replay input differs from durable decision"));
            }
            Some(read)
        } else {
            None
        };
        let predicted = self.next_cut(target.micros, target.observed_micros, read.as_ref())?;
        if predicted != target {
            return Err(fail(
                "graph replay progress differs from durable observation",
            ));
        }
        let (admission, reply) = self.acquire().await?;
        self.publish(&pending, read.and_then(|r| r.batch)).await?;
        self.checkpoint(admission, reply).await
    }
    async fn drive(&mut self) -> Result<()> {
        self.recover().await?;
        let started = tokio::time::Instant::now();
        let base = self.cut.micros;
        let mut next_tick = started + self.tick;
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            if self.outboxes.iter().any(|o| o.failed() > 0) {
                return Err(fail("required graph HTTP failed; decision retained"));
            }
            let command = self.commands.try_recv().ok();
            let read = if command.is_none() {
                self.poll().await?
            } else {
                None
            };
            if read.is_none() && command.is_none() && tokio::time::Instant::now() < next_tick {
                tokio::select! {biased;_=self.cancel.cancelled()=>return Ok(()),_=tokio::time::sleep_until((tokio::time::Instant::now()+Duration::from_millis(10)).min(next_tick))=>{}}
                continue;
            }
            let (admission, reply) =
                if let Some(AlignedCmd::Checkpoint { admission, reply }) = command {
                    (admission, Some(reply))
                } else {
                    self.acquire().await?
                };
            let micros = base
                .checked_add(
                    i64::try_from(started.elapsed().as_micros())
                        .map_err(|_| fail("graph clock overflow"))?,
                )
                .ok_or_else(|| fail("graph clock overflow"))?;
            let observed = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| fail("invalid wall observation"))?
                    .as_micros(),
            )
            .map_err(|_| fail("wall observation overflow"))?;
            let next = self.next_cut(micros, observed, read.as_ref())?;
            let decision = Decision::new(
                self.generation,
                self.semantics,
                &next,
                read.as_ref().map(|r| self.inputs[r.index].id),
                read.as_ref()
                    .and_then(|r| r.batch.as_ref())
                    .map(|b| self.hash(b))
                    .transpose()?,
                read.as_ref().is_some_and(|r| r.eof),
            )?;
            decision.check(self.generation, self.semantics, &self.cut)?;
            self.save(decision.clone()).await?;
            self.publish(&decision, read.and_then(|r| r.batch)).await?;
            self.checkpoint(admission, reply).await?;
            next_tick = tokio::time::Instant::now() + self.tick;
            self.diag
                .observation
                .health(true, HealthState::Ready, "graph_time_committed", None);
        }
    }
    async fn run(mut self) -> Result<()> {
        let _lifecycle = self.diag.observation.lifecycle(true);
        let _sources = self
            .inputs
            .iter()
            .map(|s| s.diag.observation.lifecycle(true))
            .collect::<Vec<_>>();
        for input in &self.inputs {
            input
                .diag
                .observation
                .health(true, HealthState::Ready, "graph_file_open", None);
        }
        let result = self.drive().await;
        if result.is_err() {
            self.cancel.cancel();
        }
        result
    }
}

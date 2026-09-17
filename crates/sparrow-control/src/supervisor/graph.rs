//! Graph connector composition and File multi-input checkpoint cuts. Sources
//! remain independent tasks; the coordinator never reads a source's cursor
//! concurrently with its read/publish operation.
use super::*;
use crate::file_source::{self, FileProgress};
use sparrow_connectors::{FileContract, TargetPolicy};
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{ErrorCode, OperatorId};
use sparrow_runtime::{GraphInput, GraphOutput};
use std::collections::BTreeMap;

struct Cut {
    id: u64,
    deadline: tokio::time::Instant,
    reply: tokio::sync::oneshot::Sender<Result<SourcePosition>>,
}
enum Input {
    File {
        source: FileReplaySource,
        contract: FileContract,
        tx: observed::Sender<IngressEvent>,
        cuts: Option<tokio::sync::mpsc::Receiver<Cut>>,
        fail_on_decode: bool,
        route_decode: bool,
    },
    Mqtt {
        source: MqttSource,
        tx: observed::Sender<sparrow_model::QueuedRow>,
    },
    Http {
        source: HttpPushSource,
        tx: observed::Sender<sparrow_model::Row>,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    kind: String,
    path: String,
    size: u64,
    fingerprint: u64,
    offset: u64,
    record: u64,
}
fn pack(positions: BTreeMap<u32, SourcePosition>) -> Result<SourcePosition> {
    let mut rows = 0u64;
    let map: Vec<_> = positions
        .into_iter()
        .map(|(id, p)| {
            rows = rows.checked_add(p.record_index).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "graph source row counter overflow",
                )
            })?;
            Ok((
                id,
                Position {
                    kind: p.identity.kind,
                    path: p.identity.path,
                    size: p.identity.size,
                    fingerprint: p.identity.fingerprint,
                    offset: p.offset_bytes,
                    record: p.record_index,
                },
            ))
        })
        .collect::<Result<_>>()?;
    let path = serde_json::to_string(&map)
        .map_err(|e| SparrowError::new(ErrorCode::CodecViolation, e.to_string()))?;
    if path.len() > 64 * 1024 {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "graph source manifest exceeds 64 KiB",
        ));
    }
    Ok(SourcePosition {
        identity: SourceIdentity {
            kind: "file-dag-v1".into(),
            path,
            size: 0,
            fingerprint: 0,
        },
        offset_bytes: 0,
        record_index: rows,
    })
}
fn unpack(source: &SourcePosition) -> Result<BTreeMap<u32, SourcePosition>> {
    if source.identity.kind != "file-dag-v1"
        || source.offset_bytes != 0
        || source.identity.size != 0
        || source.identity.fingerprint != 0
    {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "not a graph File source manifest",
        ));
    }
    let records: Vec<(u32, Position)> =
        serde_json::from_str(&source.identity.path).map_err(|e| {
            SparrowError::new(ErrorCode::CodecViolation, format!("graph positions: {e}"))
        })?;
    if records.is_empty() || records.len() > 16 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "invalid graph source count",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut rows = 0u64;
    for (id, position) in &records {
        if !seen.insert(*id) {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "duplicate graph source position",
            ));
        }
        rows = rows.checked_add(position.record).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::CodecViolation,
                "graph source row counter overflow",
            )
        })?;
    }
    if rows != source.record_index {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "graph source row counters disagree",
        ));
    }
    Ok(records
        .into_iter()
        .map(|(id, p)| {
            (
                id,
                SourcePosition {
                    identity: SourceIdentity {
                        kind: p.kind,
                        path: p.path,
                        size: p.size,
                        fingerprint: p.fingerprint,
                    },
                    offset_bytes: p.offset,
                    record_index: p.record,
                },
            )
        })
        .collect())
}

impl Supervisor {
    pub(super) async fn start_graph(
        &self,
        spec: &crate::spec::PipelineSpec,
        plan: PhysicalPlan,
        policy: &TargetPolicy,
    ) -> Result<RunningJob> {
        let io = spec.graph_io.as_ref().expect("validated graph I/O");
        let aligned = spec.recovery == "aligned";
        let iot_profile = plan.has_iot();
        let admission = self.kernel.prepare_source_admission(plan.pipeline)?;
        let owner = admission.owner();
        let manifest = if aligned {
            Some(Arc::new(CheckpointPlan::from_physical(&plan)?))
        } else {
            None
        };
        let checkpoint_policy = spec.checkpoint.clone().unwrap_or_default();
        let mut restored_positions = BTreeMap::new();
        let mut restore = None;
        let mut restore_iot = Vec::new();
        let mut restored_from = None;
        let mut generation = [0u8; 16];
        let mut store = None;
        if let Some(manifest) = &manifest {
            let directory = spec.checkpoint_dir.clone().expect("validated directory");
            let policy = checkpoint_policy.clone();
            let selected = spec.restore.clone();
            let layout = manifest.clone();
            let max_keys = self.kernel.job_budget().max_state_keys;
            let opened = self
                .store
                .run_blocking(move || {
                    let mut store = if iot_profile {
                        CheckpointStore::open_iot_exclusive(
                            std::path::Path::new(&directory),
                            max_keys,
                            policy.retention(),
                        )?
                    } else {
                        CheckpointStore::open_graph_exclusive(
                            std::path::Path::new(&directory),
                            max_keys,
                            policy.retention(),
                        )?
                    };
                    let inventory = store.inventory()?;
                    let restore = selected.as_ref().is_some_and(|r| r.kind == "checkpoint")
                        || (policy.resume_latest
                            && (inventory.current.is_some() || inventory.current_error.is_some()));
                    if policy.resume_latest && !restore && !inventory.generations.is_empty() {
                        return Err(SparrowError::new(
                            ErrorCode::UnsupportedRestore,
                            "graph history exists without CURRENT",
                        ));
                    }
                    let snap = if restore {
                        let id = selected
                            .as_ref()
                            .and_then(|r| r.snapshot_id.as_deref())
                            .filter(|id| !id.is_empty() && *id != "aligned");
                        let snap = if let Some(id) = id {
                            let id = id.parse().map_err(|_| {
                                SparrowError::new(ErrorCode::InvalidArgument, "invalid snapshot_id")
                            })?;
                            let snap = store.recover_pipeline_id(id)?;
                            store.pin_recovery_point(id)?;
                            snap
                        } else {
                            store.recover_pipeline_required()?
                        };
                        snap.check_compatible(&layout)?;
                        Some(snap)
                    } else {
                        None
                    };
                    Ok((store, snap))
                })
                .await?;
            let (opened, snapshot) = opened;
            if let Some(snapshot) = snapshot {
                restored_positions = unpack(&snapshot.source)?;
                if restored_positions.keys().copied().collect::<Vec<_>>()
                    != io.sources.keys().copied().collect::<Vec<_>>()
                {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "graph restore source set differs",
                    ));
                }
                generation = snapshot.generation;
                restored_from = Some(snapshot.checkpoint_id);
                restore = Some(snapshot.windows);
                restore_iot = snapshot.iot;
            } else {
                use ring::rand::{SecureRandom, SystemRandom};
                SystemRandom::new().fill(&mut generation).map_err(|_| {
                    SparrowError::new(
                        ErrorCode::Internal,
                        "state generation randomness unavailable",
                    )
                })?;
            }
            store = Some(opened);
        }
        let diag = IoDiagnostics::new();
        let acks = AlignedAcks::default();
        let mut request = JobRequest::new(plan.clone(), vec![], SharedCapture::disabled())
            .with_source_admission(admission)
            .with_observation(diag.observation.clone());
        let mut inputs = Vec::new();
        let mut ports = GraphPortDiagnostics {
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
        };
        let mut cut_senders = BTreeMap::new();
        for (id, source_spec) in &io.sources {
            let diag = IoDiagnostics::new();
            diag.observation.initialize(&owner)?;
            ports.sources.insert(*id, diag.clone());
            let schema = plan
                .stages
                .iter()
                .find_map(|s| match s {
                    sparrow_plan::PhysicalStage::MemorySource {
                        operator, schema, ..
                    } if operator.raw() == *id => Some(schema.clone()),
                    _ => None,
                })
                .expect("bound source");
            let mut single = spec.clone();
            single.graph_io = None;
            single.source = source_spec.clone();
            let mut binding = GraphInput::default();
            let input = match source_spec.kind.as_str() {
                "file" | "file_replay" | "replay" => {
                    let contract = crate::validate::resolve_file_contract(
                        &single,
                        RecoveryPolicy::parse(&spec.recovery)?,
                    )?;
                    let mut config = FileReplayConfig::new(
                        source_spec.path.as_ref().expect("validated file path"),
                        schema,
                    );
                    config.contract = contract;
                    config.fail_on_decode = spec.effective_fail_on_decode();
                    config.recovery = RecoveryPolicy::parse(&spec.recovery)?;
                    let position = restored_positions.remove(id);
                    let source = self
                        .store
                        .run_blocking(move || {
                            let mut source =
                                FileReplaySource::open(&config).map_err(SparrowError::from)?;
                            if let Some(position) = position {
                                source.seek(&position)?;
                            }
                            Ok(source)
                        })
                        .await?;
                    let (tx, rx) = observed::channel(source_spec.inbox_capacity);
                    diag.observe_source(&tx);
                    binding.events = Some(rx);
                    let cuts = if aligned {
                        let (tx, rx) = tokio::sync::mpsc::channel(1);
                        cut_senders.insert(*id, tx);
                        Some(rx)
                    } else {
                        None
                    };
                    let route_decode=plan.side_outputs.iter().any(|(i,s)|s.kind==sparrow_plan::graph::SideOutputKind::DecodeError&&matches!(plan.stages[*i],sparrow_plan::PhysicalStage::MemorySource {operator,..} if operator.raw()==*id));
                    Input::File {
                        source,
                        contract,
                        tx,
                        cuts,
                        fail_on_decode: spec.effective_fail_on_decode(),
                        route_decode,
                    }
                }
                "mqtt" => {
                    let mut cfg = mqtt_config(
                        source_spec,
                        schema,
                        self.demo_endpoints().as_ref(),
                        &format!("graph-{id}"),
                    )?;
                    cfg.fail_on_decode = spec.effective_fail_on_decode();
                    cfg.check_inbox_budget(self.kernel.job_budget().queue_bytes)
                        .map_err(SparrowError::from)?;
                    let source = MqttSource::bind(cfg, &self.secrets, policy, diag.clone())
                        .map_err(SparrowError::from)?;
                    let (tx, rx) = observed::channel(source_spec.inbox_capacity);
                    diag.observe_source(&tx);
                    binding.budgeted = Some(rx);
                    Input::Mqtt { source, tx }
                }
                "http_push" => {
                    let cfg = http_push_config(source_spec, schema)?;
                    let source = HttpPushSource::bind(cfg, &self.secrets, policy, diag.clone())
                        .await
                        .map_err(SparrowError::from)?;
                    let (tx, rx) = observed::channel(source_spec.inbox_capacity);
                    diag.observe_source(&tx);
                    binding.live = Some(rx);
                    Input::Http { source, tx }
                }
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::FeatureUnavailable,
                        "unsupported graph source",
                    ))
                }
            };
            request.graph_inputs.insert(OperatorId::new(*id), binding);
            inputs.push((*id, input));
        }
        let mut output_receivers = Vec::new();
        for (id, sink) in &io.sinks {
            let (tx, rx) = observed::channel(sink.outbox_capacity);
            let diag = IoDiagnostics::new();
            diag.observation.initialize(&owner)?;
            diag.observe_sink(&tx);
            ports.sinks.insert(*id, diag);
            let outbox = Arc::new(InflightCounter::new());
            request.graph_outputs.insert(
                OperatorId::new(*id),
                GraphOutput {
                    capture: SharedCapture::disabled(),
                    live: Some(tx),
                    outbox: Some(outbox.clone()),
                },
            );
            output_receivers.push((*id, sink.clone(), rx, outbox));
        }
        if let Some(manifest) = &manifest {
            let opened = store.take().unwrap();
            let state_generation = generation;
            store = Some(
                self.store
                    .run_blocking(move || {
                        opened.activate_state_generation(state_generation)?;
                        Ok(opened)
                    })
                    .await?,
            );
            request = request.with_aligned(AlignedJob {
                restore: None,
                pipeline: Some(PipelineRestore {
                    plan: manifest.clone(),
                    generation,
                    restore,
                    iot: restore_iot,
                }),
                acks: acks.clone(),
                outbox: Arc::new(InflightCounter::new()),
            });
        }
        let checkpoint_inventory = store.as_ref().map(CheckpointStore::inventory).transpose()?;
        let job = self.kernel.submit(request)?;
        let cancel = job.cancellation();
        let mut sinks = Vec::new();
        for (id, sink, rx, outbox) in output_receivers {
            let mut single = spec.clone();
            single.graph_io = None;
            single.sink = sink;
            match self.spawn_sink(
                &single,
                rx,
                cancel.clone(),
                ports.sinks[&id].clone(),
                self.demo_endpoints().as_ref(),
                policy,
                Some(outbox),
            ) {
                Ok(handle) => sinks.push(handle),
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
        let mut tasks = tokio::task::JoinSet::new();
        let max_row_bytes = self.kernel.ingress_row_limit();
        for (id, input) in inputs {
            let child = cancel.clone();
            let diag = ports.sources[&id].clone();
            let owner = owner.clone();
            tasks.spawn_on(
                async move {
                    let result = match input {
                        Input::File {
                            source,
                            contract,
                            tx,
                            cuts,
                            fail_on_decode,
                            route_decode,
                        } => {
                            file_actor(
                                source,
                                contract,
                                tx,
                                cuts,
                                child.clone(),
                                diag,
                                fail_on_decode,
                                route_decode,
                            )
                            .await
                        }
                        Input::Mqtt { source, tx } => source
                            .run_budgeted(tx, child.clone(), owner, max_row_bytes)
                            .await
                            .map_err(SparrowError::from),
                        Input::Http { source, tx } => {
                            source.run(tx, child.clone()).await;
                            Ok(())
                        }
                    };
                    if result.is_err() {
                        child.cancel();
                    }
                    result.map_err(|e| e.at_operator(id.into()))
                },
                &self.kernel.handle(),
            );
        }
        let mut scheduler = None;
        let mut command = None;
        let mut checkpoint = None;
        if let (Some(store), Some(manifest)) = (store, manifest) {
            let inventory = checkpoint_inventory.expect("prepared checkpoint inventory");
            let next = store.next_checkpoint_id();
            let control =
                CheckpointControl::new(checkpoint_policy.clone(), job.attempt.raw(), inventory);
            control.restored_from(restored_from);
            control.state_generation(generation);
            let (cmd, rx) = tokio::sync::mpsc::channel(1);
            if checkpoint_policy.interval_ms.is_some() {
                scheduler = Some(self.kernel.handle().spawn(periodic_checkpoints(
                    cmd.clone(),
                    control.clone(),
                    cancel.clone(),
                )));
            }
            command = Some(cmd);
            checkpoint = Some(control);
            let child = cancel.clone();
            let metrics = self.kernel.metrics.clone();
            let revision = plan.revision.raw();
            let max_keys = self.kernel.job_budget().max_state_keys;
            tasks.spawn_on(
                async move {
                    coordinate(
                        store,
                        manifest,
                        acks,
                        cut_senders,
                        rx,
                        next,
                        revision,
                        max_keys,
                        owner,
                        metrics,
                        child,
                    )
                    .await
                },
                &self.kernel.handle(),
            );
        }
        let child = cancel.clone();
        let source = self.kernel.handle().spawn(async move {
            let mut primary = None;
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    other => {
                        child.cancel();
                        let error = match other {
                            Ok(Err(e)) => e,
                            Err(e) => SparrowError::new(
                                ErrorCode::JobFailed,
                                format!("graph connector task: {e}"),
                            ),
                            _ => unreachable!(),
                        };
                        if primary.is_none() {
                            primary = Some(error);
                        }
                    }
                }
            }
            primary.map_or(Ok(()), Err)
        });
        let kind = if let (Some(cmd), Some(checkpoint)) = (command, checkpoint) {
            RunningKind::Aligned {
                cancel,
                cmd,
                handle: job,
                source,
                sink,
                scheduler,
                checkpoint,
            }
        } else {
            RunningKind::Live {
                handle: job,
                source,
                sink,
            }
        };
        Ok(RunningJob {
            graph_ports: Some(Arc::new(ports)),
            source_kind: "graph",
            sink_kind: "graph",
            started_at: Instant::now(),
            stable: false,
            kind,
            revision: plan.revision.raw(),
            diag,
        })
    }
}

async fn file_actor(
    mut source: FileReplaySource,
    contract: FileContract,
    tx: observed::Sender<IngressEvent>,
    mut cuts: Option<tokio::sync::mpsc::Receiver<Cut>>,
    cancel: CancellationToken,
    diag: Arc<IoDiagnostics>,
    fail_on_decode: bool,
    route_decode: bool,
) -> Result<()> {
    let _lifecycle = diag.observation.lifecycle(true);
    diag.observation
        .health(true, HealthState::Ready, "file_open", None);
    let mut terminal = false;
    let mut next_poll = None;
    loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>return Ok(()),
            cut=async {match &mut cuts {Some(rx)=>rx.recv().await,None=>std::future::pending().await}}=>{
                let Some(cut)=cut else{return Ok(());};
                if cut.reply.is_closed()||tokio::time::Instant::now()>=cut.deadline{continue;}
                let (returned,position)=file_source::checkpoint_file_position(source).await?;source=returned;
                let result=match position {
                    Err(e)=>Err(e),Ok(position)=>{
                        tokio::select!{biased;
                            _=cancel.cancelled()=>return Ok(()),
                            sent=tokio::time::timeout_at(cut.deadline,tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier {checkpoint_id:cut.id})))=>{
                                match sent{Ok(Ok(()))=>Ok(position),_=>Err(SparrowError::new(ErrorCode::ResourceExhausted,"graph barrier injection timed out or input closed"))}
                            }
                        }
                    }
                };
                let _=cut.reply.send(result);
            },
            _=file_source::wait_for_file_poll(next_poll),if !terminal=>{
                let (returned,polls)=file_source::take_file_batch(source).await?;source=returned;
                let at=file_source::observe_file_batch(&diag,&polls);
                let progress=if route_decode {
                    let mut progress=FileProgress::Continue;
                    for poll in polls {
                        if matches!(poll,sparrow_connectors::FilePoll::DecodeError) {
                            diag.decode_errors.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                            tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),sent=tx.send(IngressEvent::DecodeError)=>{sent.map_err(|_|SparrowError::new(ErrorCode::Cancelled,"decode side ingress closed"))?;}}
                        }else{
                            progress=tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),result=file_source::apply_file_poll(poll,contract,&tx,&diag,&mut terminal,None,fail_on_decode,at)=>result?};
                        }
                    }
                    progress
                }else{file_source::apply_file_batch(polls,contract,&tx,&diag,&mut terminal,None,fail_on_decode,at,&cancel).await?};
                match progress {
                    FileProgress::Continue=>next_poll=None,
                    FileProgress::Wait=>next_poll=Some(tokio::time::Instant::now()+file_source::FILE_EOF_POLL),
                    FileProgress::Done=>{
                        if terminal{tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),sent=tx.send(IngressEvent::Control(StreamControl::EndOfInput))=>{sent.map_err(|_|SparrowError::new(ErrorCode::Cancelled,"graph EOF ingress closed"))?;}}}
                        if !terminal||cuts.is_none(){return Ok(());}
                    },
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn coordinate(
    store: CheckpointStore,
    manifest: Arc<CheckpointPlan>,
    acks: AlignedAcks,
    sources: BTreeMap<u32, tokio::sync::mpsc::Sender<Cut>>,
    mut commands: tokio::sync::mpsc::Receiver<AlignedCmd>,
    mut next: u64,
    revision: u64,
    max_keys: usize,
    owner: Arc<sparrow_model::MemoryOwner>,
    metrics: Arc<sparrow_runtime::RuntimeMetrics>,
    cancel: CancellationToken,
) -> Result<()> {
    let store = Arc::new(std::sync::Mutex::new(store));
    loop {
        let command =
            tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),cmd=commands.recv()=>cmd};
        let Some(AlignedCmd::Checkpoint {
            reply,
            mut admission,
        }) = command
        else {
            return Ok(());
        };
        if reply.is_closed() || tokio::time::Instant::now() >= admission.deadline {
            continue;
        }
        let id = next;
        next = next.checked_add(1).ok_or_else(|| {
            SparrowError::new(ErrorCode::BoundExceeded, "checkpoint ID exhausted")
        })?;
        admission.phase("aligning");
        let attempt = async {
            let request = acks.begin_with_deadline(id, admission.deadline)?;
            let mut waiting = Vec::new();
            for (source, tx) in &sources {
                let (reply, rx) = tokio::sync::oneshot::channel();
                tx.send(Cut {
                    id,
                    deadline: admission.deadline,
                    reply,
                })
                .await
                .map_err(|_| {
                    SparrowError::new(
                        ErrorCode::JobFailed,
                        "graph source stopped before checkpoint",
                    )
                })?;
                waiting.push((*source, rx));
            }
            let mut positions = BTreeMap::new();
            for (source, rx) in waiting {
                positions.insert(
                    source,
                    rx.await.map_err(|_| {
                        SparrowError::new(ErrorCode::JobFailed, "graph source cut missing")
                    })??,
                );
            }
            let frozen = request
                .wait_participants(
                    admission
                        .deadline
                        .saturating_duration_since(tokio::time::Instant::now()),
                )
                .await?;
            Ok::<_, SparrowError>((pack(positions)?, frozen))
        };
        let result = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=tokio::time::timeout_at(admission.deadline,attempt)=>result.unwrap_or_else(|_|Err(SparrowError::new(ErrorCode::ResourceExhausted,"graph checkpoint alignment timed out")))};
        let result = match result {
            Err(e) => Err(e),
            Ok((position, frozen)) => {
                admission.phase("committing");
                let store = store.clone();
                let manifest = manifest.clone();
                let owner = owner.clone();
                tokio::task::spawn_blocking(move || {
                    let started = Instant::now();
                    let mut store = store.lock().expect("graph checkpoint store");
                    let bytes = PipelineSnapshot::encode_frozen(
                        id,
                        &position,
                        position.record_index,
                        revision,
                        &manifest,
                        frozen,
                        &owner,
                        max_keys,
                    )?;
                    let length = bytes.bytes().len() as u64;
                    let id = store.commit_prepared(&bytes)?;
                    Ok::<_, SparrowError>((id, length, started.elapsed(), store.inventory().ok()))
                })
                .await
                .map_err(|e| {
                    SparrowError::new(ErrorCode::Internal, format!("graph checkpoint commit: {e}"))
                })?
            }
        };
        match result {
            Ok((id, bytes, duration, inventory)) => {
                metrics.record_checkpoint(duration, bytes);
                admission.finish(&Ok(id), inventory);
                let _ = reply.send(Ok(id));
            }
            Err(e) => {
                metrics.record_checkpoint_abort();
                admission.finish(&Err(e.clone()), None);
                let _ = reply.send(Err(e));
            }
        }
    }
}

//! Explicit lineage operations. Old checkpoint bytes are never edited or
//! relabelled as a new codec. Rebuilds start empty only after explicit approval.
use crate::input_dlq::{audit_reason, hash, invalid};
use crate::{PipelineSpec, Store};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::{collections::BTreeMap, path::Path};
pub(crate) static PREVIEW_WORK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub kind: String,
    pub path: String,
    pub size: u64,
    pub fingerprint: u64,
    pub offset: u64,
    pub records: u64,
}
impl Position {
    pub fn source(&self) -> SourcePosition {
        SourcePosition {
            offset_bytes: self.offset,
            record_index: self.records,
            identity: SourceIdentity {
                kind: self.kind.clone(),
                path: self.path.clone(),
                size: self.size,
                fingerprint: self.fingerprint,
            },
        }
    }
}
impl From<&SourcePosition> for Position {
    fn from(p: &SourcePosition) -> Self {
        Self {
            kind: p.identity.kind.clone(),
            path: p.identity.path.clone(),
            size: p.identity.size,
            fingerprint: p.identity.fingerprint,
            offset: p.offset_bytes,
            records: p.record_index,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayStart {
    pub operation: String,
    pub start: Position,
    pub end: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRequest {
    pub operation: String,
    /// resume: compatible same-lineage state; fork/replay: explicitly empty
    /// state at a selected source cut; dlq_replay: separate finite file lineage.
    pub mode: String,
    pub approve_parent_revision: u64,
    pub checkpoint_id: u64,
    #[serde(default)]
    pub from_checkpoint: Option<u64>,
    pub target: String,
    pub spec: PipelineSpec,
    pub reason: String,
    #[serde(default)]
    pub accept_state_reset: bool,
    #[serde(default)]
    pub accept_duplicate_outputs: bool,
    #[serde(default)]
    pub approve_digest: Option<String>,
    #[serde(default)]
    pub dlq_positions: Vec<u64>,
    #[serde(default)]
    pub corrections: BTreeMap<u64, Value>,
    #[serde(default)]
    pub artifact_directory: Option<String>,
}
pub(crate) struct Prepared {
    pub request: RecoveryRequest,
    pub spec: PipelineSpec,
    pub digest: String,
    pub request_hash: String,
    pub parent: String,
    pub schema: String,
    pub parent_schema: String,
    pub input_provenance: Vec<Value>,
    pub artifact: Option<Vec<u8>>,
}
impl Prepared {
    pub fn view(&self) -> Value {
        json!({"operation":self.request.operation,"mode":self.request.mode,"parent":self.parent,"parent_revision":self.request.approve_parent_revision,"checkpoint_id":self.request.checkpoint_id,"target":self.request.target,"approve_digest":self.digest,"target_spec":self.spec,
        "state":if self.request.mode=="resume"{"preserved_compatible_checkpoint"}else{"empty_state_new_lineage"},
        "duplicates":"possible; new_lineage_does_not_undo_previous_outputs","old_outbox_may_keep_draining":true,"input_provenance":self.input_provenance,
        "external_retention_and_reader_identity":"checked_again_at_source_activation","auto_start":false,"old_checkpoint_modified":false})
    }
}
pub(crate) fn request_hash(r: &RecoveryRequest) -> Result<String> {
    serde_json::to_vec(r)
        .map(|v| hash(&v))
        .map_err(|_| invalid("recovery request encoding"))
}
pub(crate) fn stopped(store: &Store, name: &str) -> Result<()> {
    if store.desired(name)?.status != crate::status::PipelineStatus::Stopped
        || store.actual(name)?.status != crate::status::PipelineStatus::Stopped
    {
        return Err(invalid(
            "recovery operation requires desired and actual stopped; stop and wait first",
        ));
    }
    Ok(())
}
pub(crate) fn scope(spec: &PipelineSpec, plan: &sparrow_plan::PhysicalPlan) -> Result<()> {
    if spec.recovery!="aligned" || !matches!(spec.source.kind.as_str(),"file"|"file_replay"|"replay"|"jetstream") || !spec.source.payload_format()?.is_json()
        || spec.sink.kind!="http" || spec.sink.action.is_some() || !spec.sink.payload_format()?.is_json()
        || spec.graph_io.is_some() || plan.edges.is_some() || !plan.side_outputs.is_empty() || !spec.reference_tables.is_empty()
        || !spec.external_lookups.is_empty() || !plan.source_times.is_empty() || plan.has_iot() || plan.has_analysis() || plan.has_extended_aggs() || plan.has_new_windows()
        || plan.has_external_plugins() || plan.stages.iter().filter(|s|matches!(s,sparrow_plan::PhysicalStage::WindowAgg{..})).count()>1
        || plan.stages.iter().any(|s|matches!(s,sparrow_plan::PhysicalStage::WindowAgg{spec,..} if !matches!(spec.kind,sparrow_model::WindowKind::Count{..}))) {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"lineage operations currently support aligned JSON File/JetStream -> zero state or one legacy Count -> HTTP, no automatic codec conversion"));
    }
    Ok(())
}
fn checkpoint_dir(spec: &PipelineSpec) -> Result<&str> {
    spec.checkpoint_dir
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid("explicit checkpoint directory required"))
}
fn empty_directory(path: &str) -> Result<()> {
    if !Path::new(path).is_absolute() {
        return Err(invalid("absolute storage path required"));
    }
    sparrow_connectors::check_data_path(Path::new(path)).map_err(SparrowError::from)?;
    if Path::new(path)
        .try_exists()
        .map_err(|_| invalid("cannot inspect storage"))?
        && std::fs::read_dir(path)
            .map_err(|_| invalid("cannot inspect storage"))?
            .next()
            .is_some()
    {
        return Err(invalid(
            "new lineage requires fresh storage; do not reuse/copy an old checkpoint directory",
        ));
    }
    Ok(())
}
/// Preview performs the same compatibility checks as execute. It does not
/// create a reader, checkpoint, operation, source cut or desired-state change.
pub(crate) fn prepare(store: &Store, parent: &str, r: &RecoveryRequest) -> Result<Prepared> {
    crate::store::check_name(&r.operation)?;
    crate::store::check_name(&r.target)?;
    audit_reason(&r.reason)?;
    if !matches!(r.mode.as_str(), "resume" | "fork" | "replay" | "dlq_replay")
        || !r.accept_duplicate_outputs
    {
        return Err(invalid(
            "select a recovery mode and acknowledge possible repeated outputs",
        ));
    }
    stopped(store, parent)?;
    let row = store.get_pipeline(parent)?;
    let parent_schema = store.get_stream(&row.spec.stream)?.schema_json;
    if row.latest_revision != r.approve_parent_revision {
        return Err(invalid("parent revision changed; review again"));
    }
    let parent_plan =
        crate::validate::bind_plan_with_store(store, &row.spec, parent, row.latest_revision)?;
    scope(&row.spec, &parent_plan)?;
    let dir = checkpoint_dir(&row.spec)?;
    sparrow_connectors::check_data_path(Path::new(dir)).map_err(SparrowError::from)?;
    let mut history = sparrow_runtime::CheckpointStore::open_readonly(dir)?;
    let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
    let (selected, _credit) = history.recover_pipeline_owned(Some(r.checkpoint_id), &owner)?;
    let layout = sparrow_plan::CheckpointPlan::from_physical(&parent_plan)?;
    selected.check_compatible(&layout)?;
    if row.spec.source.kind == "jetstream" {
        let cfg = row
            .spec
            .source
            .jetstream
            .as_ref()
            .ok_or_else(|| invalid("JetStream config missing"))?;
        let parts: Vec<_> = selected.source.identity.path.split(':').collect();
        if selected.source.identity.kind != "jetstream-v1"
            || parts.len() != 5
            || parts[0] != cfg.namespace
            || parts[1] != cfg.stream
            || parts[3] != cfg.ownership_bucket
            || parts[4] != cfg.consumer
        {
            return Err(invalid("checkpoint belongs to a different source reader"));
        }
    } else {
        use sparrow_io::ReplayableSource;
        let schema = crate::validate::stream_to_schema(&store.get_stream(&row.spec.stream)?)?;
        let mut cfg = sparrow_connectors::FileReplayConfig::new(
            row.spec
                .source
                .path
                .as_ref()
                .ok_or_else(|| invalid("File path missing"))?,
            schema,
        );
        cfg.contract = crate::validate::resolve_file_contract(
            &row.spec,
            sparrow_model::RecoveryPolicy::Aligned,
        )?;
        let mut source =
            sparrow_connectors::FileReplaySource::open(&cfg).map_err(SparrowError::from)?;
        source.seek(&selected.source)?;
    }
    // Current semantics must still describe the selected checkpoint, even on
    // the legacy RCP2 path; a fork is not permission to misdescribe its parent.
    if selected.plan.semantics != layout.semantics {
        return Err(invalid(
            "selected checkpoint does not match the parent computation",
        ));
    }
    if row.spec.source.input_dlq.is_some() {
        use sparrow_io::poison::InputQuarantine;
        crate::input_dlq::configured(store, parent)?.check_start(&selected.source)?;
    }
    let mut target = r.spec.clone();
    crate::input_dlq::separate_storage(&target, r.artifact_directory.as_deref())?;
    if target.source.replay_start.is_some()
        && (r.mode != "resume" || target.source.replay_start != row.spec.source.replay_start)
    {
        return Err(invalid(
            "replay_start is operation-managed, not a user-supplied cursor",
        ));
    }
    let mut artifact = None;
    let mut input_provenance = Vec::new();
    let mut start = Position::from(&selected.source);
    if r.mode == "resume" {
        if r.target != parent
            || r.from_checkpoint.is_some()
            || !r.dlq_positions.is_empty()
            || !r.corrections.is_empty()
            || r.artifact_directory.is_some()
            || target.source != row.spec.source
            || target.stream != row.spec.stream
            || target.sink != row.spec.sink
            || target.checkpoint_dir != row.spec.checkpoint_dir
        {
            return Err(invalid("resume preserves source, schema, required sink and checkpoint directory; use a new lineage for changes"));
        }
        if history.inventory()?.current != Some(r.checkpoint_id) {
            return Err(invalid("resume migration must use the approved CURRENT"));
        }
        target.restore = None;
        target
            .checkpoint
            .get_or_insert_with(Default::default)
            .resume_latest = true;
    } else {
        if !r.accept_state_reset || r.target == parent {
            return Err(invalid(
                "new lineage requires another pipeline name and explicit empty-state approval",
            ));
        }
        empty_directory(checkpoint_dir(&target)?)?;
        if target.checkpoint_dir == row.spec.checkpoint_dir {
            return Err(invalid(
                "new lineage cannot reuse parent checkpoint directory",
            ));
        }
        if let Some(q) = &target.sink.durable_outbox {
            empty_directory(&q.directory)?;
        }
        if let Some(q) = &target.source.input_dlq {
            empty_directory(&q.directory)?;
        }
        target.restore = None;
        target.fail_on_decode = true;
        target
            .checkpoint
            .get_or_insert_with(Default::default)
            .resume_latest = true;
        if r.mode == "dlq_replay" {
            if r.dlq_positions.is_empty()
                || r.dlq_positions.len() > 64
                || r.from_checkpoint.is_some()
                || target.source.kind != "file"
                || target.source.input_dlq.is_some()
            {
                return Err(invalid("DLQ replay requires 1..64 exact positions and a separate File target without automatic input DLQ recursion"));
            }
            let directory = r
                .artifact_directory
                .as_deref()
                .ok_or_else(|| invalid("DLQ replay artifact_directory required"))?;
            if empty_directory(directory).is_err() {
                let existing = store
                    .recovery_operation(&r.operation)?
                    .ok_or_else(|| invalid("DLQ artifact directory is not empty"))?;
                if existing["phase"] != "preparing"
                    || existing["request_hash"] != request_hash(r)?
                    || existing["artifact_directory"] != directory
                {
                    return Err(invalid("DLQ artifacts belong to another operation"));
                }
                check_artifact_directory(directory)?;
            }
            let queue = crate::input_dlq::configured(store, parent)?;
            let mut bytes = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            for position in &r.dlq_positions {
                if *position > selected.source.offset_bytes || !seen.insert(*position) {
                    return Err(invalid("DLQ replay positions must be unique and already covered by the approved checkpoint"));
                }
                let original = queue.body(*position)?;
                let original_sha = hash(&original);
                let payload = if let Some(corrected) = r.corrections.get(position) {
                    if !corrected.is_object() {
                        return Err(invalid("corrected input must be a JSON object"));
                    }
                    serde_json::to_vec(corrected).map_err(|_| invalid("correction encoding"))?
                } else {
                    original
                };
                if payload.len() > 65536
                    || payload.iter().all(u8::is_ascii_whitespace)
                    || payload.contains(&b'\n')
                    || bytes.len().saturating_add(payload.len() + 1) > 1024 * 1024
                {
                    return Err(invalid(
                        "DLQ replay payload exceeds bounded single-line JSON export",
                    ));
                }
                bytes.extend_from_slice(&payload);
                input_provenance.push(json!({"position":position,"original_sha256":original_sha,"replay_sha256":hash(&payload),"corrected":r.corrections.contains_key(position)}));
                bytes.push(b'\n');
            }
            if r.corrections.keys().any(|k| !seen.contains(k)) {
                return Err(invalid("correction selects an unapproved DLQ record"));
            }
            target.source.path = Some(
                Path::new(directory)
                    .join("input.ndjson")
                    .to_string_lossy()
                    .into(),
            );
            target.source.file_contract = Some("append_only".into());
            target.source.format = None;
            target.source.csv = None;
            target.source.protobuf = None;
            // File identity is filled after durable artifact creation, but the
            // byte hash is part of the preview digest and publication record.
            start = Position {
                kind: "file".into(),
                path: target.source.path.clone().unwrap(),
                size: 0,
                fingerprint: 0,
                offset: 0,
                records: 0,
            };
            target.source.replay_start = Some(Box::new(ReplayStart {
                operation: r.operation.clone(),
                start: start.clone(),
                end: Some(bytes.len() as u64),
            }));
            artifact = Some(bytes);
        } else {
            if !r.dlq_positions.is_empty()
                || !r.corrections.is_empty()
                || r.artifact_directory.is_some()
            {
                return Err(invalid("DLQ parameters require dlq_replay mode"));
            }
            let mut wanted = target.source.clone();
            wanted.input_dlq = row.spec.source.input_dlq.clone();
            wanted.replay_start = row.spec.source.replay_start.clone();
            if let (Some(p), Some(n)) = (&row.spec.source.jetstream, &mut wanted.jetstream) {
                if r.target == parent || n.consumer == p.consumer {
                    return Err(invalid(
                        "JetStream new lineage needs a distinct logical consumer",
                    ));
                }
                n.consumer = p.consumer.clone();
            }
            if wanted != row.spec.source {
                return Err(invalid("history/fork must keep the same source binding (except the new JetStream consumer and quarantine directory)"));
            }
            if r.mode == "replay" {
                if let Some(id) = r.from_checkpoint {
                    let (from, _credit) = history.recover_pipeline_owned(Some(id), &owner)?;
                    from.check_compatible(&layout)?;
                    if from.source.identity.kind != selected.source.identity.kind
                        || from.source.identity.path != selected.source.identity.path
                    {
                        return Err(invalid(
                            "replay endpoints belong to different source incarnations",
                        ));
                    }
                    start = Position::from(&from.source);
                } else {
                    start.offset = 0;
                    start.records = 0;
                }
                if start.offset >= selected.source.offset_bytes {
                    return Err(invalid("replay requires a nonempty source range"));
                }
            } else if r.from_checkpoint.is_some() {
                return Err(invalid(
                    "fork starts at checkpoint_id; from_checkpoint belongs to replay",
                ));
            }
            if row.spec.source.input_dlq.is_some() {
                use sparrow_io::poison::InputQuarantine;
                crate::input_dlq::configured(store, parent)?.check_start(&start.source())?;
            }
            if let Some(config) = &target.source.jetstream {
                let (prefix, _) = start
                    .path
                    .rsplit_once(':')
                    .ok_or_else(|| invalid("JetStream reader-bound identity missing"))?;
                start.path = format!("{prefix}:{}", config.consumer);
            } else {
                target.source.file_contract = Some("append_only".into());
            }
            target.source.replay_start = Some(Box::new(ReplayStart {
                operation: r.operation.clone(),
                start: start.clone(),
                end: if r.mode == "replay" {
                    Some(selected.source.offset_bytes)
                } else {
                    None
                },
            }));
        }
    }
    let plan = crate::validate::bind_plan_with_store(
        store,
        &target,
        &r.target,
        if r.mode == "resume" {
            row.latest_revision + 1
        } else {
            1
        },
    )?;
    scope(&target, &plan)?;
    crate::validate::validate_aligned_plan(&target, &plan)?;
    if r.mode == "resume" {
        let layout = sparrow_plan::CheckpointPlan::from_physical(&plan)?;
        selected.check_compatible(&layout)?;
        if selected.plan.semantics != layout.semantics {
            return Err(invalid(
                "resume cannot change computation; use explicit fork/replay rebuild",
            ));
        }
    }
    let schema = store.get_stream(&target.stream)?.schema_json;
    let mut approved = r.clone();
    approved.approve_digest = None;
    let digest=hash(&serde_json::to_vec(&json!({"request":approved,"parent_spec":row.spec,"parent_schema":parent_schema,"source":Position::from(&selected.source),"generation":selected.generation,"schema":schema,"input_provenance":input_provenance,"artifact_sha256":artifact.as_ref().map(|b|hash(b))})).map_err(|_|invalid("preview encoding"))?);
    Ok(Prepared {
        request: r.clone(),
        spec: target,
        digest,
        request_hash: request_hash(r)?,
        parent: parent.into(),
        schema,
        parent_schema,
        input_provenance,
        artifact,
    })
}

pub(crate) fn validate_start(store: &Store, name: &str, spec: &PipelineSpec) -> Result<()> {
    let Some(start) = &spec.source.replay_start else {
        return Ok(());
    };
    let operation = store
        .recovery_operation(&start.operation)?
        .ok_or_else(|| invalid("lineage operation record missing"))?;
    if operation["phase"] != "ready"
        || operation["target"] != name
        || operation["target_spec"]["source"]["replay_start"]
            != serde_json::to_value(start).map_err(|_| invalid("lineage encoding"))?
        || operation["schema"] != store.get_stream(&spec.stream)?.schema_json
    {
        return Err(invalid(
            "lineage, range or input schema differs from its approved operation",
        ));
    }
    if let Some(digest) = operation["artifact_sha256"].as_str() {
        let dir = operation["artifact_directory"]
            .as_str()
            .ok_or_else(|| invalid("lineage artifact directory missing"))?;
        check_artifact_directory(dir)?;
        let bytes = read_artifact(&Path::new(dir).join("input.ndjson"), 1024 * 1024)?;
        if hash(&bytes) != digest {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "lineage input artifact hash differs",
            ));
        }
    }
    Ok(())
}
fn read_artifact(path: &Path, cap: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let m = std::fs::symlink_metadata(path).map_err(|_| invalid("artifact unavailable"))?;
    if !m.is_file() || m.file_type().is_symlink() || m.len() > cap as u64 {
        return Err(invalid("artifact must be a bounded regular file"));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| invalid("artifact open failed"))?
        .take(cap as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("artifact read failed"))?;
    if bytes.len() > cap {
        return Err(invalid("artifact exceeded bound while reading"));
    }
    Ok(bytes)
}
fn check_artifact_directory(dir: &str) -> Result<()> {
    sparrow_connectors::check_data_path(Path::new(dir)).map_err(SparrowError::from)?;
    for e in std::fs::read_dir(dir).map_err(|_| invalid("artifact directory unavailable"))? {
        let e = e.map_err(|_| invalid("artifact directory entry unavailable"))?;
        let m = std::fs::symlink_metadata(e.path())
            .map_err(|_| invalid("artifact metadata unavailable"))?;
        if !matches!(
            e.file_name().to_str(),
            Some("WRITER_LOCK" | "LINEAGE.json" | "input.ndjson" | ".input.tmp" | ".manifest.tmp")
        ) || !m.is_file()
            || m.file_type().is_symlink()
            || m.len() > 1024 * 1024
        {
            return Err(invalid(
                "foreign/link/oversized artifact in replay directory",
            ));
        }
    }
    Ok(())
}
pub(crate) fn write_artifact(p: &mut Prepared) -> Result<()> {
    use sparrow_io::ReplayableSource;
    use std::io::Write;
    let Some(bytes) = p.artifact.as_ref() else {
        return Ok(());
    };
    let dir = p
        .request
        .artifact_directory
        .as_deref()
        .ok_or_else(|| invalid("artifact directory missing"))?;
    std::fs::create_dir_all(dir).map_err(|_| invalid("cannot create replay directory"))?;
    let _lock = sparrow_io::fs_lock::FileLock::acquire(&Path::new(dir).join("WRITER_LOCK"))?;
    check_artifact_directory(dir)?;
    let manifest=serde_json::to_vec(&json!({"operation":p.request.operation,"request_hash":p.request_hash,"body_sha256":hash(bytes)})).map_err(|_|invalid("artifact manifest encoding"))?;
    for (name, tmp, body) in [
        ("LINEAGE.json", ".manifest.tmp", manifest.as_slice()),
        ("input.ndjson", ".input.tmp", bytes.as_slice()),
    ] {
        let path = Path::new(dir).join(name);
        if path
            .try_exists()
            .map_err(|_| invalid("artifact inspection failed"))?
        {
            if read_artifact(&path, 1024 * 1024)? != body {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "existing replay artifact differs; refusing overwrite",
                ));
            }
            continue;
        }
        let temp = Path::new(dir).join(tmp);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut f = options
            .open(&temp)
            .map_err(|_| invalid("artifact create failed"))?;
        f.write_all(body)
            .and_then(|_| f.sync_all())
            .map_err(|_| invalid("artifact durable write failed"))?;
        std::fs::rename(temp, path).map_err(|_| invalid("artifact publish failed"))?;
        std::fs::File::open(dir)
            .and_then(|f| f.sync_all())
            .map_err(|_| invalid("artifact directory sync failed"))?;
    }
    let schema = crate::validate::stream_to_schema(&crate::store::StreamRow {
        name: p.spec.stream.clone(),
        schema_json: p.schema.clone(),
    })?;
    let mut config =
        sparrow_connectors::FileReplayConfig::new(p.spec.source.path.as_ref().unwrap(), schema);
    config.contract = sparrow_connectors::FileContract::AppendOnly;
    let source = sparrow_connectors::FileReplaySource::open(&config).map_err(SparrowError::from)?;
    p.spec.source.replay_start.as_mut().unwrap().start = Position::from(&source.position());
    Ok(())
}

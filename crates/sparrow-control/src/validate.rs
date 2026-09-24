use std::sync::Arc;

use sparrow_connectors::{
    check_data_path, refuse_delivery_name, refuse_durable_recovery, refuse_qos_durable,
    ConnectorCapabilities, FileContract, FileReplayConfig, HttpPushSourceConfig, HttpSinkConfig,
    MqttSinkConfig, MqttSourceConfig, ReplaySupport, SecretResolver, TargetPolicy, TlsConfig,
};
use sparrow_model::{
    DeliveryGuarantee, ErrorCode, PipelineId, RecoveryPolicy, RestoreClaim, Result, RevisionId,
    Schema, SchemaId, SparrowError,
};
use sparrow_plan::catalog::schema_from_fields;
use sparrow_plan::{bind_graph, physicalize, Catalog, PhysicalPlan, PlanOptions};
use sparrow_sql::bind_sql;

use crate::spec::{PipelineSpec, SinkSpec, SourceSpec, StreamSpec};
use crate::store::{Store, StreamRow};

#[derive(Clone, Debug)]
pub struct DemoEndpoints {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub http_url: String,
    pub http_port: u16,
}

#[derive(Clone, Debug)]
pub struct ExplainReport {
    pub accepted: bool,
    pub stages: Vec<String>,
    pub fused: bool,
    pub mailbox_count: usize,
    pub delivery: &'static str,
    pub recovery: &'static str,
    pub replay: &'static str,
    pub honesty: &'static str,
    pub physical: Vec<String>,
    pub fusion: String,
    pub time: String,
    pub state: String,
    pub guarantee: String,
    pub experimental: bool,
}

pub const HONESTY: &str =
    "V1 default is live_best_effort + restart_fresh (recovery=none). aligned is the production ReplayableSource checkpoint path (not exactly-once; recover from verified committed manifests only). MQTT replay remains unsupported; MQTT cannot pretend durable restore.";

pub fn honesty_json() -> serde_json::Value {
    serde_json::json!({
        "delivery": DeliveryGuarantee::LiveBestEffort.as_str(),
        "recovery": RecoveryPolicy::RestartFresh.as_str(),
        "recovery_aligned": RecoveryPolicy::Aligned.as_str(),
        "replay_mqtt": ReplaySupport::Unsupported.as_str(),
        "replay_file": ReplaySupport::Replayable.as_str(),
        "exactly_once": "rejected",
        "honesty": HONESTY,
    })
}

pub fn stream_schema(name: &str, spec: &StreamSpec) -> Result<Schema> {
    let id = SchemaId::new(fnv(name));
    schema_from_fields(&spec.fields, id)
}

pub fn stream_to_schema(row: &StreamRow) -> Result<Schema> {
    let spec: StreamSpec = serde_json::from_str(&row.schema_json)
        .map_err(|e| SparrowError::new(ErrorCode::InvalidSchema, format!("stream schema: {e}")))?;
    stream_schema(&row.name, &spec)
}

pub fn binder_catalog(store: &Store) -> Result<Catalog> {
    let mut cat = Catalog::new();
    for row in store.list_streams()? {
        cat.insert(row.name.clone(), stream_to_schema(&row)?);
    }
    Ok(cat)
}

/// Resolve immutable dependencies before binding any production pipeline.
/// Never insert a table's mutable `latest` schema into this catalog.
pub fn bind_plan_with_store(
    store: &Store,
    spec: &PipelineSpec,
    name: &str,
    revision: u64,
) -> Result<PhysicalPlan> {
    spec.basic_check()?;
    let references = store.reference_bindings(spec)?;
    let mut catalog = binder_catalog(store)?;
    for table in &references {
        if catalog.get(&table.name).is_ok()
            || spec.graph.as_ref().is_some_and(|graph| graph.catalog.iter().any(|t| t.name == table.name)) {
            return Err(SparrowError::new(ErrorCode::InvalidSchema,
                "managed reference table schema must not shadow a stream or inline graph catalog"));
        }
        catalog.insert(table.name.clone(), table.table.schema(&table.name)?);
    }
    let plan = bind_plan(spec, &catalog, name, revision)?;
    let mut used = std::collections::BTreeSet::new();
    for stage in &plan.stages {
        let sparrow_plan::PhysicalStage::Lookup { spec: lookup, input, .. } = stage else { continue };
        let table = references.iter().find(|t| t.name == lookup.table).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument,
                format!("Lookup '{}' requires an explicit immutable reference_tables binding", lookup.table))
        })?;
        if lookup.temporal || lookup.as_of_field.is_some() {
            return Err(SparrowError::new(ErrorCode::FeatureUnavailable,
                "managed temporal Lookup requires a pinned version timeline and is not yet enabled"));
        }
        if lookup.table_keys != table.table.keys {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,
                "Lookup table keys must exactly match the immutable table key order"));
        }
        let schema = table.table.schema(&table.name)?;
        for (stream_key, table_key) in lookup.stream_keys.iter().zip(&lookup.table_keys) {
            let source = input.field_by_name(stream_key).ok_or_else(||
                SparrowError::new(ErrorCode::InvalidSchema, "Lookup stream key is absent"))?;
            let target = schema.field_by_name(table_key).ok_or_else(||
                SparrowError::new(ErrorCode::InvalidSchema, "Lookup table key is absent"))?;
            if source.data_type != target.data_type {
                return Err(SparrowError::new(ErrorCode::TypeMismatch,
                    "Lookup stream and table key types must match without coercion"));
            }
        }
        used.insert(table.name.as_str());
    }
    if used.len() != spec.reference_tables.len() {
        return Err(SparrowError::new(ErrorCode::InvalidArgument,
            "reference_tables contains a binding not used by any Lookup"));
    }
    Ok(plan)
}

pub fn bind_plan(
    spec: &PipelineSpec,
    catalog: &Catalog,
    name: &str,
    revision: u64,
) -> Result<PhysicalPlan> {
    spec.basic_check()?;
    spec.check_delivery()?;
    let pipeline = PipelineId::new(fnv(name) as u64);
    let rev = RevisionId::new(revision);
    let mut bound = if let Some(sql) = &spec.sql {
        bind_sql(sql, catalog, pipeline, rev)?
    } else if let Some(graph) = &spec.graph {
        bind_graph(graph, catalog)?
    } else {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "exactly one of sql or graph",
        ));
    };
    // Authoring GraphSpec is preserved in the catalog; execution identity is
    // the named pipeline and its committed revision, not client-supplied IDs.
    bound.pipeline=pipeline;bound.revision=rev;
    let plan = physicalize(&bound, &PlanOptions { fuse: true });
    if let Some(io) = &spec.graph_io {
        let sources: std::collections::BTreeSet<_> = plan.stages.iter().filter_map(|s|match s { sparrow_plan::PhysicalStage::MemorySource { operator,.. } => Some(operator.raw()),_=>None }).collect();
        let sinks: std::collections::BTreeSet<_> = plan.stages.iter().filter_map(|s|match s { sparrow_plan::PhysicalStage::CaptureSink { operator,.. } | sparrow_plan::PhysicalStage::BestEffortSink { operator,.. } => Some(operator.raw()),_=>None }).collect();
        if plan.edges.is_none() || sources != io.sources.keys().copied().collect() || sinks != io.sinks.keys().copied().collect() {
            return Err(SparrowError::new(ErrorCode::InvalidArgument,"graph_io must bind every graph source/sink ID exactly once"));
        }
        for (stage,side) in &plan.side_outputs {
            if side.kind==sparrow_plan::graph::SideOutputKind::DecodeError {
                let sparrow_plan::PhysicalStage::MemorySource {operator,..}=&plan.stages[*stage] else{unreachable!()};
                if spec.effective_fail_on_decode() || !matches!(io.sources[&operator.raw()].kind.as_str(),"file"|"file_replay"|"replay") {
                    return Err(SparrowError::new(ErrorCode::FeatureUnavailable,"decode-error side outputs currently require File input and fail_on_decode=false"));
                }
            }
        }
    } else if plan.edges.is_some() {
        return Err(SparrowError::new(ErrorCode::InvalidArgument,"DAG pipelines require explicit graph_io source/sink bindings"));
    }
    Ok(plan)
}

pub fn replay_label_for_source(kind: &str) -> &'static str {
    match kind {
        "file" | "file_replay" | "replay" => "replayable",
        "jetstream" if cfg!(feature="jetstream") => "replayable",
        "mqtt" | "mqtt_source" | "http_push" | "http" => "unsupported",
        _ => sparrow_plan::REPLAY_UNBOUND,
    }
}

pub fn replay_label_for_spec(spec: &PipelineSpec) -> &'static str {
    spec.graph_io.as_ref().map_or_else(
        || replay_label_for_source(&spec.source.kind),
        |io| {
            if io.sources.values().all(|source| {
                matches!(source.kind.as_str(), "file" | "file_replay" | "replay")
            }) {
                "replayable"
            } else {
                "unsupported"
            }
        },
    )
}

pub fn explain_plan(plan: &PhysicalPlan) -> ExplainReport {
    explain_plan_with(plan, RecoveryPolicy::RestartFresh, sparrow_plan::REPLAY_UNBOUND)
}

pub fn explain_plan_with(
    plan: &PhysicalPlan,
    recovery: RecoveryPolicy,
    replay: &'static str,
) -> ExplainReport {
    let g = sparrow_plan::GraphExplain::from_plan_with(plan, recovery, replay);
    ExplainReport {
        accepted: g.accepted,
        stages: g.stages,
        fused: g.fused,
        mailbox_count: g.mailbox_count,
        delivery: g.delivery,
        recovery: g.recovery,
        replay: g.replay,
        honesty: g.honesty,
        physical: g.physical,
        fusion: g.fusion,
        time: g.time,
        state: g.state,
        guarantee: g.guarantee,
        experimental: g.experimental,
    }
}

pub fn store_policy(store: &Store, demo: Option<&DemoEndpoints>) -> Result<TargetPolicy> {
    let mut p = TargetPolicy::deny_all();
    for (host, port) in store.allowlist()? {
        p = p.with_allow(host, port);
    }
    if let Some(d) = demo {
        p = p
            .with_allow(d.mqtt_host.clone(), d.mqtt_port)
            .with_allow("127.0.0.1", d.http_port);
    }
    Ok(p)
}

pub struct StoreSecrets {
    store: Arc<Store>,
}

impl StoreSecrets {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

impl SecretResolver for StoreSecrets {
    fn resolve(&self, name: &str) -> sparrow_connectors::Result<String> {
        if let Some(env_name) = name.strip_prefix("env:") {
            return std::env::var(env_name).map_err(|_| {
                sparrow_connectors::ConnectorError::new(
                    ErrorCode::SecretMissing,
                    format!("environment secret `{env_name}` is not set"),
                )
            });
        }
        match self.store.get_secret(name) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(sparrow_connectors::ConnectorError::new(
                ErrorCode::SecretMissing,
                format!("secret `{name}` is not configured"),
            )),
            Err(e) => Err(sparrow_connectors::ConnectorError::new(e.code, e.message)),
        }
    }
}

pub fn validate_io(
    spec: &PipelineSpec,
    schema: &Schema,
    secrets: &dyn SecretResolver,
    policy: &TargetPolicy,
    demo: Option<&DemoEndpoints>,
) -> Result<()> {
    spec.check_delivery()?;
    if let Some(io) = &spec.graph_io {
        for (operator, source) in &io.sources {
            validate_source_io(spec, source, schema, secrets, policy, demo)
                .map_err(|e| e.at_operator((*operator).into()))?;
        }
        for (operator, sink) in &io.sinks {
            validate_sink_io(sink, secrets, policy, demo)
                .map_err(|e| e.at_operator((*operator).into()))?;
        }
        return Ok(());
    }

    validate_source_io(spec, &spec.source, schema, secrets, policy, demo)?;
    validate_sink_io(&spec.sink, secrets, policy, demo)
}

fn validate_source_io(
    spec: &PipelineSpec,
    source: &SourceSpec,
    schema: &Schema,
    secrets: &dyn SecretResolver,
    policy: &TargetPolicy,
    demo: Option<&DemoEndpoints>,
) -> Result<()> {
    match source.kind.as_str() {
        #[cfg(feature="jetstream")]
        "jetstream" => {
            let config=source.jetstream.as_ref().ok_or_else(||SparrowError::new(ErrorCode::InvalidArgument,"JetStream config required"))?;
            config.reader().validate()?;
            config.connection().validate(policy)?;
            check_data_path(std::path::Path::new(spec.checkpoint_dir.as_deref().ok_or_else(||SparrowError::new(ErrorCode::InvalidArgument,"checkpoint_dir required"))?)).map_err(io)?;
            if let Some(reference)=&config.token_secret { secrets.resolve(reference).map_err(io)?; }
        }
        "mqtt" => {
            refuse_durable_recovery(&spec.restore_claim()?).map_err(io)?;
            let mqtt = mqtt_config(source, schema.clone(), demo, "validate")?;
            mqtt.validate(secrets, policy).map_err(io)?;
        }
        "http_push" => {
            refuse_durable_recovery(&spec.restore_claim()?).map_err(io)?;
            let push = http_push_config(source, schema.clone())?;
            push.validate(secrets, policy).map_err(io)?;
        }
        "file" | "file_replay" | "replay" => {
            let path = source.path.clone().ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "file source requires source.path",
                )
            })?;
            let recovery = RecoveryPolicy::parse(&spec.recovery)?;
            sparrow_connectors::check_data_path(std::path::Path::new(&path)).map_err(io)?;
            if let Some(dir) = &spec.checkpoint_dir {
                check_data_path(std::path::Path::new(dir)).map_err(io)?;
            }
            let mut cfg = FileReplayConfig::new(path, schema.clone());
            cfg.restore = spec.restore_claim()?;
            cfg.recovery = recovery;
            let mut source_spec = spec.clone();
            source_spec.graph_io = None;
            source_spec.source = source.clone();
            cfg.contract = resolve_file_contract(&source_spec, recovery)?;
            cfg.validate().map_err(io)?;
        }
        other => {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("source kind `{other}` is not supported (mqtt|http_push|file)"),
            ));
        }
    }
    Ok(())
}

fn validate_sink_io(
    sink: &SinkSpec,
    secrets: &dyn SecretResolver,
    policy: &TargetPolicy,
    demo: Option<&DemoEndpoints>,
) -> Result<()> {
    match sink.kind.as_str() {
        "http" => {
            let http = http_config(sink, demo)?;
            http.validate(secrets, policy).map_err(io)?;
        }
        "log" => {}
        "mqtt" => {
            let mqtt = mqtt_sink_config(sink, demo)?;
            mqtt.validate(secrets, policy).map_err(io)?;
        }
        other => {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("sink kind `{other}` is not supported (http|log|mqtt)"),
            ));
        }
    }
    Ok(())
}

/// Validate graph connector settings against the schemas of the physical
/// source/sink chains.  The legacy [`validate_io`] entry point is retained for
/// callers that only have the pipeline stream schema; Server validation uses
/// this plan-aware variant so a graph source bound to another catalog table is
/// not checked with the first/top-level stream schema.
pub fn validate_io_with_plan(
    spec: &PipelineSpec,
    schema: &Schema,
    plan: &PhysicalPlan,
    secrets: &dyn SecretResolver,
    policy: &TargetPolicy,
    demo: Option<&DemoEndpoints>,
) -> Result<()> {
    spec.check_delivery()?;
    let Some(io) = &spec.graph_io else {
        return validate_io(spec, schema, secrets, policy, demo);
    };

    for (operator, source) in &io.sources {
        let actual = graph_endpoint_schema(plan, *operator, true)?;
        validate_source_io(spec, source, &actual, secrets, policy, demo)
            .map_err(|e| e.at_operator((*operator).into()))?;
    }
    for (operator, sink) in &io.sinks {
        let _actual = graph_endpoint_schema(plan, *operator, false)?;
        validate_sink_io(sink, secrets, policy, demo)
            .map_err(|e| e.at_operator((*operator).into()))?;
    }
    Ok(())
}

pub(crate) fn graph_endpoint_schema(plan: &PhysicalPlan, operator: u32, source: bool) -> Result<Schema> {
    plan.stages
        .iter()
        .find_map(|stage| match stage {
            sparrow_plan::PhysicalStage::MemorySource { operator: id, schema, .. }
                if source && id.raw() == operator => Some(schema.clone()),
            sparrow_plan::PhysicalStage::CaptureSink { operator: id, schema, .. }
                if !source && id.raw() == operator => Some(schema.clone()),
            sparrow_plan::PhysicalStage::BestEffortSink { operator: id, schema, .. }
                if !source && id.raw() == operator => Some(schema.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("graph I/O operator {operator} has no physical endpoint schema"),
            )
        })
}

pub fn mqtt_config(
    source: &SourceSpec,
    schema: Schema,
    demo: Option<&DemoEndpoints>,
    instance_id: &str,
) -> Result<MqttSourceConfig> {
    refuse_qos_durable(source.qos).map_err(io)?;
    if source.skip_verify {
        return Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            "TLS skip_verify is rejected",
        ));
    }
    let (host, port) = if source.use_demo_io {
        let d = demo.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.use_demo_io requires sparrow-server --demo-io",
            )
        })?;
        (d.mqtt_host.clone(), d.mqtt_port)
    } else {
        let host = source.host.clone().ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "MQTT host is required")
        })?;
        let port = source.port.ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "MQTT port is required")
        })?;
        (host, port)
    };
    let mut cfg = MqttSourceConfig::demo(host, port, schema);
    cfg.topic = source.topic.clone();
    cfg.client_id = source.client_id.clone().unwrap_or_else(|| {
        format!(
            "sparrow-{instance_id}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    });
    cfg.qos = source.qos;
    cfg.clean_session = source.clean_session;
    cfg.username_secret = source.username_secret.clone();
    cfg.password_secret = source.password_secret.clone();
    let tls_enabled = source.tls && !source.use_demo_io;
    if (source.username_secret.is_some() || source.password_secret.is_some()) && !tls_enabled {
        return Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            "MQTT username/password require TLS (refusing plaintext credentials)",
        ));
    }
    cfg.tls = TlsConfig {
        enabled: tls_enabled,
        skip_verify: source.skip_verify,
    };
    cfg.inbox_capacity = source.inbox_capacity;
    cfg.tcp_quickack = source.tcp_quickack.unwrap_or(false);
    cfg.inbox_bytes = Some(source.inbox_bytes.unwrap_or(256 * 1024));
    if let Some(ms) = source.inbox_wait_ms {
        cfg.inbox_wait_timeout = std::time::Duration::from_millis(ms);
    }
    cfg.restore = RestoreClaim::None;
    Ok(cfg)
}

pub fn http_config(sink: &SinkSpec, demo: Option<&DemoEndpoints>) -> Result<HttpSinkConfig> {
    if sink.skip_verify {
        return Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            "TLS skip_verify is rejected",
        ));
    }
    let url = if sink.use_demo_io {
        let d = demo.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "sink.use_demo_io requires sparrow-server --demo-io",
            )
        })?;
        d.http_url.clone()
    } else {
        sink.url.clone().ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "HTTP sink url is required")
        })?
    };
    if sink.header_secret.is_some() && !url.starts_with("https://") {
        return Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            "HTTP header_secret requires an https:// URL (refusing plaintext credentials)",
        ));
    }
    let mut cfg = HttpSinkConfig::demo(url.clone());
    cfg.outbox_capacity = sink.outbox_capacity;
    if let Some(n) = sink.batch_rows { cfg.batch_rows = n; }
    if let Some(n) = sink.batch_bytes { cfg.batch_bytes = n; }
    if let Some(ms) = sink.linger_ms { cfg.linger = std::time::Duration::from_millis(ms); }
    if let Some(n) = sink.max_inflight { cfg.max_inflight = n; }
    cfg.header_secret = sink.header_secret.clone();
    cfg.tls = TlsConfig {
        enabled: (sink.tls && url.starts_with("https://")) || sink.header_secret.is_some(),
        skip_verify: sink.skip_verify,
    };
    cfg.restore = RestoreClaim::None;
    Ok(cfg)
}

pub fn http_push_config(source: &SourceSpec, schema: Schema) -> Result<HttpPushSourceConfig> {
    let mut cfg = HttpPushSourceConfig::demo(schema);
    if let Some(bind) = &source.bind {
        cfg.bind = bind.clone();
    }
    if let Some(path) = &source.path {
        cfg.path = path.clone();
    }
    cfg.inbox_capacity = source.inbox_capacity;
    cfg.restore = RestoreClaim::None;
    Ok(cfg)
}

pub fn mqtt_sink_config(sink: &SinkSpec, demo: Option<&DemoEndpoints>) -> Result<MqttSinkConfig> {
    let (host, port) = if sink.use_demo_io {
        let d = demo.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "sink.use_demo_io requires sparrow-server --demo-io",
            )
        })?;
        (d.mqtt_host.clone(), d.mqtt_port)
    } else {
        let host = sink.host.clone().ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "MQTT sink host is required")
        })?;
        let port = sink.port.ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "MQTT sink port is required")
        })?;
        (host, port)
    };
    let mut cfg = MqttSinkConfig::demo(host, port);
    if let Some(t) = &sink.topic {
        cfg.topic = t.clone();
    }
    if let Some(id) = &sink.client_id {
        cfg.client_id = id.clone();
    }
    cfg.qos = sink.qos;
    cfg.clean_session = sink.clean_session;
    cfg.outbox_capacity = sink.outbox_capacity;
    cfg.tls = TlsConfig {
        enabled: sink.tls && !sink.use_demo_io,
        skip_verify: false,
    };
    cfg.restore = RestoreClaim::None;
    Ok(cfg)
}

/// Resolve the file growth / EOF contract (N5).
///
/// Explicit `source.file_contract` wins. Unspecified defaults to
/// [`FileContract::AppendOnly`]: EOF is poll-only (no terminal watermark,
/// job stays up for growing files). Finite fixtures that must emit last
/// ET windows set `sealed` / `immutable`.
pub fn resolve_file_contract(
    spec: &PipelineSpec,
    _recovery: RecoveryPolicy,
) -> Result<FileContract> {
    match spec
        .source
        .file_contract
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => FileContract::parse(raw).map_err(io),
        None => Ok(FileContract::AppendOnly),
    }
}

/// Decode the canonical table digest stored in a pipeline binding.  The
/// control plane is the only layer that turns the textual SHA-256 into the
/// typed checkpoint dependency identity; runtime CRC is deliberately supplied
/// separately by the verified, detached table snapshot.
pub(crate) fn decode_reference_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "reference table binding requires a 64-character SHA-256",
        ));
    }
    let mut out = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = (pair[0] as char).to_digit(16).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "reference table SHA-256 is not hex")
        })?;
        let low = (pair[1] as char).to_digit(16).ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "reference table SHA-256 is not hex")
        })?;
        out[index] = ((high << 4) | low) as u8;
    }
    Ok(out)
}

/// Build only an eligibility/shape dependency list from the persisted spec.
/// `runtime_crc32=0` is intentional here: no placeholder is ever passed to a
/// real aligned checkpoint or stored in a manifest; startup replaces it with
/// `ReferenceTable::verified_dependency()` from the loaded table.
pub(crate) fn reference_dependency_shape(
    spec: &PipelineSpec,
) -> Result<Vec<sparrow_plan::ReferenceTableDependency>> {
    spec.reference_tables
        .iter()
        .map(|(name, binding)| {
            Ok(sparrow_plan::ReferenceTableDependency {
                name: name.clone(),
                revision: binding.revision,
                canonical_sha256: decode_reference_sha256(&binding.sha256)?,
                runtime_crc32: 0,
            })
        })
        .collect()
}

/// Check that actual detached tables and the spec describe exactly the same
/// immutable dependency set.  This catches a changed revision, digest,
/// missing table, or duplicate name before constructing the checkpoint layout
/// used for restore.  Runtime CRC is intentionally allowed to be zero: the
/// actual value is obtained from the verified table snapshot, not inferred
/// from this identity-only check.
pub(crate) fn validate_reference_dependencies(
    spec: &PipelineSpec,
    dependencies: &[sparrow_plan::ReferenceTableDependency],
) -> Result<()> {
    if dependencies.len() != spec.reference_tables.len() {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "verified reference dependency count differs from the pipeline binding",
        ));
    }
    let mut ordered = dependencies.to_vec();
    ordered.sort_by(|a, b| a.name.cmp(&b.name));
    if ordered.windows(2).any(|pair| pair[0].name == pair[1].name) {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "verified reference dependency names are not unique",
        ));
    }
    for dependency in ordered {
        let binding = spec.reference_tables.get(&dependency.name).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("verified reference table '{}' is not bound by the pipeline", dependency.name),
            )
        })?;
        let canonical = decode_reference_sha256(&binding.sha256)?;
        if dependency.revision != binding.revision || dependency.canonical_sha256 != canonical {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "verified reference table '{}' does not match its bound revision/digest",
                    dependency.name
                ),
            ));
        }
    }
    Ok(())
}

/// Construct the exact participant plan for an aligned attempt.  For B2-A
/// this is fed the real runtime CRCs; the eligibility-only caller passes the
/// spec-derived shape with CRC=0.
pub(crate) fn checkpoint_plan_with_references(
    spec: &PipelineSpec,
    plan: &PhysicalPlan,
    dependencies: &[sparrow_plan::ReferenceTableDependency],
) -> Result<sparrow_plan::CheckpointPlan> {
    if spec.reference_tables.is_empty() {
        if !dependencies.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "reference dependencies supplied for a pipeline without bindings",
            ));
        }
        return sparrow_plan::CheckpointPlan::from_physical(plan);
    }
    validate_reference_dependencies(spec, dependencies)?;
    sparrow_plan::CheckpointPlan::from_physical_with_references(plan, dependencies.to_vec())
}

fn validate_reference_checkpoint_profile(
    spec: &PipelineSpec,
    plan: &PhysicalPlan,
) -> Result<()> {
    if spec.reference_tables.is_empty() {
        return Ok(());
    }
    if spec.checkpoint_dir.as_deref().is_none_or(str::is_empty)
        || spec.sink.kind != "http"
        || !plan.side_outputs.is_empty()
        || !plan.source_times.is_empty()
    {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "reference-table checkpoint profile requires an explicit checkpoint_dir, required HTTP output and no side/time outputs",
        ));
    }

    let graph = spec.graph_io.is_some() || plan.edges.is_some();
    let source_kind = if graph {
        let io = spec.graph_io.as_ref().ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "reference-table graph checkpoint requires explicit graph_io",
            )
        })?;
        if plan.edges.is_none()
            || io.sources.is_empty()
            || io.sources.values().any(|source| {
                !matches!(source.kind.as_str(), "file" | "file_replay" | "replay")
            })
            || io.sinks.is_empty()
            || io.sinks.values().any(|sink| sink.kind != "http")
            || plan.stages.iter().any(|stage| {
                matches!(
                    stage,
                    sparrow_plan::PhysicalStage::BestEffortSink { .. }
                )
            })
            || plan
                .edges
                .as_ref()
                .is_some_and(|edges| edges.iter().any(|edge| edge.best_effort))
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "reference-table graph checkpoint requires replayable File sources and required HTTP sinks without lossy edges",
            ));
        }
        "file-dag-v1"
    } else {
        if !matches!(
            spec.source.kind.as_str(),
            "file" | "file_replay" | "replay" | "jetstream"
        ) {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "reference-table checkpoint requires a replayable File or JetStream source",
            ));
        }
        if !matches!(
            plan.stages.first(),
            Some(sparrow_plan::PhysicalStage::MemorySource { .. })
        ) || !matches!(
            plan.stages.last(),
            Some(sparrow_plan::PhysicalStage::CaptureSink { .. })
        ) {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "reference-table checkpoint requires one leading source and one trailing required sink",
            ));
        }
        if spec.source.kind == "jetstream" {
            "jetstream-v1"
        } else {
            "file"
        }
    };

    // The plan/runtime pair owns the supported state matrix and profile
    // number.  This keeps control from accidentally opening a new IoT kind
    // (for example Hysteresis) through the old v8 stateless path.
    let dependencies = reference_dependency_shape(spec)?;
    let layout = sparrow_plan::CheckpointPlan::from_physical_with_references(
        plan,
        dependencies,
    )?;
    let version = sparrow_runtime::pipeline_checkpoint::snapshot_version_for(&layout, source_kind)?;
    let expected = if graph {
        11
    } else if source_kind == "jetstream-v1" {
        10
    } else if layout.states.is_empty() {
        8
    } else {
        9
    };
    if version != expected {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            format!(
                "reference checkpoint profile mismatch: source/topology selects v{expected}, runtime selected v{version}"
            ),
        ));
    }
    Ok(())
}

/// Aligned recovery: honor Filter/Project on the Kernel path; reject
/// dishonest plans rather than strip stages (P0-1/P0-2/A1). B2-A adds a
/// separate profile for static immutable Lookup dependencies.
pub fn validate_aligned_plan(
    spec: &PipelineSpec,
    plan: &PhysicalPlan,
) -> sparrow_model::Result<()> {
    let dependencies = if spec.reference_tables.is_empty() {
        None
    } else {
        Some(reference_dependency_shape(spec)?)
    };
    validate_aligned_plan_inner(spec, plan, dependencies.as_deref())
}

fn validate_aligned_plan_inner(
    spec: &PipelineSpec,
    plan: &PhysicalPlan,
    dependencies: Option<&[sparrow_plan::ReferenceTableDependency]>,
) -> sparrow_model::Result<()> {
    let recovery = RecoveryPolicy::parse(&spec.recovery)?;
    if spec.graph_io.as_ref().is_some_and(|io|io.idle_after_ms.is_some())
        && !(recovery.is_aligned() && plan.edges.is_some() && (plan.has_processing_time_state() || plan.has_event_time_window())) {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"idle_after_ms is only supported by durable time graphs"));
    }
    if recovery.is_aligned() && plan.edges.is_some() && (plan.has_processing_time_state() || plan.has_event_time_window()) {
        validate_time_graph_profile(spec,plan)?;
    } else if plan.has_timed_iot() || (recovery.is_aligned() && plan.has_processing_time_state()) { validate_paused_time_profile(spec,plan)?; }
    if !recovery.is_aligned() {
        return Ok(());
    }
    if plan.has_iot() && spec.graph_io.is_none() {
        validate_linear_iot_profile(spec)?;
    }
    if let Some(io) = &spec.graph_io {
        if spec.checkpoint_dir.as_deref().is_none_or(str::is_empty) || io.sources.values().any(|s| !matches!(s.kind.as_str(),"file"|"file_replay"|"replay"))
            || io.sinks.values().any(|s|s.kind!="http") {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"graph aligned requires explicit checkpoint_dir, replayable File inputs and required HTTP outputs"));
        }
    }
    if !matches!(spec.source.kind.as_str(), "file" | "file_replay" | "replay")
        && !(cfg!(feature="jetstream") && spec.source.kind=="jetstream") {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "aligned recovery requires a ReplayableSource (file); MQTT replay remains unsupported",
        ));
    }
    if let Some(path) = &spec.source.path {
        check_data_path(std::path::Path::new(path)).map_err(io)?;
    }
    if let Some(dir) = &spec.checkpoint_dir {
        check_data_path(std::path::Path::new(dir)).map_err(io)?;
    }
    if let Some(dependencies) = dependencies {
        validate_reference_checkpoint_profile(spec, plan)?;
        checkpoint_plan_with_references(spec, plan, dependencies)?;
    } else {
        sparrow_plan::CheckpointPlan::from_physical(plan)?;
    }
    if spec.source.kind=="jetstream" && !plan.has_processing_time_state() && plan.stages.iter().any(|stage|
        matches!(stage,sparrow_plan::PhysicalStage::WindowAgg{spec,..} if !matches!(spec.kind,sparrow_model::WindowKind::Count{..}))) {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"JetStream replay currently admits zero state or one/two Count windows only; event/processing-time replay context not verified"));
    }
    Ok(())
}

fn validate_linear_iot_profile(spec: &PipelineSpec) -> Result<()> {
    if spec.checkpoint_dir.as_deref().is_none_or(str::is_empty) {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "linear aligned IoT recovery requires an explicit checkpoint_dir and HTTP sink",
        ));
    }
    if spec.sink.kind != "http" {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "linear aligned IoT recovery requires an explicit checkpoint_dir and HTTP sink",
        ));
    }
    Ok(())
}

fn validate_paused_time_profile(spec:&PipelineSpec,plan:&PhysicalPlan)->Result<()> {
    let supported_source=matches!(spec.source.kind.as_str(),"file"|"file_replay"|"replay")
        || (cfg!(feature="jetstream") && spec.source.kind=="jetstream");
    if spec.recovery!="aligned" || !supported_source || spec.graph_io.is_some() || plan.edges.is_some()
        || !spec.reference_tables.is_empty() || !plan.source_times.is_empty() || !plan.side_outputs.is_empty()
        || spec.sink.kind!="http" || spec.sink.skip_verify
        || spec.checkpoint_dir.as_deref().is_none_or(str::is_empty)
        || !spec.fail_on_decode
        || !spec.checkpoint.as_ref().is_some_and(|p|p.resume_latest && p.interval_ms.is_some_and(|n|(100..=1000).contains(&n)))
        || spec.restore.as_ref().is_some_and(|r| r.kind!="checkpoint" || r.snapshot_id.as_deref().is_some_and(|id|!id.is_empty() && id!="aligned")) {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,
            "paused processing-time recovery requires linear File/JetStream, verified HTTP, explicit checkpoint_dir, fail_on_decode=true and resume_latest with interval_ms=100..1000; historical restore, references and side/time inputs are not enabled"));
    }
    if spec.source.kind!="jetstream" && resolve_file_contract(spec,RecoveryPolicy::Aligned)?!=sparrow_connectors::FileContract::AppendOnly {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"paused-time File input requires append_only; EOF must not stop timers"));
    }
    sparrow_plan::CheckpointPlan::from_physical(plan)?;
    Ok(())
}

pub(crate) fn validate_time_graph_profile(spec:&PipelineSpec,plan:&PhysicalPlan)->Result<()> {
    let Some(io)=&spec.graph_io else {return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"durable time graph requires graph_io"));};
    if spec.recovery!="aligned" || plan.edges.is_none() || !spec.reference_tables.is_empty() || !plan.side_outputs.is_empty()
        || !spec.fail_on_decode || spec.checkpoint_dir.as_deref().is_none_or(str::is_empty)
        || !spec.checkpoint.as_ref().is_some_and(|p|p.resume_latest && p.interval_ms.is_some_and(|n|(100..=1000).contains(&n)))
        || spec.restore.as_ref().is_some_and(|r|r.kind!="checkpoint" || r.snapshot_id.as_deref().is_some_and(|s|!s.is_empty()&&s!="aligned"))
        || io.sources.values().any(|s|!matches!(s.kind.as_str(),"file"|"file_replay"|"replay"))
        || io.sinks.values().any(|s|s.kind!="http"||s.skip_verify)
        || io.idle_after_ms.is_some_and(|n|!(100..=86400000).contains(&n)) {
        return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"durable time graph requires File inputs, verified required HTTP, independent directory, fail_on_decode, resume_latest and 100..1000 ms decisions; optional idle_after_ms=100..86400000; no historical replay/references/side outputs"));
    }
    let manifest=sparrow_plan::CheckpointPlan::from_physical(plan)?;
    if !manifest.is_time_graph() {return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"graph has no supported durable time state"));}
    for source in io.sources.values() {let mut single=spec.clone();single.graph_io=None;single.source=source.clone();resolve_file_contract(&single,RecoveryPolicy::Aligned)?;}
    Ok(())
}

pub fn capabilities_json() -> serde_json::Value {
    let mqtt = ConnectorCapabilities::MQTT_SOURCE;
    let http = ConnectorCapabilities::HTTP_SINK;
    let push = ConnectorCapabilities::HTTP_PUSH;
    let mqtt_sink = ConnectorCapabilities::MQTT_SINK;
    let file = ConnectorCapabilities::FILE_REPLAY;
    serde_json::json!({
        "inventory":crate::capability::inventory(),
        "delivery": DeliveryGuarantee::LiveBestEffort.as_str(),
        "recovery": RecoveryPolicy::RestartFresh.as_str(),
        "recovery_pt_window": RecoveryPolicy::RestartFresh.none_label(),
        "recovery_aligned": RecoveryPolicy::Aligned.as_str(),
        "exactly_once": "rejected",
        "connectors": [
            {
                "kind": mqtt.kind,
                "replay": mqtt.replay.as_str(),
                "delivery": mqtt.delivery.as_str(),
                "recovery": mqtt.recovery.as_str(),
            },
            {
                "kind": http.kind,
                "replay": http.replay.as_str(),
                "delivery": http.delivery.as_str(),
                "recovery": http.recovery.as_str(),
            },
            {
                "kind": push.kind,
                "replay": push.replay.as_str(),
                "delivery": push.delivery.as_str(),
                "recovery": push.recovery.as_str(),
            },
            {
                "kind": mqtt_sink.kind,
                "replay": mqtt_sink.replay.as_str(),
                "delivery": mqtt_sink.delivery.as_str(),
                "recovery": mqtt_sink.recovery.as_str(),
            },
            {
                "kind": file.kind,
                "replay": file.replay.as_str(),
                "delivery": file.delivery.as_str(),
                "recovery": file.recovery.as_str(),
                "aligned": true,
            },
            {
                "kind":"jetstream","enabled_by_build":cfg!(feature="jetstream"),"maturity":"preview",
                "replay":if cfg!(feature="jetstream"){"replayable"}else{"unavailable"},
                "delivery":"checkpointed_at_least_once","recovery":"aligned","requires_eligible_profile":true,
                "state_profiles":if cfg!(feature="jetstream"){serde_json::json!({"legacy_count":"v4","iot_ttl0":"v7"})}else{serde_json::json!({})},
                "certified":false,
            }
        ],
        "honesty": HONESTY,
    })
}

pub fn reject_named_delivery(name: &str) -> Result<DeliveryGuarantee> {
    refuse_delivery_name(name).map_err(io)
}

/// Effective delivery + recovery + risk for a stored pipeline spec.
pub fn effective_guarantees(spec: &PipelineSpec) -> serde_json::Value {
    let recovery = RecoveryPolicy::parse(&spec.recovery).unwrap_or(RecoveryPolicy::RestartFresh);
    let reliable=cfg!(feature="jetstream") && spec.source.kind=="jetstream";
    let replayable = matches!(spec.source.kind.as_str(), "file" | "file_replay" | "replay") || reliable;
    let replay = if replayable {
        ReplaySupport::Replayable.as_str()
    } else {
        ReplaySupport::Unsupported.as_str()
    };
    let recovery_risk = if recovery.is_aligned() && replayable {
        "aligned_plan_not_validated"
    } else if replayable {
        "restart_fresh_loses_in_memory_state"
    } else {
        "no_durable_restore; live_best_effort_drops_ok"
    };
    serde_json::json!({
        "delivery": if reliable {DeliveryGuarantee::CheckpointedAtLeastOnce.as_str()} else {DeliveryGuarantee::LiveBestEffort.as_str()},
        "recovery": recovery.as_str(),
        "replay": replay,
        "exactly_once": false,
        "recovery_risk": recovery_risk,
        "aligned_eligible": if replayable { serde_json::Value::Null } else { serde_json::json!(false) },
        "aligned_eligibility_reason": if replayable { "requires_bound_plan" } else { "source_not_replayable" },
        "honesty": HONESTY,
    })
}

fn iot_ttls(plan: &PhysicalPlan) -> Vec<i64> {
    plan.stages
        .iter()
        .filter_map(|stage| match stage {
            sparrow_plan::PhysicalStage::Iot { spec, .. } => Some(spec.ttl_micros),
            _ => None,
        })
        .collect()
}

fn plan_has_hysteresis(plan: &PhysicalPlan) -> bool {
    plan.stages.iter().any(|stage| {
        matches!(
            stage,
            sparrow_plan::PhysicalStage::Iot { spec, .. } if spec.hysteresis.is_some()
        )
    })
}

fn reference_source_kind(spec: &PipelineSpec, plan: &PhysicalPlan) -> &'static str {
    if plan.edges.is_some() {
        "file-dag-v1"
    } else if spec.source.kind == "jetstream" {
        "jetstream-v1"
    } else {
        "file"
    }
}

fn reference_snapshot_version(spec: &PipelineSpec, plan: &PhysicalPlan) -> Result<u16> {
    let dependencies = reference_dependency_shape(spec)?;
    let layout = sparrow_plan::CheckpointPlan::from_physical_with_references(plan, dependencies)?;
    sparrow_runtime::pipeline_checkpoint::snapshot_version_for(
        &layout,
        reference_source_kind(spec, plan),
    )
}

/// Eligibility is a property of both the replayable source and the bound plan,
/// independent of whether the stored spec currently requests aligned recovery.
pub fn effective_guarantees_with_plan(spec: &PipelineSpec, plan: &PhysicalPlan) -> serde_json::Value {
    let mut value = effective_guarantees(spec);
    if plan.stages.iter().any(|s| matches!(s,sparrow_plan::PhysicalStage::Iot {spec,..} if spec.is_alarm())) {
        value["alarm"] = serde_json::json!({"maturity":"development_preview","certified":false,
            "phases":["normal","pending","active","recovering"],"events":["activate","resolve","notify"],
            "episode_identity":["state_generation","operator","configured_key_values","per_key_episode"],
            "notification_policy":"state_events_always_emitted; resolve_bypasses_cooldown; one_latest_activation_pending_per_key",
            "notification_age":"expired_pending_notification_is_counted_not_sent; equal_age_expires",
            "state_eviction":"none; Normal_retains_episode_counter; key_or_byte_exhaustion_fails",
            "timers":"two_logical_slots_per_key; due_before_input; condition_wins_equal_notification_deadline"});
    }
    if plan.edges.is_some() && (plan.has_processing_time_state() || plan.has_event_time_window()) {
        let mut candidate=spec.clone();candidate.recovery="aligned".into();
        let checked = validate_time_graph_profile(&candidate, plan);
        let version = sparrow_plan::CheckpointPlan::from_physical(plan).ok().and_then(|p|
            sparrow_runtime::snapshot_version_for(&p, sparrow_runtime::graph_cut::KIND).ok());
        value["graph"] = serde_json::json!("dag");
        value["aligned_eligible"] = serde_json::json!(checked.is_ok());
        value["aligned_eligibility_reason"] = serde_json::json!(checked.err().map(|e| e.message));
        if spec.recovery!="aligned" {
            // Eligibility is not activation: ordinary live graphs still use
            // host time/ready-order and have no durable output identity.
            value["checkpoint_participants"]=serde_json::json!({"active":false,"snapshot_version":null,
                "candidate_snapshot_version":version,"certified":false});
            value["graph_time"]=serde_json::json!({"mode":"live_restart_fresh","durable_rounds":false,
                "stable_output_ids":false,"ordering":"legacy_ready_order"});
            value["recovery_risk"]=serde_json::json!("restart_fresh_loses_graph_state");
            if plan.has_iot() {value["iot"]=serde_json::json!({"recovery":"restart_fresh_empty_state_no_persisted_generation","ttl_micros":iot_ttls(plan)});}
            return value;
        }
        value["checkpoint_participants"] = serde_json::json!({
            "snapshot_version":version,"manifest":"CPL1/CP01DAG2","certified":false,
            "scope":"required_File_HTTP_time_graph; PT_or_ET_not_mixed","max_states":16,
            "recovery":"CURRENT_only; TIME_PENDING_required; full_graph_semantics",
            "ordering":"logged_global_decisions; bounded_fixed_edge_order_Union",
            "clock":"paused_processing_time; separately_recorded_ET_wall_observation",
            "idle_after_ms":spec.graph_io.as_ref().and_then(|io|io.idle_after_ms),
            "eof":"permanent_only_for_sealed_or_immutable; append_only_remains_active_without_explicit_idle",
            "output_ids":"generation_and_sink_scoped_epoch; persisted_per_sink_ordinal",
            "throughput":"serialized_per_decision_fsync_and_all_required_HTTP_flush; not_high_throughput"
        });
        value["recovery_risk"] = serde_json::json!("HTTP_may_repeat_before_CURRENT; deduplicate_by_output_identity; no_exactly_once_or_cross_sink_rollback");
        return value;
    }
    if plan.has_timed_iot() || (spec.recovery == "aligned" && plan.has_processing_time_state()) {
        let checked=validate_paused_time_profile(spec,plan);
        let manifest = sparrow_plan::CheckpointPlan::from_physical(plan).ok();
        let version = manifest.as_ref().and_then(|p| sparrow_runtime::snapshot_version_for(p,
            if spec.source.kind=="jetstream" { sparrow_runtime::processing_cut::JETSTREAM_KIND } else { sparrow_runtime::processing_cut::FILE_KIND }).ok());
        value["aligned_eligible"]=serde_json::json!(checked.is_ok());
        value["aligned_eligibility_reason"]=serde_json::json!(checked.err().map(|e|e.message));
        value["recovery_risk"]=serde_json::json!("required_HTTP_may_repeat_before_commit; deduplicate_by_output_identity; no_exactly_once");
        value["iot"]=serde_json::json!({"operators":["hold_for","debounce","change_detect","deadband","hysteresis"],"clock":"paused_source_ordered",
            "snapshot_version":version,"maturity":"preview","certified":false,
            "profile":if manifest.as_ref().is_some_and(|p|p.is_single_timed_iot()) {"one_timed_state_linear_no_references"} else {"bounded_linear_time_states"},"recovery":"CURRENT_only; TIME_PENDING_required",
            "max_states":2,"windows":["processing_time_tumbling","count"],"ttl":"last_valid_input; expire_before_input_at_equal_cut",
            "ordering":"forward_time_before_timer_output; downstream_due_before_upstream_derived_rows",
            "checkpoint":"one_input_or_idle_tick_per_durable_decision; commit_before_next_decision",
            "idle_tick_ms":spec.checkpoint.as_ref().and_then(|p|p.interval_ms),
            "downtime":"paused; startup_and_pending_replay_do_not_advance_time",
            "throughput":"serialized_per_decision_fsync_and_required_HTTP_flush; not_the_high_throughput_profile"});
        return value;
    }
    let reference_version = if spec.reference_tables.is_empty() {
        None
    } else {
        reference_snapshot_version(spec, plan).ok()
    };
    if !spec.reference_tables.is_empty() {
        value["reference_tables"] = serde_json::json!({
            "bindings":spec.reference_tables,
            "selection":"immutable_revision_and_sha256; never_latest",
            "validation":"requires_exact_resolution_at_bind_and_start",
            "running_update":"new_table_publication_does_not_replace_bound_revision",
            "recovery":"restart_fresh_by_default; aligned profiles v8(stateless File), v9(File state), v10(JetStream), v11(File DAG)",
            "gc_pin":"all_retained_pipeline_revisions",
            "checkpoint_dependencies":"profile-specific snapshots store exact revision, canonical SHA-256 and runtime table CRC; table rows are not copied into the checkpoint",
            "checkpoint_profile":reference_version.map(|version| format!("v{version}")),
            "certified":false
        });
    }
    let recovery = RecoveryPolicy::parse(&spec.recovery).unwrap_or(RecoveryPolicy::RestartFresh);
    let reliable_iot = cfg!(feature = "jetstream") && spec.source.kind == "jetstream";
    let unreferenced_iot_version = if plan_has_hysteresis(plan) {
        if reliable_iot { 13 } else { 12 }
    } else if reliable_iot {
        7
    } else {
        6
    };
    let iot_snapshot_version = reference_version.unwrap_or(unreferenced_iot_version);
    let iot_restore_compatibility = if reference_version.is_some() {
        "source_all_state_and_full_plan_semantics; exact reference revision/SHA/CRC; separate profile-specific directory"
    } else if plan_has_hysteresis(plan) {
        "source_all_state_and_full_plan_semantics; separate Hysteresis profile directory"
    } else if reliable_iot {
        "source_all_state_and_full_plan_semantics; separate directories from v3/v4/v5/v6"
    } else {
        "source_all_state_and_full_plan_semantics; separate directories from v3/v4/v5"
    };
    let iot_downstream_changes = if reference_version.is_some() {
        "any table revision/schema/CRC or computation change requires explicit fresh or compatible reference restore; external outputs are not rolled back"
    } else if plan_has_hysteresis(plan) {
        "full_plan_change_requires_explicit_fresh_or_compatible_hysteresis_restore; external_outputs_are_not_rolled_back"
    } else if reliable_iot {
        "full_plan_change_requires_explicit_fresh_or_compatible_v7_restore; external_outputs_are_not_rolled_back"
    } else {
        "full_plan_change_requires_explicit_fresh_or_compatible_v6_restore; external_outputs_are_not_rolled_back"
    };
    if plan.has_iot() {
        let iot_recovery = if recovery.is_aligned() {
            if reference_version.is_some() {
                format!("aligned_v{iot_snapshot_version}_ttl_disabled")
            } else if plan_has_hysteresis(plan) {
                format!("aligned_v{iot_snapshot_version}_hysteresis_ttl_disabled")
            } else if reliable_iot {
                "aligned_v7_ttl_disabled".to_string()
            } else {
                "aligned_v6_ttl_disabled".to_string()
            }
        } else {
            "restart_fresh_empty_state_no_persisted_generation".to_string()
        };
        let iot_continuity = if recovery.is_aligned() {
            format!("preserved_from_compatible_v{iot_snapshot_version}_checkpoint")
        } else {
            "not_preserved_on_restart_or_reset".to_string()
        };
        value["iot"] = serde_json::json!({
            "operators":if plan_has_hysteresis(plan) { vec!["change_detect","deadband","hysteresis"] } else { vec!["change_detect","deadband"] },
            "state":"bounded_task_owned_key_state",
            "snapshot_version":iot_snapshot_version,
            "recovery":iot_recovery,
            "continuity":iot_continuity,
            "ttl_micros":iot_ttls(plan)
        });
    }
    if plan.edges.is_some() {
        let mut aligned=spec.clone();aligned.recovery="aligned".into();
        let eligibility=validate_aligned_plan(&aligned,plan);
        value["graph"]=serde_json::json!("dag");
        value["aligned_eligible"]=serde_json::json!(eligibility.is_ok());
        value["aligned_eligibility_reason"]=serde_json::json!(eligibility.err().map_or_else(||"graph_required_file_count_participants".into(),|e|e.message));
        let iot = plan.has_iot();
        let iot_ttl = iot_ttls(plan);
        let snapshot_version = reference_version.unwrap_or(if iot {
            iot_snapshot_version
        } else {
            5
        });
        let manifest = if reference_version.is_some() { "CPL3" } else { "CPL1/CP01DAG1" };
        value["checkpoint_participants"]=serde_json::json!({
            "scope":if reference_version.is_some() {"graph_File_required_HTTP_static_Lookup_Count_or_IoT"} else if iot {"graph_File_required_HTTP_stateless_Count_or_IoT"} else {"graph_File_required_HTTP_stateless_or_Count"},
            "snapshot_version":snapshot_version,
            "manifest":manifest,
            "manifest_version":if reference_version.is_some() {"CPL3"} else {"CPL1"},
            "restore_compatibility":if iot {iot_restore_compatibility} else {"strict_full_graph_and_all_source_cursors; separate_directory_from_v3_v4"},
            "certified":false
        });
        if iot {
            value["iot"] = serde_json::json!({
                "operators":if plan_has_hysteresis(plan) { vec!["change_detect","deadband","hysteresis"] } else { vec!["change_detect","deadband"] },
                "state":"bounded_task_owned_key_state",
                "snapshot_version":iot_snapshot_version,
                "recovery":if spec.recovery=="aligned" {format!("aligned_v{iot_snapshot_version}_ttl_disabled")} else {"restart_fresh_empty_state_no_persisted_generation".to_string()},
                "continuity":if spec.recovery=="aligned" {format!("preserved_from_compatible_v{iot_snapshot_version}_checkpoint")} else {"not_preserved_on_restart_or_reset".to_string()},
                "ttl_micros":iot_ttl
            });
        }
        value["recovery_risk"]=serde_json::json!(if spec.recovery=="aligned"{"external_outputs_may_repeat; no_cross_sink_rollback; Count_Union_interleaving_not_global_order"}else{"restart_fresh_loses_graph_state"});
        value["replay"]=serde_json::json!(if spec.graph_io.as_ref().is_some_and(|io|io.sources.values().all(|s|matches!(s.kind.as_str(),"file"|"file_replay"|"replay"))){"replayable"}else{"unsupported"});
        return value;
    }
    if spec.source.kind=="jetstream" {
        let accepted=spec.basic_check().and_then(|_|spec.check_delivery()).and_then(|_|validate_aligned_plan(spec,plan));
        let iot = plan.has_iot();
        let accepted_ok = accepted.is_ok();
        let eligibility_reason = accepted
            .as_ref()
            .err()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| {
                if let Some(version) = reference_version {
                    format!("jetstream_static_reference_dependencies_profile{version}")
                } else if iot {
                    format!("jetstream_reliable_iot_v{iot_snapshot_version}_ttl0_count_or_iot_state")
                } else {
                    "jetstream_zero_or_one_two_count_windows".into()
                }
            });
        value["requested_delivery"]=serde_json::json!(spec.delivery);
        if !accepted_ok {value["delivery"]=serde_json::json!("unavailable");}
        value["aligned_eligible"]=serde_json::json!(accepted_ok);
        value["aligned_eligibility_reason"]=serde_json::json!(eligibility_reason);
        let snapshot_version = reference_version.unwrap_or(if iot {
            iot_snapshot_version
        } else {
            4
        });
        let profile = reference_version.map_or_else(
            || {
                if plan_has_hysteresis(plan) {
                    "hysteresis_reliable_v13".to_string()
                } else if iot {
                    "reliable_iot_v7".to_string()
                } else {
                    "jetstream_v4".to_string()
                }
            },
            |version| format!("reference_jetstream_v{version}"),
        );
        value["checkpoint_participants"]=serde_json::json!({"snapshot_version":snapshot_version,"profile":profile,"manifest_version":if reference_version.is_some() {"CPL3"} else {"CPL1"},"scope":if reference_version.is_some() {"single_jetstream_static_reference_required_http"} else if iot {"single_jetstream_single_required_http_sink_iot"} else {"single_jetstream_single_required_http_sink"},
            "restore_compatibility":if iot {iot_restore_compatibility} else {"full_computation_and_source_reader_binding"},"downstream_changes":if iot {iot_downstream_changes} else {"rejected_until_explicit_lineage_fork"},
            "confirmation":"HTTP_2xx_acceptance_not_business_commit","source_ack":"after_durable_checkpoint","output_ids":"128bit_epoch_64bit_ordinal",
            "certified":false,"maturity":"preview","poison":"fail_then_finite_retry_or_held","durable_outbox":false,"dlq":false});
        value["recovery_risk"]=serde_json::json!("uncommitted_outputs_may_repeat_with_stable_ids; retention_expiry_refuses_restore; no_HA");
        if iot {
            value["iot"] = serde_json::json!({
                "operators":if plan_has_hysteresis(plan) { vec!["change_detect","deadband","hysteresis"] } else { vec!["change_detect","deadband"] },
                "state":"bounded_task_owned_key_state",
                "snapshot_version":iot_snapshot_version,
                "recovery":if accepted_ok {format!("aligned_v{iot_snapshot_version}_ttl_disabled")} else {"unavailable".to_string()},
                "continuity":if accepted_ok {format!("preserved_from_compatible_v{iot_snapshot_version}_checkpoint")} else {"unavailable_until_validation_passes".to_string()},
                "ttl_micros":iot_ttls(plan)
            });
        }
        if let Some(config)=&spec.source.jetstream {
            value["jetstream_execution"]=serde_json::json!({
                "input_batching":"already_ready_rows_bounded_by_kernel_and_pull",
                "idle_backoff_ms":{"initial":5,"maximum":250,"nonempty_fetch":0},
                "idle_steady_pulls_per_second_max":4,
                "ack_policy":"explicit","ack_concurrency":16,"ack_attempts":3,
                "checkpoint_timeout":"abort_checkpoint_keep_attempt_without_ACK; bootstrap_blocks_input",
                "sdk_reservation_bytes":512usize*1024+32*config.pull_bytes+(config.pull_messages+2)*4608,
                "ack_workspace_bytes":64*1024,
                "payload_admission":"shared_Job_budget_not_protocol_max_payload"});
        }
        return value;
    }
    if matches!(spec.source.kind.as_str(), "file" | "file_replay" | "replay") {
        let checkpoint_plan = if !spec.reference_tables.is_empty() {
            let mut aligned = spec.clone();
            aligned.recovery = "aligned".into();
            validate_aligned_plan(&aligned, plan).and_then(|_| {
                let dependencies = reference_dependency_shape(&aligned)?;
                checkpoint_plan_with_references(&aligned, plan, &dependencies)
            })
        } else if plan.has_iot() {
            validate_linear_iot_profile(spec)
                .and_then(|_| sparrow_plan::CheckpointPlan::from_physical(plan))
        } else {
            sparrow_plan::CheckpointPlan::from_physical(plan)
        };
        match checkpoint_plan {
            Ok(manifest) => {
                value["aligned_eligible"] = serde_json::json!(true);
                let iot = plan.has_iot();
                let iot_ttl = iot_ttls(plan);
                value["aligned_eligibility_reason"] = serde_json::json!(if let Some(version) = reference_version {
                    format!("static_reference_dependencies_profile{version}")
                } else if iot {
                    format!("IoT_state_participants_v{iot_snapshot_version}")
                } else {
                    match manifest.states.len() {
                        0=>"zero_state_file_cut",
                        1=>"single_supported_window",
                        _=>"two_count_window_participants",
                    }.to_string()
                });
                let snapshot_version = reference_version.unwrap_or(if iot {
                    iot_snapshot_version
                } else {
                    sparrow_runtime::pipeline_checkpoint::PIPELINE_SNAPSHOT_VERSION
                });
                let manifest_version = if reference_version.is_some() { "CPL3" } else { "CPL1" };
                value["checkpoint_participants"] = serde_json::json!({"source":manifest.source.raw(),"required_sink":manifest.sink.raw(),
                    "snapshot_version":snapshot_version,
                    "manifest_version":manifest_version,"semantics_version":if iot {"CP01_full_plan_IoT_state"} else if reference_version.is_some() {"CP01_full_plan_static_reference_dependencies"} else {"CP01+RCP2"},"restore_compatibility":if iot {iot_restore_compatibility} else if reference_version.is_some() {"source_and_exact_static_reference_revision_sha256_runtime_crc; separate profile-specific directory"} else {"source_and_all_state_upstream_prefixes; plain_CP01_full_plan_strict"},
                    "downstream_changes":if reference_version.is_some() {"any table revision/schema/CRC or computation change requires explicit fresh or compatible reference restore; external outputs are not rolled back"} else if iot {iot_downstream_changes} else {"allowed_after_last_state; plain_CP01_snapshots_remain_full_plan_strict; external_outputs_are_not_rolled_back"},
                    "states":manifest.states.iter().map(|state|match state.id {
                        sparrow_plan::ParticipantId::State {operator,slot,shard}=>serde_json::json!({"operator":operator.raw(),"slot":slot.raw(),"shard":shard,"codec":state.codec,"window_kind":state.window_kind}),
                        _=>unreachable!(),
                    }).collect::<Vec<_>>(),"profile":reference_version.map_or_else(|| format!("v{snapshot_version}"), |version| format!("reference_v{version}")),"scope":if reference_version.is_some() {"single_file_static_reference_required_http_linear_shapes"} else {"single_file_single_required_sink_tested_linear_shapes"},"certified":false});
                if iot {
                    value["iot"] = serde_json::json!({
                        "operators":if plan_has_hysteresis(plan) { vec!["change_detect","deadband","hysteresis"] } else { vec!["change_detect","deadband"] },
                        "state":"bounded_task_owned_key_state",
                        "snapshot_version":iot_snapshot_version,
                        "recovery":if spec.recovery=="aligned" {format!("aligned_v{iot_snapshot_version}_ttl_disabled")} else {"restart_fresh_empty_state_no_persisted_generation".to_string()},
                        "continuity":if spec.recovery=="aligned" {format!("preserved_from_compatible_v{iot_snapshot_version}_checkpoint")} else {"not_preserved_on_restart_or_reset".to_string()},
                        "ttl_micros":iot_ttl
                    });
                }
                if spec.recovery == "aligned" {
                    value["recovery_risk"] = serde_json::json!("committed_checkpoint_only");
                }
            }
            Err(e) => {
                value["aligned_eligible"] = serde_json::json!(false);
                value["aligned_eligibility_reason"] = serde_json::json!(e.message);
                if plan.has_iot() {
                    value["checkpoint_participants"] = serde_json::json!({
                        "scope":"single_file_single_required_sink_iot",
                        "snapshot_version":iot_snapshot_version,
                        "profile":format!("iot_v{iot_snapshot_version}"),
                        "manifest_version":if reference_version.is_some() {"CPL3"} else {"CPL1"},
                        "eligible":false,
                        "certified":false
                    });
                    value["iot"] = serde_json::json!({
                        "operators":if plan_has_hysteresis(plan) { vec!["change_detect","deadband","hysteresis"] } else { vec!["change_detect","deadband"] },
                        "state":"bounded_task_owned_key_state",
                        "snapshot_version":iot_snapshot_version,
                        "recovery":"restart_fresh_empty_state_no_persisted_generation",
                        "continuity":"not_preserved_on_restart_or_reset",
                        "ttl_micros":iot_ttls(plan)
                    });
                }
                if spec.recovery == "aligned" {
                    value["recovery_risk"] = serde_json::json!("aligned_plan_rejected");
                }
            }
        }
    }
    value
}

fn io(err: sparrow_connectors::ConnectorError) -> SparrowError {
    SparrowError::new(err.code, err.message)
}

fn fnv(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    if h == 0 {
        1
    } else {
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::RestoreSpec;

    #[test]
    fn mqtt_inbox_wait_spec_default_override_and_validation() {
        let base = serde_json::json!({
            "stream": "sensors", "sql": "SELECT device_id FROM sensors",
            "source": {"kind": "mqtt", "host": "127.0.0.1", "port": 1883},
            "sink": {"kind": "log"}
        });
        let schema = Schema::new(SchemaId::new(1), vec![sparrow_model::Field::new(
            sparrow_model::FieldId::new(1), "device_id", sparrow_model::DataType::Utf8, false,
        )]).unwrap();
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&base).unwrap()).unwrap();
        assert_eq!(spec.source.inbox_wait_ms, None);
        let default_cfg = mqtt_config(&spec.source, schema.clone(), None, "budget").unwrap();
        assert_eq!(default_cfg.inbox_bytes, Some(256*1024));
        assert!(!default_cfg.tcp_quickack);
        assert!(serde_json::to_value(&spec).unwrap()["source"].get("inbox_wait_ms").is_none());
        assert_eq!(mqtt_config(&spec.source, schema.clone(), None, "wait").unwrap().inbox_wait_timeout,
            std::time::Duration::from_millis(5));
        for ms in [0, 5, 1000] {
            let mut value = base.clone();
            value["source"]["inbox_wait_ms"] = serde_json::json!(ms);
            let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
            let cfg = mqtt_config(&spec.source, schema.clone(), None, "wait").unwrap();
            assert_eq!(cfg.inbox_wait_timeout, std::time::Duration::from_millis(ms));
        }
        for ms in [serde_json::json!(1001), serde_json::json!(u64::MAX), serde_json::json!(-1), serde_json::json!(0.5)] {
            let mut value = base.clone();
            value["source"]["inbox_wait_ms"] = ms;
            assert!(PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
        }
        let mut value = base;
        value["source"]["kind"] = serde_json::json!("file");
        value["source"]["inbox_wait_ms"] = serde_json::json!(5);
        assert_eq!(PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn rejects_checkpoint_and_at_least_once() {
        let mut spec = PipelineSpec {
            reference_tables: Default::default(),
            graph_io: None,
            version: 1,
            stream: "sensors".into(),
            sql: Some("SELECT device_id FROM sensors".into()),
            graph: None,
            source: SourceSpec {
                jetstream: None,
                kind: "mqtt".into(),
                host: Some("127.0.0.1".into()),
                port: Some(1883),
                topic: "t".into(),
                client_id: None,
                qos: 0,
                clean_session: true,
                username_secret: None,
                password_secret: None,
                skip_verify: false,
                inbox_capacity: 8,
                inbox_wait_ms: None,
                tcp_quickack: None,
                inbox_bytes: None,
                use_demo_io: false,
                bind: None,
                path: None,
                tls: false,
                file_contract: None,
            },
            sink: crate::spec::SinkSpec {
                kind: "http".into(),
                url: Some("http://127.0.0.1:1/".into()),
                skip_verify: false,
                outbox_capacity: 8,
                batch_rows: None,
                batch_bytes: None,
                linger_ms: None,
                max_inflight: None,
                use_demo_io: false,
                header_secret: None,
                host: None,
                port: None,
                topic: None,
                client_id: None,
                qos: 0,
                clean_session: true,
                tls: false,
            },
            delivery: "at_least_once".into(),
            recovery: "restart_fresh".into(),
            restore: None,
            checkpoint_dir: None,
            checkpoint: None,
            fail_on_decode: false,
        };
        assert_eq!(
            spec.check_delivery().unwrap_err().code,
            ErrorCode::UnsupportedDelivery
        );
        spec.delivery = "live_best_effort".into();
        spec.restore = Some(RestoreSpec {
            kind: "checkpoint".into(),
            snapshot_id: Some("snap".into()),
            client_id: None,
        });
        assert_eq!(
            spec.check_delivery().unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        spec.recovery = "aligned".into();
        assert_eq!(
            spec.check_delivery().unwrap_err().code,
            ErrorCode::UnsupportedRestore,
            "MQTT + aligned checkpoint must still reject"
        );
        spec.source.kind = "file".into();
        spec.source.path = Some("/tmp/events.ndjson".into());
        assert!(spec.check_delivery().is_ok());
        let g = effective_guarantees(&spec);
        assert_eq!(g["recovery"], "aligned");
        assert_eq!(g["recovery_risk"], "aligned_plan_not_validated");
        assert!(g["aligned_eligible"].is_null());
    }

    #[test]
    fn v02_default_mqtt_client_id_is_unique_per_instance() {
        let src = SourceSpec {
            jetstream: None,
            kind: "mqtt".into(),
            host: Some("127.0.0.1".into()),
            port: Some(1883),
            topic: "t".into(),
            client_id: None,
            qos: 0,
            clean_session: true,
            username_secret: None,
            password_secret: None,
            skip_verify: false,
            inbox_capacity: 8,
            inbox_wait_ms: None,
            tcp_quickack: None,
            inbox_bytes: None,
            use_demo_io: false,
            bind: None,
            path: None,
            tls: false,
            file_contract: None,
        };
        let schema = Schema::new(
            SchemaId::new(1),
            vec![sparrow_model::Field::new(
                sparrow_model::FieldId::new(1),
                "device_id",
                sparrow_model::DataType::Utf8,
                false,
            )],
        )
        .unwrap();
        let a = mqtt_config(&src, schema.clone(), None, "pipe-a").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = mqtt_config(&src, schema, None, "pipe-b").unwrap();
        assert_ne!(a.client_id, b.client_id);
        assert!(a.client_id.contains("pipe-a"));
        assert!(b.client_id.contains("pipe-b"));
        assert!(!a.client_id.eq("sparrow-source"));
    }

    #[test]
    fn p0_11_mqtt_credentials_require_tls() {
        let mut src = SourceSpec {
            jetstream: None,
            kind: "mqtt".into(),
            host: Some("127.0.0.1".into()),
            port: Some(1883),
            topic: "t".into(),
            client_id: None,
            qos: 0,
            clean_session: true,
            username_secret: Some("user".into()),
            password_secret: Some("pw".into()),
            skip_verify: false,
            inbox_capacity: 8,
            inbox_wait_ms: None,
            tcp_quickack: None,
            inbox_bytes: None,
            use_demo_io: false,
            bind: None,
            path: None,
            tls: false,
            file_contract: None,
        };
        let schema = Schema::new(
            SchemaId::new(1),
            vec![sparrow_model::Field::new(
                sparrow_model::FieldId::new(1),
                "device_id",
                sparrow_model::DataType::Utf8,
                false,
            )],
        )
        .unwrap();
        let err = mqtt_config(&src, schema.clone(), None, "cred").unwrap_err();
        assert_eq!(err.code, ErrorCode::PolicyDenied);
        src.tls = true;
        let cfg = mqtt_config(&src, schema, None, "cred").unwrap();
        assert!(cfg.tls.enabled, "tls:true must be wired into MQTT config");
    }

    #[test]
    fn n16_http_header_secret_requires_https() {
        let mut sink = crate::spec::SinkSpec {
            kind: "http".into(),
            url: Some("http://127.0.0.1:8443/ingest".into()),
            skip_verify: false,
            outbox_capacity: 8,
            batch_rows: None,
            batch_bytes: None,
            linger_ms: None,
            max_inflight: None,
            use_demo_io: false,
            header_secret: Some("tok".into()),
            host: None,
            port: None,
            topic: None,
            client_id: None,
            qos: 0,
            clean_session: true,
            tls: false,
        };
        let err = http_config(&sink, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::PolicyDenied);
        sink.url = Some("https://127.0.0.1:8443/ingest".into());
        let cfg = http_config(&sink, None).unwrap();
        assert!(cfg.tls.enabled, "header_secret must imply TLS");
        assert_eq!(cfg.header_secret.as_deref(), Some("tok"));
    }
}

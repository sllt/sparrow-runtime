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
    let bound = if let Some(sql) = &spec.sql {
        bind_sql(sql, catalog, pipeline, rev)?
    } else if let Some(graph) = &spec.graph {
        bind_graph(graph, catalog)?
    } else {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "exactly one of sql or graph",
        ));
    };
    Ok(physicalize(&bound, &PlanOptions { fuse: true }))
}

pub fn replay_label_for_source(kind: &str) -> &'static str {
    match kind {
        "file" | "file_replay" | "replay" => "replayable",
        "mqtt" | "mqtt_source" | "http_push" | "http" => "unsupported",
        _ => sparrow_plan::REPLAY_UNBOUND,
    }
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
    match spec.source.kind.as_str() {
        "mqtt" => {
            refuse_durable_recovery(&spec.restore_claim()?).map_err(io)?;
            let mqtt = mqtt_config(&spec.source, schema.clone(), demo, "validate")?;
            mqtt.validate(secrets, policy).map_err(io)?;
        }
        "http_push" => {
            refuse_durable_recovery(&spec.restore_claim()?).map_err(io)?;
            let push = http_push_config(&spec.source, schema.clone())?;
            push.validate(secrets, policy).map_err(io)?;
        }
        "file" | "file_replay" | "replay" => {
            let path = spec.source.path.clone().ok_or_else(|| {
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
            cfg.contract = resolve_file_contract(spec, recovery)?;
            cfg.validate().map_err(io)?;
        }
        other => {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("source kind `{other}` is not supported (mqtt|http_push|file)"),
            ));
        }
    }
    match spec.sink.kind.as_str() {
        "http" => {
            let http = http_config(&spec.sink, demo)?;
            http.validate(secrets, policy).map_err(io)?;
        }
        "log" => {}
        "mqtt" => {
            let mqtt = mqtt_sink_config(&spec.sink, demo)?;
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

/// Aligned recovery: honor Filter/Project on the Kernel path; reject
/// dishonest plans (PT windows, Dedup, Lookup) rather than strip stages (P0-1/P0-2/A1).
pub fn validate_aligned_plan(
    spec: &PipelineSpec,
    plan: &PhysicalPlan,
) -> sparrow_model::Result<()> {
    let recovery = RecoveryPolicy::parse(&spec.recovery)?;
    if !recovery.is_aligned() {
        return Ok(());
    }
    if !matches!(spec.source.kind.as_str(), "file" | "file_replay" | "replay") {
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
    let mut has_window = false;
    for stage in &plan.stages {
        match stage {
            sparrow_plan::PhysicalStage::WindowAgg { spec: w, .. } => {
                has_window = true;
                if matches!(
                    w.kind,
                    sparrow_model::WindowKind::TumblingProcessingTime { .. }
                ) {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "processing-time windows cannot use recovery=aligned (capabilities: recovery_pt_window=restart_fresh; no PT timer on the aligned path)",
                    ));
                }
            }
            sparrow_plan::PhysicalStage::Deduplicate { .. }
            | sparrow_plan::PhysicalStage::Lookup { .. } => {
                return Err(SparrowError::new(
                    ErrorCode::FeatureUnavailable,
                    "aligned recovery does not snapshot Dedup/Lookup; refuse dishonest strip",
                ));
            }
            _ => {}
        }
    }
    if !has_window {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "aligned recovery requires a window operator in the plan",
        ));
    }
    Ok(())
}

pub fn capabilities_json() -> serde_json::Value {
    let mqtt = ConnectorCapabilities::MQTT_SOURCE;
    let http = ConnectorCapabilities::HTTP_SINK;
    let push = ConnectorCapabilities::HTTP_PUSH;
    let mqtt_sink = ConnectorCapabilities::MQTT_SINK;
    let file = ConnectorCapabilities::FILE_REPLAY;
    serde_json::json!({
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
    let replayable = matches!(spec.source.kind.as_str(), "file" | "file_replay" | "replay");
    let replay = if replayable {
        ReplaySupport::Replayable.as_str()
    } else {
        ReplaySupport::Unsupported.as_str()
    };
    let recovery_risk = if recovery.is_aligned() && replayable {
        "committed_checkpoint_only"
    } else if replayable {
        "restart_fresh_loses_in_memory_state"
    } else {
        "no_durable_restore; live_best_effort_drops_ok"
    };
    serde_json::json!({
        "delivery": DeliveryGuarantee::LiveBestEffort.as_str(),
        "recovery": recovery.as_str(),
        "replay": replay,
        "exactly_once": false,
        "recovery_risk": recovery_risk,
        "aligned_eligible": replayable,
        "honesty": HONESTY,
    })
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
    fn rejects_checkpoint_and_at_least_once() {
        let mut spec = PipelineSpec {
            version: 1,
            stream: "sensors".into(),
            sql: Some("SELECT device_id FROM sensors".into()),
            graph: None,
            source: SourceSpec {
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
        assert_eq!(g["recovery_risk"], "committed_checkpoint_only");
    }

    #[test]
    fn v02_default_mqtt_client_id_is_unique_per_instance() {
        let src = SourceSpec {
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

use std::sync::Arc;

use sparrow_connectors::{
    refuse_delivery_name, refuse_durable_recovery, refuse_qos_durable, ConnectorCapabilities,
    HttpSinkConfig, MqttSourceConfig, ReplaySupport, SecretResolver, TargetPolicy, TlsConfig,
};
use sparrow_model::{
    DeliveryGuarantee, ErrorCode, PipelineId, RecoveryPolicy, RestoreClaim, Result, RevisionId,
    Schema, SchemaId, SparrowError,
};
use sparrow_plan::catalog::schema_from_fields;
use sparrow_plan::{bind_graph, physicalize, Catalog, PhysicalPlan, PhysicalStage, PlanOptions};
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
}

pub const HONESTY: &str =
    "V0.1 is live_best_effort + restart_fresh. MQTT replay is unsupported. A process restart is a fresh attempt, not checkpoint restore.";

pub fn honesty_json() -> serde_json::Value {
    serde_json::json!({
        "delivery": DeliveryGuarantee::LiveBestEffort.as_str(),
        "recovery": RecoveryPolicy::RestartFresh.as_str(),
        "replay": ReplaySupport::Unsupported.as_str(),
        "honesty": HONESTY,
    })
}

pub fn stream_schema(name: &str, spec: &StreamSpec) -> Result<Schema> {
    let id = SchemaId::new(fnv(name));
    schema_from_fields(&spec.fields, id)
}

pub fn stream_to_schema(row: &StreamRow) -> Result<Schema> {
    let spec: StreamSpec = serde_json::from_str(&row.schema_json).map_err(|e| {
        SparrowError::new(ErrorCode::InvalidSchema, format!("stream schema: {e}"))
    })?;
    stream_schema(&row.name, &spec)
}

pub fn binder_catalog(store: &Store) -> Result<Catalog> {
    let mut cat = Catalog::new();
    for row in store.list_streams()? {
        cat.insert(row.name.clone(), stream_to_schema(&row)?);
    }
    Ok(cat)
}

pub fn bind_plan(spec: &PipelineSpec, catalog: &Catalog, name: &str, revision: u64) -> Result<PhysicalPlan> {
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

pub fn explain_plan(plan: &PhysicalPlan) -> ExplainReport {
    let stages = plan
        .stages
        .iter()
        .map(|s| match s {
            PhysicalStage::MemorySource { name, .. } => format!("source:{name}"),
            PhysicalStage::Transform { steps } => {
                let kinds: Vec<&str> = steps
                    .iter()
                    .map(|st| match st {
                        sparrow_plan::TransformStep::Filter { .. } => "filter",
                        sparrow_plan::TransformStep::Project { .. } => "project",
                        sparrow_plan::TransformStep::Map { .. } => "map",
                    })
                    .collect();
                format!("transform:{}", kinds.join("+"))
            }
            PhysicalStage::CaptureSink { name, .. } => format!("sink:{name}"),
        })
        .collect();
    ExplainReport {
        accepted: true,
        stages,
        fused: plan.fused(),
        mailbox_count: plan.mailbox_count(),
        delivery: DeliveryGuarantee::LiveBestEffort.as_str(),
        recovery: RecoveryPolicy::RestartFresh.as_str(),
        replay: ReplaySupport::Unsupported.as_str(),
        honesty: HONESTY,
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
    refuse_durable_recovery(&spec.restore_claim()?).map_err(io)?;
    if spec.source.kind != "mqtt" {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("source kind `{}` is not supported in V0.1 (mqtt only)", spec.source.kind),
        ));
    }
    if spec.sink.kind != "http" && spec.sink.kind != "log" {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!("sink kind `{}` is not supported (http|log)", spec.sink.kind),
        ));
    }
    let mqtt = mqtt_config(&spec.source, schema.clone(), demo)?;
    mqtt.validate(secrets, policy).map_err(io)?;
    if spec.sink.kind == "http" {
        let http = http_config(&spec.sink, demo)?;
        http.validate(secrets, policy).map_err(io)?;
    }
    Ok(())
}

pub fn mqtt_config(
    source: &SourceSpec,
    schema: Schema,
    demo: Option<&DemoEndpoints>,
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
    cfg.client_id = source
        .client_id
        .clone()
        .unwrap_or_else(|| "sparrow-source".into());
    cfg.qos = source.qos;
    cfg.clean_session = source.clean_session;
    cfg.username_secret = source.username_secret.clone();
    cfg.password_secret = source.password_secret.clone();
    cfg.tls = TlsConfig {
        enabled: false,
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
    let mut cfg = HttpSinkConfig::demo(url);
    cfg.outbox_capacity = sink.outbox_capacity;
    cfg.header_secret = sink.header_secret.clone();
    cfg.tls = TlsConfig {
        enabled: false,
        skip_verify: sink.skip_verify,
    };
    cfg.restore = RestoreClaim::None;
    Ok(cfg)
}

pub fn capabilities_json() -> serde_json::Value {
    let mqtt = ConnectorCapabilities::MQTT_SOURCE;
    let http = ConnectorCapabilities::HTTP_SINK;
    serde_json::json!({
        "delivery": DeliveryGuarantee::LiveBestEffort.as_str(),
        "recovery": RecoveryPolicy::RestartFresh.as_str(),
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
            }
        ],
        "honesty": HONESTY,
    })
}

pub fn reject_named_delivery(name: &str) -> Result<DeliveryGuarantee> {
    refuse_delivery_name(name).map_err(io)
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
            },
            sink: crate::spec::SinkSpec {
                kind: "http".into(),
                url: Some("http://127.0.0.1:1/".into()),
                skip_verify: false,
                outbox_capacity: 8,
                use_demo_io: false,
                header_secret: None,
            },
            delivery: "at_least_once".into(),
            recovery: "restart_fresh".into(),
            restore: None,
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
    }
}

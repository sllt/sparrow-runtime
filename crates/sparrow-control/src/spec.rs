use serde::{Deserialize, Serialize};
use sparrow_model::{
    DeliveryGuarantee, ErrorCode, RecoveryPolicy, RestoreClaim, Result, SparrowError,
};
use sparrow_plan::graph::GraphSpec;

pub const PIPELINE_SPEC_VERSION: u32 = 1;
pub const MAX_SQL_BYTES: usize = 8 * 1024;
pub const MAX_SPEC_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    #[serde(default = "one")]
    pub version: u32,
    pub stream: String,
    /// Immutable reference revisions. Publishing a newer table never changes
    /// the data observed by this pipeline revision.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub reference_tables: std::collections::BTreeMap<String, ReferenceBinding>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub external_lookups: std::collections::BTreeMap<String, crate::lookup::ExternalLookupSpec>,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub graph: Option<GraphSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_io: Option<GraphIoSpec>,
    pub source: SourceSpec,
    pub sink: SinkSpec,
    #[serde(default = "live")]
    pub delivery: String,
    #[serde(default = "fresh")]
    pub recovery: String,
    #[serde(default)]
    pub restore: Option<RestoreSpec>,
    /// Directory for aligned File/replay checkpoints. Defaults to `{path}.sparrow-chk`.
    #[serde(default)]
    pub checkpoint_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<crate::checkpoint::CheckpointSpec>,
    /// P1-17: decode errors fail the job instead of only incrementing
    /// `IoDiagnostics.decode_errors`. Also enabled by `SPARROW_FAIL_ON_DECODE=1`.
    #[serde(default)]
    pub fail_on_decode: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceBinding {
    pub revision: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub follow_latest: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphIoSpec {
    pub sources: std::collections::BTreeMap<u32, SourceSpec>,
    pub sinks: std::collections::BTreeMap<u32, SinkSpec>,
    /// Explicit paused-time inactivity policy for durable File time graphs.
    /// None never infers idle from an empty append-only file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_after_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<sparrow_expr::plugins::extension::Binding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jetstream: Option<JetStreamSpec>,
    /// Required exclusively for `kind = "http_poll"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_poll: Option<HttpPollSpec>,
    /// Required exclusively for `kind = "nats"` (NATS Core, not JetStream).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nats: Option<NatsSourceSpec>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default = "default_topic")]
    pub topic: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub qos: u8,
    #[serde(default = "default_true")]
    pub clean_session: bool,
    #[serde(default)]
    pub username_secret: Option<String>,
    #[serde(default)]
    pub password_secret: Option<String>,
    #[serde(default)]
    pub skip_verify: bool,
    #[serde(default = "default_inbox")]
    pub inbox_capacity: usize,
    /// MQTT only: bounded full-inbox wait in milliseconds (default 5, 0..=1000).
    /// Zero restores immediate drop. This does not enable reliable delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_quickack: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
    #[serde(default)]
    pub use_demo_io: bool,
    #[serde(default)]
    pub bind: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    /// MQTT/HTTP TLS. Credentials require this to be true (P0-11).
    #[serde(default)]
    pub tls: bool,
    /// File growth + EOF contract: `append_only`, `sealed`, or `immutable`.
    /// Default is `append_only` (poll on EOF, no terminal watermark) for both
    /// aligned and restart_fresh. Set `sealed` on finite fixtures that must
    /// emit final ET windows and then complete.
    #[serde(default)]
    pub file_contract: Option<String>,
}

/// K2 is opt-in and intentionally narrower than File aligned recovery. The
/// wire shape remains readable on feature-off binaries, which reject it before
/// creating a connection, reader or checkpoint generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JetStreamSpec {
    pub servers: Vec<String>,
    pub namespace: String,
    pub stream: String,
    pub consumer: String,
    pub ownership_bucket: String,
    #[serde(default)]
    pub token_secret: Option<String>,
    #[serde(default = "js_pending")]
    pub max_pending: usize,
    #[serde(default = "js_pending_bytes")]
    pub pending_bytes: usize,
    #[serde(default = "js_pull")]
    pub pull_messages: usize,
    #[serde(default = "js_pull_bytes")]
    pub pull_bytes: usize,
    /// Operational tuning for the regular reliable actor only; not a new
    /// delivery/restore policy. None preserves the existing 250ms idle cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_backoff_max_ms: Option<u64>,
}
fn js_pending() -> usize {
    128
}
fn js_pending_bytes() -> usize {
    256 * 1024
}
fn js_pull() -> usize {
    8
}
fn js_pull_bytes() -> usize {
    72 * 1024
}
impl JetStreamSpec {
    #[cfg(feature = "jetstream")]
    pub(crate) fn connection(&self) -> sparrow_connectors::jetstream::ConnectionConfig {
        sparrow_connectors::jetstream::ConnectionConfig {
            servers: self.servers.clone(),
            token_secret: self.token_secret.clone(),
            subscription_capacity: self.pull_messages + 2,
            pull_bytes: self.pull_bytes,
        }
    }
    #[cfg(feature = "jetstream")]
    pub(crate) fn reader(&self) -> sparrow_connectors::jetstream::ReaderConfig {
        sparrow_connectors::jetstream::ReaderConfig {
            namespace: self.namespace.clone(),
            stream: self.stream.clone(),
            consumer: self.consumer.clone(),
            ownership_bucket: self.ownership_bucket.clone(),
            max_pending: self.max_pending,
            pending_bytes: self.pending_bytes,
            pull_messages: self.pull_messages,
            pull_bytes: self.pull_bytes,
        }
    }
}

/// NATS Core subscribe Source (at-most-once; distinct from `jetstream`).
/// The wire shape stays readable on builds without the `nats` feature, which
/// reject it before connecting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsSourceSpec {
    pub servers: Vec<String>,
    /// `*` tokens and a trailing `>` allowed.
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_group: Option<String>,
    /// SecretRef; requires `tls://` servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    /// Largest server `max_payload` accepted (default 65536).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<usize>,
    /// Source wire prefetch buffer in messages (default 8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_capacity: Option<usize>,
    /// Decoded-row Queue credit for the inbox; default 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
}

/// NATS Core publish Sink to one literal subject.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsSinkSpec {
    pub servers: Vec<String>,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    /// Largest encoded row accepted (default 65536); larger rows are dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<usize>,
    /// SDK command buffer in messages (default 8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_capacity: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_timeout_ms: Option<u64>,
    /// Shutdown budget for queued batches plus the final flush.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

#[cfg(feature = "nats")]
fn nats_client(
    servers: &[String],
    token_secret: &Option<String>,
    reconnect_attempts: Option<usize>,
    connect_timeout_ms: Option<u64>,
    max_payload_bytes: Option<usize>,
    capacity: Option<usize>,
) -> sparrow_connectors::NatsClientConfig {
    let mut c = sparrow_connectors::NatsClientConfig::new(servers.to_vec());
    c.token_secret = token_secret.clone();
    if let Some(n) = reconnect_attempts {
        c.reconnect_attempts = n;
    }
    if let Some(ms) = connect_timeout_ms {
        c.connect_timeout = std::time::Duration::from_millis(ms);
    }
    if let Some(n) = max_payload_bytes {
        c.max_payload_bytes = n;
    }
    if let Some(n) = capacity {
        c.capacity = n;
    }
    c
}

#[cfg(feature = "nats")]
impl NatsSourceSpec {
    pub fn client_config(&self) -> sparrow_connectors::NatsClientConfig {
        nats_client(
            &self.servers,
            &self.token_secret,
            self.reconnect_attempts,
            self.connect_timeout_ms,
            self.max_payload_bytes,
            self.subscription_capacity,
        )
    }

    pub fn connector_config(
        &self,
        schema: sparrow_model::Schema,
        inbox_capacity: usize,
        fail_on_decode: bool,
    ) -> sparrow_connectors::NatsSourceConfig {
        let mut c = sparrow_connectors::NatsSourceConfig::new(
            self.servers.clone(),
            self.subject.clone(),
            schema,
        );
        c.client = self.client_config();
        c.queue_group = self.queue_group.clone();
        if let Some(n) = self.inbox_bytes {
            c.inbox_bytes = n;
        }
        c.inbox_capacity = inbox_capacity;
        c.fail_on_decode = fail_on_decode;
        c
    }
}

#[cfg(feature = "nats")]
impl NatsSinkSpec {
    pub fn client_config(&self) -> sparrow_connectors::NatsClientConfig {
        nats_client(
            &self.servers,
            &self.token_secret,
            self.reconnect_attempts,
            self.connect_timeout_ms,
            self.max_payload_bytes,
            self.client_capacity,
        )
    }

    pub fn connector_config(&self, outbox_capacity: usize) -> sparrow_connectors::NatsSinkConfig {
        let mut c =
            sparrow_connectors::NatsSinkConfig::new(self.servers.clone(), self.subject.clone());
        c.client = self.client_config();
        if let Some(ms) = self.publish_timeout_ms {
            c.publish_timeout = std::time::Duration::from_millis(ms);
        }
        if let Some(ms) = self.flush_timeout_ms {
            c.flush_timeout = std::time::Duration::from_millis(ms);
        }
        c.outbox_capacity = outbox_capacity;
        c
    }
}

/// HTTP Poll Source options. Credentials are named secret references only;
/// the stored revision never contains a credential value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpPollSpec {
    pub url: String,
    pub interval_ms: u64,
    #[serde(default = "http_poll_timeout")]
    pub timeout_ms: u64,
    /// Failure backoff ceiling; defaults to max(interval_ms, 60000) and may not
    /// exceed max(interval_ms, 3600000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_max_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpPollAuthSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<HttpPollHeaderSpec>,
    /// `json` (object or array of objects) or `ndjson`.
    #[serde(default = "http_poll_format")]
    pub format: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<usize>,
    /// Opt-in ETag / Last-Modified conditional GET.
    #[serde(default, skip_serializing_if = "is_false")]
    pub conditional: bool,
    /// Decoded-row Queue credit for the inbox; default 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpPollAuthSpec {
    Bearer {
        token_secret: String,
    },
    Basic {
        username_secret: String,
        password_secret: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpPollHeaderSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_secret: Option<String>,
}

fn http_poll_timeout() -> u64 {
    5_000
}
fn http_poll_format() -> String {
    "json".into()
}

impl HttpPollSpec {
    /// Connector config for one Source. Static/secret/policy validation is
    /// the connector's `validate`; this only maps the wire shape.
    pub fn connector_config(
        &self,
        schema: sparrow_model::Schema,
        inbox_capacity: usize,
        fail_on_decode: bool,
    ) -> Result<sparrow_connectors::HttpPollSourceConfig> {
        use sparrow_connectors::{HttpPollAuth, HttpPollFormat, HttpPollHeader};
        let mut config = sparrow_connectors::HttpPollSourceConfig::new(self.url.clone(), schema);
        config.interval = std::time::Duration::from_millis(self.interval_ms);
        config.timeout = std::time::Duration::from_millis(self.timeout_ms);
        config.backoff_max = std::time::Duration::from_millis(
            self.backoff_max_ms
                .unwrap_or_else(|| self.interval_ms.max(60_000)),
        );
        config.auth = match &self.auth {
            None => HttpPollAuth::None,
            Some(HttpPollAuthSpec::Bearer { token_secret }) => HttpPollAuth::Bearer {
                token_secret: token_secret.clone(),
            },
            Some(HttpPollAuthSpec::Basic {
                username_secret,
                password_secret,
            }) => HttpPollAuth::Basic {
                username_secret: username_secret.clone(),
                password_secret: password_secret.clone(),
            },
        };
        config.headers = self
            .headers
            .iter()
            .map(|h| HttpPollHeader {
                name: h.name.clone(),
                value: h.value.clone(),
                value_secret: h.value_secret.clone(),
            })
            .collect();
        config.format = HttpPollFormat::parse(&self.format).map_err(SparrowError::from)?;
        if let Some(bytes) = self.max_response_bytes {
            config.max_response_bytes = bytes;
        }
        config.conditional = self.conditional;
        if let Some(bytes) = self.inbox_bytes {
            config.inbox_bytes = bytes;
        }
        config.inbox_capacity = inbox_capacity;
        config.restore = RestoreClaim::None;
        config.fail_on_decode = fail_on_decode;
        Ok(config)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<sparrow_expr::plugins::extension::Binding>,
    /// Required exclusively for `kind = "nats"` (NATS Core publish).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nats: Option<NatsSinkSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<Box<sparrow_formats::action::ActionSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<Box<FileSinkSpec>>,
    pub kind: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub skip_verify: bool,
    #[serde(default = "default_outbox")]
    pub outbox_capacity: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linger_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_inflight: Option<usize>,
    #[serde(default)]
    pub use_demo_io: bool,
    #[serde(default)]
    pub header_secret: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub qos: u8,
    #[serde(default = "default_true")]
    pub clean_session: bool,
    #[serde(default)]
    pub tls: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSinkSpec {
    pub directory: String,
    pub segment_bytes: u64,
    pub max_bytes: u64,
    pub max_files: usize,
    pub row_bytes: usize,
    #[serde(default)]
    pub sync_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreSpec {
    pub kind: String,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
}

fn one() -> u32 {
    1
}
fn live() -> String {
    "live_best_effort".into()
}
fn fresh() -> String {
    "restart_fresh".into()
}
fn default_topic() -> String {
    "sensors/json".into()
}
fn default_true() -> bool {
    true
}
fn default_inbox() -> usize {
    16
}
fn default_outbox() -> usize {
    16
}

pub fn fail_on_decode_from_env() -> bool {
    match std::env::var("SPARROW_FAIL_ON_DECODE") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

impl PipelineSpec {
    pub fn has_live_lookups(&self) -> bool {
        !self.external_lookups.is_empty()
            || self.reference_tables.values().any(|r| r.follow_latest)
    }
    pub fn has_external_plugins(&self) -> bool {
        self.source.plugin.is_some() || self.sink.plugin.is_some()
            || self.graph.as_ref().is_some_and(|g|g.nodes.iter().any(|n|n.plugin.is_some()))
            || self.graph_io.as_ref().is_some_and(|io|io.sources.values().any(|s|s.plugin.is_some())||io.sinks.values().any(|s|s.plugin.is_some()))
    }
    pub fn requires_explicit_restart(&self) -> bool {
        self.fixed_snapshot_id().is_some() || self.has_external_plugins() || self.has_live_lookups()
    }
    /// A numeric restore is an explicit replay operation, never an implicit
    /// request to replay the same history after failure/process restart.
    pub fn fixed_snapshot_id(&self) -> Option<u64> {
        self.restore
            .as_ref()
            .filter(|r| r.kind == "checkpoint")?
            .snapshot_id
            .as_deref()?
            .parse()
            .ok()
    }

    pub fn checkpoint_warnings(&self) -> Vec<&'static str> {
        let mut warnings = Vec::new();
        if self.has_live_lookups() {
            warnings.push("live_lookup_requires_explicit_start_after_failure_or_process_restart; updates_are_observed_not_replayed");
        }
        if self.has_external_plugins() {warnings.push("external_plugin_requires_explicit_start_after_failure_or_process_restart");}
        if self.fixed_snapshot_id().is_some() {
            warnings
                .push("fixed_snapshot_requires_explicit_start_after_failure_or_process_restart");
            if self.checkpoint.as_ref().is_some_and(|p| p.resume_latest) {
                warnings.push(
                    "fixed_snapshot_overrides_resume_latest_remove_fixed_restore_to_follow_current",
                );
            }
        } else if self.restore.is_none()
            && self
                .checkpoint
                .as_ref()
                .is_some_and(|p| p.interval_ms.is_some() && !p.resume_latest)
        {
            warnings.push("periodic_checkpoint_enabled_but_automatic_resume_disabled");
        }
        warnings
    }

    /// Spec flag or `SPARROW_FAIL_ON_DECODE=1|true|yes` (P1-17).
    pub fn effective_fail_on_decode(&self) -> bool {
        self.fail_on_decode || fail_on_decode_from_env()
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_SPEC_BYTES {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                format!("pipeline spec {}B exceeds {MAX_SPEC_BYTES}", bytes.len()),
            ));
        }
        let spec: Self = serde_json::from_slice(bytes).map_err(|e| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("pipeline spec JSON: {e}"),
            )
        })?;
        spec.validate()?;
        Ok(spec)
    }

    /// Spec-level checks used by PUT / bind. Rejects blank SQL.
    pub fn validate(&self) -> Result<()> {
        self.basic_check()
    }

    pub fn basic_check(&self) -> Result<()> {
        if self.reference_tables.len() + self.external_lookups.len() > 8 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "at most eight managed/external lookup bindings are allowed",
            ));
        }
        for (name, external) in &self.external_lookups {
            crate::store::check_name(name)?;
            external.validate(name)?;
            if name == &self.stream || self.reference_tables.contains_key(name) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    "external Lookup must not shadow a stream/reference binding",
                ));
            }
        }
        if self.has_live_lookups()
            && (self.delivery != "live_best_effort"
                || self.recovery != "restart_fresh"
                || self.restore.is_some()
                || self.checkpoint.is_some()
                || self.checkpoint_dir.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "live/remote Lookup is restart_fresh only; responses and update boundaries are not a replay log",
            ));
        }
        for (kind, binding) in [(&self.source.kind, &self.source.plugin), (&self.sink.kind, &self.sink.plugin)] {
            if (kind == "plugin") != binding.is_some() {
                return Err(SparrowError::new(ErrorCode::InvalidArgument, "plugin binding is required exclusively for kind=plugin"));
            }
            if let Some(binding) = binding { binding.validate()?; }
        }
        let external = self.source.plugin.is_some() || self.sink.plugin.is_some()
            || self.graph.as_ref().is_some_and(|g|g.nodes.iter().any(|n|n.plugin.is_some()));
        if external && (self.recovery != "restart_fresh" || self.delivery != "live_best_effort"
            || self.restore.is_some() || self.checkpoint.is_some() || self.checkpoint_dir.is_some()) {
            return Err(SparrowError::new(ErrorCode::UnsupportedRestore, "external plugins are live_best_effort/restart_fresh only; no durable acknowledgement or checkpoint"));
        }
        if self.source.plugin.is_some() && (self.source.host.is_some() || self.source.port.is_some()
            || self.source.path.is_some() || self.source.bind.is_some() || self.source.client_id.is_some()
            || self.source.username_secret.is_some() || self.source.password_secret.is_some()
            || self.source.use_demo_io || self.source.tls || self.source.skip_verify
            || self.source.file_contract.is_some() || self.source.qos != 0 || !self.source.clean_session
            || self.source.topic != default_topic() || self.source.inbox_wait_ms.is_some()
            || self.source.inbox_bytes.is_some() || self.source.tcp_quickack.is_some()) {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, "external source options belong exclusively in plugin.config"));
        }
        if self.sink.plugin.is_some() && (self.sink.action.is_some() || self.sink.file.is_some()
            || self.sink.url.is_some() || self.sink.host.is_some() || self.sink.port.is_some()
            || self.sink.topic.is_some() || self.sink.client_id.is_some() || self.sink.header_secret.is_some()
            || self.sink.use_demo_io || self.sink.tls || self.sink.skip_verify || self.sink.qos != 0
            || !self.sink.clean_session || self.sink.batch_rows.is_some() || self.sink.batch_bytes.is_some()
            || self.sink.linger_ms.is_some() || self.sink.max_inflight.is_some()) {
            return Err(SparrowError::new(ErrorCode::InvalidArgument, "external sink options belong exclusively in plugin.config; action rendering is not applied"));
        }
        if self.reference_tables.len() > 8 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "at most eight immutable reference table bindings are allowed",
            ));
        }
        for (name, binding) in &self.reference_tables {
            crate::store::check_name(name)?;
            if binding.revision == 0
                || binding.revision > i64::MAX as u64
                || binding.sha256.len() != 64
                || !binding
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "reference table binding requires a positive revision and lowercase SHA-256",
                ));
            }
            if name == &self.stream {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "reference table name must not shadow the source stream",
                ));
            }
        }
        if let Some(graph) = &self.graph {
            for node in &graph.nodes {
                if node.kind == "memory_source"
                    && node
                        .table
                        .as_ref()
                        .is_some_and(|table| {
                            self.reference_tables.contains_key(table)
                                || self.external_lookups.contains_key(table)
                        })
                {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidSchema,
                        "graph memory_source must not shadow a managed reference table",
                    ));
                }
            }
        }
        if !self.reference_tables.is_empty() && RecoveryPolicy::parse(&self.recovery)?.is_aligned()
        {
            // Reference-dependent aligned profiles are selected after binding:
            // v8 is linear stateless File, v9 is linear File with supported
            // Count/IoT state, v10 is linear JetStream, and v11 is a required
            // File/HTTP graph.  Keep only the connector/checkpoint envelope at
            // this spec layer; PhysicalPlan validation owns the state/topology
            // matrix and prevents a future operator from being opened here by
            // accident.
            if !matches!(
                self.source.kind.as_str(),
                "file" | "file_replay" | "replay" | "jetstream"
            ) || self.sink.kind != "http"
                || self.checkpoint_dir.as_deref().is_none_or(str::is_empty)
            {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "reference-table checkpoint profile requires replayable File/JetStream input, explicit checkpoint_dir and a required HTTP sink",
                ));
            }
        }
        if let Some(io) = &self.graph_io {
            // SQL bounded stream joins also bind to an explicit graph. The
            // physical binder below validates exact ports/topology, so ordinary
            // linear SQL still cannot smuggle unused graph I/O bindings.
            if (self.graph.is_none() && self.sql.is_none())
                || io.sources.is_empty()
                || io.sinks.is_empty()
                || io.sources.len() > 16
                || io.sinks.len() > 16
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "graph_io requires SQL/Graph and 1..16 explicit source/sink bindings",
                ));
            }
            if io.sources.values().next() != Some(&self.source)
                || io.sinks.values().next() != Some(&self.sink)
            {
                return Err(SparrowError::new(ErrorCode::InvalidArgument,"legacy source/sink must match the lowest graph_io operator IDs (no shadow configuration)"));
            }
            for source in io.sources.values() {
                if source.kind == "jetstream" {
                    return Err(SparrowError::new(
                        ErrorCode::FeatureUnavailable,
                        "JetStream remains on its tested linear reliability profile",
                    ));
                }
                let mut single = self.clone();
                single.graph_io = None;
                single.source = source.clone();
                single.basic_check()?;
            }
            for sink in io.sinks.values() {
                let mut single = self.clone();
                single.graph_io = None;
                single.sink = sink.clone();
                single.basic_check()?;
            }
        }
        if self.source.jetstream.is_some() != (self.source.kind == "jetstream") {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.jetstream is required exclusively for kind=jetstream",
            ));
        }
        if self.source.http_poll.is_some() != (self.source.kind == "http_poll") {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.http_poll is required exclusively for kind=http_poll",
            ));
        }
        if self.source.kind == "http_poll"
            && (self.source.jetstream.is_some()
                || self.source.plugin.is_some()
                || self.source.host.is_some()
                || self.source.port.is_some()
                || self.source.path.is_some()
                || self.source.bind.is_some()
                || self.source.client_id.is_some()
                || self.source.username_secret.is_some()
                || self.source.password_secret.is_some()
                || self.source.use_demo_io
                || self.source.tls
                || self.source.skip_verify
                || self.source.file_contract.is_some()
                || self.source.qos != 0
                || !self.source.clean_session
                || self.source.topic != default_topic())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "HTTP poll options belong in source.http_poll (TLS follows the https:// URL); mixed connector fields refused",
            ));
        }
        if self.source.kind == "http_poll"
            && (self.delivery != "live_best_effort"
                || self.recovery != "restart_fresh"
                || self.restore.is_some()
                || self.checkpoint.is_some()
                || self.checkpoint_dir.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "HTTP poll is live_best_effort/restart_fresh only (replay=unsupported); no checkpoint or restore",
            ));
        }
        self.check_nats()?;
        if self
            .source
            .jetstream
            .as_ref()
            .and_then(|js| js.idle_backoff_max_ms)
            .is_some_and(|ms| !(5..=250).contains(&ms))
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "JetStream idle_backoff_max_ms must be 5..250; default 250",
            ));
        }
        if self.source.kind == "jetstream" {
            #[cfg(not(feature = "jetstream"))]
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "JetStream support requires the jetstream build feature",
            ));
            #[cfg(feature = "jetstream")]
            {
                self.source
                    .jetstream
                    .as_ref()
                    .expect("checked JetStream config")
                    .reader()
                    .validate()?;
                if self.delivery != "checkpointed_at_least_once"
                    || self.recovery != "aligned"
                    || self.sink.kind != "http"
                    || self.sink.skip_verify
                    || self.source.skip_verify
                    || self.checkpoint_dir.as_deref().is_none_or(str::is_empty)
                    || !self
                        .checkpoint
                        .as_ref()
                        .is_some_and(|p| p.resume_latest && p.interval_ms.is_some())
                    || self.restore.as_ref().is_some_and(|r| {
                        r.kind != "checkpoint"
                            || r.snapshot_id
                                .as_deref()
                                .is_some_and(|id| id != "aligned" && !id.is_empty())
                    })
                {
                    return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"JetStream requires aligned, explicit checkpoint_dir, periodic resume_latest, verified HTTP sink; fixed replay/reset is not yet exposed"));
                }
                if self.source.host.is_some()
                    || self.source.port.is_some()
                    || self.source.path.is_some()
                    || self.source.bind.is_some()
                    || self.source.client_id.is_some()
                    || self.source.username_secret.is_some()
                    || self.source.password_secret.is_some()
                    || self.source.use_demo_io
                    || self.source.tls
                    || self.source.file_contract.is_some()
                    || self.source.qos != 0
                    || !self.source.clean_session
                    || self.source.topic != default_topic()
                {
                    return Err(SparrowError::new(ErrorCode::InvalidArgument,"JetStream connection options belong in source.jetstream; mixed connector fields refused"));
                }
            }
        }
        if !(1..=4096).contains(&self.source.inbox_capacity)
            || !(1..=4096).contains(&self.sink.outbox_capacity)
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "source inbox and sink outbox must be 1..=4096 items",
            ));
        }
        if let Some(policy) = &self.checkpoint {
            policy.validate()?;
            if self.recovery != "aligned"
                || !matches!(
                    self.source.kind.as_str(),
                    "file" | "file_replay" | "replay" | "jetstream"
                )
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "checkpoint policy requires aligned File/replay or JetStream",
                ));
            }
        }
        if self.version != PIPELINE_SPEC_VERSION {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("pipeline spec version {} unsupported", self.version),
            ));
        }
        if self.stream.is_empty() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "stream is required",
            ));
        }
        if (self.source.tcp_quickack.is_some() || self.source.inbox_bytes.is_some())
            && self.source.kind != "mqtt"
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "tcp_quickack and inbox_bytes are MQTT-only",
            ));
        }
        if self.sink.kind != "http"
            && (self.sink.batch_rows.is_some()
                || self.sink.batch_bytes.is_some()
                || self.sink.linger_ms.is_some()
                || self.sink.max_inflight.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "batch/linger/max_inflight are HTTP-only",
            ));
        }
        if let Some(ms) = self.source.inbox_wait_ms {
            if self.source.kind != "mqtt" {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "source.inbox_wait_ms is only supported for MQTT",
                ));
            }
            if ms > 1000 {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "MQTT inbox_wait_ms must be in 0..=1000",
                ));
            }
        }
        match (&self.sql, &self.graph) {
            (Some(sql), None) => {
                if sql.trim().is_empty() {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "SQL must not be empty",
                    ));
                }
                if sql.len() > MAX_SQL_BYTES {
                    return Err(SparrowError::new(
                        ErrorCode::MaxRecordSize,
                        format!("SQL {}B exceeds {MAX_SQL_BYTES}", sql.len()),
                    ));
                }
            }
            (None, Some(_)) => {}
            _ => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "exactly one of `sql` or `graph` is required",
                ));
            }
        }
        Ok(())
    }

    /// NATS Core is live, at-most-once: refuse JetStream-like claims and
    /// mixed connector fields on either side.
    fn check_nats(&self) -> Result<()> {
        if self.source.nats.is_some() != (self.source.kind == "nats")
            || self.sink.nats.is_some() != (self.sink.kind == "nats")
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.nats / sink.nats are required exclusively for kind=nats",
            ));
        }
        let source = self.source.kind == "nats";
        let sink = self.sink.kind == "nats";
        if !source && !sink {
            return Ok(());
        }
        if !cfg!(feature = "nats") {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "NATS Core support requires the nats build feature",
            ));
        }
        if source
            && (self.source.jetstream.is_some()
                || self.source.http_poll.is_some()
                || self.source.plugin.is_some()
                || self.source.host.is_some()
                || self.source.port.is_some()
                || self.source.path.is_some()
                || self.source.bind.is_some()
                || self.source.client_id.is_some()
                || self.source.username_secret.is_some()
                || self.source.password_secret.is_some()
                || self.source.use_demo_io
                || self.source.tls
                || self.source.skip_verify
                || self.source.file_contract.is_some()
                || self.source.qos != 0
                || !self.source.clean_session
                || self.source.topic != default_topic())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "NATS options belong in source.nats (TLS follows tls:// servers); mixed connector fields refused",
            ));
        }
        if sink
            && (self.sink.plugin.is_some()
                || self.sink.action.is_some()
                || self.sink.file.is_some()
                || self.sink.url.is_some()
                || self.sink.skip_verify
                || self.sink.use_demo_io
                || self.sink.header_secret.is_some()
                || self.sink.host.is_some()
                || self.sink.port.is_some()
                || self.sink.topic.is_some()
                || self.sink.client_id.is_some()
                || self.sink.qos != 0
                || !self.sink.clean_session
                || self.sink.tls)
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "NATS options belong in sink.nats (static subject, no actions); mixed connector fields refused",
            ));
        }
        if self.delivery != "live_best_effort"
            || self.recovery != "restart_fresh"
            || self.restore.is_some()
            || self.checkpoint.is_some()
            || self.checkpoint_dir.is_some()
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "NATS Core is live_best_effort/restart_fresh, at-most-once (no ack, no replay); use the JetStream profile for checkpointed delivery",
            ));
        }
        Ok(())
    }

    pub fn restore_claim(&self) -> Result<RestoreClaim> {
        match &self.restore {
            None => Ok(RestoreClaim::None),
            Some(r) => match r.kind.as_str() {
                "none" | "" => Ok(RestoreClaim::None),
                "checkpoint" => Ok(RestoreClaim::Checkpoint {
                    snapshot_id: r.snapshot_id.clone().unwrap_or_default(),
                }),
                "mqtt_session" => Ok(RestoreClaim::MqttSession {
                    client_id: r.client_id.clone().unwrap_or_default(),
                }),
                other => Ok(RestoreClaim::External {
                    kind: other.into(),
                    detail: r.snapshot_id.clone().unwrap_or_default(),
                }),
            },
        }
    }

    pub fn check_delivery(&self) -> Result<(DeliveryGuarantee, RecoveryPolicy)> {
        let g = DeliveryGuarantee::parse(&self.delivery)?;
        if (g == DeliveryGuarantee::CheckpointedAtLeastOnce)
            != (cfg!(feature = "jetstream") && self.source.kind == "jetstream")
        {
            return Err(SparrowError::new(ErrorCode::UnsupportedDelivery,"checkpointed_at_least_once is required exclusively for an enabled JetStream profile"));
        }
        let r = RecoveryPolicy::parse(&self.recovery)?;
        let claim = self.restore_claim()?;
        let replayable = matches!(self.source.kind.as_str(), "file" | "file_replay" | "replay")
            || (cfg!(feature = "jetstream") && self.source.kind == "jetstream");
        sparrow_model::check_recovery_capabilities(&self.source.kind, replayable, r, &claim)?;
        Ok((g, r))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamSpec {
    pub fields: Vec<sparrow_plan::graph::FieldSpec>,
}

impl StreamSpec {
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_SPEC_BYTES {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                format!("stream spec {}B exceeds {MAX_SPEC_BYTES}", bytes.len()),
            ));
        }
        serde_json::from_slice(bytes).map_err(|e| {
            SparrowError::new(ErrorCode::InvalidArgument, format!("stream spec JSON: {e}"))
        })
    }
}

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
    /// Required exclusively for `kind = "databus"` (in-process topic bus).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub databus: Option<DataBusSourceSpec>,
    /// Required exclusively for `kind = "websocket"` (client mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket: Option<WebSocketSourceSpec>,
    /// Required exclusively for `kind = "kafka"` (consumer group).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka: Option<KafkaSourceSpec>,
    /// Required exclusively for `kind = "postgres"` (`postgres` build feature).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres: Option<Box<crate::postgres_spec::PostgresSourceSpec>>,
    /// Required exclusively for `kind = "tcp"` (client mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp: Option<TcpSourceSpec>,
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
    /// Payload/record format: `json` (default; NDJSON for File), `csv` or
    /// `protobuf`. See docs/FORMATS.md for the per-kind matrix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// CSV options; accepted only with `format = "csv"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csv: Option<sparrow_formats::CsvOptions>,
    /// Protobuf options; required with (and accepted only with)
    /// `format = "protobuf"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protobuf: Option<sparrow_formats::ProtobufOptions>,
}

/// Source kinds whose bytes carry a selectable record format.
pub const CSV_SOURCE_KINDS: &[&str] = &[
    "mqtt",
    "http_push",
    "http_poll",
    "nats",
    "jetstream",
    "websocket",
    "kafka",
    "tcp",
    "file",
    "file_replay",
    "replay",
];
/// Sink kinds whose bytes carry a selectable record format.
pub const CSV_SINK_KINDS: &[&str] = &["mqtt", "http", "nats", "jetstream", "websocket", "kafka", "tcp", "file"];
/// Source kinds that decode protobuf: one message per broker message /
/// WebSocket binary message / HTTP push body, or a length-delimited stream
/// per HTTP Poll response. File kinds are refused (newline framing only).
pub const PROTOBUF_SOURCE_KINDS: &[&str] =
    &["mqtt", "http_push", "http_poll", "nats", "jetstream", "websocket", "kafka"];
/// Sink kinds that encode protobuf (HTTP: a length-delimited stream body).
pub const PROTOBUF_SINK_KINDS: &[&str] =
    &["mqtt", "http", "nats", "jetstream", "websocket", "kafka"];

fn payload_format(
    side: &str,
    kind: &str,
    format: Option<&str>,
    csv: Option<&sparrow_formats::CsvOptions>,
    protobuf: Option<&sparrow_formats::ProtobufOptions>,
    role: sparrow_formats::CsvRole,
) -> Result<sparrow_formats::PayloadFormat> {
    let invalid = |message: String| SparrowError::new(ErrorCode::InvalidArgument, message);
    let name = format.unwrap_or("json");
    if csv.is_some() && name != "csv" {
        return Err(invalid(format!(
            "{side}.csv requires {side}.format = \"csv\""
        )));
    }
    if protobuf.is_some() && name != "protobuf" {
        return Err(invalid(format!(
            "{side}.protobuf requires {side}.format = \"protobuf\""
        )));
    }
    let supported = |kinds: &[&str], label: &str| {
        if kinds.contains(&kind) {
            Ok(())
        } else {
            Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                format!("{side} kind `{kind}` has no {label} format ({})", kinds.join("|")),
            ))
        }
    };
    let decode = role == sparrow_formats::CsvRole::Decode;
    match name {
        "json" => Ok(sparrow_formats::PayloadFormat::Json),
        "csv" => {
            supported(if decode { CSV_SOURCE_KINDS } else { CSV_SINK_KINDS }, "CSV")?;
            let options = csv.cloned().unwrap_or_default();
            Ok(sparrow_formats::PayloadFormat::csv(options.compile(role)?))
        }
        "protobuf" => {
            supported(
                if decode { PROTOBUF_SOURCE_KINDS } else { PROTOBUF_SINK_KINDS },
                "protobuf",
            )?;
            let options = protobuf.ok_or_else(|| {
                invalid(format!(
                    "{side}.format = \"protobuf\" requires {side}.protobuf (descriptor_set, message)"
                ))
            })?;
            Ok(sparrow_formats::PayloadFormat::protobuf(options.compile(role)?))
        }
        other => Err(invalid(format!(
            "{side}.format `{other}` is not supported (json|csv|protobuf)"
        ))),
    }
}

impl SourceSpec {
    /// The validated record format of this source (JSON unless `format=csv`).
    pub fn payload_format(&self) -> Result<sparrow_formats::PayloadFormat> {
        payload_format(
            "source",
            &self.kind,
            self.format.as_deref(),
            self.csv.as_ref(),
            self.protobuf.as_ref(),
            sparrow_formats::CsvRole::Decode,
        )
    }
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
            // The pipeline sets `source.format` on top (SourceSpec owns it).
            payload_format: sparrow_formats::PayloadFormat::Json,
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

/// Local DataBus subscription: another pipeline's `databus` Sink in the
/// same runtime publishes JSON rows decoded with this stream's schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataBusSourceSpec {
    /// Topic pattern; `*` = one token, trailing `>` = one or more.
    pub topic: String,
    /// Subscriber buffer in messages (default 1024).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_capacity: Option<usize>,
    /// Subscriber buffer payload bytes, charged to the job reservation
    /// (default 256 KiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<usize>,
    /// `drop_oldest` (default), `drop_newest` or `block`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overflow: Option<String>,
    /// `block` only: longest publisher wait per message (default 1000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_timeout_ms: Option<u64>,
    /// Decoded-row Queue credit for the inbox; default 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
}

impl DataBusSourceSpec {
    pub fn subscription(&self) -> Result<sparrow_connectors::databus::SubscriptionConfig> {
        use sparrow_connectors::databus::{source, Overflow, SubscriptionConfig};
        Ok(SubscriptionConfig {
            pattern: self.topic.clone(),
            capacity: self
                .buffer_capacity
                .unwrap_or(source::DEFAULT_BUFFER_MESSAGES),
            max_bytes: self.buffer_bytes.unwrap_or(source::DEFAULT_BUFFER_BYTES),
            overflow: match &self.overflow {
                Some(policy) => Overflow::parse(policy)?,
                None => Overflow::default(),
            },
            block_timeout: self.block_timeout_ms.map_or(
                source::DEFAULT_BLOCK_TIMEOUT,
                std::time::Duration::from_millis,
            ),
        })
    }

    pub fn connector_config(
        &self,
        schema: sparrow_model::Schema,
        inbox_capacity: usize,
        fail_on_decode: bool,
    ) -> Result<sparrow_connectors::DataBusSourceConfig> {
        let mut c = sparrow_connectors::DataBusSourceConfig::new(self.topic.clone(), schema);
        c.subscription = self.subscription()?;
        if let Some(n) = self.inbox_bytes {
            c.inbox_bytes = n;
        }
        c.inbox_capacity = inbox_capacity;
        c.fail_on_decode = fail_on_decode;
        Ok(c)
    }
}

/// Local DataBus publish to one literal topic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataBusSinkSpec {
    pub topic: String,
    /// Stop/EOF budget for publishing queued batches (default 1000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

impl DataBusSinkSpec {
    pub fn connector_config(
        &self,
        outbox_capacity: usize,
    ) -> sparrow_connectors::DataBusSinkConfig {
        let mut c = sparrow_connectors::DataBusSinkConfig::new(self.topic.clone());
        if let Some(ms) = self.flush_timeout_ms {
            c.flush_timeout = std::time::Duration::from_millis(ms);
        }
        c.outbox_capacity = outbox_capacity;
        c
    }
}

/// InfluxDB v2 write Sink (`POST <url>/api/v2/write`), live-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfluxDbSinkSpec {
    /// `https://host[:port][/prefix]`.
    pub url: String,
    pub org: String,
    pub bucket: String,
    /// Secret reference resolving to the API token.
    pub token_secret: String,
    /// PEM bundle that replaces the built-in roots (verification stays on).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
    /// Fixed measurement name; exclusive with `measurement_column`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement: Option<String>,
    /// `utf8` column holding each row's measurement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement_column: Option<String>,
    /// `utf8` columns written as tags.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Field columns; default: every column that is not the measurement
    /// column, a tag or the time column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    /// `timestamp` column; without it InfluxDB stamps points on arrival.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,
    /// `ns|us|ms|s` (default `us`); requires `time_column`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    #[serde(default)]
    pub gzip: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_initial_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_ms: Option<u64>,
    /// Stop budget shared by the request in flight and queued rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

impl InfluxDbSinkSpec {
    pub fn connector_config(
        &self,
        outbox_capacity: usize,
    ) -> Result<sparrow_connectors::InfluxDbSinkConfig> {
        use sparrow_connectors::influxdb::{InfluxMapping, Measurement, Precision};
        use std::time::Duration;
        let invalid = |m: &str| SparrowError::new(ErrorCode::InvalidArgument, m.to_string());
        let measurement = match (&self.measurement, &self.measurement_column) {
            (Some(m), None) => Measurement::Fixed(m.clone()),
            (None, Some(c)) => Measurement::Column(c.clone()),
            _ => {
                return Err(invalid(
                    "sink.influxdb needs exactly one of measurement / measurement_column",
                ))
            }
        };
        let precision = match (&self.precision, &self.time_column) {
            (None, _) => Precision::default(),
            (Some(_), None) => {
                return Err(invalid(
                    "sink.influxdb precision requires time_column (InfluxDB stamps points itself otherwise)",
                ))
            }
            (Some(p), Some(_)) => Precision::parse(p)
                .ok_or_else(|| invalid("sink.influxdb precision must be ns, us, ms or s"))?,
        };
        let mapping = InfluxMapping {
            measurement,
            tags: self.tags.clone(),
            fields: self.fields.clone(),
            time_column: self.time_column.clone(),
            precision,
        };
        let mut c = sparrow_connectors::InfluxDbSinkConfig::new(
            self.url.clone(),
            self.org.clone(),
            self.bucket.clone(),
            self.token_secret.clone(),
            mapping,
        );
        c.ca_pem = self.ca_pem.clone();
        c.gzip = self.gzip;
        c.outbox_capacity = outbox_capacity;
        if let Some(n) = self.batch_rows {
            c.batch_rows = n;
        }
        if let Some(n) = self.batch_bytes {
            c.batch_bytes = n;
        }
        if let Some(n) = self.max_retries {
            c.max_retries = n;
        }
        for (value, slot) in [
            (self.flush_interval_ms, &mut c.flush_interval),
            (self.timeout_ms, &mut c.timeout),
            (self.connect_timeout_ms, &mut c.connect_timeout),
            (self.retry_initial_ms, &mut c.retry_initial),
            (self.retry_max_ms, &mut c.retry_max),
            (self.flush_timeout_ms, &mut c.flush_timeout),
        ] {
            if let Some(ms) = value {
                *slot = Duration::from_millis(ms);
            }
        }
        Ok(c)
    }
}

/// JetStream publish Sink: PubAck-confirmed, at-least-once into an existing
/// stream that binds `subject`. Never creates streams.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JetStreamSinkSpec {
    pub servers: Vec<String>,
    pub stream: String,
    /// Literal subject bound by `stream`.
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<usize>,
    /// SDK command buffer in messages (default 4, separate from pending PubAcks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_capacity: Option<usize>,
    /// Per attempt: send plus PubAck wait (default 2000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_timeout_ms: Option<u64>,
    /// Messages awaiting a PubAck at once (default 8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_inflight_acks: Option<usize>,
    /// Extra attempts per message (default 5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    /// Stop/EOF budget for confirming already queued batches (default 5000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
    /// Output column (utf8/int64/uint64) sent as `Nats-Msg-Id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg_id_column: Option<String>,
}

#[cfg(feature = "jetstream")]
impl JetStreamSinkSpec {
    pub fn client_config(&self) -> sparrow_connectors::NatsClientConfig {
        let mut client = nats_client(
            &self.servers,
            &self.token_secret,
            self.reconnect_attempts,
            self.connect_timeout_ms,
            self.max_payload_bytes,
            self.client_capacity,
        );
        if self.client_capacity.is_none() {
            client.capacity =
                sparrow_connectors::jetstream::JetStreamSinkConfig::DEFAULT_CLIENT_CAPACITY;
        }
        client
    }

    pub fn connector_config(
        &self,
        outbox_capacity: usize,
    ) -> sparrow_connectors::jetstream::JetStreamSinkConfig {
        let mut c = sparrow_connectors::jetstream::JetStreamSinkConfig::new(
            self.servers.clone(),
            self.stream.clone(),
            self.subject.clone(),
        );
        c.client = self.client_config();
        if let Some(ms) = self.ack_timeout_ms {
            c.ack_timeout = std::time::Duration::from_millis(ms);
        }
        if let Some(n) = self.max_inflight_acks {
            c.max_inflight_acks = n;
        }
        if let Some(n) = self.max_retries {
            c.max_retries = n;
        }
        if let Some(ms) = self.flush_timeout_ms {
            c.flush_timeout = std::time::Duration::from_millis(ms);
        }
        c.msg_id_column = self.msg_id_column.clone();
        c.outbox_capacity = outbox_capacity;
        c
    }
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

/// How a WebSocket Source maps one message to records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketFramingSpec {
    #[default]
    Message,
    Ndjson,
}

/// What a WebSocket Source does with binary messages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketBinarySpec {
    #[default]
    Drop,
    Decode,
}

/// Frame type of every message a WebSocket Sink sends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketFrameSpec {
    #[default]
    Text,
    Binary,
}

/// What a WebSocket Sink does when its send queue is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketOverflowSpec {
    #[default]
    Block,
    DropNewest,
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// WebSocket client Source (`ws://` / `wss://`), live and at-most-once.
/// Credentials are SecretRefs only and require `wss://`. The wire shape
/// stays readable on builds without the `websocket` feature, which reject it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketSourceSpec {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpPollAuthSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<HttpPollHeaderSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subprotocols: Vec<String>,
    /// PEM CA bundle replacing the built-in roots (private CA / self-signed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_max_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    /// Largest message/frame accepted (default 65536, at most 1 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub framing: WebSocketFramingSpec,
    #[serde(default, skip_serializing_if = "is_default")]
    pub binary_frames: WebSocketBinarySpec,
    /// Decoded-row Queue credit for the inbox; default 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
    /// Complete-message wire prefetch (default 4, 1..=64); full = drop newest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch_capacity: Option<usize>,
}

/// WebSocket client Sink: one message per row, live and at-most-once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketSinkSpec {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpPollAuthSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<HttpPollHeaderSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subprotocols: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_max_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    /// Largest encoded row sent (default 65536); larger rows are dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub frame: WebSocketFrameSpec,
    /// Bounded send queue in messages (default 16).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub overflow: WebSocketOverflowSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_timeout_ms: Option<u64>,
    /// Shutdown budget for queued rows plus the Close frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

#[cfg(feature = "websocket")]
#[allow(clippy::too_many_arguments)]
fn websocket_client(
    url: &str,
    auth: &Option<HttpPollAuthSpec>,
    headers: &[HttpPollHeaderSpec],
    subprotocols: &[String],
    tls_ca_pem: &Option<String>,
    connect_timeout_ms: Option<u64>,
    ping_interval_ms: Option<u64>,
    idle_timeout_ms: Option<u64>,
    reconnect_max_ms: Option<u64>,
    reconnect_attempts: Option<usize>,
    max_message_bytes: Option<usize>,
) -> sparrow_connectors::websocket::WebSocketClientConfig {
    use sparrow_connectors::websocket::{WebSocketAuth, WebSocketHeader};
    use std::time::Duration;
    let mut c = sparrow_connectors::websocket::WebSocketClientConfig::new(url);
    c.auth = match auth {
        None => WebSocketAuth::None,
        Some(HttpPollAuthSpec::Bearer { token_secret }) => WebSocketAuth::Bearer {
            token_secret: token_secret.clone(),
        },
        Some(HttpPollAuthSpec::Basic {
            username_secret,
            password_secret,
        }) => WebSocketAuth::Basic {
            username_secret: username_secret.clone(),
            password_secret: password_secret.clone(),
        },
    };
    c.headers = headers
        .iter()
        .map(|h| WebSocketHeader {
            name: h.name.clone(),
            value: h.value.clone(),
            value_secret: h.value_secret.clone(),
        })
        .collect();
    c.subprotocols = subprotocols.to_vec();
    c.tls_ca_pem = tls_ca_pem.clone();
    if let Some(ms) = connect_timeout_ms {
        c.connect_timeout = Duration::from_millis(ms);
    }
    if let Some(ms) = ping_interval_ms {
        c.ping_interval = Duration::from_millis(ms);
    }
    if let Some(ms) = idle_timeout_ms {
        c.idle_timeout = Duration::from_millis(ms);
    }
    if let Some(ms) = reconnect_max_ms {
        c.reconnect_max = Duration::from_millis(ms);
    }
    if let Some(n) = reconnect_attempts {
        c.reconnect_attempts = n;
    }
    if let Some(n) = max_message_bytes {
        c.max_message_bytes = n;
    }
    c
}

#[cfg(feature = "websocket")]
impl WebSocketSourceSpec {
    pub fn reservation(&self) -> usize {
        sparrow_connectors::WebSocketSourceConfig::reservation_for(
            &self.client_config(),
            self.prefetch_capacity.unwrap_or(sparrow_connectors::websocket::DEFAULT_PREFETCH_CAPACITY),
        )
    }

    pub fn client_config(&self) -> sparrow_connectors::websocket::WebSocketClientConfig {
        websocket_client(
            &self.url,
            &self.auth,
            &self.headers,
            &self.subprotocols,
            &self.tls_ca_pem,
            self.connect_timeout_ms,
            self.ping_interval_ms,
            self.idle_timeout_ms,
            self.reconnect_max_ms,
            self.reconnect_attempts,
            self.max_message_bytes,
        )
    }

    pub fn connector_config(
        &self,
        schema: sparrow_model::Schema,
        inbox_capacity: usize,
        fail_on_decode: bool,
    ) -> sparrow_connectors::WebSocketSourceConfig {
        use sparrow_connectors::websocket::{BinaryFrames, WebSocketFraming};
        let mut c = sparrow_connectors::WebSocketSourceConfig::new(self.url.clone(), schema);
        c.client = self.client_config();
        c.framing = match self.framing {
            WebSocketFramingSpec::Message => WebSocketFraming::Message,
            WebSocketFramingSpec::Ndjson => WebSocketFraming::Ndjson,
        };
        c.binary_frames = match self.binary_frames {
            WebSocketBinarySpec::Drop => BinaryFrames::Drop,
            WebSocketBinarySpec::Decode => BinaryFrames::Decode,
        };
        if let Some(n) = self.inbox_bytes {
            c.inbox_bytes = n;
        }
        if let Some(n) = self.prefetch_capacity {
            c.prefetch_capacity = n;
        }
        c.inbox_capacity = inbox_capacity;
        c.fail_on_decode = fail_on_decode;
        c
    }
}

#[cfg(feature = "websocket")]
impl WebSocketSinkSpec {
    pub fn connector_config(
        &self,
        outbox_capacity: usize,
    ) -> sparrow_connectors::WebSocketSinkConfig {
        use sparrow_connectors::websocket::{Overflow, SinkFrame};
        use std::time::Duration;
        let mut c = sparrow_connectors::WebSocketSinkConfig::new(self.url.clone());
        c.client = websocket_client(
            &self.url,
            &self.auth,
            &self.headers,
            &self.subprotocols,
            &self.tls_ca_pem,
            self.connect_timeout_ms,
            self.ping_interval_ms,
            self.idle_timeout_ms,
            self.reconnect_max_ms,
            self.reconnect_attempts,
            self.max_message_bytes,
        );
        c.frame = match self.frame {
            WebSocketFrameSpec::Text => SinkFrame::Text,
            WebSocketFrameSpec::Binary => SinkFrame::Binary,
        };
        c.overflow = match self.overflow {
            WebSocketOverflowSpec::Block => Overflow::Block,
            WebSocketOverflowSpec::DropNewest => Overflow::DropNewest,
        };
        if let Some(n) = self.queue_capacity {
            c.queue_capacity = n;
        }
        if let Some(ms) = self.send_timeout_ms {
            c.send_timeout = Duration::from_millis(ms);
        }
        if let Some(ms) = self.flush_timeout_ms {
            c.flush_timeout = Duration::from_millis(ms);
        }
        c.outbox_capacity = outbox_capacity;
        c
    }
}

/// Where a Kafka Source starts a partition with no committed offset.
/// Required: there is no implicit default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaOffsetResetSpec {
    Earliest,
    Latest,
    /// Fail the Source instead of choosing a position.
    Error,
}

/// Kafka producer acknowledgement level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaAcksSpec {
    #[default]
    All,
    Leader,
}

/// Kafka consumer-group Source (plaintext brokers on the allowlist).
/// Offsets are committed to the group only past rows admitted into the job;
/// a commit made under another identity (cluster, topic, group, partition
/// count, format) or by another client is refused. The wire shape stays
/// readable on builds without the `kafka` feature, which reject it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceSpec {
    /// Bootstrap `host:port` list (1..=8), each on the target allowlist.
    pub brokers: Vec<String>,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: KafkaOffsetResetSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_check_interval_ms: Option<u64>,
    /// Largest record value accepted (default 65536, at most 1 MiB);
    /// larger records are poison.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_poll_interval_ms: Option<u64>,
    /// Bound on the final commit and consumer close at stop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_timeout_ms: Option<u64>,
    /// Decoded-row Queue credit for the inbox; default 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
}

/// Kafka producer Sink: one record per row, batch acknowledged after every
/// delivery report; idempotent `acks=all` unless `idempotence = false`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkSpec {
    pub brokers: Vec<String>,
    pub topic: String,
    /// Utf8/Bytes column used as the record key (NULL: no key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotence: Option<bool>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub acks: KafkaAcksSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_check_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_in_flight: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linger_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

#[cfg(feature = "kafka")]
fn kafka_client(
    brokers: &[String],
    client_id: &Option<String>,
    socket_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    policy_check_interval_ms: Option<u64>,
) -> sparrow_connectors::KafkaClientConfig {
    use std::time::Duration;
    let mut c = sparrow_connectors::KafkaClientConfig::new(brokers.to_vec());
    if let Some(id) = client_id {
        c.client_id = id.clone();
    }
    if let Some(ms) = socket_timeout_ms {
        c.socket_timeout = Duration::from_millis(ms);
    }
    if let Some(ms) = request_timeout_ms {
        c.request_timeout = Duration::from_millis(ms);
    }
    if let Some(ms) = policy_check_interval_ms {
        c.policy_check_interval = Duration::from_millis(ms);
    }
    c
}

#[cfg(feature = "kafka")]
impl KafkaSourceSpec {
    pub fn reservation(&self) -> usize {
        use sparrow_connectors::kafka::source::{DEFAULT_FETCH_MAX_BYTES, DEFAULT_PREFETCH_BYTES};
        sparrow_connectors::KafkaSourceConfig::reservation_for(
            self.prefetch_bytes.unwrap_or(DEFAULT_PREFETCH_BYTES),
            self.fetch_max_bytes.unwrap_or(DEFAULT_FETCH_MAX_BYTES),
        )
    }

    pub fn connector_config(
        &self,
        schema: sparrow_model::Schema,
        inbox_capacity: usize,
        fail_on_decode: bool,
    ) -> sparrow_connectors::KafkaSourceConfig {
        use sparrow_connectors::kafka::OffsetReset;
        use std::time::Duration;
        let reset = match self.auto_offset_reset {
            KafkaOffsetResetSpec::Earliest => OffsetReset::Earliest,
            KafkaOffsetResetSpec::Latest => OffsetReset::Latest,
            KafkaOffsetResetSpec::Error => OffsetReset::Error,
        };
        let mut c = sparrow_connectors::KafkaSourceConfig::new(
            self.brokers.clone(),
            self.topic.clone(),
            self.group_id.clone(),
            reset,
            schema,
        );
        c.client = kafka_client(
            &self.brokers,
            &self.client_id,
            self.socket_timeout_ms,
            self.request_timeout_ms,
            self.policy_check_interval_ms,
        );
        let ms = Duration::from_millis;
        if let Some(n) = self.max_message_bytes {
            c.max_message_bytes = n;
        }
        if let Some(n) = self.fetch_max_bytes {
            c.fetch_max_bytes = n;
        }
        if let Some(n) = self.prefetch_bytes {
            c.prefetch_bytes = n;
        }
        if let Some(v) = self.commit_interval_ms {
            c.commit_interval = ms(v);
        }
        if let Some(v) = self.session_timeout_ms {
            c.session_timeout = ms(v);
        }
        if let Some(v) = self.max_poll_interval_ms {
            c.max_poll_interval = ms(v);
        }
        if let Some(v) = self.stop_timeout_ms {
            c.stop_timeout = ms(v);
        }
        if let Some(n) = self.inbox_bytes {
            c.inbox_bytes = n;
        }
        c.inbox_capacity = inbox_capacity;
        c.fail_on_decode = fail_on_decode;
        c
    }
}

#[cfg(feature = "kafka")]
impl KafkaSinkSpec {
    pub fn connector_config(&self, outbox_capacity: usize) -> sparrow_connectors::KafkaSinkConfig {
        use sparrow_connectors::kafka::KafkaAcks;
        use std::time::Duration;
        let mut c = sparrow_connectors::KafkaSinkConfig::new(self.brokers.clone(), self.topic.clone());
        c.client = kafka_client(
            &self.brokers,
            &self.client_id,
            self.socket_timeout_ms,
            self.request_timeout_ms,
            self.policy_check_interval_ms,
        );
        c.key_column = self.key_column.clone();
        if let Some(v) = self.idempotence {
            c.idempotence = v;
        }
        c.acks = match self.acks {
            KafkaAcksSpec::All => KafkaAcks::All,
            KafkaAcksSpec::Leader => KafkaAcks::Leader,
        };
        let ms = Duration::from_millis;
        if let Some(n) = self.max_in_flight {
            c.max_in_flight = n;
        }
        if let Some(n) = self.queue_bytes {
            c.queue_bytes = n;
        }
        if let Some(v) = self.linger_ms {
            c.linger = ms(v);
        }
        if let Some(v) = self.delivery_timeout_ms {
            c.delivery_timeout = ms(v);
        }
        if let Some(n) = self.max_message_bytes {
            c.max_message_bytes = n;
        }
        if let Some(v) = self.flush_timeout_ms {
            c.flush_timeout = ms(v);
        }
        c.outbox_capacity = outbox_capacity;
        c
    }
}

/// TCP record framing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TcpFramingSpec {
    /// `\n`-terminated records (`\r\n` accepted).
    #[default]
    Lines,
    /// Big-endian length prefix (`length_bytes` 2 or 4), then the record.
    LengthPrefixed,
}

/// What a TCP Source does with a record over `max_frame_bytes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TcpOversizeSpec {
    #[default]
    Resync,
    Disconnect,
}

/// What a TCP Sink does when its send queue is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TcpOverflowSpec {
    #[default]
    Block,
    DropNewest,
}

/// TCP client Source, live and at-most-once. Optional TLS (`tls: true`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSourceSpec {
    pub host: String,
    pub port: u16,
    #[serde(default, skip_serializing_if = "is_default")]
    pub tls: bool,
    /// PEM CA bundle replacing the built-in roots (private CA / self-signed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    /// TCP keepalive idle time; off when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_max_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub framing: TcpFramingSpec,
    /// Length prefix width for `length_prefixed`: 2 or 4 (default 4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_bytes: Option<u8>,
    /// Largest record (default and maximum 65536).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub oversize: TcpOversizeSpec,
    /// No bytes for this long = dead peer (default 60000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_ms: Option<u64>,
    /// Decoded-row Queue credit for the inbox; default 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
}

/// TCP client Sink: one framed record per row, live and at-most-once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSinkSpec {
    pub host: String,
    pub port: u16,
    #[serde(default, skip_serializing_if = "is_default")]
    pub tls: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_max_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub framing: TcpFramingSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_bytes: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame_bytes: Option<usize>,
    /// Bounded send queue in frames (default 16).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub overflow: TcpOverflowSpec,
    /// Bound on writing one frame, partial writes included (default 5000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_timeout_ms: Option<u64>,
    /// Shutdown budget for queued rows (default 2000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
fn tcp_client(
    host: &str,
    port: u16,
    tls: bool,
    tls_ca_pem: &Option<String>,
    connect_timeout_ms: Option<u64>,
    keepalive_ms: Option<u64>,
    reconnect_max_ms: Option<u64>,
    reconnect_attempts: Option<usize>,
    framing: TcpFramingSpec,
    length_bytes: Option<u8>,
    max_frame_bytes: Option<usize>,
) -> sparrow_connectors::tcp::TcpClientConfig {
    use sparrow_connectors::tcp::{PrefixWidth, TcpFraming};
    use std::time::Duration;
    let mut c = sparrow_connectors::tcp::TcpClientConfig::new(host, port);
    c.tls = tls;
    c.tls_ca_pem = tls_ca_pem.clone();
    if let Some(ms) = connect_timeout_ms {
        c.connect_timeout = Duration::from_millis(ms);
    }
    c.keepalive = keepalive_ms.map(Duration::from_millis);
    if let Some(ms) = reconnect_max_ms {
        c.reconnect_max = Duration::from_millis(ms);
    }
    if let Some(n) = reconnect_attempts {
        c.reconnect_attempts = n;
    }
    c.framing = match framing {
        TcpFramingSpec::Lines => TcpFraming::Lines,
        TcpFramingSpec::LengthPrefixed => TcpFraming::LengthPrefixed,
    };
    // Other widths are refused by `check_tcp`.
    c.prefix_width = match length_bytes {
        Some(2) => PrefixWidth::U16,
        _ => PrefixWidth::U32,
    };
    // An omitted limit defaults to what the prefix can express (65535 for
    // length_bytes 2); an explicit value above it is refused by validate.
    c.max_frame_bytes = match max_frame_bytes {
        Some(n) => n,
        None if c.framing == TcpFraming::LengthPrefixed => {
            c.max_frame_bytes.min(c.prefix_width.max_len())
        }
        None => c.max_frame_bytes,
    };
    c
}

impl TcpSourceSpec {
    pub fn client_config(&self) -> sparrow_connectors::tcp::TcpClientConfig {
        tcp_client(
            &self.host,
            self.port,
            self.tls,
            &self.tls_ca_pem,
            self.connect_timeout_ms,
            self.keepalive_ms,
            self.reconnect_max_ms,
            self.reconnect_attempts,
            self.framing,
            self.length_bytes,
            self.max_frame_bytes,
        )
    }

    pub fn connector_config(
        &self,
        schema: sparrow_model::Schema,
        inbox_capacity: usize,
        fail_on_decode: bool,
    ) -> sparrow_connectors::TcpSourceConfig {
        use sparrow_connectors::tcp::OversizePolicy;
        let mut c = sparrow_connectors::TcpSourceConfig::new(self.host.clone(), self.port, schema);
        c.client = self.client_config();
        c.oversize = match self.oversize {
            TcpOversizeSpec::Resync => OversizePolicy::Resync,
            TcpOversizeSpec::Disconnect => OversizePolicy::Disconnect,
        };
        if let Some(ms) = self.idle_timeout_ms {
            c.idle_timeout = std::time::Duration::from_millis(ms);
        }
        if let Some(n) = self.inbox_bytes {
            c.inbox_bytes = n;
        }
        c.inbox_capacity = inbox_capacity;
        c.fail_on_decode = fail_on_decode;
        c
    }
}

impl TcpSinkSpec {
    pub fn connector_config(&self, outbox_capacity: usize) -> sparrow_connectors::TcpSinkConfig {
        use sparrow_connectors::tcp::TcpOverflow;
        use std::time::Duration;
        let mut c = sparrow_connectors::TcpSinkConfig::new(self.host.clone(), self.port);
        c.client = tcp_client(
            &self.host,
            self.port,
            self.tls,
            &self.tls_ca_pem,
            self.connect_timeout_ms,
            self.keepalive_ms,
            self.reconnect_max_ms,
            self.reconnect_attempts,
            self.framing,
            self.length_bytes,
            self.max_frame_bytes,
        );
        c.overflow = match self.overflow {
            TcpOverflowSpec::Block => TcpOverflow::Block,
            TcpOverflowSpec::DropNewest => TcpOverflow::DropNewest,
        };
        if let Some(n) = self.queue_capacity {
            c.queue_capacity = n;
        }
        if let Some(ms) = self.send_timeout_ms {
            c.send_timeout = Duration::from_millis(ms);
        }
        if let Some(ms) = self.flush_timeout_ms {
            c.flush_timeout = Duration::from_millis(ms);
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

/// Redis Sink (one command per row, pipelined), live-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisSinkSpec {
    /// `redis://host[:port][/db]` or `rediss://...` (TLS).
    pub url: String,
    /// ACL user name secret reference (requires `password_secret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_secret: Option<String>,
    /// Password secret reference (requires `rediss://`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_secret: Option<String>,
    /// PEM bundle that replaces the built-in roots (verification stays on).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    /// `set | hset | xadd | publish | lpush | rpush`.
    pub command: String,
    /// Key template (`{column}` placeholders); every command but `publish`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Channel template; `publish` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Column whose text is the value (set/publish/lpush/rpush, hset with
    /// `field`); default: the row as a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_column: Option<String>,
    /// `set` only: `PX` expiry in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// `hset` (one hash field per column) or `xadd` (stream entry fields).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    /// `hset` only: one templated hash field holding the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// `xadd` only: `MAXLEN` trim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maxlen: Option<u64>,
    /// `xadd` with `maxlen`: `MAXLEN ~` instead of exact `MAXLEN =`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub approximate: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_initial_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_ms: Option<u64>,
    /// Stop budget shared by the pipeline in flight and queued rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

impl RedisSinkSpec {
    pub fn command(&self) -> Result<sparrow_connectors::redis::RedisCommand> {
        use sparrow_connectors::redis::{HashFields, RedisCommand, RedisValue, Template};
        let invalid = |m: &str| SparrowError::new(ErrorCode::InvalidArgument, m.to_string());
        let template = |t: &str| Template::parse(t);
        let value = || match &self.value_column {
            Some(c) => RedisValue::Column(c.clone()),
            None => RedisValue::Json,
        };
        let command = self.command.as_str();
        let key = match (command, &self.key, &self.channel) {
            ("publish", None, Some(c)) => template(c)?,
            ("publish", _, _) => {
                return Err(invalid("sink.redis publish needs channel (and no key)"))
            }
            (_, Some(k), None) => template(k)?,
            _ => {
                return Err(invalid(
                    "sink.redis needs key (channel is for publish only)",
                ))
            }
        };
        let only = |allowed: bool, what: &str| {
            if allowed {
                Ok(())
            } else {
                Err(invalid(&format!(
                    "sink.redis {what} does not apply to command {command}"
                )))
            }
        };
        only(self.ttl_ms.is_none() || command == "set", "ttl_ms")?;
        only(self.field.is_none() || command == "hset", "field")?;
        only(
            self.fields.is_none() || matches!(command, "hset" | "xadd"),
            "fields",
        )?;
        only(self.maxlen.is_none() || command == "xadd", "maxlen")?;
        only(
            !self.approximate || self.maxlen.is_some(),
            "approximate (without maxlen)",
        )?;
        only(
            self.value_column.is_none() || command != "xadd",
            "value_column",
        )?;
        Ok(match command {
            "set" => RedisCommand::Set {
                key,
                value: value(),
                ttl: self.ttl_ms.map(std::time::Duration::from_millis),
            },
            "hset" => RedisCommand::Hset {
                key,
                fields: match (&self.fields, &self.field) {
                    (Some(columns), None) if self.value_column.is_none() => {
                        HashFields::Columns(columns.clone())
                    }
                    (None, Some(field)) => HashFields::Field {
                        field: template(field)?,
                        value: value(),
                    },
                    _ => {
                        return Err(invalid(
                            "sink.redis hset needs either fields, or field with an optional value_column",
                        ))
                    }
                },
            },
            "xadd" => RedisCommand::Xadd {
                key,
                fields: self
                    .fields
                    .clone()
                    .ok_or_else(|| invalid("sink.redis xadd needs fields"))?,
                maxlen: self.maxlen,
                approximate: self.approximate,
            },
            "publish" => RedisCommand::Publish {
                channel: key,
                value: value(),
            },
            "lpush" | "rpush" => RedisCommand::Push {
                key,
                value: value(),
                left: command == "lpush",
            },
            _ => {
                return Err(invalid(
                    "sink.redis command must be set, hset, xadd, publish, lpush or rpush",
                ))
            }
        })
    }

    pub fn connector_config(
        &self,
        outbox_capacity: usize,
    ) -> Result<sparrow_connectors::RedisSinkConfig> {
        use std::time::Duration;
        let mut target = sparrow_connectors::redis::RedisTarget::new(self.url.clone());
        target.username_secret = self.username_secret.clone();
        target.password_secret = self.password_secret.clone();
        target.ca_pem = self.ca_pem.clone();
        if let Some(ms) = self.connect_timeout_ms {
            target.connect_timeout = Duration::from_millis(ms);
        }
        let mut c = sparrow_connectors::RedisSinkConfig::new(target, self.command()?);
        c.outbox_capacity = outbox_capacity;
        if let Some(n) = self.pipeline_rows {
            c.pipeline_rows = n;
        }
        if let Some(n) = self.pipeline_bytes {
            c.pipeline_bytes = n;
        }
        if let Some(n) = self.max_retries {
            c.max_retries = n;
        }
        for (value, slot) in [
            (self.flush_interval_ms, &mut c.flush_interval),
            (self.timeout_ms, &mut c.timeout),
            (self.retry_initial_ms, &mut c.retry_initial),
            (self.retry_max_ms, &mut c.retry_max),
            (self.flush_timeout_ms, &mut c.flush_timeout),
        ] {
            if let Some(ms) = value {
                *slot = Duration::from_millis(ms);
            }
        }
        Ok(c)
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
    /// Required exclusively for `kind = "jetstream"` (PubAck-confirmed publish).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jetstream: Option<JetStreamSinkSpec>,
    /// Required exclusively for `kind = "databus"` (in-process topic bus).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub databus: Option<DataBusSinkSpec>,
    /// Required exclusively for `kind = "influxdb"` (InfluxDB v2 write).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub influxdb: Option<Box<InfluxDbSinkSpec>>,
    /// Required exclusively for `kind = "websocket"` (client mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket: Option<WebSocketSinkSpec>,
    /// Required exclusively for `kind = "kafka"` (producer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka: Option<KafkaSinkSpec>,
    /// Required exclusively for `kind = "redis"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis: Option<Box<RedisSinkSpec>>,
    /// Required exclusively for `kind = "postgres"` (`postgres` build feature).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres: Option<Box<crate::postgres_spec::PostgresSinkSpec>>,
    /// Required exclusively for `kind = "tcp"` (client mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp: Option<TcpSinkSpec>,
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
    /// Payload/record format: `json` (default; NDJSON for File), `csv` or
    /// `protobuf`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// CSV options; accepted only with `format = "csv"`. Decode-only options
    /// (trim, multiline, columns, ...) are refused on a sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csv: Option<sparrow_formats::CsvOptions>,
    /// Protobuf options; required with (and accepted only with)
    /// `format = "protobuf"`. Decode-only options are refused on a sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protobuf: Option<sparrow_formats::ProtobufOptions>,
}

impl SinkSpec {
    /// The validated record format of this sink (JSON unless `format=csv`).
    pub fn payload_format(&self) -> Result<sparrow_formats::PayloadFormat> {
        payload_format(
            "sink",
            &self.kind,
            self.format.as_deref(),
            self.csv.as_ref(),
            self.protobuf.as_ref(),
            sparrow_formats::CsvRole::Encode,
        )
    }
}

impl SinkSpec {
    /// HTTP/MQTT/File/plugin/action fields (on a sink of another kind).
    pub(crate) fn has_foreign_fields(&self) -> bool {
        self.plugin.is_some()
            || self.action.is_some()
            || self.file.is_some()
            || self.url.is_some()
            || self.skip_verify
            || self.use_demo_io
            || self.header_secret.is_some()
            || self.host.is_some()
            || self.port.is_some()
            || self.topic.is_some()
            || self.client_id.is_some()
            || self.qos != 0
            || !self.clean_session
            || self.tls
    }
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
        self.check_jetstream_sink()?;
        self.check_databus()?;
        self.check_influxdb()?;
        self.check_websocket()?;
        self.check_kafka()?;
        self.check_redis()?;
        self.check_postgres()?;
        self.check_tcp()?;
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
        if source && (self.source_has_foreign_fields() || self.source.databus.is_some()) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "NATS options belong in source.nats (TLS follows tls:// servers); mixed connector fields refused",
            ));
        }
        if sink && self.sink_has_foreign_fields() {
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

    /// MQTT/HTTP/File/plugin/JetStream fields on a NATS Core or DataBus source.
    fn source_has_foreign_fields(&self) -> bool {
        self.source.jetstream.is_some()
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
            || self.source.topic != default_topic()
    }

    /// Local DataBus is live, at-most-once and in-process only: no replay
    /// point exists, so durable claims are refused; mixed fields refused; a
    /// pipeline may not subscribe to a topic it publishes itself.
    fn check_databus(&self) -> Result<()> {
        if self.source.databus.is_some() != (self.source.kind == "databus")
            || self.sink.databus.is_some() != (self.sink.kind == "databus")
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.databus / sink.databus are required exclusively for kind=databus",
            ));
        }
        // A graph's lowest-ID legacy endpoints need not use DataBus. Inspect
        // all endpoints before the legacy fast path; per-endpoint recursion
        // cannot detect a non-legacy Source/Sink pair feeding itself.
        let (sources, sinks): (Vec<&SourceSpec>, Vec<&SinkSpec>) = match &self.graph_io {
            Some(io) => (io.sources.values().collect(), io.sinks.values().collect()),
            None => (vec![&self.source], vec![&self.sink]),
        };
        for pattern in sources.iter().filter_map(|s| s.databus.as_ref()) {
            for topic in sinks.iter().filter_map(|s| s.databus.as_ref()) {
                if sparrow_connectors::databus::patterns_overlap(&pattern.topic, &topic.topic) {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!(
                            "DataBus source `{}` would receive this pipeline's own sink topic `{}` (feedback loop)",
                            pattern.topic, topic.topic
                        ),
                    ));
                }
            }
        }
        let source = self.source.kind == "databus";
        let sink = self.sink.kind == "databus";
        let graph_databus = sources.iter().any(|s| s.kind == "databus")
            || sinks.iter().any(|s| s.kind == "databus");
        if !source && !sink && !graph_databus {
            return Ok(());
        }
        if source && (self.source_has_foreign_fields() || self.source.nats.is_some()) {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "DataBus options belong in source.databus; mixed connector fields refused",
            ));
        }
        if sink
            && (self.sink_has_foreign_fields()
                || self.sink.nats.is_some()
                || self.sink.jetstream.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "DataBus options belong in sink.databus (static topic, no actions); mixed connector fields refused",
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
                "Local DataBus is live_best_effort/restart_fresh, at-most-once (in-memory, no replay point); no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// Kafka: the resume position is the group's committed offset (bound to
    /// an identity), not a Sparrow checkpoint, so the job contract is
    /// live_best_effort/restart_fresh and checkpoint/restore are refused.
    fn check_kafka(&self) -> Result<()> {
        if self.source.kafka.is_some() != (self.source.kind == "kafka")
            || self.sink.kafka.is_some() != (self.sink.kind == "kafka")
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.kafka / sink.kafka are required exclusively for kind=kafka",
            ));
        }
        let source = self.source.kind == "kafka";
        let sink = self.sink.kind == "kafka";
        if !source && !sink {
            return Ok(());
        }
        if !cfg!(feature = "kafka") {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "Kafka support requires the kafka build feature",
            ));
        }
        if source
            && (self.source_has_foreign_fields()
                || self.source.nats.is_some()
                || self.source.databus.is_some()
                || self.source.websocket.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "Kafka options belong in source.kafka; mixed connector fields refused",
            ));
        }
        if sink
            && (self.sink_has_foreign_fields()
                || self.sink.nats.is_some()
                || self.sink.jetstream.is_some()
                || self.sink.databus.is_some()
                || self.sink.websocket.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "Kafka options belong in sink.kafka (one record per row, no actions); mixed connector fields refused",
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
                "Kafka is live_best_effort/restart_fresh: the source resumes from its group's committed offsets (only past admitted rows), not from a Sparrow checkpoint; no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// WebSocket (client mode) is live, at-most-once: no replay point, so
    /// durable claims are refused; connector fields must not be mixed.
    fn check_websocket(&self) -> Result<()> {
        if self.source.websocket.is_some() != (self.source.kind == "websocket")
            || self.sink.websocket.is_some() != (self.sink.kind == "websocket")
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.websocket / sink.websocket are required exclusively for kind=websocket",
            ));
        }
        let source = self.source.kind == "websocket";
        let sink = self.sink.kind == "websocket";
        if !source && !sink {
            return Ok(());
        }
        if !cfg!(feature = "websocket") {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "WebSocket support requires the websocket build feature",
            ));
        }
        if source
            && (self.source_has_foreign_fields()
                || self.source.nats.is_some()
                || self.source.databus.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "WebSocket options belong in source.websocket (TLS follows the wss:// URL); mixed connector fields refused",
            ));
        }
        if sink
            && (self.sink_has_foreign_fields()
                || self.sink.nats.is_some()
                || self.sink.jetstream.is_some()
                || self.sink.databus.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "WebSocket options belong in sink.websocket (one message per row, no actions); mixed connector fields refused",
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
                "WebSocket is live_best_effort/restart_fresh, at-most-once (no ack, no replay); no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// TCP (client mode) is live, at-most-once: no replay point, so durable
    /// claims are refused; connector fields must not be mixed.
    fn check_tcp(&self) -> Result<()> {
        // Public typed validators call check_delivery without basic_check.
        // Visit non-legacy graph endpoints as well, before the fast path.
        if let Some(io) = &self.graph_io {
            let mut single = self.clone();
            single.graph_io = None;
            for source in io.sources.values() {
                single.source = source.clone();
                single.check_tcp()?;
            }
            single.source = self.source.clone();
            for sink in io.sinks.values() {
                single.sink = sink.clone();
                single.check_tcp()?;
            }
        }
        if self.source.tcp.is_some() != (self.source.kind == "tcp")
            || self.sink.tcp.is_some() != (self.sink.kind == "tcp")
        {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "source.tcp / sink.tcp are required exclusively for kind=tcp",
            ));
        }
        let source = self.source.tcp.as_ref();
        let sink = self.sink.tcp.as_ref();
        if source.is_none() && sink.is_none() {
            return Ok(());
        }
        if source.is_some() && self.source_has_foreign_fields() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "TCP options (host, port, tls) belong in source.tcp; mixed connector fields refused",
            ));
        }
        if sink.is_some() && self.sink_has_foreign_fields() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "TCP options (host, port, tls) belong in sink.tcp (one record per row, no actions); mixed connector fields refused",
            ));
        }
        let widths = [
            source.map(|s| (s.framing, s.length_bytes)),
            sink.map(|s| (s.framing, s.length_bytes)),
        ];
        for (framing, length_bytes) in widths.into_iter().flatten() {
            if let Some(n) = length_bytes {
                if framing != TcpFramingSpec::LengthPrefixed || !(n == 2 || n == 4) {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "TCP length_bytes is 2 or 4 and only applies to framing=length_prefixed",
                    ));
                }
            }
        }
        if self.delivery != "live_best_effort"
            || self.recovery != "restart_fresh"
            || self.restore.is_some()
            || self.checkpoint.is_some()
            || self.checkpoint_dir.is_some()
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "TCP is live_best_effort/restart_fresh, at-most-once (no ack, no replay); no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// HTTP/MQTT/File/plugin/action fields on a NATS-family sink.
    fn sink_has_foreign_fields(&self) -> bool {
        self.sink.has_foreign_fields()
    }

    /// Redis Sink: live-only. Neither the target nor the command is bound
    /// into checkpoints, and XADD/PUBLISH/LPUSH/RPUSH are not idempotent, so
    /// every durable claim is refused, for the legacy and graph sinks alike.
    fn check_redis(&self) -> Result<()> {
        let graph: Vec<&SinkSpec> = self
            .graph_io
            .as_ref()
            .map(|io| io.sinks.values().collect())
            .unwrap_or_default();
        let mut any = false;
        for sink in std::iter::once(&self.sink).chain(graph) {
            if sink.redis.is_some() != (sink.kind == "redis") {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "sink.redis is required exclusively for sink kind=redis",
                ));
            }
            if sink.kind != "redis" {
                continue;
            }
            any = true;
            if sink.has_foreign_fields()
                || sink.nats.is_some()
                || sink.jetstream.is_some()
                || sink.databus.is_some()
                || sink.websocket.is_some()
                || sink.tcp.is_some()
                || sink.influxdb.is_some()
                || sink.batch_rows.is_some()
                || sink.batch_bytes.is_some()
                || sink.linger_ms.is_some()
                || sink.max_inflight.is_some()
                || sink.format.is_some()
                || sink.csv.is_some()
                || sink.protobuf.is_some()
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "Redis options belong in sink.redis (no actions); mixed connector fields refused",
                ));
            }
        }
        if any
            && (self.delivery != "live_best_effort"
                || self.recovery != "restart_fresh"
                || self.restore.is_some()
                || self.checkpoint.is_some()
                || self.checkpoint_dir.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "Redis Sink is live_best_effort/restart_fresh (no checkpoint binds the target; XADD/PUBLISH/LPUSH/RPUSH are not idempotent); no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// PostgreSQL Source/Sink: live-only. The tracking value is not a
    /// checkpoint replay point (a row committed late with a smaller value is
    /// skipped) and plain INSERT is not idempotent, so every durable claim is
    /// refused, for legacy and graph endpoints alike.
    fn check_postgres(&self) -> Result<()> {
        let (graph_sources, graph_sinks): (Vec<&SourceSpec>, Vec<&SinkSpec>) = self
            .graph_io
            .as_ref()
            .map(|io| (io.sources.values().collect(), io.sinks.values().collect()))
            .unwrap_or_default();
        let mut any = false;
        for source in std::iter::once(&self.source).chain(graph_sources) {
            if source.postgres.is_some() != (source.kind == "postgres") {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "source.postgres is required exclusively for source kind=postgres",
                ));
            }
            if source.kind != "postgres" {
                continue;
            }
            any = true;
            if source.jetstream.is_some()
                || source.http_poll.is_some()
                || source.nats.is_some()
                || source.databus.is_some()
                || source.websocket.is_some()
                || source.tcp.is_some()
                || source.plugin.is_some()
                || source.host.is_some()
                || source.port.is_some()
                || source.path.is_some()
                || source.bind.is_some()
                || source.client_id.is_some()
                || source.username_secret.is_some()
                || source.password_secret.is_some()
                || source.use_demo_io
                || source.tls
                || source.skip_verify
                || source.file_contract.is_some()
                || source.qos != 0
                || !source.clean_session
                || source.topic != default_topic()
                || source.inbox_wait_ms.is_some()
                || source.tcp_quickack.is_some()
                || source.inbox_bytes.is_some()
                || source.format.is_some()
                || source.csv.is_some()
                || source.protobuf.is_some()
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL options belong in source.postgres (rows come typed from the query; no format); mixed connector fields refused",
                ));
            }
        }
        for sink in std::iter::once(&self.sink).chain(graph_sinks) {
            if sink.postgres.is_some() != (sink.kind == "postgres") {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "sink.postgres is required exclusively for sink kind=postgres",
                ));
            }
            if sink.kind != "postgres" {
                continue;
            }
            any = true;
            if sink.has_foreign_fields()
                || sink.nats.is_some()
                || sink.jetstream.is_some()
                || sink.databus.is_some()
                || sink.websocket.is_some()
                || sink.tcp.is_some()
                || sink.influxdb.is_some()
                || sink.redis.is_some()
                || sink.batch_rows.is_some()
                || sink.batch_bytes.is_some()
                || sink.linger_ms.is_some()
                || sink.max_inflight.is_some()
                || sink.format.is_some()
                || sink.csv.is_some()
                || sink.protobuf.is_some()
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL options belong in sink.postgres (no actions, no format); mixed connector fields refused",
                ));
            }
        }
        if !any {
            return Ok(());
        }
        if !cfg!(feature = "postgres") {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "PostgreSQL support requires the postgres build feature",
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
                "PostgreSQL Source/Sink are live_best_effort/restart_fresh (the tracking value is not a replay point; INSERT is not idempotent); no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// InfluxDB Sink: live-only. Target identity (url/org/bucket/mapping) is
    /// not bound into checkpoints, so every durable claim is refused, for the
    /// legacy sink and for graph sinks alike.
    fn check_influxdb(&self) -> Result<()> {
        let graph: Vec<&SinkSpec> = self
            .graph_io
            .as_ref()
            .map(|io| io.sinks.values().collect())
            .unwrap_or_default();
        let mut any = false;
        for sink in std::iter::once(&self.sink).chain(graph) {
            if sink.influxdb.is_some() != (sink.kind == "influxdb") {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "sink.influxdb is required exclusively for sink kind=influxdb",
                ));
            }
            if sink.kind != "influxdb" {
                continue;
            }
            any = true;
            if sink.has_foreign_fields()
                || sink.nats.is_some()
                || sink.jetstream.is_some()
                || sink.databus.is_some()
                || sink.websocket.is_some()
                || sink.tcp.is_some()
                || sink.format.is_some()
                || sink.csv.is_some()
                || sink.protobuf.is_some()
                || sink.batch_rows.is_some()
                || sink.batch_bytes.is_some()
                || sink.linger_ms.is_some()
                || sink.max_inflight.is_some()
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "InfluxDB options belong in sink.influxdb (no actions); mixed connector fields refused",
                ));
            }
        }
        if any
            && (self.delivery != "live_best_effort"
                || self.recovery != "restart_fresh"
                || self.restore.is_some()
                || self.checkpoint.is_some()
                || self.checkpoint_dir.is_some())
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "InfluxDB Sink is live_best_effort/restart_fresh (no checkpoint binds the write target); no checkpoint or restore",
            ));
        }
        Ok(())
    }

    /// JetStream Sink: PubAck-confirmed, at-least-once into the stream. It
    /// may join an aligned checkpoint only on the linear File profile, whose
    /// barrier waits for the Sink outbox receipts (acked after PubAcks).
    fn check_jetstream_sink(&self) -> Result<()> {
        if self.sink.jetstream.is_some() != (self.sink.kind == "jetstream") {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "sink.jetstream is required exclusively for sink kind=jetstream",
            ));
        }
        if self.sink.kind != "jetstream" {
            return Ok(());
        }
        if !cfg!(feature = "jetstream") {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "JetStream Sink requires the jetstream build feature",
            ));
        }
        if self.sink.nats.is_some() || self.sink_has_foreign_fields() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "JetStream Sink options belong in sink.jetstream (static subject, no actions); mixed connector fields refused",
            ));
        }
        let durable = self.recovery != "restart_fresh"
            || self.restore.is_some()
            || self.checkpoint.is_some()
            || self.checkpoint_dir.is_some();
        if durable
            && (self.graph_io.is_some()
                || !matches!(self.source.kind.as_str(), "file" | "file_replay" | "replay"))
        {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "JetStream Sink joins aligned checkpoints only in the linear File profile; other checkpoint profiles require the HTTP sink",
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
        // Public IO validators accept typed/serde-created specs too, so they
        // must not rely solely on from_json/basic_check for the DataBus gate.
        self.check_databus()?;
        self.check_redis()?;
        self.check_postgres()?;
        self.check_influxdb()?;
        self.check_tcp()?;
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

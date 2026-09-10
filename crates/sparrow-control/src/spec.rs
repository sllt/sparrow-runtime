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
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub graph: Option<GraphSpec>,
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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    pub kind: String,
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
    #[serde(default)]
    pub use_demo_io: bool,
    #[serde(default)]
    pub bind: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    /// MQTT/HTTP TLS. Credentials require this to be true (P0-11).
    #[serde(default)]
    pub tls: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkSpec {
    pub kind: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub skip_verify: bool,
    #[serde(default = "default_outbox")]
    pub outbox_capacity: usize,
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

impl PipelineSpec {
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
        let r = RecoveryPolicy::parse(&self.recovery)?;
        let claim = self.restore_claim()?;
        let replayable = matches!(self.source.kind.as_str(), "file" | "file_replay" | "replay");
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

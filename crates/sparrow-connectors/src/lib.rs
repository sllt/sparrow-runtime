//! Production I/O adapters. MQTT/HTTP/file live here — never in
//! `sparrow-runtime` or `sparrow-model`.
//!
//! Default delivery is `live_best_effort` + `restart_fresh`. MQTT replay is
//! declared unsupported. File/replay source is Replayable for V1 aligned
//! checkpoint only. Buffers are bounded; a full inbox drops.

pub mod capabilities;
pub mod diag;
pub mod error;
pub mod file_replay;
pub mod http;
pub mod http_push;
pub mod log;
pub mod mqtt;
pub mod policy;
pub mod secret;
pub mod tls;

pub use capabilities::{
    refuse_delivery_name, refuse_durable_recovery, refuse_qos_durable, refuse_unsupported_recovery,
    ConnectorCapabilities, ReplaySupport,
};
pub use file_replay::{FileContract, FileReplayConfig, FileReplaySource};
pub use diag::{IoDiagnostics, IoSnapshot};
pub use error::{ConnectorError, Result};
pub use http::{HttpCapture, HttpSink, HttpSinkConfig};
pub use http_push::{HttpPushSource, HttpPushSourceConfig};
pub use log::{LogSink, LogSinkConfig};
pub use mqtt::{
    publish_qos0, publish_qos0_many, EmbeddedBroker, MqttSink, MqttSinkConfig, MqttSource,
    MqttSourceConfig,
};
pub use policy::{AllowedTarget, TargetPolicy};
pub use secret::{MapSecretResolver, SecretResolver};
pub use tls::TlsConfig;

/// Encode a sensor-shaped JSON event for the M2 demo publisher.
pub fn sensor_json(
    device_id: &str,
    temperature: f64,
    humidity: f64,
    ts: i64,
    alert: bool,
) -> Vec<u8> {
    serde_json::json!({
        "device_id": device_id,
        "temperature": temperature,
        "humidity": humidity,
        "ts": ts,
        "payload": {
            "temp": temperature,
            "humidity": humidity,
            "alert": alert
        }
    })
    .to_string()
    .into_bytes()
}

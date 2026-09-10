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
pub use diag::{IoDiagnostics, IoSnapshot};
pub use error::{ConnectorError, Result};
pub use file_replay::{FileContract, FilePoll, FileReplayConfig, FileReplaySource};
#[cfg(feature = "demo-io")]
pub use http::HttpCapture;
pub use http::{HttpSink, HttpSinkConfig};
pub use http_push::{HttpPushSource, HttpPushSourceConfig};
pub use log::{LogSink, LogSinkConfig};
#[cfg(feature = "demo-io")]
pub use mqtt::{publish_qos0, publish_qos0_many, EmbeddedBroker};
pub use mqtt::{MqttSink, MqttSinkConfig, MqttSource, MqttSourceConfig};
pub use policy::{
    check_bind_addr, check_data_path, check_data_path_in, configured_data_roots, data_roots,
    default_data_root, default_data_roots, ensure_default_data_root, AllowedTarget, TargetPolicy,
};
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

#[cfg(test)]
mod a6_tests {
    #[cfg(feature = "demo-io")]
    #[test]
    fn a6_demo_io_exports_embedded_broker_and_http_capture() {
        fn assert_exported(
            _: Option<crate::EmbeddedBroker>,
            _: Option<crate::HttpCapture>,
        ) {
        }
        assert_exported(None, None);
    }

    #[cfg(not(feature = "demo-io"))]
    #[test]
    fn a6_demo_io_disabled_has_no_demo_types() {
        // Production `--no-default-features` must compile this crate without
        // EmbeddedBroker / HttpCapture. This test existing is the proof.
    }
}

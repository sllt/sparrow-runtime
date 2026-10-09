use sparrow_model::{
    check_recovery_capabilities, DeliveryContract, DeliveryGuarantee, ErrorCode, RecoveryPolicy,
    RestoreClaim,
};

use crate::error::{ConnectorError, Result};

pub use sparrow_io::ReplaySupport;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectorCapabilities {
    pub kind: &'static str,
    pub replay: ReplaySupport,
    pub delivery: DeliveryGuarantee,
    pub recovery: RecoveryPolicy,
}

impl ConnectorCapabilities {
    pub const MQTT_SOURCE: Self = Self {
        kind: "mqtt",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    pub const HTTP_SINK: Self = Self {
        kind: "http",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    pub const LOG_SINK: Self = Self {
        kind: "log",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    pub const HTTP_PUSH: Self = Self {
        kind: "http_push",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// Periodic GET of a business API. A response is not a replay log.
    pub const HTTP_POLL: Self = Self {
        kind: "http_poll",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// NATS Core subscribe. At-most-once: no ack, no persistence, no replay.
    /// Distinct from the JetStream reliable profile.
    pub const NATS_SOURCE: Self = Self {
        kind: "nats",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// In-process DataBus subscribe. At-most-once: bounded buffer with an
    /// explicit overflow policy, no persistence, no replay.
    pub const DATABUS_SOURCE: Self = Self {
        kind: "databus",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// In-process DataBus publish. Done once the bus has offered the row to
    /// every matching subscriber; not a downstream processing receipt.
    pub const DATABUS_SINK: Self = Self {
        kind: "databus_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// Redis commands (SET/HSET/XADD/PUBLISH/LPUSH/RPUSH) acknowledged by
    /// their reply. Checkpoints/replay are refused (the target is not bound
    /// into checkpoints), so restarts start fresh.
    pub const REDIS_SINK: Self = Self {
        kind: "redis_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// InfluxDB v2 `/api/v2/write`. A batch is acknowledged after HTTP 204;
    /// checkpoints/replay are refused (target identity is not bound into
    /// checkpoints), so restarts start fresh.
    pub const INFLUXDB_SINK: Self = Self {
        kind: "influxdb_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// NATS Core publish. Handed to the client; no server receipt.
    pub const NATS_SINK: Self = Self {
        kind: "nats_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// JetStream publish confirmed by PubAck: at-least-once into the stream
    /// (duplicates possible on retry unless `Nats-Msg-Id` dedup applies).
    /// Participates in aligned checkpoints by acking the outbox only after
    /// every PubAck; it has no replay of its own.
    pub const JETSTREAM_SINK: Self = Self {
        kind: "jetstream_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::CheckpointedAtLeastOnce,
        recovery: RecoveryPolicy::Aligned,
    };

    /// WebSocket client Source. Live, at-most-once: no ack, no replay;
    /// messages sent while disconnected are lost.
    pub const WEBSOCKET_SOURCE: Self = Self {
        kind: "websocket",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// WebSocket client Sink. A sent frame is written to the socket; there
    /// is no application receipt.
    pub const WEBSOCKET_SINK: Self = Self {
        kind: "websocket_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// TCP client Source (`lines` / `length_prefixed`): live, no ack, no
    /// replay.
    pub const TCP_SOURCE: Self = Self {
        kind: "tcp",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// TCP client Sink. A sent frame is written to the socket; there is no
    /// application receipt.
    pub const TCP_SINK: Self = Self {
        kind: "tcp_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    pub const MQTT_SINK: Self = Self {
        kind: "mqtt_sink",
        replay: ReplaySupport::Unsupported,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    pub const FILE_REPLAY: Self = Self {
        kind: "file",
        replay: ReplaySupport::Replayable,
        delivery: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::Aligned,
    };
}

/// Rejects at-least-once, MQTT session, or any restore claim on the
/// default (non-experimental) path. MQTT/HTTP still use this.
pub fn refuse_durable_recovery(claim: &RestoreClaim) -> Result<()> {
    DeliveryContract::V0_4
        .validate_restore(claim)
        .map_err(|e| ConnectorError::new(e.code, e.to_string()))
}

/// Capability matrix entry point used by File replay and pipeline validate.
pub fn refuse_unsupported_recovery(
    source_kind: &str,
    replayable: bool,
    recovery: RecoveryPolicy,
    claim: &RestoreClaim,
) -> Result<()> {
    check_recovery_capabilities(source_kind, replayable, recovery, claim)
        .map_err(|e| ConnectorError::new(e.code, e.to_string()))
}

pub fn refuse_delivery_name(name: &str) -> Result<DeliveryGuarantee> {
    let guarantee=DeliveryGuarantee::parse(name).map_err(|e| ConnectorError::new(e.code, e.to_string()))?;
    if guarantee!=DeliveryGuarantee::LiveBestEffort {
        return Err(ConnectorError::new(ErrorCode::UnsupportedDelivery,"checkpointed delivery requires a validated JetStream pipeline; standalone live connectors cannot claim it"));
    }
    Ok(guarantee)
}

pub fn refuse_recovery_name(name: &str) -> Result<RecoveryPolicy> {
    RecoveryPolicy::parse(name).map_err(|e| ConnectorError::new(e.code, e.to_string()))
}

pub fn refuse_qos_durable(qos: u8) -> Result<()> {
    if qos == 0 {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ErrorCode::UnsupportedDelivery,
            format!(
                "MQTT QoS {qos} implies durable / at-least-once delivery; V1 is live_best_effort QoS 0 only (replay={})",
                ReplaySupport::Unsupported.as_str()
            ),
        ))
    }
}

pub fn refuse_dirty_session(clean_session: bool) -> Result<()> {
    if clean_session {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ErrorCode::UnsupportedRestore,
            "MQTT clean_session=false requests session restore; replay=unsupported so durable restore is rejected",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_declares_replay_unsupported() {
        assert_eq!(
            ConnectorCapabilities::MQTT_SOURCE.replay,
            ReplaySupport::Unsupported
        );
        assert!(refuse_qos_durable(1).is_err());
        assert!(refuse_dirty_session(false).is_err());
        assert_eq!(
            refuse_durable_recovery(&RestoreClaim::MqttSession {
                client_id: "edge".into()
            })
            .unwrap_err()
            .code(),
            ErrorCode::UnsupportedRestore
        );
        assert!(refuse_unsupported_recovery(
            "mqtt",
            false,
            RecoveryPolicy::Aligned,
            &RestoreClaim::Checkpoint {
                snapshot_id: "x".into()
            },
        )
        .is_err());
        assert!(refuse_unsupported_recovery(
            "file",
            true,
            RecoveryPolicy::Aligned,
            &RestoreClaim::Checkpoint {
                snapshot_id: "x".into()
            },
        )
        .is_ok());
    }
}

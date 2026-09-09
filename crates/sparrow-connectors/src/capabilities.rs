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
    DeliveryGuarantee::parse(name).map_err(|e| ConnectorError::new(e.code, e.to_string()))
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

use sparrow_model::{
    DeliveryContract, DeliveryGuarantee, ErrorCode, RecoveryPolicy, RestoreClaim,
};

use crate::error::{ConnectorError, Result};

/// Replay is not available for MQTT in V0.1. Declared so capability
/// negotiation can refuse durable-recovery configs instead of lying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplaySupport {
    Unsupported,
}

impl ReplaySupport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
        }
    }
}

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
}

/// Rejects at-least-once, checkpoint, MQTT session, or any restore claim.
pub fn refuse_durable_recovery(claim: &RestoreClaim) -> Result<()> {
    DeliveryContract::V0_1
        .validate_restore(claim)
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
                "MQTT QoS {qos} implies durable / at-least-once delivery; V0.1 is live_best_effort QoS 0 only (replay={})",
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
            "MQTT clean_session=false requests session restore; V0.1 is restart_fresh (replay=unsupported)",
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
    }
}

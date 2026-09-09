//! Explicit delivery and recovery vocabulary.
//!
//! V0.1 never claims exactly-once, MQTT session recovery, or checkpoint
//! restore. Unsupported restore claims are rejected at the contract boundary
//! rather than silently ignored.

use crate::error::{ErrorCode, Result, SparrowError};

/// Promises that apply while a job attempt is *live*.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeliveryGuarantee {
    /// Drop-ok live path. Records may be lost on backpressure, crash, or
    /// operator failure. This is the only supported V0.1 guarantee.
    LiveBestEffort,
}

impl DeliveryGuarantee {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveBestEffort => "live_best_effort",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "live_best_effort" | "LiveBestEffort" => Ok(Self::LiveBestEffort),
            "exactly_once" | "exactly-once" | "at_least_once" | "at-least-once" => {
                Err(SparrowError::new(
                    ErrorCode::UnsupportedDelivery,
                    format!("{name} is not a V0.1 delivery guarantee"),
                )
                .context("requested", name)
                .context("supported", Self::LiveBestEffort.as_str()))
            }
            other => Err(SparrowError::new(
                ErrorCode::UnsupportedDelivery,
                format!("unknown delivery guarantee '{other}'"),
            )),
        }
    }
}

/// What happens after a job or process restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecoveryPolicy {
    /// Start empty. No in-flight replay, no MQTT resume, no checkpoint load.
    RestartFresh,
}

impl RecoveryPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RestartFresh => "restart_fresh",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "restart_fresh" | "RestartFresh" => Ok(Self::RestartFresh),
            other => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("recovery policy '{other}' is not supported in V0.1"),
            )
            .context("supported", Self::RestartFresh.as_str())),
        }
    }
}

/// A restore claim presented by a catalog, connector, or operator.
///
/// Only [`RestoreClaim::None`] is accepted in V0.1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreClaim {
    None,
    Checkpoint { snapshot_id: String },
    MqttSession { client_id: String },
    External { kind: String, detail: String },
}

impl RestoreClaim {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Checkpoint { snapshot_id } => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "checkpoint restore is not available in V0.1",
            )
            .context("claim", "checkpoint")
            .context("snapshot_id", snapshot_id.clone())),
            Self::MqttSession { client_id } => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "MQTT session recovery is not available in V0.1",
            )
            .context("claim", "mqtt_session")
            .context("client_id", client_id.clone())),
            Self::External { kind, detail } => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("restore claim '{kind}' is not available in V0.1"),
            )
            .context("claim", kind.clone())
            .context("detail", detail.clone())),
        }
    }
}

/// Closed pair of live delivery + restart behaviour for a job attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryContract {
    pub guarantee: DeliveryGuarantee,
    pub recovery: RecoveryPolicy,
}

impl DeliveryContract {
    pub const V0_1: Self = Self {
        guarantee: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    pub fn validate_restore(&self, claim: &RestoreClaim) -> Result<()> {
        let _ = self;
        claim.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v0_rejects_exactly_once_and_mqtt_restore() {
        assert!(DeliveryGuarantee::parse("exactly_once").is_err());
        assert_eq!(
            RestoreClaim::MqttSession {
                client_id: "edge-1".into()
            }
            .validate()
            .unwrap_err()
            .code,
            ErrorCode::UnsupportedRestore
        );
        assert!(RestoreClaim::None.validate().is_ok());
        assert_eq!(DeliveryContract::V0_1.guarantee.as_str(), "live_best_effort");
        assert_eq!(DeliveryContract::V0_1.recovery.as_str(), "restart_fresh");
    }
}

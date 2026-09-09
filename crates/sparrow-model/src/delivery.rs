//! Explicit delivery and recovery vocabulary.
//!
//! Default remains `live_best_effort` + `restart_fresh`. V0.4 adds an
//! **experimental** aligned-checkpoint path. It is never advertised as
//! default exactly-once. MQTT without replay still cannot claim durable
//! restore.

use crate::error::{ErrorCode, Result, SparrowError};

/// Promises that apply while a job attempt is *live*.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeliveryGuarantee {
    /// Drop-ok live path. Records may be lost on backpressure, crash, or
    /// operator failure. This is the only supported live guarantee.
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
                    format!("{name} is not a Sparrow delivery guarantee (V0.4 is still live_best_effort; experimental checkpoint is not exactly-once)"),
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
    ///
    /// Processing-time and event-time windows advertise this as
    /// `recovery=none` unless the job opts into the experimental path.
    RestartFresh,
    /// Experimental aligned single-job checkpoint. **Not** production
    /// exactly-once. Requires a ReplayableSource. Recover from a
    /// *committed* manifest only.
    ExperimentalAligned,
}

impl RecoveryPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RestartFresh => "restart_fresh",
            Self::ExperimentalAligned => "experimental_aligned",
        }
    }

    /// Honesty label for PT/ET-window pipelines (`none` ≡ `restart_fresh`).
    pub const fn none_label(self) -> &'static str {
        match self {
            Self::RestartFresh => "none",
            Self::ExperimentalAligned => "experimental",
        }
    }

    pub const fn is_experimental(self) -> bool {
        matches!(self, Self::ExperimentalAligned)
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "restart_fresh" | "RestartFresh" | "none" | "None" => Ok(Self::RestartFresh),
            "experimental" | "experimental_aligned" | "ExperimentalAligned" => {
                Ok(Self::ExperimentalAligned)
            }
            "exactly_once" | "exactly-once" | "at_least_once" | "at-least-once" => {
                Err(SparrowError::new(
                    ErrorCode::UnsupportedDelivery,
                    format!("recovery policy '{name}' is not supported; experimental checkpoint is not exactly-once"),
                )
                .context("supported", "none | restart_fresh | experimental_aligned"))
            }
            "checkpoint" | "aligned" | "restore" => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "recovery policy '{name}' is not a production claim; use experimental_aligned (experimental, not exactly-once)"
                ),
            )
            .context("supported", "none | restart_fresh | experimental_aligned")),
            other => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("recovery policy '{other}' is not supported"),
            )
            .context("supported", "none | restart_fresh | experimental_aligned")),
        }
    }
}

/// A restore claim presented by a catalog, connector, or operator.
///
/// [`RestoreClaim::None`] is the default. [`RestoreClaim::Checkpoint`] is
/// accepted only with [`RecoveryPolicy::ExperimentalAligned`] **and** a
/// ReplayableSource (enforced by [`check_recovery_capabilities`]).
/// MQTT session restore is always rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreClaim {
    None,
    Checkpoint { snapshot_id: String },
    MqttSession { client_id: String },
    External { kind: String, detail: String },
}

impl RestoreClaim {
    /// Default-path validation: only `None` is accepted (restart_fresh).
    pub fn validate(&self) -> Result<()> {
        self.validate_with_policy(RecoveryPolicy::RestartFresh)
    }

    pub fn validate_with_policy(&self, policy: RecoveryPolicy) -> Result<()> {
        match (self, policy) {
            (Self::None, _) => Ok(()),
            (Self::Checkpoint { snapshot_id }, RecoveryPolicy::ExperimentalAligned) => {
                let _ = snapshot_id;
                Ok(())
            }
            (Self::Checkpoint { snapshot_id }, _) => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "checkpoint restore requires recovery=experimental_aligned (experimental; not exactly-once, not the default)",
            )
            .context("claim", "checkpoint")
            .context("snapshot_id", snapshot_id.clone())
            .context("policy", policy.as_str())),
            (Self::MqttSession { client_id }, _) => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "MQTT session recovery is unsupported (replay=unsupported; cannot pretend durable restore)",
            )
            .context("claim", "mqtt_session")
            .context("client_id", client_id.clone())),
            (Self::External { kind, detail }, _) => Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("restore claim '{kind}' is not available"),
            )
            .context("claim", kind.clone())
            .context("detail", detail.clone())),
        }
    }
}

/// Capability matrix: configs that require unsupported recovery are rejected.
///
/// `replayable` is the connector's declared ReplayableSource capability.
/// MQTT / HTTP push are never replayable.
pub fn check_recovery_capabilities(
    source_kind: &str,
    replayable: bool,
    recovery: RecoveryPolicy,
    claim: &RestoreClaim,
) -> Result<()> {
    claim.validate_with_policy(recovery)?;
    let mqtt_like = matches!(
        source_kind,
        "mqtt" | "mqtt_source" | "http_push" | "http"
    );
    if matches!(claim, RestoreClaim::MqttSession { .. }) {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            "MQTT session restore is rejected (replay=unsupported)",
        )
        .context("source", source_kind));
    }
    if recovery.is_experimental() && !replayable {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            format!(
                "recovery=experimental_aligned requires a ReplayableSource; {source_kind} declares replay=unsupported"
            ),
        )
        .context("source", source_kind)
        .context("experimental", "true"));
    }
    if matches!(claim, RestoreClaim::Checkpoint { .. }) && !replayable {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            format!(
                "checkpoint restore requires a ReplayableSource; {source_kind} cannot pretend durable restore"
            ),
        )
        .context("source", source_kind));
    }
    if mqtt_like && (recovery.is_experimental() || !matches!(claim, RestoreClaim::None)) {
        return Err(SparrowError::new(
            ErrorCode::UnsupportedRestore,
            format!(
                "{source_kind} is live_best_effort + replay=unsupported; experimental checkpoint / restore claims are rejected"
            ),
        )
        .context("source", source_kind));
    }
    Ok(())
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

    /// V0.2 live contract. Processing-time windows are `recovery=none`
    /// (`restart_fresh`): results after a crash are **not** identical.
    pub const V0_2: Self = Self {
        guarantee: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// V0.3 live contract. Event-time windows are still `recovery=none`
    /// unless a job opts into experimental aligned checkpoint.
    pub const V0_3: Self = Self {
        guarantee: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// V0.4 default live contract (still not exactly-once).
    pub const V0_4: Self = Self {
        guarantee: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::RestartFresh,
    };

    /// Experimental aligned-checkpoint contract. Not default. Not exactly-once.
    pub const V0_4_EXPERIMENTAL: Self = Self {
        guarantee: DeliveryGuarantee::LiveBestEffort,
        recovery: RecoveryPolicy::ExperimentalAligned,
    };

    pub const PT_WINDOW_HONESTY: &'static str =
        "processing-time windows are recovery=none / restart_fresh unless experimental_aligned is opted in; a default restart opens empty windows. Experimental checkpoint is not exactly-once.";

    pub const ET_WINDOW_HONESTY: &'static str =
        "event-time windows are recovery=none / restart_fresh by default; watermarks and open windows are not restored unless experimental_aligned + ReplayableSource. Final-only lateness uses output holdback. Exactly-once is rejected.";

    pub const EXPERIMENTAL_CHECKPOINT_HONESTY: &'static str =
        "experimental_aligned is an experimental single-job checkpoint path (barrier align, freeze+chunk write, manifest commit). Recover from committed manifests only. Not default exactly-once. MQTT without replay cannot pretend durable restore.";

    pub fn validate_restore(&self, claim: &RestoreClaim) -> Result<()> {
        claim.validate_with_policy(self.recovery)
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
        assert_eq!(RecoveryPolicy::parse("none").unwrap(), RecoveryPolicy::RestartFresh);
        assert_eq!(
            RecoveryPolicy::parse("checkpoint").unwrap_err().code,
            ErrorCode::UnsupportedRestore
        );
        assert!(DeliveryContract::V0_2.recovery.as_str() == "restart_fresh");
        assert!(DeliveryContract::PT_WINDOW_HONESTY.contains("recovery=none"));
    }

    #[test]
    fn experimental_checkpoint_is_opt_in_not_exactly_once() {
        assert_eq!(
            RecoveryPolicy::parse("experimental").unwrap(),
            RecoveryPolicy::ExperimentalAligned
        );
        assert!(RecoveryPolicy::ExperimentalAligned.is_experimental());
        assert!(RestoreClaim::Checkpoint {
            snapshot_id: "1".into()
        }
        .validate()
        .is_err());
        assert!(RestoreClaim::Checkpoint {
            snapshot_id: "1".into()
        }
        .validate_with_policy(RecoveryPolicy::ExperimentalAligned)
        .is_ok());
        assert!(check_recovery_capabilities(
            "mqtt",
            false,
            RecoveryPolicy::ExperimentalAligned,
            &RestoreClaim::Checkpoint {
                snapshot_id: "1".into()
            },
        )
        .is_err());
        assert!(check_recovery_capabilities(
            "file",
            true,
            RecoveryPolicy::ExperimentalAligned,
            &RestoreClaim::Checkpoint {
                snapshot_id: "chk-1".into()
            },
        )
        .is_ok());
        assert!(DeliveryGuarantee::parse("exactly_once").is_err());
    }
}

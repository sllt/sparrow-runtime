//! Shared client settings: bootstrap brokers under the target allowlist,
//! and the advertised-broker check run against cluster metadata.

use std::time::Duration;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::metadata::Metadata;
use sparrow_model::{ErrorCode, Result};

use super::error;
use crate::TargetPolicy;

pub const MAX_BROKERS: usize = 8;
const MAX_HOST_BYTES: usize = 253;

#[derive(Clone, Debug)]
pub struct KafkaClientConfig {
    /// Bootstrap brokers as `host:port` (no scheme). Every broker the
    /// cluster advertises must be on the target allowlist too.
    pub brokers: Vec<String>,
    pub client_id: String,
    /// librdkafka `socket.timeout.ms` (default 10 s, 1..=60 s).
    pub socket_timeout: Duration,
    /// Blocking metadata / committed-offset requests (default 10 s).
    pub request_timeout: Duration,
    /// How often advertised brokers are re-checked against the allowlist
    /// (default 30 s, 1..=600 s).
    pub policy_check_interval: Duration,
}

impl KafkaClientConfig {
    pub fn new(brokers: Vec<String>) -> Self {
        Self {
            brokers,
            client_id: "sparrow".into(),
            socket_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            policy_check_interval: Duration::from_secs(30),
        }
    }

    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        if !(1..=MAX_BROKERS).contains(&self.brokers.len()) {
            return Err(error(
                ErrorCode::BoundExceeded,
                format!("Kafka requires 1..={MAX_BROKERS} bootstrap brokers"),
            ));
        }
        super::check_name("client_id", &self.client_id, 64)?;
        let secs = |d: Duration, lo: u64, hi: u64| {
            (Duration::from_secs(lo)..=Duration::from_secs(hi)).contains(&d)
        };
        if !secs(self.socket_timeout, 1, 60)
            || !secs(self.request_timeout, 1, 60)
            || !secs(self.policy_check_interval, 1, 600)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "Kafka timeouts: socket/request 1..=60 s, policy_check_interval 1..=600 s",
            ));
        }
        for broker in &self.brokers {
            let (host, port) = parse_broker(broker)?;
            policy
                .check_host_port(host, port)
                .map_err(|e| error(e.code(), format!("Kafka broker `{broker}` not allowed")))?;
        }
        Ok(())
    }

    /// librdkafka settings common to consumers and producers.
    pub(crate) fn base(&self) -> ClientConfig {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", self.brokers.join(","))
            .set("client.id", &self.client_id)
            .set("security.protocol", "plaintext")
            .set("socket.timeout.ms", millis(self.socket_timeout))
            // Keep reconnect attempts bounded in rate; librdkafka retries
            // forever, the connectors decide when to give up.
            .set("reconnect.backoff.ms", "100")
            .set("reconnect.backoff.max.ms", "2000")
            .set("log.connection.close", "false")
            .set_log_level(RDKafkaLogLevel::Warning);
        c
    }
}

pub(crate) fn millis(d: Duration) -> String {
    d.as_millis().to_string()
}

/// `host:port` or `[v6]:port`; the port is required.
pub(crate) fn parse_broker(broker: &str) -> Result<(&str, u16)> {
    let bad = || {
        error(
            ErrorCode::InvalidArgument,
            format!("Kafka broker `{broker}` must be host:port (no scheme, user or path)"),
        )
    };
    let (host, port) = if let Some(rest) = broker.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or_else(bad)?;
        (host, port)
    } else {
        broker.rsplit_once(':').ok_or_else(bad)?
    };
    if host.is_empty()
        || host.len() > MAX_HOST_BYTES
        || host.contains(['/', '@', ' ', ','])
        || (!broker.starts_with('[') && host.contains(':'))
    {
        return Err(bad());
    }
    let port: u16 = port.parse().map_err(|_| bad())?;
    if port == 0 {
        return Err(bad());
    }
    Ok((host, port))
}

/// Every broker the cluster advertises must be allowed: librdkafka connects
/// to advertised listeners, not only to the bootstrap list.
pub(crate) fn check_advertised(metadata: &Metadata, policy: &TargetPolicy) -> Result<()> {
    for b in metadata.brokers() {
        let port = u16::try_from(b.port()).map_err(|_| {
            error(
                ErrorCode::PolicyDenied,
                "Kafka broker advertised an invalid port",
            )
        })?;
        policy.check_host_port(b.host(), port).map_err(|e| {
            error(
                e.code(),
                format!(
                    "Kafka cluster advertises broker {}:{} which is not on the allowlist",
                    b.host(),
                    port
                ),
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brokers_are_host_port_under_the_allowlist() {
        let policy = TargetPolicy::allow("127.0.0.1", 9092).with_allow("kafka.local", 19092);
        assert_eq!(
            parse_broker("kafka.local:19092").unwrap(),
            ("kafka.local", 19092)
        );
        assert_eq!(parse_broker("[::1]:9092").unwrap(), ("::1", 9092));
        for bad in [
            "kafka.local",
            "PLAINTEXT://kafka.local:9092",
            "u@kafka.local:9092",
            "kafka.local:0",
            "kafka.local:99999",
            ":9092",
            "::1:9092",
            "a,b:9092",
        ] {
            assert_eq!(
                parse_broker(bad).unwrap_err().code,
                ErrorCode::InvalidArgument,
                "{bad}"
            );
        }
        let ok = KafkaClientConfig::new(vec!["127.0.0.1:9092".into(), "kafka.local:19092".into()]);
        assert!(ok.validate(&policy).is_ok());
        let denied = KafkaClientConfig::new(vec!["127.0.0.1:9093".into()]);
        assert_eq!(
            denied.validate(&policy).unwrap_err().code,
            ErrorCode::PolicyDenied
        );
        let none = KafkaClientConfig::new(vec![]);
        assert_eq!(
            none.validate(&policy).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        let mut slow = ok.clone();
        slow.socket_timeout = Duration::from_millis(10);
        assert_eq!(
            slow.validate(&policy).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        let mut id = ok;
        id.client_id = "bad id".into();
        assert_eq!(
            id.validate(&policy).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
    }
}

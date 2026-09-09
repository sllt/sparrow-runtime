use std::net::IpAddr;

use sparrow_model::ErrorCode;

use crate::error::{ConnectorError, Result};

/// Deny-by-default host:port allowlist. Open internet and loopback SSRF
/// targets (metadata hosts, non-listed ports) are rejected.
#[derive(Clone, Debug, Default)]
pub struct TargetPolicy {
    allowed: Vec<AllowedTarget>,
}

#[derive(Clone, Debug)]
pub struct AllowedTarget {
    pub host: String,
    pub port: u16,
}

impl TargetPolicy {
    pub fn deny_all() -> Self {
        Self { allowed: Vec::new() }
    }

    pub fn allow(host: impl Into<String>, port: u16) -> Self {
        Self {
            allowed: vec![AllowedTarget {
                host: host.into(),
                port,
            }],
        }
    }

    pub fn with_allow(mut self, host: impl Into<String>, port: u16) -> Self {
        self.allowed.push(AllowedTarget {
            host: host.into(),
            port,
        });
        self
    }

    pub fn check_host_port(&self, host: &str, port: u16) -> Result<()> {
        if is_blocked_host(host) {
            return Err(ConnectorError::new(
                ErrorCode::PolicyDenied,
                format!("target host `{host}` is blocked (link-local / metadata / unspecified)"),
            ));
        }
        let ok = self
            .allowed
            .iter()
            .any(|t| host_eq(&t.host, host) && t.port == port);
        if ok {
            Ok(())
        } else {
            Err(ConnectorError::new(
                ErrorCode::PolicyDenied,
                format!(
                    "target `{host}:{port}` is not on the allowlist (deny by default; no open SSRF)"
                ),
            ))
        }
    }

    pub fn check_http_url(&self, url: &str) -> Result<()> {
        let parsed = url::Url::parse(url).map_err(|e| {
            ConnectorError::new(ErrorCode::InvalidArgument, format!("invalid HTTP url: {e}"))
        })?;
        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("unsupported URL scheme `{scheme}`"),
            ));
        }
        let host = parsed.host_str().ok_or_else(|| {
            ConnectorError::new(ErrorCode::InvalidArgument, "HTTP url is missing a host")
        })?;
        let port = parsed
            .port()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });
        self.check_host_port(host, port)
    }
}

fn host_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn is_blocked_host(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    if lower == "169.254.169.254" || lower == "metadata.google.internal" {
        return true;
    }
    if let Ok(ip) = lower.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(v4) => v4.is_link_local() || v4.is_unspecified() || v4.is_multicast(),
            IpAddr::V6(v6) => v6.is_unspecified() || v6.is_multicast(),
        }
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_by_default() {
        let p = TargetPolicy::deny_all();
        assert_eq!(
            p.check_host_port("127.0.0.1", 1883).unwrap_err().code(),
            ErrorCode::PolicyDenied
        );
    }

    #[test]
    fn allowlist_exact_port() {
        let p = TargetPolicy::allow("127.0.0.1", 1883);
        p.check_host_port("127.0.0.1", 1883).unwrap();
        assert!(p.check_host_port("127.0.0.1", 1884).is_err());
        assert!(p.check_host_port("8.8.8.8", 1883).is_err());
    }

    #[test]
    fn blocks_metadata() {
        let p = TargetPolicy::allow("169.254.169.254", 80);
        assert_eq!(
            p.check_host_port("169.254.169.254", 80)
                .unwrap_err()
                .code(),
            ErrorCode::PolicyDenied
        );
    }
}

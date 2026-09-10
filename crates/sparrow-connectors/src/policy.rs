use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

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

/// Allowlisted roots for File/replay paths and `checkpoint_dir` (P0-12).
///
/// Override with `SPARROW_DATA_ROOTS` (colon-separated). Defaults include the
/// process cwd, `/tmp`, and the platform temp dir so tests and local demos work.
pub fn data_roots() -> Vec<PathBuf> {
    if let Ok(raw) = std::env::var("SPARROW_DATA_ROOTS") {
        let roots: Vec<PathBuf> = raw
            .split(':')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect();
        if !roots.is_empty() {
            return roots;
        }
    }
    let mut roots = vec![
        std::env::temp_dir(),
        PathBuf::from("/tmp"),
        PathBuf::from("/var/tmp"),
    ];
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }
    roots
}

/// Reject file / checkpoint paths that escape the allowlisted roots.
pub fn check_data_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Err(ConnectorError::new(
            ErrorCode::InvalidArgument,
            "file path is empty",
        ));
    }
    let resolved = resolve_for_policy(path)?;
    let roots = data_roots();
    let mut resolved_roots = Vec::new();
    for root in &roots {
        resolved_roots.push(resolve_for_policy(root).unwrap_or_else(|_| root.clone()));
    }
    let ok = resolved_roots.iter().any(|root| {
        resolved.starts_with(root) || path.starts_with(root)
    });
    if ok {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ErrorCode::PolicyDenied,
            format!(
                "path `{}` is outside SPARROW_DATA_ROOTS allowlist",
                path.display()
            ),
        ))
    }
}

fn resolve_for_policy(path: &Path) -> Result<PathBuf> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| {
                ConnectorError::new(ErrorCode::Internal, format!("cwd: {e}"))
            })?
            .join(path)
    };
    let mut cur = abs.clone();
    let mut missing = Vec::new();
    while !cur.exists() {
        match cur.file_name() {
            Some(name) => {
                missing.push(name.to_os_string());
                cur.pop();
            }
            None => break,
        }
    }
    let mut canon = if cur.exists() {
        cur.canonicalize().map_err(|e| {
            ConnectorError::new(
                ErrorCode::PolicyDenied,
                format!("canonicalize {}: {e}", cur.display()),
            )
        })?
    } else {
        cur
    };
    for name in missing.iter().rev() {
        if name == ".." {
            return Err(ConnectorError::new(
                ErrorCode::PolicyDenied,
                "path must not contain `..` escapes outside an existing prefix",
            ));
        }
        canon.push(name);
    }
    Ok(canon)
}

/// HttpPush bind policy (P0-12): loopback is allowed; unspecified / public
/// binds are denied unless the host:port is on the allowlist.
pub fn check_bind_addr(bind: &str, policy: &TargetPolicy) -> Result<()> {
    let (host, port) = parse_bind(bind)?;
    if host == "0.0.0.0" || host == "::" || host == "[::]" {
        return Err(ConnectorError::new(
            ErrorCode::PolicyDenied,
            format!("HttpPush bind `{bind}` is unspecified; refuse open bind"),
        ));
    }
    if is_loopback_host(&host) {
        return Ok(());
    }
    if is_blocked_host(&host) {
        return Err(ConnectorError::new(
            ErrorCode::PolicyDenied,
            format!("HttpPush bind host `{host}` is blocked"),
        ));
    }
    policy.check_host_port(&host, port)
}

fn parse_bind(bind: &str) -> Result<(String, u16)> {
    if let Ok(sa) = bind.parse::<SocketAddr>() {
        return Ok((sa.ip().to_string(), sa.port()));
    }
    if let Some((host, port)) = bind.rsplit_once(':') {
        let port = port.parse::<u16>().map_err(|_| {
            ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("invalid HttpPush bind `{bind}`"),
            )
        })?;
        let host = host.trim_matches(|c| c == '[' || c == ']').to_string();
        if host.is_empty() {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("invalid HttpPush bind `{bind}`"),
            ));
        }
        return Ok((host, port));
    }
    Err(ConnectorError::new(
        ErrorCode::InvalidArgument,
        format!("invalid HttpPush bind `{bind}`"),
    ))
}

fn is_loopback_host(host: &str) -> bool {
    let h = host.trim_matches(|c| c == '[' || c == ']');
    if h.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Ok(ip) = h.parse::<IpAddr>() {
        return ip.is_loopback();
    }
    false
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

    #[test]
    fn p0_12_tmp_path_is_allowed() {
        let p = std::env::temp_dir().join("sparrow-policy-ok.ndjson");
        check_data_path(&p).expect("temp dir is a default data root");
    }

    #[test]
    fn p0_12_http_push_rejects_unspecified_bind() {
        let p = TargetPolicy::deny_all();
        assert_eq!(
            check_bind_addr("0.0.0.0:8080", &p).unwrap_err().code(),
            ErrorCode::PolicyDenied
        );
        check_bind_addr("127.0.0.1:0", &p).unwrap();
    }
}

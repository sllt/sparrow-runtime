use std::ffi::OsString;
use std::net::{IpAddr, SocketAddr};
use std::path::{Component, Path, PathBuf};

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
        Self {
            allowed: Vec::new(),
        }
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

/// Allowlisted roots for File/replay paths and `checkpoint_dir` (P0-12 / N3).
///
/// Policy (honest, fail-closed):
/// - `SPARROW_DATA_ROOTS` (colon-separated) is the allowlist when set.
///   An empty value means deny every file path.
/// - When unset and `SPARROW_SAFE_MODE=1` (or `--safe-mode`), file paths are
///   denied until roots are configured. systemd units often have cwd `/`.
/// - Otherwise the only default is [`default_data_root`] (`{temp_dir}/sparrow`).
///   CWD is never a default root. `/tmp` as a whole is not a default root
///   (lexical `..` and siblings must not inherit a sandbox).
pub fn data_roots() -> Vec<PathBuf> {
    if let Some(roots) = configured_data_roots() {
        return roots;
    }
    if std::env::var("SPARROW_SAFE_MODE").ok().as_deref() == Some("1") {
        return Vec::new();
    }
    default_data_roots()
}

/// Roots from `SPARROW_DATA_ROOTS`. `None` if the variable is unset.
/// `Some(vec![])` if it is set but empty (deny all).
pub fn configured_data_roots() -> Option<Vec<PathBuf>> {
    let raw = std::env::var("SPARROW_DATA_ROOTS").ok()?;
    Some(
        raw.split(':')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect(),
    )
}

/// Default allowlist when `SPARROW_DATA_ROOTS` is unset (non-safe-mode).
/// Never includes the process cwd.
pub fn default_data_roots() -> Vec<PathBuf> {
    vec![default_data_root()]
}

/// Sparrow-owned data directory under the process temp dir.
pub fn default_data_root() -> PathBuf {
    std::env::temp_dir().join("sparrow")
}

/// Create [`default_data_root`] if needed (tests / local demos).
pub fn ensure_default_data_root() -> PathBuf {
    let p = default_data_root();
    let _ = std::fs::create_dir_all(&p);
    p
}

/// Reject file / checkpoint paths that escape the allowlisted roots.
pub fn check_data_path(path: &Path) -> Result<()> {
    check_data_path_in(path, &data_roots())
}

/// Same as [`check_data_path`] against an explicit root list (tests).
pub fn check_data_path_in(path: &Path, roots: &[PathBuf]) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Err(ConnectorError::new(
            ErrorCode::InvalidArgument,
            "file path is empty",
        ));
    }
    if roots.is_empty() {
        return Err(ConnectorError::new(
            ErrorCode::PolicyDenied,
            format!(
                "path `{}` is denied: SPARROW_DATA_ROOTS is unset/empty (safe/production requires an explicit allowlist)",
                path.display()
            ),
        ));
    }
    let resolved = resolve_for_policy(path)?;
    let ok = roots.iter().any(|root| {
        let root_res = match resolve_for_policy(root) {
            Ok(p) => p,
            Err(_) => return false,
        };
        resolved.starts_with(&root_res)
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
            .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("cwd: {e}")))?
            .join(path)
    };
    let normalized = lexical_normalize(&abs)?;
    let mut cur = normalized.clone();
    let mut missing: Vec<OsString> = Vec::new();
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
        if name == ".." || name == "." {
            return Err(ConnectorError::new(
                ErrorCode::PolicyDenied,
                "path must not contain `..` escapes outside an existing prefix",
            ));
        }
        canon.push(name);
    }
    Ok(canon)
}

/// Resolve `.` / `..` lexically. `..` that would escape the filesystem root
/// is rejected so `/tmp/../etc/passwd` becomes `/etc/passwd` (then fail the
/// allowlist), never a lexical prefix of `/tmp`.
fn lexical_normalize(path: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(c.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return Err(ConnectorError::new(
                        ErrorCode::PolicyDenied,
                        "path `..` escapes the filesystem root",
                    ));
                }
            }
            Component::Normal(s) => out.push(s),
        }
    }
    if out.as_os_str().is_empty() {
        return Err(ConnectorError::new(
            ErrorCode::PolicyDenied,
            "path normalized to empty",
        ));
    }
    Ok(out)
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
            p.check_host_port("169.254.169.254", 80).unwrap_err().code(),
            ErrorCode::PolicyDenied
        );
    }

    #[test]
    fn p0_12_tmp_path_is_allowed() {
        let p = ensure_default_data_root().join("sparrow-policy-ok.ndjson");
        check_data_path_in(&p, &default_data_roots())
            .expect("sparrow data dir is the default root");
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

    #[test]
    fn n3_path_traversal_rejected() {
        let tmp_root = vec![PathBuf::from("/tmp")];
        // These normalize *out of* /tmp (the old lexical starts_with bypass).
        let escape_tmp = [
            PathBuf::from("/tmp/../etc/passwd"),
            PathBuf::from("/tmp/foo/../../etc/passwd"),
            PathBuf::from("/tmp/./../etc/passwd"),
            default_data_root().join("a/../../../etc/passwd"),
        ];
        for p in escape_tmp {
            let err = check_data_path_in(&p, &tmp_root).expect_err(&format!(
                "traversal must be rejected even when /tmp is a root: {}",
                p.display()
            ));
            assert_eq!(
                err.code(),
                ErrorCode::PolicyDenied,
                "{} => {}",
                p.display(),
                err
            );
        }
        let sparrow = default_data_roots();
        // Nested `../` that leaves the sparrow dir (may still be under /tmp).
        let escape_sparrow = [
            default_data_root().join("../etc/passwd"),
            default_data_root().join("nested/../../etc/passwd"),
        ];
        for p in escape_sparrow {
            let err = check_data_path_in(&p, &sparrow).expect_err(&format!(
                "nested ../ must leave the sparrow root: {}",
                p.display()
            ));
            assert_eq!(
                err.code(),
                ErrorCode::PolicyDenied,
                "{} => {}",
                p.display(),
                err
            );
        }
        let under = ensure_default_data_root().join("n3-ok.ndjson");
        check_data_path_in(&under, &sparrow).expect("path inside default sparrow root");
        let sibling = std::env::temp_dir().join("n3-not-under-sparrow.ndjson");
        assert_eq!(
            check_data_path_in(&sibling, &sparrow).unwrap_err().code(),
            ErrorCode::PolicyDenied
        );

        #[cfg(unix)]
        {
            let root = ensure_default_data_root();
            let stamp = format!(
                "{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let outside = std::env::temp_dir().join(format!("n3-outside-{stamp}"));
            std::fs::write(&outside, b"secret").unwrap();
            let link = root.join(format!("n3-symlink-{stamp}"));
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            let err = check_data_path_in(&link, &sparrow).expect_err("symlink-out must be denied");
            assert_eq!(err.code(), ErrorCode::PolicyDenied, "{err}");
            let _ = std::fs::remove_file(&link);
            let _ = std::fs::remove_file(&outside);
        }
    }

    #[test]
    fn n3_cwd_not_default_root() {
        let defaults = default_data_roots();
        if let Ok(cwd) = std::env::current_dir() {
            assert!(
                !defaults.iter().any(|r| r == &cwd),
                "cwd must not be a default data root (systemd may be /): cwd={cwd:?} defaults={defaults:?}"
            );
            let only_cwd = cwd.join("n3-cwd-only-should-deny.ndjson");
            if !only_cwd.starts_with(default_data_root()) {
                assert_eq!(
                    check_data_path_in(&only_cwd, &defaults).unwrap_err().code(),
                    ErrorCode::PolicyDenied,
                    "a path that is only under cwd must be denied"
                );
            }
        }
        assert!(
            !defaults.iter().any(|r| r == Path::new("/")
                || r == Path::new("/tmp")
                || r == Path::new("/var/tmp")
                || *r == std::env::temp_dir()),
            "defaults must be the sparrow-owned dir, not cwd/tmp as a whole: {defaults:?}"
        );
        assert_eq!(defaults, vec![default_data_root()]);
    }
}

//! K5.1 static-token RBAC. Tokens are never stored in memory as plaintext
//! when loaded from an auth file (only SHA-256 digests), never returned and
//! never written to audit. Authentication failures do not touch SQLite.
use axum::http::Method;
use serde::Deserialize;
use sparrow_model::{ErrorCode, SparrowError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
    fn parse(s: &str) -> Option<Role> {
        match s {
            "viewer" => Some(Role::Viewer),
            "operator" => Some(Role::Operator),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Principal {
    pub actor: String,
    pub role: Role,
}

struct Entry {
    actor: String,
    role: Role,
    digest: [u8; 32],
}

/// Authentication configuration. `From<&str>`/`From<String>` build the legacy
/// single-token mode, which maps the token to `admin` with actor `token`
/// (the pre-K5 audit identity, kept for compatibility).
pub struct Auth {
    entries: Vec<Entry>,
    mode: &'static str,
}

pub const LEGACY_ACTOR: &str = "token";
const MAX_AUTH_FILE: u64 = 64 * 1024;
const MAX_PRINCIPALS: usize = 64;

fn digest(token: &[u8]) -> [u8; 32] {
    let hex = sparrow_plugin::sha256(token);
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0);
    }
    out
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl From<String> for Auth {
    fn from(token: String) -> Self {
        Auth::legacy(&token)
    }
}
impl From<&str> for Auth {
    fn from(token: &str) -> Self {
        Auth::legacy(token)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDoc {
    version: u32,
    principals: Vec<FilePrincipal>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePrincipal {
    actor: String,
    role: String,
    token_sha256: String,
}

fn bad(msg: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, format!("auth file: {}", msg.into()))
}

impl Auth {
    pub fn legacy(token: &str) -> Self {
        // An empty legacy token authenticates nobody (fail closed).
        let entries = if token.is_empty() {
            Vec::new()
        } else {
            vec![Entry { actor: LEGACY_ACTOR.into(), role: Role::Admin, digest: digest(token.as_bytes()) }]
        };
        Auth { entries, mode: "legacy_single_token" }
    }

    pub fn mode(&self) -> &'static str {
        self.mode
    }

    /// Parse an auth document. Any anomaly is an error; there is no partial load.
    pub fn from_json(bytes: &[u8]) -> Result<Self, SparrowError> {
        if bytes.len() as u64 > MAX_AUTH_FILE {
            return Err(bad("exceeds 64 KiB"));
        }
        let doc: FileDoc = serde_json::from_slice(bytes).map_err(|e| bad(format!("invalid JSON ({})", e.classify() as u8)))?;
        if doc.version != 1 {
            return Err(bad("unsupported version (expected 1)"));
        }
        if doc.principals.is_empty() || doc.principals.len() > MAX_PRINCIPALS {
            return Err(bad("principals must contain 1..=64 entries"));
        }
        let mut entries: Vec<Entry> = Vec::new();
        for p in doc.principals {
            if p.actor.is_empty()
                || p.actor.len() > 64
                || !p.actor.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.@".contains(&b))
            {
                return Err(bad("actor must be 1..=64 of [A-Za-z0-9-_.@]"));
            }
            let role = Role::parse(&p.role).ok_or_else(|| bad(format!("unknown role for actor {}", p.actor)))?;
            let d = parse_hex32(&p.token_sha256)
                .ok_or_else(|| bad(format!("token_sha256 for actor {} must be 64 lowercase hex", p.actor)))?;
            if entries.iter().any(|e| e.actor == p.actor) {
                return Err(bad(format!("duplicate actor {}", p.actor)));
            }
            if entries.iter().any(|e| ct_eq(&e.digest, &d)) {
                return Err(bad("duplicate credential digest"));
            }
            entries.push(Entry { actor: p.actor, role, digest: d });
        }
        Ok(Auth { entries, mode: "auth_file" })
    }

    /// Load from disk; on unix the file must not be group/other accessible.
    pub fn load_file(path: &std::path::Path) -> Result<Self, SparrowError> {
        let meta = std::fs::metadata(path).map_err(|e| bad(format!("cannot stat: {}", e.kind())))?;
        if !meta.is_file() {
            return Err(bad("not a regular file"));
        }
        if meta.len() > MAX_AUTH_FILE {
            return Err(bad("exceeds 64 KiB"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(bad("must be readable only by the server user (chmod 600)"));
            }
        }
        let bytes = std::fs::read(path).map_err(|e| bad(format!("cannot read: {}", e.kind())))?;
        Self::from_json(&bytes)
    }

    pub fn authenticate(&self, token: &str) -> Option<Principal> {
        if token.is_empty() {
            return None;
        }
        let d = digest(token.as_bytes());
        let mut found = None;
        for e in &self.entries {
            if ct_eq(&e.digest, &d) {
                found = Some(Principal { actor: e.actor.clone(), role: e.role });
            }
        }
        found
    }
}

/// Action catalogue: (method, path pattern, action, minimum role). `{}`
/// matches exactly one non-empty segment. Unlisted routes require admin.
pub const ACTIONS: &[(&str, &str, &str, Role)] = &[
    ("GET", "/v1/auth/me", "auth.me", Role::Viewer),
    ("GET", "/v1/capabilities", "capabilities.read", Role::Viewer),
    ("GET", "/v1/metrics", "metrics.read", Role::Viewer),
    ("GET", "/v1/pipelines", "pipelines.list", Role::Viewer),
    ("GET", "/v1/pipelines/{}/status", "pipeline.status", Role::Viewer),
    ("GET", "/v1/pipelines/{}/diagnose", "pipeline.diagnose", Role::Viewer),
    ("GET", "/v1/audit", "audit.summary", Role::Viewer),
    ("POST", "/v1/validate", "plan.validate", Role::Operator),
    ("POST", "/v1/explain", "plan.explain", Role::Operator),
    ("POST", "/v1/graphs/validate", "graph.validate", Role::Operator),
    ("POST", "/v1/graphs/explain", "graph.explain", Role::Operator),
    ("POST", "/v1/test", "plan.test", Role::Operator),
    ("POST", "/v1/query", "query.preview", Role::Operator),
    ("POST", "/v1/preview", "preview.run", Role::Operator),
    ("GET", "/v1/streams", "streams.list", Role::Operator),
    ("GET", "/v1/streams/{}", "stream.read", Role::Operator),
    ("PUT", "/v1/streams/{}", "stream.write", Role::Operator),
    ("GET", "/v1/tables", "tables.list", Role::Operator),
    ("GET", "/v1/tables/{}", "table.read", Role::Operator),
    ("PUT", "/v1/tables/{}", "table.write", Role::Operator),
    ("GET", "/v1/tables/{}/revisions", "table.revisions", Role::Operator),
    ("GET", "/v1/tables/{}/revisions/{}", "table.revision", Role::Operator),
    ("GET", "/v1/tables/{}/dependencies", "table.dependencies", Role::Operator),
    ("POST", "/v1/tables/{}/mutate", "table.mutate", Role::Operator),
    ("POST", "/v1/tables/{}/gc", "table.gc", Role::Admin),
    ("POST", "/v1/tables/{}/rollback", "table.rollback", Role::Admin),
    ("GET", "/v1/pipelines/{}", "pipeline.read", Role::Operator),
    ("PUT", "/v1/pipelines/{}", "pipeline.publish", Role::Operator),
    ("POST", "/v1/pipelines/{}/start", "pipeline.start", Role::Operator),
    ("POST", "/v1/pipelines/{}/stop", "pipeline.stop", Role::Operator),
    ("POST", "/v1/pipelines/{}/checkpoint", "pipeline.checkpoint", Role::Operator),
    ("GET", "/v1/pipelines/{}/checkpoints", "pipeline.checkpoints", Role::Operator),
    ("GET", "/v1/pipelines/{}/outbox", "outbox.status", Role::Operator),
    ("GET", "/v1/pipelines/{}/input-dlq", "input_dlq.status", Role::Operator),
    ("GET", "/v1/pipelines/{}/recovery/operations", "recovery.operations", Role::Operator),
    ("GET", "/v1/pipelines/{}/recovery/operations/{}", "recovery.operation", Role::Operator),
    ("GET", "/v1/plugins", "plugins.list", Role::Operator),
    ("GET", "/v1/plugins/{}/references", "plugin.references", Role::Operator),
    ("GET", "/v1/demo/io", "demo.io", Role::Operator),
    ("POST", "/v1/demo/publish-fixture", "demo.publish", Role::Operator),
    ("GET", "/v1/demo/capture", "demo.capture", Role::Operator),
    ("GET", "/v1/drafts", "drafts.list", Role::Operator),
    ("GET", "/v1/drafts/{}", "draft.read", Role::Operator),
    ("PUT", "/v1/drafts/{}", "draft.write", Role::Operator),
    ("DELETE", "/v1/drafts/{}", "draft.delete", Role::Operator),
    ("POST", "/v1/drafts/{}/check", "draft.check", Role::Operator),
    ("POST", "/v1/drafts/{}/publish", "draft.publish", Role::Operator),
    ("GET", "/v1/publications/{}", "publication.read", Role::Operator),
    ("GET", "/v1/pipelines/{}/revisions", "pipeline.revisions", Role::Operator),
    ("GET", "/v1/pipelines/{}/revisions/{}", "pipeline.revision", Role::Operator),
    ("GET", "/v1/secrets", "secrets.names", Role::Operator),
    ("GET", "/v1/connections", "connections.list", Role::Operator),
    ("GET", "/v1/connections/{}", "connection.read", Role::Operator),
    ("PUT", "/v1/connections/{}", "connection.write", Role::Operator),
    ("DELETE", "/v1/connections/{}", "connection.delete", Role::Operator),
    ("POST", "/v1/connections/{}/test", "connection.test", Role::Operator),
    ("GET", "/v1/pipelines/{}/outbox/entries", "outbox.entries", Role::Admin),
    ("GET", "/v1/pipelines/{}/outbox/entries/{}", "outbox.entry", Role::Admin),
    ("POST", "/v1/pipelines/{}/outbox/command", "outbox.command", Role::Admin),
    ("GET", "/v1/pipelines/{}/input-dlq/entries", "input_dlq.entries", Role::Admin),
    ("GET", "/v1/pipelines/{}/input-dlq/entries/{}", "input_dlq.entry", Role::Admin),
    ("POST", "/v1/pipelines/{}/input-dlq/purge", "input_dlq.purge", Role::Admin),
    ("POST", "/v1/pipelines/{}/recovery/preview", "recovery.preview", Role::Admin),
    ("POST", "/v1/pipelines/{}/recovery/execute", "recovery.execute", Role::Admin),
    ("POST", "/v1/pipelines/{}/recovery/operations/{}/finish", "recovery.finish", Role::Admin),
    ("POST", "/v1/pipelines/{}/recovery/operations/{}/abort", "recovery.abort", Role::Admin),
    ("POST", "/v1/pipelines/{}/restore", "pipeline.restore", Role::Admin),
    ("POST", "/v1/pipelines/{}/kill", "pipeline.kill", Role::Admin),
    ("POST", "/v1/pipelines/{}/retire", "pipeline.retire", Role::Admin),
    ("PUT", "/v1/allowlist", "allowlist.write", Role::Admin),
    ("PUT", "/v1/secrets/{}", "secret.write", Role::Admin),
    ("POST", "/v1/plugins/install", "plugin.install", Role::Admin),
    ("POST", "/v1/plugins/{}/enable", "plugin.enable", Role::Admin),
    ("POST", "/v1/plugins/{}/disable", "plugin.disable", Role::Admin),
    ("POST", "/v1/plugins/{}/uninstall", "plugin.uninstall", Role::Admin),
    ("POST", "/v1/plugins/{}/attest", "plugin.attest", Role::Admin),
];

fn matches(pattern: &str, path: &str) -> bool {
    let mut p = pattern.split('/');
    let mut s = path.split('/');
    loop {
        match (p.next(), s.next()) {
            (None, None) => return true,
            (Some("{}"), Some(seg)) if !seg.is_empty() => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }
    }
}

/// Minimum role for a request. HEAD is treated as GET. Unknown → admin.
pub fn required(method: &Method, path: &str) -> (&'static str, Role) {
    let m = if method == Method::HEAD { "GET" } else { method.as_str() };
    ACTIONS
        .iter()
        .find(|(am, pat, _, _)| *am == m && matches(pat, path))
        .map(|(_, _, a, r)| (*a, *r))
        .unwrap_or(("unlisted", Role::Admin))
}

pub fn allowed_actions(role: Role) -> Vec<&'static str> {
    ACTIONS.iter().filter(|(_, _, _, r)| *r <= role).map(|(_, _, a, _)| *a).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn doc(p: &str) -> String {
        format!(r#"{{"version":1,"principals":[{p}]}}"#)
    }
    fn h(t: &str) -> String {
        sparrow_plugin::sha256(t.as_bytes())
    }

    #[test]
    fn k5_auth_file_fail_closed_cases() {
        let a = format!(r#"{{"actor":"a","role":"viewer","token_sha256":"{}"}}"#, h("t1"));
        let b = format!(r#"{{"actor":"a","role":"admin","token_sha256":"{}"}}"#, h("t2"));
        let c = format!(r#"{{"actor":"c","role":"admin","token_sha256":"{}"}}"#, h("t1"));
        assert!(Auth::from_json(doc(&a).as_bytes()).is_ok());
        for bad in [
            doc(""),
            doc(&format!("{a},{b}")),
            doc(&format!("{a},{c}")),
            doc(&a.replace("viewer", "root")),
            doc(&a.replace(&h("t1"), "ABC")),
            doc(&a.replace("\"a\"", "\"a b\"")),
            format!(r#"{{"version":2,"principals":[{a}]}}"#),
            format!(r#"{{"version":1,"principals":[{a}],"extra":1}}"#),
            "not json".into(),
        ] {
            assert!(Auth::from_json(bad.as_bytes()).is_err(), "{bad}");
        }
    }

    #[test]
    fn k5_authenticate_and_legacy() {
        let a = Auth::from_json(
            doc(&format!(r#"{{"actor":"ops","role":"operator","token_sha256":"{}"}}"#, h("secret-op"))).as_bytes(),
        )
        .unwrap();
        assert_eq!(a.authenticate("secret-op").unwrap().role, Role::Operator);
        assert!(a.authenticate("secret-o").is_none());
        assert!(a.authenticate("").is_none());
        let l = Auth::from("legacy-token-value");
        let p = l.authenticate("legacy-token-value").unwrap();
        assert_eq!((p.actor.as_str(), p.role), (LEGACY_ACTOR, Role::Admin));
        assert!(Auth::from("").authenticate("").is_none());
    }

    #[test]
    fn k5_route_matrix() {
        assert_eq!(required(&Method::GET, "/v1/pipelines/x/status").1, Role::Viewer);
        assert_eq!(required(&Method::GET, "/v1/pipelines/x").1, Role::Operator);
        assert_eq!(required(&Method::POST, "/v1/pipelines/x/kill").1, Role::Admin);
        assert_eq!(required(&Method::GET, "/v1/pipelines//status").1, Role::Admin);
        assert_eq!(required(&Method::GET, "/v1/nope").1, Role::Admin);
        assert_eq!(required(&Method::DELETE, "/v1/pipelines/x").1, Role::Admin);
    }
}

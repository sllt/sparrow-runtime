//! Client setup shared by NATS Core and JetStream: endpoint policy, token
//! SecretRef resolution and server address parsing. Errors never echo URLs,
//! subjects supplied by data, or credential values.

use sparrow_model::{ErrorCode, Result, SparrowError};

use crate::{SecretResolver, TargetPolicy};

pub const MAX_SERVERS: usize = 4;
pub const MAX_SERVER_URL_BYTES: usize = 1024;
pub const MAX_TOKEN_BYTES: usize = 4096;
pub const MAX_SUBJECT_BYTES: usize = 4096;
pub const MAX_QUEUE_GROUP_BYTES: usize = 256;
pub const DEFAULT_PORT: u16 = 4222;

pub(crate) fn error(code: ErrorCode, message: &str) -> SparrowError {
    SparrowError::new(code, message)
}

/// 1..=4 `nats://` / `tls://` endpoints without credentials, path, query or
/// fragment; a token requires `tls://`; every host:port must be allowlisted.
pub fn validate_servers(servers: &[String], uses_token: bool, policy: &TargetPolicy) -> Result<()> {
    if !(1..=MAX_SERVERS).contains(&servers.len()) {
        return Err(error(
            ErrorCode::BoundExceeded,
            "NATS requires 1..=4 server URLs",
        ));
    }
    for server in servers {
        let u = url::Url::parse(server)
            .map_err(|_| error(ErrorCode::InvalidArgument, "invalid NATS URL"))?;
        if server.len() > MAX_SERVER_URL_BYTES
            || !matches!(u.scheme(), "nats" | "tls")
            || !u.username().is_empty()
            || u.password().is_some()
            || !matches!(u.path(), "" | "/")
            || u.query().is_some()
            || u.fragment().is_some()
        {
            return Err(error(
                ErrorCode::PolicyDenied,
                "NATS URLs require nats/tls and no credentials, path, query or fragment",
            ));
        }
        if uses_token && u.scheme() != "tls" {
            return Err(error(
                ErrorCode::PolicyDenied,
                "NATS authentication requires TLS",
            ));
        }
        policy
            .check_host_port(
                u.host_str()
                    .ok_or_else(|| error(ErrorCode::InvalidArgument, "NATS host required"))?,
                u.port().unwrap_or(DEFAULT_PORT),
            )
            .map_err(|e| error(e.code(), "NATS endpoint not allowed"))?;
    }
    Ok(())
}

/// Resolve a token SecretRef. The value is returned for the SDK only.
pub fn resolve_token(secrets: &dyn SecretResolver, reference: &str) -> Result<String> {
    let token = secrets
        .resolve(reference)
        .map_err(|e| error(e.code(), "NATS token SecretRef resolution failed"))?;
    if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
        return Err(error(ErrorCode::InvalidArgument, "NATS token size invalid"));
    }
    Ok(token)
}

pub fn parse_servers(servers: &[String]) -> Result<Vec<async_nats::ServerAddr>> {
    servers
        .iter()
        .map(|s| s.parse::<async_nats::ServerAddr>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| error(ErrorCode::InvalidArgument, "invalid NATS server address"))
}

/// Publish subjects are literal (no wildcards); subscribe subjects may use
/// `*` tokens and a trailing `>`. Tokens are non-empty printable ASCII
/// without whitespace.
pub fn check_subject(subject: &str, allow_wildcards: bool) -> Result<()> {
    let invalid = || {
        error(
            ErrorCode::InvalidArgument,
            if allow_wildcards {
                "NATS subscribe subject must be 1..=4096 bytes of non-empty dot-separated tokens; `*` or a trailing `>` only as whole tokens"
            } else {
                "NATS publish subject must be 1..=4096 bytes of non-empty dot-separated literal tokens (no wildcards)"
            },
        )
    };
    if subject.is_empty() || subject.len() > MAX_SUBJECT_BYTES {
        return Err(invalid());
    }
    let tokens: Vec<&str> = subject.split('.').collect();
    for (i, token) in tokens.iter().enumerate() {
        if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(invalid());
        }
        let wildcard = *token == "*" || (*token == ">" && i + 1 == tokens.len());
        if token.contains(['*', '>']) && !(allow_wildcards && wildcard) {
            return Err(invalid());
        }
    }
    Ok(())
}

pub fn check_queue_group(group: &str) -> Result<()> {
    if group.is_empty()
        || group.len() > MAX_QUEUE_GROUP_BYTES
        || !group
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(error(
            ErrorCode::InvalidArgument,
            "NATS queue_group must be 1..=256 ASCII alphanumeric, `_`, `-` or `.`",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MapSecretResolver;

    #[test]
    fn nats_common_server_policy_and_subject_grammar() {
        let p = TargetPolicy::allow("127.0.0.1", 4222);
        validate_servers(&["nats://127.0.0.1:4222".into()], false, &p).unwrap();
        validate_servers(&["nats://127.0.0.1".into()], false, &p).unwrap();
        for (servers, token, code) in [
            (vec![], false, ErrorCode::BoundExceeded),
            (
                vec!["nats://127.0.0.1:4222".to_string(); 5],
                false,
                ErrorCode::BoundExceeded,
            ),
            (
                vec!["nats://u:p@127.0.0.1:4222".into()],
                false,
                ErrorCode::PolicyDenied,
            ),
            (
                vec!["nats://127.0.0.1:4222/x".into()],
                false,
                ErrorCode::PolicyDenied,
            ),
            (
                vec!["nats://127.0.0.1:4222?x=1".into()],
                false,
                ErrorCode::PolicyDenied,
            ),
            (
                vec!["ws://127.0.0.1:4222".into()],
                false,
                ErrorCode::PolicyDenied,
            ),
            (
                vec!["nats://127.0.0.1:4222".into()],
                true,
                ErrorCode::PolicyDenied,
            ),
            (
                vec!["nats://127.0.0.1:4223".into()],
                false,
                ErrorCode::PolicyDenied,
            ),
            (
                vec!["nats://169.254.169.254:4222".into()],
                false,
                ErrorCode::PolicyDenied,
            ),
        ] {
            assert_eq!(
                validate_servers(&servers, token, &p).unwrap_err().code,
                code,
                "{servers:?}"
            );
        }
        let e =
            validate_servers(&["nats://u:hunter2@127.0.0.1:4222".into()], false, &p).unwrap_err();
        assert!(!e.message.contains("hunter2"));

        for ok in ["a", "a.b.c", "sensors.*.temp", "sensors.>", "*", ">"] {
            check_subject(ok, true).unwrap();
        }
        for bad in ["", "a..b", ".a", "a.", "a b", "a.>.b", "a*", "a.b>", "x\n"] {
            assert!(check_subject(bad, true).is_err(), "{bad:?}");
        }
        check_subject("out.rows", false).unwrap();
        for bad in ["out.*", "out.>", "*"] {
            assert!(check_subject(bad, false).is_err(), "{bad:?}");
        }
        assert!(check_subject(&"a".repeat(4097), true).is_err());
        check_queue_group("workers-1.a_b").unwrap();
        for bad in ["", "a b", "a*", &"g".repeat(257)] {
            assert!(check_queue_group(bad).is_err(), "{bad:?}");
        }
        let secrets = MapSecretResolver::new(
            [
                ("t".to_string(), "s3cret".to_string()),
                ("e".to_string(), String::new()),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(resolve_token(&secrets, "t").unwrap(), "s3cret");
        assert_eq!(
            resolve_token(&secrets, "e").unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            resolve_token(&secrets, "missing").unwrap_err().code,
            ErrorCode::SecretMissing
        );
    }
}

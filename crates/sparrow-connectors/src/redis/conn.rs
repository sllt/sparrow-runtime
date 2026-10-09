//! Redis endpoint, credentials and one RESP2 connection.
//!
//! `redis://host[:port][/db]` or `rediss://` (TLS, certificate verification
//! always on; `ca_pem` replaces the built-in roots). The host:port must pass
//! the [`TargetPolicy`]. Credentials are secret references resolved once at
//! bind and sent with `AUTH [username] password` (Redis 6+ ACL form when a
//! username is given); they require `rediss://`. The connection speaks RESP2
//! only (no `HELLO`), so Redis 6 and 7 behave the same.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use sparrow_model::{ErrorCode, Result, SparrowError};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use super::resp::{self, Frame, ReadError, Reader};
use crate::{SecretResolver, TargetPolicy};

pub const MAX_CA_PEM_BYTES: usize = 64 * 1024;
const MAX_SECRET_BYTES: usize = 4096;
const MAX_URL_BYTES: usize = 2048;
pub const MAX_DB: u32 = 4095;

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message.into())
}

fn install_rustls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Where and how to connect. Holds secret *names*, never secret values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisTarget {
    pub url: String,
    pub username_secret: Option<String>,
    pub password_secret: Option<String>,
    pub ca_pem: Option<String>,
    pub connect_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub db: u32,
}

impl RedisTarget {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username_secret: None,
            password_secret: None,
            ca_pem: None,
            connect_timeout: Duration::from_secs(2),
        }
    }

    pub fn endpoint(&self) -> Result<Endpoint> {
        if self.url.len() > MAX_URL_BYTES || self.url.chars().any(char::is_control) {
            return Err(err(
                ErrorCode::InvalidArgument,
                "Redis url exceeds 2048 bytes or contains control characters",
            ));
        }
        let url = url::Url::parse(&self.url)
            .map_err(|_| err(ErrorCode::InvalidArgument, "Redis url is not a valid URL"))?;
        let tls = match url.scheme() {
            "redis" => false,
            "rediss" => true,
            _ => {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "Redis url scheme must be redis:// or rediss://",
                ))
            }
        };
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "Redis url must not carry userinfo, query or fragment (credentials are secret references)",
            ));
        }
        let host = match url.host() {
            Some(url::Host::Domain(d)) if !d.is_empty() => d.to_string(),
            Some(url::Host::Ipv4(a)) => a.to_string(),
            Some(url::Host::Ipv6(a)) => a.to_string(),
            _ => return Err(err(ErrorCode::InvalidArgument, "Redis url needs a host")),
        };
        let port = url.port().unwrap_or(6379);
        let db = match url.path() {
            "" | "/" => 0,
            path => {
                let digits = &path[1..];
                if digits.is_empty()
                    || digits.len() > 4
                    || !digits.bytes().all(|b| b.is_ascii_digit())
                {
                    return Err(err(
                        ErrorCode::InvalidArgument,
                        "Redis url path must be /<db> with db 0..=4095",
                    ));
                }
                let db: u32 = digits.parse().unwrap_or(u32::MAX);
                if db > MAX_DB {
                    return Err(err(
                        ErrorCode::InvalidArgument,
                        "Redis url path must be /<db> with db 0..=4095",
                    ));
                }
                db
            }
        };
        Ok(Endpoint {
            host,
            port,
            tls,
            db,
        })
    }

    pub fn validate(&self) -> Result<()> {
        let endpoint = self.endpoint()?;
        for name in [&self.username_secret, &self.password_secret]
            .into_iter()
            .flatten()
        {
            if name.is_empty() || name.len() > 128 {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "Redis secret reference must be 1..=128 bytes",
                ));
            }
        }
        if self.username_secret.is_some() && self.password_secret.is_none() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "Redis username_secret requires password_secret (AUTH username password)",
            ));
        }
        if self.password_secret.is_some() && !endpoint.tls {
            return Err(err(
                ErrorCode::PolicyDenied,
                "Redis credentials require rediss:// (TLS); refusing to send AUTH in clear text",
            ));
        }
        if let Some(pem) = &self.ca_pem {
            if !endpoint.tls {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "Redis ca_pem requires rediss://",
                ));
            }
            root_store(Some(pem))?;
        }
        let ms = self.connect_timeout.as_millis();
        if !(100..=60_000).contains(&ms) {
            return Err(err(
                ErrorCode::BoundExceeded,
                "Redis connect_timeout_ms must be 100..=60000",
            ));
        }
        Ok(())
    }

    /// Check the target policy, resolve credentials and build TLS settings.
    pub fn bind(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<BoundTarget> {
        self.validate()?;
        let endpoint = self.endpoint()?;
        policy
            .check_host_port(&endpoint.host, endpoint.port)
            .map_err(SparrowError::from)?;
        let resolve = |name: &String| -> Result<String> {
            let value = secrets.resolve(name).map_err(SparrowError::from)?;
            if value.is_empty() || value.len() > MAX_SECRET_BYTES {
                return Err(err(
                    ErrorCode::SecretMissing,
                    "Redis credential secret is empty or exceeds 4096 bytes",
                ));
            }
            Ok(value)
        };
        let auth = match &self.password_secret {
            Some(password) => Some(Auth {
                username: self.username_secret.as_ref().map(resolve).transpose()?,
                password: resolve(password)?,
            }),
            None => None,
        };
        let tls = if endpoint.tls {
            install_rustls_provider();
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store(self.ca_pem.as_deref())?)
                .with_no_client_auth();
            let name = ServerName::try_from(endpoint.host.clone())
                .map_err(|_| err(ErrorCode::InvalidArgument, "Redis TLS server name invalid"))?;
            Some((tokio_rustls::TlsConnector::from(Arc::new(config)), name))
        } else {
            None
        };
        Ok(BoundTarget {
            endpoint,
            auth,
            tls,
            connect_timeout: self.connect_timeout,
        })
    }
}

fn root_store(pem: Option<&str>) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    match pem {
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        Some(pem) => {
            let invalid = || {
                err(
                    ErrorCode::InvalidArgument,
                    "Redis ca_pem must hold 1..=16 PEM CA certificates within 64 KiB",
                )
            };
            if pem.len() > MAX_CA_PEM_BYTES {
                return Err(invalid());
            }
            let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| invalid())?;
            if !(1..=16).contains(&certs.len()) {
                return Err(invalid());
            }
            for cert in certs {
                roots.add(cert).map_err(|_| invalid())?;
            }
        }
    }
    Ok(roots)
}

struct Auth {
    username: Option<String>,
    password: String,
}

pub struct BoundTarget {
    pub endpoint: Endpoint,
    auth: Option<Auth>,
    tls: Option<(tokio_rustls::TlsConnector, ServerName<'static>)>,
    connect_timeout: Duration,
}

impl std::fmt::Debug for BoundTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No host (may be internal) and never credentials.
        f.debug_struct("BoundTarget")
            .field("tls", &self.tls.is_some())
            .field("auth", &self.auth.is_some())
            .finish_non_exhaustive()
    }
}

pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

pub struct Connection {
    pub io: Pin<Box<dyn Stream>>,
}

impl Connection {
    /// `false` if the peer already closed the connection (or sent bytes
    /// nobody asked for). Polls the socket once; a connection closed after
    /// this probe is still found only by the next read or write.
    pub async fn probe_alive(&mut self, reader: &mut Reader) -> bool {
        if reader.has_buffered() {
            return false;
        }
        // Pending means nothing arrived; the read is cancel-safe.
        let pending = tokio::time::timeout(Duration::ZERO, reader.next(&mut self.io))
            .await
            .is_err();
        // An incomplete unsolicited frame is not an idle connection either.
        // Otherwise its prefix can be mistaken for the next command's reply.
        pending && !reader.has_buffered()
    }
}

/// Why a connection could not be established. Nothing of a user command
/// has been sent in either case.
#[derive(Debug)]
pub enum ConnectError {
    /// TCP/TLS/timeout/peer closed: retryable.
    Io(&'static str),
    /// The server refused AUTH or SELECT: configuration, not retryable.
    Rejected(ErrorCode, &'static str),
}

impl ConnectError {
    pub fn reason(&self) -> &'static str {
        match self {
            ConnectError::Io(r) | ConnectError::Rejected(_, r) => r,
        }
    }
}

impl BoundTarget {
    pub fn tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Connect, TLS-handshake, `AUTH` and `SELECT`, all within
    /// `connect_timeout`.
    pub async fn connect(&self) -> std::result::Result<Connection, ConnectError> {
        match tokio::time::timeout(self.connect_timeout, self.connect_inner()).await {
            Ok(result) => result,
            Err(_) => Err(ConnectError::Io("redis_connect_timeout")),
        }
    }

    async fn connect_inner(&self) -> std::result::Result<Connection, ConnectError> {
        let tcp = TcpStream::connect((self.endpoint.host.as_str(), self.endpoint.port))
            .await
            .map_err(|_| ConnectError::Io("redis_connect_failed"))?;
        let _ = tcp.set_nodelay(true);
        let mut io: Pin<Box<dyn Stream>> = match &self.tls {
            None => Box::pin(tcp),
            Some((connector, name)) => Box::pin(
                connector
                    .connect(name.clone(), tcp)
                    .await
                    .map_err(|_| ConnectError::Io("redis_tls_handshake_failed"))?,
            ),
        };
        let mut steps: Vec<(&[u8], ErrorCode, &'static str)> = Vec::new();
        let mut request = Vec::new();
        if let Some(auth) = &self.auth {
            let mut args: Vec<&[u8]> = vec![b"AUTH"];
            if let Some(user) = &auth.username {
                args.push(user.as_bytes());
            }
            args.push(auth.password.as_bytes());
            request.reserve_exact(resp::command_len(&args));
            resp::push_command(&mut request, &args);
            steps.push((b"AUTH", ErrorCode::PolicyDenied, "redis_auth_rejected"));
        }
        let db = self.endpoint.db.to_string();
        if self.endpoint.db != 0 {
            let args: [&[u8]; 2] = [b"SELECT", db.as_bytes()];
            request.reserve_exact(resp::command_len(&args));
            resp::push_command(&mut request, &args);
            steps.push((
                b"SELECT",
                ErrorCode::InvalidArgument,
                "redis_select_rejected",
            ));
        }
        if steps.is_empty() {
            return Ok(Connection { io });
        }
        io.write_all(&request)
            .await
            .map_err(|_| ConnectError::Io("redis_handshake_write_failed"))?;
        // Clear the credential copy now that it is on the wire.
        request.fill(0);
        drop(request);
        let mut reader = Reader::new(0, 0);
        for (_, code, reason) in steps {
            match reader.next(&mut io).await {
                Ok(Frame::Simple(r)) if reader.bytes(r.clone()) == b"OK" => {}
                Ok(Frame::Error(_)) => return Err(ConnectError::Rejected(code, reason)),
                Ok(_) | Err(ReadError::Protocol(_)) => {
                    return Err(ConnectError::Rejected(
                        ErrorCode::CodecViolation,
                        "redis_handshake_protocol_error",
                    ))
                }
                Err(_) => return Err(ConnectError::Io("redis_handshake_read_failed")),
            }
        }
        if reader.has_buffered() {
            return Err(ConnectError::Rejected(
                ErrorCode::CodecViolation,
                "redis_handshake_protocol_error",
            ));
        }
        Ok(Connection { io })
    }
}

/// How a Redis error reply is treated, from its first word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    /// NOAUTH / WRONGPASS / NOPERM: credentials or ACL.
    Auth,
    /// MOVED / ASK / CROSSSLOT / CLUSTERDOWN: Redis Cluster is not supported.
    Cluster,
    /// READONLY: connected to a replica.
    ReadOnly,
    /// WRONGTYPE: the key holds another data type.
    WrongType,
    /// Anything else (ERR, OOM, LOADING, BUSY, MISCONF, ...).
    Other,
}

pub fn classify_error(message: &[u8]) -> ErrorClass {
    let word = message.split(|b| *b == b' ').next().unwrap_or_default();
    match word {
        b"NOAUTH" | b"WRONGPASS" | b"NOPERM" => ErrorClass::Auth,
        b"MOVED" | b"ASK" | b"CROSSSLOT" | b"CLUSTERDOWN" => ErrorClass::Cluster,
        b"READONLY" => ErrorClass::ReadOnly,
        b"WRONGTYPE" => ErrorClass::WrongType,
        _ => ErrorClass::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_are_strict() {
        let ep = RedisTarget::new("rediss://cache.local:6380/3")
            .endpoint()
            .unwrap();
        assert_eq!(
            ep,
            Endpoint {
                host: "cache.local".into(),
                port: 6380,
                tls: true,
                db: 3
            }
        );
        let ep = RedisTarget::new("redis://[::1]").endpoint().unwrap();
        assert_eq!(
            (ep.host.as_str(), ep.port, ep.tls, ep.db),
            ("::1", 6379, false, 0)
        );
        assert_eq!(RedisTarget::new("redis://h/").endpoint().unwrap().db, 0);
        assert_eq!(
            RedisTarget::new("redis://h/4095").endpoint().unwrap().db,
            4095
        );
        for bad in [
            "http://h",
            "redis://user:pw@h",
            "redis://:pw@h",
            "redis://h?db=1",
            "redis://h#x",
            "redis://h/4096",
            "redis://h/01234",
            "redis://h/-1",
            "redis://h/a",
            "redis://h/1/2",
            "redis:///0",
            "redis://h\n",
        ] {
            assert!(RedisTarget::new(bad).endpoint().is_err(), "{bad}");
        }
        assert!(RedisTarget::new(format!("redis://{}", "h".repeat(2048)))
            .endpoint()
            .is_err());
    }

    #[test]
    fn credentials_need_tls_and_valid_ca() {
        let mut t = RedisTarget::new("redis://h");
        t.password_secret = Some("pw".into());
        assert_eq!(t.validate().unwrap_err().code, ErrorCode::PolicyDenied);
        t.url = "rediss://h".into();
        t.validate().unwrap();
        t.password_secret = None;
        t.username_secret = Some("user".into());
        assert_eq!(t.validate().unwrap_err().code, ErrorCode::InvalidArgument);
        let mut t = RedisTarget::new("redis://h");
        t.ca_pem = Some("x".into());
        assert!(t.validate().is_err(), "ca_pem without TLS");
        t.url = "rediss://h".into();
        assert!(t.validate().is_err(), "not a PEM");
        t.ca_pem = Some("x".repeat(MAX_CA_PEM_BYTES + 1));
        assert!(t.validate().is_err());
        let mut t = RedisTarget::new("redis://h");
        t.connect_timeout = Duration::from_millis(99);
        assert_eq!(t.validate().unwrap_err().code, ErrorCode::BoundExceeded);
        t.password_secret = Some(String::new());
        assert!(t.validate().is_err());
    }

    #[test]
    fn bind_checks_policy_and_secrets_and_never_prints_them() {
        let secrets = crate::MapSecretResolver::new(
            [("pw", "hunter2"), ("empty", "")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        let mut t = RedisTarget::new("rediss://cache.local:6380");
        t.password_secret = Some("pw".into());
        let denied = t.bind(&secrets, &TargetPolicy::deny_all()).unwrap_err();
        assert_eq!(denied.code, ErrorCode::PolicyDenied);
        let policy = TargetPolicy::allow("cache.local", 6380);
        let bound = t.bind(&secrets, &policy).unwrap();
        let shown = format!("{bound:?}");
        assert!(
            !shown.contains("hunter2") && !shown.contains("cache.local"),
            "{shown}"
        );
        t.password_secret = Some("empty".into());
        assert_eq!(
            t.bind(&secrets, &policy).unwrap_err().code,
            ErrorCode::SecretMissing
        );
        t.password_secret = Some("absent".into());
        assert!(t.bind(&secrets, &policy).is_err());
    }

    #[test]
    fn error_classes() {
        for (msg, class) in [
            (&b"NOAUTH Authentication required."[..], ErrorClass::Auth),
            (
                b"WRONGPASS invalid username-password pair",
                ErrorClass::Auth,
            ),
            (
                b"NOPERM User u has no permissions to run the 'set' command",
                ErrorClass::Auth,
            ),
            (b"MOVED 3999 127.0.0.1:6381", ErrorClass::Cluster),
            (b"ASK 3999 127.0.0.1:6381", ErrorClass::Cluster),
            (
                b"CROSSSLOT Keys in request don't hash to the same slot",
                ErrorClass::Cluster,
            ),
            (b"CLUSTERDOWN The cluster is down", ErrorClass::Cluster),
            (
                b"READONLY You can't write against a read only replica.",
                ErrorClass::ReadOnly,
            ),
            (
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ErrorClass::WrongType,
            ),
            (
                b"OOM command not allowed when used memory > 'maxmemory'.",
                ErrorClass::Other,
            ),
            (b"ERR syntax error", ErrorClass::Other),
            (b"", ErrorClass::Other),
        ] {
            assert_eq!(
                classify_error(msg),
                class,
                "{}",
                String::from_utf8_lossy(msg)
            );
        }
    }
}

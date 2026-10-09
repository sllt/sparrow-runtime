//! WebSocket client setup shared by the Source and the Sink: URL/allowlist
//! policy, credentials resolved once at bind into redacted header values,
//! TCP/TLS (rustls) connect, the RFC 6455 handshake with optional
//! subprotocols, and the capped, jittered reconnect backoff.
//!
//! Errors never echo the URL (its path/query may carry tokens), header
//! values or credentials; handshake failures report the HTTP status only.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use sparrow_model::{ErrorCode, Result, SparrowError};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::WebSocketStream;

pub(crate) use crate::net::ConnectFailure;
use crate::net::{connect_stream, root_store, Endpoint, NetStream, TlsClient};
pub use crate::net::{reconnect_delay, DEFAULT_RECONNECT_ATTEMPTS, MAX_RECONNECT_ATTEMPTS};
use crate::{SecretResolver, TargetPolicy};

pub const MIN_MESSAGE_BYTES: usize = 1024;
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MESSAGE_BYTES: usize = 64 * 1024;
pub const MAX_HEADERS: usize = 16;
pub const MAX_SUBPROTOCOLS: usize = 8;
const MAX_URL_BYTES: usize = 4096;
const MAX_SECRET_BYTES: usize = 4096;
const READ_BUFFER_BYTES: usize = 16 * 1024;
/// Ledger estimate of the per-connection fixed state (TLS records, handshake,
/// read buffer, task state). Accounting, not an RSS guarantee.
const CONNECTION_FIXED_RESERVATION: usize = 128 * 1024;

pub(crate) fn error(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message.into())
}

/// Handshake authentication. Secret names only; values are resolved at bind.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum WebSocketAuth {
    #[default]
    None,
    Bearer {
        token_secret: String,
    },
    Basic {
        username_secret: String,
        password_secret: String,
    },
}

/// One handshake header: exactly one of `value` (non-secret literal) or
/// `value_secret` (SecretRef).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSocketHeader {
    pub name: String,
    pub value: Option<String>,
    pub value_secret: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSocketClientConfig {
    /// `ws://` or `wss://`; no credentials or fragment. Path/query allowed.
    pub url: String,
    pub auth: WebSocketAuth,
    pub headers: Vec<WebSocketHeader>,
    /// Offered in `Sec-WebSocket-Protocol`; the server must select one of
    /// them (the handshake fails otherwise).
    pub subprotocols: Vec<String>,
    /// PEM CA bundle that *replaces* the built-in web PKI roots for `wss://`
    /// (private CA / self-signed server). Certificate verification stays on.
    pub tls_ca_pem: Option<String>,
    /// TCP connect + TLS + HTTP upgrade, each bounded by this.
    pub connect_timeout: Duration,
    /// Send a Ping when nothing was sent for this long.
    pub ping_interval: Duration,
    /// No frame (data, Ping, Pong, Close) received while waiting to read for
    /// this long = dead connection: closed and reconnected.
    pub idle_timeout: Duration,
    /// Ceiling of the exponential reconnect delay (from 100 ms, jittered in [d/2, d]).
    pub reconnect_max: Duration,
    /// Consecutive failed connects per outage before the connector gives up.
    pub reconnect_attempts: usize,
    /// Largest WebSocket message (and frame) accepted or sent. The protocol
    /// layer (tungstenite 0.30 `FrameCodec::read_frame`) refuses a frame from
    /// its header before reserving its payload; a fragmented message is
    /// refused by its running size before the next fragment is appended (that
    /// fragment, itself <= this bound, has been read).
    pub max_message_bytes: usize,
}

impl WebSocketClientConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth: WebSocketAuth::None,
            headers: Vec::new(),
            subprotocols: Vec::new(),
            tls_ca_pem: None,
            connect_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            reconnect_max: Duration::from_secs(5),
            reconnect_attempts: DEFAULT_RECONNECT_ATTEMPTS,
            max_message_bytes: DEFAULT_MESSAGE_BYTES,
        }
    }

    fn uses_credentials(&self) -> bool {
        self.auth != WebSocketAuth::None || self.headers.iter().any(|h| h.value_secret.is_some())
    }

    /// Static bounds, URL policy and allowlist. Secrets are checked by `bind`.
    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        let ms = |d: Duration| d.as_millis();
        if !(100..=60_000).contains(&ms(self.connect_timeout))
            || !(100..=300_000).contains(&ms(self.ping_interval))
            || !(200..=600_000).contains(&ms(self.idle_timeout))
            || self.idle_timeout <= self.ping_interval
            || !(100..=300_000).contains(&ms(self.reconnect_max))
            || !(1..=MAX_RECONNECT_ATTEMPTS).contains(&self.reconnect_attempts)
            || !(MIN_MESSAGE_BYTES..=MAX_MESSAGE_BYTES).contains(&self.max_message_bytes)
            || self.headers.len() > MAX_HEADERS
            || self.subprotocols.len() > MAX_SUBPROTOCOLS
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket bounds: connect_timeout_ms 100..=60000, ping_interval_ms 100..=300000, idle_timeout_ms 200..=600000 and > ping_interval_ms, reconnect_max_ms 100..=300000, reconnect_attempts 1..=1000, max_message_bytes 1024..=1048576, <=16 headers, <=8 subprotocols",
            ));
        }
        let target = self.target()?;
        if self.uses_credentials() && !target.tls {
            return Err(error(
                ErrorCode::PolicyDenied,
                "WebSocket credentials (auth / secret headers) require a wss:// URL",
            ));
        }
        if self.tls_ca_pem.is_some() && !target.tls {
            return Err(error(
                ErrorCode::InvalidArgument,
                "WebSocket tls_ca_pem requires a wss:// URL",
            ));
        }
        policy
            .check_host_port(&target.host, target.port)
            .map_err(|e| error(e.code(), "WebSocket endpoint is not on the allowlist"))?;
        let mut seen = std::collections::HashSet::new();
        for protocol in &self.subprotocols {
            // RFC 6455 token: visible ASCII without separators.
            if protocol.is_empty()
                || protocol.len() > 128
                || !protocol
                    .bytes()
                    .all(|b| b.is_ascii_graphic() && !b"()<>@,;:\\\"/[]?={}".contains(&b))
                || !seen.insert(protocol.as_str())
            {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "WebSocket subprotocols must be distinct 1..=128 byte RFC 6455 tokens",
                ));
            }
        }
        if let Some(pem) = &self.tls_ca_pem {
            root_store(Some(pem), "WebSocket")?;
        }
        Ok(())
    }

    fn target(&self) -> Result<Endpoint> {
        let invalid = || {
            error(
                ErrorCode::InvalidArgument,
                "WebSocket url must be ws:// or wss:// with a host, no credentials or fragment, <=4096 bytes",
            )
        };
        if self.url.len() > MAX_URL_BYTES {
            return Err(invalid());
        }
        let url = url::Url::parse(&self.url).map_err(|_| invalid())?;
        let tls = match url.scheme() {
            "ws" => false,
            "wss" => true,
            _ => return Err(invalid()),
        };
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err(invalid());
        }
        let host = url.host_str().ok_or_else(invalid)?;
        // `[::1]` → `::1` for the allowlist, TCP connect and TLS name.
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = url.port().unwrap_or(if tls { 443 } else { 80 });
        Ok(Endpoint { host, port, tls })
    }

    /// Per-connection read-side ledger charge: fixed state plus, from the
    /// pinned tungstenite 0.30 read path, one frame payload in the read buffer
    /// and one assembling fragmented message (each <= `max_message_bytes`).
    /// Outgoing data frames are charged by the Sink on top of this.
    pub fn connection_reservation(&self) -> usize {
        CONNECTION_FIXED_RESERVATION.saturating_add(self.max_message_bytes.saturating_mul(2))
    }

    pub(crate) fn protocol_config(&self) -> WebSocketConfig {
        WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER_BYTES)
            .write_buffer_size(0)
            .max_write_buffer_size(self.max_message_bytes.saturating_add(READ_BUFFER_BYTES))
            .max_message_size(Some(self.max_message_bytes))
            .max_frame_size(Some(self.max_message_bytes))
    }

    /// Validate, resolve credentials once and pre-build the TLS settings.
    pub fn bind(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<BoundClient> {
        self.validate(policy)?;
        let target = self.target()?;
        let headers = build_headers(self, secrets)?;
        let tls = if target.tls {
            Some(TlsClient::new(
                &target.host,
                self.tls_ca_pem.as_deref(),
                "WebSocket",
            )?)
        } else {
            None
        };
        Ok(BoundClient {
            config: self.clone(),
            target,
            headers: Arc::new(headers),
            tls,
        })
    }
}

fn reserved_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "connection"
            | "upgrade"
            | "content-length"
            | "transfer-encoding"
            | "sec-websocket-key"
            | "sec-websocket-version"
            | "sec-websocket-extensions"
            | "sec-websocket-protocol"
            | "sec-websocket-accept"
    )
}

fn checked_secret(secrets: &dyn SecretResolver, name: &str) -> Result<String> {
    if name.is_empty() || name.len() > 128 {
        return Err(error(
            ErrorCode::InvalidArgument,
            "WebSocket secret names must be 1..=128 bytes",
        ));
    }
    let value = secrets
        .resolve(name)
        .map_err(|e| error(e.code(), "WebSocket credential SecretRef resolution failed"))?;
    if value.is_empty() || value.len() > MAX_SECRET_BYTES {
        return Err(error(
            ErrorCode::InvalidArgument,
            "WebSocket credential is empty or exceeds 4096 bytes",
        ));
    }
    Ok(value)
}

fn sensitive(value: &str, what: &str) -> Result<HeaderValue> {
    let mut header = HeaderValue::from_str(value).map_err(|_| {
        error(
            ErrorCode::InvalidArgument,
            format!("WebSocket {what} is not a valid header value"),
        )
    })?;
    header.set_sensitive(true);
    Ok(header)
}

fn build_headers(
    config: &WebSocketClientConfig,
    secrets: &dyn SecretResolver,
) -> Result<Vec<(HeaderName, HeaderValue)>> {
    let mut out = Vec::with_capacity(config.headers.len() + 1);
    match &config.auth {
        WebSocketAuth::None => {}
        WebSocketAuth::Bearer { token_secret } => {
            let token = checked_secret(secrets, token_secret)?;
            out.push((
                HeaderName::from_static("authorization"),
                sensitive(&format!("Bearer {token}"), "bearer token")?,
            ));
        }
        WebSocketAuth::Basic {
            username_secret,
            password_secret,
        } => {
            let user = checked_secret(secrets, username_secret)?;
            if user.contains(':') {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "WebSocket basic-auth username must not contain ':'",
                ));
            }
            let password = checked_secret(secrets, password_secret)?;
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
            out.push((
                HeaderName::from_static("authorization"),
                sensitive(&format!("Basic {encoded}"), "basic credential")?,
            ));
        }
    }
    let mut seen: std::collections::HashSet<HeaderName> =
        out.iter().map(|(name, _)| name.clone()).collect();
    for header in &config.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|_| error(ErrorCode::InvalidArgument, "invalid WebSocket header name"))?;
        if reserved_header(&name) {
            return Err(error(
                ErrorCode::InvalidArgument,
                format!("WebSocket header `{name}` is managed by the connector"),
            ));
        }
        if !seen.insert(name.clone()) {
            return Err(error(
                ErrorCode::InvalidArgument,
                format!("duplicate WebSocket header `{name}` (auth sets authorization)"),
            ));
        }
        let value = match (&header.value, &header.value_secret) {
            (Some(value), None) => {
                if value.len() > MAX_SECRET_BYTES {
                    return Err(error(
                        ErrorCode::BoundExceeded,
                        "WebSocket header value exceeds 4096 bytes",
                    ));
                }
                HeaderValue::from_str(value).map_err(|_| {
                    error(
                        ErrorCode::InvalidArgument,
                        format!("WebSocket header `{name}` has an invalid value"),
                    )
                })?
            }
            (None, Some(secret)) => sensitive(&checked_secret(secrets, secret)?, "secret header")?,
            _ => {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    format!(
                        "WebSocket header `{name}` requires exactly one of value or value_secret"
                    ),
                ))
            }
        };
        out.push((name, value));
    }
    Ok(out)
}

pub(crate) type WsStream = WebSocketStream<NetStream>;

/// A validated client with credentials resolved into redacted header values.
#[derive(Clone)]
pub struct BoundClient {
    pub(crate) config: WebSocketClientConfig,
    target: Endpoint,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    tls: Option<TlsClient>,
}

impl std::fmt::Debug for BoundClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundClient")
            .field("host", &self.target.host)
            .field("port", &self.target.port)
            .field("tls", &self.target.tls)
            .field("headers", &self.headers.len())
            .finish_non_exhaustive()
    }
}

impl BoundClient {
    /// One connect attempt: TCP, TLS (wss) and the HTTP upgrade together
    /// within `connect_timeout`.
    pub(crate) async fn connect(&self) -> std::result::Result<WsStream, ConnectFailure> {
        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let io = connect_stream(&self.target, self.tls.as_ref(), deadline, None).await?;
        let mut request = self
            .config
            .url
            .as_str()
            .into_client_request()
            .map_err(|_| ConnectFailure::Handshake)?;
        for (name, value) in self.headers.iter() {
            request.headers_mut().insert(name.clone(), value.clone());
        }
        if !self.config.subprotocols.is_empty() {
            let offered = self.config.subprotocols.join(", ");
            request.headers_mut().insert(
                HeaderName::from_static("sec-websocket-protocol"),
                HeaderValue::from_str(&offered).map_err(|_| ConnectFailure::Handshake)?,
            );
        }
        let (ws, _response) = tokio::time::timeout_at(
            deadline,
            tokio_tungstenite::client_async_with_config(
                request,
                io,
                Some(self.config.protocol_config()),
            ),
        )
        .await
        .map_err(|_| ConnectFailure::Timeout)?
        .map_err(|e| match e {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                ConnectFailure::Rejected(response.status().as_u16())
            }
            _ => ConnectFailure::Handshake,
        })?;
        Ok(ws)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MapSecretResolver;

    fn secrets() -> MapSecretResolver {
        MapSecretResolver::new(
            [
                ("tok".to_string(), "hunter2".to_string()),
                ("user".to_string(), "u".to_string()),
                ("bad-user".to_string(), "a:b".to_string()),
            ]
            .into_iter()
            .collect(),
        )
    }

    #[test]
    fn websocket_client_url_policy_bounds_and_secret_rules() {
        let p = TargetPolicy::allow("127.0.0.1", 9000).with_allow("localhost", 443);
        let ok = WebSocketClientConfig::new("ws://127.0.0.1:9000/feed?x=1");
        ok.validate(&p).unwrap();
        WebSocketClientConfig::new("wss://localhost/feed")
            .validate(&p)
            .unwrap();
        type M = fn(&mut WebSocketClientConfig);
        let cases: [(M, ErrorCode); 13] = [
            (
                |c| c.url = "http://127.0.0.1:9000/".into(),
                ErrorCode::InvalidArgument,
            ),
            (
                |c| c.url = "ws://u:hunter2@127.0.0.1:9000/".into(),
                ErrorCode::InvalidArgument,
            ),
            (
                |c| c.url = "ws://127.0.0.1:9000/#f".into(),
                ErrorCode::InvalidArgument,
            ),
            (
                |c| c.url = "ws://127.0.0.1:9001/".into(),
                ErrorCode::PolicyDenied,
            ),
            (
                |c| c.url = "ws://169.254.169.254:9000/".into(),
                ErrorCode::PolicyDenied,
            ),
            (
                |c| {
                    c.auth = WebSocketAuth::Bearer {
                        token_secret: "tok".into(),
                    }
                },
                ErrorCode::PolicyDenied,
            ),
            (
                |c| c.tls_ca_pem = Some("x".into()),
                ErrorCode::InvalidArgument,
            ),
            (|c| c.max_message_bytes = 10, ErrorCode::BoundExceeded),
            (
                |c| c.idle_timeout = c.ping_interval,
                ErrorCode::BoundExceeded,
            ),
            (|c| c.reconnect_attempts = 0, ErrorCode::BoundExceeded),
            (
                |c| c.subprotocols = vec!["a b".into()],
                ErrorCode::InvalidArgument,
            ),
            (
                |c| c.subprotocols = vec!["v1".into(), "v1".into()],
                ErrorCode::InvalidArgument,
            ),
            (
                |c| c.connect_timeout = Duration::ZERO,
                ErrorCode::BoundExceeded,
            ),
        ];
        for (i, (mutate, code)) in cases.into_iter().enumerate() {
            let mut c = ok.clone();
            mutate(&mut c);
            let e = c.validate(&p).unwrap_err();
            assert_eq!(e.code, code, "case {i}: {}", e.message);
            assert!(
                !e.message.contains("hunter2") && !e.message.contains("169.254"),
                "{e}"
            );
        }
        // Credentials resolve at bind; values never appear in errors/Debug.
        let mut c = WebSocketClientConfig::new("wss://localhost/feed");
        c.auth = WebSocketAuth::Bearer {
            token_secret: "tok".into(),
        };
        c.headers = vec![WebSocketHeader {
            name: "x-tenant".into(),
            value: Some("t1".into()),
            value_secret: None,
        }];
        let bound = c.bind(&secrets(), &p).unwrap();
        assert!(!format!("{bound:?}").contains("hunter2"));
        let headers = build_headers(&c, &secrets()).unwrap();
        assert_eq!(headers[0].1.to_str().unwrap(), "Bearer hunter2");
        assert!(headers[0].1.is_sensitive());
        for (header, value, secret) in [
            ("authorization", Some("x"), None),
            ("sec-websocket-protocol", Some("x"), None),
            ("x-a", None, None),
            ("x-a", Some("x"), Some("tok")),
            ("x-a", None, Some("missing")),
        ] {
            let mut bad = c.clone();
            bad.headers = vec![WebSocketHeader {
                name: header.into(),
                value: value.map(Into::into),
                value_secret: secret.map(Into::into),
            }];
            let e = bad.bind(&secrets(), &p).unwrap_err();
            assert!(!e.message.contains("hunter2"), "{e}");
        }
        c.auth = WebSocketAuth::Basic {
            username_secret: "bad-user".into(),
            password_secret: "tok".into(),
        };
        assert!(c.bind(&secrets(), &p).is_err());
        let mut ca = WebSocketClientConfig::new("wss://localhost/feed");
        ca.tls_ca_pem =
            Some("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".into());
        assert_eq!(
            ca.validate(&p).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        ca.tls_ca_pem = Some(String::from_utf8(super::super::tls_fixture::CA.to_vec()).unwrap());
        ca.validate(&p).unwrap();
    }
}

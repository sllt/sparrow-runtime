//! WebSocket client setup shared by the Source and the Sink: URL/allowlist
//! policy, credentials resolved once at bind into redacted header values,
//! TCP/TLS (rustls) connect, the RFC 6455 handshake with optional
//! subprotocols, and the capped, jittered reconnect backoff.
//!
//! Errors never echo the URL (its path/query may carry tokens), header
//! values or credentials; handshake failures report the HTTP status only.

use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::Engine;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use sparrow_model::{ErrorCode, Result, SparrowError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;

use crate::{SecretResolver, TargetPolicy};

pub const MIN_MESSAGE_BYTES: usize = 1024;
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MESSAGE_BYTES: usize = 64 * 1024;
pub const MAX_RECONNECT_ATTEMPTS: usize = 1000;
pub const DEFAULT_RECONNECT_ATTEMPTS: usize = 10;
pub const MAX_HEADERS: usize = 16;
pub const MAX_SUBPROTOCOLS: usize = 8;
pub const MAX_HEADER_NAME_BYTES: usize = 128;
pub const MAX_HANDSHAKE_BYTES: usize = 16 * 1024;
const MAX_URL_BYTES: usize = 4096;
const MAX_SECRET_BYTES: usize = 4096;
const MAX_CA_PEM_BYTES: usize = 64 * 1024;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const READ_BUFFER_BYTES: usize = 16 * 1024;
/// Ledger estimate of the per-connection fixed state (TLS records, handshake,
/// read buffer, task state). Accounting, not an RSS guarantee.
const CONNECTION_FIXED_RESERVATION: usize = 128 * 1024;

fn install_rustls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

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
    /// One total deadline for TCP connect + TLS + HTTP upgrade.
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
        if self.headers.iter().any(|h| {
            h.name.len() > MAX_HEADER_NAME_BYTES
                || h.value.as_ref().is_some_and(|v| v.len() > MAX_SECRET_BYTES)
        }) {
            return Err(error(
                ErrorCode::BoundExceeded,
                "WebSocket header names must be <=128 bytes; values <=4096 bytes",
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
            root_store(Some(pem))?;
        }
        Ok(())
    }

    fn target(&self) -> Result<Target> {
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
        Ok(Target { host, port, tls })
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
            install_rustls_provider();
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store(self.tls_ca_pem.as_deref())?)
                .with_no_client_auth();
            let name = ServerName::try_from(target.host.clone()).map_err(|_| {
                error(
                    ErrorCode::InvalidArgument,
                    "WebSocket TLS server name invalid",
                )
            })?;
            Some((tokio_rustls::TlsConnector::from(Arc::new(config)), name))
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
    tls: bool,
}

fn root_store(pem: Option<&str>) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    match pem {
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        Some(pem) => {
            let invalid = || {
                error(
                    ErrorCode::InvalidArgument,
                    "WebSocket tls_ca_pem must hold 1..=16 PEM CA certificates within 64 KiB",
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

fn credential_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "authorization" | "proxy-authorization" | "cookie"
    )
}

/// Count the actual request line and connector-managed headers. The SDK's
/// frame/message bounds do not apply to the HTTP upgrade request.
fn handshake_base_bytes(config: &WebSocketClientConfig) -> Result<usize> {
    let request = config.url.as_str().into_client_request().map_err(|_| {
        error(
            ErrorCode::InvalidArgument,
            "invalid WebSocket handshake URL",
        )
    })?;
    let mut bytes = 15usize
        .saturating_add(
            request
                .uri()
                .path_and_query()
                .map_or(1, |p| p.as_str().len()),
        )
        .saturating_add(2);
    for (name, value) in request.headers() {
        bytes = bytes
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len())
            .saturating_add(4);
    }
    if !config.subprotocols.is_empty() {
        bytes = bytes
            .saturating_add("sec-websocket-protocol".len() + 4)
            .saturating_add(
                config
                    .subprotocols
                    .iter()
                    .map(String::len)
                    .fold(0usize, usize::saturating_add),
            )
            .saturating_add(
                config
                    .subprotocols
                    .len()
                    .saturating_sub(1)
                    .saturating_mul(2),
            );
    }
    Ok(bytes)
}

fn admit_header(bytes: &mut usize, name: &HeaderName, value: &HeaderValue) -> Result<()> {
    *bytes = bytes
        .saturating_add(name.as_str().len())
        .saturating_add(value.as_bytes().len())
        .saturating_add(4);
    if *bytes > MAX_HANDSHAKE_BYTES {
        return Err(error(
            ErrorCode::BoundExceeded,
            "WebSocket complete upgrade request exceeds 16 KiB",
        ));
    }
    Ok(())
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
    let mut bytes = handshake_base_bytes(config)?;
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
    for (name, value) in &out {
        admit_header(&mut bytes, name, value)?;
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
                if credential_header(&name) {
                    return Err(error(
                        ErrorCode::InvalidArgument,
                        "WebSocket authentication headers require auth or value_secret over wss://",
                    ));
                }
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
        admit_header(&mut bytes, &name, &value)?;
        out.push((name, value));
    }
    Ok(out)
}

pub(crate) trait WsIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> WsIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
pub(crate) type WsStream = WebSocketStream<Pin<Box<dyn WsIo>>>;
pub(crate) type WsRead = SplitStream<WsStream>;
pub(crate) type WsWrite = SplitSink<WsStream, Message>;
pub(crate) type Incoming = Option<std::result::Result<Message, tungstenite::Error>>;

/// The first observer of cancellation fixes one deadline for both the pump
/// and the writer, including any send already in flight.
pub(crate) struct FlushBudget {
    timeout: Duration,
    deadline: OnceLock<Instant>,
}

impl FlushBudget {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: OnceLock::new(),
        }
    }

    pub(crate) fn deadline(&self, cancel: &CancellationToken) -> Option<Instant> {
        if cancel.is_cancelled() {
            Some(*self.deadline.get_or_init(|| Instant::now() + self.timeout))
        } else {
            self.deadline.get().copied()
        }
    }

    pub(crate) fn expired(&self, cancel: &CancellationToken) -> bool {
        self.deadline(cancel).is_some_and(|at| Instant::now() >= at)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DuplexSent {
    Ok,
    Failed,
    ReadClosed,
    TimedOut,
    Idle,
    Cancelled,
}

/// A blocked write must not stop reading control frames. SplitStream's
/// protocol reader also flushes automatic Pong replies without blocking its
/// reads; the split lock is released whenever either poll returns Pending.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_duplex(
    writer: &mut WsWrite,
    reader: &mut WsRead,
    message: Message,
    cancel: &CancellationToken,
    flush: Option<&FlushBudget>,
    timeout: Duration,
    idle_timeout: Duration,
    last_frame: &mut Instant,
    read_quota: usize,
    mut on_frame: impl FnMut(Incoming) -> bool,
) -> DuplexSent {
    let send_deadline = Instant::now() + timeout;
    let send = writer.send(message);
    tokio::pin!(send);
    let mut reads = 0usize;
    loop {
        let flush_deadline = flush.and_then(|budget| budget.deadline(cancel));
        let at = flush_deadline.map_or(send_deadline, |f| f.min(send_deadline));
        if flush.is_none() && cancel.is_cancelled() {
            return DuplexSent::Cancelled;
        }
        if Instant::now() >= at {
            return DuplexSent::TimedOut;
        }
        let idle = *last_frame + idle_timeout;
        if Instant::now() >= idle {
            return DuplexSent::Idle;
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled(), if flush_deadline.is_none() => {
                if let Some(budget) = flush {
                    budget.deadline(cancel);
                } else {
                    return DuplexSent::Cancelled;
                }
            }
            _ = tokio::time::sleep_until(at) => return DuplexSent::TimedOut,
            _ = tokio::time::sleep_until(idle) => return DuplexSent::Idle,
            result = &mut send => return if result.is_ok() { DuplexSent::Ok } else { DuplexSent::Failed },
            frame = reader.next() => {
                if matches!(&frame, Some(Ok(_))) {
                    *last_frame = Instant::now();
                }
                if !on_frame(frame) {
                    return DuplexSent::ReadClosed;
                }
                reads += 1;
                // Also cooperate with timers/cancellation for an in-memory
                // transport whose reads are forever immediately Ready.
                if reads % read_quota.clamp(1, 16) == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

/// A validated client with credentials resolved into redacted header values.
#[derive(Clone)]
pub struct BoundClient {
    pub(crate) config: WebSocketClientConfig,
    target: Target,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    tls: Option<(tokio_rustls::TlsConnector, ServerName<'static>)>,
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

/// Why one connect attempt failed (counted, then retried with backoff).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectFailure {
    Tcp,
    Tls,
    Handshake,
    /// The server answered the upgrade with this HTTP status.
    Rejected(u16),
    Timeout,
}

impl ConnectFailure {
    pub(crate) fn reason(self) -> String {
        match self {
            Self::Tcp => "WebSocket TCP connect failed".into(),
            Self::Tls => {
                "WebSocket TLS handshake failed (certificate verification is mandatory)".into()
            }
            Self::Handshake => {
                "WebSocket upgrade handshake failed (protocol/subprotocol mismatch)".into()
            }
            Self::Rejected(status) => format!("WebSocket upgrade rejected with HTTP {status}"),
            Self::Timeout => "WebSocket connect timed out".into(),
        }
    }
}

impl BoundClient {
    /// One bounded connect attempt: TCP, TLS (wss), HTTP upgrade.
    pub(crate) async fn connect(&self) -> std::result::Result<WsStream, ConnectFailure> {
        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let tcp = tokio::time::timeout_at(
            deadline,
            TcpStream::connect((self.target.host.as_str(), self.target.port)),
        )
        .await
        .map_err(|_| ConnectFailure::Timeout)?
        .map_err(|_| ConnectFailure::Tcp)?;
        let _ = tcp.set_nodelay(true);
        let io: Pin<Box<dyn WsIo>> = match &self.tls {
            None => Box::pin(tcp),
            Some((connector, name)) => Box::pin(
                tokio::time::timeout_at(deadline, connector.connect(name.clone(), tcp))
                    .await
                    .map_err(|_| ConnectFailure::Timeout)?
                    .map_err(|_| ConnectFailure::Tls)?,
            ),
        };
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

/// Exponential from 100 ms, capped at `max`, with "equal" jitter in
/// `[delay/2, delay]` so a fleet of clients does not reconnect in lockstep.
pub fn reconnect_delay(attempt: usize, max: Duration) -> Duration {
    let exp = attempt.saturating_sub(1).min(16) as u32;
    let delay = INITIAL_RECONNECT_DELAY
        .saturating_mul(1u32 << exp)
        .min(max)
        .max(Duration::from_millis(1));
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_usize(attempt);
    let half = delay.as_nanos() as u64 / 2;
    Duration::from_nanos(half + hasher.finish() % (half + 1))
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

    #[test]
    fn websocket_reconnect_delay_is_capped_and_jittered() {
        let max = Duration::from_millis(800);
        for attempt in 1..=40 {
            let d = reconnect_delay(attempt, max);
            let full = Duration::from_millis(100 * (1u64 << (attempt - 1).min(16))).min(max);
            assert!(d >= full / 2 && d <= full, "attempt {attempt}: {d:?}");
        }
        let spread: std::collections::HashSet<_> =
            (0..32).map(|_| reconnect_delay(6, max)).collect();
        assert!(spread.len() > 1, "jitter must vary the delay");
    }

    #[test]
    fn websocket_auth_headers_cannot_bypass_secretref_or_tls_and_handshake_is_bounded() {
        let policy = TargetPolicy::allow("127.0.0.1", 9000);
        for name in ["Authorization", "pRoXy-AuThOrIzAtIoN", "Cookie"] {
            for scheme in ["ws", "wss"] {
                let mut c = WebSocketClientConfig::new(format!("{scheme}://127.0.0.1:9000/"));
                c.headers.push(WebSocketHeader {
                    name: name.into(),
                    value: Some("Bearer hunter2".into()),
                    value_secret: None,
                });
                let e = c.bind(&secrets(), &policy).unwrap_err();
                assert_eq!(e.code, ErrorCode::InvalidArgument);
                assert!(!e.message.contains("hunter2"));
            }
            let mut c = WebSocketClientConfig::new("ws://127.0.0.1:9000/");
            c.headers.push(WebSocketHeader {
                name: name.into(),
                value: None,
                value_secret: Some("tok".into()),
            });
            assert_eq!(
                c.bind(&secrets(), &policy).unwrap_err().code,
                ErrorCode::PolicyDenied
            );
            c.url = "wss://127.0.0.1:9000/".into();
            let bound = c.bind(&secrets(), &policy).unwrap();
            assert!(!format!("{bound:?}").contains("hunter2"));
            assert!(build_headers(&c, &secrets()).unwrap()[0].1.is_sensitive());
        }
        let mut c = WebSocketClientConfig::new("ws://127.0.0.1:9000/");
        c.headers.push(WebSocketHeader {
            name: "x".repeat(MAX_HEADER_NAME_BYTES + 1),
            value: Some("v".into()),
            value_secret: None,
        });
        assert_eq!(
            c.bind(&secrets(), &policy).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        c.headers = (0..4)
            .map(|i| WebSocketHeader {
                name: format!("x-{i}"),
                value: Some("x".repeat(MAX_SECRET_BYTES)),
                value_secret: None,
            })
            .collect();
        assert_eq!(
            c.bind(&secrets(), &policy).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        let secret = MapSecretResolver::new(std::collections::HashMap::from([(
            "big".into(),
            "s".repeat(MAX_SECRET_BYTES),
        )]));
        c.url = "wss://127.0.0.1:9000/".into();
        for header in &mut c.headers {
            header.value = None;
            header.value_secret = Some("big".into());
        }
        assert_eq!(
            c.bind(&secret, &policy).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
    }
}

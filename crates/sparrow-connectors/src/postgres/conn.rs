//! PostgreSQL endpoint, credentials, TLS and one tokio-postgres connection.
//!
//! `postgresql://host[:port]/dbname` (no userinfo, no query): the role name
//! is the plain `user` field, the password a secret reference. `sslmode` is
//! one of
//!
//! * `verify-full` (default): TLS is required (a server answering the
//!   SSLRequest with `N` fails the connect), the chain is verified against
//!   the built-in web roots or `ca_pem`, and the certificate must name the
//!   URL host;
//! * `disable`: plain TCP; refused together with a password (the server, or
//!   anyone on the path, may ask for the cleartext password).
//!
//! `prefer`, `allow`, `require` and `verify-ca` are refused: they either
//! fall back to plain text or skip the host-name check. SCRAM channel
//! binding is not used (`channel_binding=disable`); the server identity comes
//! from `verify-full`.
//!
//! Every byte the server sends passes [`Guarded`], which fails the
//! connection on any backend message longer than the role's limit before
//! tokio-postgres buffers it (its codec has no message-size limit of its
//! own).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use sparrow_model::{ErrorCode, Result, SparrowError};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_postgres::config::{ChannelBinding as ChannelBindingMode, SslMode};
use tokio_postgres::tls::{ChannelBinding, NoTls, TlsConnect, TlsStream};
use tokio_postgres::{CancelToken, Client};

use crate::{SecretResolver, TargetPolicy};

pub const MAX_CA_PEM_BYTES: usize = 64 * 1024;
const MAX_SECRET_BYTES: usize = 4096;
const MAX_URL_BYTES: usize = 2048;
const MAX_NAME_BYTES: usize = 63;
/// Per-connection ledger: socket/TLS buffers, the client's read and write
/// buffers and one response chunk (an accounting estimate, not an RSS
/// guarantee).
pub const CONNECTION_RESERVATION: usize = 128 * 1024;
/// Slack over a role's row bound for the DataRow framing, row
/// descriptions and error/notice messages.
pub const MESSAGE_SLACK: usize = 64 * 1024;

pub(crate) fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message.into())
}

fn install_rustls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// `"name"` with embedded quotes doubled. Names are 1..=63 bytes (the
/// server's NAMEDATALEN) without NUL or control characters.
pub fn quote_ident(name: &str) -> Result<String> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(err(
            ErrorCode::InvalidArgument,
            "PostgreSQL identifiers must be 1..=63 bytes without control characters",
        ));
    }
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    Ok(out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgSslMode {
    Disable,
    VerifyFull,
}

impl PgSslMode {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "disable" => Ok(Self::Disable),
            "verify-full" => Ok(Self::VerifyFull),
            "prefer" | "allow" => Err(err(
                ErrorCode::PolicyDenied,
                format!("PostgreSQL sslmode={s} may fall back to plain text; use verify-full (or disable for a trusted local link without a password)"),
            )),
            "require" | "verify-ca" => Err(err(
                ErrorCode::PolicyDenied,
                format!("PostgreSQL sslmode={s} does not verify the server host name; use verify-full (ca_pem for a private CA)"),
            )),
            _ => Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL sslmode must be disable or verify-full",
            )),
        }
    }
}

/// Where and how to connect. Holds the password secret *name*, never its
/// value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgTarget {
    pub url: String,
    pub user: String,
    pub password_secret: Option<String>,
    pub sslmode: PgSslMode,
    pub ca_pem: Option<String>,
    pub connect_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub dbname: String,
}

impl PgTarget {
    pub fn new(url: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            user: user.into(),
            password_secret: None,
            sslmode: PgSslMode::VerifyFull,
            ca_pem: None,
            connect_timeout: Duration::from_secs(5),
        }
    }

    pub fn endpoint(&self) -> Result<Endpoint> {
        if self.url.len() > MAX_URL_BYTES || self.url.chars().any(char::is_control) {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL url exceeds 2048 bytes or contains control characters",
            ));
        }
        let url = url::Url::parse(&self.url).map_err(|_| {
            err(
                ErrorCode::InvalidArgument,
                "PostgreSQL url is not a valid URL",
            )
        })?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL url scheme must be postgresql:// (or postgres://)",
            ));
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL url must not carry userinfo, query or fragment (user is a field, the password a secret reference, TLS is sslmode)",
            ));
        }
        let host = match url.host() {
            Some(url::Host::Domain(d)) if !d.is_empty() => d.to_string(),
            Some(url::Host::Ipv4(a)) => a.to_string(),
            Some(url::Host::Ipv6(a)) => a.to_string(),
            _ => {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL url needs a host",
                ))
            }
        };
        let port = url.port().unwrap_or(5432);
        let path = url.path().strip_prefix('/').unwrap_or("");
        let dbname = percent_decode(path).ok_or_else(|| {
            err(
                ErrorCode::InvalidArgument,
                "PostgreSQL url path must be /<dbname> (1..=63 bytes, UTF-8, no '/')",
            )
        })?;
        if dbname.is_empty()
            || dbname.len() > MAX_NAME_BYTES
            || dbname.contains('/')
            || dbname.chars().any(char::is_control)
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL url path must be /<dbname> (1..=63 bytes, UTF-8, no '/')",
            ));
        }
        Ok(Endpoint { host, port, dbname })
    }

    pub fn validate(&self) -> Result<()> {
        self.endpoint()?;
        if self.user.is_empty()
            || self.user.len() > MAX_NAME_BYTES
            || self.user.chars().any(char::is_control)
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL user must be 1..=63 bytes without control characters",
            ));
        }
        if let Some(name) = &self.password_secret {
            if name.is_empty() || name.len() > 128 {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL secret reference must be 1..=128 bytes",
                ));
            }
            if self.sslmode != PgSslMode::VerifyFull {
                return Err(err(
                    ErrorCode::PolicyDenied,
                    "PostgreSQL password_secret requires sslmode=verify-full; refusing to send a password over an unauthenticated link",
                ));
            }
        }
        if let Some(pem) = &self.ca_pem {
            if self.sslmode != PgSslMode::VerifyFull {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL ca_pem requires sslmode=verify-full",
                ));
            }
            root_store(Some(pem))?;
        }
        let ms = self.connect_timeout.as_millis();
        if !(100..=60_000).contains(&ms) {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL connect_timeout_ms must be 100..=60000",
            ));
        }
        Ok(())
    }

    /// Check the target policy, resolve the password and build TLS
    /// settings. `max_message` bounds every backend message.
    pub fn bind(
        &self,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        max_message: usize,
    ) -> Result<BoundTarget> {
        self.validate()?;
        let endpoint = self.endpoint()?;
        policy
            .check_host_port(&endpoint.host, endpoint.port)
            .map_err(SparrowError::from)?;
        let password = match &self.password_secret {
            Some(name) => {
                let value = secrets.resolve(name).map_err(SparrowError::from)?;
                if value.is_empty() || value.len() > MAX_SECRET_BYTES {
                    return Err(err(
                        ErrorCode::SecretMissing,
                        "PostgreSQL password secret is empty or exceeds 4096 bytes",
                    ));
                }
                Some(value)
            }
            None => None,
        };
        let tls = match self.sslmode {
            PgSslMode::Disable => None,
            PgSslMode::VerifyFull => {
                install_rustls_provider();
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store(self.ca_pem.as_deref())?)
                    .with_no_client_auth();
                let name = ServerName::try_from(endpoint.host.clone()).map_err(|_| {
                    err(
                        ErrorCode::InvalidArgument,
                        "PostgreSQL TLS server name invalid",
                    )
                })?;
                Some(PgTls {
                    connector: tokio_rustls::TlsConnector::from(Arc::new(config)),
                    name,
                    max_message,
                })
            }
        };
        let mut config = tokio_postgres::Config::new();
        config
            .user(&self.user)
            .dbname(&endpoint.dbname)
            .application_name("sparrow")
            .channel_binding(ChannelBindingMode::Disable)
            .ssl_mode(if tls.is_some() {
                SslMode::Require
            } else {
                SslMode::Disable
            });
        if let Some(password) = &password {
            config.password(password.as_bytes());
        }
        Ok(BoundTarget {
            endpoint,
            config,
            tls,
            connect_timeout: self.connect_timeout,
            max_message,
        })
    }
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn root_store(pem: Option<&str>) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    match pem {
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        Some(pem) => {
            let invalid = || {
                err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL ca_pem must hold 1..=16 PEM CA certificates within 64 KiB",
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

/// Reader-side bound on backend messages. Parses the `tag, int32 length`
/// framing of everything read and fails the read (`InvalidData`) as soon as
/// a header announces a body longer than `max`, before tokio-postgres's
/// codec would wait for and buffer it.
pub struct Guarded<S> {
    inner: S,
    max: usize,
    header: [u8; 5],
    header_len: usize,
    remaining: usize,
}

impl<S> Guarded<S> {
    pub fn new(inner: S, max: usize) -> Self {
        Self {
            inner,
            max,
            header: [0; 5],
            header_len: 0,
            remaining: 0,
        }
    }

    /// Account for `bytes` just read; `false` on an oversize or malformed
    /// header.
    pub fn observe(&mut self, bytes: &[u8]) -> bool {
        let mut i = 0;
        while i < bytes.len() {
            if self.remaining > 0 {
                let take = self.remaining.min(bytes.len() - i);
                self.remaining -= take;
                i += take;
                continue;
            }
            self.header[self.header_len] = bytes[i];
            self.header_len += 1;
            i += 1;
            if self.header_len == 5 {
                self.header_len = 0;
                let len = i32::from_be_bytes([
                    self.header[1],
                    self.header[2],
                    self.header[3],
                    self.header[4],
                ]);
                let Ok(len) = usize::try_from(len) else {
                    return false;
                };
                if len < 4 || len - 4 > self.max {
                    return false;
                }
                self.remaining = len - 4;
            }
        }
        true
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Guarded<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if this.observe(&buf.filled()[before..]) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "postgres_backend_message_exceeds_limit",
                    )))
                }
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Guarded<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl TlsStream for Guarded<tokio_rustls::client::TlsStream<TcpStream>> {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

/// rustls `verify-full` connector handed to tokio-postgres after the
/// SSLRequest was accepted.
#[derive(Clone)]
pub struct PgTls {
    connector: tokio_rustls::TlsConnector,
    name: ServerName<'static>,
    max_message: usize,
}

type TlsFuture<T> = Pin<Box<dyn Future<Output = std::io::Result<T>> + Send>>;

impl TlsConnect<TcpStream> for PgTls {
    type Stream = Guarded<tokio_rustls::client::TlsStream<TcpStream>>;
    type Error = std::io::Error;
    type Future = TlsFuture<Self::Stream>;

    fn connect(self, stream: TcpStream) -> Self::Future {
        Box::pin(async move {
            let tls = self.connector.connect(self.name, stream).await?;
            Ok(Guarded::new(tls, self.max_message))
        })
    }
}

pub struct BoundTarget {
    pub endpoint: Endpoint,
    config: tokio_postgres::Config,
    tls: Option<PgTls>,
    connect_timeout: Duration,
    max_message: usize,
}

impl std::fmt::Debug for BoundTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No host (may be internal) and never credentials.
        f.debug_struct("BoundTarget")
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

/// Why a connection could not be established. No statement was sent.
#[derive(Debug)]
pub enum ConnectError {
    /// TCP/TLS/timeout/peer closed/server busy: retryable.
    Io(&'static str),
    /// The server refused the role, password or database: configuration,
    /// not retryable.
    Rejected(ErrorCode, &'static str),
}

impl ConnectError {
    pub fn reason(&self) -> &'static str {
        match self {
            ConnectError::Io(r) | ConnectError::Rejected(_, r) => r,
        }
    }
}

/// What a server error means for the caller, from its SQLSTATE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlClass {
    /// Connection, shutdown, serialization/deadlock, resources, cancel:
    /// the transaction was rolled back; retrying may succeed.
    Transient,
    /// Integrity/data errors in the rows (23xxx, 22xxx, 21000): the batch
    /// is wrong, retrying cannot help.
    Data,
    /// Authentication, privileges, missing objects, syntax, unsupported
    /// feature: configuration; stop.
    Fatal,
    /// Anything else: fail this request.
    Other,
}

pub fn classify_sqlstate(code: &str) -> SqlClass {
    let class = code.get(..2).unwrap_or("");
    match class {
        "08" | "53" | "40" => SqlClass::Transient,
        "57" if matches!(code, "57P01" | "57P02" | "57P03" | "57014") => SqlClass::Transient,
        "23" | "22" | "21" => SqlClass::Data,
        "28" | "42" | "3D" | "3F" | "0A" | "2B" => SqlClass::Fatal,
        "55" if code == "55P03" => SqlClass::Transient,
        _ => SqlClass::Other,
    }
}

/// SQLSTATE of a server error, `None` for client-side/I/O errors.
pub fn sqlstate(e: &tokio_postgres::Error) -> Option<&str> {
    e.as_db_error().map(|db| db.code().code())
}

/// Aborts the connection task when dropped (closing the socket, which
/// makes the server roll back an open transaction).
struct TaskGuard(tokio::task::JoinHandle<()>);
impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One established session. Dropping it closes the socket.
pub struct PgConn {
    pub client: Client,
    cancel: CancelToken,
    _task: TaskGuard,
}

impl PgConn {
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }
}

/// Sends a cancel request for the session's running statement when dropped
/// while armed: a dropped query future does not stop the server (the
/// client keeps draining its replies), so an abandoned statement is
/// cancelled explicitly. The request runs on a spawned task bounded by the
/// connect timeout.
pub struct CancelOnDrop {
    token: Option<CancelToken>,
    target: Arc<BoundTarget>,
}

impl CancelOnDrop {
    pub fn new(conn: &PgConn, target: &Arc<BoundTarget>) -> Self {
        Self {
            token: Some(conn.cancel.clone()),
            target: target.clone(),
        }
    }

    pub fn disarm(mut self) {
        self.token = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let target = self.target.clone();
                handle.spawn(async move {
                    let _ = target.cancel(&token).await;
                });
            }
        }
    }
}

impl BoundTarget {
    pub fn tls(&self) -> bool {
        self.tls.is_some()
    }

    pub fn max_message(&self) -> usize {
        self.max_message
    }

    async fn tcp(&self) -> std::result::Result<TcpStream, ConnectError> {
        let tcp = TcpStream::connect((self.endpoint.host.as_str(), self.endpoint.port))
            .await
            .map_err(|_| ConnectError::Io("postgres_connect_failed"))?;
        let _ = tcp.set_nodelay(true);
        Ok(tcp)
    }

    /// TCP, TLS (`verify-full`), startup and authentication, all within
    /// `connect_timeout`. The connection task is spawned on the current
    /// runtime and aborted when the returned [`PgConn`] drops.
    pub async fn connect(&self) -> std::result::Result<PgConn, ConnectError> {
        match tokio::time::timeout(self.connect_timeout, self.connect_inner()).await {
            Ok(result) => result,
            Err(_) => Err(ConnectError::Io("postgres_connect_timeout")),
        }
    }

    async fn connect_inner(&self) -> std::result::Result<PgConn, ConnectError> {
        let tcp = self.tcp().await?;
        let (client, task) = match &self.tls {
            Some(tls) => {
                let (client, connection) = self
                    .config
                    .connect_raw(tcp, tls.clone())
                    .await
                    .map_err(startup_error)?;
                (
                    client,
                    tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                )
            }
            None => {
                let guarded = Guarded::new(tcp, self.max_message);
                let (client, connection) = self
                    .config
                    .connect_raw(guarded, NoTls)
                    .await
                    .map_err(startup_error)?;
                (
                    client,
                    tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                )
            }
        };
        let cancel = client.cancel_token();
        Ok(PgConn {
            client,
            cancel,
            _task: TaskGuard(task),
        })
    }

    /// Ask the server to cancel the statement running on `token`'s session
    /// (a separate connection, same TLS settings), bounded by the connect
    /// timeout. Best effort: the server may have finished already.
    pub async fn cancel(&self, token: &CancelToken) -> bool {
        let attempt = async {
            let tcp = self.tcp().await.ok()?;
            match &self.tls {
                Some(tls) => token.cancel_query_raw(tcp, tls.clone()).await.ok(),
                None => token.cancel_query_raw(tcp, NoTls).await.ok(),
            }
        };
        matches!(
            tokio::time::timeout(self.connect_timeout, attempt).await,
            Ok(Some(()))
        )
    }
}

fn startup_error(e: tokio_postgres::Error) -> ConnectError {
    match sqlstate(&e) {
        Some(code) if code.starts_with("28") => {
            ConnectError::Rejected(ErrorCode::PolicyDenied, "postgres_auth_rejected")
        }
        Some("3D000") => {
            ConnectError::Rejected(ErrorCode::InvalidArgument, "postgres_database_missing")
        }
        Some(code) if code.starts_with("42") => ConnectError::Rejected(
            ErrorCode::PolicyDenied,
            "postgres_connect_privilege_rejected",
        ),
        Some(_) => ConnectError::Io("postgres_server_refused_connection"),
        None => ConnectError::Io("postgres_handshake_failed"),
    }
}

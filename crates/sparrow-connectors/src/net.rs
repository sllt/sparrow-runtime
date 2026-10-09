//! Client networking shared by the stream connectors (WebSocket, TCP):
//! rustls provider install, CA bundles, a bounded TCP(+TLS) connect,
//! redacted connect-failure reasons and the capped, jittered reconnect delay.
//!
//! Nothing here formats an address, URL or credential into an error: the
//! reasons are fixed strings prefixed with the connector label.

use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use sparrow_model::{ErrorCode, Result, SparrowError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

pub const MAX_RECONNECT_ATTEMPTS: usize = 1000;
pub const DEFAULT_RECONNECT_ATTEMPTS: usize = 10;
const MAX_CA_PEM_BYTES: usize = 64 * 1024;
const MAX_CA_CERTS: usize = 16;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);

/// Install the process-wide rustls `ring` provider once (idempotent; a
/// provider installed elsewhere first is kept).
pub(crate) fn install_rustls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Built-in web PKI roots, or *only* the given PEM bundle (1..=16 CA
/// certificates within 64 KiB). Verification is never disabled.
pub(crate) fn root_store(pem: Option<&str>, label: &str) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    match pem {
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        Some(pem) => {
            let invalid = || {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "{label} tls_ca_pem must hold 1..=16 PEM CA certificates within 64 KiB"
                    ),
                )
            };
            if pem.len() > MAX_CA_PEM_BYTES {
                return Err(invalid());
            }
            let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| invalid())?;
            if !(1..=MAX_CA_CERTS).contains(&certs.len()) {
                return Err(invalid());
            }
            for cert in certs {
                roots.add(cert).map_err(|_| invalid())?;
            }
        }
    }
    Ok(roots)
}

/// Host, port and transport of one client endpoint. Contains no
/// credentials, so `Debug` is safe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Endpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

/// A pre-built rustls connector plus the verified server name.
#[derive(Clone)]
pub(crate) struct TlsClient {
    connector: tokio_rustls::TlsConnector,
    name: ServerName<'static>,
}

impl TlsClient {
    pub(crate) fn new(host: &str, ca_pem: Option<&str>, label: &str) -> Result<Self> {
        install_rustls_provider();
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store(ca_pem, label)?)
            .with_no_client_auth();
        let name = ServerName::try_from(host.to_string()).map_err(|_| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("{label} TLS server name invalid"),
            )
        })?;
        Ok(Self {
            connector: tokio_rustls::TlsConnector::from(Arc::new(config)),
            name,
        })
    }
}

pub(crate) trait NetIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> NetIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
/// A connected plain or TLS byte stream.
pub(crate) type NetStream = Pin<Box<dyn NetIo>>;

/// Why one connect attempt failed (counted, then retried with backoff).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectFailure {
    Tcp,
    Tls,
    /// Application handshake (e.g. the WebSocket upgrade) failed.
    #[cfg_attr(not(feature = "websocket"), allow(dead_code))]
    Handshake,
    /// The server answered an HTTP upgrade with this status.
    #[cfg_attr(not(feature = "websocket"), allow(dead_code))]
    Rejected(u16),
    Timeout,
}

impl ConnectFailure {
    /// Fixed, redacted reason: never the address, URL or a credential.
    pub(crate) fn reason(self, label: &str) -> String {
        match self {
            Self::Tcp if label == "TCP" => "TCP connect failed".into(),
            Self::Tcp => format!("{label} TCP connect failed"),
            Self::Tls => {
                format!("{label} TLS handshake failed (certificate verification is mandatory)")
            }
            Self::Handshake => {
                format!("{label} upgrade handshake failed (protocol/subprotocol mismatch)")
            }
            Self::Rejected(status) => format!("{label} upgrade rejected with HTTP {status}"),
            Self::Timeout => format!("{label} connect timed out"),
        }
    }
}

/// One TCP connect (+ TLS when `tls` is set), all steps within one
/// `deadline` (a caller's later handshake may share it). `keepalive` enables
/// TCP keepalive probes after that much idle time.
pub(crate) async fn connect_stream(
    endpoint: &Endpoint,
    tls: Option<&TlsClient>,
    deadline: tokio::time::Instant,
    keepalive: Option<Duration>,
) -> std::result::Result<NetStream, ConnectFailure> {
    let tcp = tokio::time::timeout_at(
        deadline,
        TcpStream::connect((endpoint.host.as_str(), endpoint.port)),
    )
    .await
    .map_err(|_| ConnectFailure::Timeout)?
    .map_err(|_| ConnectFailure::Tcp)?;
    let _ = tcp.set_nodelay(true);
    if let Some(idle) = keepalive {
        let probes = socket2::TcpKeepalive::new()
            .with_time(idle)
            .with_interval(idle.min(Duration::from_secs(75)));
        socket2::SockRef::from(&tcp)
            .set_tcp_keepalive(&probes)
            .map_err(|_| ConnectFailure::Tcp)?;
    }
    match tls {
        None => Ok(Box::pin(tcp)),
        Some(tls) => Ok(Box::pin(
            tokio::time::timeout_at(deadline, tls.connector.connect(tls.name.clone(), tcp))
                .await
                .map_err(|_| ConnectFailure::Timeout)?
                .map_err(|_| ConnectFailure::Tls)?,
        )),
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
    let half = u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX) / 2;
    Duration::from_nanos(half + hasher.finish() % (half + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_delay_is_capped_and_jittered() {
        let max = Duration::from_millis(800);
        for attempt in 1..40 {
            let full = Duration::from_millis(100 * (1u64 << (attempt - 1).min(16))).min(max);
            let d = reconnect_delay(attempt, max);
            assert!(d >= full / 2 && d <= full, "attempt {attempt}: {d:?}");
        }
        let spread: std::collections::HashSet<_> =
            (0..32).map(|_| reconnect_delay(6, max)).collect();
        assert!(spread.len() > 1, "jitter must vary the delay");
        // Huge inputs saturate instead of overflowing or panicking.
        let capped = Duration::from_millis(100 << 16);
        for attempt in [usize::MAX, usize::MAX - 1, 1 << 40] {
            let d = reconnect_delay(attempt, Duration::MAX);
            assert!(d >= capped / 2 && d <= capped, "{d:?}");
        }
        assert!(reconnect_delay(1, Duration::ZERO) <= Duration::from_millis(1));
    }

    #[test]
    fn ca_bundle_is_bounded_and_failures_are_redacted() {
        assert!(root_store(None, "TCP").unwrap().len() > 10);
        for bad in [
            "",
            "x",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        ] {
            let e = root_store(Some(bad), "TCP").unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument);
            assert!(e.message.starts_with("TCP tls_ca_pem"), "{e}");
        }
        assert!(root_store(Some(&"A".repeat(MAX_CA_PEM_BYTES + 1)), "TCP").is_err());
        for f in [
            ConnectFailure::Tcp,
            ConnectFailure::Tls,
            ConnectFailure::Timeout,
            ConnectFailure::Handshake,
            ConnectFailure::Rejected(401),
        ] {
            assert!(f.reason("TCP").starts_with("TCP "));
        }
    }
}

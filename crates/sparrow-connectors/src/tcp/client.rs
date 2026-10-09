//! TCP client setup shared by the Source and the Sink: endpoint/allowlist
//! policy, bounds, optional TLS (rustls, private CA), keepalive and framing
//! limits. Errors never echo the host or port beyond the fixed reasons.

use std::time::Duration;

use sparrow_model::{ErrorCode, Result, SparrowError};

use super::framing::{FrameReader, PrefixWidth, TcpFraming};
use crate::net::{
    connect_stream, root_store, ConnectFailure, Endpoint, NetStream, TlsClient,
    DEFAULT_RECONNECT_ATTEMPTS, MAX_RECONNECT_ATTEMPTS,
};
use crate::TargetPolicy;

pub const MIN_FRAME_BYTES: usize = 16;
/// One frame is one record; records stay within the 64 KiB decode limit.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;
/// Ledger estimate of per-connection fixed state (socket, TLS records, task).
const CONNECTION_FIXED_RESERVATION: usize = 64 * 1024;

pub(crate) fn error(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message.into())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpClientConfig {
    /// DNS name or IP literal (IPv6 without brackets).
    pub host: String,
    pub port: u16,
    /// TLS (rustls) with mandatory verification; `tls_ca_pem` replaces the
    /// built-in web PKI roots.
    pub tls: bool,
    pub tls_ca_pem: Option<String>,
    /// TCP connect and TLS handshake together, bounded by this.
    pub connect_timeout: Duration,
    /// TCP keepalive idle time; `None` = off.
    pub keepalive: Option<Duration>,
    /// Ceiling of the exponential reconnect delay (from 100 ms, jittered).
    pub reconnect_max: Duration,
    /// Consecutive failed connects per outage before giving up.
    pub reconnect_attempts: usize,
    pub framing: TcpFraming,
    pub prefix_width: PrefixWidth,
    /// Largest record (line without terminator / frame payload).
    pub max_frame_bytes: usize,
}

impl TcpClientConfig {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            tls: false,
            tls_ca_pem: None,
            connect_timeout: Duration::from_secs(5),
            keepalive: None,
            reconnect_max: Duration::from_secs(5),
            reconnect_attempts: DEFAULT_RECONNECT_ATTEMPTS,
            framing: TcpFraming::Lines,
            prefix_width: PrefixWidth::U32,
            max_frame_bytes: MAX_FRAME_BYTES,
        }
    }

    /// Static bounds, endpoint syntax and the allowlist.
    pub fn validate(&self, policy: &TargetPolicy) -> Result<()> {
        let ms = |d: Duration| d.as_millis();
        if !(100..=60_000).contains(&ms(self.connect_timeout))
            || self
                .keepalive
                .is_some_and(|k| !(1_000..=7_200_000).contains(&ms(k)))
            || !(100..=300_000).contains(&ms(self.reconnect_max))
            || !(1..=MAX_RECONNECT_ATTEMPTS).contains(&self.reconnect_attempts)
            || !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&self.max_frame_bytes)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP bounds: connect_timeout_ms 100..=60000, keepalive_ms 1000..=7200000, reconnect_max_ms 100..=300000, reconnect_attempts 1..=1000, max_frame_bytes 16..=65536",
            ));
        }
        if self.framing == TcpFraming::LengthPrefixed
            && self.max_frame_bytes > self.prefix_width.max_len()
        {
            // Refused, never clamped: the configured limit must be one the
            // prefix can express.
            return Err(error(
                ErrorCode::BoundExceeded,
                "TCP max_frame_bytes exceeds what length_bytes can express (length_bytes 2: <=65535)",
            ));
        }
        let valid_host = !self.host.is_empty()
            && self.host.len() <= 253
            && (self.host.parse::<std::net::IpAddr>().is_ok()
                || self
                    .host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'));
        if !valid_host || self.port == 0 {
            return Err(error(
                ErrorCode::InvalidArgument,
                "TCP host must be a DNS name or IP literal (no brackets, scheme or path) and port 1..=65535",
            ));
        }
        if self.tls_ca_pem.is_some() && !self.tls {
            return Err(error(
                ErrorCode::InvalidArgument,
                "TCP tls_ca_pem requires tls: true",
            ));
        }
        policy
            .check_host_port(&self.host, self.port)
            .map_err(|e| error(e.code(), "TCP endpoint is not on the allowlist"))?;
        if let Some(pem) = &self.tls_ca_pem {
            root_store(Some(pem), "TCP")?;
        }
        Ok(())
    }

    /// Record limit: `max_frame_bytes` (validate refuses a value the prefix
    /// cannot express; nothing is clamped).
    pub fn frame_limit(&self) -> usize {
        self.max_frame_bytes
    }

    /// Largest wire frame: a record plus its prefix or `\r\n` terminator.
    pub fn frame_bytes(&self) -> usize {
        self.max_frame_bytes
            .saturating_add(self.prefix_width.bytes().max(2))
    }

    /// Per-connection ledger charge: fixed state, the fixed read buffer and
    /// one frame being written. Saturating.
    pub fn connection_reservation(&self) -> usize {
        CONNECTION_FIXED_RESERVATION
            .saturating_add(FrameReader::capacity(self.frame_limit(), self.prefix_width))
            .saturating_add(self.frame_bytes())
    }

    /// Validate and pre-build the TLS settings.
    pub fn bind(&self, policy: &TargetPolicy) -> Result<BoundTcp> {
        self.validate(policy)?;
        let tls = if self.tls {
            Some(TlsClient::new(
                &self.host,
                self.tls_ca_pem.as_deref(),
                "TCP",
            )?)
        } else {
            None
        };
        Ok(BoundTcp {
            config: self.clone(),
            endpoint: Endpoint {
                host: self.host.clone(),
                port: self.port,
                tls: self.tls,
            },
            tls,
        })
    }
}

/// A validated client with TLS settings built once.
#[derive(Clone)]
pub struct BoundTcp {
    pub(crate) config: TcpClientConfig,
    endpoint: Endpoint,
    tls: Option<TlsClient>,
}

impl std::fmt::Debug for BoundTcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundTcp")
            .field("endpoint", &self.endpoint)
            .field("framing", &self.config.framing)
            .finish_non_exhaustive()
    }
}

impl BoundTcp {
    /// One connect attempt (TCP, then TLS when configured) within
    /// `connect_timeout` in total.
    pub(crate) async fn connect(&self) -> std::result::Result<NetStream, ConnectFailure> {
        connect_stream(
            &self.endpoint,
            self.tls.as_ref(),
            tokio::time::Instant::now() + self.config.connect_timeout,
            self.config.keepalive,
        )
        .await
    }

    pub(crate) fn reader(&self, oversize: super::framing::OversizePolicy) -> FrameReader {
        FrameReader::new(
            self.config.framing,
            self.config.prefix_width,
            self.config.frame_limit(),
            oversize,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_client_endpoint_policy_and_bounds() {
        let p = TargetPolicy::allow("127.0.0.1", 9000).with_allow("localhost", 9443);
        let ok = TcpClientConfig::new("127.0.0.1", 9000);
        ok.validate(&p).unwrap();
        let mut tls = TcpClientConfig::new("localhost", 9443);
        tls.tls = true;
        tls.bind(&p).unwrap();
        type M = fn(&mut TcpClientConfig);
        let cases: [(M, ErrorCode); 12] = [
            (|c| c.host = "".into(), ErrorCode::InvalidArgument),
            (
                |c| c.host = "tcp://127.0.0.1".into(),
                ErrorCode::InvalidArgument,
            ),
            (|c| c.host = "[::1]".into(), ErrorCode::InvalidArgument),
            (|c| c.port = 0, ErrorCode::InvalidArgument),
            (|c| c.port = 9001, ErrorCode::PolicyDenied),
            (
                |c| c.host = "169.254.169.254".into(),
                ErrorCode::PolicyDenied,
            ),
            (
                |c| c.tls_ca_pem = Some("x".into()),
                ErrorCode::InvalidArgument,
            ),
            (|c| c.max_frame_bytes = 8, ErrorCode::BoundExceeded),
            (|c| c.max_frame_bytes = 1 << 20, ErrorCode::BoundExceeded),
            (|c| c.reconnect_attempts = 0, ErrorCode::BoundExceeded),
            (
                |c| c.keepalive = Some(Duration::from_millis(10)),
                ErrorCode::BoundExceeded,
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
            assert!(!e.message.contains("169.254"), "{e}");
        }
        let mut ca = tls.clone();
        ca.tls_ca_pem =
            Some("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".into());
        assert_eq!(
            ca.validate(&p).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        ca.tls_ca_pem = Some(String::from_utf8(super::super::tls_fixture::CA.to_vec()).unwrap());
        ca.validate(&p).unwrap();
        // length_bytes 2 with a limit above 65535 is refused, not clamped.
        let mut narrow = ok.clone();
        narrow.framing = TcpFraming::LengthPrefixed;
        narrow.prefix_width = PrefixWidth::U16;
        narrow.max_frame_bytes = 65536;
        let e = narrow.validate(&p).unwrap_err();
        assert_eq!(e.code, ErrorCode::BoundExceeded);
        assert!(e.message.contains("length_bytes"), "{e}");
        narrow.max_frame_bytes = 65535;
        narrow.validate(&p).unwrap();
        assert_eq!(narrow.frame_limit(), 65535);
        // The same limit is fine for 4-byte prefixes and lines.
        narrow.max_frame_bytes = 65536;
        narrow.prefix_width = PrefixWidth::U32;
        narrow.validate(&p).unwrap();
        narrow.framing = TcpFraming::Lines;
        narrow.prefix_width = PrefixWidth::U16;
        narrow.validate(&p).unwrap();
    }

    #[test]
    fn tcp_reservation_math_saturates_on_huge_values() {
        let p = TargetPolicy::allow("127.0.0.1", 9000);
        let mut huge = TcpClientConfig::new("127.0.0.1", 9000);
        huge.max_frame_bytes = usize::MAX;
        for framing in [TcpFraming::Lines, TcpFraming::LengthPrefixed] {
            huge.framing = framing;
            assert_eq!(huge.frame_bytes(), usize::MAX);
            assert_eq!(huge.connection_reservation(), usize::MAX);
            assert_eq!(
                huge.validate(&p).unwrap_err().code,
                ErrorCode::BoundExceeded
            );
        }
        for width in [PrefixWidth::U16, PrefixWidth::U32] {
            assert_eq!(FrameReader::capacity(usize::MAX, width), usize::MAX);
            assert_eq!(FrameReader::capacity(usize::MAX - 1, width), usize::MAX);
        }
    }
}

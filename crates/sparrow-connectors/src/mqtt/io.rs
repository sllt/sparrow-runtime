use std::pin::Pin;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use rustls::pki_types::ServerName;
use sparrow_model::ErrorCode;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use super::codec::{decode, decode_remaining_length, encode, Packet, MAX_PACKET_BYTES};
use crate::error::{ConnectorError, Result};
use crate::tls::TlsConfig;

fn install_rustls_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

pub type MqttStream = Pin<Box<dyn MqttIo>>;

pub trait MqttIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> MqttIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub async fn connect_plain(
    host: &str,
    port: u16,
    tls: &TlsConfig,
    wait: Duration,
) -> Result<MqttStream> {
    connect_with_quickack(host, port, tls, wait, false, None).await
}

pub async fn connect_with_quickack(
    host: &str,
    port: u16,
    tls: &TlsConfig,
    wait: Duration,
    quickack: bool,
    diag: Option<Arc<crate::diag::IoDiagnostics>>,
) -> Result<MqttStream> {
    tls.validate()?;
    let addr = format!("{host}:{port}");
    let stream = timeout(wait, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            ConnectorError::new(
                ErrorCode::Internal,
                format!("MQTT connect timeout to {addr}"),
            )
        })?
        .map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("MQTT connect {addr}: {e}"))
        })?;
    let _ = stream.set_nodelay(true);
    #[cfg(target_os = "linux")]
    let stream: MqttStream = if quickack {
        Box::pin(QuickAckStream {
            inner: stream,
            enabled: true,
            diag,
        })
    } else {
        Box::pin(stream)
    };
    #[cfg(not(target_os = "linux"))]
    let stream: MqttStream = {
        let _ = (quickack, diag);
        Box::pin(stream)
    };
    if !tls.enabled {
        return Ok(stream);
    }
    install_rustls_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(host.to_owned()).map_err(|e| {
        ConnectorError::new(
            ErrorCode::InvalidArgument,
            format!("MQTT TLS server name `{host}`: {e}"),
        )
    })?;
    let tls_stream = timeout(wait, connector.connect(server_name, stream))
        .await
        .map_err(|_| {
            ConnectorError::new(
                ErrorCode::Internal,
                format!("MQTT TLS handshake timeout to {addr}"),
            )
        })?
        .map_err(|e| {
            ConnectorError::new(
                ErrorCode::Internal,
                format!("MQTT TLS handshake {addr}: {e}"),
            )
        })?;
    Ok(Box::pin(tls_stream))
}

// Wrap the TCP transport *below* rustls. Rearm after real, nonempty socket
// reads, not per MQTT packet or TLS plaintext read. No saved raw fd/unsafe FFI.
#[cfg(target_os = "linux")]
struct QuickAckStream {
    inner: TcpStream,
    enabled: bool,
    diag: Option<Arc<crate::diag::IoDiagnostics>>,
}

#[cfg(target_os = "linux")]
impl AsyncRead for QuickAckStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if self.enabled
            && matches!(&result, std::task::Poll::Ready(Ok(())))
            && buf.filled().len() > before
        {
            if let Some(diag) = &self.diag {
                diag.mqtt_quickack_calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if socket2::SockRef::from(&self.inner)
                .set_tcp_quickack(true)
                .is_err()
            {
                if let Some(diag) = &self.diag {
                    diag.mqtt_quickack_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                self.enabled = false; // optional tuning failure must not drop data or retry forever
            }
        }
        result
    }
}

#[cfg(target_os = "linux")]
impl AsyncWrite for QuickAckStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub async fn write_packet(stream: &mut MqttStream, packet: &Packet) -> Result<()> {
    let bytes = encode(packet)?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT write: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT flush: {e}")))
}

/// Cancel-safe framed reader. Bytes already pulled from the socket stay in
/// `buf` when a `select!` branch (ping / cancel) wins (R19).
#[derive(Default)]
pub struct MqttFramedReader {
    buf: Vec<u8>,
}

impl MqttFramedReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn next(&mut self, stream: &mut MqttStream) -> Result<Packet> {
        loop {
            if let Some(pkt) = try_decode_frame(&mut self.buf)? {
                return Ok(pkt);
            }
            let mut tmp = [0u8; 1024];
            let n = stream
                .read(&mut tmp)
                .await
                .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT read: {e}")))?;
            if n == 0 {
                return Err(ConnectorError::new(
                    ErrorCode::Internal,
                    "MQTT connection closed",
                ));
            }
            self.buf.extend_from_slice(&tmp[..n]);
            if self.buf.len() > MAX_PACKET_BYTES + 8 {
                return Err(ConnectorError::new(
                    ErrorCode::MaxRecordSize,
                    "MQTT framed buffer exceeded",
                ));
            }
        }
    }
}

fn try_decode_frame(buf: &mut Vec<u8>) -> Result<Option<Packet>> {
    if buf.is_empty() {
        return Ok(None);
    }
    let first = buf[0];
    let mut used = 1usize;
    let mut len_buf = Vec::new();
    loop {
        if used >= buf.len() {
            return Ok(None);
        }
        let b = buf[used];
        used += 1;
        len_buf.push(b);
        if b & 0x80 == 0 {
            break;
        }
        if len_buf.len() > 4 {
            return Err(ConnectorError::new(
                ErrorCode::CodecViolation,
                "MQTT remaining length too long",
            ));
        }
    }
    let (len, _) = decode_remaining_length(&len_buf)?;
    if len > MAX_PACKET_BYTES {
        return Err(ConnectorError::new(
            ErrorCode::MaxRecordSize,
            format!("MQTT packet {len}B exceeds {MAX_PACKET_BYTES}"),
        ));
    }
    if buf.len() < used + len {
        return Ok(None);
    }
    let payload = buf[used..used + len].to_vec();
    let pkt = decode(first, &payload)?;
    buf.drain(..used + len);
    Ok(Some(pkt))
}

/// Read the next packet using a **persistent** framed reader.
///
/// Creating a new `MqttFramedReader` per call drops bytes already pulled from
/// the socket when TCP coalesces multiple MQTT packets (R19).
pub async fn read_packet(reader: &mut MqttFramedReader, stream: &mut MqttStream) -> Result<Packet> {
    reader.next(stream).await
}

#[cfg(test)]
#[path = "tls_fixture.rs"]
mod tls_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::codec::encode;
    use crate::mqtt::codec::Packet;

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn quickack_transport_under_tls_preserves_verification_and_payload() {
        use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
        install_rustls_provider();
        let cert = CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap();
        let key = PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap();
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(tls_fixture::CA).unwrap())
            .unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = tokio_rustls::TlsAcceptor::from(Arc::new(server))
                .accept(socket)
                .await
                .unwrap();
            tls.write_all(b"verified tls payload").await.unwrap();
            tls.shutdown().await.unwrap();
        });
        let diag = crate::diag::IoDiagnostics::new();
        let socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let transport = QuickAckStream {
            inner: socket,
            enabled: true,
            diag: Some(diag.clone()),
        };
        let mut tls = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), transport)
            .await
            .unwrap();
        let mut data = Vec::new();
        tls.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, b"verified tls payload");
        broker.await.unwrap();
        assert!(
            diag.mqtt_quickack_calls
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );
        assert_eq!(
            diag.mqtt_quickack_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn quickack_rearms_only_after_nonempty_tcp_reads() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let diag = crate::diag::IoDiagnostics::new();
        let tls_config = TlsConfig::disabled();
        let connect = connect_with_quickack(
            "127.0.0.1",
            port,
            &tls_config,
            Duration::from_secs(1),
            true,
            Some(diag.clone()),
        );
        let (stream, accepted) = tokio::join!(connect, listener.accept());
        let mut stream = stream.unwrap();
        let (mut peer, _) = accepted.unwrap();
        for b in [b'x', b'y'] {
            peer.write_all(&[b]).await.unwrap();
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte[0], b);
        }
        peer.shutdown().await.unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(
            diag.mqtt_quickack_calls
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            diag.mqtt_quickack_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn r19_partial_frame_is_retained() {
        let pkt = Packet::PingResp;
        let bytes = encode(&pkt).unwrap();
        let mut buf = bytes[..1].to_vec();
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 1, "half-frame must stay in the reader buffer");
        buf.extend_from_slice(&bytes[1..]);
        let got = try_decode_frame(&mut buf).unwrap().unwrap();
        assert!(matches!(got, Packet::PingResp));
        assert!(buf.is_empty());
    }
}

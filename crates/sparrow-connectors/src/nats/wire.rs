//! Deliberately narrow NATS Core source transport. One static subscription,
//! no discovery, headers or request/reply. INFO is gated before every SUB;
//! Ready requires the PONG following CONNECT + SUB + PING on this socket.

use super::{
    client::{reconnect_delay, BoundToken, NatsClientConfig},
    common::{self, error},
};
use crate::diag::IoDiagnostics;
use sparrow_model::observation::HealthState;
use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result};
use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

const MAX_LINE: usize = 16 * 1024;
const PING_INTERVAL: Duration = Duration::from_secs(2);
const PONG_TIMEOUT: Duration = Duration::from_secs(4);

trait WireIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> WireIo for T {}
type Stream = Pin<Box<dyn WireIo>>;

pub(crate) struct Message {
    // None is a wire-valid message larger than the JSON record bound. It is
    // streamed past using fixed scratch, never allocated as a whole frame.
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) received_at: Instant,
    _reservation: Arc<MemoryLease>,
}

pub(crate) struct Wire {
    pub(crate) messages: mpsc::Receiver<Message>,
    task: tokio::task::JoinHandle<Result<()>>,
    stop: CancellationToken,
    _reservation: Arc<MemoryLease>,
}

impl Wire {
    pub(crate) fn start(
        config: NatsClientConfig,
        token: Option<BoundToken>,
        subject: String,
        group: Option<String>,
        record_limit: usize,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
        stop: CancellationToken,
    ) -> Result<Self> {
        let reservation =
            Arc::new(owner.acquire(CreditKind::Reservation, config.sdk_reservation())?);
        let (tx, messages) = mpsc::channel(config.capacity);
        let actor = Actor {
            config,
            token,
            subject,
            group,
            record_limit,
            owner,
            diag,
            tx,
            reservation: reservation.clone(),
        };
        let child = stop.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = child.cancelled() => Ok(()),
                _ = actor.tx.closed() => Ok(()),
                r = actor.run(&child) => r,
            };
            // Interrupt admission too, so a failed actor cannot leave its pump
            // waiting indefinitely for a full ingress to become writable.
            child.cancel();
            result
        });
        Ok(Self {
            messages,
            task,
            stop,
            _reservation: reservation,
        })
    }

    pub(crate) async fn close(mut self) -> Result<()> {
        self.stop.cancel();
        self.messages.close();
        // Keep both the receiving buffer and its charge until the actor exits.
        (&mut self.task)
            .await
            .map_err(|_| error(ErrorCode::JobFailed, "NATS wire task panicked"))?
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

struct Actor {
    config: NatsClientConfig,
    token: Option<BoundToken>,
    subject: String,
    group: Option<String>,
    record_limit: usize,
    owner: Arc<MemoryOwner>,
    diag: Arc<IoDiagnostics>,
    tx: mpsc::Sender<Message>,
    reservation: Arc<MemoryLease>,
}

impl Actor {
    async fn run(&self, stop: &CancellationToken) -> Result<()> {
        let mut ever_ready = false;
        let mut attempts = 0usize;
        let mut initial_failures = 0usize;
        let mut server = 0usize;
        loop {
            if attempts > 0 {
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(reconnect_delay(attempts)) => {}
                }
            }
            let mut ready = false;
            let result = self
                .session(&self.config.servers[server], ever_ready, &mut ready)
                .await;
            server = (server + 1) % self.config.servers.len();
            let Err(e) = result else { return Ok(()) };
            if !e.retryable {
                return Err(e);
            }
            self.diag
                .nats_source_client_errors
                .fetch_add(1, Ordering::Relaxed);
            if ready {
                ever_ready = true;
                attempts = 0;
                self.diag
                    .nats_source_disconnects
                    .fetch_add(1, Ordering::Relaxed);
                self.diag.observation.health(
                    true,
                    HealthState::Reconnecting,
                    "nats_disconnected_bounded_reconnect",
                    None,
                );
            }
            if !ever_ready {
                initial_failures += 1;
                if initial_failures >= self.config.servers.len() {
                    return Err(e);
                }
            } else {
                attempts += 1;
                if attempts > self.config.reconnect_attempts {
                    return Err(error(
                        ErrorCode::JobFailed,
                        "NATS connection closed after bounded reconnect attempts",
                    )
                    .retryable(true));
                }
            }
        }
    }

    fn info(&self, bytes: &[u8]) -> Result<(bool, usize)> {
        // serde_json's intermediate object/strings are admitted before parse.
        let _scratch = self.owner.acquire(
            CreditKind::Reservation,
            bytes.len().saturating_mul(64).saturating_add(4096),
        )?;
        let value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|_| error(ErrorCode::CodecViolation, "invalid NATS INFO"))?;
        let max = value
            .get("max_payload")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| error(ErrorCode::CodecViolation, "NATS INFO max_payload required"))?;
        if max > self.config.max_payload_bytes {
            return Err(error(
                ErrorCode::BoundExceeded,
                "NATS server max_payload exceeds source max_payload_bytes",
            ));
        }
        Ok((
            value
                .get("tls_required")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            max,
        ))
    }

    async fn session(&self, server: &str, reconnect: bool, ready: &mut bool) -> Result<()> {
        let endpoint = url::Url::parse(server)
            .map_err(|_| error(ErrorCode::InvalidArgument, "invalid NATS server URL"))?;
        let host = endpoint
            .host_str()
            .ok_or_else(|| error(ErrorCode::InvalidArgument, "NATS host required"))?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let port = endpoint.port().unwrap_or(common::DEFAULT_PORT);
        let mut reader = Reader::new(self.config.max_payload_bytes, self.record_limit);
        let opening = async {
            let tcp = tokio::net::TcpStream::connect((host, port))
                .await
                .map_err(|_| io_error("NATS connect failed"))?;
            tcp.set_nodelay(true)
                .map_err(|_| io_error("NATS socket setup failed"))?;
            let mut stream: Stream = Box::pin(tcp);
            let Frame::Info(info) = reader.next(&mut stream).await? else {
                return Err(error(
                    ErrorCode::CodecViolation,
                    "NATS first frame must be INFO",
                ));
            };
            let (requires_tls, max_payload) = self.info(&info)?;
            reader.max_payload = max_payload;
            let tls_required = requires_tls || endpoint.scheme() == "tls";
            if tls_required {
                stream = upgrade_tls(stream, host).await?;
            }
            let mut connect = serde_json::json!({"verbose":false,"pedantic":false,"protocol":1,
                "lang":"rust","version":"sparrow","echo":true,"headers":false,"tls_required":tls_required});
            if let Some(token) = &self.token {
                connect["auth_token"] = token.value().into();
            }
            let sub = match &self.group {
                Some(group) => format!("SUB {} {} 1\r\n", self.subject, group),
                None => format!("SUB {} 1\r\n", self.subject),
            };
            let command = format!("CONNECT {connect}\r\n{sub}PING\r\n");
            stream
                .write_all(command.as_bytes())
                .await
                .map_err(|_| io_error("NATS subscribe write failed"))?;
            reader.messages_allowed = true;
            // No other client PING precedes this one, so the first PONG is the
            // ordered server barrier for CONNECT and this exact subscription.
            loop {
                match reader.next(&mut stream).await? {
                    Frame::Pong => break,
                    frame => self.handle(frame, &mut stream, &mut reader).await?,
                }
            }
            Ok::<_, sparrow_model::SparrowError>(stream)
        };
        let mut stream = tokio::time::timeout(self.config.connect_timeout, opening)
            .await
            .map_err(|_| io_error("NATS connect/subscription barrier timed out"))??;
        *ready = true;
        if reconnect {
            self.diag
                .nats_source_reconnects
                .fetch_add(1, Ordering::Relaxed);
        }
        self.diag
            .observation
            .health(true, HealthState::Ready, "nats_subscribed", None);
        let mut heartbeat = tokio::time::interval(PING_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        let mut pong_due = None;
        loop {
            tokio::select! {
                biased;
                _ = async { match pong_due { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                    return Err(io_error("NATS PONG timed out"));
                }
                _ = heartbeat.tick() => {
                    if pong_due.is_none() {
                        self.write(&mut stream, b"PING\r\n").await?;
                        pong_due = Some(tokio::time::Instant::now() + PONG_TIMEOUT);
                    }
                }
                frame = reader.next(&mut stream) => {
                    match frame? {
                        Frame::Pong => pong_due = None,
                        frame => self.handle(frame, &mut stream, &mut reader).await?,
                    }
                }
            }
        }
    }

    async fn write(&self, stream: &mut Stream, bytes: &[u8]) -> Result<()> {
        tokio::time::timeout(self.config.connect_timeout, stream.write_all(bytes))
            .await
            .map_err(|_| io_error("NATS control write timed out"))?
            .map_err(|_| io_error("NATS control write failed"))
    }

    async fn handle(&self, frame: Frame, stream: &mut Stream, reader: &mut Reader) -> Result<()> {
        match frame {
            Frame::Ping => self.write(stream, b"PONG\r\n").await?,
            Frame::Info(bytes) => {
                reader.max_payload = self.info(&bytes)?.1;
            }
            Frame::Message(payload) => {
                let message = Message {
                    payload,
                    received_at: Instant::now(),
                    _reservation: self.reservation.clone(),
                };
                match self.tx.try_send(message) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.diag
                            .nats_source_slow_consumer
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                }
                // Share the executor fairly with the admission pump without
                // ever waiting for a prefetch slot or stopping heartbeat I/O.
                tokio::task::yield_now().await;
            }
            Frame::Pong | Frame::Ok => {}
        }
        Ok(())
    }
}

fn io_error(message: &str) -> sparrow_model::SparrowError {
    error(ErrorCode::JobFailed, message).retryable(true)
}

async fn upgrade_tls(stream: Stream, host: &str) -> Result<Stream> {
    // NATS upgrades the existing TCP socket AFTER its plaintext INFO. Unlike
    // MQTT's helper, this must not initiate TLS immediately upon TCP connect.
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| io_error("NATS TLS configuration failed"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|_| error(ErrorCode::InvalidArgument, "invalid NATS TLS server name"))?;
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, stream)
        .await
        .map_err(|_| io_error("NATS TLS handshake failed"))?;
    Ok(Box::pin(tls))
}

#[derive(Debug)]
enum Frame {
    Info(Vec<u8>),
    Ping,
    Pong,
    Ok,
    Message(Option<Vec<u8>>),
}
struct Body {
    remaining: usize,
    bytes: Option<Vec<u8>>,
    trailer: [u8; 2],
    trailer_used: usize,
}

// Persistent partial-line/body state: cancelling next() for a heartbeat does
// not discard bytes already read. No unbounded read_until/read_to_end buffer.
struct Reader {
    line: Vec<u8>,
    body: Option<Body>,
    max_payload: usize,
    record_limit: usize,
    messages_allowed: bool,
}
impl Reader {
    fn new(max_payload: usize, record_limit: usize) -> Self {
        Self {
            line: Vec::with_capacity(MAX_LINE),
            body: None,
            max_payload,
            record_limit,
            messages_allowed: false,
        }
    }
    async fn next(&mut self, stream: &mut Stream) -> Result<Frame> {
        loop {
            if let Some(body) = &mut self.body {
                if body.remaining > 0 {
                    let mut scratch = [0u8; 1024];
                    let cap = body.remaining.min(scratch.len());
                    let n = stream
                        .read(&mut scratch[..cap])
                        .await
                        .map_err(|_| io_error("NATS frame read failed"))?;
                    if n == 0 {
                        return Err(io_error("NATS connection closed"));
                    }
                    if let Some(bytes) = &mut body.bytes {
                        bytes.extend_from_slice(&scratch[..n]);
                    }
                    body.remaining -= n;
                    continue;
                }
                if body.trailer_used < 2 {
                    let n = stream
                        .read(&mut body.trailer[body.trailer_used..])
                        .await
                        .map_err(|_| io_error("NATS frame trailer failed"))?;
                    if n == 0 {
                        return Err(io_error("NATS connection closed"));
                    }
                    body.trailer_used += n;
                    continue;
                }
                if body.trailer != *b"\r\n" {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "invalid NATS frame trailer",
                    ));
                }
                return Ok(Frame::Message(self.body.take().expect("body").bytes));
            }
            let mut byte = [0u8; 1];
            let n = stream
                .read(&mut byte)
                .await
                .map_err(|_| io_error("NATS control read failed"))?;
            if n == 0 {
                return Err(io_error("NATS connection closed"));
            }
            if self.line.len() == MAX_LINE {
                return Err(error(
                    ErrorCode::BoundExceeded,
                    "NATS control line exceeds bound",
                ));
            }
            self.line.push(byte[0]);
            if !self.line.ends_with(b"\r\n") {
                continue;
            }
            let text = std::str::from_utf8(&self.line[..self.line.len() - 2])
                .map_err(|_| error(ErrorCode::CodecViolation, "invalid NATS control encoding"))?;
            let frame = if let Some(info) = text.strip_prefix("INFO ") {
                Some(Frame::Info(info.as_bytes().to_vec()))
            } else if text == "PING" {
                Some(Frame::Ping)
            } else if text == "PONG" {
                Some(Frame::Pong)
            } else if text == "+OK" {
                Some(Frame::Ok)
            } else if text.starts_with("-ERR") {
                return Err(io_error("NATS server refused operation"));
            } else if text.starts_with("HMSG ") {
                return Err(error(
                    ErrorCode::CodecViolation,
                    "NATS headers were not negotiated",
                ));
            } else if let Some(args) = text.strip_prefix("MSG ") {
                if !self.messages_allowed {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "NATS MSG before gated subscription",
                    ));
                }
                let mut fields = args.split_ascii_whitespace();
                let subject = fields
                    .next()
                    .ok_or_else(|| error(ErrorCode::CodecViolation, "invalid NATS MSG"))?;
                common::check_subject(subject, false)?;
                if fields.next() != Some("1") {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "invalid NATS subscription id",
                    ));
                }
                let third = fields
                    .next()
                    .ok_or_else(|| error(ErrorCode::CodecViolation, "invalid NATS MSG length"))?;
                let length = match fields.next() {
                    Some(length) => {
                        common::check_subject(third, false)?;
                        length
                    }
                    None => third,
                };
                if fields.next().is_some() {
                    return Err(error(ErrorCode::CodecViolation, "invalid NATS MSG arity"));
                }
                if length.is_empty() || !length.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(error(ErrorCode::CodecViolation, "invalid NATS MSG length"));
                }
                let length = length
                    .parse::<usize>()
                    .map_err(|_| error(ErrorCode::CodecViolation, "invalid NATS MSG length"))?;
                if length > self.max_payload {
                    return Err(error(
                        ErrorCode::BoundExceeded,
                        "NATS frame exceeds payload bound",
                    ));
                }
                self.body = Some(Body {
                    remaining: length,
                    bytes: (length <= self.record_limit).then(|| Vec::with_capacity(length)),
                    trailer: [0; 2],
                    trailer_used: 0,
                });
                None
            } else {
                return Err(error(
                    ErrorCode::CodecViolation,
                    "unsupported NATS control frame",
                ));
            };
            self.line.clear();
            if let Some(frame) = frame {
                return Ok(frame);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wire_rejects_overlong_headers_lengths_and_invalid_frames_before_growth() {
        let (mut peer, io) = tokio::io::duplex(128);
        peer.write_all(b"MSG in.a 1 65536\r\n").await.unwrap();
        let mut stream: Stream = Box::pin(io);
        let mut reader = Reader::new(65536, 65536);
        assert_eq!(
            reader.next(&mut stream).await.unwrap_err().code,
            ErrorCode::CodecViolation
        );
        assert!(
            reader.body.is_none(),
            "no message allocation before gated SUB"
        );
        for (input, code) in [
            (vec![b'x'; MAX_LINE + 1], ErrorCode::BoundExceeded),
            (b"MSG in.a 1 65537\r\n".to_vec(), ErrorCode::BoundExceeded),
            (
                format!("MSG in.a 1 {}\r\n", usize::MAX).into_bytes(),
                ErrorCode::BoundExceeded,
            ),
            (
                b"HMSG in.a 1 60000 60030\r\n".to_vec(),
                ErrorCode::CodecViolation,
            ),
            (b"MSG in.a 2 1\r\n".to_vec(), ErrorCode::CodecViolation),
            (b"MSG in.a 1 -1\r\n".to_vec(), ErrorCode::CodecViolation),
            (b"MSG in.a 1 1\r\nxXX".to_vec(), ErrorCode::CodecViolation),
        ] {
            let (mut peer, io) = tokio::io::duplex(MAX_LINE + 1024);
            peer.write_all(&input).await.unwrap();
            let mut stream: Stream = Box::pin(io);
            let mut reader = Reader::new(65536, 65536);
            reader.messages_allowed = true;
            assert_eq!(reader.next(&mut stream).await.unwrap_err().code, code);
            assert!(reader.line.capacity() <= MAX_LINE);
            assert!(reader.body.as_ref().is_none_or(|body| body
                .bytes
                .as_ref()
                .is_none_or(|bytes| bytes.capacity() <= 65536)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wire_fragmented_body_survives_cancelled_read_and_oversize_is_streamed() {
        let (mut peer, io) = tokio::io::duplex(4096);
        let mut stream: Stream = Box::pin(io);
        let mut reader = Reader::new(1024, 32);
        reader.messages_allowed = true;
        peer.write_all(b"MSG in.a 1 7\r\n{\"v\":").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), reader.next(&mut stream))
                .await
                .is_err()
        );
        assert_eq!(reader.body.as_ref().unwrap().remaining, 2);
        peer.write_all(b"1}\r\n").await.unwrap();
        let Frame::Message(Some(body)) = reader.next(&mut stream).await.unwrap() else {
            panic!("message expected")
        };
        assert_eq!(body.as_slice(), br#"{"v":1}"#);
        peer.write_all(b"MSG in.a 1 1024\r\n").await.unwrap();
        peer.write_all(&vec![b' '; 1024]).await.unwrap();
        peer.write_all(b"\r\n").await.unwrap();
        assert!(matches!(
            reader.next(&mut stream).await.unwrap(),
            Frame::Message(None)
        ));
    }

    #[tokio::test]
    async fn wire_message_keeps_credit_after_actor_and_receiver_exit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket
                .write_all(b"INFO {\"max_payload\":65536}\r\n")
                .await
                .unwrap();
            for _ in 0..3 {
                let mut line = Vec::new();
                while !line.ends_with(b"\r\n") {
                    let mut byte = [0; 1];
                    socket.read_exact(&mut byte).await.unwrap();
                    line.push(byte[0]);
                    assert!(line.len() < 32 * 1024);
                }
            }
            socket
                .write_all(b"PONG\r\nMSG in.a 1 2\r\n{}\r\n")
                .await
                .unwrap();
            done_rx.await.unwrap();
        });
        let mut config = NatsClientConfig::new(vec![format!("nats://127.0.0.1:{port}")]);
        config.capacity = 1;
        let expected = config.sdk_reservation();
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let mut wire = Wire::start(
            config,
            None,
            "in.>".into(),
            None,
            1024,
            owner.clone(),
            IoDiagnostics::new(),
            CancellationToken::new(),
        )
        .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(2), wire.messages.recv())
            .await
            .unwrap()
            .unwrap();
        wire.close().await.unwrap();
        assert_eq!(
            owner.usage().reservation_bytes,
            expected,
            "in-flight frame still owns the shared credit"
        );
        drop(message);
        assert_eq!(owner.usage().reservation_bytes, 0);
        done_tx.send(()).unwrap();
        peer.await.unwrap();
    }
}

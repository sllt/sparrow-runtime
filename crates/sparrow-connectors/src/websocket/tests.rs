//! WebSocket Source/Sink tests against in-process tokio-tungstenite servers
//! (plain and rustls `wss://` with the repository's localhost test CA).
// tungstenite's server handshake `Callback` returns `Result<Response, ErrorResponse>`.
#![allow(clippy::result_large_err)]

use super::*;
use crate::{IoDiagnostics, MapSecretResolver, TargetPolicy};
use futures_util::{SinkExt, StreamExt};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, QueuedRow, ResourceBudget,
    RestoreClaim, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

/// A finite, immediately readable frame burst and independently gated writes.
/// No socket buffer sizes or random network scheduling are needed for the
/// fairness, duplex and cancellation regressions.
pub(super) struct Probe {
    pub(super) remaining: AtomicUsize,
    pub(super) reads: AtomicUsize,
    pub(super) write_attempts: AtomicUsize,
    pub(super) blocked: AtomicBool,
    writes: Mutex<Vec<u8>>,
}

struct ProbeIo {
    state: Arc<Probe>,
    frame: [u8; 3],
    offset: usize,
}

impl AsyncRead for ProbeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.state.remaining.load(Ordering::Relaxed) == 0 {
            return Poll::Pending;
        }
        let size = buf.remaining().min(3 - self.offset);
        buf.put_slice(&self.frame[self.offset..self.offset + size]);
        self.offset += size;
        if self.offset == 3 {
            self.offset = 0;
            self.state.remaining.fetch_sub(1, Ordering::Relaxed);
            self.state.reads.fetch_add(1, Ordering::Relaxed);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ProbeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.state.write_attempts.fetch_add(1, Ordering::Relaxed);
        if self.state.blocked.load(Ordering::Relaxed) {
            return Poll::Pending;
        }
        self.state.writes.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.state.blocked.load(Ordering::Relaxed) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub(super) async fn probe_stream(
    opcode: u8,
    frames: usize,
    blocked: bool,
) -> (client::WsStream, Arc<Probe>) {
    let state = Arc::new(Probe {
        remaining: AtomicUsize::new(frames),
        reads: AtomicUsize::new(0),
        write_attempts: AtomicUsize::new(0),
        blocked: AtomicBool::new(blocked),
        writes: Mutex::new(Vec::new()),
    });
    let io: crate::net::NetStream = Box::pin(ProbeIo {
        state: state.clone(),
        frame: [0x80 | opcode, 1, b'x'],
        offset: 0,
    });
    let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        Some(WebSocketClientConfig::new("ws://localhost/").protocol_config()),
    )
    .await;
    (ws, state)
}

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

const TOKEN: &str = "hunter2-ws-token";

fn secrets() -> MapSecretResolver {
    MapSecretResolver::new(HashMap::from([
        ("ws.token".to_string(), TOKEN.to_string()),
        ("ws.wrong".to_string(), "not-the-token".to_string()),
    ]))
}

fn json_row(v: i64) -> Message {
    Message::text(format!(r#"{{"device_id":"d","v":{v}}}"#))
}

fn csv_format(role: sparrow_formats::CsvRole) -> sparrow_formats::PayloadFormat {
    sparrow_formats::PayloadFormat::csv(
        sparrow_formats::CsvOptions::default()
            .compile(role)
            .unwrap(),
    )
}

async fn until(deadline: Duration, mut f: impl FnMut() -> bool) {
    tokio::time::timeout(deadline, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition deadline");
}

// ------------------------------------------------------------- servers

type ServerWs = tokio_tungstenite::WebSocketStream<std::pin::Pin<Box<dyn crate::net::NetIo>>>;

struct Listener {
    listener: TcpListener,
    port: u16,
    tls: Option<tokio_rustls::TlsAcceptor>,
}

impl Listener {
    async fn plain() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        Self {
            listener,
            port,
            tls: None,
        }
    }

    async fn rebind(port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        Self {
            listener,
            port,
            tls: None,
        }
    }

    async fn tls() -> Self {
        use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap()],
                PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap(),
            )
            .unwrap();
        let mut l = Self::plain().await;
        l.tls = Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)));
        l
    }

    fn url(&self) -> String {
        match self.tls {
            Some(_) => format!("wss://localhost:{}/feed", self.port),
            None => format!("ws://127.0.0.1:{}/feed", self.port),
        }
    }

    fn policy(&self) -> TargetPolicy {
        TargetPolicy::allow("127.0.0.1", self.port).with_allow("localhost", self.port)
    }

    /// Accept one upgrade; `check` may reject the handshake.
    async fn accept_with(
        &self,
        check: impl FnOnce(&Request, Response) -> Result<Response, ErrorResponse> + Unpin,
    ) -> Option<ServerWs> {
        let (tcp, _) = self.listener.accept().await.unwrap();
        let io: std::pin::Pin<Box<dyn crate::net::NetIo>> = match &self.tls {
            None => Box::pin(tcp),
            Some(acceptor) => Box::pin(acceptor.accept(tcp).await.ok()?),
        };
        tokio_tungstenite::accept_hdr_async(io, check).await.ok()
    }

    async fn accept(&self) -> ServerWs {
        self.accept_with(|_, r| Ok(r)).await.expect("upgrade")
    }
}

// ------------------------------------------------------------- sources

struct RunningSource {
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    rx: sparrow_io::observed::Receiver<QueuedRow>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<sparrow_model::Result<()>>,
}

fn start_source(
    url: String,
    policy: &TargetPolicy,
    mutate: impl FnOnce(&mut WebSocketSourceConfig),
) -> RunningSource {
    start_source_with(url, policy, ResourceBudget::compact(), mutate)
}

fn start_source_with(
    url: String,
    policy: &TargetPolicy,
    budget: ResourceBudget,
    mutate: impl FnOnce(&mut WebSocketSourceConfig),
) -> RunningSource {
    let mut config = WebSocketSourceConfig::new(url, schema());
    mutate(&mut config);
    let diag = IoDiagnostics::new();
    let capacity = config.inbox_capacity;
    let source = WebSocketSource::bind(config, &secrets(), policy, diag.clone()).unwrap();
    let owner = MemoryOwner::new(budget);
    diag.observation.initialize(&owner).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(source.run_budgeted(tx, cancel.clone(), owner.clone(), 64 * 1024));
    RunningSource {
        diag,
        owner,
        rx,
        cancel,
        task,
    }
}

impl RunningSource {
    async fn take(&mut self, n: usize) -> Vec<i64> {
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while got.len() < n {
                got.push(self.rx.recv().await.expect("source closed"));
            }
        })
        .await
        .expect("rows deadline");
        let mut builder = RowBatchBuilder::new(
            Arc::new(schema()),
            self.owner.clone(),
            CreditKind::Reservation,
            got.len().max(1),
            (1 << 20).min(self.owner.budget().reservation_bytes / 4),
        )
        .unwrap();
        for q in got {
            q.push_into(&mut builder).unwrap();
        }
        builder
            .finish()
            .unwrap()
            .rows()
            .iter()
            .map(|r| match r.values[1] {
                Scalar::Int64(v) => v,
                ref other => panic!("{other:?}"),
            })
            .collect()
    }

    async fn stop(self) -> sparrow_model::Result<()> {
        self.cancel.cancel();
        let r = tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("WebSocket source must stop promptly")
            .unwrap();
        drop(self.rx);
        until(Duration::from_secs(5), || {
            self.owner.usage().reservation_bytes == 0
        })
        .await;
        r
    }
}

// --------------------------------------------------------------- unit

#[test]
fn websocket_source_and_sink_config_gates() {
    let p = TargetPolicy::allow("127.0.0.1", 9000);
    let base = WebSocketSourceConfig::new("ws://127.0.0.1:9000/feed", schema());
    base.validate(&p).unwrap();
    assert_eq!(WebSocketSourceConfig::capabilities().kind, "websocket");
    assert_eq!(
        WebSocketSourceConfig::capabilities().replay,
        crate::ReplaySupport::Unsupported
    );
    type S = fn(&mut WebSocketSourceConfig);
    let cases: Vec<(S, ErrorCode)> = vec![
        (|c| c.prefetch_capacity = 0, ErrorCode::BoundExceeded),
        (|c| c.prefetch_capacity = 65, ErrorCode::BoundExceeded),
        (|c| c.prefetch_capacity = 64, ErrorCode::BoundExceeded),
        (|c| c.inbox_capacity = 0, ErrorCode::BoundExceeded),
        (
            |c| c.inbox_bytes = 4 * 1024 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.json_limits.max_bytes = 128 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.client.max_message_bytes = 2 * 1024 * 1024,
            ErrorCode::BoundExceeded,
        ),
        (
            |c| {
                c.restore = RestoreClaim::Checkpoint {
                    snapshot_id: "1".into(),
                }
            },
            ErrorCode::UnsupportedRestore,
        ),
        (
            |c| {
                c.payload_format = csv_format(sparrow_formats::CsvRole::Decode);
                c.framing = WebSocketFraming::Ndjson;
            },
            ErrorCode::InvalidArgument,
        ),
        (
            |c| {
                c.payload_format = csv_format(sparrow_formats::CsvRole::Decode);
                c.binary_frames = BinaryFrames::Decode;
            },
            ErrorCode::InvalidArgument,
        ),
        // Protobuf needs binary_frames=decode and one message per message.
        (
            |c| c.payload_format = protobuf_format(sparrow_formats::CsvRole::Decode),
            ErrorCode::InvalidArgument,
        ),
        (
            |c| {
                c.payload_format = protobuf_format(sparrow_formats::CsvRole::Decode);
                c.binary_frames = BinaryFrames::Decode;
                c.framing = WebSocketFraming::Ndjson;
            },
            ErrorCode::InvalidArgument,
        ),
        (
            |c| {
                c.payload_format = crate::protobuf_test_support::format(
                    sparrow_formats::CsvRole::Decode,
                    |o| {
                        o.fields.remove("v");
                    },
                );
                c.binary_frames = BinaryFrames::Decode;
            },
            ErrorCode::InvalidSchema,
        ),
    ];
    for (i, (mutate, code)) in cases.iter().enumerate() {
        let mut c = base.clone();
        mutate(&mut c);
        assert_eq!(c.validate(&p).unwrap_err().code, *code, "source case {i}");
    }
    // 1 MiB messages: 2 MiB + 128 KiB of connection buffers fit half of a
    // compact reservation only if it is large enough; the check is explicit.
    assert!(base.check_reservation_budget(256 * 1024).is_err());

    let sink = WebSocketSinkConfig::new("ws://127.0.0.1:9000/out");
    sink.validate(&p).unwrap();
    assert_eq!(WebSocketSinkConfig::capabilities().kind, "websocket_sink");
    type K = fn(&mut WebSocketSinkConfig);
    let cases: Vec<(K, ErrorCode)> = vec![
        (|c| c.queue_capacity = 0, ErrorCode::BoundExceeded),
        (|c| c.queue_capacity = 1025, ErrorCode::BoundExceeded),
        (|c| c.outbox_capacity = 0, ErrorCode::BoundExceeded),
        (
            |c| c.send_timeout = Duration::from_millis(1),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.flush_timeout = Duration::from_secs(61),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| {
                c.queue_capacity = 1024;
                c.client.max_message_bytes = 1024 * 1024;
            },
            ErrorCode::BoundExceeded,
        ),
        (
            |c| {
                c.payload_format = csv_format(sparrow_formats::CsvRole::Encode);
                c.frame = SinkFrame::Binary;
            },
            ErrorCode::InvalidArgument,
        ),
        (
            |c| c.payload_format = protobuf_format(sparrow_formats::CsvRole::Encode),
            ErrorCode::InvalidArgument,
        ),
        (
            |c| {
                c.restore = RestoreClaim::Checkpoint {
                    snapshot_id: "1".into(),
                }
            },
            ErrorCode::UnsupportedRestore,
        ),
    ];
    for (i, (mutate, code)) in cases.iter().enumerate() {
        let mut c = sink.clone();
        mutate(&mut c);
        assert_eq!(c.validate(&p).unwrap_err().code, *code, "sink case {i}");
    }
}

// --------------------------------------------------------- source e2e

#[tokio::test]
async fn websocket_source_decodes_text_ndjson_and_counts_binary_bad_and_oversize_records() {
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.framing = WebSocketFraming::Ndjson;
        c.client.max_message_bytes = 256 * 1024;
    });
    let mut ws = server.accept().await;
    ws.send(json_row(1)).await.unwrap();
    ws.send(Message::text(
        "{\"device_id\":\"d\",\"v\":2}\r\n\n{\"device_id\":\"d\",\"v\":3}\n",
    ))
    .await
    .unwrap();
    ws.send(Message::binary(br#"{"device_id":"d","v":99}"#.to_vec()))
        .await
        .unwrap();
    ws.send(Message::text("{not json}")).await.unwrap();
    // One NDJSON line above the 64 KiB record limit inside an accepted message.
    ws.send(Message::text(format!(
        "{{\"device_id\":\"{}\",\"v\":98}}\n{{\"device_id\":\"d\",\"v\":4}}",
        "x".repeat(70 * 1024)
    )))
    .await
    .unwrap();
    assert_eq!(run.take(4).await, vec![1, 2, 3, 4]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_received, 5);
    assert_eq!(snap.websocket_source_rows, 4);
    assert_eq!(snap.websocket_source_dropped_binary, 1);
    assert_eq!(snap.websocket_source_dropped_bad, 1);
    assert_eq!(snap.websocket_source_dropped_oversize, 1);
    assert_eq!(snap.websocket_source_connects, 1);
    run.stop().await.unwrap();
    // The source closes politely on stop.
    let closing = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap();
    assert!(
        matches!(closing, Some(Ok(Message::Close(_))) | None),
        "{closing:?}"
    );
}

#[tokio::test]
async fn websocket_source_binary_decode_and_fail_on_decode() {
    let server = Listener::plain().await;
    let run = start_source(server.url(), &server.policy(), |c| {
        c.binary_frames = BinaryFrames::Decode;
        c.fail_on_decode = true;
    });
    let mut ws = server.accept().await;
    let mut run = run;
    ws.send(Message::binary(br#"{"device_id":"d","v":7}"#.to_vec()))
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![7]);
    ws.send(Message::text("[]")).await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), run.task)
        .await
        .unwrap()
        .unwrap();
    assert!(r.unwrap_err().message.contains("fail_on_decode"));
    let health = run.diag.observation.endpoints().unwrap().0;
    assert_eq!(
        health.state,
        sparrow_model::observation::HealthState::Failed
    );
    assert_eq!(health.reason, "websocket_decode_failed");
    assert_eq!(health.failures, 1);
    assert_eq!(health.last_error_code, Some(ErrorCode::CodecViolation));
}

#[tokio::test]
async fn websocket_csv_over_text_frames_source_and_sink() {
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.payload_format = csv_format(sparrow_formats::CsvRole::Decode);
    });
    let mut ws = server.accept().await;
    for text in [
        "device_id,v\na,1\n",
        "v,device_id\r\n2,b\r\n",
        "device_id,v\nc,x\n",
    ] {
        ws.send(Message::text(text)).await.unwrap();
    }
    ws.send(Message::text("device_id,v\n\"d,e\",3\n"))
        .await
        .unwrap();
    assert_eq!(run.take(3).await, vec![1, 2, 3]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_dropped_bad, 1);
    assert_eq!(snap.csv_type_errors, 1);
    run.stop().await.unwrap();

    let sink_server = Listener::plain().await;
    let (_owner, diag, tx, cancel, task) = start_sink(&sink_server, |c| {
        c.payload_format = csv_format(sparrow_formats::CsvRole::Encode);
    });
    let mut peer = sink_server.accept().await;
    tx.send(batch(&_owner, 1, 2)).await.unwrap();
    for v in 1..=2 {
        let m = next_data(&mut peer).await;
        assert_eq!(m, Message::text(format!("device_id,v\ns,{v}\n")));
    }
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(diag.snapshot().websocket_sink_sent, 2);
}

fn protobuf_format(role: sparrow_formats::CsvRole) -> sparrow_formats::PayloadFormat {
    crate::protobuf_test_support::format(role, |_| {})
}

#[tokio::test]
async fn websocket_protobuf_over_binary_frames_source_and_sink() {
    use crate::protobuf_test_support::reading;
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.payload_format = protobuf_format(sparrow_formats::CsvRole::Decode);
        c.binary_frames = BinaryFrames::Decode;
    });
    let mut ws = server.accept().await;
    ws.send(Message::binary(reading("a", 1))).await.unwrap();
    // Malformed: truncated string.
    ws.send(Message::binary(vec![0x0a, 0x09, b'x'])).await.unwrap();
    // A text message is refused as bad even when its bytes are a valid
    // message (here exactly `reading("b", 2)`): never decoded.
    let text = String::from_utf8(reading("b", 2)).unwrap();
    ws.send(Message::text(text.clone())).await.unwrap();
    ws.send(Message::binary(reading("c", 3))).await.unwrap();
    assert_eq!(run.take(2).await, vec![1, 3]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_dropped_bad, 2);
    assert_eq!(snap.protobuf_malformed, 2);
    assert_eq!(snap.decode_errors, 2);
    run.stop().await.unwrap();

    // With fail_on_decode a text message fails the source.
    let server = Listener::plain().await;
    let run = start_source(server.url(), &server.policy(), |c| {
        c.payload_format = protobuf_format(sparrow_formats::CsvRole::Decode);
        c.binary_frames = BinaryFrames::Decode;
        c.fail_on_decode = true;
    });
    let mut ws = server.accept().await;
    ws.send(Message::text(text)).await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), run.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.unwrap_err().code, ErrorCode::CodecViolation);
    assert_eq!(run.diag.snapshot().protobuf_malformed, 1);

    let sink_server = Listener::plain().await;
    let (_owner, diag, tx, cancel, task) = start_sink(&sink_server, |c| {
        c.payload_format = protobuf_format(sparrow_formats::CsvRole::Encode);
        c.frame = SinkFrame::Binary;
    });
    let mut peer = sink_server.accept().await;
    tx.send(batch(&_owner, 1, 2)).await.unwrap();
    for v in 1..=2 {
        let m = next_data(&mut peer).await;
        assert_eq!(m, Message::binary(reading("s", v)));
    }
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(diag.snapshot().websocket_sink_sent, 2);
    assert_eq!(diag.snapshot().protobuf_encode_errors, 0);
}

#[tokio::test]
async fn websocket_source_heartbeat_timeout_reconnects() {
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.client.ping_interval = Duration::from_millis(100);
        c.client.idle_timeout = Duration::from_millis(400);
    });
    // First connection: never read, so Pings are never answered.
    let silent = server.accept().await;
    let mut ws = server.accept().await;
    let first = run.diag.snapshot();
    assert_eq!(first.websocket_source_heartbeat_timeouts, 1);
    assert!(first.websocket_source_pings_sent >= 2, "{first:?}");
    ws.send(json_row(5)).await.unwrap();
    assert_eq!(run.take(1).await, vec![5]);
    // A peer that answers Pings keeps the connection alive past idle_timeout.
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = ws.next().await {} });
    tokio::time::sleep(Duration::from_millis(900)).await;
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_reconnects, 1);
    assert_eq!(snap.websocket_source_heartbeat_timeouts, 1);
    drop(silent);
    run.stop().await.unwrap();
    reader.abort();
}

#[tokio::test]
async fn websocket_source_server_restart_reconnects_and_resumes() {
    let server = Listener::plain().await;
    let port = server.port;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.client.reconnect_max = Duration::from_millis(200);
        c.client.reconnect_attempts = 50;
    });
    let mut ws = server.accept().await;
    for v in 1..=3 {
        ws.send(json_row(v)).await.unwrap();
    }
    assert_eq!(run.take(3).await, vec![1, 2, 3]);
    // Server goes away entirely (listener and connection)...
    drop(ws);
    drop(server);
    until(Duration::from_secs(5), || {
        run.diag.snapshot().websocket_source_connect_failures >= 1
    })
    .await;
    // ...and comes back on the same port.
    let server = Listener::rebind(port).await;
    let mut ws = server.accept().await;
    for v in 4..=6 {
        ws.send(json_row(v)).await.unwrap();
    }
    assert_eq!(run.take(3).await, vec![4, 5, 6]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_reconnects, 1);
    assert_eq!(snap.websocket_source_disconnects, 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn websocket_source_oversize_message_is_rejected_before_decode_and_reconnects() {
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.client.max_message_bytes = 1024;
    });
    let mut ws = server.accept().await;
    ws.send(Message::text(format!(
        "{{\"device_id\":\"{}\",\"v\":1}}",
        "x".repeat(2000)
    )))
    .await
    .unwrap();
    let mut ws2 = server.accept().await;
    ws2.send(json_row(2)).await.unwrap();
    assert_eq!(run.take(1).await, vec![2]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_dropped_oversize, 1);
    assert_eq!(snap.websocket_source_dropped_bad, 0);
    assert_eq!(snap.websocket_source_reconnects, 1);
    drop(ws);
    run.stop().await.unwrap();
}

/// Server-side check of the handshake: bearer token, custom header and
/// subprotocol negotiation.
fn require_auth(req: &Request, mut resp: Response) -> Result<Response, ErrorResponse> {
    let header = |name: &str| {
        req.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    if header("authorization") != format!("Bearer {TOKEN}") || header("x-tenant") != "t1" {
        let mut reject = ErrorResponse::new(Some("denied".into()));
        *reject.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
        return Err(reject);
    }
    assert_eq!(header("sec-websocket-protocol"), "sparrow.v2, sparrow.v1");
    resp.headers_mut()
        .insert("sec-websocket-protocol", "sparrow.v1".parse().unwrap());
    Ok(resp)
}

fn auth_client(c: &mut WebSocketClientConfig, token_secret: &str) {
    c.auth = WebSocketAuth::Bearer {
        token_secret: token_secret.into(),
    };
    c.headers = vec![WebSocketHeader {
        name: "x-tenant".into(),
        value: Some("t1".into()),
        value_secret: None,
    }];
    c.subprotocols = vec!["sparrow.v2".into(), "sparrow.v1".into()];
    c.tls_ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
}

#[tokio::test]
async fn websocket_wss_custom_ca_auth_header_and_subprotocol() {
    let server = Listener::tls().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        auth_client(&mut c.client, "ws.token")
    });
    let mut ws = server.accept_with(require_auth).await.expect("authorized");
    ws.send(json_row(42)).await.unwrap();
    assert_eq!(run.take(1).await, vec![42]);
    run.stop().await.unwrap();

    // Wrong token: the server answers 401 on every attempt; the bounded
    // attempts fail the Source retryably without leaking the credential.
    let run = start_source(server.url(), &server.policy(), |c| {
        auth_client(&mut c.client, "ws.wrong");
        c.client.reconnect_attempts = 2;
        c.client.reconnect_max = Duration::from_millis(100);
    });
    for _ in 0..2 {
        assert!(server.accept_with(require_auth).await.is_none());
    }
    let e = tokio::time::timeout(Duration::from_secs(5), run.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(e.retryable, "{e:?}");
    assert!(e.message.contains("HTTP 401"), "{e}");
    assert!(
        !e.message.contains("not-the-token") && !e.message.contains("feed"),
        "{e}"
    );
    assert_eq!(run.diag.snapshot().websocket_source_connect_failures, 2);
    let health = run.diag.observation.endpoints().unwrap().0;
    assert_eq!(health.reason, "websocket_connect_exhausted");
    assert_eq!(health.failures, 1);
    assert_eq!(health.last_error_code, Some(ErrorCode::JobFailed));

    // Without the private CA the built-in web PKI roots refuse the server.
    let run = start_source(server.url(), &server.policy(), |c| {
        c.client.reconnect_attempts = 1;
    });
    let _ = server.accept_with(|_, r| Ok(r)).await;
    let e = tokio::time::timeout(Duration::from_secs(5), run.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(e.message.contains("TLS"), "{e}");
}

// --------------------------------------------------------------- sinks

fn batch(owner: &Arc<MemoryOwner>, from: i64, to: i64) -> RowBatch {
    wide_batch(owner, from, to, 1)
}

fn wide_batch(owner: &Arc<MemoryOwner>, from: i64, to: i64, width: usize) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        (to - from + 1) as usize,
        1 << 22,
    )
    .unwrap();
    for v in from..=to {
        builder
            .push(Row {
                values: vec![Scalar::utf8("s".repeat(width)), Scalar::Int64(v)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

type SinkParts = (
    Arc<MemoryOwner>,
    Arc<IoDiagnostics>,
    sparrow_io::observed::Sender<RowBatch>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
);

fn start_sink_with(
    url: String,
    policy: &TargetPolicy,
    outbox: Option<Arc<sparrow_model::InflightCounter>>,
    mutate: impl FnOnce(&mut WebSocketSinkConfig),
) -> SinkParts {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    diag.observation.initialize(&owner).unwrap();
    let mut config = WebSocketSinkConfig::new(url);
    mutate(&mut config);
    let capacity = config.outbox_capacity;
    let sink =
        WebSocketSink::bind(config, &secrets(), policy, owner.clone(), diag.clone()).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(sink.run(rx, cancel.clone(), outbox));
    (owner, diag, tx, cancel, task)
}

fn start_sink(server: &Listener, mutate: impl FnOnce(&mut WebSocketSinkConfig)) -> SinkParts {
    start_sink_with(server.url(), &server.policy(), None, mutate)
}

/// Next data message (Pings/Pongs are answered by the protocol layer).
async fn next_data(ws: &mut ServerWs) -> Message {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(m)) => return m,
                other => panic!("peer ended: {other:?}"),
            }
        }
    })
    .await
    .expect("message deadline")
}

fn value_of(m: &Message) -> i64 {
    let v: serde_json::Value = serde_json::from_str(m.to_text().unwrap()).unwrap();
    v["v"].as_i64().unwrap()
}

#[tokio::test]
async fn websocket_sink_sends_rows_and_flushes_queued_batches_on_stop() {
    let server = Listener::plain().await;
    let outbox = Arc::new(sparrow_model::InflightCounter::new());
    let (owner, diag, tx, cancel, task) =
        start_sink_with(server.url(), &server.policy(), Some(outbox.clone()), |c| {
            c.outbox_capacity = 8;
        });
    let mut ws = server.accept().await;
    outbox.enqueue();
    tx.send(batch(&owner, 1, 2)).await.unwrap();
    for v in 1..=2 {
        let m = next_data(&mut ws).await;
        assert!(m.is_text());
        assert_eq!(value_of(&m), v);
    }
    // Queue several batches and stop at once: they are still sent, then a
    // Close frame, within flush_timeout.
    for i in 0..5 {
        outbox.enqueue();
        tx.try_send(batch(&owner, 100 + 10 * i, 100 + 10 * i + 2))
            .unwrap();
    }
    cancel.cancel();
    let mut got = Vec::new();
    loop {
        match next_data(&mut ws).await {
            Message::Close(_) => break,
            m => got.push(value_of(&m)),
        }
    }
    assert_eq!(got.len(), 15);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let snap = diag.snapshot();
    assert_eq!(snap.websocket_sink_sent, 17, "{snap:?}");
    assert_eq!(snap.websocket_sink_closes, 1);
    assert_eq!(snap.websocket_sink_discarded_on_close, 0);
    assert_eq!(snap.websocket_sink_queue_items, 0);
    assert_eq!(outbox.failed(), 0);
    drop(tx);
    until(Duration::from_secs(5), || {
        owner.usage().reservation_bytes == 0
    })
    .await;
}

#[tokio::test]
async fn websocket_sink_ends_on_input_close_and_drops_oversize_rows() {
    let server = Listener::plain().await;
    let (owner, diag, tx, _cancel, task) = start_sink(&server, |c| {
        c.client.max_message_bytes = 1024;
        c.frame = SinkFrame::Binary;
    });
    let mut ws = server.accept().await;
    tx.send(wide_batch(&owner, 1, 1, 2000)).await.unwrap();
    tx.send(batch(&owner, 2, 2)).await.unwrap();
    drop(tx);
    let m = next_data(&mut ws).await;
    assert!(m.is_binary());
    assert_eq!(value_of(&Message::text(m.into_text().unwrap())), 2);
    assert!(matches!(next_data(&mut ws).await, Message::Close(_)));
    task.await.unwrap();
    let snap = diag.snapshot();
    assert_eq!(snap.websocket_sink_dropped_oversize, 1);
    assert_eq!(snap.websocket_sink_sent, 1);
}

#[tokio::test]
async fn websocket_sink_slow_server_backpressure_overflow_and_send_timeout() {
    let server = Listener::plain().await;
    socket2::SockRef::from(&server.listener)
        .set_recv_buffer_size(4096)
        .unwrap();
    // drop_newest: a server that never reads fills the socket, the writer's
    // send times out, and the full queue drops new rows instead of blocking.
    let (owner, diag, tx, cancel, task) = start_sink(&server, |c| {
        c.overflow = Overflow::DropNewest;
        c.queue_capacity = 4;
        c.send_timeout = Duration::from_millis(200);
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let stalled = server.accept().await;
    let accepts = Arc::new(AtomicUsize::new(0));
    let reconnects = {
        let accepts = accepts.clone();
        let server = Arc::new(server);
        let server2 = server.clone();
        (
            tokio::spawn(async move {
                let mut held = Vec::new();
                loop {
                    let ws = server2.accept().await;
                    accepts.fetch_add(1, Ordering::Relaxed);
                    held.push(ws); // keep it open, never read
                }
            }),
            server,
        )
    };
    for i in 0..300 {
        tx.send(wide_batch(&owner, i, i, 32 * 1024)).await.unwrap();
    }
    until(Duration::from_secs(10), || {
        let s = diag.snapshot();
        s.websocket_sink_send_timeouts >= 1 && s.websocket_sink_dropped_overflow >= 1
    })
    .await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let snap = diag.snapshot();
    assert!(snap.websocket_sink_sent < 300, "{snap:?}");
    assert_eq!(snap.websocket_sink_queue_items, 0, "{snap:?}");
    assert!(snap.websocket_sink_disconnects >= 1, "{snap:?}");
    drop((stalled, reconnects.1));
    reconnects.0.abort();

    // block: the same stall turns into pipeline backpressure, not drops,
    // and a stop cuts the blocked send at flush_timeout.
    let server = Listener::plain().await;
    socket2::SockRef::from(&server.listener)
        .set_recv_buffer_size(4096)
        .unwrap();
    let (owner, diag, tx, cancel, task) = start_sink(&server, |c| {
        c.queue_capacity = 2;
        c.send_timeout = Duration::from_secs(30);
        c.flush_timeout = Duration::from_millis(300);
        c.outbox_capacity = 2;
    });
    let _stalled = server.accept().await;
    let feeder = tokio::spawn(async move {
        for i in 0..400 {
            if tx.send(wide_batch(&owner, i, i, 60 * 1024)).await.is_err() {
                break;
            }
        }
    });
    // Stalled: nothing more is written for a while.
    let mut last = u64::MAX;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let sent = diag.snapshot().websocket_sink_sent;
            if sent == last {
                break;
            }
            last = sent;
        }
    })
    .await
    .expect("the sink must stall against a server that never reads");
    assert!(
        !feeder.is_finished(),
        "a blocked sink must hold back the outbox"
    );
    let snap = diag.snapshot();
    assert!(snap.websocket_sink_sent < 400, "{snap:?}");
    assert!(snap.websocket_sink_backpressure_waits >= 1, "{snap:?}");
    assert_eq!(snap.websocket_sink_dropped_overflow, 0);
    let stopped = std::time::Instant::now();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        stopped.elapsed() < Duration::from_secs(3),
        "stop must not wait for send_timeout"
    );
    let snap = diag.snapshot();
    assert_eq!(snap.websocket_sink_send_timeouts, 1, "{snap:?}");
    assert!(snap.websocket_sink_discarded_on_close >= 1, "{snap:?}");
    assert_eq!(snap.websocket_sink_queue_items, 0, "{snap:?}");
    feeder.abort();
}

#[tokio::test]
async fn websocket_sink_reconnects_after_server_restart_then_fails_closed_when_exhausted() {
    let server = Listener::plain().await;
    let port = server.port;
    let (owner, diag, tx, cancel, task) = start_sink(&server, |c| {
        c.client.reconnect_max = Duration::from_millis(100);
        c.client.reconnect_attempts = 3;
    });
    let mut ws = server.accept().await;
    tx.send(batch(&owner, 1, 1)).await.unwrap();
    assert_eq!(value_of(&next_data(&mut ws).await), 1);
    drop(ws);
    drop(server);
    let server = Listener::rebind(port).await;
    let mut ws = server.accept().await;
    tx.send(batch(&owner, 2, 2)).await.unwrap();
    assert_eq!(value_of(&next_data(&mut ws).await), 2);
    assert_eq!(diag.snapshot().websocket_sink_reconnects, 1);
    // Gone for good: bounded attempts, then the job is failed closed.
    drop(ws);
    drop(server);
    tokio::time::timeout(Duration::from_secs(10), cancel.cancelled())
        .await
        .expect("sink must cancel the job when reconnects are exhausted");
    task.await.unwrap();
    let snap = diag.snapshot();
    assert_eq!(snap.websocket_sink_fatal, 1);
    assert_eq!(snap.websocket_sink_connect_failures, 3);
    let health = diag.observation.endpoints().unwrap().1;
    assert_eq!(
        health.state,
        sparrow_model::observation::HealthState::Failed
    );
    assert_eq!(health.reason, "websocket_sink_connect_exhausted");
    assert_eq!(health.failures, 1);
}

#[tokio::test]
async fn websocket_source_full_inbox_keeps_heartbeats_and_drops_whole_prefetch_messages() {
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.inbox_capacity = 1;
        c.prefetch_capacity = 2;
        c.framing = WebSocketFraming::Ndjson;
        c.client.ping_interval = Duration::from_millis(100);
        c.client.idle_timeout = Duration::from_millis(400);
    });
    let mut peer = server.accept().await;
    peer.send(json_row(1)).await.unwrap();
    until(Duration::from_secs(2), || {
        run.diag.snapshot().websocket_source_rows == 1
    })
    .await;
    peer.send(json_row(2)).await.unwrap();
    until(Duration::from_secs(2), || {
        run.diag.snapshot().websocket_source_backpressure_waits == 1
    })
    .await;
    for v in [3, 4] {
        peer.send(json_row(v)).await.unwrap();
    }
    until(Duration::from_secs(2), || {
        run.diag.snapshot().websocket_source_received == 4
    })
    .await;
    // Both rows in this one complete data message are dropped as one overflow.
    peer.send(Message::text(
        "{\"device_id\":\"d\",\"v\":5}\n{\"device_id\":\"d\",\"v\":6}",
    ))
    .await
    .unwrap();
    until(Duration::from_secs(2), || {
        run.diag.snapshot().websocket_source_dropped_overflow == 1
    })
    .await;
    peer.send(Message::Ping(b"probe".to_vec().into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match peer.next().await {
                Some(Ok(Message::Pong(bytes))) if bytes.as_ref() == b"probe" => break,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                other => panic!("heartbeat peer ended: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    // Keep answering our Source's Pings while admission remains blocked for
    // longer than idle_timeout. No extra data traffic is necessary.
    let _ = tokio::time::timeout(Duration::from_millis(650), async {
        while let Some(Ok(_)) = peer.next().await {}
        panic!("Source closed despite answered heartbeats");
    })
    .await;
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_rows, 1);
    assert_eq!(snap.websocket_source_dropped_overflow, 1);
    assert_eq!(snap.websocket_source_disconnects, 0);
    assert_eq!(snap.websocket_source_heartbeat_timeouts, 0);
    assert!(snap.websocket_source_pings_sent >= 2);
    assert_eq!(run.take(4).await, [1, 2, 3, 4]);
    let owner = run.owner.clone();
    run.stop().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
async fn websocket_wire_message_retains_credit_after_actor_and_receiver_close() {
    let server = Listener::plain().await;
    let config = WebSocketSourceConfig::new(server.url(), schema());
    let reservation = config.reservation();
    let source = Arc::new(
        WebSocketSource::bind(config, &secrets(), &server.policy(), IoDiagnostics::new()).unwrap(),
    );
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut wire = source::Wire::start(source, owner.clone(), CancellationToken::new()).unwrap();
    let mut peer = server.accept().await;
    peer.send(json_row(7)).await.unwrap();
    let message = wire.messages.recv().await.unwrap();
    wire.close().await.unwrap();
    assert_eq!(owner.usage().reservation_bytes, reservation);
    assert!(message.payload.starts_with(b"{"));
    drop(message);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn websocket_source_ping_pending_write_observes_cancel_without_connect_timeout() {
    let (ws, probe) = probe_stream(10, 0, true).await;
    let (mut writer, mut reader) = ws.split();
    let cancel = CancellationToken::new();
    let child = cancel.clone();
    let task = tokio::spawn(async move {
        let mut last = tokio::time::Instant::now();
        client::send_duplex(
            &mut writer,
            &mut reader,
            Message::Ping(Default::default()),
            &child,
            None,
            Duration::from_secs(60),
            Duration::from_secs(600),
            &mut last,
            16,
            |_| true,
        )
        .await
    });
    until(Duration::from_secs(1), || {
        probe.write_attempts.load(Ordering::Relaxed) > 0
    })
    .await;
    let at = tokio::time::Instant::now();
    cancel.cancel();
    assert_eq!(task.await.unwrap(), client::DuplexSent::Cancelled);
    assert_eq!(tokio::time::Instant::now(), at);
}

#[tokio::test]
async fn websocket_duplex_write_pending_still_reads_control_frames() {
    let (ws, probe) = probe_stream(10, 2, true).await;
    let (mut writer, mut reader) = ws.split();
    let mut last = tokio::time::Instant::now();
    let cancel = CancellationToken::new();
    let result = client::send_duplex(
        &mut writer,
        &mut reader,
        Message::text("payload"),
        &cancel,
        None,
        Duration::from_millis(200),
        Duration::from_secs(600),
        &mut last,
        16,
        |_| {
            if probe.reads.load(Ordering::Relaxed) == 2 {
                probe.blocked.store(false, Ordering::Relaxed);
            }
            true
        },
    )
    .await;
    assert_eq!(result, client::DuplexSent::Ok);
    assert_eq!(probe.reads.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn websocket_source_ndjson_blank_lines_are_json_whitespace_only() {
    let server = Listener::plain().await;
    let mut run = start_source(server.url(), &server.policy(), |c| {
        c.framing = WebSocketFraming::Ndjson;
    });
    let mut ws = server.accept().await;
    // Space/tab/CR lines are blank; a form feed or vertical tab is not JSON
    // whitespace, so that line is decoded (and rejected as bad JSON).
    ws.send(Message::text(
        " \t\r\n\x0c\n\x0b\n{\"device_id\":\"d\",\"v\":5}\n",
    ))
    .await
    .unwrap();
    assert_eq!(run.take(1).await, vec![5]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_dropped_bad, 2, "{snap:?}");
    assert_eq!(snap.websocket_source_rows, 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn websocket_source_charges_decode_scratch_before_parsing() {
    let server = Listener::plain().await;
    // Smallest owner satisfying the half-budget gate for connection +
    // prefetch; enough for a small row, not a 60 KiB JSON decode estimate.
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = WebSocketSourceConfig::new(server.url(), schema()).reservation() * 2;
    let mut run = start_source_with(server.url(), &server.policy(), budget, |c| {
        c.client.max_message_bytes = 64 * 1024;
    });
    let mut ws = server.accept().await;
    // Malformed on purpose: had it been parsed it would count dropped_bad.
    ws.send(Message::text(format!(
        "{{\"device_id\":\"{}\",\"v\":",
        "x".repeat(60 * 1024)
    )))
    .await
    .unwrap();
    ws.send(json_row(8)).await.unwrap();
    assert_eq!(run.take(1).await, vec![8]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.websocket_source_dropped_budget, 1, "{snap:?}");
    assert_eq!(snap.websocket_source_dropped_bad, 0, "{snap:?}");
    assert_eq!(snap.decode_errors, 0, "{snap:?}");
    assert_eq!(snap.websocket_source_rows, 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn websocket_sink_charges_encode_before_encoding_and_holds_no_credit_after() {
    let server = Listener::plain().await;
    let outbox = Arc::new(sparrow_model::InflightCounter::new());
    let (owner, diag, tx, cancel, task) =
        start_sink_with(server.url(), &server.policy(), Some(outbox.clone()), |_| {});
    let mut ws = server.accept().await;
    let bound = owner.usage().reservation_bytes;
    // Exhaust the job reservation: rows from another owner cannot be encoded.
    let rows_owner = MemoryOwner::new(ResourceBudget::compact());
    let hog = owner
        .acquire(
            CreditKind::Reservation,
            owner.budget().reservation_bytes - bound,
        )
        .unwrap();
    outbox.enqueue();
    tx.send(batch(&rows_owner, 1, 3)).await.unwrap();
    until(Duration::from_secs(5), || {
        diag.snapshot().websocket_sink_dropped_budget == 3
    })
    .await;
    // Not acknowledged: the batch was not fully queued.
    until(Duration::from_secs(5), || outbox.failed() == 1).await;
    drop(hog);
    outbox.enqueue();
    tx.send(batch(&rows_owner, 4, 4)).await.unwrap();
    assert_eq!(value_of(&next_data(&mut ws).await), 4);
    let snap = diag.snapshot();
    assert_eq!(snap.websocket_sink_sent, 1, "{snap:?}");
    assert_eq!(snap.websocket_sink_dropped_bad, 0, "{snap:?}");
    assert_eq!(snap.csv_encode_errors, 0, "{snap:?}");
    // Encode leases are released once the message is queued and sent.
    until(Duration::from_secs(5), || {
        owner.usage().reservation_bytes == bound
    })
    .await;
    cancel.cancel();
    task.await.unwrap();
    drop(tx);
    until(Duration::from_secs(5), || {
        owner.usage().reservation_bytes == 0
    })
    .await;
}

#[test]
fn websocket_reservation_math_saturates_and_huge_values_are_refused() {
    let policy = TargetPolicy::allow("127.0.0.1", 9000);
    let mut sink = WebSocketSinkConfig::new("ws://127.0.0.1:9000/");
    sink.client.max_message_bytes = usize::MAX;
    sink.queue_capacity = usize::MAX;
    assert_eq!(sink.reservation(), usize::MAX);
    assert_eq!(
        sink.validate(&policy).unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    let _ = sink.client.protocol_config();
    let mut source = WebSocketSourceConfig::new("ws://127.0.0.1:9000/", schema());
    source.client.max_message_bytes = usize::MAX;
    assert_eq!(source.reservation(), usize::MAX);
    assert_eq!(
        source.validate(&policy).unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    source.client.max_message_bytes = 64 * 1024;
    source.inbox_bytes = usize::MAX;
    assert_eq!(
        source.validate(&policy).unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    // Default sink: read side + outgoing message/frame + 16 queued messages.
    let sink = WebSocketSinkConfig::new("ws://127.0.0.1:9000/");
    assert_eq!(sink.reservation(), 128 * 1024 + (2 + 2 + 16) * 64 * 1024);
}

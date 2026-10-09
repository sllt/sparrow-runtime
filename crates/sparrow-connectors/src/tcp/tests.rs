//! TCP Source/Sink tests against in-process tokio TCP servers (plain and
//! rustls with the repository's localhost test CA).

use super::*;
use crate::{IoDiagnostics, TargetPolicy};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, QueuedRow, ResourceBudget,
    RestoreClaim, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

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

fn json(v: i64) -> String {
    format!(r#"{{"device_id":"d","v":{v}}}"#)
}

fn prefixed(payload: &[u8], width: PrefixWidth) -> Vec<u8> {
    let mut out = match width {
        PrefixWidth::U16 => (payload.len() as u16).to_be_bytes().to_vec(),
        PrefixWidth::U32 => (payload.len() as u32).to_be_bytes().to_vec(),
    };
    out.extend(payload);
    out
}

fn csv_format(
    role: sparrow_formats::CsvRole,
    header: bool,
    multiline: bool,
) -> sparrow_formats::PayloadFormat {
    let options = sparrow_formats::CsvOptions {
        header,
        multiline,
        ..Default::default()
    };
    sparrow_formats::PayloadFormat::csv(options.compile(role).unwrap())
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

struct Server {
    listener: TcpListener,
    port: u16,
    tls: Option<tokio_rustls::TlsAcceptor>,
}

type Conn = std::pin::Pin<Box<dyn crate::net::NetIo>>;

impl Server {
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
        Self {
            listener: TcpListener::bind(("127.0.0.1", port)).await.unwrap(),
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
        let mut s = Self::plain().await;
        s.tls = Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)));
        s
    }

    fn host(&self) -> &'static str {
        if self.tls.is_some() {
            "localhost"
        } else {
            "127.0.0.1"
        }
    }

    fn policy(&self) -> TargetPolicy {
        TargetPolicy::allow("127.0.0.1", self.port).with_allow("localhost", self.port)
    }

    async fn accept(&self) -> Conn {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (tcp, _) = self.listener.accept().await.unwrap();
            let conn: Conn = match &self.tls {
                None => Box::pin(tcp),
                Some(acceptor) => Box::pin(acceptor.accept(tcp).await.unwrap()),
            };
            conn
        })
        .await
        .expect("accept deadline")
    }
}

/// Read until `n` newline-terminated lines arrived.
async fn read_lines(conn: &mut Conn, n: usize) -> Vec<String> {
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while buf.iter().filter(|&&b| b == b'\n').count() < n {
            let mut chunk = [0u8; 4096];
            let k = conn.read(&mut chunk).await.unwrap();
            assert!(k > 0, "peer closed early");
            buf.extend_from_slice(&chunk[..k]);
        }
    })
    .await
    .expect("lines deadline");
    String::from_utf8(buf)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

async fn read_to_eof(conn: &mut Conn) -> Vec<u8> {
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), conn.read_to_end(&mut buf))
        .await
        .expect("eof deadline")
        .unwrap();
    buf
}

// ------------------------------------------------------------- sources

struct RunningSource {
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    rx: sparrow_io::observed::Receiver<QueuedRow>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<sparrow_model::Result<()>>,
}

fn start_source(server: &Server, mutate: impl FnOnce(&mut TcpSourceConfig)) -> RunningSource {
    let mut config = TcpSourceConfig::new(server.host(), server.port, schema());
    config.client.tls = server.tls.is_some();
    if config.client.tls {
        config.client.tls_ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
    }
    mutate(&mut config);
    let diag = IoDiagnostics::new();
    let capacity = config.inbox_capacity;
    let source = TcpSource::bind(config, &server.policy(), diag.clone()).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
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
            1 << 20,
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
            .expect("TCP source must stop promptly")
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
fn tcp_source_and_sink_config_gates() {
    let p = TargetPolicy::allow("127.0.0.1", 9000);
    let base = TcpSourceConfig::new("127.0.0.1", 9000, schema());
    base.validate(&p).unwrap();
    assert_eq!(TcpSourceConfig::capabilities().kind, "tcp");
    assert_eq!(TcpSinkConfig::capabilities().kind, "tcp_sink");
    assert_eq!(
        TcpSinkConfig::capabilities().replay,
        crate::ReplaySupport::Unsupported
    );
    type S = fn(&mut TcpSourceConfig);
    let cases: Vec<(S, ErrorCode)> = vec![
        (|c| c.inbox_capacity = 0, ErrorCode::BoundExceeded),
        (
            |c| c.idle_timeout = Duration::from_millis(10),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| c.inbox_bytes = 4 * 1024 * 1024,
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
                c.payload_format = csv_format(sparrow_formats::CsvRole::Decode, true, true);
            },
            ErrorCode::InvalidArgument,
        ),
    ];
    for (i, (mutate, code)) in cases.into_iter().enumerate() {
        let mut c = base.clone();
        mutate(&mut c);
        assert_eq!(c.validate(&p).unwrap_err().code, code, "case {i}");
    }
    // Multiline CSV is fine inside length-prefixed frames.
    let mut c = base.clone();
    c.payload_format = csv_format(sparrow_formats::CsvRole::Decode, true, true);
    c.client.framing = TcpFraming::LengthPrefixed;
    c.validate(&p).unwrap();

    let sink = TcpSinkConfig::new("127.0.0.1", 9000);
    sink.validate(&p).unwrap();
    let mut big = sink.clone();
    big.queue_capacity = 64;
    assert_eq!(
        big.validate(&p).unwrap_err().code,
        ErrorCode::BoundExceeded,
        "64 x 64 KiB frames exceed half the compact reservation"
    );
    let mut bad = sink.clone();
    bad.send_timeout = Duration::ZERO;
    assert_eq!(bad.validate(&p).unwrap_err().code, ErrorCode::BoundExceeded);
    assert!(sink.reservation() <= ResourceBudget::compact().reservation_bytes / 2);
}

// ------------------------------------------------------------- source e2e

#[tokio::test]
async fn tcp_source_lines_fragmented_coalesced_crlf_and_blank_lines() {
    let server = Server::plain().await;
    let mut run = start_source(&server, |_| {});
    let mut conn = server.accept().await;
    // Byte by byte, CRLF and blank lines in between.
    let slow = format!("{}\r\n\r\n   \n{}\n", json(1), json(2));
    for b in slow.as_bytes() {
        conn.write_all(&[*b]).await.unwrap();
        conn.flush().await.unwrap();
    }
    assert_eq!(run.take(2).await, vec![1, 2]);
    // Many records coalesced into one write.
    let burst: String = (3..=202).map(|v| json(v) + "\n").collect();
    conn.write_all(burst.as_bytes()).await.unwrap();
    assert_eq!(run.take(200).await, (3..=202).collect::<Vec<_>>());
    let snap = run.diag.snapshot();
    assert_eq!(snap.tcp_source_received, 202);
    assert_eq!(snap.tcp_source_rows, 202);
    assert_eq!(snap.tcp_source_dropped_bad, 0);
    assert_eq!(snap.tcp_source_connects, 1);
    assert!(snap.tcp_source_bytes_read >= burst.len() as u64);
    // A record cut off by EOF is never decoded.
    conn.write_all(br#"{"device_id":"d","v":99"#).await.unwrap();
    drop(conn);
    until(Duration::from_secs(5), || {
        run.diag.snapshot().tcp_source_dropped_partial == 1
    })
    .await;
    let _reconnected = server.accept().await;
    run.stop().await.unwrap();
}

#[tokio::test]
async fn tcp_source_length_prefixed_u16_and_u32() {
    for width in [PrefixWidth::U16, PrefixWidth::U32] {
        let server = Server::plain().await;
        let mut run = start_source(&server, |c| {
            c.client.framing = TcpFraming::LengthPrefixed;
            c.client.prefix_width = width;
            c.client.max_frame_bytes = width.max_len().min(c.client.max_frame_bytes);
        });
        let mut conn = server.accept().await;
        let mut wire = Vec::new();
        for v in 1..=50 {
            wire.extend(prefixed(json(v).as_bytes(), width));
        }
        // First half byte by byte, second half in one write.
        let (a, b) = wire.split_at(wire.len() / 2);
        for byte in a {
            conn.write_all(&[*byte]).await.unwrap();
        }
        conn.write_all(b).await.unwrap();
        // An empty frame decodes as a bad record, not a stream error.
        conn.write_all(&prefixed(b"", width)).await.unwrap();
        conn.write_all(&prefixed(json(51).as_bytes(), width))
            .await
            .unwrap();
        assert_eq!(run.take(51).await, (1..=51).collect::<Vec<_>>());
        let snap = run.diag.snapshot();
        assert_eq!(snap.tcp_source_received, 52, "{width:?}");
        assert_eq!(snap.tcp_source_dropped_bad, 1, "{width:?}");
        run.stop().await.unwrap();
    }
}

#[tokio::test]
async fn tcp_source_oversize_resync_and_disconnect() {
    // lines + resync: a long line is skipped, the connection survives.
    let server = Server::plain().await;
    let mut run = start_source(&server, |c| c.client.max_frame_bytes = 1024);
    let mut conn = server.accept().await;
    let long = format!(r#"{{"device_id":"{}","v":0}}"#, "x".repeat(200_000));
    conn.write_all(format!("{long}\n{}\n", json(7)).as_bytes())
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![7]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.tcp_source_dropped_oversize, 1);
    assert_eq!(snap.tcp_source_disconnects, 0);
    run.stop().await.unwrap();

    // lines + disconnect: the connection is dropped and re-established.
    let server = Server::plain().await;
    let mut run = start_source(&server, |c| {
        c.client.max_frame_bytes = 1024;
        c.oversize = OversizePolicy::Disconnect;
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let mut conn = server.accept().await;
    conn.write_all(format!("{long}\n").as_bytes()).await.ok();
    let mut again = server.accept().await;
    again
        .write_all(format!("{}\n", json(8)).as_bytes())
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![8]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.tcp_source_dropped_oversize, 1);
    assert_eq!(snap.tcp_source_disconnects, 1);
    assert_eq!(snap.tcp_source_reconnects, 1);
    drop(conn);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn tcp_source_length_prefix_over_max_is_rejected_before_buffering() {
    // resync: the declared payload is skipped without being buffered; the
    // ledger charge stays the fixed connection reservation.
    let server = Server::plain().await;
    let mut run = start_source(&server, |c| {
        c.client.framing = TcpFraming::LengthPrefixed;
        c.client.max_frame_bytes = 1024;
    });
    let mut conn = server.accept().await;
    conn.write_all(&200_000u32.to_be_bytes()).await.unwrap();
    for _ in 0..50 {
        conn.write_all(&[b'z'; 4000]).await.unwrap();
    }
    let reserved = run.owner.usage().reservation_bytes;
    conn.write_all(&prefixed(json(1).as_bytes(), PrefixWidth::U32))
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![1]);
    assert_eq!(run.diag.snapshot().tcp_source_dropped_oversize, 1);
    assert!(
        reserved
            <= TcpClientConfig {
                framing: TcpFraming::LengthPrefixed,
                max_frame_bytes: 1024,
                ..TcpClientConfig::new("127.0.0.1", 1)
            }
            .connection_reservation()
                + 64 * 1024,
        "{reserved}"
    );
    run.stop().await.unwrap();

    // disconnect: a 4 GiB declaration drops the connection immediately.
    let server = Server::plain().await;
    let run = start_source(&server, |c| {
        c.client.framing = TcpFraming::LengthPrefixed;
        c.oversize = OversizePolicy::Disconnect;
    });
    let mut conn = server.accept().await;
    conn.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    assert!(read_to_eof(&mut conn).await.is_empty(), "client hung up");
    let snap = run.diag.snapshot();
    assert_eq!(snap.tcp_source_dropped_oversize, 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn tcp_source_server_restart_reconnects_and_resumes() {
    let server = Server::plain().await;
    let port = server.port;
    let mut run = start_source(&server, |c| {
        c.client.reconnect_max = Duration::from_millis(200)
    });
    let mut conn = server.accept().await;
    conn.write_all(format!("{}\n", json(1)).as_bytes())
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![1]);
    drop(conn);
    drop(server);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let server = Server::rebind(port).await;
    let mut conn = server.accept().await;
    conn.write_all(format!("{}\n{}\n", json(2), json(3)).as_bytes())
        .await
        .unwrap();
    assert_eq!(run.take(2).await, vec![2, 3]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.tcp_source_reconnects, 1);
    assert_eq!(snap.tcp_source_disconnects, 1);
    assert!(snap.tcp_source_connect_failures >= 1, "{snap:?}");
    run.stop().await.unwrap();
}

#[tokio::test]
async fn tcp_source_idle_timeout_reconnects() {
    let server = Server::plain().await;
    let mut run = start_source(&server, |c| {
        c.idle_timeout = Duration::from_millis(300);
        c.client.reconnect_max = Duration::from_millis(100);
        c.client.keepalive = Some(Duration::from_secs(1));
    });
    let silent = server.accept().await;
    let mut conn = server.accept().await;
    until(Duration::from_secs(5), || {
        run.diag.snapshot().tcp_source_reconnects == 1
    })
    .await;
    assert_eq!(run.diag.snapshot().tcp_source_idle_timeouts, 1);
    conn.write_all(format!("{}\n", json(5)).as_bytes())
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![5]);
    drop(silent);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn tcp_source_tls_with_private_ca_and_without_it() {
    let server = Server::tls().await;
    let mut run = start_source(&server, |_| {});
    let mut conn = server.accept().await;
    conn.write_all(format!("{}\n", json(11)).as_bytes())
        .await
        .unwrap();
    conn.flush().await.unwrap();
    assert_eq!(run.take(1).await, vec![11]);
    run.stop().await.unwrap();

    // Built-in web PKI roots refuse the self-signed server; the error is
    // redacted and retryable.
    let run = start_source(&server, |c| {
        c.client.tls_ca_pem = None;
        c.client.reconnect_attempts = 2;
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let server = Arc::new(server);
    let acceptor = {
        let server = server.clone();
        tokio::spawn(async move {
            loop {
                let (tcp, _) = server.listener.accept().await.unwrap();
                let acceptor = server.tls.clone().unwrap();
                tokio::spawn(async move {
                    let _ = acceptor.accept(tcp).await;
                });
            }
        })
    };
    let e = tokio::time::timeout(Duration::from_secs(10), run.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::JobFailed);
    assert!(e.retryable, "{e:?}");
    assert!(e.message.contains("TLS handshake failed"), "{e}");
    assert!(!e.message.contains("localhost") && !e.message.contains(&server.port.to_string()));
    assert_eq!(run.diag.snapshot().tcp_source_connect_failures, 2);
    acceptor.abort();
}

#[tokio::test]
async fn tcp_source_csv_lines_header_per_connection() {
    let server = Server::plain().await;
    let mut run = start_source(&server, |c| {
        c.payload_format = csv_format(sparrow_formats::CsvRole::Decode, true, false);
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let mut conn = server.accept().await;
    conn.write_all(b"v,device_id\r\n1,a\r\n2,b\nnot-a-number,c\n3,\"q,uoted\"\n")
        .await
        .unwrap();
    assert_eq!(run.take(3).await, vec![1, 2, 3]);
    assert_eq!(run.diag.snapshot().tcp_source_dropped_bad, 1);
    drop(conn);
    // A new connection is a new document: its first line is the header.
    let mut conn = server.accept().await;
    conn.write_all(b"device_id,v\nz,4\n").await.unwrap();
    assert_eq!(run.take(1).await, vec![4]);
    // An unusable header drops the connection (counted bad).
    drop(conn);
    let mut conn = server.accept().await;
    conn.write_all(b"nope\n").await.unwrap();
    assert!(read_to_eof(&mut conn).await.is_empty());
    assert_eq!(run.diag.snapshot().tcp_source_dropped_bad, 2);
    run.stop().await.unwrap();
}

// ------------------------------------------------------------- sinks

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

fn start_sink(
    server: &Server,
    outbox: Option<Arc<sparrow_model::InflightCounter>>,
    mutate: impl FnOnce(&mut TcpSinkConfig),
) -> SinkParts {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    diag.observation.initialize(&owner).unwrap();
    let mut config = TcpSinkConfig::new(server.host(), server.port);
    mutate(&mut config);
    let capacity = config.outbox_capacity;
    let sink = TcpSink::bind(config, &server.policy(), owner.clone(), diag.clone()).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(sink.run(rx, cancel.clone(), outbox));
    (owner, diag, tx, cancel, task)
}

fn value_of(line: &str) -> i64 {
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    v["v"].as_i64().unwrap()
}

#[tokio::test]
async fn tcp_sink_lines_send_and_flush_queued_batches_on_stop() {
    let server = Server::plain().await;
    let outbox = Arc::new(sparrow_model::InflightCounter::new());
    let (owner, diag, tx, cancel, task) =
        start_sink(&server, Some(outbox.clone()), |c| c.outbox_capacity = 8);
    let mut conn = server.accept().await;
    outbox.enqueue();
    tx.send(batch(&owner, 1, 5)).await.unwrap();
    let first = read_lines(&mut conn, 5).await;
    assert_eq!(
        first.iter().map(|l| value_of(l)).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    // Queued but unsent when the stop arrives: flushed, then shut down.
    for i in 0..3 {
        outbox.enqueue();
        tx.send(batch(&owner, 6 + i * 4, 9 + i * 4)).await.unwrap();
    }
    cancel.cancel();
    let rest = read_to_eof(&mut conn).await;
    let rest: Vec<i64> = String::from_utf8(rest)
        .unwrap()
        .lines()
        .map(value_of)
        .collect();
    assert_eq!(rest, (6..=17).collect::<Vec<_>>());
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let snap = diag.snapshot();
    assert_eq!(snap.tcp_sink_sent, 17);
    assert_eq!(snap.tcp_sink_closes, 1);
    assert_eq!(snap.tcp_sink_discarded_on_close, 0);
    assert_eq!(snap.tcp_sink_queue_items, 0);
    assert_eq!(outbox.acked(), 4);
}

#[tokio::test]
async fn tcp_sink_length_prefixed_and_oversize_drop() {
    let server = Server::plain().await;
    let (owner, diag, tx, _cancel, task) = start_sink(&server, None, |c| {
        c.client.framing = TcpFraming::LengthPrefixed;
        c.client.prefix_width = PrefixWidth::U16;
        c.client.max_frame_bytes = 1024;
    });
    let mut conn = server.accept().await;
    tx.send(wide_batch(&owner, 1, 1, 2000)).await.unwrap();
    tx.send(batch(&owner, 2, 3)).await.unwrap();
    drop(tx);
    let wire = read_to_eof(&mut conn).await;
    let mut expected = prefixed(json_s(2).as_bytes(), PrefixWidth::U16);
    expected.extend(prefixed(json_s(3).as_bytes(), PrefixWidth::U16));
    assert_eq!(wire, expected);
    task.await.unwrap();
    let snap = diag.snapshot();
    assert_eq!(snap.tcp_sink_dropped_oversize, 1);
    assert_eq!(snap.tcp_sink_sent, 2);
    assert_eq!(snap.tcp_sink_bytes_written, expected.len() as u64);
}

fn json_s(v: i64) -> String {
    format!(r#"{{"device_id":"s","v":{v}}}"#)
}

#[tokio::test]
async fn tcp_sink_csv_lines_header_per_connection_and_reconnect() {
    let server = Server::plain().await;
    let (owner, diag, tx, cancel, task) = start_sink(&server, None, |c| {
        c.payload_format = csv_format(sparrow_formats::CsvRole::Encode, true, false);
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let mut conn = server.accept().await;
    tx.send(batch(&owner, 1, 2)).await.unwrap();
    assert_eq!(
        read_lines(&mut conn, 3).await,
        ["device_id,v", "s,1", "s,2"]
    );
    // Peer goes away: reconnect, and the new connection gets its header.
    conn.write_all(b"ignored by the sink").await.unwrap();
    until(Duration::from_secs(5), || {
        diag.snapshot().tcp_sink_ignored_bytes > 0
    })
    .await;
    drop(conn);
    let mut conn = server.accept().await;
    until(Duration::from_secs(5), || {
        diag.snapshot().tcp_sink_reconnects == 1
    })
    .await;
    tx.send(batch(&owner, 3, 3)).await.unwrap();
    assert_eq!(read_lines(&mut conn, 2).await, ["device_id,v", "s,3"]);
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(diag.snapshot().tcp_sink_sent, 3);
}

#[tokio::test]
async fn tcp_sink_slow_peer_backpressure_overflow_and_send_timeout() {
    // drop_newest: a peer that never reads fills the socket; the partial
    // write times out and the full queue drops new rows.
    let server = Server::plain().await;
    socket2::SockRef::from(&server.listener)
        .set_recv_buffer_size(4096)
        .unwrap();
    let (owner, diag, tx, cancel, task) = start_sink(&server, None, |c| {
        c.overflow = TcpOverflow::DropNewest;
        c.queue_capacity = 4;
        c.send_timeout = Duration::from_millis(200);
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let server = Arc::new(server);
    let holder = {
        let server = server.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                held.push(server.accept().await); // never read
            }
        })
    };
    for i in 0..300 {
        tx.send(wide_batch(&owner, i, i, 32 * 1024)).await.unwrap();
    }
    until(Duration::from_secs(10), || {
        let s = diag.snapshot();
        s.tcp_sink_send_timeouts >= 1 && s.tcp_sink_dropped_overflow >= 1
    })
    .await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let snap = diag.snapshot();
    assert!(snap.tcp_sink_sent < 300, "{snap:?}");
    assert!(snap.tcp_sink_disconnects >= 1, "{snap:?}");
    assert_eq!(snap.tcp_sink_queue_items, 0, "{snap:?}");
    holder.abort();

    // block: the stall becomes outbox backpressure, and a stop cuts the
    // blocked write at flush_timeout.
    let server = Server::plain().await;
    socket2::SockRef::from(&server.listener)
        .set_recv_buffer_size(4096)
        .unwrap();
    let (owner, diag, tx, cancel, task) = start_sink(&server, None, |c| {
        c.queue_capacity = 2;
        c.outbox_capacity = 2;
        c.send_timeout = Duration::from_secs(30);
        c.flush_timeout = Duration::from_millis(300);
    });
    let _stalled = server.accept().await;
    let feeder = tokio::spawn(async move {
        for i in 0..400 {
            if tx.send(wide_batch(&owner, i, i, 60 * 1024)).await.is_err() {
                break;
            }
        }
    });
    let mut last = u64::MAX;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let sent = diag.snapshot().tcp_sink_sent;
            if sent == last {
                break;
            }
            last = sent;
        }
    })
    .await
    .expect("the sink must stall against a peer that never reads");
    assert!(
        !feeder.is_finished(),
        "a blocked sink holds back the outbox"
    );
    let snap = diag.snapshot();
    assert!(snap.tcp_sink_backpressure_waits >= 1, "{snap:?}");
    assert_eq!(snap.tcp_sink_dropped_overflow, 0);
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
    assert_eq!(snap.tcp_sink_send_timeouts, 1, "{snap:?}");
    assert!(snap.tcp_sink_discarded_on_close >= 1, "{snap:?}");
    feeder.abort();
}

#[tokio::test]
async fn tcp_sink_fails_closed_after_reconnect_attempts() {
    let server = Server::plain().await;
    let (owner, diag, tx, cancel, task) = start_sink(&server, None, |c| {
        c.client.reconnect_attempts = 2;
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let conn = server.accept().await;
    drop(conn);
    drop(server);
    tx.send(batch(&owner, 1, 1)).await.ok();
    tokio::time::timeout(Duration::from_secs(10), cancel.cancelled())
        .await
        .expect("fatal sink cancels the job");
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let snap = diag.snapshot();
    assert_eq!(snap.tcp_sink_fatal, 1);
    assert_eq!(snap.tcp_sink_connect_failures, 2);
    assert_eq!(snap.tcp_sink_queue_items, 0);
}

/// Take every free Reservation byte of `owner` (after the connector charged
/// its fixed buffers) so the next decode/encode charge is refused.
fn hog(owner: &Arc<MemoryOwner>) -> sparrow_model::MemoryLease {
    let free = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    owner.acquire(CreditKind::Reservation, free).unwrap()
}

#[tokio::test]
async fn tcp_source_decode_scratch_is_charged_before_parsing() {
    // JSON: without credit for the parse working set the record is counted
    // dropped_budget and never parsed; the connection survives.
    let server = Server::plain().await;
    let mut run = start_source(&server, |_| {});
    let mut conn = server.accept().await;
    until(Duration::from_secs(5), || {
        run.owner.usage().reservation_bytes > 0
    })
    .await;
    let held = hog(&run.owner);
    conn.write_all(format!("{}\n", json(1)).as_bytes())
        .await
        .unwrap();
    until(Duration::from_secs(5), || {
        run.diag.snapshot().tcp_source_dropped_budget == 1
    })
    .await;
    assert_eq!(run.diag.snapshot().tcp_source_dropped_bad, 0);
    drop(held);
    conn.write_all(format!("{}\n", json(2)).as_bytes())
        .await
        .unwrap();
    assert_eq!(run.take(1).await, vec![2]);
    assert_eq!(run.diag.snapshot().tcp_source_disconnects, 0);
    run.stop().await.unwrap();

    // CSV over lines: an uncharged header ends the connection, so the next
    // line is never taken for the header; the new connection starts over.
    let server = Server::plain().await;
    let mut run = start_source(&server, |c| {
        c.payload_format = csv_format(sparrow_formats::CsvRole::Decode, true, false);
        c.client.reconnect_max = Duration::from_millis(100);
    });
    let mut conn = server.accept().await;
    until(Duration::from_secs(5), || {
        run.owner.usage().reservation_bytes > 0
    })
    .await;
    let held = hog(&run.owner);
    conn.write_all(b"device_id,v\nz,9\n").await.unwrap();
    assert!(read_to_eof(&mut conn).await.is_empty(), "client hung up");
    let snap = run.diag.snapshot();
    assert_eq!(snap.tcp_source_dropped_budget, 1, "{snap:?}");
    assert_eq!(snap.tcp_source_dropped_bad, 0, "{snap:?}");
    drop(held);
    let mut conn = server.accept().await;
    conn.write_all(b"v,device_id\n4,z\n").await.unwrap();
    assert_eq!(run.take(1).await, vec![4]);
    assert_eq!(run.diag.snapshot().tcp_source_disconnects, 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn tcp_sink_encode_scratch_is_charged_before_encoding() {
    for framing in [TcpFraming::Lines, TcpFraming::LengthPrefixed] {
        let server = Server::plain().await;
        let outbox = Arc::new(sparrow_model::InflightCounter::new());
        let (owner, diag, tx, cancel, task) = start_sink(&server, Some(outbox.clone()), |c| {
            c.client.framing = framing
        });
        let mut conn = server.accept().await;
        let refused = batch(&owner, 1, 2);
        let sent = batch(&owner, 3, 3);
        let held = hog(&owner);
        outbox.enqueue();
        tx.send(refused).await.unwrap();
        until(Duration::from_secs(5), || {
            diag.snapshot().tcp_sink_dropped_budget == 2
        })
        .await;
        drop(held);
        outbox.enqueue();
        tx.send(sent).await.unwrap();
        drop(tx);
        let wire = read_to_eof(&mut conn).await;
        let expected = match framing {
            TcpFraming::Lines => format!("{}\n", json_s(3)).into_bytes(),
            TcpFraming::LengthPrefixed => prefixed(json_s(3).as_bytes(), PrefixWidth::U32),
        };
        assert_eq!(wire, expected, "{framing:?}");
        task.await.unwrap();
        let snap = diag.snapshot();
        assert_eq!(snap.tcp_sink_sent, 1, "{framing:?}");
        assert_eq!(snap.tcp_sink_dropped_bad, 0, "{framing:?}");
        assert_eq!(outbox.acked(), 1, "the refused batch is not acknowledged");
        drop(cancel);
    }
}

#[test]
fn tcp_sink_reservation_saturates_and_counts_writer_frame_and_header() {
    let mut huge = TcpSinkConfig::new("127.0.0.1", 1);
    huge.client.max_frame_bytes = usize::MAX;
    huge.queue_capacity = usize::MAX;
    assert_eq!(huge.reservation(), usize::MAX);
    assert!(huge.check_reservation_budget(usize::MAX).is_err());

    let json = TcpSinkConfig::new("127.0.0.1", 1);
    let frame = json.client.frame_bytes();
    assert_eq!(
        json.reservation(),
        json.client.connection_reservation() + (json.queue_capacity + 1) * frame
    );
    let mut csv = json.clone();
    csv.payload_format = csv_format(sparrow_formats::CsvRole::Encode, true, false);
    assert_eq!(csv.reservation(), json.reservation() + frame, "header slot");
}

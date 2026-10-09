//! In-process HTTPS mock of `/api/v2/write`. Real-server coverage lives in
//! `real_tests` (opt-in, `SPARROW_INFLUXD`).

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Mutex;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sparrow_model::{
    DataType, Field, FieldId, ResourceBudget, Row, RowBatchBuilder, Scalar, SchemaId,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;
use crate::secret::MapSecretResolver;

use crate::http::observation_tls_fixture as tls_fixture;

#[cfg(test)]
#[path = "real_tests.rs"]
mod real_tests;

#[derive(Clone)]
pub(super) struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: &'static str,
    delay: Duration,
    /// Close the connection after reading the request, without a response.
    hang_up: bool,
}

impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: "",
            delay: Duration::ZERO,
            hang_up: false,
        }
    }
    fn header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_string()));
        self
    }
    fn body(mut self, body: &'static str) -> Self {
        self.body = body;
        self
    }
    fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
    fn hang_up() -> Self {
        Self {
            hang_up: true,
            ..Self::status(0)
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Captured {
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    at: std::time::Instant,
}

impl Captured {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
    /// The line protocol, gunzipped when the request was compressed.
    fn text(&self) -> String {
        if self.header("content-encoding") == Some("gzip") {
            let mut out = String::new();
            flate2::read::GzDecoder::new(&self.body[..])
                .read_to_string(&mut out)
                .unwrap();
            out
        } else {
            String::from_utf8(self.body.clone()).unwrap()
        }
    }
}

struct Mock {
    port: u16,
    requests: Arc<Mutex<Vec<Captured>>>,
}

impl Mock {
    async fn start(replies: Vec<Reply>) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap()],
                PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap(),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let (req, rep) = (requests.clone(), replies.clone());
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let (req, rep) = (req.clone(), rep.clone());
                tokio::spawn(async move {
                    let Ok(mut stream) = acceptor.accept(socket).await else {
                        return;
                    };
                    let mut buf = Vec::new();
                    loop {
                        let Some(head_end) = read_until(&mut stream, &mut buf).await else {
                            return;
                        };
                        let head = String::from_utf8(buf[..head_end].to_vec()).unwrap();
                        let mut lines = head.split("\r\n");
                        let target = lines.next().unwrap().to_string();
                        let headers: Vec<(String, String)> = lines
                            .filter(|l| !l.is_empty())
                            .map(|l| {
                                let (n, v) = l.split_once(':').unwrap();
                                (n.trim().to_ascii_lowercase(), v.trim().to_string())
                            })
                            .collect();
                        let len: usize = headers
                            .iter()
                            .find(|(n, _)| n == "content-length")
                            .map_or(0, |(_, v)| v.parse().unwrap());
                        buf.drain(..head_end + 4);
                        while buf.len() < len {
                            let mut chunk = [0u8; 16 * 1024];
                            let n = stream.read(&mut chunk).await.unwrap_or(0);
                            if n == 0 {
                                return;
                            }
                            buf.extend_from_slice(&chunk[..n]);
                        }
                        let body: Vec<u8> = buf.drain(..len).collect();
                        req.lock().unwrap().push(Captured {
                            target,
                            headers,
                            body,
                            at: std::time::Instant::now(),
                        });
                        let reply = rep
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| Reply::status(204));
                        tokio::time::sleep(reply.delay).await;
                        if reply.hang_up {
                            return;
                        }
                        let mut out = format!(
                            "HTTP/1.1 {} X\r\ncontent-length: {}\r\n",
                            reply.status,
                            reply.body.len()
                        );
                        for (n, v) in &reply.headers {
                            out.push_str(&format!("{n}: {v}\r\n"));
                        }
                        out.push_str("\r\n");
                        out.push_str(reply.body);
                        if stream.write_all(out.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self { port, requests }
    }

    fn requests(&self) -> Vec<Captured> {
        self.requests.lock().unwrap().clone()
    }

    fn texts(&self) -> Vec<String> {
        self.requests().iter().map(Captured::text).collect()
    }
}

async fn read_until<S: tokio::io::AsyncRead + Unpin>(
    s: &mut S,
    buf: &mut Vec<u8>,
) -> Option<usize> {
    loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            return Some(i);
        }
        let mut chunk = [0u8; 4096];
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

pub(super) fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "ts", DataType::TimestampMicrosUTC, false),
            Field::new(FieldId::new(2), "host", DataType::Utf8, true),
            Field::new(FieldId::new(3), "v", DataType::Float64, true),
            Field::new(FieldId::new(4), "s", DataType::Utf8, true),
        ],
    )
    .unwrap()
}

pub(super) fn mapping(time: bool) -> InfluxMapping {
    InfluxMapping {
        measurement: Measurement::Fixed("cpu".into()),
        tags: vec!["host".into()],
        // Implicit fields would include the timestamp column `ts`, which
        // has no line-protocol field type.
        fields: (!time).then(|| vec!["v".to_string(), "s".to_string()]),
        time_column: time.then(|| "ts".to_string()),
        precision: Precision::Us,
    }
}

pub(super) fn row(ts: i64, host: &str, v: f64, s: &str) -> Row {
    Row {
        values: vec![
            Scalar::TimestampMicrosUTC(ts),
            Scalar::utf8(host),
            Scalar::Float64(v),
            Scalar::utf8(s),
        ],
    }
}

pub(super) fn batch(owner: &Arc<MemoryOwner>, rows: Vec<Row>) -> RowBatch {
    let mut b = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        rows.len().max(1),
        1 << 18,
    )
    .unwrap();
    for r in rows {
        b.push(r).unwrap();
    }
    b.finish().unwrap()
}

pub(super) fn config(port: u16, time: bool) -> InfluxDbSinkConfig {
    let mut c = InfluxDbSinkConfig::new(
        format!("https://localhost:{port}/prefix/"),
        "my org",
        "b/x",
        "tok",
        mapping(time),
    );
    c.ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
    c.flush_interval = Duration::from_millis(20);
    c.retry_initial = Duration::from_millis(10);
    c.retry_max = Duration::from_millis(100);
    c.timeout = Duration::from_millis(2000);
    c.flush_timeout = Duration::from_millis(300);
    c
}

fn secrets() -> MapSecretResolver {
    MapSecretResolver::new([("tok".to_string(), "s3cr3t-token".to_string())].into())
}

struct Harness {
    tx: Option<sparrow_io::observed::Sender<RowBatch>>,
    outbox: Arc<InflightCounter>,
    cancel: CancellationToken,
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    task: tokio::task::JoinHandle<()>,
}

impl Harness {
    fn start(config: InfluxDbSinkConfig, owner: Arc<MemoryOwner>) -> Self {
        let port = url::Url::parse(&config.url).unwrap().port().unwrap();
        let diag = IoDiagnostics::new();
        diag.observation.initialize(&owner).unwrap();
        let sink = InfluxDbSink::bind(
            config,
            &secrets(),
            &TargetPolicy::allow("localhost", port),
            owner.clone(),
            diag.clone(),
        )
        .unwrap();
        let (tx, rx) = sparrow_io::observed::channel(64);
        let outbox = Arc::new(InflightCounter::new());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
        Self {
            tx: Some(tx),
            outbox,
            cancel,
            diag,
            owner,
            task,
        }
    }

    async fn send(&self, rows: Vec<Row>) {
        self.outbox.enqueue();
        self.tx
            .as_ref()
            .unwrap()
            .send(batch(&self.owner, rows))
            .await
            .unwrap();
    }

    /// EOF and wait for the Sink to finish.
    async fn finish(mut self) -> (Arc<InflightCounter>, crate::diag::IoSnapshot) {
        drop(self.tx.take());
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .expect("sink finishes")
            .unwrap();
        (self.outbox.clone(), self.diag.snapshot())
    }
}

fn owner() -> Arc<MemoryOwner> {
    MemoryOwner::new(ResourceBudget::compact())
}

#[tokio::test]
async fn request_failure_does_not_fail_the_next_batch_without_rows_in_that_request() {
    let mock = Mock::start(vec![Reply::status(400), Reply::status(204)]).await;
    let mut c = config(mock.port, true);
    c.batch_bytes = 1024;
    c.flush_interval = Duration::from_secs(60);
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "a", 1.0, &"x".repeat(700))]).await;
    h.send(vec![row(2, "b", 2.0, &"y".repeat(700))]).await;
    let (outbox, snap) = h.finish().await;
    let requests = mock.texts();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("cpu,host=a "));
    assert!(requests[1].starts_with("cpu,host=b "));
    assert_eq!(outbox.failed(), 1);
    assert_eq!(
        outbox.acked(),
        1,
        "second batch was carried only by the successful request"
    );
    assert_eq!(snap.influxdb_sink_rows_written, 1);
}

#[tokio::test]
async fn status_retry_requires_explicit_time_column() {
    for status in [429, 503] {
        let mock = Mock::start(vec![Reply::status(status), Reply::status(204)]).await;
        let h = Harness::start(config(mock.port, false), owner());
        h.send(vec![row(1, "h", 1.0, "x")]).await;
        let (outbox, snap) = h.finish().await;
        assert_eq!(mock.requests().len(), 1);
        assert_eq!(outbox.failed(), 1);
        assert_eq!(outbox.acked(), 0);
        assert_eq!(snap.influxdb_sink_retries, 0);
        assert_eq!(snap.influxdb_sink_rejected_rows, 1);
    }
}

fn bound_sink() -> InfluxDbSink {
    InfluxDbSink::bind(
        config(8086, true),
        &secrets(),
        &TargetPolicy::allow("localhost", 8086),
        owner(),
        IoDiagnostics::new(),
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn expired_stop_wins_over_ready_work_and_never_extends_deadline() {
    let sink = bound_sink();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut deadline = None;
    assert_eq!(
        sink.bounded(std::future::ready(1), &mut deadline, &cancel)
            .await,
        Some(1)
    );
    let first = deadline.expect("even immediately ready work observes cancellation");
    tokio::time::advance(sink.config.flush_timeout).await;
    assert_eq!(
        sink.bounded(std::future::ready(2), &mut deadline, &cancel)
            .await,
        None
    );
    assert_eq!(deadline, Some(first));
    let outbox = Arc::new(InflightCounter::new());
    outbox.enqueue();
    let mut state = State {
        pending: None,
        compiled: None,
        deadline,
        fatal: false,
    };
    sink.write_batch(
        batch(&owner(), vec![row(1, "h", 1.0, "x")]),
        &mut state,
        &cancel,
        Some(&outbox),
    )
    .await;
    assert!(state.pending.is_none());
    assert_eq!(outbox.failed(), 1);
    assert_eq!(sink.diag.snapshot().influxdb_sink_discarded_on_close, 1);
}

#[tokio::test]
async fn bad_batches_do_not_accumulate_receipts_and_ack_slots_stay_bounded() {
    let sink = bound_sink();
    let cancel = CancellationToken::new();
    let outbox = Arc::new(InflightCounter::new());
    let mut state = State {
        pending: None,
        compiled: None,
        deadline: None,
        fatal: false,
    };
    for _ in 0..100 {
        outbox.enqueue();
        sink.write_batch(
            batch(&owner(), vec![row(1, "h", f64::NAN, "x")]),
            &mut state,
            &cancel,
            Some(&outbox),
        )
        .await;
        let request = state.pending.as_ref().unwrap();
        assert_eq!(request.rows, 0);
        assert!(
            request.acks.is_empty(),
            "failed encodes never retain receipt handles"
        );
    }
    assert_eq!(outbox.failed(), 100);
    let before = sink.owner.usage().reservation_bytes;
    let receipt = Arc::new(BatchAck {
        outbox: None,
        guard: None,
        encoded: AtomicBool::new(true),
        failed: AtomicBool::new(false),
        _lease: sink
            .owner
            .acquire(CreditKind::Reservation, ACK_STATE)
            .unwrap(),
    });
    let request = state.pending.as_mut().unwrap();
    request.hold(&receipt, 1).unwrap();
    assert_eq!(
        request.acks.capacity(),
        1,
        "not minimum geometric allocation of four slots"
    );
    assert_eq!(
        sink.owner.usage().reservation_bytes - before,
        ACK_STATE + ACK_SLOT
    );
    drop(receipt);
    drop(state);
    assert_eq!(sink.owner.usage().reservation_bytes, 0);
}

#[tokio::test]
async fn exact_line_protocol_url_and_headers_then_ack() {
    let mock = Mock::start(vec![]).await;
    let h = Harness::start(config(mock.port, true), owner());
    h.send(vec![
        row(1_700_000_000_000_001, "h 1", 1.5, "say \"hi\""),
        row(1_700_000_000_000_002, "h,2", -0.0, "C:\\x"),
    ])
    .await;
    h.send(vec![row(-1, "h=3", 1e300, "")]).await;
    let baseline_owner = h.owner.clone();
    let (outbox, snap) = h.finish().await;
    assert_eq!(outbox.acked(), 2);
    assert_eq!(outbox.failed(), 0);
    let reqs = mock.requests();
    assert_eq!(reqs.len(), 1, "both batches coalesced into one request");
    let r = &reqs[0];
    assert_eq!(
        r.target,
        "POST /prefix/api/v2/write?org=my+org&bucket=b%2Fx&precision=us HTTP/1.1"
    );
    assert_eq!(r.header("authorization"), Some("Token s3cr3t-token"));
    assert_eq!(r.header("content-type"), Some("text/plain; charset=utf-8"));
    assert_eq!(r.header("content-encoding"), None);
    assert_eq!(
        r.text(),
        "cpu,host=h\\ 1 v=1.5,s=\"say \\\"hi\\\"\" 1700000000000001\n\
         cpu,host=h\\,2 v=-0.0,s=\"C:\\\\x\" 1700000000000002\n\
         cpu,host=h\\=3 v=1e300,s=\"\" -1\n"
    );
    assert_eq!(snap.influxdb_sink_rows_written, 3);
    assert_eq!(snap.influxdb_sink_requests_ok, 1);
    assert_eq!(snap.influxdb_sink_bytes_sent, r.body.len() as u64);
    // Every request lease was released.
    let delivery = baseline_owner.usage();
    assert!(delivery.reservation_bytes < 64 * 1024, "{delivery:?}");
}

#[tokio::test]
async fn batch_rows_bytes_and_interval_bound_requests() {
    let mock = Mock::start(vec![]).await;
    let mut c = config(mock.port, true);
    c.batch_rows = 2;
    c.flush_interval = Duration::from_secs(60);
    let h = Harness::start(c, owner());
    h.send((0..5).map(|i| row(i, "h", 1.0, "x")).collect())
        .await;
    let (outbox, _) = h.finish().await;
    assert_eq!(outbox.acked(), 1);
    let lines: Vec<usize> = mock.texts().iter().map(|t| t.lines().count()).collect();
    assert_eq!(lines, vec![2, 2, 1]);

    // batch_bytes: a 474-byte row fits twice into 1 KiB, never three times.
    let mock = Mock::start(vec![]).await;
    let mut c = config(mock.port, true);
    c.batch_bytes = 1024;
    c.flush_interval = Duration::from_secs(60);
    let h = Harness::start(c, owner());
    let big = "y".repeat(450);
    h.send((0..3).map(|i| row(i, "h", 1.0, &big)).collect())
        .await;
    // A row larger than batch_bytes alone is dropped; its batch fails.
    h.send(vec![row(9, "h", 1.0, &"z".repeat(2000))]).await;
    let (outbox, snap) = h.finish().await;
    let sizes: Vec<usize> = mock.requests().iter().map(|r| r.body.len()).collect();
    assert_eq!(sizes.len(), 2, "{sizes:?}");
    assert!(sizes.iter().all(|s| *s <= 1024));
    assert_eq!(snap.influxdb_sink_dropped_oversize, 1);
    assert_eq!((outbox.acked(), outbox.failed()), (1, 1));

    // Interval: a lone row is sent without EOF.
    let mock = Mock::start(vec![]).await;
    let h = Harness::start(config(mock.port, true), owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while mock.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("flushed by the interval");
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.outbox.acked() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("acked without EOF");
    h.finish().await;
}

#[tokio::test]
async fn gzip_body_is_charged_and_round_trips() {
    let mock = Mock::start(vec![]).await;
    let mut c = config(mock.port, true);
    c.gzip = true;
    let h = Harness::start(c, owner());
    let rows: Vec<Row> = (0..200)
        .map(|i| row(i, "host-a", i as f64, "repeated text"))
        .collect();
    let expected: String = (0..200)
        .map(|i| format!("cpu,host=host-a v={:?},s=\"repeated text\" {i}\n", i as f64))
        .collect();
    h.send(rows).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(outbox.acked(), 1);
    let r = &mock.requests()[0];
    assert_eq!(r.header("content-encoding"), Some("gzip"));
    assert_eq!(r.text(), expected);
    assert!(r.body.len() < expected.len() / 4, "compressed");
    assert_eq!(snap.influxdb_sink_gzip_fallbacks, 0);
    assert_eq!(snap.influxdb_sink_bytes_sent, r.body.len() as u64);
}

#[tokio::test]
async fn retries_429_503_with_capped_retry_after_then_acks() {
    let mock = Mock::start(vec![
        Reply::status(429).header("retry-after", "3600"),
        Reply::status(503),
        Reply::status(503).header("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT"),
        Reply::status(204),
    ])
    .await;
    let mut c = config(mock.port, false);
    c.retry_max = Duration::from_millis(150);
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(outbox.acked(), 1);
    let reqs = mock.requests();
    assert_eq!(reqs.len(), 4);
    assert!(
        reqs.windows(2).all(|w| w[0].body == w[1].body),
        "same body resent"
    );
    let first_gap = reqs[1].at - reqs[0].at;
    assert!(
        first_gap >= Duration::from_millis(140) && first_gap < Duration::from_secs(2),
        "Retry-After capped at retry_max: {first_gap:?}"
    );
    assert_eq!(snap.influxdb_sink_retries, 3);
    assert_eq!(snap.influxdb_sink_retry_after_waits, 1);
    assert_eq!(snap.influxdb_sink_retry_after_capped, 1);

    // Exhausted retries fail the batch.
    let mock = Mock::start(vec![Reply::status(503); 3]).await;
    let mut c = config(mock.port, true);
    c.max_retries = 2;
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert_eq!(mock.requests().len(), 3);
    assert_eq!(snap.influxdb_sink_retries, 2);
}

#[tokio::test]
async fn terminal_statuses_are_not_retried() {
    for (status, partial, rejected) in [
        (400, 0, 1),
        (413, 0, 1),
        (422, 1, 0),
        (500, 0, 1),
        (418, 0, 1),
    ] {
        let mock = Mock::start(vec![
            Reply::status(status).body(r#"{"code":"invalid","message":"x"}"#)
        ])
        .await;
        let h = Harness::start(config(mock.port, true), owner());
        h.send(vec![row(1, "h", 1.0, "x"), row(2, "h", 1.0, "x")])
            .await;
        h.send(vec![row(3, "h", 1.0, "x")]).await;
        let (outbox, snap) = h.finish().await;
        assert_eq!(mock.requests().len(), 1, "{status}");
        assert_eq!((outbox.acked(), outbox.failed()), (0, 2), "{status}");
        assert_eq!(snap.influxdb_sink_partial_writes, partial, "{status}");
        assert_eq!(snap.influxdb_sink_rejected_requests, rejected, "{status}");
        assert_eq!(snap.influxdb_sink_rejected_rows, 3 * rejected, "{status}");
        assert_eq!(snap.influxdb_sink_retries, 0);
        assert_eq!(snap.influxdb_sink_fatal, 0);
    }
}

#[tokio::test]
async fn auth_and_missing_bucket_are_fatal() {
    for status in [401, 403, 404] {
        let mock = Mock::start(vec![Reply::status(status)]).await;
        let mut c = config(mock.port, true);
        c.batch_rows = 1;
        let h = Harness::start(c, owner());
        h.send(vec![row(1, "h", 1.0, "x"), row(2, "h", 1.0, "x")])
            .await;
        h.send(vec![row(3, "h", 1.0, "x")]).await;
        let cancel = h.cancel.clone();
        let diag = h.diag.clone();
        let (outbox, snap) = h.finish().await;
        assert!(cancel.is_cancelled(), "{status} stops the job");
        assert_eq!(snap.influxdb_sink_fatal, 1);
        assert_eq!(mock.requests().len(), 1, "nothing sent after {status}");
        assert_eq!((outbox.acked(), outbox.failed()), (0, 2));
        assert_eq!(
            diag.observation.endpoints().unwrap().1.state,
            HealthState::Failed
        );
    }
}

#[tokio::test]
async fn transport_errors_retry_only_when_resend_is_idempotent() {
    // The server reads the request and hangs up: the request may have been
    // applied. With a time column the resend overwrites the same points.
    let mock = Mock::start(vec![Reply::hang_up(), Reply::status(204)]).await;
    let h = Harness::start(config(mock.port, true), owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(outbox.acked(), 1);
    assert_eq!(mock.requests().len(), 2);
    assert_eq!(snap.influxdb_sink_retries, 1);

    // Without a time column the server would stamp a second copy: no retry.
    let mock = Mock::start(vec![Reply::hang_up(), Reply::status(204)]).await;
    let h = Harness::start(config(mock.port, false), owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert_eq!(mock.requests().len(), 1);
    assert_eq!(snap.influxdb_sink_retries, 0);
    assert_eq!(
        mock.texts()[0],
        "cpu,host=h v=1.0,s=\"x\"\n",
        "no timestamp"
    );

    // Connection refused: the request never left, so it is retried even
    // without a time column.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let mut c = config(port, false);
    c.max_retries = 3;
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert_eq!(snap.influxdb_sink_retries, 3);
}

#[tokio::test]
async fn stop_deadline_covers_the_request_in_flight() {
    let mock = Mock::start(vec![Reply::status(204).delay(Duration::from_secs(30))]).await;
    let mut c = config(mock.port, true);
    c.flush_timeout = Duration::from_millis(200);
    c.timeout = Duration::from_secs(60);
    let mut h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x"), row(2, "h", 1.0, "x")])
        .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while mock.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    h.send(vec![row(3, "h", 1.0, "x")]).await;
    let started = std::time::Instant::now();
    h.cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), &mut h.task)
        .await
        .expect("bounded by flush_timeout")
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    let snap = h.diag.snapshot();
    assert_eq!((h.outbox.acked(), h.outbox.failed()), (0, 2));
    assert_eq!(snap.influxdb_sink_discarded_on_close, 3);
    assert_eq!(
        h.owner.usage().reservation_bytes,
        0,
        "request lease released"
    );

    // Backoff sleeps are inside the deadline too.
    let mock = Mock::start(vec![Reply::status(503).header("retry-after", "30")]).await;
    let mut c = config(mock.port, true);
    c.retry_max = Duration::from_secs(60);
    c.flush_timeout = Duration::from_millis(200);
    let mut h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while mock.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = std::time::Instant::now();
    h.cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), &mut h.task)
        .await
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(h.outbox.failed(), 1);
}

#[tokio::test]
async fn bad_rows_fail_their_batch_but_not_the_request() {
    let mock = Mock::start(vec![]).await;
    let h = Harness::start(config(mock.port, true), owner());
    h.send(vec![row(1, "h", f64::NAN, "x"), row(2, "h", 1.0, "x")])
        .await;
    h.send(vec![row(3, "h\\", 1.0, "x")]).await;
    h.send(vec![row(4, "h", 2.0, "y")]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(
        mock.texts(),
        vec!["cpu,host=h v=1.0,s=\"x\" 2\ncpu,host=h v=2.0,s=\"y\" 4\n"]
    );
    assert_eq!((outbox.acked(), outbox.failed()), (1, 2));
    assert_eq!(snap.influxdb_sink_dropped_bad, 2);
}

#[tokio::test]
async fn rows_without_credit_are_dropped_not_buffered() {
    let mock = Mock::start(vec![]).await;
    let owner = owner();
    let h = Harness::start(config(mock.port, true), owner.clone());
    // Another operator holds almost all of the reservation.
    let left = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    let hog = owner.acquire(CreditKind::Reservation, left - 2048).unwrap();
    // The batch itself is built before the hog would block it.
    let b = batch(
        &MemoryOwner::new(ResourceBudget::compact()),
        vec![row(1, "h", 1.0, "x")],
    );
    h.outbox.enqueue();
    h.tx.as_ref().unwrap().send(b).await.unwrap();
    let (outbox, snap) = h.finish().await;
    drop(hog);
    assert_eq!(outbox.failed(), 1);
    assert_eq!(snap.influxdb_sink_dropped_budget, 1);
    assert!(mock.requests().is_empty());
}

#[tokio::test]
async fn schema_that_does_not_match_the_mapping_is_fatal() {
    let mock = Mock::start(vec![]).await;
    let mut c = config(mock.port, true);
    c.mapping.tags = vec!["nope".into()];
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let cancel = h.cancel.clone();
    let (outbox, snap) = h.finish().await;
    assert!(cancel.is_cancelled());
    assert_eq!(snap.influxdb_sink_fatal, 1);
    assert_eq!(outbox.failed(), 1);
    assert!(mock.requests().is_empty());
}

#[tokio::test]
async fn default_roots_do_not_trust_a_private_ca() {
    let mock = Mock::start(vec![]).await;
    let mut c = config(mock.port, true);
    c.ca_pem = None;
    c.max_retries = 0;
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    let (outbox, _) = h.finish().await;
    assert_eq!(outbox.failed(), 1);
    assert!(mock.requests().is_empty());
}

#[test]
fn config_validation_and_url() {
    let c = config(8086, true);
    c.validate().unwrap();
    assert_eq!(
        c.write_url().unwrap().as_str(),
        "https://localhost:8086/prefix/api/v2/write?org=my+org&bucket=b%2Fx&precision=us"
    );
    let mut root = config(8086, false);
    root.url = "https://influx.example".into();
    root.mapping.precision = Precision::Ns;
    assert_eq!(
        root.write_url().unwrap().as_str(),
        "https://influx.example/api/v2/write?org=my+org&bucket=b%2Fx&precision=ns"
    );
    type M = fn(&mut InfluxDbSinkConfig);
    let refused: [(M, ErrorCode); 14] = [
        (
            |c| c.url = "http://localhost:8086".into(),
            ErrorCode::PolicyDenied,
        ),
        (
            |c| c.url = "https://u:p@localhost".into(),
            ErrorCode::InvalidArgument,
        ),
        (
            |c| c.url = "https://localhost/?x=1".into(),
            ErrorCode::InvalidArgument,
        ),
        (
            |c| c.url = "https://localhost/#f".into(),
            ErrorCode::InvalidArgument,
        ),
        (|c| c.org = String::new(), ErrorCode::InvalidArgument),
        (|c| c.bucket = "a\nb".into(), ErrorCode::InvalidArgument),
        (
            |c| c.token_secret = String::new(),
            ErrorCode::InvalidArgument,
        ),
        (|c| c.batch_rows = 0, ErrorCode::BoundExceeded),
        (|c| c.batch_bytes = 1023, ErrorCode::BoundExceeded),
        (|c| c.batch_bytes = usize::MAX, ErrorCode::BoundExceeded),
        (|c| c.max_retries = 21, ErrorCode::BoundExceeded),
        (
            |c| c.retry_max = Duration::from_millis(50),
            ErrorCode::BoundExceeded,
        ),
        (
            |c| {
                c.restore = RestoreClaim::Checkpoint {
                    snapshot_id: "s".into(),
                }
            },
            ErrorCode::UnsupportedRestore,
        ),
        (
            |c| c.mapping.measurement = Measurement::Fixed("_x".into()),
            ErrorCode::InvalidArgument,
        ),
    ];
    for (i, (mutate, code)) in refused.into_iter().enumerate() {
        let mut c = config(8086, true);
        mutate(&mut c);
        assert_eq!(c.validate().unwrap_err().code, code, "case {i}");
    }
}

#[test]
fn reservation_math_is_checked_and_saturating() {
    let mut c = config(8086, true);
    c.batch_bytes = 256 * 1024;
    c.batch_rows = 5000;
    let compact = ResourceBudget::compact().reservation_bytes;
    c.check_reservation_budget(compact).unwrap();
    c.gzip = true;
    c.check_reservation_budget(compact).unwrap();
    c.batch_bytes = 1024 * 1024;
    assert_eq!(
        c.check_reservation_budget(compact).unwrap_err().code,
        ErrorCode::BoundExceeded,
        "1 MiB + gzip bound + scratch exceeds half of 4 MiB"
    );
    c.gzip = false;
    c.check_reservation_budget(compact).unwrap();
    c.batch_bytes = usize::MAX;
    assert_eq!(c.peak_bytes(), None);
    assert!(c.check_reservation_budget(usize::MAX).is_err());
    c.batch_bytes = 1024;
    c.batch_rows = usize::MAX;
    assert_eq!(c.peak_bytes(), None);
    assert_eq!(gzip_bound(usize::MAX), None);
    assert_eq!(gzip_bound(0), Some(1042));
    assert_eq!(gzip_bound(1 << 20), Some((1 << 20) + 1024 + 1042));
}

#[test]
fn backoff_is_capped_jittered_and_saturating() {
    let i = Duration::from_millis(100);
    let m = Duration::from_secs(10);
    assert_eq!(backoff(i, m, 1, 0), Duration::from_millis(50));
    assert_eq!(backoff(i, m, 1, 50_000_000), Duration::from_millis(100));
    assert_eq!(
        backoff(i, m, 1, u64::MAX),
        Duration::from_nanos(50_000_000 + u64::MAX % 50_000_001)
    );
    for attempt in [1, 2, 3, 7, 8, 30, 31, 32, u32::MAX] {
        for r in [0, 1, 12345, u64::MAX] {
            let d = backoff(i, m, attempt, r);
            let nominal = i
                .saturating_mul(1u32 << attempt.saturating_sub(1).min(31))
                .min(m);
            assert!(d >= nominal / 2 && d <= nominal, "{attempt} {r} {d:?}");
        }
    }
    assert_eq!(
        backoff(Duration::MAX, Duration::MAX, u32::MAX, 0),
        Duration::from_nanos(u64::MAX / 2)
    );
    use reqwest::header::HeaderValue;
    assert_eq!(
        retry_after(Some(&HeaderValue::from_static("7"))),
        Some(Duration::from_secs(7))
    );
    assert_eq!(
        retry_after(Some(&HeaderValue::from_static("99999999999999999999999"))),
        Some(Duration::from_secs(u64::MAX))
    );
    for v in ["", "-1", "1.5", "Wed, 21 Oct 2015 07:28:00 GMT"] {
        assert_eq!(
            retry_after(Some(&HeaderValue::from_str(v).unwrap())),
            None,
            "{v}"
        );
    }
    assert_eq!(retry_after(None), None);
}

#[test]
fn debug_never_prints_the_token_or_target() {
    let owner = owner();
    let sink = InfluxDbSink::bind(
        config(8086, true),
        &secrets(),
        &TargetPolicy::allow("localhost", 8086),
        owner,
        IoDiagnostics::new(),
    )
    .unwrap();
    let text = format!("{sink:?}");
    assert!(
        !text.contains("s3cr3t") && !text.contains("localhost"),
        "{text}"
    );
    assert!(sink.authorization.is_sensitive());
}

#[test]
fn bind_refuses_policy_secret_and_budget_problems() {
    let c = config(8086, true);
    let e = InfluxDbSink::bind(
        c.clone(),
        &secrets(),
        &TargetPolicy::deny_all(),
        owner(),
        IoDiagnostics::new(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::PolicyDenied);
    let e = InfluxDbSink::bind(
        c.clone(),
        &MapSecretResolver::empty(),
        &TargetPolicy::allow("localhost", 8086),
        owner(),
        IoDiagnostics::new(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::SecretMissing);
    let tiny = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 256 * 1024,
        ..ResourceBudget::compact()
    });
    let e = InfluxDbSink::bind(
        c.clone(),
        &secrets(),
        &TargetPolicy::allow("localhost", 8086),
        tiny,
        IoDiagnostics::new(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::BoundExceeded);
    let mut bad_ca = c;
    bad_ca.ca_pem = Some("not a pem".into());
    let e = InfluxDbSink::bind(
        bad_ca,
        &secrets(),
        &TargetPolicy::allow("localhost", 8086),
        owner(),
        IoDiagnostics::new(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn gzip_without_scratch_credit_sends_plain() {
    let mock = Mock::start(vec![]).await;
    let owner = owner();
    let mut c = config(mock.port, true);
    c.gzip = true;
    let h = Harness::start(c, owner.clone());
    // Leave room for the request buffer but not for the compressor.
    let left = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    let hog = owner
        .acquire(
            CreditKind::Reservation,
            left - (REQUEST_OVERHEAD + 64 * 1024),
        )
        .unwrap();
    let b = batch(
        &MemoryOwner::new(ResourceBudget::compact()),
        vec![row(1, "h", 1.0, "x")],
    );
    h.outbox.enqueue();
    h.tx.as_ref().unwrap().send(b).await.unwrap();
    let (outbox, snap) = h.finish().await;
    drop(hog);
    assert_eq!(outbox.acked(), 1);
    assert_eq!(snap.influxdb_sink_gzip_fallbacks, 1);
    let r = &mock.requests()[0];
    assert_eq!(r.header("content-encoding"), None);
    assert_eq!(r.text(), "cpu,host=h v=1.0,s=\"x\" 1\n");
}

#[tokio::test]
async fn flush_request_sends_without_waiting_for_the_interval() {
    let mock = Mock::start(vec![]).await;
    let mut c = config(mock.port, true);
    c.flush_interval = Duration::from_secs(60);
    let h = Harness::start(c, owner());
    h.send(vec![row(1, "h", 1.0, "x")]).await;
    h.send(vec![row(2, "h", 1.0, "x")]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(mock.requests().is_empty(), "held for the interval");
    h.outbox.request_flush();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.outbox.acked() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("flushed on request");
    assert_eq!(mock.requests().len(), 1);
    h.finish().await;
}
